package kv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/internal/baseline"
)

// Store is a leaderless, eventually-consistent replicated key-value store.
//
// Every node holds a full replica and serves local reads. Writes are applied
// locally under an HLC version and pushed to WriteReplicas-1 peers for acks
// before Set returns, so an acknowledged write survives the loss of any W-1
// nodes at that instant; fire-and-forget gossip and an anti-entropy sweep
// then converge the whole scope (cluster or group). Conflicting writes to the
// same key are resolved everywhere by the deterministic LWW rules on Entry,
// so all replicas agree on the winner even though no coordinator ordered the
// writes. There is deliberately no atomic read-modify-write: two concurrent
// writers race and one wins. Callers that need co-ordination should use the
// lock package.
//
// Scope follows the supplied Membership — cluster-wide for
// ClusterMembership, a metadata-defined group (a zone) for GroupMembership —
// and entries never leak outside it. Multiple stores with different Names
// coexist on one cluster.
type Store struct {
	cluster  *gossip.Cluster
	members  Membership
	config   *Config
	tbl      *table
	registry *registry
	stateTip gossip.HandlerID
	metaTip  gossip.HandlerID

	mu     sync.Mutex
	closed bool

	stopCh chan struct{}
	doneCh chan struct{}

	// sweeping single-flights the anti-entropy work so a slow sweep skips
	// gossip ticks instead of piling up; synced/syncedCount are only touched
	// inside a sweep, ordered by the sweeping atomic.
	sweeping    atomic.Bool
	synced      atomic.Bool
	syncedCount atomic.Int32 // member count the store last caught up at

	// mark is the adaptive group-size water mark the write bar is measured
	// against — the shared baseline tracker (internal/baseline), the same
	// rules the leader package applies to election quorum.
	mark *baseline.Tracker

	// viewMu guards the event-driven member view. Unlike the lock pool —
	// whose every mutation is a network round trip and therefore queries the
	// cluster's own (event-maintained) candidate list each time — the kv
	// write path is a ~400ns local operation, and querying AliveNodes()
	// costs a node-list scan plus a slice allocation per Set. So the store
	// caches: the view is rebuilt on node state and metadata events, the
	// gossip tick only advances the mark's stability clocks against the
	// cache, and writes filter a small slice instead of scanning. viewIDs
	// accumulates every member ever observed (the departure-eligibility
	// set, which cannot be queried from the cluster — a leaving node is
	// already out of the alive list by the time handlers run), pruning
	// nodes the cluster has forgotten. lastCount is the current view's
	// size, kept as an atomic for the resync-on-growth trigger.
	viewMu    sync.Mutex
	viewNodes []*gossip.Node // alive members at the last event
	viewIDs   map[gossip.NodeID]struct{}
	lastCount atomic.Int32

	// Snapshot state: dirty counts table changes (local writes plus adopted
	// remote entries) since the last successful save; a store with a nil
	// Persister never snapshots. snapshotting single-flights the save so a
	// slow Persister — the save runs on its own goroutine — skips triggers
	// instead of piling up; lastSaveNano anchors the interval check.
	// changes counts all table activity and drives amortised housekeeping;
	// snapScheduled flags the one-shot interval timer armed on the clean-to-
	// dirty transition (debounce by timer, not by tick).
	dirty         atomic.Int64
	changes       atomic.Int64
	snapScheduled atomic.Bool
	snapshotting  atomic.Bool
	lastSaveNano  atomic.Int64
	saveWG        sync.WaitGroup

	// clockTimer advances the baseline's stability/dwell windows — the one
	// genuinely time-based rule — as a scheduled one-shot re-armed after
	// every Observe, never a periodic tick. retryTimer re-runs a failed
	// catch-up with capped backoff. Both are stopped in Close.
	clockTimer    *time.Timer
	retryTimer    *time.Timer
	retryMu       sync.Mutex
	retryDelay    time.Duration
	sweepDebounce *time.Timer
}

// NewStore creates a store on the given cluster, scoped to membership.
//
// The membership may be cluster-wide (ClusterMembership) or NodeGroup-scoped
// (GroupMembership); every node expected to hold replicas should create the
// store. Pass the same Config.Name on every participating node.
func NewStore(cluster *gossip.Cluster, membership Membership, config *Config) *Store {
	if cluster == nil {
		panic("kv: cluster must not be nil")
	}
	if membership == nil {
		panic("kv: membership must not be nil")
	}

	config = config.validate()

	// Derive the water-mark timings from the cluster's failure detection
	// when unset, mirroring the leader package's defaults.
	if config.StabilityPeriod <= 0 {
		config.StabilityPeriod = 2 * cluster.DeadNodeTimeout()
	}
	if config.ShrinkDwell <= 0 {
		config.ShrinkDwell = 4 * cluster.DeadNodeTimeout()
	}

	s := &Store{
		cluster: cluster,
		members: membership,
		config:  config,
		tbl:     newTable(config),
		mark: baseline.New(config.StabilityPeriod, config.ShrinkDwell,
			!config.AutoShrinkDisabled, cluster),
		viewIDs: make(map[gossip.NodeID]struct{}),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}

	s.registry = getOrCreateRegistry(cluster)
	s.registry.registerStore(config.Name, s)

	// Restore from long-term storage before anything else: the seeded table
	// then participates in the first bidirectional full sync, so stale
	// snapshot entries lose to fresher cluster state while newer snapshot
	// entries propagate back out — a full-group outage restores to the
	// freshest state any node had persisted.
	if config.Persister != nil {
		s.loadSnapshot()
	}

	// Membership is event-driven: node state and metadata changes rebuild
	// the member view and feed the water mark; the gossip tick only
	// advances the mark's stability clocks. Graceful departures lower the
	// mark immediately — a Leave() broadcast is a positive signal from
	// outside the failure domain, which a partitioned node cannot fake.
	s.stateTip = cluster.HandleNodeStateChangeFunc(s.handleNodeStateChange)
	s.metaTip = cluster.HandleNodeMetadataChangeFunc(s.handleNodeMetadataChange)

	// Record the current view immediately so a store created on an
	// established group enforces its quorum from the first write, not from
	// the first event. A store with no peers is vacuously in sync — but the
	// count it synced at means later-appearing peers trigger a real catch-up
	// pull.
	// Seed the mark with the healthy construction-time view before anything
	// else can observe: the baseline seeds immediately on first observation,
	// so that observation must be the full group, not an already-shrunk one.
	s.observe(s.refreshView())
	if len(s.targets()) == 0 {
		s.synced.Store(true)
		s.syncedCount.Store(s.lastCount.Load())
	}

	go func() {
		<-s.stopCh
		close(s.doneCh)
	}()

	return s
}

// Close shuts the store down. A dirty persistent store flushes one final
// snapshot first (best-effort). Entries held by peers survive; a restarted
// node recovers them from its Persister and via the full-sync catch-up.
func (s *Store) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()

	if s.config.Persister != nil && s.dirty.Load() > 0 {
		// Wait out any in-flight save so the Persister never sees concurrent
		// Save calls, then flush what is left.
		for !s.snapshotting.CompareAndSwap(false, true) {
			time.Sleep(2 * time.Millisecond)
		}
		d := s.dirty.Swap(0)
		if err := s.snapshotNow(); err != nil {
			s.dirty.Add(d)
			s.cluster.Logger().WithError(err).Warn("kv: final snapshot on close failed")
		}
		s.snapshotting.Store(false)
	}

	close(s.stopCh)
	<-s.doneCh
	s.saveWG.Wait()

	s.mu.Lock()
	if s.clockTimer != nil {
		s.clockTimer.Stop()
	}
	s.mu.Unlock()

	s.retryMu.Lock()
	if s.retryTimer != nil {
		s.retryTimer.Stop()
	}
	if s.sweepDebounce != nil {
		s.sweepDebounce.Stop()
	}
	s.retryMu.Unlock()

	s.cluster.RemoveNodeStateChangeHandler(s.stateTip)
	s.cluster.RemoveNodeMetadataChangeHandler(s.metaTip)
	s.tbl.close()
	s.registry.unregisterStore(s.config.Name)
}

// Name returns the store's name.
func (s *Store) Name() string { return s.config.Name }

// WriteReplicas returns the configured W for this store.
func (s *Store) WriteReplicas() int { return s.config.WriteReplicas }

// Synced reports whether the store has completed an initial catch-up with
// its peers (or has none). Until then, reads may miss entries the rest of
// the scope already holds.
func (s *Store) Synced() bool { return s.synced.Load() }

// Get returns the value for key from the local replica. A key past its TTL
// or deleted reads as missing. There is no ErrNoSuchKey: the boolean is the
// answer.
func (s *Store) Get(key string) ([]byte, bool) {
	if err := s.checkClosed(); err != nil {
		return nil, false
	}
	return s.tbl.get(key)
}

// Set stores value under key and returns once the write is durable on
// WriteReplicas nodes (see Config.WriteReplicas for the degradation rules).
//
// ttl <= 0 means no expiry; a positive ttl must not exceed MaxTTL. On
// ErrWriteQuorum the write has not taken effect — the local copy is
// compensated with a tombstone and any value half-landed on a peer is
// dominated by it.
func (s *Store) Set(key string, value []byte, ttl time.Duration) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if key == "" {
		return ErrKeyEmpty
	}
	if len(value) > s.config.MaxValueSize {
		return fmt.Errorf("%w: %d bytes exceeds cap of %d", ErrValueTooLarge, len(value), s.config.MaxValueSize)
	}
	if ttl < 0 || ttl > s.config.MaxTTL {
		return fmt.Errorf("%w: must be between 0 and %v, got %v", ErrTTLOutOfRange, s.config.MaxTTL, ttl)
	}
	if s.config.MaxKeys > 0 {
		if _, exists := s.tbl.get(key); !exists && s.tbl.len() >= s.config.MaxKeys {
			return fmt.Errorf("%w: %d live keys (cap %d)", ErrTooManyKeys, s.tbl.len(), s.config.MaxKeys)
		}
	}

	ent := s.tbl.setLocal(s.cluster.LocalNode().ID, key, value, ttl)
	if ent == nil {
		return ErrStoreClosed
	}
	s.markDirty(1)

	if err := s.replicateBatch([]*Entry{ent}); err != nil {
		// The write is not durable: compensate with a tombstone so the
		// half-written value cannot survive on a peer that did ack.
		s.compensate(ent)
		return err
	}
	return nil
}

// Delete removes key from the store. It is idempotent, and it is a write:
// the tombstone mints a fresh version, so it must be durable on WriteReplicas
// nodes like any Set. A delete that falls short of quorum still stands — the
// safe direction under uncertainty — so ErrWriteQuorum here means "not
// confirmed durable, will converge via anti-entropy", never "undone".
func (s *Store) Delete(key string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if key == "" {
		return ErrKeyEmpty
	}

	ent := s.tbl.deleteLocal(s.cluster.LocalNode().ID, key)
	if ent == nil {
		return ErrStoreClosed
	}
	s.markDirty(1)

	return s.replicateBatch([]*Entry{ent})
}

// DeletePrefix tombstones every live key under prefix and returns how many
// were deleted. It is a scan followed by one replicated batch of tombstones
// (not a magic prefix marker, which would race with concurrent writes to the
// prefix). Quorum failure semantics match Delete: deletions stand, the error
// reports durability.
func (s *Store) DeletePrefix(prefix string) (int, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	tombs := s.tbl.deletePrefixLocal(s.cluster.LocalNode().ID, prefix)
	if len(tombs) == 0 {
		return 0, nil
	}
	s.markDirty(len(tombs))

	if err := s.replicateBatch(tombs); err != nil {
		return len(tombs), err
	}
	return len(tombs), nil
}

// Keys returns the sorted live keys under prefix; the empty prefix returns
// all live keys.
func (s *Store) Keys(prefix string) []string {
	return s.tbl.keys(prefix)
}

// Len returns the number of live keys in the local replica.
func (s *Store) Len() int { return s.tbl.len() }

// Sync runs one bidirectional full-sync exchange with the store's peers and
// reports whether any peer answered. Catch-up also happens automatically on
// the gossip tick; this exists for callers that want convergence now, such
// as tests or a node rejoining after downtime.
func (s *Store) Sync() bool {
	if err := s.checkClosed(); err != nil {
		return false
	}
	if ok := s.catchUp(); ok {
		s.synced.Store(true)
		s.syncedCount.Store(s.lastCount.Load())
		return true
	}
	return false
}

// Forget discounts a member from the quorum bar without requiring a graceful
// leave — the operator's external assertion that the node is gone for good,
// mirroring Cluster.ForgetNode. Local view only: peers keep their own (more
// conservative) bars until they forget the node themselves or see it leave.
func (s *Store) Forget(id gossip.NodeID) bool {
	return s.mark.Forget(id)
}

// compensate undoes a local write whose replication fell short: the fresh
// tombstone beats the half-written value everywhere by version.
func (s *Store) compensate(ent *Entry) {
	if tomb := s.tbl.deleteLocal(s.cluster.LocalNode().ID, ent.Key); tomb != nil {
		go s.gossipEntries([]*Entry{tomb})
	}
}

// onGossipTick runs on the cluster's gossip event: reap inline (a short map
// scan, safe for the synchronous tick), hand a due snapshot to its own
// goroutine (never blocking the tick on a Persister), then single-flighted
// background anti-entropy — catch-up until synced and again whenever the
// observed group has grown (a lone store meeting its first peers pulls their
// state), and re-gossip batches in between.
// observe feeds the baseline and re-arms its clock timer: every membership
// event and every timer firing routes through here, so the stability and
// dwell windows advance on schedule without any periodic tick.
func (s *Store) observe(nodes []*gossip.Node) {
	s.mark.Observe(nodes, s.config.NowFn())

	if d := s.mark.Deadline(s.config.NowFn()); d > 0 {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		if s.clockTimer != nil {
			s.clockTimer.Stop()
		}
		s.clockTimer = time.AfterFunc(d, func() {
			if s.checkClosed() != nil {
				return
			}
			s.observe(s.currentView())
		})
		s.mu.Unlock()
	}
}

// maybeSweep runs the anti-entropy pass single-flighted on its own goroutine:
// catch-up until synced and again whenever the observed group has grown
// (a lone store meeting its first peers pulls their state), and re-gossip
// batches in between. Called from the gossip tick and — for faster
// convergence — directly from membership events, which is why the work is
// dispatched rather than done inline: the event goroutine must not wait on
// network round trips.
func (s *Store) maybeSweep() {
	s.sweep(false)
}

// maybeSweepAll is the event-triggered variant: when already synced it
// regossips every batch to every peer, because a membership event is rare
// and must heal totally rather than across successive paced rounds.
func (s *Store) maybeSweepAll() {
	s.sweep(true)
}

func (s *Store) sweep(all bool) {
	if s.sweeping.CompareAndSwap(false, true) {
		go func() {
			defer s.sweeping.Store(false)
			if !s.synced.Load() || s.lastCount.Load() > s.syncedCount.Load() {
				if s.catchUp() {
					s.synced.Store(true)
					s.syncedCount.Store(s.lastCount.Load())
				}
				return
			}
			if all {
				s.regossipAll()
				return
			}
			s.regossip()
		}()
	}
}

// sweepOnGrowth kicks the anti-entropy pass when the member view has grown
// past what it last synced with — the event-driven trigger that lets a
// joining, healing, or group-growing store pull state immediately instead
// of waiting out the gossip interval. Growth-gated by construction: shrink
// and flap events do not fire sweeps, so churn cannot stampede the peers
// with full-sync requests.
func (s *Store) sweepOnGrowth() {
	if s.lastCount.Load() > s.syncedCount.Load() {
		s.maybeSweepAll()
	}
}

// --- optional persistence (see persist.go for the contract) ---

// markDirty records table changes and drives everything that was once
// tick-based, amortised over activity instead: every so many changes the
// table is reaped (memory hygiene; reads already expire lazily) and the
// anti-entropy sweep runs, so a busy store keeps itself healthy without any
// periodic timer. A store with a Persister additionally counts toward the
// snapshot triggers — the write threshold here, the interval via a one-shot
// timer armed on the clean-to-dirty transition.
func (s *Store) markDirty(n int) {
	if n <= 0 {
		return
	}
	total := s.changes.Add(int64(n))
	if total%reapEvery == 0 {
		go s.tbl.reap()
	}

	// Every change (re)arms one debounced total sweep, firing shortly after
	// activity settles: a fire-and-forget delivery silently dropped —
	// sub-detection partitions produce neither events nor send errors — is
	// healed by the next burst's wake. No activity, no timer.
	s.retryMu.Lock()
	if s.sweepDebounce != nil {
		s.sweepDebounce.Stop()
	}
	s.sweepDebounce = time.AfterFunc(sweepDebounceDelay, func() { s.sweep(true) })
	s.retryMu.Unlock()

	if s.config.Persister == nil {
		return
	}
	d := s.dirty.Add(int64(n))
	if d >= int64(s.config.SnapshotWrites) {
		s.maybeSnapshotAsync()
		return
	}
	if d == int64(n) && s.snapScheduled.CompareAndSwap(false, true) {
		// Clean store just went dirty: arm the interval debounce.
		s.mu.Lock()
		if !s.closed {
			time.AfterFunc(s.config.SnapshotInterval, func() {
				s.snapScheduled.Store(false)
				s.maybeSnapshotAsync()
			})
		}
		s.mu.Unlock()
	}
}

// Housekeeping cadence: reap every 1024 changes; sweeps ride the mutation
// debounce. A quiet store runs neither — its reads are lazy-expired, and
// healing waits for a membership event, further activity, or an explicit
// Sync().
const (
	reapEvery          = 1024
	sweepDebounceDelay = 750 * time.Millisecond
)

// adopt merges a batch of remote entries and counts what actually changed,
// so a replica that only ever receives gossip still persists its view of
// the data to its own Persister.
func (s *Store) adopt(entries []*Entry) int {
	n := s.tbl.applyAll(entries)
	s.markDirty(n)
	return n
}

// maybeSnapshotAsync hands a due snapshot to a dedicated goroutine. A save
// in flight holds the single-flight flag, so a slow Persister makes later
// triggers skip rather than pile up — and nothing on the gossip tick, the
// anti-entropy sweep, or the write path ever waits on storage.
func (s *Store) maybeSnapshotAsync() {
	if s.config.Persister == nil {
		return
	}
	if !s.snapshotDue(s.config.NowFn()) {
		return
	}
	if !s.snapshotting.CompareAndSwap(false, true) {
		return
	}

	// Register with the WaitGroup under the close flag so a save racing
	// Close cannot outlive it.
	s.mu.Lock()
	if s.closed {
		s.snapshotting.Store(false)
		s.mu.Unlock()
		return
	}
	s.saveWG.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.saveWG.Done()
		defer s.snapshotting.Store(false)
		s.runSnapshot()
	}()
}

// snapshotDue reports whether unsaved changes have reached the write
// threshold or outlived the interval. A clean store is never due — no
// write activity means no disk activity.
func (s *Store) snapshotDue(now time.Time) bool {
	d := s.dirty.Load()
	if d <= 0 {
		return false
	}
	if d >= int64(s.config.SnapshotWrites) {
		return true
	}
	return now.UnixNano()-s.lastSaveNano.Load() >= int64(s.config.SnapshotInterval)
}

// runSnapshot swaps the dirty counter to zero, saves, and restores the count
// on failure so no change is silently considered persisted. The swap happens
// before the snapshot is taken: changes landing during the save accumulate
// in the fresh counter and are simply re-captured next time — the safe
// (over-persisting) direction.
func (s *Store) runSnapshot() {
	d := s.dirty.Swap(0)
	if d <= 0 {
		return
	}
	if err := s.snapshotNow(); err != nil {
		s.dirty.Add(d)
		s.cluster.Logger().WithError(err).Warn("kv: snapshot save failed; will retry")
		return
	}
	s.lastSaveNano.Store(s.config.NowFn().UnixNano())
}

// snapshotNow hands the current view — live entries and live tombstones —
// to the Persister, which encodes it however it wants.
func (s *Store) snapshotNow() error {
	return s.config.Persister.Save(&StoreSnapshot{
		Store:   s.config.Name,
		Entries: s.tbl.snapshot(),
	})
}

// loadSnapshot seeds the table from the Persister at construction. A load
// error, a nil snapshot, or one for a different store name logs and starts
// empty: peers repopulate the store via sync, and a snapshot that cannot be
// read cannot be trusted regardless.
func (s *Store) loadSnapshot() {
	snap, err := s.config.Persister.Load()
	if err != nil {
		s.cluster.Logger().WithError(err).Warn("kv: snapshot load failed; starting empty")
		return
	}
	if snap == nil {
		return
	}
	if snap.Store != s.config.Name {
		s.cluster.Logger().Warn(fmt.Sprintf("kv: snapshot is for store %q, not %q; starting empty", snap.Store, s.config.Name))
		return
	}
	s.tbl.applyAll(snap.Entries) // not markDirty: these came from storage
}

// Snapshot forces an immediate save and returns once it is complete — the
// manual trigger, a no-op in memory-only mode. It waits out any in-flight
// save first, so a forced snapshot is never lost behind a slow one.
func (s *Store) Snapshot() error {
	if s.config.Persister == nil {
		return nil
	}
	if err := s.checkClosed(); err != nil {
		return err
	}

	for !s.snapshotting.CompareAndSwap(false, true) {
		if err := s.checkClosed(); err != nil {
			return err
		}
		time.Sleep(2 * time.Millisecond)
	}
	defer s.snapshotting.Store(false)

	d := s.dirty.Swap(0)
	if err := s.snapshotNow(); err != nil {
		s.dirty.Add(d)
		return err
	}
	s.lastSaveNano.Store(s.config.NowFn().UnixNano())
	return nil
}

// handleNodeStateChange rebuilds the member view on any node state change
// and feeds it to the water mark, advancing its stability clock on real
// membership events. A graceful departure lowers the mark immediately; a
// crash or partition produces no signal — only absence — and is handled by
// the mark's one-at-a-time dwell rule.
func (s *Store) handleNodeStateChange(node *gossip.Node, prevState gossip.NodeState) {
	if node == nil {
		return
	}
	// Eligibility is decided from the pre-event view: a leaving node is
	// typically already out of the alive set by the time handlers run.
	wasMember := s.inView(node.ID)

	// A peer returning from suspect or dead is not growth, but we may have
	// missed its writes while it was partitioned from us — pull.
	peerReturned := prevState != gossip.NodeAlive && node.GetObservedState() == gossip.NodeAlive

	nodes := s.refreshView()
	s.observe(nodes)
	if s.lastCount.Load() > s.syncedCount.Load() || peerReturned {
		s.maybeSweepAll()
	}

	if node.GetObservedState() == gossip.NodeLeaving && wasMember {
		s.mark.NoteGracefulDeparture(node.ID)
		s.observe(nodes)
	}
}

// handleNodeMetadataChange rebuilds the member view when node metadata
// changes — group-scoped memberships gain and lose members this way.
func (s *Store) handleNodeMetadataChange(node *gossip.Node) {
	s.observe(s.refreshView())
	s.sweepOnGrowth()
}

// refreshView rebuilds the cached member view from the membership scope and
// returns it. Alive-membership snapshots plus the ever-seen eligibility set
// are maintained here; callers feed the result to the water mark.
func (s *Store) refreshView() []*gossip.Node {
	nodes := aliveMembers(s.members, s.cluster)

	s.viewMu.Lock()
	s.viewNodes = nodes
	s.viewIDs = pruneForgotten(s.cluster, s.viewIDs, nodes)
	s.viewMu.Unlock()

	s.lastCount.Store(int32(len(nodes)))
	return nodes
}

// inView reports whether the node has ever been observed inside the store's
// scope — the departure-eligibility set.
func (s *Store) inView(id gossip.NodeID) bool {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	_, ok := s.viewIDs[id]
	return ok
}

// currentView returns a copy of the cached member view — no membership
// scan; the view is maintained by events.
func (s *Store) currentView() []*gossip.Node {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	out := make([]*gossip.Node, len(s.viewNodes))
	copy(out, s.viewNodes)
	return out
}

// pruneForgotten adds the currently observed members to the ever-seen set
// and drops entries for nodes the cluster no longer knows at all.
func pruneForgotten(c *gossip.Cluster, seen map[gossip.NodeID]struct{}, nodes []*gossip.Node) map[gossip.NodeID]struct{} {
	for _, n := range nodes {
		seen[n.ID] = struct{}{}
	}
	for id := range seen {
		if c.GetNode(id) == nil {
			delete(seen, id)
		}
	}
	return seen
}

// targets returns the store's alive peers — the cached member view minus
// the local node — as candidate replica targets. The view is maintained by
// events, so this is a filter of a small slice, never a membership scan.
func (s *Store) targets() []*gossip.Node {
	self := s.cluster.LocalNode().ID
	view := s.currentView()
	out := make([]*gossip.Node, 0, len(view))
	for _, n := range view {
		if n == nil || n.ID == self {
			continue
		}
		if n.GetObservedState() != gossip.NodeAlive {
			continue
		}
		out = append(out, n)
	}
	return out
}

// needAcks is the number of peer acks a write must collect: W-1, degraded to
// the adopted water mark. The mark follows the shared baseline rules — it
// rises with stable growth and falls only on graceful leaves, one-at-a-time
// dwell-confirmed shrinkage, or Forget — so peer failures never lower the
// bar, but a genuinely scaled-down group re-opens for writes.
func (s *Store) needAcks() int {
	need := s.config.WriteReplicas - 1
	if mark := s.mark.Size() - 1; need > mark {
		need = mark
	}
	if need < 0 {
		need = 0
	}
	return need
}

// aliveMembers filters a membership's nodes to alive ones, treating the local
// node as a member of its own store even when the membership does not list it
// (e.g. a client-node store scoped to a group it does not belong to — its
// local copy still counts as one replica).
func aliveMembers(m Membership, c *gossip.Cluster) []*gossip.Node {
	var out []*gossip.Node
	self := c.LocalNode().ID
	selfSeen := false
	for _, n := range m.Nodes() {
		if n == nil {
			continue
		}
		if n.ID == self {
			selfSeen = true
		}
		if n.GetObservedState() == gossip.NodeAlive {
			out = append(out, n)
		}
	}
	if !selfSeen {
		out = append(out, c.LocalNode())
	}
	return out
}

func (s *Store) checkClosed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	return nil
}
