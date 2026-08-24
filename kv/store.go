package kv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paularlott/gossip"
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
	cluster   *gossip.Cluster
	members   Membership
	config    *Config
	tbl       *table
	registry  *registry
	gossipTip gossip.HandlerID
	stateTip  gossip.HandlerID

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

	// water is the adaptive group-size mark the write bar is measured
	// against (see water.go). lastCount is the most recently observed
	// member count, kept as an atomic for the resync-on-growth trigger.
	water     *waterTracker
	lastCount atomic.Int32
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
		tbl:     newTable(config, cluster.LocalNode().ID),
		water: newWaterTracker(config.StabilityPeriod, config.ShrinkDwell,
			!config.AutoShrinkDisabled, cluster, config.NowFn),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	s.registry = getOrCreateRegistry(cluster)
	s.registry.registerStore(config.Name, s)

	// Anti-entropy rides the cluster's gossip event — the same self-adjusting
	// cadence the cluster uses for its own state exchange — so the store
	// keeps no timer of its own.
	s.gossipTip = cluster.HandleGossipFunc(s.onGossipTick)

	// Graceful departures lower the water mark immediately: a Leave()
	// broadcast is a positive signal from outside the failure domain, which
	// a partitioned node cannot fake.
	s.stateTip = cluster.HandleNodeStateChangeFunc(s.handleNodeStateChange)

	// Record the current view immediately so a store created on an
	// established group enforces its quorum from the first write, not from
	// the first tick. A store with no peers is vacuously in sync — but the
	// count it synced at means later-appearing peers trigger a real catch-up
	// pull.
	s.observeMembers()
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

// Close shuts the store down. Entries held by peers survive; a restarted
// node recovers them via the full-sync catch-up.
func (s *Store) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()

	close(s.stopCh)
	<-s.doneCh

	s.cluster.RemoveGossipHandler(s.gossipTip)
	s.cluster.RemoveNodeStateChangeHandler(s.stateTip)
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

	ent := s.tbl.setLocal(key, value, ttl)
	if ent == nil {
		return ErrStoreClosed
	}

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

	ent := s.tbl.deleteLocal(key)
	if ent == nil {
		return ErrStoreClosed
	}

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

	tombs := s.tbl.deletePrefixLocal(prefix)
	if len(tombs) == 0 {
		return 0, nil
	}

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

// EntryCount returns total entries including tombstones (diagnostics).
func (s *Store) EntryCount() int { return s.tbl.entryCount() }

// TombstoneCount returns the number of live tombstones (diagnostics).
func (s *Store) TombstoneCount() int { return s.tbl.tombstoneCount() }

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
	return s.water.forget(id)
}

// compensate undoes a local write whose replication fell short: the fresh
// tombstone beats the half-written value everywhere by version.
func (s *Store) compensate(ent *Entry) {
	if tomb := s.tbl.deleteLocal(ent.Key); tomb != nil {
		go s.gossipEntries([]*Entry{tomb})
	}
}

// onGossipTick runs on the cluster's gossip event: reap inline (a short map
// scan, safe for the synchronous tick), then single-flighted background
// anti-entropy — catch-up until synced and again whenever the observed group
// has grown (a lone store meeting its first peers pulls their state), and
// re-gossip batches in between.
func (s *Store) onGossipTick() {
	s.observeMembers()
	s.tbl.reap()

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
			s.regossip()
		}()
	}
}

// handleNodeStateChange lowers the water mark when a member announces a
// graceful departure. Crashes and partitions produce no signal — only
// absence — and are handled by the tracker's one-at-a-time dwell rule.
func (s *Store) handleNodeStateChange(node *gossip.Node, prevState gossip.NodeState) {
	if node == nil || node.GetObservedState() != gossip.NodeLeaving {
		return
	}
	if s.water.wasMember(node.ID) {
		s.water.noteGracefulDeparture(node.ID)
	}
}

// observeMembers feeds the current alive-member view into the water mark.
// The count includes the local node when it is a member.
func (s *Store) observeMembers() {
	alive := aliveMembers(s.members, s.cluster)
	s.lastCount.Store(int32(len(alive)))
	s.water.observe(alive, s.config.NowFn())
}

// targets returns the store's alive peers — members minus the local node —
// as candidate replica targets.
func (s *Store) targets() []*gossip.Node {
	self := s.cluster.LocalNode().ID
	out := make([]*gossip.Node, 0, len(s.members.Nodes()))
	for _, n := range s.members.Nodes() {
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
// the adopted water mark. The mark follows the rules in water.go — it rises
// with stable growth and falls only on graceful leaves, one-at-a-time
// dwell-confirmed shrinkage, or Forget — so peer failures never lower the
// bar, but a genuinely scaled-down group re-opens for writes.
func (s *Store) needAcks() int {
	need := s.config.WriteReplicas - 1
	if mark := s.water.size() - 1; need > mark {
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
