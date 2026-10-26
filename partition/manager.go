package partition

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/logger"
)

// partMutex is the per-partition mutex mu[p]. Unlike sync.Mutex it can be waited for until a context ends, so a handler
// that cannot get its partition refuses instead of waiting for it indefinitely.
type partMutex chan struct{}

func newPartMutex() partMutex { return make(partMutex, 1) }

// lock waits for the mutex until ctx ends.
func (m partMutex) lock(ctx context.Context) error {
	select {
	case m <- struct{}{}:
		return nil
	default:
	}
	select {
	case m <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m partMutex) unlock() { <-m }

// tryLock takes p's mutex for a request handler (which runs in a goroutine of the Manager, never on one of the cluster's
// incoming workers), which refuses rather than wait for it longer than OwnerTimeout.
func (m *Manager) tryLock(ps *partState) bool {
	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.OwnerTimeout)
	defer cancel()
	return ps.mu.lock(ctx) == nil
}

// partState is the volatile (memory only) state of one partition.
type partState struct {
	// mu serialises everything that reads or changes the partition's durable state, and takeover and replication
	// (which wait for the network with it held: a partition does one thing at a time). Lock order: mu, then vmu.
	mu partMutex

	// maxCounter is the highest Epoch.Counter seen for the partition anywhere (promises, refusals, heartbeats, writes).
	maxCounter atomic.Uint64

	takingOver atomic.Bool  // a takeover goroutine is running
	catchingUp atomic.Bool  // an asynchronous catch-up is in flight
	claiming   atomic.Bool  // a claim request is in flight
	applying   atomic.Bool  // applyEntries is draining next (see queueEntry)
	changes    atomic.Int32 // forwarded changes of the partition held as owner (Config.Tuning.MaxChangesPerPartition)

	// roundMu guards the read-confirmation rounds (see read.go): roundRunning says one is in flight, roundNext is the one
	// that starts when it ends, which the reads that arrived meanwhile wait for.
	roundMu      sync.Mutex
	roundRunning bool
	roundNext    *readRound

	// appliedMu guards appliedCh, the channel WaitApplied waits on: it is closed (and replaced) whenever this node commits
	// or imports something on the partition.
	appliedMu sync.Mutex
	appliedCh chan struct{}

	// vmu guards the fields below; it is never held across I/O.
	vmu          sync.RWMutex
	owner        gossip.NodeID // owner believed in (from heartbeats, or self); EmptyNodeID: none
	ownerEpoch   Epoch
	ownerSeen    time.Time // last heartbeat from the owner (for this node as owner: last majority contact)
	heard        time.Time // follower: last heartbeat from an owner, for read freshness (CanRead); a hand-over keeps it
	leading      bool      // this node completed a promise round at myEpoch and has not stepped down
	myEpoch      Epoch
	settled      bool      // owner only: the takeover has committed the write it settled (takeoverSettled)
	leaderSince  time.Time // owner only: when its promise round began
	lastMajority time.Time // owner only, once settled: last time a write or heartbeat round got majority answers
	relinquished Epoch     // highest epoch whose owner told us it stepped down: its late heartbeats are ignored

	// Ranked fallback (see ownershipTick). ownerlessSince is when the partition became ownerless at this node (zero:
	// not known to be); it is reset whenever an owner is seen. failures counts the takeovers of the partition that did not
	// complete, one after the other, and backoffUntil is when this node may try (or claim) again. lastClaim is when it last
	// asked the owner to hand the partition over.
	ownerlessSince time.Time
	failures       int
	backoffUntil   time.Time
	lastClaim      time.Time

	// The cache of the durable state (see status.go): promised and committed are lower bounds of the Store's values,
	// raised after every SetPromised, Commit and Import the package makes and whenever it reads the State under the
	// partition's mutex; cacheLoaded says they have been read from the Store once. They let a heartbeat entry or a
	// commit notice be decided without the partition's mutex or a Store call.
	promised    Epoch
	committed   Pos
	cacheLoaded bool

	// What the owner reports (accepted heartbeat entries and commit notices), for staleness and divergence (status.go).
	// ownerCommitted is the highest committed position an owner reported, raised at reportedAt. lagSince is when this
	// node learned of a reported position lagTarget that it has still not committed (zero: not behind); aheadSince is
	// since when every report has shown a committed Seq below this node's (zero: not ahead); diverged is set once that
	// lasted divergeAfter, and kept until the Manager is restarted.
	ownerCommitted Pos
	hbCommitted    Pos // the highest committed position an owner reported in a heartbeat (claimDue)
	reportedAt     time.Time
	lagTarget      uint64
	lagSince       time.Time
	aheadSince     time.Time
	diverged       bool

	// next is the latest accepted entry whose settling and catch-up have not run yet (see queueEntry).
	next *pendingEntry

	// Background catch-ups of the partition that failed in a row, and when that was last logged at warn level.
	catchUpFailures int
	catchUpWarned   time.Time
}

// appliedChan returns the channel that is closed at the next local commit or import of the partition.
func (ps *partState) appliedChan() chan struct{} {
	ps.appliedMu.Lock()
	defer ps.appliedMu.Unlock()
	if ps.appliedCh == nil {
		ps.appliedCh = make(chan struct{})
	}
	return ps.appliedCh
}

// notifyApplied wakes everyone in WaitApplied.
func (ps *partState) notifyApplied() {
	ps.appliedMu.Lock()
	if ps.appliedCh != nil {
		close(ps.appliedCh)
		ps.appliedCh = nil
	}
	ps.appliedMu.Unlock()
}

// raise lifts maxCounter to c.
func (ps *partState) raise(c uint64) {
	for {
		cur := ps.maxCounter.Load()
		if c <= cur || ps.maxCounter.CompareAndSwap(cur, c) {
			return
		}
	}
}

// ownershipEvent is one change of a partition's believed owner.
type ownershipEvent struct {
	p     int
	owner gossip.NodeID
	epoch Epoch
}

// Manager runs the partition protocol on one node. Create it with New, then Start it.
type Manager struct {
	cfg   Config
	store Store
	net   network
	log   logger.Logger
	self  gossip.NodeID
	now   func() time.Time // replaced in tests

	parts []*partState

	// formed: the node has taken part in the protocol or synchronised with an intact majority (Config.Formed); until
	// then it is unformed, and says so in its State answers.
	formed atomic.Bool

	ready    atomic.Bool // the startup synchronisation has completed (partition.ready, unless the node is ineligible)
	resync   atomic.Bool // resync mode: the node's data is not intact; it promises and stores nothing
	eligible atomic.Bool // SetEligible: the node may own partitions

	ctx    context.Context // ends on Stop
	cancel context.CancelFunc

	lifeMu     sync.Mutex
	started    bool
	stopped    bool
	syncWarned time.Time // when syncWaiting last logged at warn level
	wg         sync.WaitGroup

	changeSem  chan struct{} // MaxConcurrentChanges: changes running on this node as owner for other nodes
	readSem    chan struct{} // MaxConcurrentReads: reads running on this node as owner for other nodes
	reqSem     chan struct{} // MaxConcurrentRequests: promise, write, claim, state and heartbeat requests being served
	catchUpSem chan struct{} // MaxConcurrentCatchUps: background catch-ups running

	readRounds  atomic.Int64 // leadership confirmation rounds run (for tests: reads share them)
	divergences atomic.Int64 // partitions found diverged since Start (Status.Divergences)

	handlersMu sync.RWMutex
	handlers   []func(p int, owner gossip.NodeID, epoch Epoch)
	resynced   []func()
	evMu       sync.Mutex
	evQueue    []ownershipEvent
	evWake     chan struct{}
}

// New creates a Manager on the cluster. It validates and defaults cfg (see Config) but starts nothing.
func New(c *gossip.Cluster, cfg Config) (*Manager, error) {
	if c == nil {
		return nil, errors.New("partition: cluster is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = c.Logger()
	}
	return newManager(cfg, &clusterNet{c: c})
}

// newManager builds a Manager over any network.
func newManager(cfg Config, nw network) (*Manager, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	m := &Manager{
		cfg:        cfg,
		store:      cfg.Store,
		net:        nw,
		log:        cfg.Logger.WithGroup("partition"),
		self:       nw.self(),
		now:        time.Now,
		parts:      make([]*partState, cfg.Partitions),
		changeSem:  make(chan struct{}, cfg.Tuning.MaxConcurrentChanges),
		readSem:    make(chan struct{}, cfg.Tuning.MaxConcurrentReads),
		reqSem:     make(chan struct{}, cfg.Tuning.MaxConcurrentRequests),
		catchUpSem: make(chan struct{}, cfg.Tuning.MaxConcurrentCatchUps),
		evWake:     make(chan struct{}, 1),
	}
	m.eligible.Store(true)
	m.formed.Store(cfg.Formed == nil || cfg.SetFormed == nil || cfg.Formed())
	m.ctx, m.cancel = context.WithCancel(context.Background())
	for i := range m.parts {
		m.parts[i] = &partState{mu: newPartMutex()}
	}
	return m, nil
}

// Start registers the protocol's handlers and starts its loops. It returns an error if the Manager was already started or
// stopped, or if another Manager already runs on the cluster.
func (m *Manager) Start() error {
	m.lifeMu.Lock()
	defer m.lifeMu.Unlock()
	if m.stopped {
		return errors.New("partition: manager stopped")
	}
	if m.started {
		return errors.New("partition: manager already started")
	}
	m.resync.Store(!m.store.Intact())
	m.ready.Store(false)
	m.net.publish(m.cfg.ClusterSize, m.cfg.ClusterID, false)
	if err := m.net.register(m); err != nil {
		return err
	}
	m.started = true

	for _, loop := range []func(){m.eventLoop, m.ownershipLoop, m.heartbeatLoop, m.syncLoop} {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			loop()
		}()
	}
	m.log.Info("partition manager started", "partitions", m.cfg.Partitions, "size", m.cfg.ClusterSize,
		"resync", m.resync.Load())
	return nil
}

// Stop stops the loops and steps down from every partition this node owns, telling the others so that a successor
// can take over at once. It does not leave the cluster. A stopped Manager cannot be started again.
func (m *Manager) Stop() {
	m.lifeMu.Lock()
	if m.stopped {
		m.lifeMu.Unlock()
		return
	}
	m.stopped = true
	started := m.started
	m.lifeMu.Unlock()

	if started {
		m.ready.Store(false)
		m.net.publish(m.cfg.ClusterSize, m.cfg.ClusterID, false)
		for p := range m.parts {
			m.stepDown(p)
		}
	}
	m.cancel()
	m.wg.Wait()
	if started {
		m.net.unregister()
	}
}

// spawn runs fn in a goroutine tracked by Stop; it does nothing once the Manager is stopped.
func (m *Manager) spawn(fn func()) bool {
	m.lifeMu.Lock()
	if m.stopped {
		m.lifeMu.Unlock()
		return false
	}
	m.wg.Add(1)
	m.lifeMu.Unlock()
	go func() {
		defer m.wg.Done()
		fn()
	}()
	return true
}

// spawnOnce runs fn in a goroutine of the Manager unless one started with the same flag is still running: the work
// that has at most one instance in flight per partition (takeover, claim, catch-up).
func (m *Manager) spawnOnce(running *atomic.Bool, fn func()) {
	if !running.CompareAndSwap(false, true) {
		return
	}
	if !m.spawn(func() {
		defer running.Store(false)
		fn()
	}) {
		running.Store(false)
	}
}

// isStopped reports whether Stop has been called.
func (m *Manager) isStopped() bool {
	m.lifeMu.Lock()
	defer m.lifeMu.Unlock()
	return m.stopped
}

// part returns the volatile state of p, or nil if p is out of range.
func (m *Manager) part(p int) *partState {
	if p < 0 || p >= len(m.parts) {
		return nil
	}
	return m.parts[p]
}

// Resyncing reports whether this node is in resync mode: its data was lost or restored (Store.Intact was false at Start)
// and it has not yet caught up from a majority of the other nodes. It promises and stores nothing, and serves no reads,
// until it has.
func (m *Manager) Resyncing() bool { return m.resync.Load() }

// HandleResyncedFunc registers a function called once, when the node leaves resync mode (so that an application can, for
// example, rewrite the marker that tells it its data directory can be trusted again). It is not called for a node that
// started intact. Register it before Start. It runs on the goroutine of the startup synchronisation, before the node
// declares itself ready, so it should not block for long.
func (m *Manager) HandleResyncedFunc(fn func()) {
	m.handlersMu.Lock()
	m.resynced = append(m.resynced, fn)
	m.handlersMu.Unlock()
}

// leaveResync ends resync mode and calls the HandleResyncedFunc handlers, once.
func (m *Manager) leaveResync() {
	if !m.resync.CompareAndSwap(true, false) {
		return
	}
	m.log.Info("resync complete")
	m.handlersMu.RLock()
	hs := append([]func(){}, m.resynced...)
	m.handlersMu.RUnlock()
	for _, h := range hs {
		h()
	}
}

// WaitApplied returns once this node has applied the write at pos (its committed sequence is at least pos.Seq: equal
// sequence numbers are the same write), or when ctx ends, with ctx's error. It wakes on every local commit or import, so
// after a Write on another node it returns as soon as the owner's commit notice reaches this one. It returns
// ErrUnavailable if the Manager stops while it waits.
func (m *Manager) WaitApplied(ctx context.Context, p int, pos Pos) error {
	ps := m.part(p)
	if ps == nil {
		return fmt.Errorf("partition: partition %d out of range [0, %d)", p, len(m.parts))
	}
	for {
		ch := ps.appliedChan() // before the check, so that a commit between the two is not missed
		st, err := m.store.State(p)
		if err != nil {
			return err
		}
		if st.Committed.Seq >= pos.Seq {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-m.ctx.Done():
			return ErrUnavailable
		}
	}
}

// Ready reports whether this node has caught up with a majority and waited out ReadyHoldDown, so that it is a
// candidate for ownership (if it is eligible, see SetEligible).
func (m *Manager) Ready() bool { return m.ready.Load() }

// SetEligible makes this node eligible to own partitions, or not; a Manager starts eligible. An application calls it
// with false when it must not lead anything for a while: its clock guard finds the clock unreliable, its configuration
// was refused, it is about to be drained. An ineligible node steps down from every partition it leads (telling the
// others, so that a successor takes over at once), publishes itself as not ready, so that it is nobody's candidate,
// and neither takes over nor claims anything; it still follows the owners, acknowledges their writes, promises, serves
// catch-up and state, and reads its own copy (CanRead). SetEligible(true) makes it a candidate again once it is Ready.
func (m *Manager) SetEligible(eligible bool) {
	if m.eligible.Swap(eligible) == eligible {
		return
	}
	m.log.Info("eligibility changed", "eligible", eligible)
	m.publishReady()
	if !eligible {
		for p := range m.parts {
			m.stepDown(p)
		}
	}
}

// Eligible reports what SetEligible last set (true for a new Manager).
func (m *Manager) Eligible() bool { return m.eligible.Load() }

// publishReady publishes partition.ready: the node is ready and eligible. It does nothing before Start or after Stop.
func (m *Manager) publishReady() {
	m.lifeMu.Lock()
	defer m.lifeMu.Unlock()
	if m.started && !m.stopped {
		m.net.publish(m.cfg.ClusterSize, m.cfg.ClusterID, m.ready.Load() && m.eligible.Load())
	}
}

// MinTransportDeadline is the least time the cluster's transport must let a request wait for its reply (gossip
// Config.TCPDeadline): a little more than WriteTimeout, which bounds how long a node waits for the owner's answer to a
// change or read it forwarded. With a shorter transport deadline a forwarded change that takes longer than it (the owner
// waited for the partition, or for a slow follower) is reported ErrUnknown although the owner may still commit it. An
// application checks it once at start, e.g. refusing to run if gossipConfig.TCPDeadline < m.MinTransportDeadline().
func (m *Manager) MinTransportDeadline() time.Duration {
	return m.cfg.WriteTimeout + m.cfg.HeartbeatInterval
}

// Owner returns the owner of p this node believes in: itself while it leads p, otherwise the node whose heartbeat
// for p it has seen within OwnerTimeout. It reports false if there is none.
func (m *Manager) Owner(p int) (gossip.NodeID, bool) {
	ps := m.part(p)
	if ps == nil {
		return gossip.EmptyNodeID, false
	}
	ps.vmu.RLock()
	defer ps.vmu.RUnlock()
	if ps.owner == gossip.EmptyNodeID {
		return gossip.EmptyNodeID, false
	}
	if ps.leading || m.now().Sub(ps.ownerSeen) < m.cfg.OwnerTimeout {
		return ps.owner, true
	}
	return gossip.EmptyNodeID, false
}

// IsOwner reports whether this node holds p's ownership: it completed the takeover and has not stepped down.
func (m *Manager) IsOwner(p int) bool {
	ps := m.part(p)
	if ps == nil {
		return false
	}
	ps.vmu.RLock()
	defer ps.vmu.RUnlock()
	return ps.leading
}

// CanRead reports whether this node may serve reads of p from its own copy. As p's owner: it has had majority contact
// within StaleAfter, which a new owner has only once the write it settles in its takeover is committed. As a follower: it
// has seen the owner's heartbeat or commit notice within StaleAfter, and it has not trailed a committed position the
// owner reported for StaleAfter or longer (its catch-up keeps failing), and p has not diverged here (see
// PartitionStatus). A node that is cut off stops serving, and so does one in resync mode (its data lost or restored).
func (m *Manager) CanRead(p int) bool {
	ps := m.part(p)
	if ps == nil || m.resync.Load() {
		return false
	}
	ps.vmu.RLock()
	defer ps.vmu.RUnlock()
	return m.canReadLocked(ps, m.now())
}

// canReadLocked is CanRead; the caller holds ps.vmu.
func (m *Manager) canReadLocked(ps *partState, now time.Time) bool {
	if ps.leading {
		return ps.settled && now.Sub(ps.lastMajority) < m.cfg.StaleAfter
	}
	if ps.diverged || (!ps.lagSince.IsZero() && now.Sub(ps.lagSince) >= m.cfg.StaleAfter) {
		return false
	}
	// Freshness, not the owner's tenure: a follower keeps serving what it has when its owner hands over (relinquish
	// clears ownerSeen so that a successor's promise round is not refused), within StaleAfter of the last heartbeat, as
	// it does between heartbeats.
	return !ps.heard.IsZero() && now.Sub(ps.heard) < m.cfg.StaleAfter
}

// HandleOwnershipFunc registers a function called whenever this node's belief about the owner of a partition
// changes: a heartbeat names a new owner or epoch, this node takes over, or the owner it believed in is lost (owner is
// then the zero NodeID, and epoch is the epoch it held). Calls are made one at a time, in order, from a goroutine of
// their own, so the function may block briefly but should not wait for the Manager.
func (m *Manager) HandleOwnershipFunc(fn func(p int, owner gossip.NodeID, epoch Epoch)) {
	m.handlersMu.Lock()
	m.handlers = append(m.handlers, fn)
	m.handlersMu.Unlock()
}

// fire queues an ownership event for delivery.
func (m *Manager) fire(p int, owner gossip.NodeID, epoch Epoch) {
	m.evMu.Lock()
	m.evQueue = append(m.evQueue, ownershipEvent{p, owner, epoch})
	m.evMu.Unlock()
	select {
	case m.evWake <- struct{}{}:
	default:
	}
}

func (m *Manager) eventLoop() {
	for {
		m.evMu.Lock()
		q := m.evQueue
		m.evQueue = nil
		m.evMu.Unlock()
		if len(q) > 0 {
			m.handlersMu.RLock()
			hs := append([]func(int, gossip.NodeID, Epoch){}, m.handlers...)
			m.handlersMu.RUnlock()
			for _, ev := range q {
				for _, h := range hs {
					h(ev.p, ev.owner, ev.epoch)
				}
			}
			continue
		}
		select {
		case <-m.evWake:
		case <-m.ctx.Done():
			return
		}
	}
}

// setOwner records a heartbeat from owner; it fires an event if the believed owner or epoch changed.
func (m *Manager) setOwner(p int, owner gossip.NodeID, epoch Epoch) {
	ps := m.parts[p]
	ps.vmu.Lock()
	changed := m.setOwnerLocked(ps, owner, epoch)
	ps.vmu.Unlock()
	if changed {
		m.fire(p, owner, epoch)
	}
}

// setOwnerLocked records the owner and reports whether it or its epoch changed; the caller holds ps.vmu.
func (m *Manager) setOwnerLocked(ps *partState, owner gossip.NodeID, epoch Epoch) bool {
	changed := ps.owner != owner || ps.ownerEpoch != epoch
	ps.owner, ps.ownerEpoch, ps.ownerSeen = owner, epoch, m.now()
	ps.heard = ps.ownerSeen
	ps.ownerlessSince = time.Time{}
	return changed
}

// stepDown makes this node stop leading p and reports whether it was leading. If it was, the owner it believed in
// (itself) is forgotten and every alive node is told (Relinquish), whatever the reason (a failed replication, lost
// majority contact, a fence, a store failure, a higher epoch, a claim, ineligibility, Stop): a follower that still
// believed in this node stops being sticky to it at once, so that a successor's promise round is not refused for
// OwnerTimeout.
func (m *Manager) stepDown(p int) bool {
	ps := m.parts[p]
	ps.vmu.Lock()
	epoch, was := m.leaveLocked(ps)
	ps.vmu.Unlock()
	m.leftLeadership(p, epoch, was)
	return was
}

// stepDownFrom steps down from p if this node still leads it at epoch, and reports whether it did.
func (m *Manager) stepDownFrom(p int, epoch Epoch) bool {
	ps := m.parts[p]
	ps.vmu.Lock()
	var was bool
	if ps.leading && ps.myEpoch == epoch {
		_, was = m.leaveLocked(ps)
	}
	ps.vmu.Unlock()
	m.leftLeadership(p, epoch, was)
	return was
}

// leaveLocked ends this node's tenure of the partition, if it leads it, and returns the tenure's epoch; the caller holds
// ps.vmu and then calls leftLeadership.
func (m *Manager) leaveLocked(ps *partState) (Epoch, bool) {
	if !ps.leading {
		return Epoch{}, false
	}
	ps.leading = false
	if ps.owner == m.self {
		ps.owner = gossip.EmptyNodeID
		ps.ownerSeen = time.Time{}
	}
	return ps.myEpoch, true
}

// leftLeadership is what follows leaveLocked once ps.vmu is released: the log line, the ownership event and the
// relinquish message.
func (m *Manager) leftLeadership(p int, epoch Epoch, was bool) {
	if !was {
		return
	}
	m.log.Info("stepped down", "partition", p, "epoch", epoch.String())
	m.fire(p, gossip.EmptyNodeID, epoch)
	m.sendRelinquish(p, epoch)
}

// sendRelinquish tells every alive node that this node stepped down from p's tenure at epoch, so their stickiness
// lapses at once.
func (m *Manager) sendRelinquish(p int, epoch Epoch) {
	msg := &relinquishMsg{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID, P: p, Epoch: epoch}
	for _, pi := range m.net.peers() {
		if err := m.net.send(pi.ID, partRelinquishMsg, msg); err != nil {
			m.log.Debug("relinquish send failed", "partition", p, "error", err)
		}
	}
}

// becomeLeader records a completed promise round at epoch e, begun at start. The tenure counts no majority contact
// (lastMajority, and with it CanRead) until the takeover has committed the write it settles, or found none to settle
// (takeoverSettled); the takeover holds the partition's mutex until then, so no change or read runs before it either.
func (m *Manager) becomeLeader(p int, e Epoch, start time.Time) {
	ps := m.parts[p]
	ps.vmu.Lock()
	ps.leading = true
	ps.myEpoch = e
	ps.owner, ps.ownerEpoch, ps.ownerSeen = m.self, e, start
	ps.settled, ps.leaderSince, ps.lastMajority = false, start, time.Time{}
	ps.ownerlessSince = time.Time{}
	ps.lagTarget, ps.lagSince, ps.aheadSince = 0, time.Time{}, time.Time{}
	ps.vmu.Unlock()
	m.fire(p, m.self, e)
}

// touchMajority notes majority contact at t for p's tenure at epoch e (it only moves forward). It does nothing for a
// tenure whose takeover has not settled yet (see settle): a heartbeat round that reaches a majority meanwhile says nothing
// about the write the takeover is settling.
func (m *Manager) touchMajority(p int, e Epoch, t time.Time) {
	ps := m.parts[p]
	ps.vmu.Lock()
	if ps.leading && ps.settled && ps.myEpoch == e && t.After(ps.lastMajority) {
		ps.lastMajority = t
		ps.ownerSeen = t
	}
	ps.vmu.Unlock()
}

// takeoverSettled records that the takeover of p at epoch e has committed the write it settled (or found none to
// settle), with majority contact at t: from now on the tenure counts majority contact (lastMajority), so it may serve
// local reads (CanRead) and accept changes.
func (m *Manager) takeoverSettled(p int, e Epoch, t time.Time) {
	ps := m.parts[p]
	ps.vmu.Lock()
	if ps.leading && ps.myEpoch == e {
		ps.settled = true
	}
	ps.vmu.Unlock()
	m.touchMajority(p, e, t)
}

// leadingEpoch returns p's epoch and whether this node leads it.
func (m *Manager) leadingEpoch(p int) (Epoch, bool) {
	ps := m.parts[p]
	ps.vmu.RLock()
	defer ps.vmu.RUnlock()
	return ps.myEpoch, ps.leading
}

// recentMajority reports whether this node, leading p, has had majority contact within OwnerTimeout.
func (m *Manager) recentMajority(ps *partState) bool {
	ps.vmu.RLock()
	defer ps.vmu.RUnlock()
	last := ps.lastMajority
	if !ps.settled {
		last = ps.leaderSince // a takeover that is settling: its promise round was the majority contact
	}
	return ps.leading && m.now().Sub(last) < m.cfg.OwnerTimeout
}

// observe raises p's maxCounter to the counters of the epochs a message carried.
func (m *Manager) observe(p int, epochs ...Epoch) {
	ps := m.part(p)
	if ps == nil {
		return
	}
	for _, e := range epochs {
		ps.raise(e.Counter)
	}
}

// sleep waits d, or until ctx ends or the Manager stops; it reports whether the full wait elapsed.
func (m *Manager) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-m.ctx.Done():
		return false
	}
}

// String names the Manager by its node, for logs.
func (m *Manager) String() string { return fmt.Sprintf("partition.Manager(%s)", m.self) }
