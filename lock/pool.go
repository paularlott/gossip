package lock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paularlott/gossip"
)

// Leadership is the slice of leader election that a Pool depends on.
//
// *leader.LeaderElection satisfies this. Keeping it an interface means a pool can
// be driven by a cluster-wide election or one scoped to a NodeGroup without the
// pool caring which, and lets tests substitute a controllable fake.
type Leadership interface {
	// HasLeader reports whether a leader is currently known and valid.
	HasLeader() bool

	// IsLeader reports whether the local node is that leader.
	IsLeader() bool

	// GetLeaderID returns the current leader's node ID.
	GetLeaderID() gossip.NodeID

	// Term returns the current election term. It must increase on every
	// leadership change.
	Term() uint64

	// WatchLeadership registers fn to be called whenever leadership state
	// changes, and immediately invokes it once with the current state so
	// callers need no separate initial read. The callback must not block; it
	// runs on the goroutine that observed the change. The pool reacts to
	// every change through this — it does not poll. The returned cancel
	// unregisters fn.
	WatchLeadership(fn func(isLeader bool, term uint64)) (cancel func())

	// Candidates returns the nodes over which leadership is contested — the
	// whole cluster's alive nodes for a cluster-wide election, or the group's
	// members for a MetadataCriteria-scoped one. The pool draws its replica
	// targets and recovery queries from this set, which is what keeps
	// replication inside the group for a group-scoped election.
	Candidates() []*gossip.Node
}

// Pool provides distributed advisory locks with a single serialising authority
// — whoever currently holds leadership — and W-durable replication of every
// mutation to the wider group.
//
// All grants, releases, and extends are ordered by the leader and pushed to
// WriteReplicas-1 peers before being acknowledged, so an acked mutation
// survives the loss of any W-1 nodes at that instant; a fire-and-forget gossip
// layer then spreads entries across the whole group. A newly elected leader
// recovers by merging replica state from every live peer (deadline-bounded)
// instead of waiting out a warm-up period, which is what makes failover fast
// even when MaxTTL is minutes.
//
// Scope follows the election that is supplied. Give it a cluster-wide election
// and locks are cluster-wide; give it one scoped to a NodeGroup (via the
// election's MetadataCriteria) and that group coordinates instead. Any node
// may take locks either way, including nodes outside the group — though every
// node expected to hold replicas should create the pool.
type Pool struct {
	cluster    *gossip.Cluster
	leadership Leadership
	config     *Config
	tbl        *table
	registry   *registry

	mu     sync.Mutex
	closed bool

	// leadership tracking
	syncMu       sync.Mutex // serialises syncLeadership across event callbacks
	stateMu      sync.Mutex
	wasLeader    bool
	lastTerm     uint64
	recoverWG    sync.WaitGroup
	unwatch      func() // cancels the leadership subscription
	stopCh       chan struct{}
	doneCh       chan struct{}
	stateHandler gossip.HandlerID
	gossipTick   gossip.HandlerID

	// sweeping single-flights the anti-entropy work so a slow sweep skips
	// gossip ticks instead of piling up; synced is only touched inside a
	// sweep, ordered by the sweeping atomic.
	sweeping atomic.Bool
	synced   bool
}

// NewPool creates a lock pool driven by the supplied leadership.
//
// The election may be cluster-wide or NodeGroup-scoped; the pool simply asks it
// who the leader is. Pass the same Config.Name on every participating node.
func NewPool(cluster *gossip.Cluster, leadership Leadership, config *Config) *Pool {
	if cluster == nil {
		panic("lock: cluster must not be nil")
	}
	if leadership == nil {
		panic("lock: leadership must not be nil")
	}

	config = config.validate()

	p := &Pool{
		cluster:    cluster,
		leadership: leadership,
		config:     config,
		tbl:        newTable(config.MaxTTL),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}

	p.registry = getOrCreateRegistry(cluster)
	p.registry.registerPool(config.Name, p)

	// Free a dead holder's locks promptly rather than waiting out their TTL.
	p.stateHandler = cluster.HandleNodeStateChangeFunc(p.handleNodeStateChange)

	// Subscribe to leadership changes. The immediate callback establishes
	// initial state; every later change arrives as an event. No polling.
	// The subscription is cancelled in Close so a stopped pool is not
	// retained by the election.
	p.unwatch = p.leadership.WatchLeadership(func(isLeader bool, term uint64) {
		p.syncLeadership()
	})

	// Anti-entropy rides the cluster's gossip event — the same self-adjusting
	// cadence the cluster uses for its own state exchange — so the pool keeps
	// no timer of its own.
	p.gossipTick = cluster.HandleGossipFunc(p.onGossipTick)

	// Lifecycle goroutine: owns doneCh so Close can wait for shutdown.
	go func() {
		<-p.stopCh
		close(p.doneCh)
	}()

	return p
}

// Close shuts the pool down. No handover is performed: replicas across the
// group already hold every acked mutation, and the next leader recovers by
// merging them.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	close(p.stopCh)
	<-p.doneCh
	p.recoverWG.Wait()

	if p.unwatch != nil {
		p.unwatch()
	}

	p.cluster.RemoveGossipHandler(p.gossipTick)
	p.cluster.RemoveNodeStateChangeHandler(p.stateHandler)
	p.tbl.close()
	p.registry.unregisterPool(p.config.Name)
}

// Name returns the pool's name.
func (p *Pool) Name() string { return p.config.Name }

// IsLeader reports whether this node currently serves the pool's lock table.
func (p *Pool) IsLeader() bool { return p.leadership.IsLeader() }

// ReplicaCount returns the number of live locks this node currently knows
// about, whether or not it is the leader. Diagnostic.
func (p *Pool) ReplicaCount() int { return p.tbl.count() }

// WriteReplicas returns the configured W for this pool.
func (p *Pool) WriteReplicas() int { return p.config.WriteReplicas }

// onGossipTick runs on the cluster's gossip event: until the pool has synced
// once it catches the local replica store up with its peers (a late joiner
// otherwise holds nothing until the next leadership change), afterwards it
// sweeps entries to its peers, healing lost fire-and-forget gossip.
//
// The work is dispatched rather than done inline: gossip handlers run
// synchronously on the cluster's gossip goroutine and their duration feeds the
// cluster's interval adjustment, so network waits must not block the tick.
// Single-flighted — a slow sweep skips ticks instead of piling up.
func (p *Pool) onGossipTick() {
	// Reap inline: a short map scan, safe for the synchronous tick.
	p.tbl.reap()

	if !p.sweeping.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer p.sweeping.Store(false)

		if !p.synced {
			p.synced = p.catchUp()
			return
		}
		p.regossip()
	}()
}

// --- leadership transitions ---

// syncLeadership reacts to becoming or ceasing to be the leader. Safe to call
// from any goroutine — watcher callbacks and the constructor — as callers are
// serialised here.
func (p *Pool) syncLeadership() {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()

	isLeader := p.leadership.IsLeader()
	term := p.leadership.Term()

	p.stateMu.Lock()
	was := p.wasLeader
	prevTerm := p.lastTerm

	switch {
	case isLeader && (!was || term != prevTerm):
		// Taking office, either freshly or in a new term. Entries from a
		// previous tenure are kept (they are this node's replica view) but
		// service stays closed until recovery has merged every live peer's
		// view: between terms another leader may have granted.
		p.wasLeader = true
		p.lastTerm = term
		p.stateMu.Unlock()

		p.tbl.assumeLeadership(term)

		// Register with the wait group under the close flag so a callback
		// racing Close cannot Add after Close's Wait begins.
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		p.recoverWG.Add(1)
		p.mu.Unlock()

		go p.recoverState(term)

	case !isLeader && was:
		// Standing down. Stop serving; keep the entries as the replica view.
		p.wasLeader = false
		p.lastTerm = term
		p.stateMu.Unlock()

		p.tbl.relinquishLeadership()

	default:
		p.lastTerm = term
		p.stateMu.Unlock()
	}
}

// handleNodeStateChange frees locks belonging to a node that has died.
func (p *Pool) handleNodeStateChange(node *gossip.Node, prevState gossip.NodeState) {
	if node.GetObservedState() != gossip.NodeDead {
		return
	}
	if !p.leadership.IsLeader() || !p.tbl.servingNow() {
		return
	}
	if tombs := p.tbl.releaseByOwner(node.ID); len(tombs) > 0 {
		if err := p.replicateBatch(tombs); err != nil {
			// The locks stay released locally and on any replica that did
			// apply the tombstones; TTL expiry cleans up the rest.
			p.cluster.Logger().WithError(err).Warn("lock: replicating holder-death releases fell short",
				"node_id", node.ID.String())
		}
	}
}

// --- public API ---

// TryAcquire makes one attempt to take the lock.
//
// ttl is required and must fall within [MinTTL, MaxTTL]. Returns
// ErrLockNotAcquired when the key is held elsewhere, ErrNoLeader when no leader
// is currently available, ErrWarmingUp while a new leader is recovering state,
// or an error wrapping ErrWriteQuorum when the grant could not be made durable
// on W nodes (in which case the grant has not taken effect).
func (p *Pool) TryAcquire(key string, ttl time.Duration) (*Lock, error) {
	if err := p.checkClosed(); err != nil {
		return nil, err
	}
	if err := p.validateTTL(ttl); err != nil {
		return nil, err
	}
	return p.acquireOnce(key, ttl)
}

// Acquire blocks until the lock is taken or ctx is done.
//
// Transient conditions — the key being held, no leader yet, recovery in
// progress, or replication falling short — are retried until ctx expires. Only
// genuinely fatal errors return early.
func (p *Pool) Acquire(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	if err := p.checkClosed(); err != nil {
		return nil, err
	}
	if err := p.validateTTL(ttl); err != nil {
		return nil, err
	}

	for {
		lk, err := p.acquireOnce(key, ttl)
		if err == nil {
			return lk, nil
		}
		if !isRetryable(err) {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(p.config.RetryInterval):
		}
	}
}

// Query reports whether a key is held, by whom, under which token, and for how
// much longer.
//
// The answer comes from the leader's table, which is only authoritative once it
// has completed recovery. Before that the truth is unknown — locks recovered
// from replicas may still be arriving — so Query returns ErrWarmingUp or
// ErrNoLeader rather than a "not held" that callers could act on. Both are
// transient; retry.
func (p *Pool) Query(key string) (held bool, owner gossip.NodeID, token Token, remaining time.Duration, err error) {
	if err = p.checkClosed(); err != nil {
		return
	}

	if p.leadership.IsLeader() {
		h, o, t, ms, ready := p.tbl.query(key)
		if !ready {
			// Leadership observed but recovery has not completed; the truth
			// about held locks is not yet known.
			return false, gossip.EmptyNodeID, Token{}, 0,
				fmt.Errorf("%w (leader recovering state)", ErrWarmingUp)
		}
		return h, o, t, time.Duration(ms) * time.Millisecond, nil
	}

	leaderNode, err := p.leaderNode()
	if err != nil {
		return
	}

	req := &queryRequest{PoolName: p.config.Name, Key: key}
	var resp queryResponse
	if err = p.cluster.SendToWithResponse(leaderNode, lockQueryMsg, req, &resp); err != nil {
		err = fmt.Errorf("lock: query failed: %w", err)
		return
	}
	if resp.Reason == reasonNotLeader {
		err = ErrNoLeader
		return
	}
	if resp.Reason == reasonRecovering {
		err = fmt.Errorf("%w (leader recovering state)", ErrWarmingUp)
		return
	}

	return resp.Held, resp.Owner, resp.Token, time.Duration(resp.TTLMs) * time.Millisecond, nil
}

// --- internals ---

func (p *Pool) acquireOnce(key string, ttl time.Duration) (*Lock, error) {
	localID := p.cluster.LocalNode().ID

	if p.leadership.IsLeader() {
		// The table refuses while it has not completed recovery, returning
		// reasonRecovering, so no separate check is needed here.
		ent, granted, reason := p.tbl.tryAcquire(key, localID, ttl)
		if !granted {
			return nil, classifyDenial(reason)
		}
		if err := p.replicateEntry(ent); err != nil {
			// The grant was not durable: compensate with a tombstone so the
			// half-written grant cannot resurrect at a later recovery.
			p.compensate(ent)
			return nil, err
		}
		return &Lock{pool: p, key: key, token: ent.Token}, nil
	}

	leaderNode, err := p.leaderNode()
	if err != nil {
		return nil, err
	}

	req := &acquireRequest{
		PoolName:    p.config.Name,
		Key:         key,
		TTLMs:       ttl.Milliseconds(),
		RequesterID: localID,
	}
	var resp acquireResponse
	if err := p.cluster.SendToWithResponse(leaderNode, lockAcquireMsg, req, &resp); err != nil {
		// The leader may have just changed; treat as retryable.
		return nil, fmt.Errorf("%w: %v", ErrNoLeader, err)
	}
	if !resp.Granted {
		return nil, classifyDenial(resp.Reason)
	}

	return &Lock{pool: p, key: key, token: resp.Token}, nil
}

// compensate undoes a locally-applied grant whose replication fell short. The
// tombstone is applied locally and gossiped so peers that did receive the
// pushed grant do not resurrect it at recovery.
func (p *Pool) compensate(grant replicaEntry) {
	if tomb, ok, _ := p.tbl.release(grant.Key, grant.Token); ok {
		go p.gossipEntries([]replicaEntry{tomb})
	}
}

func (p *Pool) release(key string, token Token) error {
	if err := p.checkClosed(); err != nil {
		return err
	}

	if p.leadership.IsLeader() {
		tomb, released, reason := p.tbl.release(key, token)
		if !released {
			return fmt.Errorf("lock: release rejected: %s", reason)
		}
		if tomb.Token.IsZero() {
			return nil // nothing to replicate (absent or already expired)
		}
		if err := p.replicateEntry(tomb); err != nil {
			// The tombstone exists locally and wherever it did land; TTL
			// expiry covers the remainder. Report so the caller knows.
			return err
		}
		return nil
	}

	leaderNode, err := p.leaderNode()
	if err != nil {
		return err
	}

	req := &releaseRequest{PoolName: p.config.Name, Key: key, Token: token}
	var resp releaseResponse
	if err := p.cluster.SendToWithResponse(leaderNode, lockReleaseMsg, req, &resp); err != nil {
		return fmt.Errorf("lock: release failed: %w", err)
	}
	if !resp.Released {
		if resp.Reason == reasonNotLeader {
			return ErrNoLeader
		}
		if resp.Reason == reasonRecovering {
			return fmt.Errorf("%w (leader recovering state)", ErrWarmingUp)
		}
		return fmt.Errorf("lock: release rejected: %s", resp.Reason)
	}
	return nil
}

func (p *Pool) extend(key string, token Token, ttl time.Duration) error {
	if err := p.checkClosed(); err != nil {
		return err
	}
	if err := p.validateTTL(ttl); err != nil {
		return err
	}

	if p.leadership.IsLeader() {
		ent, extended, reason := p.tbl.extend(key, token, ttl)
		if !extended {
			return fmt.Errorf("lock: extend rejected: %s", reason)
		}
		if err := p.replicateEntry(ent); err != nil {
			return err
		}
		return nil
	}

	leaderNode, err := p.leaderNode()
	if err != nil {
		return err
	}

	req := &extendRequest{PoolName: p.config.Name, Key: key, Token: token, TTLMs: ttl.Milliseconds()}
	var resp extendResponse
	if err := p.cluster.SendToWithResponse(leaderNode, lockExtendMsg, req, &resp); err != nil {
		return fmt.Errorf("lock: extend failed: %w", err)
	}
	if !resp.Extended {
		if resp.Reason == reasonNotLeader {
			return ErrNoLeader
		}
		if resp.Reason == reasonRecovering {
			return fmt.Errorf("%w (leader recovering state)", ErrWarmingUp)
		}
		return fmt.Errorf("lock: extend rejected: %s", resp.Reason)
	}
	return nil
}

// leaderNode resolves the current leader to a node we can send to.
func (p *Pool) leaderNode() (*gossip.Node, error) {
	if !p.leadership.HasLeader() {
		return nil, ErrNoLeader
	}
	id := p.leadership.GetLeaderID()
	if id == gossip.EmptyNodeID {
		return nil, ErrNoLeader
	}
	node := p.cluster.GetNode(id)
	if node == nil {
		return nil, ErrNoLeader
	}
	return node, nil
}

func (p *Pool) validateTTL(ttl time.Duration) error {
	if ttl < p.config.MinTTL || ttl > p.config.MaxTTL {
		return fmt.Errorf("%w: must be between %v and %v, got %v",
			ErrTTLOutOfRange, p.config.MinTTL, p.config.MaxTTL, ttl)
	}
	return nil
}

func (p *Pool) checkClosed() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	return nil
}

// classifyDenial maps a leader's refusal reason onto a typed error.
func classifyDenial(reason string) error {
	switch {
	case reason == reasonNotLeader:
		return ErrNoLeader
	case reason == reasonRecovering:
		return fmt.Errorf("%w (leader recovering state)", ErrWarmingUp)
	case reason == reasonWriteQuorum:
		return ErrWriteQuorum
	default:
		return ErrLockNotAcquired
	}
}

// isRetryable reports whether a blocking Acquire should keep waiting.
func isRetryable(err error) bool {
	return errors.Is(err, ErrLockNotAcquired) ||
		errors.Is(err, ErrNoLeader) ||
		errors.Is(err, ErrWarmingUp) ||
		errors.Is(err, ErrWriteQuorum)
}
