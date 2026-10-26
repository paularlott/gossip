package partition

import (
	"context"
	"time"

	"github.com/paularlott/gossip"
)

// The handlers hold the decision logic of the protocol's messages. They take decoded requests and return the reply, so
// they can be exercised without a network; clusterNet.register wraps them for the cluster. A request handler runs in a
// goroutine of the Manager (the reply is deferred), never on one of the cluster's incoming workers; one that changes a
// partition's durable state does so under that partition's mutex, and refuses rather than wait for it longer than
// OwnerTimeout. The one-way handlers (commit notice, relinquish) run on an incoming worker and never wait for a mutex.

// handlePromise answers a PartPromiseMsg from the node proposing req.Epoch.
func (m *Manager) handlePromise(from gossip.NodeID, req *promiseReq) *promiseResp {
	if !m.compatible(req.Size, req.Cluster) {
		return &promiseResp{SizeMismatch: true}
	}
	ps := m.part(req.P)
	if ps == nil || m.isStopped() {
		return &promiseResp{}
	}
	m.observe(req.P, req.Epoch)
	if !m.tryLock(ps) {
		return &promiseResp{}
	}
	defer ps.mu.unlock()

	st, err := m.store.State(req.P)
	if err != nil {
		m.log.Error("promise: reading state failed", "partition", req.P, "error", err)
		return &promiseResp{}
	}
	m.observe(req.P, st.Promised)
	if m.resync.Load() || m.diverged(req.P) {
		// A node whose data is not intact never promises, nor one whose committed data of the partition diverged from
		// its owner's: its answer would report writes that were never committed, which the next owner would adopt.
		return &promiseResp{Promised: st.Promised}
	}

	ps.vmu.RLock()
	owner, seen := ps.owner, ps.ownerSeen
	ps.vmu.RUnlock()
	// Stickiness: a node that has heard from p's live owner does not promise to a challenger, so that a slow heartbeat
	// does not move a partition.
	sticky := owner != gossip.EmptyNodeID && owner != req.Epoch.Node && m.now().Sub(seen) < m.cfg.OwnerTimeout

	if req.Epoch.Compare(st.Promised) > 0 && !sticky {
		// Durable before the reply: the promise must survive a crash that follows the answer.
		if err := m.setPromised(req.P, req.Epoch); err != nil {
			m.log.Error("promise: recording promise failed", "partition", req.P, "error", err)
			return &promiseResp{Promised: st.Promised}
		}
		if e, leading := m.leadingEpoch(req.P); leading && e.Less(req.Epoch) {
			m.stepDown(req.P)
		}
		return &promiseResp{OK: true, Promised: req.Epoch, Committed: st.Committed, Pending: st.Pending}
	}
	return &promiseResp{Promised: st.Promised}
}

// handleChange answers a PartChangeMsg: it runs the change if this node holds the partition and otherwise says what it
// believes.
func (m *Manager) handleChange(ctx context.Context, req *changeReq) *changeResp {
	return m.runIfOwner(ctx, req.Size, req.Cluster, req.P, req.Deadline, m.changeSem, true, func(ctx context.Context) *changeResp {
		return m.runChange(ctx, req.P, req.Change, true)
	})
}

// handleRead answers a PartReadMsg like handleChange does a change.
func (m *Manager) handleRead(ctx context.Context, req *readReq) *changeResp {
	return m.runIfOwner(ctx, req.Size, req.Cluster, req.P, req.Deadline, m.readSem, false, func(ctx context.Context) *changeResp {
		return m.runRead(ctx, req.P, req.Req, true)
	})
}

// runIfOwner is what a forwarded change or read has in common: run does the work if this node holds p and has a free
// slot in sem (and, for a change, among the partition's MaxChangesPerPartition), with a context that ends at the
// requester's deadline (Unix nanoseconds; 0: none) and at most WriteTimeout from now. A node that does not hold p answers
// with what it believes, and an owner at a limit with busy (nothing was run, and the sender retries); a request is never
// forwarded again.
func (m *Manager) runIfOwner(ctx context.Context, size int, cluster string, p int, deadline int64, sem chan struct{},
	perPartition bool, run func(context.Context) *changeResp) *changeResp {
	if !m.compatible(size, cluster) {
		return &changeResp{SizeMismatch: true}
	}
	ps := m.part(p)
	if ps == nil || m.isStopped() {
		return &changeResp{Status: statusUnavailable}
	}
	if !m.IsOwner(p) {
		return m.notOwner(p)
	}
	if perPartition {
		if ps.changes.Add(1) > int32(m.cfg.MaxChangesPerPartition) {
			ps.changes.Add(-1)
			return &changeResp{Status: statusBusy}
		}
		defer ps.changes.Add(-1)
	}
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	default:
		return &changeResp{Status: statusBusy}
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.WriteTimeout)
	defer cancel()
	if deadline != 0 {
		var cancelDL context.CancelFunc
		ctx, cancelDL = context.WithDeadline(ctx, time.Unix(0, deadline))
		defer cancelDL()
	}
	return run(ctx)
}

// deadlineFloor is the least time a forwarded change or read must have left before its requester's deadline when the
// owner, holding the partition's mutex, is about to start it: 2 x HeartbeatInterval (at most half of WriteTimeout). A
// write begun later than that could not report back in time, and would leave its requester with ErrUnknown.
func (m *Manager) deadlineFloor() time.Duration {
	return min(2*m.cfg.HeartbeatInterval, m.cfg.WriteTimeout/2)
}

// tooLate reports whether a forwarded operation, whose context ends at its requester's deadline, has too little time
// left to start (see deadlineFloor). An operation of this node's own caller is never too late: the caller waits for it.
func (m *Manager) tooLate(ctx context.Context, forwarded bool) bool {
	if !forwarded {
		return false
	}
	dl, ok := ctx.Deadline()
	return ok && time.Until(dl) < m.deadlineFloor()
}

// notOwner is the reply of a node that does not hold p: what it believes the owner is.
func (m *Manager) notOwner(p int) *changeResp {
	resp := &changeResp{Status: statusNotOwner}
	if owner, ok := m.Owner(p); ok && owner != m.self {
		ps := m.parts[p]
		ps.vmu.RLock()
		resp.Owner, resp.Epoch = owner, ps.ownerEpoch
		ps.vmu.RUnlock()
	}
	return resp
}

// handleClaim answers a PartClaimMsg: a node that believes it is p's rank 0 asks this node, which leads p, to hand it
// over. If the claimant is rank 0 among this node's own candidates, this node steps down from p once any change in
// progress has finished (it takes the partition's mutex), tells the others and replies OK; otherwise it replies that
// it did not.
func (m *Manager) handleClaim(from gossip.NodeID, req *claimReq) *claimResp {
	if !m.compatible(req.Size, req.Cluster) {
		return &claimResp{SizeMismatch: true}
	}
	ps := m.part(req.P)
	if ps == nil || m.isStopped() || from == m.self || !m.IsOwner(req.P) {
		return &claimResp{}
	}
	if rank, ok := rendezvousRank(req.P, from, m.candidates()); !ok || rank != 0 {
		return &claimResp{}
	}
	if !m.tryLock(ps) {
		return &claimResp{}
	}
	defer ps.mu.unlock()
	epoch, _ := m.leadingEpoch(req.P)
	if !m.stepDown(req.P) { // and relinquishes
		return &claimResp{}
	}
	m.log.Info("handed over on a claim", "partition", req.P, "to", from.String())
	return &claimResp{OK: true, Epoch: epoch}
}

// handleState answers a PartStateMsg with every partition's committed position, promise and pending write. It reads
// without the partitions' mutexes: both values only move forward, so a value that is a moment stale is a lower bound.
// If the Store fails the reply carries no per-partition data, which the asker discards as no answer.
func (m *Manager) handleState(req *stateReq) *stateResp {
	if !m.compatible(req.Size, req.Cluster) {
		return &stateResp{SizeMismatch: true}
	}
	resp := &stateResp{
		Intact:    !m.resync.Load(),
		Forming:   !m.resync.Load() && !m.established.Load(),
		Committed: make([]Pos, len(m.parts)),
		Promised:  make([]Epoch, len(m.parts)),
	}
	for p := range m.parts {
		st, err := m.store.State(p)
		if err != nil {
			m.log.Error("state request: reading state failed", "partition", p, "error", err)
			return &stateResp{}
		}
		resp.Promised[p] = st.Promised
		if m.diverged(p) {
			continue // its committed data and pending write are not to be taken by anyone (see PartitionStatus.Diverged)
		}
		resp.Committed[p] = st.Committed
		if st.Pending != nil {
			if resp.Pending == nil {
				resp.Pending = make([]*Write, len(m.parts))
			}
			resp.Pending[p] = st.Pending
		}
		m.observe(p, st.Promised, st.Committed.Epoch)
	}
	return resp
}

// handleRelinquish processes a PartRelinquishMsg: if the sender is the owner this node believes in, its stickiness
// lapses at once (ownerSeen is cleared), so a successor's promise round is not refused.
func (m *Manager) handleRelinquish(from gossip.NodeID, msg *relinquishMsg) {
	ps := m.part(msg.P)
	if !m.compatible(msg.Size, msg.Cluster) || ps == nil || msg.Epoch.Node != from {
		return
	}
	m.observe(msg.P, msg.Epoch)
	ps.vmu.Lock()
	// A heartbeat that was in flight when the owner stepped down may still arrive: remember the epoch is over.
	if ps.relinquished.Less(msg.Epoch) {
		ps.relinquished = msg.Epoch
	}
	if ps.owner == from && ps.ownerEpoch.Compare(msg.Epoch) <= 0 {
		ps.ownerSeen = time.Time{}
	}
	ps.vmu.Unlock()
}
