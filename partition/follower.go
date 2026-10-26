package partition

import (
	"github.com/paularlott/gossip"
)

// handleWrite answers a PartWriteMsg from the owner `from`: it stores the write as this node's pending write if it
// follows what this node has committed, and acknowledges it. That acknowledgement is a promise that the write is stored
// durably, so a node refuses anything it is not sure of.
func (m *Manager) handleWrite(from gossip.NodeID, req *writeReq) *writeResp {
	if !m.compatible(req.Size, req.Cluster) {
		return &writeResp{SizeMismatch: true}
	}
	w := req.Write
	p := w.Partition
	ps := m.part(p)
	if ps == nil || m.isStopped() {
		return &writeResp{}
	}
	m.observe(p, w.Pos.Epoch, w.Prev.Epoch, req.OwnerCommitted.Epoch)
	if m.resync.Load() || m.diverged(p) {
		// Not intact, or this node's committed data of p diverged from the owner's: it would acknowledge by Seq a write
		// it does not hold.
		return &writeResp{}
	}
	if !m.tryLock(ps) {
		return &writeResp{}
	}
	defer ps.mu.unlock()

	st, err := m.store.State(p)
	if err != nil {
		m.log.Error("write: reading state failed", "partition", p, "error", err)
		return &writeResp{}
	}
	if w.Pos.Epoch.Less(st.Promised) {
		return &writeResp{Promised: st.Promised}
	}
	// A write continues the write it names: one position after it. Anything else is malformed and refused.
	if seq, err := increment(w.Prev.Seq); err != nil || w.Pos.Seq != seq {
		m.log.Error("write: malformed write refused", "partition", p, "pos", w.Pos.String(), "prev", w.Prev.String())
		return &writeResp{Promised: st.Promised}
	}
	if st.Promised.Less(w.Pos.Epoch) {
		if err := m.setPromised(p, w.Pos.Epoch); err != nil {
			m.log.Error("write: recording promise failed", "partition", p, "error", err)
			return &writeResp{Promised: st.Promised}
		}
		if e, leading := m.leadingEpoch(p); leading && e.Less(w.Pos.Epoch) {
			m.stepDown(p) // a higher epoch is writing: this node was deposed
		}
	}

	m.settle(p, req.OwnerCommitted)
	st, err = m.store.State(p)
	if err != nil {
		m.log.Error("write: reading state failed", "partition", p, "error", err)
		return &writeResp{}
	}
	// What this node has committed is compared with the write by sequence number, not by Pos. An owner that committed
	// write s and lost the ownership before the others heard of it leaves them with s only pending; the next owner's
	// promise round finds that pending write and re-proposes the same data as s under its own epoch. The same write is
	// then committed at s under two positions, and a Seq is the write's identity (takeover's H does the same):
	//   - committed Seq below w.Prev.Seq: this node is behind and needs catch-up;
	//   - committed Seq == w.Pos.Seq: this node already holds this write, committed under another position: it is
	//     acknowledged (stored, which is all an ack says) and not stored again;
	//   - committed Seq above w.Pos.Seq: a late copy of the write (requests are handled concurrently; this node has
	//     moved on since): refused, which changes nothing;
	//   - committed Seq == w.Prev.Seq: the write follows, and its Prev becomes this node's own label of that Seq, which
	//     is what Store.Commit checks.
	switch {
	case st.Committed.Seq < w.Prev.Seq:
		m.catchUpAsync(p, from)
		return &writeResp{Promised: st.Promised, Committed: st.Committed, NeedCatchUp: true}
	case st.Committed.Seq == w.Pos.Seq:
		return &writeResp{OK: true, Promised: st.Promised, Committed: st.Committed}
	case st.Committed.Seq > w.Pos.Seq:
		// A late copy of a write: requests are handled concurrently, so this node may have committed it, and later
		// ones, before this copy's handler ran (from the owner's next writes or commit notices). The owner counted
		// other acknowledgements; refusing changes nothing.
		m.log.Debug("write: already committed beyond this write", "partition", p,
			"committed", st.Committed.String(), "write", w.Pos.String())
		return &writeResp{Promised: st.Promised, Committed: st.Committed}
	}
	w.Prev = st.Committed
	if err := m.store.SetPending(p, &w); err != nil {
		m.log.Error("write: storing the pending write failed", "partition", p, "error", err)
		return &writeResp{Promised: st.Promised, Committed: st.Committed}
	}
	return &writeResp{OK: true, Promised: st.Promised, Committed: st.Committed}
}

// settle decides this node's pending write of p against oc, an owner's committed position. The caller holds the
// partition's mutex.
//
//	no pending                                      nothing
//	pending.Epoch == oc.Epoch, pending.Seq <= oc.Seq   commit it: that epoch's owner wrote one write per Seq and
//	                                                   committed through oc
//	pending.Epoch != oc.Epoch, pending.Seq <= oc.Seq   clear it: that Seq is decided, possibly as a different write;
//	                                                   catch-up brings the committed one
//	pending.Seq > oc.Seq                            keep: not decided yet
func (m *Manager) settle(p int, oc Pos) {
	st, err := m.store.State(p)
	if err != nil {
		m.log.Error("settle: reading state failed", "partition", p, "error", err)
		return
	}
	pp := st.Pending
	if pp == nil || pp.Pos.Seq > oc.Seq {
		return
	}
	if pp.Pos.Epoch == oc.Epoch {
		// Commit only applies to the write that follows the committed one; anything else is left for catch-up.
		if pp.Pos.Seq != st.Committed.Seq+1 {
			m.log.Warn("settle: pending write does not follow the committed one, left alone", "partition", p,
				"pending", pp.Pos.String(), "committed", st.Committed.String())
			return
		}
		// A copy: what State returned is the Store's, and may be shared with it.
		w := *pp
		w.Prev = st.Committed // this node's own label of the Seq before: see handleWrite
		if err := m.commit(p, &w); err != nil {
			m.log.Error("settle: commit failed", "partition", p, "error", err)
		}
		return
	}
	if err := m.store.SetPending(p, nil); err != nil {
		m.log.Error("settle: clearing the pending write failed", "partition", p, "error", err)
	}
}

// handleHeartbeat processes a PartHeartbeatMsg from `from`, entry by entry. It waits for no partition's mutex and calls
// no Store method (but State, once per partition, to load the cache): each entry is decided from the cached promise and
// the volatile state, and the settling and catch-up it calls for run later, in a goroutine per partition (queueEntry). So
// a busy partition delays neither the reply nor the other entries.
func (m *Manager) handleHeartbeat(from gossip.NodeID, msg *heartbeatMsg) *heartbeatResp {
	if !m.compatible(msg.Size, msg.Cluster) {
		return &heartbeatResp{SizeMismatch: true}
	}
	if m.isStopped() {
		return &heartbeatResp{}
	}
	resp := &heartbeatResp{OK: true}
	for _, e := range msg.Entries {
		fence, accepted := m.onHeartbeatEntry(from, e, true)
		if fence != nil {
			resp.Fenced = append(resp.Fenced, *fence)
		}
		if accepted {
			resp.Accepted = append(resp.Accepted, e.P)
		}
	}
	return resp
}

// handleCommit processes a PartCommitMsg from `from` exactly as one heartbeat entry. It runs on one of the cluster's
// incoming workers (a one-way message has no deferred reply) and, like handleHeartbeat, never waits for a mutex: the
// settling it calls for is queued for the partition's own goroutine, which keeps only the latest notice.
func (m *Manager) handleCommit(from gossip.NodeID, msg *commitMsg) {
	if !m.compatible(msg.Size, msg.Cluster) || m.isStopped() {
		return
	}
	m.onHeartbeatEntry(from, heartbeatEntry{P: msg.P, Epoch: msg.Epoch, Committed: msg.Committed}, false)
}

// onHeartbeatEntry decides one entry of a heartbeat (or a commit notice) from the cached promise and the volatile state,
// under ps.vmu only. It returns a fence if the entry's epoch is below this node's promise, and accepted if the entry was
// taken as the current owner's (see heartbeatResp.Accepted): this node, outside resync mode, looked at its promise after
// the entry arrived and found none above the entry's epoch; the epoch is above the last one relinquished, not below that
// of the owner it believes in, and not below a tenure of its own. The cached promise is never below a promise this
// node has answered (setPromised raises it before the answer), which is what the read confirmation needs (DESIGN.md §8).
// An accepted entry records the owner and its committed position (heartbeat says it came in a heartbeat rather than a
// commit notice), and queues the settling of the pending write and the catch-up for the partition's goroutine.
func (m *Manager) onHeartbeatEntry(from gossip.NodeID, e heartbeatEntry, heartbeat bool) (fence *heartbeatFence, accepted bool) {
	p := e.P
	ps := m.part(p)
	if ps == nil || from == m.self || e.Epoch.Node != from {
		return nil, false
	}
	m.observe(p, e.Epoch, e.Committed.Epoch)
	if m.resync.Load() {
		// Not intact: nothing is settled or caught up here; the startup sync does that.
		return nil, false
	}
	if !m.loadCache(p) {
		return nil, false
	}

	ps.vmu.Lock()
	switch {
	case e.Epoch.Less(ps.promised):
		promised := ps.promised
		ps.vmu.Unlock()
		return &heartbeatFence{P: p, Promised: promised}, false // a deposed owner
	case !ps.relinquished.Less(e.Epoch), // a late heartbeat of a tenure its owner has already ended
		ps.owner != gossip.EmptyNodeID && ps.owner != from && e.Epoch.Less(ps.ownerEpoch), // epochs only rise
		ps.leading && !ps.myEpoch.Less(e.Epoch):                                           // this node's own tenure is the higher
		ps.vmu.Unlock()
		return nil, false
	}
	deposed, was := m.leaveLocked(ps) // another node leads at a higher epoch
	changed := m.setOwnerLocked(ps, from, e.Epoch)
	divergedNow := m.noteReportLocked(ps, e.Committed, heartbeat, m.now())
	ps.vmu.Unlock()

	m.leftLeadership(p, deposed, was)
	if changed {
		m.fire(p, from, e.Epoch)
	}
	if divergedNow {
		m.divergedNow(p, from, e.Committed)
	}
	m.queueEntry(p, from, e.Committed)
	return nil, true
}

// pendingEntry is an accepted entry whose settling and catch-up have not run yet.
type pendingEntry struct {
	from gossip.NodeID
	oc   Pos
}

// queueEntry hands the settling and catch-up that an accepted entry calls for to p's goroutine (applyEntries), starting
// it if it is not running. It keeps one entry per partition, the latest: the one with the highest committed position (an
// owner's committed position only rises, and a later owner's is never below an earlier one's), which decides everything an
// older one would have. Nothing is lost by dropping the older one; the next heartbeat carries the same information
// anyway.
func (m *Manager) queueEntry(p int, from gossip.NodeID, oc Pos) {
	ps := m.parts[p]
	ps.vmu.Lock()
	if ps.next == nil || ps.next.oc.Seq < oc.Seq || (ps.next.oc.Seq == oc.Seq && ps.next.oc.Epoch.Less(oc.Epoch)) {
		ps.next = &pendingEntry{from: from, oc: oc}
	}
	ps.vmu.Unlock()
	if ps.applying.CompareAndSwap(false, true) && !m.spawn(func() { m.applyEntries(p) }) {
		ps.applying.Store(false)
	}
}

// applyEntries is p's goroutine for queued entries: it applies the latest until none is left.
func (m *Manager) applyEntries(p int) {
	ps := m.parts[p]
	for {
		ps.vmu.Lock()
		next := ps.next
		ps.next = nil
		ps.vmu.Unlock()
		if next == nil {
			ps.applying.Store(false)
			// An entry queued after the slot was found empty and before the flag was cleared started no goroutine.
			ps.vmu.RLock()
			more := ps.next != nil
			ps.vmu.RUnlock()
			if !more || !ps.applying.CompareAndSwap(false, true) {
				return
			}
			continue
		}
		m.applyEntry(p, next.from, next.oc)
	}
}

// applyEntry settles p's pending write against an owner's committed position oc, under the partition's mutex, and starts
// a catch-up from the owner if this node is behind it. oc is a fact whatever has happened since the entry was accepted
// (an owner commits only what a majority stored), so settling against it later is as safe as at once.
func (m *Manager) applyEntry(p int, from gossip.NodeID, oc Pos) {
	ps := m.parts[p]
	if err := ps.mu.lock(m.ctx); err != nil {
		return
	}
	defer ps.mu.unlock()
	if m.resync.Load() {
		return
	}
	m.settle(p, oc)
	st, err := m.store.State(p)
	if err != nil {
		m.log.Error("heartbeat: reading state failed", "partition", p, "error", err)
		return
	}
	m.refresh(p, st)
	if st.Committed.Seq < oc.Seq {
		m.catchUpAsync(p, from)
	}
}
