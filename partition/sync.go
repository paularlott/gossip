package partition

import (
	"context"
)

// syncLoop is the startup synchronisation: it repeats syncOnce until it succeeds, waits ReadyHoldDown and publishes
// partition.ready, which makes the node a candidate for ownership.
func (m *Manager) syncLoop() {
	for !m.ready.Load() {
		if m.syncOnce() {
			if !m.sleep(m.ctx, m.cfg.ReadyHoldDown) {
				return
			}
			m.ready.Store(true)
			m.publishReady()
			m.log.Info("ready", "eligible", m.eligible.Load())
			return
		}
		if !m.sleep(m.ctx, m.cfg.HeartbeatInterval) {
			return
		}
	}
}

// syncOnce asks every alive node for its state. Only the answers of intact nodes count, and only they are used: a node in
// resync mode (stateResp.Intact false) may have lost promises it made and writes it acknowledged, so its answer proves
// nothing, and two nodes that both lost their data must never resync from each other alone. Once intact answers from a
// majority of nodes including this one (in resync mode resyncQuorum of the other nodes; with ForceResync, at least one)
// are in, it catches every partition up to what the highest reporter has committed; in resync mode it also raises each
// partition's promise to the highest reported, adopts pending writes and leaves resync mode. It reports whether every
// partition is caught up to what the answers reported.
func (m *Manager) syncOnce() bool {
	resync := m.resync.Load()
	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.OwnerTimeout)
	defer cancel()
	peers, replies := askAll[stateResp](ctx, m.net, partStateMsg, &stateReq{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID})

	var answers []reply[stateResp]
	notIntact := 0
	for range peers {
		select {
		case rep := <-replies:
			r := rep.resp
			if r == nil || r.SizeMismatch || len(r.Committed) != len(m.parts) || len(r.Promised) != len(m.parts) {
				continue
			}
			if !r.Intact {
				notIntact++
				continue
			}
			answers = append(answers, rep)
		case <-ctx.Done():
		}
	}
	have, need := len(answers), majority(m.cfg.ClusterSize)
	switch {
	case !resync:
		have++ // this node's own state is one of the majority
	case m.cfg.ForceResync:
		need = 1 // an operator accepted resyncing from whichever intact nodes answer
	default:
		// Even a forming peer can be stale: another majority may have committed
		// without it. Only an explicit ForceResync may lower the recovery quorum.
		need = resyncQuorum(m.cfg.ClusterSize)
	}
	if have < need {
		m.syncWaiting(resync, len(answers), notIntact, need)
		return false
	}
	if resync && m.cfg.ForceResync && have < resyncQuorum(m.cfg.ClusterSize) {
		m.log.Warn("forced resync from fewer of the other nodes than every majority needs: writes only the others held may be lost",
			"answered", have, "needed", resyncQuorum(m.cfg.ClusterSize))
	}

	ok := true
	for p := range m.parts {
		if !m.syncPartition(p, answers, resync) {
			ok = false
		}
	}
	if ok && resync {
		m.leaveResync()
	}
	if ok && len(answers)+1 >= majority(m.cfg.ClusterSize) {
		// A majority of intact nodes, this one included: the cluster's data set is formed and this node is part of it.
		if err := m.markEstablished(); err != nil {
			m.log.Error("recording that this node is established failed", "error", err)
			return false
		}
	}
	return ok
}

// syncWaiting logs, at most once per catchUpWarnEvery at warn level, that the startup synchronisation is waiting for
// more intact nodes to answer: an operator may have to bring nodes back, or start a node that lost its data with
// ForceResync.
func (m *Manager) syncWaiting(resync bool, intact, notIntact, need int) {
	now := m.now()
	m.lifeMu.Lock()
	warn := now.Sub(m.syncWarned) >= catchUpWarnEvery
	if warn {
		m.syncWarned = now
	}
	m.lifeMu.Unlock()
	log := m.log.Debug
	if warn {
		log = m.log.Warn
	}
	log("startup sync: waiting for enough intact nodes to answer", "resync", resync, "intact_answers", intact,
		"resyncing_answers", notIntact, "needed", need, "force", m.cfg.ForceResync)
}

// syncPartition brings p up to what the answers reported, and reports whether it did: it catches up from a node that
// has the highest reported committed position, and, in resync mode, first raises the promise to the highest reported and
// afterwards adopts a pending write (see adoptPending).
func (m *Manager) syncPartition(p int, answers []reply[stateResp], resync bool) bool {
	st, err := m.store.State(p)
	if err != nil {
		m.log.Error("sync: reading state failed", "partition", p, "error", err)
		return false
	}
	best := st.Committed
	var promised Epoch
	for _, a := range answers {
		if c := a.resp.Committed[p]; c.Seq > best.Seq {
			best = c
		}
		if pr := a.resp.Promised[p]; promised.Less(pr) {
			promised = pr
		}
		m.observe(p, a.resp.Promised[p], a.resp.Committed[p].Epoch)
	}
	if resync && st.Promised.Less(promised) {
		if err := m.setPromisedLocked(p, promised); err != nil {
			m.log.Error("sync: recording promise failed", "partition", p, "error", err)
			return false
		}
	}
	if best.Seq > st.Committed.Seq && !m.catchUpFromAny(p, answers, best.Seq) {
		return false
	}
	if resync {
		if err := m.adoptPending(p, answers); err != nil {
			m.log.Error("sync: adopting a pending write failed", "partition", p, "error", err)
			return false
		}
	}
	return true
}

// catchUpFromAny catches p up to seq from the answerers that report having it, one after the other until one succeeds.
func (m *Manager) catchUpFromAny(p int, answers []reply[stateResp], seq uint64) bool {
	release, ok := m.catchUpSlot(m.ctx)
	if !ok {
		return false
	}
	defer release()
	for _, a := range answers {
		if a.resp.Committed[p].Seq < seq {
			continue
		}
		if err := m.catchUp(m.ctx, p, a.from, false); err != nil {
			m.log.Debug("sync: catch-up failed", "partition", p, "from", a.from.String(), "error", err)
			continue
		}
		if cur, err := m.store.State(p); err == nil && cur.Committed.Seq >= seq {
			return true
		}
	}
	return false
}

// adoptPending is the part of resync that restores what the lost disk may have held as an acknowledgement: the pending
// write of the highest epoch at the position after the committed one among the other nodes' answers. A write is
// committed once a majority holds it. The node whose data was lost may have been part of that majority, and then the
// only other holders may hold it as pending only (they learn it is committed from the owner's next heartbeat or write).
// If the node came back without that write, a takeover whose promise round happens to skip those holders would write
// something else at its position and the acknowledged write would be lost. Holding the write as its own pending, the
// node reports it in every promise it gives, as an intact node would. (Among the answers of resyncQuorum of the other
// nodes there is always one holder of every write a majority stored.) The caller must have caught the node up to what
// the answers committed.
func (m *Manager) adoptPending(p int, answers []reply[stateResp]) error {
	ps := m.parts[p]
	if err := ps.mu.lock(m.ctx); err != nil {
		return err
	}
	defer ps.mu.unlock()
	st, err := m.store.State(p)
	if err != nil {
		return err
	}
	var cand *Write
	for _, a := range answers {
		if len(a.resp.Pending) == len(m.parts) {
			cand = pickPending(cand, a.resp.Pending[p], st.Committed.Seq+1)
		}
	}
	if cand == nil || (st.Pending != nil && !st.Pending.Pos.Epoch.Less(cand.Pos.Epoch)) {
		return nil
	}
	m.observe(p, cand.Pos.Epoch)
	w := *cand
	w.Partition = p
	w.Prev = st.Committed // this node's own label of the position before, as handleWrite stores it
	return m.store.SetPending(p, &w)
}

// setPromisedLocked raises p's promise to e under the partition's mutex.
func (m *Manager) setPromisedLocked(p int, e Epoch) error {
	ps := m.parts[p]
	if err := ps.mu.lock(m.ctx); err != nil {
		return err
	}
	defer ps.mu.unlock()
	st, err := m.store.State(p)
	if err != nil {
		return err
	}
	if !st.Promised.Less(e) {
		return nil
	}
	return m.setPromised(p, e)
}
