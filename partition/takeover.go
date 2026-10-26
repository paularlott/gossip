package partition

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/paularlott/gossip"
)

var (
	errNoMajority = errors.New("partition: no majority of promises")
	errRetryLater = errors.New("partition: epoch not above the stored promise, retrying")
	// errNotCandidate ends a takeover whose node became ineligible, or found the partition diverged, meanwhile.
	errNotCandidate = errors.New("partition: no longer a candidate for the partition")
)

// ownershipLoop runs the ownership checks every HeartbeatInterval.
func (m *Manager) ownershipLoop() {
	t := time.NewTicker(m.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.ownershipTick()
		case <-m.ctx.Done():
			return
		}
	}
}

// ownershipTick looks at every partition once. For a partition this node leads, it steps down (and says so) if it has had
// no majority contact for OwnerTimeout: its followers on the same side of a partition would otherwise go on refreshing
// ownerSeen from its heartbeats and serving reads of data that the other side's new owner has long moved on from, which
// CanRead's bounded staleness forbids. (A voluntary step-down is always safe: followers refuse writes of epochs below
// their promise.) For a partition it does not lead, this node is a candidate of rank r (0 = the rendezvous owner, then 1,
// 2, ... by the same hash): once the partition has been ownerless for r x RankDelay it starts a takeover (after the random
// backoff); and a rank 0 node that sees a live owner other than itself claims the partition from it (see claimDue). An
// owner never steps down because of rank: it keeps what it leads until it is deposed, fails to replicate or loses its
// majority.
func (m *Manager) ownershipTick() {
	cands := m.candidates()
	for p, ps := range m.parts {
		if _, leading := m.leadingEpoch(p); leading {
			// Without majority contact, ineligible, or diverged here: step down (stepDown relinquishes).
			if !m.recentMajority(ps) || !m.eligible.Load() || m.diverged(p) {
				m.stepDown(p)
			}
			continue
		}
		if m.takeoverDue(p, cands) {
			m.spawnOnce(&ps.takingOver, func() { m.takeoverAfterBackoff(p) })
			continue
		}
		if owner, ok := m.claimDue(p, cands); ok {
			m.claim(p, owner)
		}
	}
}

// trackOwnerless reports whether p is ownerless at this node (it does not lead it and has no live owner: none known, or
// none heard from for OwnerTimeout, or the owner relinquished) and since when. The time is when the last owner went
// quiet, or when this node first noticed there was none; it is kept until an owner is seen again.
func (m *Manager) trackOwnerless(p int) (since time.Time, ownerless bool) {
	ps := m.parts[p]
	now := m.now()
	ps.vmu.Lock()
	defer ps.vmu.Unlock()
	if ps.leading || (ps.owner != gossip.EmptyNodeID && now.Sub(ps.ownerSeen) < m.cfg.OwnerTimeout) {
		ps.ownerlessSince = time.Time{}
		return time.Time{}, false
	}
	if ps.ownerlessSince.IsZero() {
		ps.ownerlessSince = now
		if !ps.ownerSeen.IsZero() {
			ps.ownerlessSince = ps.ownerSeen.Add(m.cfg.OwnerTimeout)
		}
	}
	return ps.ownerlessSince, true
}

// inBackoff reports whether this node must leave p alone (neither take it over nor claim it) because its last takeovers
// of it failed.
func (m *Manager) inBackoff(p int) bool {
	ps := m.parts[p]
	ps.vmu.RLock()
	defer ps.vmu.RUnlock()
	return m.now().Before(ps.backoffUntil)
}

// takeoverFailed puts p into backoff: OwnerTimeout after the first failure, doubling with each consecutive one, up to
// TakeoverBackoffMax.
func (m *Manager) takeoverFailed(p int) time.Duration {
	ps := m.parts[p]
	ps.vmu.Lock()
	defer ps.vmu.Unlock()
	ps.failures++
	d := m.cfg.OwnerTimeout
	for i := 1; i < ps.failures && d < m.cfg.TakeoverBackoffMax; i++ {
		d *= 2
	}
	d = min(d, m.cfg.TakeoverBackoffMax)
	ps.backoffUntil = m.now().Add(d)
	return d
}

// takeoverSucceeded ends p's backoff.
func (m *Manager) takeoverSucceeded(p int) {
	ps := m.parts[p]
	ps.vmu.Lock()
	ps.failures, ps.backoffUntil = 0, time.Time{}
	ps.vmu.Unlock()
}

// takeoverDue reports whether this node should take over p now: it is ready, eligible and a candidate of some rank r
// among cands, p has not diverged here, p has been ownerless at this node for at least r x RankDelay, and p is not in
// backoff here.
func (m *Manager) takeoverDue(p int, cands []gossip.NodeID) bool {
	if !m.ready.Load() || !m.eligible.Load() || m.diverged(p) {
		return false
	}
	rank, ok := rendezvousRank(p, m.self, cands)
	if !ok {
		return false
	}
	since, ownerless := m.trackOwnerless(p)
	return ownerless && m.now().Sub(since) >= time.Duration(rank)*m.cfg.RankDelay && !m.inBackoff(p)
}

// claimDue reports whether this node should ask p's owner to hand it over, and who the owner is: it is ready, eligible
// and rank 0 for p, does not lead it, sees a live owner other than itself, is not in backoff for p, has not claimed it
// within OwnerTimeout, and is caught up: no catch-up of p is running and it has committed at least what the owner
// reported in its last heartbeat (not its commit notices, which a node under load is always a moment behind). A claimant
// that is behind would hold the partition through a long catch-up in its takeover, during which nobody can write to it;
// it claims once it has caught up in the background. A partition that diverged here is never claimed.
func (m *Manager) claimDue(p int, cands []gossip.NodeID) (gossip.NodeID, bool) {
	if !m.ready.Load() || !m.eligible.Load() {
		return gossip.EmptyNodeID, false
	}
	if rank, ok := rendezvousRank(p, m.self, cands); !ok || rank != 0 {
		return gossip.EmptyNodeID, false
	}
	owner, live := m.Owner(p)
	if !live || owner == m.self || m.IsOwner(p) || m.inBackoff(p) {
		return gossip.EmptyNodeID, false
	}
	ps := m.parts[p]
	if ps.catchingUp.Load() || !m.loadCache(p) {
		return gossip.EmptyNodeID, false
	}
	ps.vmu.RLock()
	defer ps.vmu.RUnlock()
	caughtUp := ps.committed.Seq >= ps.hbCommitted.Seq && !ps.diverged
	return owner, caughtUp && m.now().Sub(ps.lastClaim) >= m.cfg.OwnerTimeout
}

// claim asks owner, in the background, to hand p over. The owner steps down and relinquishes if it agrees that this node
// is p's rank 0; this node then finds p ownerless and takes it over in the ordinary way.
func (m *Manager) claim(p int, owner gossip.NodeID) {
	ps := m.parts[p]
	m.spawnOnce(&ps.claiming, func() {
		ps.vmu.Lock()
		ps.lastClaim = m.now()
		ps.vmu.Unlock()
		ctx, cancel := context.WithTimeout(m.ctx, m.cfg.OwnerTimeout)
		defer cancel()
		resp, err := ask[claimResp](ctx, m.net, owner, partClaimMsg, &claimReq{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID, P: p})
		switch {
		case err != nil:
			m.log.Debug("claim failed", "partition", p, "owner", owner.String(), "error", err)
		case resp.SizeMismatch || !resp.OK:
			m.log.Debug("claim refused", "partition", p, "owner", owner.String())
		default:
			m.log.Info("claim granted", "partition", p, "owner", owner.String())
			// The owner has stepped down for this node: take over now, without waiting for the next tick and a
			// random backoff (they exist to separate nodes competing for an abandoned partition; this one was handed
			// over). The reply stands in for the relinquish message, which may arrive later.
			m.handleRelinquish(owner, &relinquishMsg{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID, P: p, Epoch: resp.Epoch})
			m.spawnOnce(&ps.takingOver, func() { m.takeoverAfter(p, 0) })
		}
	})
}

// takeoverAfterBackoff waits a random time in [0, BackoffMax) and takes over (takeoverAfter).
func (m *Manager) takeoverAfterBackoff(p int) {
	m.takeoverAfter(p, time.Duration(rand.Int63n(int64(m.cfg.BackoffMax))))
}

// takeoverAfter waits d, re-checks the trigger and takes over. A takeover that does not complete puts the partition into
// backoff at this node.
func (m *Manager) takeoverAfter(p int, d time.Duration) {
	if !m.sleep(m.ctx, d) {
		return
	}
	if !m.takeoverDue(p, m.candidates()) {
		return
	}
	err := m.takeover(m.ctx, p)
	switch {
	case err == nil:
		m.takeoverSucceeded(p)
	case errors.Is(err, errRetryLater) || errors.Is(err, errNotCandidate) || m.ctx.Err() != nil:
		// Nothing was asked of anyone, this node may no longer lead the partition, or the Manager is stopping: not a
		// failed attempt.
	default:
		d := m.takeoverFailed(p)
		if errors.Is(err, ErrExhausted) {
			m.log.Error("takeover refused: a counter is exhausted", "partition", p, "error", err)
		} else {
			m.log.Debug("takeover did not complete", "partition", p, "error", err, "backoff", d)
		}
	}
}

// answer is one node's reply to a promise request.
type answer struct {
	from      gossip.NodeID
	committed Pos
	pending   *Write
}

// takeover makes this node the owner of p at a new epoch: a promise round at an epoch above any seen, a catch-up to
// the highest committed position among the promisers, and the settling of the write the previous owner may have left
// half done. It holds the partition's mutex throughout, including while waiting for the network. A nil return means
// this node leads p and that write has been settled.
func (m *Manager) takeover(ctx context.Context, p int) error {
	ps := m.parts[p]
	if err := ps.mu.lock(ctx); err != nil {
		return err
	}
	defer ps.mu.unlock()
	if _, leading := m.leadingEpoch(p); leading {
		return nil
	}

	counter, err := increment(ps.maxCounter.Load())
	if err != nil {
		return err
	}
	e := Epoch{Counter: counter, Node: m.self}
	st, err := m.store.State(p)
	if err != nil {
		return err
	}
	if e.Compare(st.Promised) <= 0 {
		ps.raise(st.Promised.Counter)
		return errRetryLater
	}
	if err := m.setPromised(p, e); err != nil {
		return err
	}
	ps.raise(e.Counter) // a failed attempt retries at a higher epoch
	start := m.now()

	answers := []answer{{from: m.self, committed: st.Committed, pending: st.Pending}}
	answers = append(answers, m.collectPromises(ctx, p, e)...)
	if len(answers) < majority(m.cfg.ClusterSize) {
		return errNoMajority
	}

	// H: the highest committed position among the answers (equal Seq means the same write).
	h := st.Committed
	for _, a := range answers {
		if a.committed.Seq > h.Seq {
			h = a.committed
		}
	}
	if st.Committed.Seq < h.Seq {
		cur, err := m.catchUpFrom(ctx, p, answers, h.Seq)
		if err != nil {
			return err
		}
		// A source may have advanced past H by settling writes already decided; those are committed, so they are
		// the new H.
		h = cur
	}

	// The pending write to settle: among the answers' pending writes at H+1, the one of the highest epoch (the one an
	// earlier owner most recently got as far as sending). Pending writes at or below H are already decided and ignored.
	var cand *Write
	for _, a := range answers {
		cand = pickPending(cand, a.pending, h.Seq+1)
	}
	var w Write
	if cand != nil {
		seq, err := increment(h.Seq)
		if err != nil {
			return err
		}
		w = Write{Partition: p, Pos: Pos{Epoch: e, Seq: seq}, Prev: h, Data: cand.Data}
	}

	if !m.eligible.Load() || m.diverged(p) {
		return errNotCandidate // the promises it collected are harmless: it never writes at e
	}
	// The new owner counts majority contact (lastMajority: CanRead) only once the write it settles is committed, or
	// from the promise round if there is none; it holds the mutex until then, so no change or read runs before.
	m.becomeLeader(p, e, start)
	if cand != nil {
		settleStart := m.now()
		if resp := m.replicate(p, w, h); resp.Status != statusDone {
			return errNoMajority // replicate stepped down (and relinquished)
		}
		m.takeoverSettled(p, e, settleStart)
	} else {
		m.takeoverSettled(p, e, start)
	}
	m.log.Info("took over", "partition", p, "epoch", e.String(), "committed", h.String(), "settled", cand != nil)
	m.spawn(m.sendHeartbeats) // announce
	return nil
}

// collectPromises sends Promise(p, e) to every other alive node in parallel and returns the OK answers, waiting until
// a majority (this node counted) is in, or every node has answered, or OwnerTimeout passes. Refusals raise
// maxCounter.
func (m *Manager) collectPromises(ctx context.Context, p int, e Epoch) []answer {
	need := majority(m.cfg.ClusterSize)
	pctx, cancel := context.WithTimeout(ctx, m.cfg.OwnerTimeout)
	defer cancel()
	peers, replies := askAll[promiseResp](pctx, m.net, partPromiseMsg, &promiseReq{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID, P: p, Epoch: e})

	var out []answer
	for answered := 0; answered < peers && len(out)+1 < need; answered++ {
		select {
		case rep := <-replies:
			resp := rep.resp
			if resp == nil || resp.SizeMismatch {
				continue
			}
			m.observe(p, resp.Promised, resp.Committed.Epoch)
			if resp.Pending != nil {
				m.observe(p, resp.Pending.Pos.Epoch)
			}
			if resp.OK {
				out = append(out, answer{from: rep.from, committed: resp.Committed, pending: resp.Pending})
			}
		case <-pctx.Done():
			return out
		}
	}
	return out
}

// pickPending returns w if it is a pending write at seq of a higher epoch than cand's (cand may be nil), and cand
// otherwise. Taking it over all the pending writes reported by other nodes gives the one to settle (takeover) or to
// adopt (resync).
func pickPending(cand, w *Write, seq uint64) *Write {
	if w != nil && w.Pos.Seq == seq && (cand == nil || cand.Pos.Epoch.Less(w.Pos.Epoch)) {
		return w
	}
	return cand
}

// catchUpFrom brings p's committed position to at least seq by catching up from the answerers that hold it, one
// after the other until one succeeds. The caller holds the partition's mutex. It returns the committed position.
func (m *Manager) catchUpFrom(ctx context.Context, p int, answers []answer, seq uint64) (Pos, error) {
	err := errors.New("partition: no node to catch up from")
	for _, a := range answers {
		if a.from == m.self || a.committed.Seq < seq {
			continue
		}
		if err = m.catchUp(ctx, p, a.from, true); err != nil {
			continue
		}
		st, serr := m.store.State(p)
		if serr != nil {
			return Pos{}, serr
		}
		if st.Committed.Seq >= seq {
			return st.Committed, nil
		}
		err = errors.New("partition: catch-up did not reach the committed position")
	}
	return Pos{}, err
}
