package partition

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/paularlott/gossip"
)

// Write makes change to partition p through its owner and returns once the owner has committed it on a majority. The
// Result is the committed write's position and the reply the owner's Store.Prepare returned for the caller; on any error
// it is zero.
//
// The error is nil (committed), a *RefusedError (errors.Is(err, ErrRefused): the Store refused the change, with its code
// and message; nothing was written), ErrUnknown (the change was sent but its outcome is not known: a timeout, or the owner
// was lost after replication began), ErrExhausted (a counter would have wrapped; nothing was written) or ErrUnavailable
// (no owner reachable, no majority, or not ready within WriteTimeout; nothing was replicated). The change is never
// forwarded twice and is never retried once it may have been delivered. A refusal's RefusedError.Pos is the owner's
// committed position when it refused.
//
// Write waits at most WriteTimeout (or until ctx ends) for the owner, but once this node, as the owner, has begun the
// write it returns only when the write is decided: committed, or the owner stepped down, which takes at most WriteTimeout
// from the write's start. A change sent to another node carries ctx's deadline; that owner refuses it, with nothing
// done, if less than 2 x HeartbeatInterval of it is left when it could start the write.
func (m *Manager) Write(ctx context.Context, p int, change []byte) (Result, error) {
	resp, err := m.route(ctx, p, func(ctx context.Context, o gossip.NodeID) (*changeResp, error) {
		if o == m.self {
			return m.runChange(ctx, p, change, false), nil
		}
		resp, err := ask[changeResp](ctx, m.net, o, partChangeMsg, &changeReq{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID,
			P: p, Change: change, Deadline: deadlineOf(ctx)})
		switch {
		case errors.Is(err, errNotSent):
			return &changeResp{Status: statusUnavailable}, nil // certainly not delivered: try again
		case err != nil:
			// Sent, or possibly sent: the owner may have replicated it.
			m.log.Debug("write: outcome unknown", "partition", p, "error", err)
			return nil, ErrUnknown
		}
		return resp, nil
	})
	if err != nil {
		return Result{}, err
	}
	return resp.outcome()
}

// route runs an operation on p's owner and returns the reply that settles it. attempt(ctx, o) runs it on node o (m.self
// when this node leads p) and returns the owner's changeResp, or an error that ends the operation (a write that may have
// been delivered cannot be retried). A reply that settles the operation (final) is returned at once; otherwise route
// waits and tries again until WriteTimeout, and then returns ErrUnavailable:
//
//   - a node that is not the owner answers notowner, with the owner it believes in; that owner is tried next, once per
//     attempt (the hint), and otherwise this node asks the owner it believes in, or the rendezvous owner;
//   - an owner at its limit answers busy: nothing was run, and the operation is sent again after a pause that starts at
//     1ms and doubles with each busy answer in a row up to maxBusyPause, with jitter (a random time between half the
//     pause and all of it), never past ctx's end;
//   - anything else (unavailable, no owner known) waits a quarter of HeartbeatInterval.
//
// It sends nothing to another node once less than deadlineFloor is left before ctx's deadline: that owner would refuse it.
func (m *Manager) route(ctx context.Context, p int,
	attempt func(ctx context.Context, o gossip.NodeID) (*changeResp, error)) (*changeResp, error) {
	if m.part(p) == nil {
		return nil, fmt.Errorf("partition: partition %d out of range [0, %d)", p, len(m.parts))
	}
	if m.isStopped() {
		return nil, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.WriteTimeout)
	defer cancel()

	var hint gossip.NodeID // an owner a node named in a notowner reply, tried once
	var busy time.Duration // the pause after the last busy answer in a row (0: the last answer was not busy)
	for ctx.Err() == nil {
		o := hint
		hint = gossip.EmptyNodeID
		leading := m.IsOwner(p)
		if leading {
			o = m.self
		} else if o == gossip.EmptyNodeID {
			var ok bool
			if o, ok = m.Owner(p); !ok {
				o, _ = rendezvousOwner(p, m.candidates())
			}
		}

		if o != gossip.EmptyNodeID && o != m.self && m.tooLate(ctx, true) {
			// The owner would refuse it (see tooLate), and a request sent this late may not even arrive before the
			// deadline, which would make an operation that was never run look unknown. Every attempt so far did nothing.
			break
		}
		pause := m.cfg.HeartbeatInterval / 4
		if o != gossip.EmptyNodeID && (leading || o != m.self) {
			resp, err := attempt(ctx, o)
			if err != nil || resp.final() {
				return resp, err
			}
			if resp.Status != statusBusy {
				busy = 0
			}
			switch {
			case resp.Status == statusBusy:
				busy = min(max(2*busy, time.Millisecond), maxBusyPause)
				pause = busy/2 + time.Duration(rand.Int63n(int64(busy/2)+1))
			case resp.Status == statusNotOwner && resp.Owner != gossip.EmptyNodeID && resp.Owner != o && resp.Owner != m.self:
				hint = resp.Owner
				continue
			}
		}
		if !m.sleep(ctx, pause) {
			break
		}
	}
	return nil, ErrUnavailable
}

// maxBusyPause bounds the pause before an operation that an owner answered busy is sent again.
const maxBusyPause = 50 * time.Millisecond

// deadlineOf returns ctx's deadline as Unix nanoseconds, or 0 if it has none: what a forwarded request carries.
func deadlineOf(ctx context.Context) int64 {
	if dl, ok := ctx.Deadline(); ok {
		return dl.UnixNano()
	}
	return 0
}

// final reports whether a reply settles the operation (it is not a "nothing was done, try again" reply).
func (r *changeResp) final() bool {
	if r.SizeMismatch {
		return true
	}
	switch r.Status {
	case statusDone, statusRefused, statusUnknown, statusExhausted:
		return true
	}
	return false
}

// outcome maps a final reply to the result of the Write or Read that asked.
func (r *changeResp) outcome() (Result, error) {
	if r.SizeMismatch {
		return Result{}, ErrSizeMismatch
	}
	switch r.Status {
	case statusDone:
		return Result{Pos: r.Pos, Reply: r.Reply}, nil
	case statusRefused:
		return Result{}, &RefusedError{Code: r.RefusedCode, Msg: r.RefusedMsg, Pos: r.Pos}
	case statusExhausted:
		return Result{}, ErrExhausted
	case statusUnknown:
		return Result{}, ErrUnknown
	}
	return Result{}, ErrUnavailable
}

// runChange runs a change on the owner under the partition's mutex: it checks the node still leads and has had
// majority contact, prepares the write with the Store and replicates it. The reply is what a PartChangeMsg returns. ctx
// bounds the wait for the mutex; a forwarded change whose requester's deadline (ctx's) is too close once the mutex is
// held is refused with nothing done (tooLate). Replication, once begun, runs on its own WriteTimeout.
func (m *Manager) runChange(ctx context.Context, p int, change []byte, forwarded bool) *changeResp {
	ps := m.parts[p]
	if err := ps.mu.lock(ctx); err != nil {
		return &changeResp{Status: statusUnavailable}
	}
	defer ps.mu.unlock()

	epoch, leading := m.leadingEpoch(p)
	if !leading {
		return m.notOwner(p)
	}
	if !m.recentMajority(ps) {
		// No majority answered within OwnerTimeout: this node may be cut off, so it must not accept changes.
		return &changeResp{Status: statusUnavailable}
	}
	if m.tooLate(ctx, forwarded) {
		return &changeResp{Status: statusUnavailable}
	}
	st, err := m.store.State(p)
	if err != nil {
		m.log.Error("runChange: reading state failed", "partition", p, "error", err)
		m.stepDown(p) // the Store cannot be trusted
		return &changeResp{Status: statusUnavailable}
	}
	if st.Promised != epoch {
		// Fenced: a higher epoch was promised (or something else changed the promise).
		m.stepDown(p)
		return m.notOwner(p)
	}
	data, reply, err := m.store.Prepare(p, change)
	if err != nil {
		if errors.Is(err, ErrRefused) {
			re := refusal(err)
			return &changeResp{Status: statusRefused, RefusedCode: re.Code, RefusedMsg: re.Msg, Pos: st.Committed}
		}
		m.log.Error("runChange: prepare failed", "partition", p, "error", err)
		return &changeResp{Status: statusUnavailable}
	}
	seq, err := increment(st.Committed.Seq)
	if err != nil {
		return &changeResp{Status: statusExhausted}
	}
	w := Write{Partition: p, Pos: Pos{Epoch: epoch, Seq: seq}, Prev: st.Committed, Data: data}
	resp := m.replicate(p, w, st.Committed)
	if resp.Status == statusDone {
		resp.Pos, resp.Reply = w.Pos, reply
	}
	return resp
}

// replicate sends w to every other alive node and, at the same time, stores it as the owner's own pending write
// (Store.SetPending, durable), and commits it once a majority (the owner included) holds it. The owner's own
// acknowledgement counts once its SetPending has returned successfully, so a write waits for the slower of the owner's
// durable SetPending and the followers', not for their sum. The caller holds the partition's mutex.
//
// Once begun, a replication runs on its own WriteTimeout from its start, whatever is left of its caller's: a write that
// was begun is committed, or its owner steps down (and relinquishes), so that a takeover settles it; it is never left
// half done because a caller gave up. The owner never commits before its own SetPending has returned, and replicate does
// not return before it has either (the Store call cannot be abandoned, and a later one on the partition must not overtake
// it). If the owner's SetPending fails the write may already be stored on followers, so the outcome is unknown and the
// owner steps down: the same holds when the outcome cannot be told.
func (m *Manager) replicate(p int, w Write, committed Pos) *changeResp {
	epoch := w.Pos.Epoch
	start := m.now()

	own := make(chan error, 1)
	go func() { own <- m.store.SetPending(p, &w) }()

	need := majority(m.cfg.ClusterSize)
	// The sends outlive this call when a majority answers first, so that slower nodes still get the write.
	sctx, cancel := context.WithTimeout(m.ctx, m.cfg.WriteTimeout)
	peers, replies := askAll[writeResp](sctx, m.net, partWriteMsg,
		&writeReq{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID, Write: w, OwnerCommitted: committed})

	acks, answered, higher := 0, 0, false // the followers' acknowledgements
	var ownErr error
	ownDone, expired := false, false
	done := sctx.Done()
	for {
		// Never stop before the owner's own SetPending has returned: that is when it may commit, and when the Store is
		// free for the partition's next operation.
		if ownDone && (ownErr != nil || acks+1 >= need || answered == peers || expired) {
			break
		}
		select {
		case r := <-replies:
			answered++
			if resp := r.resp; resp != nil && !resp.SizeMismatch {
				m.observe(p, resp.Promised)
				higher = higher || epoch.Less(resp.Promised)
				if resp.OK {
					acks++ // a follower that answered NeedCatchUp is not counted; it catches up on its own
				}
			}
		case ownErr = <-own:
			ownDone = true
			own = nil
		case <-done:
			expired, done = true, nil
		}
	}
	go func(rest int) { // drain, then release the sends' context
		for ; rest > 0; rest-- {
			if resp := (<-replies).resp; resp != nil && !resp.SizeMismatch {
				m.observe(p, resp.Promised)
				if epoch.Less(resp.Promised) {
					m.stepDownFrom(p, epoch) // a late reply shows a higher promise: this tenure is over
				}
			}
		}
		cancel()
	}(peers - answered)

	if ownErr != nil {
		// The followers may hold the write already: not unavailable but unknown, and a takeover settles it.
		m.log.Error("replicate: storing the pending write failed", "partition", p, "error", ownErr)
		m.stepDown(p) // the Store cannot be trusted
		return &changeResp{Status: statusUnknown}
	}
	if acks+1 >= need { // the owner's own acknowledgement: its SetPending has returned
		if err := m.commit(p, &w); err != nil {
			m.log.Error("replicate: commit failed after a majority stored the write", "partition", p, "error", err)
			m.stepDown(p)
			return &changeResp{Status: statusUnknown}
		}
		m.announceCommit(p, epoch, w.Pos)
		m.touchMajority(p, epoch, start)
		if higher {
			m.stepDown(p)
		}
		return &changeResp{Status: statusDone}
	}
	// The write may or may not be committed: step down so that a takeover settles it.
	m.log.Debug("replicate: no majority stored the write in time", "partition", p, "pos", w.Pos.String(),
		"acks", acks+1, "answered", answered, "peers", peers)
	m.stepDown(p)
	return &changeResp{Status: statusUnknown}
}

// commit applies w to p through the Store, raises the cache and wakes the callers of WaitApplied.
func (m *Manager) commit(p int, w *Write) error {
	if err := m.store.Commit(w); err != nil {
		return err
	}
	m.applied(p, w.Pos)
	return nil
}

// announceCommit tells every other alive node, without waiting, that this node committed the write at pos on p in its
// tenure at epoch (PartCommitMsg), so that followers apply it within one message delay instead of at the next write or
// heartbeat. The notices may overtake one another; a receiver handles each as the idempotent heartbeat entry it is.
func (m *Manager) announceCommit(p int, epoch Epoch, pos Pos) {
	msg := &commitMsg{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID, P: p, Epoch: epoch, Committed: pos}
	m.spawn(func() {
		for _, pi := range m.net.peers() {
			if err := m.net.send(pi.ID, partCommitMsg, msg); err != nil {
				m.log.Debug("commit notice send failed", "partition", p, "to", pi.ID.String(), "error", err)
			}
		}
	})
}
