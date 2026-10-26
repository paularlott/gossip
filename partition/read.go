package partition

import (
	"context"
	"errors"
	"slices"

	"github.com/paularlott/gossip"
)

var (
	// errNotLeading is the outcome of a leadership confirmation that found this node no longer owns the partition (it was
	// fenced, or had stepped down already).
	errNotLeading = errors.New("partition: not the owner")
	// errNotConfirmed is the outcome of a confirmation round that did not get a majority of answers in time.
	errNotConfirmed = errors.New("partition: leadership not confirmed by a majority")
)

// Read answers req from the committed data of partition p, as of at least every write that was committed before Read was
// called: it is linearizable. It runs Store.Read on p's owner, after the owner has confirmed that it still owns p (see
// runRead), so it is the way to decide on the latest state rather than on this node's own copy, which may lag (CanRead and
// a local read of the Store are cheaper and not linearizable). It costs a round trip from the owner to a majority of the
// cluster, shared by the reads that arrive while it runs, plus the hop to the owner when this node is not it.
//
// It routes like Write: this node answers if it leads p, otherwise the request goes to the owner it believes in, a node
// that is not the owner names the one it believes in, and an owner that is at its limit (Config.MaxConcurrentReads) asks
// for a retry. Nothing is changed, so a read is simply sent again whenever it may not have arrived. The error is a
// *RefusedError (errors.Is(err, ErrRefused): Store.Read refused the request), ErrSizeMismatch, or ErrUnavailable (no owner
// reachable, or no majority confirmed its leadership, within WriteTimeout; Store.Read failed). The answer is the bytes
// Store.Read returned.
func (m *Manager) Read(ctx context.Context, p int, req []byte) ([]byte, error) {
	resp, err := m.route(ctx, p, func(ctx context.Context, o gossip.NodeID) (*changeResp, error) {
		if o == m.self {
			return m.runRead(ctx, p, req, false), nil
		}
		resp, err := ask[changeResp](ctx, m.net, o, partReadMsg, &readReq{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID,
			P: p, Req: req, Deadline: deadlineOf(ctx)})
		if err != nil {
			// A read changes nothing, so a request that may have been delivered is simply sent again.
			m.log.Debug("read: request failed", "partition", p, "owner", o.String(), "error", err)
			return &changeResp{Status: statusUnavailable}, nil
		}
		return resp, nil
	})
	if err != nil {
		return nil, err
	}
	res, err := resp.outcome()
	return res.Reply, err
}

// runRead runs a read on the owner (the read-index method): it notes the committed position under the partition's mutex,
// which it holds only for that (and which it has to wait for while a takeover is still settling the previous owner's
// last write, or a write is replicating), confirms with a majority that this node still owns the partition in a round that
// started after the read arrived, and then answers from the Store. The reply is what a PartReadMsg returns. A forwarded
// read whose requester's deadline (ctx's) is too close once the mutex is held is refused (tooLate).
//
// Why it is linearizable. A write that was committed before the read arrived is applied here: it was committed by this
// tenure (the owner commits before it answers), or by an earlier one, in which case the takeover settled it before it
// released the mutex (the takeover holds it until its settling write is committed), which the read waited for. Nothing
// newer than this tenure has committed anything, because the confirmation round found a majority, this node included,
// holding no promise above this node's epoch after the read arrived: a later owner needs a majority of promises, which
// would include one of those nodes, and that node promised after it answered the round, so the later owner's takeover
// completed after the read arrived.
func (m *Manager) runRead(ctx context.Context, p int, req []byte, forwarded bool) *changeResp {
	ps := m.parts[p]
	if err := ps.mu.lock(ctx); err != nil {
		return &changeResp{Status: statusUnavailable}
	}
	epoch, leading := m.leadingEpoch(p)
	if !leading {
		ps.mu.unlock()
		return m.notOwner(p)
	}
	if m.tooLate(ctx, forwarded) {
		ps.mu.unlock()
		return &changeResp{Status: statusUnavailable}
	}
	st, err := m.store.State(p)
	if err != nil {
		m.log.Error("read: reading state failed", "partition", p, "error", err)
		m.stepDown(p) // the Store cannot be trusted
		ps.mu.unlock()
		return &changeResp{Status: statusUnavailable}
	}
	if st.Promised != epoch {
		// Fenced: a higher epoch was promised (or something else changed the promise).
		m.stepDown(p)
		ps.mu.unlock()
		return m.notOwner(p)
	}
	idx := st.Committed
	ps.mu.unlock()

	if err := m.confirmLeadership(ctx, p, epoch); err != nil {
		if errors.Is(err, errNotLeading) {
			return m.notOwner(p)
		}
		return &changeResp{Status: statusUnavailable}
	}
	// A node that is no longer the owner could still answer correctly, but there is no reason to serve from one.
	if cur, leading := m.leadingEpoch(p); !leading || cur != epoch {
		return m.notOwner(p)
	}
	if st, err = m.store.State(p); err != nil || st.Committed.Seq < idx.Seq {
		// Cannot happen: committed only moves forward.
		m.log.Error("read: committed position went backwards", "partition", p, "index", idx.String(), "error", err)
		return &changeResp{Status: statusUnavailable}
	}
	reply, err := m.store.Read(p, req)
	if err != nil {
		if errors.Is(err, ErrRefused) {
			re := refusal(err)
			return &changeResp{Status: statusRefused, RefusedCode: re.Code, RefusedMsg: re.Msg, Pos: st.Committed}
		}
		m.log.Error("read: Store.Read failed", "partition", p, "error", err)
		return &changeResp{Status: statusUnavailable}
	}
	return &changeResp{Status: statusDone, Reply: reply}
}

// readRound is one leadership confirmation of a partition, shared by the reads that waited for it.
type readRound struct {
	done  chan struct{} // closed when the round has ended
	epoch Epoch         // the tenure the round confirmed (when err is nil)
	err   error         // nil: a majority, this node included, confirmed epoch
}

func newReadRound() *readRound { return &readRound{done: make(chan struct{})} }

func (r *readRound) finish(epoch Epoch, err error) {
	r.epoch, r.err = epoch, err
	close(r.done)
}

// confirmLeadership waits for a leadership confirmation round of p that started after it was called, and returns nil if
// it confirmed the tenure at epoch. Reads that arrive while a round is in flight wait for the next one (the in-flight one
// started before they arrived and proves nothing about them), and all of them share it; so there is at most one round
// running per partition, and at most one more waiting to start.
func (m *Manager) confirmLeadership(ctx context.Context, p int, epoch Epoch) error {
	r := m.joinRound(p)
	select {
	case <-r.done:
	case <-ctx.Done():
		return ctx.Err()
	case <-m.ctx.Done():
		return ErrUnavailable
	}
	if r.err != nil {
		return r.err
	}
	if r.epoch != epoch {
		return errNotLeading // this node left the tenure the read began in, and came back in another
	}
	return nil
}

// joinRound returns the round that will serve a read arriving now: a new one, started at once, if none is running, and
// otherwise the one that starts when the running one ends.
func (m *Manager) joinRound(p int) *readRound {
	ps := m.parts[p]
	ps.roundMu.Lock()
	defer ps.roundMu.Unlock()
	if ps.roundRunning {
		if ps.roundNext == nil {
			ps.roundNext = newReadRound()
		}
		return ps.roundNext
	}
	r := newReadRound()
	ps.roundRunning = true
	if !m.spawn(func() { m.runRounds(p, r) }) {
		ps.roundRunning = false
		r.finish(Epoch{}, ErrUnavailable)
	}
	return r
}

// runRounds runs r and then, for as long as reads have joined the next round meanwhile, that one.
func (m *Manager) runRounds(p int, r *readRound) {
	ps := m.parts[p]
	for r != nil {
		m.runRound(p, r)
		ps.roundMu.Lock()
		r = ps.roundNext
		ps.roundNext = nil
		ps.roundRunning = r != nil
		ps.roundMu.Unlock()
	}
}

// runRound is the leadership confirmation itself: it sends p's heartbeat entry (this tenure's epoch and the committed
// position) to every other alive node and waits until a majority of the configured size, this node included, has accepted
// it (heartbeatResp.Accepted: the receiver held no promise above the epoch when it looked), for at most OwnerTimeout.
// The round fails, and this node steps down as it does when a heartbeat or a write shows it was deposed, if a node
// answers with a promise above the epoch (it is fenced), or if no majority accepts in time (it may be cut off; the
// ownership loop would step down for the same reason once it had had no majority contact for OwnerTimeout, and the
// read must not wait for that). The node's own promise counts only if it is still the tenure's epoch.
func (m *Manager) runRound(p int, r *readRound) {
	m.readRounds.Add(1)
	epoch, leading := m.leadingEpoch(p)
	if !leading {
		r.finish(epoch, errNotLeading)
		return
	}
	st, err := m.store.State(p)
	if err != nil {
		m.log.Error("read round: reading state failed", "partition", p, "error", err)
		r.finish(epoch, err)
		return
	}
	if st.Promised != epoch {
		m.stepDownFrom(p, epoch)
		r.finish(epoch, errNotLeading)
		return
	}

	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.OwnerTimeout)
	defer cancel()
	entries := []heartbeatEntry{{P: p, Epoch: epoch, Committed: st.Committed}}
	peers, replies := m.askHeartbeat(ctx, entries)

	need := majority(m.cfg.ClusterSize)
	acks := 1 // this node
wait:
	for answered := 0; acks < need && answered < peers; answered++ {
		select {
		case rep := <-replies:
			if rep.resp == nil || !rep.resp.OK {
				continue
			}
			if m.applyFences(entries, rep.resp) {
				r.finish(epoch, errNotLeading)
				return
			}
			if slices.Contains(rep.resp.Accepted, p) {
				acks++
			}
		case <-ctx.Done():
			break wait
		}
	}
	if acks >= need {
		r.finish(epoch, nil)
		return
	}
	if m.ctx.Err() != nil {
		r.finish(epoch, ErrUnavailable) // the Manager is stopping (which steps down by itself)
		return
	}
	m.log.Debug("read round: no majority confirmed the leadership", "partition", p, "epoch", epoch.String(), "acks", acks)
	m.stepDownFrom(p, epoch) // and relinquishes
	r.finish(epoch, errNotConfirmed)
}
