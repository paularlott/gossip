package partition

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/paularlott/gossip"
)

// maxHeaderFrame bounds the header frame of a catch-up stream, so a corrupt length cannot cause a huge allocation.
const maxHeaderFrame = 64 * 1024

// A catch-up stream is a header frame, a 4 byte big-endian length followed by that many bytes of JSON (a
// catchUpHeader), and then the body, the bytes Store.Export returned. The header is JSON rather than the cluster codec
// because the library exposes that codec only on a received packet, which a node that is catching up does not have (the
// stream itself is framed, and encrypted, by the library).

func writeCatchUpHeader(w io.Writer, h catchUpHeader) error {
	b, err := json.Marshal(h)
	if err != nil {
		return err
	}
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	if _, err := w.Write(l[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func readCatchUpHeader(r io.Reader) (catchUpHeader, error) {
	var h catchUpHeader
	var l [4]byte
	if _, err := io.ReadFull(r, l[:]); err != nil {
		return h, fmt.Errorf("partition: reading catch-up header: %w", err)
	}
	n := binary.BigEndian.Uint32(l[:])
	if n == 0 || n > maxHeaderFrame {
		return h, fmt.Errorf("partition: catch-up header of %d bytes", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return h, fmt.Errorf("partition: reading catch-up header: %w", err)
	}
	if err := json.Unmarshal(b, &h); err != nil {
		return h, fmt.Errorf("partition: decoding catch-up header: %w", err)
	}
	return h, nil
}

// serveCatchUp is the source side of a catch-up: it writes the header frame {UpTo, Err} and then the Store's export.
//
// Committed data is identical on every node that has it (a Seq is committed at most once, as one write), so any node
// is a safe source up to its own commit point and catch-up needs no fencing. Pending writes are never exported. It
// does not take the partition's mutex: the Store's Export is one consistent snapshot, and the stream may be long.
func (m *Manager) serveCatchUp(req *catchUpReq, w io.Writer) error {
	fail := func(msg string) error { return writeCatchUpHeader(w, catchUpHeader{Err: msg}) }
	switch {
	case !m.compatible(req.Size, req.Cluster):
		return fail(ErrSizeMismatch.Error())
	case m.part(req.P) == nil:
		return fail(fmt.Sprintf("partition %d out of range", req.P))
	case m.isStopped():
		return fail("partition manager stopped")
	case m.diverged(req.P):
		return fail("partition diverged on this node")
	}
	after := req.After
	if req.Full {
		after = 0
	}
	upTo, r, err := m.store.Export(req.P, after)
	if err != nil {
		return fail(err.Error())
	}
	defer r.Close()
	m.observe(req.P, upTo.Epoch)
	if err := writeCatchUpHeader(w, catchUpHeader{UpTo: upTo}); err != nil {
		return err
	}
	_, err = io.Copy(w, r)
	return err
}

// catchUp brings p up to what node from has committed. It asks for the changes after this node's committed sequence;
// if from has purged the history that needs, it asks again for everything, and so it does at once if the partition is
// Incomplete here. held says whether the caller already holds the partition's mutex (a takeover does); otherwise it is
// taken to apply the import.
func (m *Manager) catchUp(ctx context.Context, p int, from gossip.NodeID, held bool) (err error) {
	st, err := m.store.State(p)
	if err != nil {
		return err
	}
	req := &catchUpReq{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID, P: p, After: st.Committed.Seq, Full: st.Incomplete}
	for {
		rc, err := m.net.catchUp(ctx, from, req)
		if err != nil {
			return err
		}
		hdr, err := readCatchUpHeader(rc)
		switch {
		case err != nil:
			rc.Close()
			return err
		case hdr.Err == ErrPurged.Error() && !req.Full:
			rc.Close()
			req = &catchUpReq{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID, P: p, Full: true}
			continue
		case hdr.Err != "":
			rc.Close()
			return fmt.Errorf("partition: catch-up of %d from %s refused: %s", p, from, hdr.Err)
		}
		defer rc.Close()
		return m.applyCatchUp(ctx, p, rc, hdr, req.Full, held)
	}
}

// applyCatchUp imports a catch-up body if it is still ahead of what this node has committed.
func (m *Manager) applyCatchUp(ctx context.Context, p int, body io.Reader, hdr catchUpHeader, full, held bool) error {
	ps := m.parts[p]
	if !held {
		if err := ps.mu.lock(ctx); err != nil {
			return err
		}
		defer ps.mu.unlock()
	}
	m.observe(p, hdr.UpTo.Epoch)
	st, err := m.store.State(p)
	if err != nil {
		return err
	}
	if hdr.UpTo.Seq <= st.Committed.Seq {
		return nil // already there: the body is discarded
	}
	if err := m.store.Import(p, body, hdr.UpTo, full); err != nil {
		return fmt.Errorf("partition: importing %d: %w", p, err)
	}
	m.applied(p, hdr.UpTo)
	// The body must have ended cleanly: a stream that broke after the Store stopped reading is not a complete reply.
	if _, err := io.Copy(io.Discard, body); err != nil {
		m.log.Error("catch-up stream ended badly after the import", "partition", p, "error", err)
	}
	m.log.Debug("caught up", "partition", p, "to", hdr.UpTo.String(), "full", full)
	return nil
}

// catchUpAsync starts a catch-up of p from node `from` in the background. At most one runs per partition: a request
// while one is in flight is dropped (a later heartbeat or write asks again if it is still needed). At most
// MaxConcurrentCatchUps run at once on the node; the others wait for a slot.
func (m *Manager) catchUpAsync(p int, from gossip.NodeID) {
	ps := m.part(p)
	if ps == nil {
		return
	}
	m.spawnOnce(&ps.catchingUp, func() {
		release, ok := m.catchUpSlot(m.ctx)
		if !ok {
			return
		}
		err := m.catchUp(m.ctx, p, from, false)
		release()
		m.noteCatchUp(p, from, err)
	})
}

// catchUpSlot waits for one of the node's MaxConcurrentCatchUps background catch-up slots, until ctx ends or the Manager
// stops; it returns the function that frees it.
func (m *Manager) catchUpSlot(ctx context.Context) (func(), bool) {
	select {
	case m.catchUpSem <- struct{}{}:
		return func() { <-m.catchUpSem }, true
	case <-ctx.Done():
	case <-m.ctx.Done():
	}
	return nil, false
}

// catchUpWarnEvery is how often repeated failures of a partition's background catch-up are logged at warn level.
const catchUpWarnEvery = 10 * time.Second

// noteCatchUp records the outcome of a background catch-up of p. One failure is logged at debug level (the next
// heartbeat asks again); failures in a row are logged at warn level, at most once per catchUpWarnEvery per partition,
// since they keep the node behind (CanRead turns false once it has trailed the owner for StaleAfter).
func (m *Manager) noteCatchUp(p int, from gossip.NodeID, err error) {
	ps := m.parts[p]
	now := m.now()
	ps.vmu.Lock()
	if err == nil {
		ps.catchUpFailures = 0
		ps.vmu.Unlock()
		return
	}
	ps.catchUpFailures++
	n := ps.catchUpFailures
	warn := n > 1 && now.Sub(ps.catchUpWarned) >= catchUpWarnEvery
	if warn {
		ps.catchUpWarned = now
	}
	ps.vmu.Unlock()
	if m.ctx.Err() != nil {
		return // stopping
	}
	if warn {
		m.log.Warn("catch-up keeps failing", "partition", p, "from", from.String(), "failures", n, "error", err)
	} else {
		m.log.Debug("catch-up failed", "partition", p, "from", from.String(), "error", err)
	}
}
