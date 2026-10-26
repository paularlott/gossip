package partition

import (
	"context"
	"time"
)

// heartbeatLoop announces this node's partitions every HeartbeatInterval.
func (m *Manager) heartbeatLoop() {
	t := time.NewTicker(m.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.sendHeartbeats()
		case <-m.ctx.Done():
			return
		}
	}
}

// askHeartbeat sends one PartHeartbeatMsg with entries to every other alive node, as askAll does.
func (m *Manager) askHeartbeat(ctx context.Context, entries []heartbeatEntry) (int, <-chan reply[heartbeatResp]) {
	return askAll[heartbeatResp](ctx, m.net, partHeartbeatMsg,
		&heartbeatMsg{Size: m.cfg.ClusterSize, Cluster: m.cfg.ClusterID, From: m.self, Entries: entries})
}

// applyFences ends the tenures among entries that a heartbeat reply shows outranked: a node holds a promise above the
// epoch, so this node steps down, as a write's reply would make it. It reports whether any tenure was outranked.
func (m *Manager) applyFences(entries []heartbeatEntry, resp *heartbeatResp) bool {
	outranked := false
	for _, f := range resp.Fenced {
		m.observe(f.P, f.Promised)
		for _, e := range entries {
			if e.P == f.P && e.Epoch.Less(f.Promised) {
				m.stepDownFrom(e.P, e.Epoch)
				outranked = true
			}
		}
	}
	return outranked
}

// sendHeartbeats sends every other alive node one PartHeartbeatMsg listing the partitions this node leads, with its
// epoch and committed position. A round whose sends were delivered to at least Majority-1 others counts as majority
// contact for every partition it listed (delivery is counted from the replies; a failed send is a failure). The
// round does not wait for slow nodes: they must not delay the heartbeats of the healthy ones.
func (m *Manager) sendHeartbeats() {
	var entries []heartbeatEntry
	for p := range m.parts {
		epoch, leading := m.leadingEpoch(p)
		if !leading {
			continue
		}
		// Read without the partition's mutex, which a replication may hold for a while: committed only moves forward,
		// so a stale value is safe.
		st, err := m.store.State(p)
		if err != nil {
			continue
		}
		entries = append(entries, heartbeatEntry{P: p, Epoch: epoch, Committed: st.Committed})
	}
	if len(entries) == 0 {
		return
	}

	start := m.now()
	need := majority(m.cfg.ClusterSize) - 1
	record := func() {
		for _, e := range entries {
			m.touchMajority(e.P, e.Epoch, start)
		}
	}
	if need <= 0 {
		record()
	}

	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.OwnerTimeout)
	peers, replies := m.askHeartbeat(ctx, entries)
	if !m.spawn(func() {
		defer cancel()
		delivered := 0
		for range peers {
			rep := <-replies
			if rep.resp == nil || !rep.resp.OK {
				continue
			}
			m.applyFences(entries, rep.resp)
			if delivered++; delivered == need {
				record()
			}
		}
	}) {
		cancel()
	}
}
