package partition

import (
	"fmt"
	"time"

	"github.com/paularlott/gossip"
)

// PartitionStatus is this node's view of one partition (Manager.PartitionStatus), for readiness checks and metrics.
type PartitionStatus struct {
	// Partition is the partition's number.
	Partition int
	// Owner is the owner this node believes in (itself while it leads), or the zero NodeID if it knows of no live one.
	Owner gossip.NodeID
	// Epoch is that owner's epoch (zero without an owner).
	Epoch Epoch
	// Leading says this node owns the partition.
	Leading bool
	// Committed is this node's committed position.
	Committed Pos
	// OwnerCommitted is the highest committed position an owner has reported to this node (its own committed position
	// while it leads).
	OwnerCommitted Pos
	// Lag is how long this node has been behind a committed position the owner reported (0: it is not behind). CanRead is
	// false once it reaches StaleAfter.
	Lag time.Duration
	// CatchingUp says a catch-up of the partition is running in the background.
	CatchingUp bool
	// Diverged says this node's committed data stayed ahead of what the owner reported for longer than three
	// OwnerTimeouts: it holds writes the owner does not have, which only lost data (a disk lost or wrongly restored, here
	// or on a majority elsewhere) can explain. Until it is restarted the node then takes no part in the partition beyond
	// following its owner: it serves no local reads of it, neither takes it over nor claims it, gives no promise and
	// acknowledges no write for it, is no catch-up source for it and leaves it out of its state answers, so that what it
	// holds reaches no other node. It is logged at error level; an operator should resync the node (start it with its
	// data marked lost).
	Diverged bool
	// CanRead is CanRead(Partition).
	CanRead bool
}

// Status is this node's view of the whole protocol (Manager.Status).
type Status struct {
	// Ready is Manager.Ready, Resyncing Manager.Resyncing and Eligible Manager.Eligible.
	Ready, Resyncing, Eligible bool
	// Divergences counts the partitions found diverged since Start.
	Divergences int64
	// Partitions is the status of every partition, by number.
	Partitions []PartitionStatus
}

// PartitionStatus returns this node's view of partition p: owner, epoch, positions, lag, catch-up, divergence and
// whether it may serve local reads. It reads no Store method but State, and that at most once per partition.
func (m *Manager) PartitionStatus(p int) (PartitionStatus, error) {
	ps := m.part(p)
	if ps == nil {
		return PartitionStatus{}, fmt.Errorf("partition: partition %d out of range [0, %d)", p, len(m.parts))
	}
	m.loadCache(p)
	resync := m.resync.Load()
	now := m.now()
	ps.vmu.RLock()
	defer ps.vmu.RUnlock()
	out := PartitionStatus{
		Partition:      p,
		Leading:        ps.leading,
		Committed:      ps.committed,
		OwnerCommitted: ps.ownerCommitted,
		CatchingUp:     ps.catchingUp.Load(),
		Diverged:       ps.diverged,
		CanRead:        !resync && m.canReadLocked(ps, now),
	}
	if ps.owner != gossip.EmptyNodeID && (ps.leading || now.Sub(ps.ownerSeen) < m.cfg.OwnerTimeout) {
		out.Owner, out.Epoch = ps.owner, ps.ownerEpoch
	}
	if ps.leading {
		out.OwnerCommitted = ps.committed
	} else if !ps.lagSince.IsZero() {
		out.Lag = now.Sub(ps.lagSince)
	}
	return out, nil
}

// Status returns this node's view of every partition, and of the node.
func (m *Manager) Status() Status {
	out := Status{Ready: m.Ready(), Resyncing: m.Resyncing(), Eligible: m.Eligible(), Divergences: m.divergences.Load(),
		Partitions: make([]PartitionStatus, len(m.parts))}
	for p := range m.parts {
		out.Partitions[p], _ = m.PartitionStatus(p)
	}
	return out
}

// divergeAfter is how long every report of the owner must show a committed position below this node's before the
// partition is marked diverged: long enough for reports that were overtaken by later commit notices to be superseded.
func (m *Manager) divergeAfter() time.Duration { return 3 * m.cfg.OwnerTimeout }

// diverged reports whether p has diverged at this node.
func (m *Manager) diverged(p int) bool {
	ps := m.parts[p]
	ps.vmu.RLock()
	defer ps.vmu.RUnlock()
	return ps.diverged
}

// loadCache fills p's cache of the durable state from the Store the first time it is needed, and reports whether the
// cache is loaded.
func (m *Manager) loadCache(p int) bool {
	ps := m.parts[p]
	ps.vmu.RLock()
	loaded := ps.cacheLoaded
	ps.vmu.RUnlock()
	if loaded {
		return true
	}
	st, err := m.store.State(p)
	if err != nil {
		m.log.Error("reading state failed", "partition", p, "error", err)
		return false
	}
	m.refresh(p, st)
	return true
}

// refresh raises p's cache to a State read from the Store.
func (m *Manager) refresh(p int, st State) {
	ps := m.parts[p]
	ps.vmu.Lock()
	if ps.promised.Less(st.Promised) {
		ps.promised = st.Promised
	}
	m.raiseCommittedLocked(ps, st.Committed)
	ps.cacheLoaded = true
	ps.vmu.Unlock()
}

// setPromised records p's promise durably (Store.SetPromised) and then raises the cache, before the caller answers
// anyone: a node's cached promise is never below a promise it has given.
// markEstablished records, durably and once, that this node is established (Config.Established).
func (m *Manager) markEstablished() error {
	if m.established.Load() {
		return nil
	}
	m.lifeMu.Lock()
	defer m.lifeMu.Unlock()
	if m.established.Load() {
		return nil
	}
	if m.cfg.SetEstablished != nil {
		if err := m.cfg.SetEstablished(); err != nil {
			return err
		}
	}
	m.established.Store(true)
	m.log.Info("established: this node takes part in the cluster's data set")
	return nil
}

// Established reports whether this node is established (see Config.Established).
func (m *Manager) Established() bool { return m.established.Load() }

func (m *Manager) setPromised(p int, e Epoch) error {
	// A node gives its first promise only once it is durably established (a resyncing node raising its promise to what
	// the others report is not taking part, and stays as it is).
	if !m.resync.Load() {
		if err := m.markEstablished(); err != nil {
			return err
		}
	}
	if err := m.store.SetPromised(p, e); err != nil {
		return err
	}
	ps := m.parts[p]
	ps.vmu.Lock()
	if ps.promised.Less(e) {
		ps.promised = e
	}
	ps.vmu.Unlock()
	return nil
}

// applied records that this node committed or imported p up to pos: it raises the cache, ends a lag that pos closes,
// and wakes the callers of WaitApplied.
func (m *Manager) applied(p int, pos Pos) {
	ps := m.parts[p]
	ps.vmu.Lock()
	m.raiseCommittedLocked(ps, pos)
	ps.vmu.Unlock()
	ps.notifyApplied()
}

// raiseCommittedLocked raises the cached committed position to pos and ends the lag it closes: if a later report is
// still ahead, the lag is measured from when that one arrived. The caller holds ps.vmu.
func (m *Manager) raiseCommittedLocked(ps *partState, pos Pos) {
	if pos.Seq > ps.committed.Seq {
		ps.committed = pos
	}
	if ps.lagSince.IsZero() || ps.committed.Seq < ps.lagTarget {
		return
	}
	if ps.committed.Seq < ps.ownerCommitted.Seq {
		ps.lagTarget, ps.lagSince = ps.ownerCommitted.Seq, ps.reportedAt
	} else {
		ps.lagTarget, ps.lagSince = 0, time.Time{}
	}
}

// noteReportLocked records an owner's report (a heartbeat entry, or a commit notice), accepted at now, that it has
// committed through oc: it starts measuring
// the lag if this node is behind it, and the time this node has been ahead of every report if it is ahead. It reports
// whether the report made the partition diverged. The caller holds ps.vmu and the cache is loaded.
func (m *Manager) noteReportLocked(ps *partState, oc Pos, heartbeat bool, now time.Time) bool {
	if oc.Seq > ps.ownerCommitted.Seq {
		ps.ownerCommitted, ps.reportedAt = oc, now
	}
	if heartbeat && oc.Seq > ps.hbCommitted.Seq {
		ps.hbCommitted = oc
	}
	if ps.committed.Seq < oc.Seq && ps.lagSince.IsZero() {
		ps.lagTarget, ps.lagSince = oc.Seq, now
	}
	switch {
	case ps.committed.Seq <= oc.Seq:
		ps.aheadSince = time.Time{}
	case ps.aheadSince.IsZero():
		ps.aheadSince = now
	case !ps.diverged && now.Sub(ps.aheadSince) >= m.divergeAfter():
		ps.diverged = true
		return true
	}
	return false
}

// divergedNow logs and counts a partition that has just been found diverged, and steps down from it.
func (m *Manager) divergedNow(p int, owner gossip.NodeID, oc Pos) {
	m.divergences.Add(1)
	ps := m.parts[p]
	ps.vmu.RLock()
	committed := ps.committed
	ps.vmu.RUnlock()
	m.log.Error("partition diverged: this node has committed writes its owner does not have; it no longer serves local reads of the partition or leads it (resync this node)",
		"partition", p, "owner", owner.String(), "owner_committed", oc.String(), "committed", committed.String(),
		"for", m.divergeAfter())
	m.stepDown(p)
}
