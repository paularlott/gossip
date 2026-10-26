package partition

// Unit tests of the production-readiness hardening: deadlines carried by forwarded requests, replication on its own
// timeout, relinquish on every step-down, handlers that never wait for a partition's mutex, intact-only resync, the
// follower-ahead (divergence) alarm, per-partition staleness and status, claims only when caught up, load isolation,
// the refusal's position, eligibility, takeover settling before reads, and a read-only Store.State.

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/gossip"
)

// leaderOf is leader with a configuration tweak: node i leads partition 1 (taken over by hand, no loops running).
func leaderOf(t *testing.T, i int, tweak func(i int, c *Config)) *fakeCluster {
	t.Helper()
	fc := newFakeCluster(t, 3, 3, tweak)
	fc.attach()
	if err := fc.nodes[i].m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	fc.nodes[i].m.wg.Wait()
	return fc
}

// fresh refreshes the owner's majority contact, so that runChange accepts changes in a test that runs no heartbeats.
func fresh(fn *fakeNode, p int) {
	e, _ := fn.m.leadingEpoch(p)
	fn.m.touchMajority(p, e, fn.m.now())
}

// --- 1. deadlines -----------------------------------------------------------------------------------------------------

// A forwarded change or read whose requester's deadline has passed, or is closer than the floor, is refused with nothing
// done once the owner holds the partition; with time left it runs.
func TestForwardedRequestRefusedNearItsDeadline(t *testing.T) {
	fc := leaderOf(t, 0, nil)
	owner := fc.nodes[0]
	floor := owner.m.deadlineFloor()
	if floor != 2*owner.m.cfg.HeartbeatInterval {
		t.Fatalf("floor = %v, want 2 x HeartbeatInterval", floor)
	}
	calls := len(owner.store.callLog())
	for _, dl := range []time.Time{time.Now().Add(-time.Second), time.Now().Add(floor / 2)} {
		fresh(owner, 1)
		if r := owner.m.handleChange(t.Context(), &changeReq{Size: 3, P: 1, Change: []byte("x"), Deadline: dl.UnixNano()}); r.Status != statusUnavailable {
			t.Errorf("change with %v left: %+v", time.Until(dl).Round(time.Millisecond), r)
		}
		if r := owner.m.handleRead(t.Context(), &readReq{Size: 3, P: 1, Req: []byte("data"), Deadline: dl.UnixNano()}); r.Status != statusUnavailable {
			t.Errorf("read with %v left: %+v", time.Until(dl).Round(time.Millisecond), r)
		}
	}
	if got := owner.store.callLog()[calls:]; len(got) != 0 {
		t.Errorf("a refused change touched the store: %v", got)
	}
	if st := owner.store.state(1); st.Committed.Seq != 0 || st.Pending != nil {
		t.Errorf("state after refusals: %+v", st)
	}
	fresh(owner, 1)
	dl := time.Now().Add(time.Second).UnixNano()
	if r := owner.m.handleChange(t.Context(), &changeReq{Size: 3, P: 1, Change: []byte("x"), Deadline: dl}); r.Status != statusDone {
		t.Errorf("change with time left: %+v", r)
	}
	if r := owner.m.handleRead(t.Context(), &readReq{Size: 3, P: 1, Req: []byte("data"), Deadline: dl}); r.Status != statusDone {
		t.Errorf("read with time left: %+v", r)
	}

	// End to end: a Write through another node carries its context's deadline; one too short for the owner to start is
	// refused (ErrUnavailable: nothing written), never left to commit after the caller has gone.
	other := fc.nodes[1]
	e, _ := owner.m.leadingEpoch(1)
	other.m.setOwner(1, owner.id, e)
	fresh(owner, 1)
	ctx, cancel := context.WithTimeout(t.Context(), floor/2)
	defer cancel()
	if _, err := other.m.Write(ctx, 1, []byte("late")); !errors.Is(err, ErrUnavailable) {
		t.Errorf("a write too close to its deadline: %v, want ErrUnavailable", err)
	}
	if d := owner.store.data(1); slices.Contains(d, "late") {
		t.Errorf("the refused write was committed: %v", d)
	}
}

// A replication runs on its own WriteTimeout from its start, not on what is left of its caller's: followers that answer
// after the caller's deadline still get the write committed, and the owner stays.
func TestReplicationRunsOnItsOwnTimeout(t *testing.T) {
	fc := leaderOf(t, 0, nil)
	owner := fc.nodes[0]
	for _, f := range fc.nodes[1:] {
		f.store.onSetPending = func(int, *Write) { time.Sleep(150 * time.Millisecond) }
	}
	fresh(owner, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if r := owner.m.runChange(ctx, 1, []byte("slow"), false); r.Status != statusDone {
		t.Fatalf("a write whose followers answered after its caller's deadline: %+v", r)
	}
	if !owner.m.IsOwner(1) {
		t.Error("the owner stepped down")
	}
	if d := owner.store.data(1); !slices.Equal(d, []string{"slow"}) {
		t.Errorf("data = %v", d)
	}
}

// Every step-down of a leading owner tells the others, so that their stickiness to it lapses at once: after a failed
// replication, a fence in a heartbeat reply and a Store failure.
func TestEveryStepDownRelinquishes(t *testing.T) {
	follow := func(fc *fakeCluster) {
		owner := fc.nodes[0]
		e, _ := owner.m.leadingEpoch(1)
		for _, f := range fc.nodes[1:] {
			f.m.setOwner(1, owner.id, e)
		}
	}
	lapsed := func(t *testing.T, fc *fakeCluster, why string) {
		t.Helper()
		waitFor(t, time.Second, "the owner to step down ("+why+")", func() bool { return !fc.nodes[0].m.IsOwner(1) })
		for _, f := range fc.nodes[1:] {
			if o, ok := f.m.Owner(1); ok {
				t.Errorf("%s: node %s still believes in owner %s", why, f.id, o)
			}
		}
	}
	t.Run("failed replication", func(t *testing.T) {
		fc := leaderOf(t, 0, nil)
		follow(fc)
		for _, f := range fc.nodes[1:] {
			f.store.failOnce("SetPending", errors.New("disk"))
		}
		fresh(fc.nodes[0], 1)
		if r := fc.nodes[0].m.runChange(t.Context(), 1, []byte("x"), false); r.Status != statusUnknown {
			t.Errorf("%+v", r)
		}
		lapsed(t, fc, "failed replication")
	})
	t.Run("fence", func(t *testing.T) {
		fc := leaderOf(t, 0, nil)
		follow(fc)
		if err := fc.nodes[2].m.setPromised(1, ep(100, 3)); err != nil {
			t.Fatal(err)
		}
		fc.nodes[0].m.sendHeartbeats()
		lapsed(t, fc, "fence")
	})
	t.Run("store failure", func(t *testing.T) {
		fc := leaderOf(t, 0, nil)
		follow(fc)
		fresh(fc.nodes[0], 1)
		fc.nodes[0].store.failOnce("State", errors.New("disk"))
		if r := fc.nodes[0].m.runChange(t.Context(), 1, []byte("x"), false); r.Status != statusUnavailable {
			t.Errorf("%+v", r)
		}
		lapsed(t, fc, "store failure")
	})
}

func TestMinTransportDeadline(t *testing.T) {
	fc := newFakeCluster(t, 1, 1, func(i int, c *Config) { c.WriteTimeout = 4 * time.Second })
	m := fc.nodes[0].m
	if got := m.MinTransportDeadline(); got <= m.cfg.WriteTimeout {
		t.Errorf("MinTransportDeadline %v does not exceed WriteTimeout %v", got, m.cfg.WriteTimeout)
	}
}

// --- 2. no handler waits for a mutex ----------------------------------------------------------------------------------

// A commit notice and a heartbeat are decided at once while their partitions are busy; the settling waits for the
// partition, and then brings it to the latest of the notices, however they overtook each other.
func TestEntriesNeverWaitForTheMutex(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	f, owner := fc.nodes[0], fc.nodes[1]
	e1 := ep(1, 2)
	owner.store.seedCommitted(1, 6, e1)
	f.store.seedCommitted(1, 3, e1)
	f.store.seedPending(1, Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("d4")})
	var rels []func()
	for p := 0; p < f.m.cfg.Partitions; p++ {
		rels = append(rels, holdPartition(t, f, p))
	}

	start := time.Now()
	for _, seq := range []uint64{4, 6, 5} { // notices overtake each other
		f.m.handleCommit(owner.id, &commitMsg{Size: 3, P: 1, Epoch: e1, Committed: Pos{Epoch: e1, Seq: seq}})
	}
	var entries []heartbeatEntry
	for p := 0; p < f.m.cfg.Partitions; p++ {
		if p != 1 {
			entries = append(entries, heartbeatEntry{P: p, Epoch: e1})
		}
	}
	r := f.m.handleHeartbeat(owner.id, &heartbeatMsg{Size: 3, Entries: entries})
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Errorf("handling the notices and the heartbeat waited %v for busy partitions", d)
	}
	if len(r.Accepted) != len(entries) {
		t.Errorf("accepted %v of %d busy partitions", r.Accepted, len(entries))
	}
	// (Which notices the partition's goroutine applies, and in which order, depends on when it starts: an older one after
	// a newer one decides nothing new. What counts is the outcome below.)
	if st := f.store.state(1); st.Committed.Seq != 3 {
		t.Errorf("settled while the partition was held: %+v", st)
	}
	for _, rel := range rels {
		rel()
	}
	waitFor(t, 5*time.Second, "the follower to settle and catch up to seq 6", func() bool { return f.store.state(1).Committed.Seq == 6 })
	if d := f.store.data(1); d[3] != "d4" {
		t.Errorf("data = %v", d)
	}
}

// --- 3. intact-only resync and divergence ----------------------------------------------------------------------------

// A node answers a state request with whether it is intact.
func TestStateAnswerSaysWhetherIntact(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m := fc.nodes[0].m
	if r := m.handleState(&stateReq{Size: 3}); !r.Intact {
		t.Errorf("an intact node: %+v", r)
	}
	m.resync.Store(true)
	if r := m.handleState(&stateReq{Size: 3}); r.Intact {
		t.Errorf("a resyncing node: %+v", r)
	}
}

// Only intact answers count toward the resync quorum and are used; ForceResync needs at least one of them.
func TestResyncCountsOnlyIntactAnswers(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	intact, other, v := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	intact.store.seedCommitted(1, 2, ep(1, 1))
	other.store.seedCommitted(1, 5, ep(1, 1)) // what a node that is resyncing reports proves nothing
	other.m.resync.Store(true)
	v.store.intact = false
	v.m.resync.Store(true)
	if v.m.syncOnce() {
		t.Fatal("resynced with one intact answer and one from a resyncing node")
	}
	v.m.cfg.ForceResync = true
	if !v.m.syncOnce() {
		t.Fatal("a forced resync with an intact node answering did not complete")
	}
	if st := v.store.state(1); st.Committed.Seq != 2 {
		t.Errorf("committed %s, want the intact node's 2 (not the resyncing node's 5)", st.Committed)
	}

	// Forced, with only nodes that are resyncing themselves: never.
	fc2 := newFakeCluster(t, 3, 3, nil)
	fc2.attach()
	for _, n := range fc2.nodes {
		n.store.intact = false
		n.m.resync.Store(true)
	}
	fc2.nodes[0].m.cfg.ForceResync = true
	if fc2.nodes[0].m.syncOnce() {
		t.Error("a forced resync completed from nodes that are all resyncing")
	}
	// An intact node at startup does not count resyncing nodes toward its majority either.
	fc2.nodes[1].m.resync.Store(false)
	if fc2.nodes[1].m.syncOnce() {
		t.Error("an intact node completed its startup sync with only resyncing nodes answering")
	}
}

// A follower whose committed data stays ahead of what its owner reports has diverged: it stops serving reads of the
// partition, is no candidate for it, and gives it no promise, no acknowledgement, no catch-up and no state. A follower
// that is ahead for a moment only (reports overtaken by commit notices) has not.
func TestFollowerAheadIsMarkedDiverged(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	f := fc.nodes[0]
	advance := setClock(f.m)
	e1 := ep(1, 2)
	f.store.seedCommitted(1, 5, e1)
	hb := func(seq uint64) {
		f.m.handleHeartbeat(nid(2), &heartbeatMsg{Size: 3, Entries: []heartbeatEntry{{P: 1, Epoch: e1, Committed: Pos{Epoch: e1, Seq: seq}}}})
	}
	step := f.m.cfg.OwnerTimeout / 2

	// Ahead for a while, then a report catches up: not diverged.
	hb(3)
	advance(2 * f.m.cfg.OwnerTimeout)
	hb(5)
	advance(2 * f.m.cfg.OwnerTimeout)
	hb(5)
	if st, _ := f.m.PartitionStatus(1); st.Diverged || !st.CanRead {
		t.Fatalf("a follower ahead for a moment only: %+v", st)
	}

	// Ahead of every report for divergeAfter: diverged.
	f.store.seedCommittedAt(1, 6, e1)
	f.store.seedCommittedAt(1, 7, e1)
	f.m.refresh(1, f.store.state(1))
	for elapsed := time.Duration(0); elapsed <= f.m.divergeAfter(); elapsed += step {
		hb(5)
		advance(step)
	}
	hb(5)
	st, err := f.m.PartitionStatus(1)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Diverged || st.CanRead || st.Committed.Seq != 7 || st.OwnerCommitted.Seq != 5 {
		t.Fatalf("status: %+v", st)
	}
	if f.m.CanRead(1) || f.m.Status().Divergences != 1 {
		t.Errorf("CanRead %v, divergences %d", f.m.CanRead(1), f.m.Status().Divergences)
	}
	f.m.ready.Store(true)
	cands := []gossip.NodeID{nid(1), nid(2), nid(3)}
	if _, ok := f.m.claimDue(1, cands); ok {
		t.Error("a diverged node claims the partition")
	}
	advance(10 * f.m.cfg.OwnerTimeout) // the owner is gone
	if f.m.takeoverDue(1, cands) {
		t.Error("a diverged node takes the partition over")
	}
	if r := f.m.handlePromise(nid(3), &promiseReq{Size: 3, P: 1, Epoch: ep(9, 3)}); r.OK {
		t.Errorf("a diverged node promised: %+v", r)
	}
	w := Write{Partition: 1, Pos: Pos{Epoch: ep(9, 3), Seq: 7}, Prev: Pos{Epoch: ep(9, 3), Seq: 6}}
	if r := f.m.handleWrite(nid(3), &writeReq{Size: 3, Write: w, OwnerCommitted: Pos{Seq: 6}}); r.OK {
		t.Errorf("a diverged node acknowledged a write: %+v", r)
	}
	if r := f.m.handleState(&stateReq{Size: 3}); r.Committed[1].Seq != 0 || (r.Pending != nil && r.Pending[1] != nil) {
		t.Errorf("a diverged node reported the partition: %+v", r)
	}
	var b strings.Builder
	if err := f.m.serveCatchUp(&catchUpReq{Size: 3, P: 1}, &b); err != nil {
		t.Fatal(err)
	}
	if hdr, err := readCatchUpHeader(strings.NewReader(b.String())); err != nil || hdr.Err == "" {
		t.Errorf("a diverged node served a catch-up: %+v %v", hdr, err)
	}
	// Other partitions are not affected.
	if r := f.m.handlePromise(nid(3), &promiseReq{Size: 3, P: 2, Epoch: ep(9, 3)}); !r.OK {
		t.Errorf("another partition: %+v", r)
	}
}

// --- 4. per-partition staleness and status ----------------------------------------------------------------------------

// A follower that hears its owner but cannot catch up to the committed position it reports stops serving reads once it
// has trailed it for StaleAfter, and serves again once it has caught up.
func TestFollowerTrailingTheOwnerStopsReading(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	f, owner := fc.nodes[0], fc.nodes[1]
	f.mgr.Store(f.m) // only the follower answers: its catch-ups from the owner fail
	e1 := ep(1, 2)
	owner.store.seedCommitted(1, 6, e1)
	f.store.seedCommitted(1, 3, e1)
	hb := func() {
		f.m.handleHeartbeat(owner.id, &heartbeatMsg{Size: 3, Entries: []heartbeatEntry{{P: 1, Epoch: e1, Committed: Pos{Epoch: e1, Seq: 6}}}})
	}
	hb()
	if !f.m.CanRead(1) {
		t.Fatal("a follower that has just heard its owner cannot read")
	}
	deadline := time.Now().Add(f.m.cfg.StaleAfter + 100*time.Millisecond)
	for time.Now().Before(deadline) {
		hb()
		time.Sleep(10 * time.Millisecond)
	}
	if f.m.CanRead(1) {
		t.Error("a follower that trailed its owner for StaleAfter can read")
	}
	st, _ := f.m.PartitionStatus(1)
	if st.Lag < f.m.cfg.StaleAfter || st.Owner != owner.id || st.OwnerCommitted.Seq != 6 || st.Committed.Seq != 3 || st.CanRead {
		t.Errorf("status: %+v", st)
	}
	// Still behind: the catch-ups failed, nothing was imported.
	if f.store.state(1).Committed.Seq != 3 {
		t.Errorf("the follower's committed position moved to %d while its catch-ups fail", f.store.state(1).Committed.Seq)
	}

	owner.mgr.Store(owner.m) // the owner answers again
	waitFor(t, 5*time.Second, "the follower to catch up", func() bool {
		hb()
		return f.store.state(1).Committed.Seq == 6
	})
	if !f.m.CanRead(1) {
		t.Error("a follower that caught up cannot read")
	}
	if st, _ := f.m.PartitionStatus(1); st.Lag != 0 {
		t.Errorf("lag after catching up: %+v", st)
	}
	if _, err := f.m.PartitionStatus(99); err == nil {
		t.Error("status of a partition out of range")
	}
	if s := f.m.Status(); len(s.Partitions) != f.m.cfg.Partitions || s.Partitions[1].Committed.Seq != 6 || !s.Eligible {
		t.Errorf("Status: %+v", s)
	}
}

// --- 5. claims only when caught up -------------------------------------------------------------------------------------

func TestClaimOnlyWhenCaughtUp(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	n := fc.nodes[0]
	n.m.ready.Store(true)
	cands := []gossip.NodeID{nid(1), nid(2), nid(3)}
	p := -1
	for q := 0; q < n.m.cfg.Partitions; q++ {
		if o, _ := rendezvousOwner(q, cands); o == n.id {
			p = q
			break
		}
	}
	if p < 0 {
		t.Skip("node 1 is rank 0 for no partition")
	}
	e := ep(1, 2)
	n.m.catchingUp(p, true) // no catch-up starts
	n.m.handleHeartbeat(nid(2), &heartbeatMsg{Size: 3, Entries: []heartbeatEntry{{P: p, Epoch: e, Committed: Pos{Epoch: e, Seq: 5}}}})
	if _, ok := n.m.claimDue(p, cands); ok {
		t.Error("claims while behind the owner")
	}
	n.store.seedCommitted(p, 5, e)
	n.m.refresh(p, n.store.state(p))
	if _, ok := n.m.claimDue(p, cands); ok {
		t.Error("claims while a catch-up is running")
	}
	n.m.catchingUp(p, false)
	if o, ok := n.m.claimDue(p, cands); !ok || o != nid(2) {
		t.Errorf("a caught-up rank 0 does not claim: %v %v", o, ok)
	}
	// A commit notice beyond it (which a node under load is always a moment behind) does not stop the claim; a
	// heartbeat does.
	n.m.handleCommit(nid(2), &commitMsg{Size: 3, P: p, Epoch: e, Committed: Pos{Epoch: e, Seq: 6}})
	if _, ok := n.m.claimDue(p, cands); !ok {
		t.Error("a commit notice stopped the claim")
	}
}

// --- 6. load isolation -------------------------------------------------------------------------------------------------

// One partition's forwarded changes cannot take more than MaxChangesPerPartition: the next is answered busy, while
// another partition's change runs.
func TestPerPartitionChangeLimit(t *testing.T) {
	fc := leaderOf(t, 0, func(i int, c *Config) { c.Tuning.MaxChangesPerPartition = 1 })
	owner := fc.nodes[0]
	if err := owner.m.takeover(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	fresh(owner, 1)
	fresh(owner, 2)
	rel := holdPartition(t, owner, 1)
	dl := time.Now().Add(time.Second).UnixNano()
	first := make(chan *changeResp, 1)
	go func() {
		first <- owner.m.handleChange(t.Context(), &changeReq{Size: 3, P: 1, Change: []byte("a"), Deadline: dl})
	}()
	waitFor(t, time.Second, "the first change to wait for the partition", func() bool { return owner.m.parts[1].changes.Load() == 1 })
	if r := owner.m.handleChange(t.Context(), &changeReq{Size: 3, P: 1, Change: []byte("b"), Deadline: dl}); r.Status != statusBusy {
		t.Errorf("a second change of the busy partition: %+v", r)
	}
	if r := owner.m.handleChange(t.Context(), &changeReq{Size: 3, P: 2, Change: []byte("c"), Deadline: dl}); r.Status != statusDone {
		t.Errorf("a change of another partition: %+v", r)
	}
	fresh(owner, 1)
	rel()
	if r := <-first; r.Status != statusDone {
		t.Errorf("the first change: %+v", r)
	}
}

// A landing node backs off exponentially, with jitter, while the owner is busy: tens of retries in 300ms, not a hundred.
func TestBusyBackoffGrows(t *testing.T) {
	fc := leaderOf(t, 0, func(i int, c *Config) { c.OwnerTimeout = 2 * time.Second; c.Tuning.MaxConcurrentChanges = 1 })
	owner, other := fc.nodes[0], fc.nodes[1]
	owner.m.changeSem <- struct{}{} // the owner is at its limit
	e, _ := owner.m.leadingEpoch(1)
	other.m.setOwner(1, owner.id, e)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, err := other.m.Write(ctx, 1, []byte("x")); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Write to a busy owner: %v", err)
	}
	if n := other.busyReplies.Load(); n < 3 || n > 30 {
		t.Errorf("%d busy answers in 300ms, want about a dozen (1ms doubling to 50ms)", n)
	}
}

// Background catch-ups share the node's MaxConcurrentCatchUps slots.
func TestCatchUpsShareANodeWideLimit(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.Tuning.MaxConcurrentCatchUps = 1 })
	fc.attach()
	f, src := fc.nodes[0], fc.nodes[1]
	src.store.seedCommitted(1, 5, ep(1, 2))
	f.m.catchUpSem <- struct{}{} // the one slot is taken
	f.m.catchUpAsync(1, src.id)
	time.Sleep(50 * time.Millisecond)
	if st := f.store.state(1); st.Committed.Seq != 0 {
		t.Fatalf("caught up without a slot: %+v", st)
	}
	if st, _ := f.m.PartitionStatus(1); !st.CatchingUp {
		t.Error("the waiting catch-up is not reported")
	}
	<-f.m.catchUpSem
	waitFor(t, 5*time.Second, "the catch-up to run once a slot is free", func() bool { return f.store.state(1).Committed.Seq == 5 })
}

// --- 7. refusals carry the owner's position ---------------------------------------------------------------------------

func TestRefusalCarriesTheOwnersPosition(t *testing.T) {
	fc := leaderOf(t, 0, nil)
	owner, other := fc.nodes[0], fc.nodes[1]
	for _, c := range []string{"a", "b"} {
		fresh(owner, 1)
		if r := owner.m.runChange(t.Context(), 1, []byte(c), false); r.Status != statusDone {
			t.Fatal(r)
		}
	}
	want := owner.store.state(1).Committed
	var re *RefusedError
	fresh(owner, 1)
	if _, err := owner.m.Write(t.Context(), 1, []byte("conflict")); !errors.As(err, &re) || re.Pos != want || re.Code != "conflict" {
		t.Errorf("on the owner: %v %+v, want Pos %s", err, re, want)
	}
	e, _ := owner.m.leadingEpoch(1)
	other.m.setOwner(1, owner.id, e)
	fresh(owner, 1)
	re = nil
	if _, err := other.m.Write(t.Context(), 1, []byte("conflict")); !errors.As(err, &re) || re.Pos != want {
		t.Fatalf("through another node: %v %+v, want Pos %s", err, re, want)
	}
	// The pattern: wait for the local copy to reach the refusal's position before re-reading it.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := other.m.WaitApplied(ctx, 1, re.Pos); err != nil {
		t.Fatal(err)
	}
	if d := other.store.data(1); !slices.Equal(d, []string{"a", "b"}) {
		t.Errorf("the local copy after WaitApplied(refusal.Pos): %v", d)
	}
	re = nil
	fresh(owner, 1)
	if _, err := owner.m.Read(t.Context(), 1, []byte("conflict")); !errors.As(err, &re) || re.Pos != want {
		t.Errorf("a refused read: %v %+v", err, re)
	}
}

// --- 8. eligibility ----------------------------------------------------------------------------------------------------

// An ineligible node steps down from what it leads (telling the others), is nobody's candidate, takes over and claims
// nothing, and still follows: writes through it commit and it applies them. Eligible again, it leads again.
func TestIneligibleNodeOnlyFollows(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	var n *fakeNode
	for _, fn := range fc.nodes {
		for p := 0; p < fn.m.cfg.Partitions; p++ {
			if fn.m.IsOwner(p) {
				n = fn
			}
		}
	}
	n.m.SetEligible(false)
	for p := 0; p < n.m.cfg.Partitions; p++ {
		if n.m.IsOwner(p) {
			t.Fatalf("still leads %d", p)
		}
	}
	if n.ready.Load() || n.m.Eligible() || !n.m.Ready() {
		t.Errorf("published ready %v, eligible %v, ready %v", n.ready.Load(), n.m.Eligible(), n.m.Ready())
	}
	fc.waitOwners(10 * time.Second)
	for p := 0; p < n.m.cfg.Partitions; p++ {
		writeRetry(t, n, p, "through the ineligible node")
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		for p := 0; p < n.m.cfg.Partitions; p++ {
			if n.m.IsOwner(p) {
				t.Fatalf("an ineligible node took %d", p)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	for p := 0; p < n.m.cfg.Partitions; p++ {
		waitFor(t, 5*time.Second, "the ineligible node to apply the writes and read", func() bool {
			return slices.Contains(n.store.data(p), "through the ineligible node") && n.m.CanRead(p)
		})
	}
	n.m.SetEligible(true)
	waitFor(t, 10*time.Second, "the node to lead again", func() bool {
		for p := 0; p < n.m.cfg.Partitions; p++ {
			if n.m.IsOwner(p) {
				return true
			}
		}
		return false
	})
}

// --- 9. a new owner reads only once its takeover has settled ------------------------------------------------------------

func TestNewOwnerCannotReadBeforeItsTakeoverSettles(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	a := fc.nodes[0]
	left := Write{Pos: Pos{Epoch: ep(1, 3), Seq: 1}, Data: []byte("left")}
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	for _, f := range fc.nodes[1:] {
		f.store.seedPending(1, left)
		f.store.onSetPending = func(int, *Write) { <-release }
	}
	done := make(chan error, 1)
	go func() { done <- a.m.takeover(t.Context(), 1) }()
	waitFor(t, 2*time.Second, "the promise round", func() bool { return a.m.IsOwner(1) })
	a.m.sendHeartbeats() // a heartbeat round that reaches a majority while the takeover settles counts for nothing
	time.Sleep(50 * time.Millisecond)
	if a.m.CanRead(1) {
		t.Error("the new owner reads before the write it settles is committed")
	}
	if st, _ := a.m.PartitionStatus(1); st.CanRead {
		t.Errorf("status: %+v", st)
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !a.m.CanRead(1) {
		t.Error("the new owner cannot read once its takeover settled")
	}
}

// --- 10. State's result is read-only -----------------------------------------------------------------------------------

// sharingStore hands out its pending write itself, as a Store that does not copy may.
type sharingStore struct {
	*memStore
	shared *Write
}

func (s *sharingStore) State(p int) (State, error) {
	st, err := s.memStore.State(p)
	if st.Pending != nil && s.shared != nil && st.Pending.Pos == s.shared.Pos {
		st.Pending = s.shared
	}
	return st, err
}

func TestSettleLeavesTheStoresWriteAlone(t *testing.T) {
	ms := newMemStore()
	e1 := ep(1, 2)
	ms.seedCommitted(1, 3, e1)
	shared := &Write{Partition: 1, Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: ep(9, 9), Seq: 3}, Data: []byte("d4")}
	ms.seedPending(1, *shared)
	s := &sharingStore{memStore: ms, shared: shared}
	m, err := newManager(testConfig(s, 3), &fakeNet{&fakeNode{id: nid(1), store: ms}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	m.settle(1, Pos{Epoch: e1, Seq: 4})
	if st := ms.state(1); st.Committed.Seq != 4 {
		t.Fatalf("not committed: %+v", st)
	}
	if shared.Prev != (Pos{Epoch: ep(9, 9), Seq: 3}) {
		t.Errorf("settle changed the Store's write: Prev %s", shared.Prev)
	}
}
