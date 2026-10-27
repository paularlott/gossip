package partition

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// takeoverSetup is a 3-node cluster of attached (not started) Managers in which node 3 is down, so that a takeover
// by node 1 hears from exactly one other node, node 2, and its choice among the answers is deterministic.
func takeoverSetup(t *testing.T) (taker, other *fakeNode) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	fc.nodes[2].down.Store(true)
	return fc.nodes[0], fc.nodes[1]
}

func dataOf(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "d" + string(rune('1'+i))
	}
	return out
}

func TestTakeoverAdoptsHighestEpochPendingAtHPlusOne(t *testing.T) {
	e0 := ep(1, 3)
	lowE, highE := ep(2, 3), ep(3, 3)
	for _, tc := range []struct {
		name         string
		selfPending  Epoch
		otherPending Epoch
		want         string
	}{
		{"the other node's is higher", lowE, highE, "other"},
		{"this node's own is higher", highE, lowE, "self"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			taker, other := takeoverSetup(t)
			for _, fn := range []*fakeNode{taker, other} {
				fn.store.seedCommitted(1, 5, e0)
			}
			h := Pos{Epoch: e0, Seq: 5}
			taker.store.seedPending(1, Write{Pos: Pos{Epoch: tc.selfPending, Seq: 6}, Prev: h, Data: []byte("self")})
			other.store.seedPending(1, Write{Pos: Pos{Epoch: tc.otherPending, Seq: 6}, Prev: h, Data: []byte("other")})

			if err := taker.m.takeover(t.Context(), 1); err != nil {
				t.Fatal(err)
			}
			if !taker.m.IsOwner(1) {
				t.Fatal("not leading after a successful takeover")
			}
			epoch, _ := taker.m.leadingEpoch(1)
			if epoch.Node != taker.id || epoch.Counter != 1 {
				t.Errorf("epoch = %v", epoch)
			}
			// The adopted write is re-proposed at the new epoch, on top of H, and committed.
			st := taker.store.state(1)
			if st.Committed != (Pos{Epoch: epoch, Seq: 6}) || st.Pending != nil {
				t.Fatalf("state after takeover: %+v", st)
			}
			if d := taker.store.data(1); !slices.Equal(d, append(dataOf(5), tc.want)) {
				t.Errorf("data = %v, want the %s pending write at the end", d, tc.want)
			}
			// The other node stored it at the new epoch on top of the same H, and the owner's commit notice has it commit it.
			// The notice is sent from the taker's goroutine; the other node settles it in one of its own (applyEntries).
			taker.m.wg.Wait()
			other.m.wg.Wait()
			ost := other.store.state(1)
			if ost.Pending != nil || ost.Committed != st.Committed || !slices.Equal(other.store.data(1), append(dataOf(5), tc.want)) {
				t.Errorf("other node: %+v, data %v", ost, other.store.data(1))
			}
			if ost.Promised != epoch {
				t.Errorf("other node promised %v, want %v", ost.Promised, epoch)
			}
			for _, fn := range []*fakeNode{taker, other} {
				if err := fn.store.checkInvariant(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

// A follower that learns that the Seq of its pending write is committed under another epoch keeps the write until it has
// caught up, and a takeover that hears only from it re-proposes the write rather than writing something else at that Seq.
// The story (a three-node chaos soak lost an acknowledged write this way): owner A commits X at (e1, 6), stored by A and
// B; A's notice does not reach B. B takes over, re-proposes X at (e2, 6) and fails to commit it (C did not answer), so B
// holds X pending at e2. A's report of its committed position (e1, 6) then reaches B, and A goes (cut off, or down) before
// B can catch up from it. C takes over with B's promise: B is the only holder of X it hears, so B must still report it. A
// follower that cleared a pending write whose Seq it learnt was decided reported nothing, and C committed its own next
// write at 6: two writes committed at one Seq, and X, which A had acknowledged, lost.
func TestTakeoverAfterSettleReproposesTheCommittedWrite(t *testing.T) {
	taker, other := takeoverSetup(t) // node 3, the old owner A, is down: the taker hears only from node 2 (B)
	e0, e1, e2 := ep(1, 3), ep(2, 3), ep(3, 2)
	h := Pos{Epoch: e0, Seq: 5}
	for _, fn := range []*fakeNode{taker, other} {
		fn.store.seedCommitted(1, 5, e0)
	}
	other.store.seedPending(1, Write{Pos: Pos{Epoch: e2, Seq: 6}, Prev: h, Data: []byte("x")})

	// A's committed position, (e1, 6), reaches B (a heartbeat, a commit notice or a write of A's).
	if err := other.m.parts[1].mu.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	other.m.settle(1, Pos{Epoch: e1, Seq: 6})
	other.m.parts[1].mu.unlock()
	if st := other.store.state(1); st.Pending == nil || string(st.Pending.Data) != "x" || st.Committed != h {
		t.Fatalf("B after learning that seq 6 is committed: %+v; want X still pending over committed 5", st)
	}

	if err := taker.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	st := taker.store.state(1)
	if st.Committed.Seq != 6 {
		t.Fatalf("the takeover did not settle X at seq 6: %+v", st)
	}
	if d := taker.store.data(1); !slices.Equal(d, append(dataOf(5), "x")) {
		t.Errorf("data = %v, want X committed at seq 6, the write A committed there", d)
	}
	taker.m.wg.Wait()
	other.m.wg.Wait()
	if d := other.store.data(1); !slices.Equal(d, append(dataOf(5), "x")) {
		t.Errorf("B's data = %v, want X committed at seq 6", d)
	}
	for _, fn := range []*fakeNode{taker, other} {
		if err := fn.store.checkInvariant(); err != nil {
			t.Error(err)
		}
	}
}

func TestTakeoverIgnoresPendingAtOrBelowH(t *testing.T) {
	taker, other := takeoverSetup(t)
	e0 := ep(1, 3)
	taker.store.seedCommitted(1, 5, e0)
	other.store.seedCommitted(1, 4, e0)
	// The other node holds a pending write at seq 5: that Seq is decided (H = 5), possibly as a different write.
	other.store.seedPending(1, Write{Pos: Pos{Epoch: ep(2, 3), Seq: 5}, Prev: Pos{Epoch: e0, Seq: 4}, Data: []byte("ghost")})

	if err := taker.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	st := taker.store.state(1)
	if st.Committed != (Pos{Epoch: e0, Seq: 5}) || st.Pending != nil {
		t.Fatalf("a pending write at or below H was re-proposed: %+v", st)
	}
	if d := taker.store.data(1); !slices.Equal(d, dataOf(5)) {
		t.Errorf("data = %v", d)
	}
	if !taker.m.IsOwner(1) {
		t.Error("not leading")
	}
	for _, fn := range []*fakeNode{taker, other} {
		for _, d := range fn.store.data(1) {
			if d == "ghost" {
				t.Fatal("the stale pending write was committed")
			}
		}
	}
}

func TestTakeoverCatchesUpToHFirst(t *testing.T) {
	taker, other := takeoverSetup(t)
	e0 := ep(1, 3)
	taker.store.seedCommitted(1, 3, e0)
	other.store.seedCommitted(1, 5, e0)
	h := Pos{Epoch: e0, Seq: 5}
	other.store.seedPending(1, Write{Pos: Pos{Epoch: ep(2, 3), Seq: 6}, Prev: h, Data: []byte("six")})

	before := len(taker.store.callLog())
	if err := taker.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if d := taker.store.data(1); !slices.Equal(d, append(dataOf(5), "six")) {
		t.Fatalf("data = %v", d)
	}
	// The order of the taker's durable steps: its promise, then the import up to H, only then the adopted write.
	calls := taker.store.callLog()[before:]
	want := []string{"SetPromised(1)", "Import(1)", "SetPending(1)", "Commit(1)"}
	if !slices.Equal(calls, want) {
		t.Errorf("calls = %v, want %v", calls, want)
	}
	// The re-proposed write follows H (the node it caught up to), not the taker's old committed position: the other node
	// stored it as pending on top of H, and the owner's commit notice has it commit it (in a goroutine of the other node).
	taker.m.wg.Wait()
	other.m.wg.Wait()
	if st := other.store.state(1); st.Committed.Seq != 6 || st.Pending != nil || !slices.Equal(other.store.data(1), append(dataOf(5), "six")) {
		t.Errorf("other node: %+v, data %v", st, other.store.data(1))
	}
}

func TestTakeoverFailsWhenCatchUpFails(t *testing.T) {
	taker, other := takeoverSetup(t)
	e0 := ep(1, 3)
	taker.store.seedCommitted(1, 3, e0)
	other.store.seedCommitted(1, 5, e0)
	other.store.failOnce("Export", errors.New("disk"))

	if err := taker.m.takeover(t.Context(), 1); err == nil {
		t.Fatal("takeover succeeded without catching up to H")
	}
	if taker.m.IsOwner(1) {
		t.Fatal("leading without having caught up")
	}
	if st := taker.store.state(1); st.Committed.Seq != 3 {
		t.Errorf("committed moved: %+v", st)
	}
	// A later attempt (at a higher epoch, as the failed one recorded its promise) succeeds.
	if err := taker.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if st := taker.store.state(1); st.Committed.Seq != 5 {
		t.Errorf("committed = %+v", st)
	}
	if epoch, _ := taker.m.leadingEpoch(1); epoch.Counter != 2 {
		t.Errorf("second attempt's epoch = %v, want counter 2", epoch)
	}
}

func TestTakeoverNeedsAMajority(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	taker := fc.nodes[0]
	fc.nodes[2].down.Store(true)
	// The only other node reachable has promised a higher epoch.
	fc.nodes[1].store.SetPromised(1, ep(9, 2))

	if err := taker.m.takeover(t.Context(), 1); !errors.Is(err, errNoMajority) {
		t.Fatalf("takeover = %v, want no majority", err)
	}
	if taker.m.IsOwner(1) {
		t.Fatal("leading without a majority")
	}
	if got := taker.m.parts[1].maxCounter.Load(); got != 9 {
		t.Fatalf("maxCounter = %d, want 9 (raised from the refusal)", got)
	}
	// With the refusal's counter known, the next attempt proposes above it and wins.
	if err := taker.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := taker.m.leadingEpoch(1); epoch.Counter != 10 {
		t.Errorf("epoch = %v, want counter 10", epoch)
	}

	// Alone, a node of a cluster of 3 is no majority.
	fc.nodes[1].down.Store(true)
	if err := fc.nodes[0].m.takeover(t.Context(), 2); !errors.Is(err, errNoMajority) {
		t.Fatalf("takeover alone = %v", err)
	}
	if fc.nodes[0].m.IsOwner(2) {
		t.Fatal("leading alone")
	}
}

func TestTakeoverBelowTheStoredPromiseRetries(t *testing.T) {
	fc := newFakeCluster(t, 1, 1, nil)
	m, st := fc.nodes[0].m, fc.nodes[0].store
	// The promise outran this node's memory of counters (a restart): the first attempt only learns it.
	st.SetPromised(1, ep(7, 1))
	if err := m.takeover(t.Context(), 1); !errors.Is(err, errRetryLater) {
		t.Fatalf("takeover = %v, want errRetryLater", err)
	}
	if m.IsOwner(1) {
		t.Fatal("leading")
	}
	if err := m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := m.leadingEpoch(1); epoch.Counter != 8 {
		t.Errorf("epoch = %v", epoch)
	}
}

func TestTakeoverFailureOfSettlingStepsDown(t *testing.T) {
	taker, other := takeoverSetup(t)
	e0 := ep(1, 3)
	taker.store.seedCommitted(1, 2, e0)
	other.store.seedCommitted(1, 2, e0)
	taker.store.seedPending(1, Write{Pos: Pos{Epoch: ep(2, 3), Seq: 3}, Prev: Pos{Epoch: e0, Seq: 2}, Data: []byte("p")})
	// The other node cannot store the re-proposed write: no majority for it.
	other.store.failOnce("SetPending", errors.New("disk"))

	if err := taker.m.takeover(t.Context(), 1); err == nil {
		t.Fatal("takeover reported success though the pending write was not committed")
	}
	if taker.m.IsOwner(1) {
		t.Fatal("leading with an unsettled write")
	}
	// The write is still pending on the taker (it counts as its own ack) and is settled by the next takeover.
	if st := taker.store.state(1); st.Pending == nil || st.Committed.Seq != 2 {
		t.Fatalf("state: %+v", st)
	}
	if err := taker.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if d := taker.store.data(1); !slices.Equal(d, []string{"d1", "d2", "p"}) {
		t.Errorf("data = %v", d)
	}
}

func TestTakeoverAnnouncesAndPeersFollow(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	taker := fc.nodes[0]
	if err := taker.m.takeover(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	epoch, _ := taker.m.leadingEpoch(2)
	waitFor(t, time.Second, "peers to hear the announcement", func() bool {
		for _, fn := range fc.nodes[1:] {
			o, ok := fn.m.Owner(2)
			if !ok || o != taker.id {
				return false
			}
		}
		return true
	})
	fc.nodes[1].m.parts[2].vmu.RLock()
	got := fc.nodes[1].m.parts[2].ownerEpoch
	fc.nodes[1].m.parts[2].vmu.RUnlock()
	if got != epoch {
		t.Errorf("peer learned epoch %v, want %v", got, epoch)
	}
}

// An old owner that committed write 5 locally, then lost the ownership before the others heard of the commit, is left
// with write 5 committed as (e1,5) while the next owner (whose promise round does not include it) re-proposes the same
// write as (e2,5). The old owner must still be able to take part in the new owner's majorities.
func TestTakeoverRepeatingAWriteTheOldOwnerCommittedLeavesItUsable(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	a, b, c := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	e0, e1 := ep(1, 3), ep(2, 3)
	for _, fn := range fc.nodes {
		fn.store.seedCommitted(1, 4, e0)
	}
	w5 := Write{Partition: 1, Pos: Pos{Epoch: e1, Seq: 5}, Prev: Pos{Epoch: e0, Seq: 4}, Data: []byte("five")}
	if err := c.store.Commit(&w5); err != nil { // the old owner committed it
		t.Fatal(err)
	}
	b.store.seedPending(1, w5) // a majority stored it: node 3 and node 2

	c.down.Store(true) // node 3 is not in the new owner's promise round
	if err := a.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	epoch, _ := a.m.leadingEpoch(1)
	if st := a.store.state(1); st.Committed != (Pos{Epoch: epoch, Seq: 5}) {
		t.Fatalf("the re-proposal is committed under the new epoch: %+v", st)
	}
	// Node 3 comes back; node 2 goes away, so the next write needs node 3's ack.
	c.down.Store(false)
	b.down.Store(true)
	a.m.touchMajority(1, epoch, a.m.now())
	if resp := a.m.runChange(t.Context(), 1, []byte("six"), false); resp.Status != statusDone {
		t.Fatalf("the old owner, holding the same write under another label, refused the next: %+v", resp)
	}
	for _, fn := range []*fakeNode{a, c} {
		if err := fn.store.checkInvariant(); err != nil {
			t.Error(err)
		}
	}
	// Heartbeat-driven settling brings node 3 level without anything special.
	c.m.handleHeartbeat(a.id, &heartbeatMsg{Size: 3, From: a.id, Entries: []heartbeatEntry{{P: 1, Epoch: epoch,
		Committed: a.store.state(1).Committed}}})
	// (The settling runs in node 3's goroutine for the partition, while the owner's commit notices still arrive.)
	want := []string{"d1", "d2", "d3", "d4", "five", "six"}
	waitFor(t, 5*time.Second, "node 3 to settle", func() bool { return slices.Equal(c.store.data(1), want) })
}

// A node whose copy of the partition is Incomplete does not take it over: it would own a partition whose data it does not
// hold. It asks nobody for a promise, and backs off.
func TestIncompleteNodeDoesNotTakeOver(t *testing.T) {
	taker, other := takeoverSetup(t)
	e0 := ep(1, 3)
	for _, fn := range []*fakeNode{taker, other} {
		fn.store.seedCommitted(1, 5, e0)
	}
	taker.store.seedIncomplete(1)
	if err := taker.m.takeover(t.Context(), 1); !errors.Is(err, errIncomplete) {
		t.Fatalf("takeover by an incomplete node: %v", err)
	}
	if _, leading := taker.m.leadingEpoch(1); leading {
		t.Fatal("an incomplete node leads the partition")
	}
	if st := other.store.state(1); st.Promised != (Epoch{}) {
		t.Errorf("the other node was asked for a promise: %+v", st)
	}
}
