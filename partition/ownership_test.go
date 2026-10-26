package partition

import (
	"context"
	"github.com/paularlott/gossip"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// ranked returns the nodes of fc ordered by their rank for p (rank 0 first).
func ranked(fc *fakeCluster, p int) []*fakeNode {
	var ids []gossip.NodeID
	for _, fn := range fc.nodes {
		ids = append(ids, fn.id)
	}
	out := slices.Clone(fc.nodes)
	rank := func(fn *fakeNode) int { r, _ := rendezvousRank(p, fn.id, ids); return r }
	slices.SortFunc(out, func(a, b *fakeNode) int { return rank(a) - rank(b) })
	return out
}

// waitRankZeroOwners waits until every partition is owned by its rank 0 node and every node up agrees (the cluster has
// settled after startup, where ownership moves to the returning nodes by claim).
func waitRankZeroOwners(t *testing.T, fc *fakeCluster, limit time.Duration) {
	t.Helper()
	waitFor(t, limit, "every partition owned by its rank 0 node", func() bool {
		for p := 0; p < fc.nodes[0].m.cfg.Partitions; p++ {
			owners := fc.owners(p)
			if len(owners) != 1 || owners[0] != ranked(fc, p)[0] {
				return false
			}
			for _, fn := range fc.nodes {
				if o, ok := fn.m.Owner(p); !ok || o != owners[0].id {
					return false
				}
			}
		}
		return true
	})
}

func TestRendezvousRankOrdersCandidates(t *testing.T) {
	var ids []gossip.NodeID
	for i := 1; i <= 6; i++ {
		ids = append(ids, nid(i))
	}
	for p := 0; p < 20; p++ {
		owner, _ := rendezvousOwner(p, ids)
		seen := map[int]bool{}
		for _, id := range ids {
			r, ok := rendezvousRank(p, id, ids)
			if !ok || seen[r] || r < 0 || r >= len(ids) {
				t.Fatalf("p%d: %s has rank %d (found %v), taken %v", p, id, r, ok, seen)
			}
			seen[r] = true
			if (r == 0) != (id == owner) {
				t.Fatalf("p%d: rank 0 is not the rendezvous owner", p)
			}
		}
		// Removing the rank 0 node moves everyone else up by one.
		rest := slices.DeleteFunc(slices.Clone(ids), func(id gossip.NodeID) bool { return id == owner })
		for _, id := range rest {
			r1, _ := rendezvousRank(p, id, ids)
			r2, _ := rendezvousRank(p, id, rest)
			if r2 != r1-1 {
				t.Fatalf("p%d: rank %d became %d after rank 0 left", p, r1, r2)
			}
		}
	}
	if _, ok := rendezvousRank(1, nid(9), ids); ok {
		t.Error("a node that is not a candidate has a rank")
	}
}

func TestTakeoverWaitsRankTimesRankDelay(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.TakeoverBackoffMax = 450 * time.Millisecond })
	var ids []gossip.NodeID
	for _, fn := range fc.nodes {
		ids = append(ids, fn.id)
	}
	for r, fn := range ranked(fc, 1) {
		m := fn.m
		m.ready.Store(true)
		advance := setClock(m)
		delay := time.Duration(r) * m.cfg.RankDelay
		// Ownerless from the first look; rank r acts after r x RankDelay.
		if due := m.takeoverDue(1, ids); due != (r == 0) {
			t.Fatalf("rank %d: due %v at once", r, due)
		}
		if r > 0 {
			advance(delay - time.Millisecond)
			if m.takeoverDue(1, ids) {
				t.Fatalf("rank %d: due before %v", r, delay)
			}
			advance(time.Millisecond)
			if !m.takeoverDue(1, ids) {
				t.Fatalf("rank %d: not due after %v", r, delay)
			}
		}
		// Not while not ready, not as a non-candidate.
		m.ready.Store(false)
		if m.takeoverDue(1, ids) {
			t.Fatalf("rank %d: due while not ready", r)
		}
		m.ready.Store(true)
		if m.takeoverDue(1, ids[:0]) {
			t.Fatalf("rank %d: due as a non-candidate", r)
		}
		// A live owner resets the clock; its silence for OwnerTimeout starts it again, from the moment it went quiet.
		other := ranked(fc, 1)[(r+1)%3]
		m.setOwner(1, other.id, ep(1, 1))
		advance(m.cfg.OwnerTimeout - time.Millisecond)
		if m.takeoverDue(1, ids) {
			t.Fatalf("rank %d: due with a live owner", r)
		}
		advance(time.Millisecond)
		due := m.takeoverDue(1, ids)
		if due != (r == 0) {
			t.Fatalf("rank %d: due %v just after the owner went quiet", r, due)
		}
		advance(delay)
		if !m.takeoverDue(1, ids) {
			t.Fatalf("rank %d: not due %v after the owner went quiet", r, delay)
		}
		// Backoff after failed takeovers: OwnerTimeout, doubling, capped at TakeoverBackoffMax; a success ends it.
		for i, want := range []time.Duration{100, 200, 400, 450, 450} {
			want *= time.Millisecond
			if got := m.takeoverFailed(1); got != want {
				t.Fatalf("rank %d: failure %d backs off %v, want %v", r, i+1, got, want)
			}
		}
		if m.takeoverDue(1, ids) || !m.inBackoff(1) {
			t.Fatalf("rank %d: not in backoff after failures", r)
		}
		advance(449 * time.Millisecond)
		if !m.inBackoff(1) {
			t.Fatalf("rank %d: backoff over too early", r)
		}
		advance(time.Millisecond)
		if m.inBackoff(1) || !m.takeoverDue(1, ids) {
			t.Fatalf("rank %d: still in backoff after the time", r)
		}
		m.takeoverFailed(1)
		m.takeoverSucceeded(1)
		if m.inBackoff(1) || m.takeoverFailed(1) != m.cfg.OwnerTimeout {
			t.Fatalf("rank %d: a success did not reset the backoff", r)
		}
	}
}

func TestClaimDue(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	var ids []gossip.NodeID
	for _, fn := range fc.nodes {
		ids = append(ids, fn.id)
	}
	order := ranked(fc, 1)
	r0, r1 := order[0], order[1]
	for _, fn := range fc.nodes {
		fn.m.ready.Store(true)
	}
	m := r0.m
	advance := setClock(m)
	if _, ok := m.claimDue(1, ids); ok {
		t.Error("a claim without an owner")
	}
	m.setOwner(1, r1.id, ep(1, 1))
	if o, ok := m.claimDue(1, ids); !ok || o != r1.id {
		t.Errorf("claimDue = %v, %v: rank 0 must claim from a live owner", o, ok)
	}
	if _, ok := r1.m.claimDue(1, ids); ok {
		t.Error("a node that is not rank 0 claimed")
	}
	m.claim(1, r1.id) // sets lastClaim (the request goes to the unattached fake network and fails)
	m.wg.Wait()
	if _, ok := m.claimDue(1, ids); ok {
		t.Error("claimed twice within OwnerTimeout")
	}
	advance(m.cfg.OwnerTimeout - time.Millisecond)
	m.setOwner(1, r1.id, ep(1, 1)) // heartbeat
	if _, ok := m.claimDue(1, ids); ok {
		t.Error("claimed again too soon")
	}
	advance(time.Millisecond)
	m.setOwner(1, r1.id, ep(1, 1))
	if _, ok := m.claimDue(1, ids); !ok {
		t.Error("no claim a whole OwnerTimeout later")
	}
	m.takeoverFailed(1)
	if _, ok := m.claimDue(1, ids); ok {
		t.Error("claimed while in backoff")
	}
	m.takeoverSucceeded(1)
	m.ready.Store(false)
	if _, ok := m.claimDue(1, ids); ok {
		t.Error("claimed while not ready")
	}
	m.ready.Store(true)
	m.becomeLeader(1, ep(2, 1), m.now())
	if _, ok := m.claimDue(1, ids); ok {
		t.Error("the owner claimed its own partition")
	}
}

func TestHandleClaim(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	for _, fn := range fc.nodes {
		fn.ready.Store(true)
		fn.m.ready.Store(true)
	}
	order := ranked(fc, 1)
	r0, x, z := order[0], order[1], order[2]
	if err := x.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	x.m.wg.Wait()
	req := &claimReq{Size: 3, P: 1}
	if resp := x.m.handleClaim(z.id, req); resp.OK || !x.m.IsOwner(1) {
		t.Error("a claim by a node that is not rank 0 was granted")
	}
	if resp := x.m.handleClaim(r0.id, &claimReq{Size: 5, P: 1}); !resp.SizeMismatch || resp.OK {
		t.Errorf("size mismatch: %+v", resp)
	}
	if resp := x.m.handleClaim(r0.id, &claimReq{Size: 3, P: 77}); resp.OK {
		t.Error("a claim of a partition out of range")
	}
	if resp := z.m.handleClaim(r0.id, req); resp.OK {
		t.Error("a node that does not lead the partition granted a claim")
	}
	// A claim by rank 0 waits for the change in progress (the partition's mutex), then hands over and relinquishes.
	ps := x.m.parts[1]
	if err := ps.mu.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	granted := make(chan *claimResp, 1)
	go func() { granted <- x.m.handleClaim(r0.id, req) }()
	select {
	case <-granted:
		t.Fatal("handed over while a change was in progress")
	case <-time.After(30 * time.Millisecond):
	}
	ps.mu.unlock()
	if resp := <-granted; !resp.OK {
		t.Fatal("the claim of rank 0 was not granted")
	}
	if x.m.IsOwner(1) {
		t.Error("still leading after the hand-over")
	}
	// The others heard the relinquish: no stickiness, so rank 0's promise round is not refused.
	if _, ok := z.m.Owner(1); ok {
		t.Error("another node still believes in the owner that handed over")
	}
	if err := r0.m.takeover(t.Context(), 1); err != nil {
		t.Fatalf("takeover after the hand-over: %v", err)
	}
}

func TestAnOwnerThatIsNotRankZeroKeepsItsPartition(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	for _, fn := range fc.nodes {
		fn.ready.Store(true)
		fn.m.ready.Store(true)
	}
	x := ranked(fc, 1)[1]
	if err := x.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	x.m.wg.Wait()
	for i := 0; i < 5; i++ {
		x.m.ownershipTick()
		x.m.touchMajority(1, x.m.parts[1].myEpoch, x.m.now())
		time.Sleep(5 * time.Millisecond)
	}
	x.m.wg.Wait()
	if !x.m.IsOwner(1) {
		t.Fatal("an owner stepped down because a node of a better rank is ready")
	}
}

func TestRankOneTakesOverFromACutOffOwner(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	waitRankZeroOwners(t, fc, 10*time.Second)
	const p = 1
	old := ranked(fc, p)[0]
	cfg := old.m.cfg
	writeRetry(t, fc.nodes[0], p, "before")
	converge(t, p, fc.nodes...)

	// Cut off, but still alive and ready in everyone's membership: the failure detector has not caught up.
	fc.isolate(old, true)
	start := time.Now()
	waitFor(t, 5*time.Second, "another node to take over", func() bool {
		for _, fn := range fc.nodes {
			if fn != old && fn.m.IsOwner(p) {
				return true
			}
		}
		return false
	})
	took := time.Since(start)
	if bound := cfg.OwnerTimeout + cfg.RankDelay + cfg.BackoffMax + 500*time.Millisecond; took > bound {
		t.Errorf("took %v to take over, want within %v (OwnerTimeout + RankDelay + BackoffMax + slack)", took, bound)
	}
	t.Logf("took over after %v", took)
	var rest []*fakeNode
	for _, fn := range fc.nodes {
		if fn != old {
			rest = append(rest, fn)
		}
	}
	writeRetry(t, rest[0], p, "after")
	waitFor(t, 5*time.Second, "the cut-off owner to step down", func() bool { return !old.m.IsOwner(p) })
	if got := converge(t, p, rest...); !slices.Equal(got, []string{"before", "after"}) {
		t.Errorf("data = %v", got)
	}
}

func TestRankZeroReturnsAndTheOwnerHandsOver(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	waitRankZeroOwners(t, fc, 10*time.Second)
	const p = 1
	order := ranked(fc, p)
	r0 := order[0]
	writeRetry(t, order[1], p, "before")

	fc.crash(r0)
	waitFor(t, 10*time.Second, "another node to own the partition", func() bool { return len(fc.owners(p)) == 1 })
	interim := fc.owners(p)[0]
	writeRetry(t, order[2], p, "during")

	// It comes back with its data. Nothing makes the interim owner give up the partition but the claim.
	fc.restart(r0, nil)
	waitFor(t, 20*time.Second, "the returning rank 0 node to own the partition again", func() bool { return r0.m.IsOwner(p) })
	waitFor(t, 5*time.Second, "the interim owner to let go", func() bool { return !interim.m.IsOwner(p) })
	fc.waitOwners(10 * time.Second)
	writeRetry(t, interim, p, "after")
	if got := converge(t, p, fc.nodes...); !slices.Equal(got, []string{"before", "during", "after"}) {
		t.Errorf("data = %v", got)
	}
}

// A rank 0 node that can send but not receive (the replies to its promise requests are lost) claims the partition, fails
// to take it over and backs off; the next rank takes it over again. The partition stays writable, and the churn is bounded.
func TestRankZeroThatCannotCompleteATakeoverBacksOff(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement skipped in -short mode")
	}
	// The backoff after a failed takeover doubles from OwnerTimeout up to TakeoverBackoffMax; capping it at 400ms keeps the
	// attempts frequent enough to count in a short run (the doubling itself is unit-tested), while still far apart
	// compared with the 20ms heartbeat: without backoff rank 0 would retry on every claim round.
	const backoffMax = 400 * time.Millisecond
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.TakeoverBackoffMax = backoffMax })
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	waitRankZeroOwners(t, fc, 10*time.Second)
	const p = 1
	order := ranked(fc, p)
	r0, writer := order[0], order[2]

	fc.crash(r0)
	waitFor(t, 10*time.Second, "another node to own the partition", func() bool { return len(fc.owners(p)) == 1 })
	r0.dropReplies.Store(true)
	fc.restart(r0, nil)

	const (
		window  = 250 * time.Millisecond
		nwin    = 20
		measure = nwin * window
	)
	var windows [nwin]int
	var ownedByR0 atomic.Bool
	start := time.Now()
	for time.Since(start) < measure {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		if _, err := writer.m.Write(ctx, p, []byte("w")); err == nil {
			windows[min(int(time.Since(start)/window), nwin-1)]++
		}
		cancel()
		if r0.m.IsOwner(p) {
			ownedByR0.Store(true)
		}
		time.Sleep(10 * time.Millisecond)
	}
	good := 0
	for _, n := range windows {
		if n > 0 {
			good++
		}
	}
	ps := r0.m.parts[p]
	ps.vmu.RLock()
	failures := ps.failures
	ps.vmu.RUnlock()
	t.Logf("writes per window %v: %d of %d windows had a write; rank 0 failed %d takeovers", windows, good, nwin, failures)
	if good < nwin*8/10 {
		t.Errorf("only %d of %d %v windows had a successful write, want at least %d", good, nwin, window, nwin*8/10)
	}
	if ownedByR0.Load() {
		t.Error("a node whose promise replies are all lost became owner")
	}
	// Backed off, it retries no more often than once per backoffMax (plus the time an attempt takes); it must also have
	// tried repeatedly.
	if maxTries := int(measure/backoffMax) + 2; failures < 2 || failures > maxTries {
		t.Errorf("rank 0 failed %d takeovers in %v: it should have tried (at least 2), and backed off (at most %d)", failures, measure, maxTries)
	}
}
