package leader

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
)

// mkNodes builds n throwaway nodes for feeding the tracker.
func mkNodes(n int) []*gossip.Node {
	out := make([]*gossip.Node, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, gossip.NewTestNode(gossip.NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 20000+i), nil))
	}
	return out
}

const (
	stability = 100 * time.Millisecond
	// Long enough that existing tests never trip auto-shrink.
	longDwell = time.Hour
)

// --- seeding and growth ---

func TestBaselineSeedsImmediately(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	b.observe(mkNodes(3), now)

	// A starting cluster must be usable at once rather than waiting a whole
	// stability period before any leader can be elected.
	if got := b.size(); got != 3 {
		t.Errorf("expected the baseline to seed at 3, got %d", got)
	}
}

func TestBaselineGrowthRequiresStability(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes3 := mkNodes(3)
	b.observe(nodes3, now)

	// Cluster grows to 5, but not yet steady.
	nodes5 := append(nodes3, mkNodes(2)...)
	b.observe(nodes5, now)
	if got := b.size(); got != 3 {
		t.Errorf("growth should not be adopted immediately, got %d", got)
	}

	// Still inside the stability window.
	b.observe(nodes5, now.Add(stability/2))
	if got := b.size(); got != 3 {
		t.Errorf("growth adopted too early, got %d", got)
	}

	// Steady for long enough.
	b.observe(nodes5, now.Add(stability+time.Millisecond))
	if got := b.size(); got != 5 {
		t.Errorf("expected the baseline to reach 5, got %d", got)
	}
}

// A count that flaps must never be adopted — this is what stops a node that
// appears for a moment from ratcheting quorum permanently upward.
func TestBaselineRejectsTransientSpike(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes3 := mkNodes(3)
	b.observe(nodes3, now)

	spike := append(nodes3, mkNodes(6)...) // brief jump to 9
	b.observe(spike, now.Add(10*time.Millisecond))
	b.observe(nodes3, now.Add(20*time.Millisecond)) // gone again
	b.observe(nodes3, now.Add(30*time.Millisecond))

	if got := b.size(); got != 3 {
		t.Errorf("a transient spike must not raise the baseline, got %d", got)
	}

	// And the settled count should not lower it either.
	b.observe(nodes3, now.Add(10*stability))
	if got := b.size(); got != 3 {
		t.Errorf("baseline should remain 3, got %d", got)
	}
}

// --- shrinkage: the dangerous direction ---

// A silent disappearance (crash or partition) must NOT lower the baseline.
func TestBaselineIgnoresSilentDisappearance(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(9)
	b.observe(nodes, now)
	if got := b.size(); got != 9 {
		t.Fatalf("expected seed of 9, got %d", got)
	}

	// Three vanish with no announcement, and stay gone well past stability.
	survivors := nodes[:6]
	for i := 0; i < 20; i++ {
		b.observe(survivors, now.Add(time.Duration(i)*stability))
	}

	if got := b.size(); got != 9 {
		t.Errorf("a silent loss must not lower the baseline (a partition looks identical), got %d", got)
	}
}

// A graceful leave is an explicit signal and must lower the baseline.
func TestBaselineGracefulDepartureLowersBaseline(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(9)
	b.observe(nodes, now)

	if !b.noteGracefulDeparture(nodes[0].ID) {
		t.Fatal("a graceful departure of an eligible node should count")
	}
	if got := b.size(); got != 8 {
		t.Errorf("expected the baseline to drop to 8, got %d", got)
	}

	if !b.noteGracefulDeparture(nodes[1].ID) {
		t.Fatal("second departure should count")
	}
	if got := b.size(); got != 7 {
		t.Errorf("expected the baseline to drop to 7, got %d", got)
	}
}

func TestBaselineGracefulDepartureIsIdempotent(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	nodes := mkNodes(5)
	b.observe(nodes, time.Now())

	b.noteGracefulDeparture(nodes[0].ID)
	if b.noteGracefulDeparture(nodes[0].ID) {
		t.Error("the same departure must not be counted twice")
	}
	if got := b.size(); got != 4 {
		t.Errorf("expected 4, got %d", got)
	}
}

// Eligibility of a departure is decided by the election, not the tracker (see
// wasEligibleForBaseline): the tracker discounts whatever it is told, exactly
// once. These tests pin the decision the election makes.

func TestWasEligibleForBaseline(t *testing.T) {
	worker := gossip.NewTestNode(gossip.NodeID(uuid.New()), "127.0.0.1:1", map[string]string{"role": "worker"})
	bystander := gossip.NewTestNode(gossip.NodeID(uuid.New()), "127.0.0.1:2", map[string]string{"role": "bystander"})

	scoped := &LeaderElection{config: &Config{MetadataCriteria: map[string]string{"role": "worker"}}}
	clusterWide := &LeaderElection{config: &Config{}}

	cases := []struct {
		name string
		le   *LeaderElection
		node *gossip.Node
		prev gossip.NodeState
		want bool
	}{
		{"scoped worker from alive", scoped, worker, gossip.NodeAlive, true},
		{"scoped worker from suspect", scoped, worker, gossip.NodeSuspect, true},
		{"scoped bystander is not ours", scoped, bystander, gossip.NodeAlive, false},
		{"scoped worker already dead", scoped, worker, gossip.NodeDead, false},
		{"cluster-wide alive node", clusterWide, bystander, gossip.NodeAlive, true},
		{"cluster-wide suspect node", clusterWide, bystander, gossip.NodeSuspect, true},
		{"cluster-wide dead then leaving", clusterWide, worker, gossip.NodeDead, false},
		{"cluster-wide unknown then leaving", clusterWide, worker, gossip.NodeUnknown, false},
	}

	for _, tc := range cases {
		if got := tc.le.wasEligibleForBaseline(tc.node, tc.prev); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestBaselineNeverDropsBelowOne(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	nodes := mkNodes(3)
	b.observe(nodes, time.Now())

	for _, n := range nodes {
		b.noteGracefulDeparture(n.ID)
	}

	if got := b.size(); got < 1 {
		t.Errorf("baseline must stay at least 1, got %d", got)
	}
}

// --- rolling restarts must not leak ---

// The failure mode a monotonic departed-counter would have had: repeated
// leave/rejoin cycles driving the baseline to zero.
func TestBaselineRollingRestartDoesNotLeak(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(5)
	b.observe(nodes, now)

	for cycle := 0; cycle < 10; cycle++ {
		target := nodes[cycle%len(nodes)]

		// Announce and depart.
		b.noteGracefulDeparture(target.ID)

		remaining := make([]*gossip.Node, 0, len(nodes)-1)
		for _, n := range nodes {
			if n.ID != target.ID {
				remaining = append(remaining, n)
			}
		}
		now = now.Add(10 * time.Millisecond)
		b.observe(remaining, now)

		if got := b.size(); got != 4 {
			t.Fatalf("cycle %d: expected 4 while the node is away, got %d", cycle, got)
		}

		// It comes back and stays.
		now = now.Add(10 * time.Millisecond)
		b.observe(nodes, now)
		now = now.Add(stability + time.Millisecond)
		b.observe(nodes, now)

		if got := b.size(); got != 5 {
			t.Fatalf("cycle %d: expected recovery to 5, got %d (leak)", cycle, got)
		}
		if got := b.departedCount(); got != 0 {
			t.Fatalf("cycle %d: departed set should be empty, holds %d", cycle, got)
		}
	}
}

// A rejoin must clear the mark so a later departure counts again.
func TestBaselineRejoinRearmsDeparture(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(5)
	b.observe(nodes, now)

	b.noteGracefulDeparture(nodes[0].ID)
	if got := b.size(); got != 4 {
		t.Fatalf("expected 4, got %d", got)
	}

	// Rejoins and settles.
	b.observe(nodes, now.Add(10*time.Millisecond))
	b.observe(nodes, now.Add(stability+20*time.Millisecond))
	if got := b.size(); got != 5 {
		t.Fatalf("expected recovery to 5, got %d", got)
	}

	// Leaves again — must count.
	if !b.noteGracefulDeparture(nodes[0].ID) {
		t.Error("a departure after a rejoin should count again")
	}
	if got := b.size(); got != 4 {
		t.Errorf("expected 4, got %d", got)
	}
}

// --- forget ---

func TestBaselineForgetDiscountsSilentNode(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(9)
	b.observe(nodes, now)

	// Three crash silently; the baseline holds, as it must.
	survivors := nodes[:6]
	b.observe(survivors, now.Add(stability*3))
	if got := b.size(); got != 9 {
		t.Fatalf("expected the baseline to hold at 9 after a silent loss, got %d", got)
	}

	// The operator asserts they are gone for good.
	for _, n := range nodes[6:] {
		if !b.forget(n.ID) {
			t.Errorf("forget should discount %v", n.ID)
		}
	}

	if got := b.size(); got != 6 {
		t.Errorf("expected the baseline to fall to 6 after forgetting, got %d", got)
	}
}

func TestBaselineForgetIsIdempotent(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	nodes := mkNodes(5)
	b.observe(nodes, time.Now())

	id := nodes[0].ID
	if !b.forget(id) {
		t.Fatal("first forget should count")
	}
	if b.forget(id) {
		t.Error("forgetting twice must not double-count")
	}
	if got := b.size(); got != 4 {
		t.Errorf("expected 4, got %d", got)
	}
}

// The tracker itself spends its decrement on any ID — knowing whether a node is
// genuinely a member is the caller's job. LeaderElection.ForgetNodeLocal gates
// on cluster knowledge (see TestClusterForgetNodeUnknownIsRejected), which is
// what stops a bogus forget broadcast from suppressing quorum.
func TestBaselineForgetAcceptsAnyID(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	b.observe(mkNodes(5), time.Now())

	if !b.forget(gossip.NodeID(uuid.New())) {
		t.Error("the tracker itself does not filter; the caller must")
	}
	if got := b.size(); got != 4 {
		t.Errorf("expected 4, got %d", got)
	}
}

// --- reset ---

func TestBaselineResetReDerives(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(9)
	b.observe(nodes, now)
	b.noteGracefulDeparture(nodes[0].ID)
	b.noteGracefulDeparture(nodes[1].ID)
	if got := b.size(); got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}

	b.reset()
	if got := b.size(); got != 0 {
		t.Errorf("reset should clear the baseline, got %d", got)
	}
	if got := b.departedCount(); got != 0 {
		t.Errorf("reset should clear departures, got %d", got)
	}

	// Re-seeds from what is visible now.
	b.observe(nodes[:5], now.Add(time.Second))
	if got := b.size(); got != 5 {
		t.Errorf("expected a re-seed at 5, got %d", got)
	}
}

// --- watch notifications ---

// WatchLeadership must fire on every leadership mutation a consumer like the
// lock pool cares about, including ones the public events miss.
func TestWatchLeadershipNotifications(t *testing.T) {
	le := &LeaderElection{}

	var mu sync.Mutex
	var got []struct {
		isLeader bool
		term     uint64
	}
	le.WatchLeadership(func(isLeader bool, term uint64) {
		mu.Lock()
		got = append(got, struct {
			isLeader bool
			term     uint64
		}{isLeader, term})
		mu.Unlock()
	})

	// Registration reports current state immediately.
	if len(got) != 1 || got[0].isLeader || got[0].term != 0 {
		t.Fatalf("registration must report the current state, got %+v", got)
	}

	// A term advance with the same leader fires no public event but must
	// notify — another node may have led in between.
	le.lock.Lock()
	le.currentTerm = 7
	le.lock.Unlock()
	le.notifyLeadershipWatchers()

	mu.Lock()
	last := got[len(got)-1]
	mu.Unlock()
	if last.term != 7 {
		t.Errorf("term change must notify watchers, last notification %+v", last)
	}

	// The callback may call back into the election — notify must not hold the
	// lock.
	le.WatchLeadership(func(isLeader bool, term uint64) {
		if le.IsLeader() || le.Term() != term {
			t.Errorf("watcher callback could not read election state: isLeader=%v term=%d (want %d)", isLeader, le.Term(), term)
		}
	})
	le.notifyLeadershipWatchers()
}

// --- the property that matters ---

// A graceful drain should walk quorum down in step, and every intermediate state
// must still be split-safe for the cluster size actually running at that point.
func TestBaselineDrainKeepsQuorumSafe(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(9)
	b.observe(nodes, now)

	le := &LeaderElection{
		config:   &Config{MinClusterSize: 0},
		baseline: b,
	}

	live := len(nodes)
	for i := 0; i < 6; i++ {
		quorum := le.calculateQuorumForNodes(live)

		// No split of the currently live cluster may produce two leaders.
		for a := 0; a <= live; a++ {
			bSide := live - a
			if a >= quorum && bSide >= quorum {
				t.Fatalf("live=%d baseline=%d quorum=%d: split %d/%d elects twice",
					live, b.size(), quorum, a, bSide)
			}
		}

		// Drain one more node.
		b.noteGracefulDeparture(nodes[i].ID)
		live--
		now = now.Add(stability + time.Millisecond)
		b.observe(nodes[i+1:], now)
	}
}

// A partition must never let the minority reach quorum, no matter how long it
// persists — the baseline holds because no departure was announced.
func TestBaselinePartitionMinorityStaysBlocked(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(9)
	b.observe(nodes, now)

	le := &LeaderElection{
		config:   &Config{},
		baseline: b,
	}

	// The minority sees only its own 3 nodes, indefinitely.
	minority := nodes[:3]
	for i := 0; i < 50; i++ {
		now = now.Add(stability)
		b.observe(minority, now)

		if q := le.calculateQuorumForNodes(3); 3 >= q {
			t.Fatalf("iteration %d: minority of 3 reached quorum %d (baseline %d)", i, q, b.size())
		}
	}

	// And the majority side can still lead.
	if q := le.calculateQuorumForNodes(6); 6 < q {
		t.Errorf("majority of 6 should reach quorum, needs %d (baseline %d)", q, b.size())
	}
}

// The whole point of the adaptive term: growth is covered without re-tuning the
// floor. A floor set for a 5-node cluster must still be safe at 9.
func TestBaselineGrowthCoversStaleFloor(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	now := time.Now()

	// Floor chosen when the cluster was 5.
	le := &LeaderElection{
		config:   &Config{MinClusterSize: 3},
		baseline: b,
	}

	// It has since grown to 9 and settled.
	nodes := mkNodes(9)
	b.observe(nodes, now)
	b.observe(nodes, now.Add(stability+time.Millisecond))

	if got := b.size(); got != 9 {
		t.Fatalf("expected the baseline to reach 9, got %d", got)
	}

	// The 3/6 split that broke the old view-dependent rule.
	q3 := le.calculateQuorumForNodes(3)
	q6 := le.calculateQuorumForNodes(6)

	if 3 >= q3 && 6 >= q6 {
		t.Fatalf("a 3/6 split elects twice (q3=%d q6=%d baseline=%d) — the stale floor was not covered",
			q3, q6, b.size())
	}
	if 6 < q6 {
		t.Errorf("the majority side should still lead, needs %d", q6)
	}
}

// Concurrency smoke test: the tracker is touched from the election loop and from
// state-change callbacks at the same time.
func TestBaselineConcurrentAccess(t *testing.T) {
	b := newBaselineTracker(stability, longDwell, true, nil)
	nodes := mkNodes(20)
	b.observe(nodes, time.Now())

	done := make(chan struct{})

	go func() {
		defer close(done)
		now := time.Now()
		for i := 0; i < 500; i++ {
			now = now.Add(time.Millisecond)
			b.observe(nodes, now)
			_ = b.size()
		}
	}()

	for i := 0; i < 20; i++ {
		b.noteGracefulDeparture(nodes[i].ID)
		_ = b.departedCount()
	}

	<-done

	if got := b.size(); got < 1 {
		t.Errorf("baseline should remain sane under concurrent use, got %d", got)
	}
}

// --- auto-shrink ---

const shrinkDwell = 200 * time.Millisecond

func TestAutoShrinkFollowsSingleLoss(t *testing.T) {
	b := newBaselineTracker(stability, shrinkDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(5)
	b.observe(nodes, now)
	if got := b.size(); got != 5 {
		t.Fatalf("expected seed of 5, got %d", got)
	}

	// One node goes missing silently.
	survivors := nodes[:4]
	b.observe(survivors, now)

	// Not yet — the dwell has to elapse.
	b.observe(survivors, now.Add(shrinkDwell/2))
	if got := b.size(); got != 5 {
		t.Errorf("shrink happened too early, baseline %d", got)
	}

	b.observe(survivors, now.Add(shrinkDwell+time.Millisecond))
	if got := b.size(); got != 4 {
		t.Errorf("expected the baseline to follow down to 4, got %d", got)
	}
}

// A larger shortfall must be left alone: it cannot be told apart from a partition
// of that size.
func TestAutoShrinkIgnoresMultiNodeLoss(t *testing.T) {
	b := newBaselineTracker(stability, shrinkDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(9)
	b.observe(nodes, now)

	// Three vanish at once.
	survivors := nodes[:6]
	for i := 0; i < 30; i++ {
		b.observe(survivors, now.Add(time.Duration(i)*shrinkDwell))
	}

	if got := b.size(); got != 9 {
		t.Errorf("a three-node shortfall must not shrink the baseline, got %d", got)
	}
}

// Losing nodes one at a time should walk the baseline down in steps.
func TestAutoShrinkWalksDownOneAtATime(t *testing.T) {
	b := newBaselineTracker(stability, shrinkDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(6)
	b.observe(nodes, now)

	for want := 5; want >= 2; want-- {
		survivors := nodes[:want]
		b.observe(survivors, now)
		now = now.Add(shrinkDwell + time.Millisecond)
		b.observe(survivors, now)

		if got := b.size(); got != want {
			t.Fatalf("expected the baseline to step to %d, got %d", want, got)
		}
	}
}

// A single step must not cascade: after following down by one, the baseline
// should hold until another node actually goes missing.
func TestAutoShrinkDoesNotCascade(t *testing.T) {
	b := newBaselineTracker(stability, shrinkDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(5)
	b.observe(nodes, now)

	survivors := nodes[:4]
	b.observe(survivors, now)
	now = now.Add(shrinkDwell + time.Millisecond)
	b.observe(survivors, now)

	if got := b.size(); got != 4 {
		t.Fatalf("expected 4, got %d", got)
	}

	// Sit at 4 for a long time — the baseline must stay at 4.
	for i := 0; i < 20; i++ {
		now = now.Add(shrinkDwell)
		b.observe(survivors, now)
	}

	if got := b.size(); got != 4 {
		t.Errorf("the baseline cascaded to %d; it should have held at 4", got)
	}
}

func TestAutoShrinkCanBeDisabled(t *testing.T) {
	b := newBaselineTracker(stability, shrinkDwell, false, nil)
	now := time.Now()

	nodes := mkNodes(5)
	b.observe(nodes, now)

	survivors := nodes[:4]
	for i := 0; i < 20; i++ {
		b.observe(survivors, now.Add(time.Duration(i)*shrinkDwell))
	}

	if got := b.size(); got != 5 {
		t.Errorf("auto-shrink is disabled, baseline should hold at 5, got %d", got)
	}
}

// A brief dip must not shrink the baseline — the dwell has to be continuous.
func TestAutoShrinkRequiresContinuousDwell(t *testing.T) {
	b := newBaselineTracker(stability, shrinkDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(5)
	b.observe(nodes, now)

	for i := 0; i < 10; i++ {
		// Flap between 4 and 5, never holding either long enough.
		b.observe(nodes[:4], now)
		now = now.Add(shrinkDwell / 3)
		b.observe(nodes, now)
		now = now.Add(shrinkDwell / 3)
	}

	if got := b.size(); got != 5 {
		t.Errorf("a flapping count must not shrink the baseline, got %d", got)
	}
}

// The safety property for auto-shrink, checked exhaustively.
//
// A step only applies when exactly one node is missing. For both sides of a split
// to shrink, each must see its own baseline minus one, which needs A = B = N-1
// with A+B = N — only true at N=2. This sweeps every split of every cluster size
// and confirms no configuration lets both sides reach quorum, given a floor above
// half.
func TestAutoShrinkNeverAllowsTwoLeaders(t *testing.T) {
	for n := 2; n <= 12; n++ {
		floor := n/2 + 1

		for a := 0; a <= n; a++ {
			bSide := n - a

			// Each side runs its own tracker, seeded at the full cluster size,
			// then settles at what it can see for a long time.
			trackerFor := func(visible int) *LeaderElection {
				bt := newBaselineTracker(stability, shrinkDwell, true, nil)
				full := mkNodes(n)
				start := time.Now()
				bt.observe(full, start)

				side := full[:visible]
				for i := 1; i <= 40; i++ {
					bt.observe(side, start.Add(time.Duration(i)*(shrinkDwell+time.Millisecond)))
				}
				return &LeaderElection{
					config:   &Config{MinClusterSize: floor},
					baseline: bt,
				}
			}

			leA := trackerFor(a)
			leB := trackerFor(bSide)

			aElects := a >= leA.calculateQuorumForNodes(a)
			bElects := bSide >= leB.calculateQuorumForNodes(bSide)

			if aElects && bElects {
				t.Errorf("N=%d floor=%d split %d/%d: both sides elect (baselines %d/%d)",
					n, floor, a, bSide, leA.baseline.size(), leB.baseline.size())
			}
		}
	}
}

// Auto-shrink must not drag the baseline below one.
func TestAutoShrinkFloorsAtOne(t *testing.T) {
	b := newBaselineTracker(stability, shrinkDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(2)
	b.observe(nodes, now)

	for i := 0; i < 20; i++ {
		now = now.Add(shrinkDwell + time.Millisecond)
		b.observe(nodes[:1], now)
	}

	if got := b.size(); got < 1 {
		t.Errorf("baseline fell below 1, got %d", got)
	}
}

// Growth after a shrink should still work.
func TestAutoShrinkThenGrowthRecovers(t *testing.T) {
	b := newBaselineTracker(stability, shrinkDwell, true, nil)
	now := time.Now()

	nodes := mkNodes(5)
	b.observe(nodes, now)

	// Shrink to 4.
	b.observe(nodes[:4], now)
	now = now.Add(shrinkDwell + time.Millisecond)
	b.observe(nodes[:4], now)
	if got := b.size(); got != 4 {
		t.Fatalf("expected 4, got %d", got)
	}

	// The node returns and settles.
	now = now.Add(10 * time.Millisecond)
	b.observe(nodes, now)
	now = now.Add(stability + time.Millisecond)
	b.observe(nodes, now)

	if got := b.size(); got != 5 {
		t.Errorf("expected recovery to 5, got %d", got)
	}
}
