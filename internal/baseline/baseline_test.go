package baseline

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/internal/clustertest"
)

const (
	stability = 100 * time.Millisecond
	// Long enough that a test never trips auto-shrink by accident.
	longDwell   = time.Hour
	shrinkDwell = 200 * time.Millisecond
)

func newTracker() *Tracker { return New(stability, longDwell, true, nil) }

// --- seeding and growth ---

func TestSeedsImmediately(t *testing.T) {
	b := newTracker()
	b.Observe(clustertest.Nodes(3), time.Now())

	// A starting cluster must be usable at once rather than waiting a whole
	// stability period before any leader can be elected.
	if got := b.Size(); got != 3 {
		t.Errorf("expected the baseline to seed at 3, got %d", got)
	}
}

func TestGrowthRequiresStability(t *testing.T) {
	b := newTracker()
	now := time.Now()

	nodes3 := clustertest.Nodes(3)
	b.Observe(nodes3, now)

	// Cluster grows to 5, but not yet steady.
	nodes5 := append(nodes3, clustertest.Nodes(2)...)
	b.Observe(nodes5, now)
	if got := b.Size(); got != 3 {
		t.Errorf("growth should not be adopted immediately, got %d", got)
	}

	// Still inside the stability window.
	b.Observe(nodes5, now.Add(stability/2))
	if got := b.Size(); got != 3 {
		t.Errorf("growth adopted too early, got %d", got)
	}

	// Steady for long enough.
	b.Observe(nodes5, now.Add(stability+time.Millisecond))
	if got := b.Size(); got != 5 {
		t.Errorf("expected the baseline to reach 5, got %d", got)
	}
}

// A count that flaps must never be adopted: this is what stops a node that
// appears for a moment from ratcheting quorum permanently upward.
func TestRejectsTransientSpike(t *testing.T) {
	b := newTracker()
	now := time.Now()

	nodes3 := clustertest.Nodes(3)
	b.Observe(nodes3, now)

	spike := append(nodes3, clustertest.Nodes(6)...) // brief jump to 9
	b.Observe(spike, now.Add(10*time.Millisecond))
	b.Observe(nodes3, now.Add(20*time.Millisecond)) // gone again
	b.Observe(nodes3, now.Add(30*time.Millisecond))
	if got := b.Size(); got != 3 {
		t.Errorf("a transient spike must not raise the baseline, got %d", got)
	}

	// And the settled count should not lower it either.
	b.Observe(nodes3, now.Add(10*stability))
	if got := b.Size(); got != 3 {
		t.Errorf("baseline should remain 3, got %d", got)
	}
}

// --- shrinkage: the dangerous direction ---

// A silent disappearance (crash or partition) must NOT lower the baseline.
func TestIgnoresSilentDisappearance(t *testing.T) {
	b := newTracker()
	now := time.Now()

	nodes := clustertest.Nodes(9)
	b.Observe(nodes, now)

	// Three vanish with no announcement, and stay gone well past stability.
	for i := range 20 {
		b.Observe(nodes[:6], now.Add(time.Duration(i)*stability))
	}
	if got := b.Size(); got != 9 {
		t.Errorf("a silent loss must not lower the baseline (a partition looks identical), got %d", got)
	}
}

// A graceful leave is an explicit signal and lowers the baseline, once per node.
// Whether a departure is relevant to an election is the election's call (see
// wasEligibleForBaseline in the leader package): the tracker discounts what it
// is told.
func TestGracefulDeparture(t *testing.T) {
	b := newTracker()
	nodes := clustertest.Nodes(9)
	b.Observe(nodes, time.Now())

	for i, want := range []int{8, 7} {
		if !b.NoteGracefulDeparture(nodes[i].ID) {
			t.Fatalf("departure %d of an eligible node should count", i)
		}
		if got := b.Size(); got != want {
			t.Errorf("after %d departures expected %d, got %d", i+1, want, got)
		}
	}

	if b.NoteGracefulDeparture(nodes[0].ID) {
		t.Error("the same departure must not be counted twice")
	}
	if got := b.Size(); got != 7 {
		t.Errorf("expected 7, got %d", got)
	}
}

func TestNeverDropsBelowOne(t *testing.T) {
	b := newTracker()
	nodes := clustertest.Nodes(3)
	b.Observe(nodes, time.Now())

	for _, n := range nodes {
		b.NoteGracefulDeparture(n.ID)
	}
	if got := b.Size(); got != 1 {
		t.Errorf("baseline must stay at 1, got %d", got)
	}
}

// --- rolling restarts must not leak ---

// The failure mode a monotonic departed-counter would have had: repeated
// leave/rejoin cycles driving the baseline to zero.
func TestRollingRestartDoesNotLeak(t *testing.T) {
	b := newTracker()
	now := time.Now()

	nodes := clustertest.Nodes(5)
	b.Observe(nodes, now)

	for cycle := range 10 {
		target := nodes[cycle%len(nodes)]

		// Announce and depart.
		b.NoteGracefulDeparture(target.ID)

		remaining := make([]*gossip.Node, 0, len(nodes)-1)
		for _, n := range nodes {
			if n.ID != target.ID {
				remaining = append(remaining, n)
			}
		}
		now = now.Add(10 * time.Millisecond)
		b.Observe(remaining, now)
		if got := b.Size(); got != 4 {
			t.Fatalf("cycle %d: expected 4 while the node is away, got %d", cycle, got)
		}

		// It comes back and stays.
		now = now.Add(10 * time.Millisecond)
		b.Observe(nodes, now)
		now = now.Add(stability + time.Millisecond)
		b.Observe(nodes, now)
		if got := b.Size(); got != 5 {
			t.Fatalf("cycle %d: expected recovery to 5, got %d (leak)", cycle, got)
		}
		if got := b.DepartedCount(); got != 0 {
			t.Fatalf("cycle %d: departed set should be empty, holds %d", cycle, got)
		}
	}
}

// A rejoin clears the mark, so a later departure counts again.
func TestRejoinRearmsDeparture(t *testing.T) {
	b := newTracker()
	now := time.Now()

	nodes := clustertest.Nodes(5)
	b.Observe(nodes, now)

	b.NoteGracefulDeparture(nodes[0].ID)
	if got := b.Size(); got != 4 {
		t.Fatalf("expected 4, got %d", got)
	}

	// Rejoins and settles.
	b.Observe(nodes, now.Add(10*time.Millisecond))
	b.Observe(nodes, now.Add(stability+20*time.Millisecond))
	if got := b.Size(); got != 5 {
		t.Fatalf("expected recovery to 5, got %d", got)
	}

	if !b.NoteGracefulDeparture(nodes[0].ID) {
		t.Error("a departure after a rejoin should count again")
	}
	if got := b.Size(); got != 4 {
		t.Errorf("expected 4, got %d", got)
	}
}

// --- forget ---

func TestForgetDiscountsSilentNodes(t *testing.T) {
	b := newTracker()
	now := time.Now()

	nodes := clustertest.Nodes(9)
	b.Observe(nodes, now)

	// Three crash silently; the baseline holds, as it must.
	b.Observe(nodes[:6], now.Add(stability*3))
	if got := b.Size(); got != 9 {
		t.Fatalf("expected the baseline to hold at 9 after a silent loss, got %d", got)
	}

	// The operator asserts they are gone for good.
	for _, n := range nodes[6:] {
		if !b.Forget(n.ID) {
			t.Errorf("forget should discount %v", n.ID)
		}
	}
	if got := b.Size(); got != 6 {
		t.Errorf("expected the baseline to fall to 6 after forgetting, got %d", got)
	}

	// Forgetting twice must not double-count.
	if b.Forget(nodes[6].ID) {
		t.Error("forgetting twice must not double-count")
	}
	if got := b.Size(); got != 6 {
		t.Errorf("expected 6, got %d", got)
	}
}

// The tracker spends its decrement on any ID: knowing whether a node is
// genuinely a member is the caller's job. LeaderElection.ForgetNodeLocal gates
// on cluster knowledge, which is what stops a bogus forget broadcast from
// suppressing quorum.
func TestForgetAcceptsAnyID(t *testing.T) {
	b := newTracker()
	b.Observe(clustertest.Nodes(5), time.Now())

	if !b.Forget(gossip.NodeID(uuid.New())) {
		t.Error("the tracker itself does not filter; the caller must")
	}
	if got := b.Size(); got != 4 {
		t.Errorf("expected 4, got %d", got)
	}
}

// --- reset ---

func TestResetReDerives(t *testing.T) {
	b := newTracker()
	now := time.Now()

	nodes := clustertest.Nodes(9)
	b.Observe(nodes, now)
	b.NoteGracefulDeparture(nodes[0].ID)
	b.NoteGracefulDeparture(nodes[1].ID)
	if got := b.Size(); got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}

	b.Reset()
	if got := b.Size(); got != 0 {
		t.Errorf("reset should clear the baseline, got %d", got)
	}
	if got := b.DepartedCount(); got != 0 {
		t.Errorf("reset should clear departures, got %d", got)
	}

	// Re-seeds from what is visible now.
	b.Observe(nodes[:5], now.Add(time.Second))
	if got := b.Size(); got != 5 {
		t.Errorf("expected a re-seed at 5, got %d", got)
	}
}

// The tracker is touched from the election loop and from state-change
// callbacks at the same time.
func TestConcurrentAccess(t *testing.T) {
	b := newTracker()
	nodes := clustertest.Nodes(20)
	b.Observe(nodes, time.Now())

	done := make(chan struct{})
	go func() {
		defer close(done)
		now := time.Now()
		for range 500 {
			now = now.Add(time.Millisecond)
			b.Observe(nodes, now)
			_ = b.Size()
		}
	}()

	for i := range 20 {
		b.NoteGracefulDeparture(nodes[i].ID)
		_ = b.DepartedCount()
	}
	<-done

	if got := b.Size(); got < 1 {
		t.Errorf("baseline should remain sane under concurrent use, got %d", got)
	}
}

// --- auto-shrink ---

// step observes the first visible nodes of the seeded cluster after the delay
// since the previous step and expects the baseline to be want.
type step struct {
	visible int
	after   time.Duration
	want    int
}

func repeat(n int, steps ...step) []step {
	var out []step
	for range n {
		out = append(out, steps...)
	}
	return out
}

func TestAutoShrink(t *testing.T) {
	d := shrinkDwell
	cases := []struct {
		name       string
		seed       int
		autoShrink bool
		steps      []step
	}{
		{"follows a single loss after the dwell", 5, true, []step{
			{4, 0, 5}, {4, d / 2, 5}, {4, d/2 + time.Millisecond, 4},
		}},
		// A larger shortfall cannot be told apart from a partition of that size.
		{"ignores a multi-node loss", 9, true, repeat(30, step{6, d, 9})},
		{"walks down one node at a time", 6, true, []step{
			{5, 0, 6}, {5, d + time.Millisecond, 5},
			{4, 0, 5}, {4, d + time.Millisecond, 4},
			{3, 0, 4}, {3, d + time.Millisecond, 3},
			{2, 0, 3}, {2, d + time.Millisecond, 2},
		}},
		// After following down by one, the baseline holds until another node
		// actually goes missing.
		{"does not cascade", 5, true, append([]step{
			{4, 0, 5}, {4, d + time.Millisecond, 4},
		}, repeat(20, step{4, d, 4})...)},
		{"can be disabled", 5, false, repeat(20, step{4, d, 5})},
		// The dwell has to be continuous: a count flapping between 4 and 5 never
		// holds either long enough.
		{"requires a continuous dwell", 5, true, repeat(10, step{4, d / 3, 5}, step{5, d / 3, 5})},
		{"floors at one", 2, true, append([]step{
			{1, d + time.Millisecond, 2}, {1, d + time.Millisecond, 1},
		}, repeat(18, step{1, d + time.Millisecond, 1})...)},
		{"growth still works after a shrink", 5, true, []step{
			{4, 0, 5}, {4, d + time.Millisecond, 4},
			{5, 10 * time.Millisecond, 4}, {5, stability + time.Millisecond, 5},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := New(stability, shrinkDwell, tc.autoShrink, nil)
			nodes := clustertest.Nodes(tc.seed)
			now := time.Now()
			b.Observe(nodes, now)
			if got := b.Size(); got != tc.seed {
				t.Fatalf("expected a seed of %d, got %d", tc.seed, got)
			}

			for i, s := range tc.steps {
				now = now.Add(s.after)
				b.Observe(nodes[:s.visible], now)
				if got := b.Size(); got != s.want {
					t.Fatalf("step %d (%d visible after %v): baseline %d, want %d", i, s.visible, s.after, got, s.want)
				}
			}
		})
	}
}

// --- a pinned floor (Config.ExpectedClusterSize) ---

func TestFloor(t *testing.T) {
	t.Run("reported before any observation", func(t *testing.T) {
		b := newTracker()
		b.SetFloor(5)
		if got := b.Size(); got != 5 {
			t.Fatalf("a pinned baseline should report the floor before any observation, got %d", got)
		}
	})

	// A node restarting alone must not seed a baseline of one.
	t.Run("seeds a restarted node", func(t *testing.T) {
		b := newTracker()
		b.SetFloor(3)
		b.Observe(clustertest.Nodes(1), time.Now())
		if got := b.Size(); got != 3 {
			t.Fatalf("expected the restarted node's baseline to be pinned at 3, got %d", got)
		}
	})

	t.Run("blocks graceful shrink below it", func(t *testing.T) {
		b := newTracker()
		b.SetFloor(3)
		nodes := clustertest.Nodes(5)
		b.Observe(nodes, time.Now())

		// Leaves above the floor still lower the baseline...
		b.NoteGracefulDeparture(nodes[0].ID)
		b.NoteGracefulDeparture(nodes[1].ID)
		if got := b.Size(); got != 3 {
			t.Fatalf("expected baseline 3 after two leaves from 5, got %d", got)
		}
		// ...but never below it.
		b.NoteGracefulDeparture(nodes[2].ID)
		b.NoteGracefulDeparture(nodes[3].ID)
		if got := b.Size(); got != 3 {
			t.Fatalf("leaves dropped the pinned baseline to %d", got)
		}
	})

	t.Run("blocks forget and reset", func(t *testing.T) {
		b := newTracker()
		b.SetFloor(3)
		nodes := clustertest.Nodes(3)
		b.Observe(nodes, time.Now())

		b.Forget(nodes[0].ID)
		if got := b.Size(); got != 3 {
			t.Fatalf("Forget dropped the pinned baseline to %d", got)
		}

		b.Reset()
		b.Observe(clustertest.Nodes(1), time.Now())
		if got := b.Size(); got != 3 {
			t.Fatalf("Reset lost the floor: baseline %d", got)
		}
	})

	t.Run("blocks auto-shrink", func(t *testing.T) {
		const dwell = 50 * time.Millisecond
		b := New(stability, dwell, true, nil)
		b.SetFloor(3)
		nodes := clustertest.Nodes(3)
		now := time.Now()
		b.Observe(nodes, now)

		// One node missing, steady for well past the dwell.
		b.Observe(nodes[:2], now.Add(time.Millisecond))
		b.Observe(nodes[:2], now.Add(10*dwell))
		b.Observe(nodes[:2], now.Add(20*dwell))
		if got := b.Size(); got != 3 {
			t.Fatalf("auto-shrink dropped the pinned baseline to %d", got)
		}
	})

	t.Run("allows growth and shrink back to it", func(t *testing.T) {
		b := newTracker()
		b.SetFloor(3)
		now := time.Now()
		nodes := clustertest.Nodes(4)
		b.Observe(nodes[:3], now)

		// Growth above the floor is still adopted after the stability period.
		b.Observe(nodes, now.Add(time.Millisecond))
		b.Observe(nodes, now.Add(2*stability))
		if got := b.Size(); got != 4 {
			t.Fatalf("expected growth to 4, got %d", got)
		}

		b.NoteGracefulDeparture(nodes[3].ID)
		if got := b.Size(); got != 3 {
			t.Fatalf("expected the baseline back at the floor (3), got %d", got)
		}
	})

	t.Run("unset leaves the baseline adaptive", func(t *testing.T) {
		b := newTracker()
		b.SetFloor(0)
		b.Observe(clustertest.Nodes(1), time.Now())
		if got := b.Size(); got != 1 {
			t.Fatalf("an unset floor should leave the baseline adaptive, got %d", got)
		}
		if got := b.Floor(); got != 0 {
			t.Fatalf("Floor() = %d, want 0", got)
		}
	})
}
