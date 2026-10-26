package leader

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/internal/baseline"
	"github.com/paularlott/gossip/internal/clustertest"
)

const (
	stability = 100 * time.Millisecond
	// Long enough that a test never trips auto-shrink by accident.
	longDwell   = time.Hour
	shrinkDwell = 200 * time.Millisecond
)

// newQuorumTestElection builds a LeaderElection with only the fields
// calculateQuorumForNodes needs, so quorum arithmetic can be tested in isolation.
func newQuorumTestElection(minSize int) *LeaderElection {
	return &LeaderElection{
		config: &Config{
			MinClusterSize: minSize,
		},
	}
}

func TestQuorumValues(t *testing.T) {
	cases := []struct {
		name     string
		floor    int
		observed int
		want     int
	}{
		// Without a floor, quorum derived from the observed count is a strict
		// majority of it: otherwise two disjoint groups could both claim consensus.
		{"no floor, empty cluster", 0, 0, 0},
		{"no floor, 1 node", 0, 1, 1},
		{"no floor, 2 nodes", 0, 2, 2},
		{"no floor, 3 nodes", 0, 3, 2},
		{"no floor, 4 nodes", 0, 4, 3},
		{"no floor, 5 nodes", 0, 5, 3},
		{"no floor, 6 nodes", 0, 6, 4},
		{"no floor, 10 nodes", 0, 10, 6},
		{"no floor, 100 nodes", 0, 100, 51},
		// A 5-node cluster has a floor of 3: the majority side can elect, the
		// minority side and a lone node cannot.
		{"floor 3, sees 5", 3, 5, 3},
		{"floor 3, sees 4", 3, 4, 3},
		{"floor 3, sees 3", 3, 3, 3},
		{"floor 3, sees 2", 3, 2, 3},
		{"floor 3, sees 1", 3, 1, 3},
		// The observed-majority term still applies above the floor.
		{"floor 3, sees 9", 3, 9, 5},
		// The floor is constant: it does not shrink with the view.
		{"floor 4, sees 0", 4, 0, 4},
		{"floor 4, sees 3", 4, 3, 4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := newQuorumTestElection(tc.floor).calculateQuorumForNodes(tc.observed)
			if got != tc.want {
				t.Errorf("quorum %d, want %d", got, tc.want)
			}
			if tc.observed > 0 && got*2 <= tc.observed {
				t.Errorf("quorum %d is not a strict majority of %d", got, tc.observed)
			}
		})
	}
}

// TestQuorumFloorMustExceedHalf is the regression test for the bug that a
// view-dependent quorum base introduced.
//
// An earlier implementation computed quorum from max(observed, configuredSize),
// which made the threshold vary with each node's local view. That is unsafe once
// the cluster outgrows the configuration: with a configured size of 5 in a
// 9-node cluster, a 4/5 split had both sides computing a threshold of 3, and both
// elected.
//
// The floor is now a constant, so the rule is simply MinClusterSize > N/2. This
// sweeps actual cluster sizes against floors and asserts that relationship is
// exactly what separates safe from unsafe: a floor above half admits no split
// with two leaders, and any other floor (including none) admits one. The unsafe
// half documents why the floor must be raised when the cluster is scaled out.
func TestQuorumFloorMustExceedHalf(t *testing.T) {
	for actualN := 1; actualN <= 12; actualN++ {
		for floor := 0; floor <= actualN; floor++ {
			le := newQuorumTestElection(floor)

			bothElected := false
			for a := 0; a <= actualN; a++ {
				b := actualN - a
				if a >= le.calculateQuorumForNodes(a) && b >= le.calculateQuorumForNodes(b) {
					bothElected = true
					break
				}
			}

			if safe := floor*2 > actualN; bothElected == safe {
				t.Errorf("N=%d floor=%d: a floor above half is %v, but a split electing two leaders is %v",
					actualN, floor, safe, bothElected)
			}
		}
	}
}

// A graceful drain should walk quorum down in step, and every intermediate state
// must still be split-safe for the cluster size actually running at that point.
func TestBaselineDrainKeepsQuorumSafe(t *testing.T) {
	b := baseline.New(stability, longDwell, true, nil)
	now := time.Now()

	nodes := clustertest.Nodes(9)
	b.Observe(nodes, now)

	le := &LeaderElection{
		config:   &Config{MinClusterSize: 0},
		baseline: b,
	}

	live := len(nodes)
	for i := range 6 {
		quorum := le.calculateQuorumForNodes(live)

		// No split of the currently live cluster may produce two leaders.
		for a := 0; a <= live; a++ {
			bSide := live - a
			if a >= quorum && bSide >= quorum {
				t.Fatalf("live=%d baseline=%d quorum=%d: split %d/%d elects twice",
					live, b.Size(), quorum, a, bSide)
			}
		}

		// Drain one more node.
		b.NoteGracefulDeparture(nodes[i].ID)
		live--
		now = now.Add(stability + time.Millisecond)
		b.Observe(nodes[i+1:], now)
	}
}

// A partition must never let the minority reach quorum, no matter how long it
// persists: the baseline holds because no departure was announced.
func TestBaselinePartitionMinorityStaysBlocked(t *testing.T) {
	b := baseline.New(stability, longDwell, true, nil)
	now := time.Now()

	nodes := clustertest.Nodes(9)
	b.Observe(nodes, now)

	le := &LeaderElection{
		config:   &Config{},
		baseline: b,
	}

	// The minority sees only its own 3 nodes, indefinitely.
	for i := range 50 {
		now = now.Add(stability)
		b.Observe(nodes[:3], now)

		if q := le.calculateQuorumForNodes(3); 3 >= q {
			t.Fatalf("iteration %d: minority of 3 reached quorum %d (baseline %d)", i, q, b.Size())
		}
	}

	// And the majority side can still lead.
	if q := le.calculateQuorumForNodes(6); 6 < q {
		t.Errorf("majority of 6 should reach quorum, needs %d (baseline %d)", q, b.Size())
	}
}

// The whole point of the adaptive term: growth is covered without re-tuning the
// floor. A floor set for a 5-node cluster must still be safe at 9.
func TestBaselineGrowthCoversStaleFloor(t *testing.T) {
	b := baseline.New(stability, longDwell, true, nil)
	now := time.Now()

	// Floor chosen when the cluster was 5.
	le := &LeaderElection{
		config:   &Config{MinClusterSize: 3},
		baseline: b,
	}

	// It has since grown to 9 and settled.
	nodes := clustertest.Nodes(9)
	b.Observe(nodes, now)
	b.Observe(nodes, now.Add(stability+time.Millisecond))
	if got := b.Size(); got != 9 {
		t.Fatalf("expected the baseline to reach 9, got %d", got)
	}

	// The 3/6 split that broke the old view-dependent rule.
	q3 := le.calculateQuorumForNodes(3)
	q6 := le.calculateQuorumForNodes(6)
	if 3 >= q3 && 6 >= q6 {
		t.Fatalf("a 3/6 split elects twice (q3=%d q6=%d baseline=%d): the stale floor was not covered",
			q3, q6, b.Size())
	}
	if 6 < q6 {
		t.Errorf("the majority side should still lead, needs %d", q6)
	}
}

// The safety property for auto-shrink, checked exhaustively.
//
// A step only applies when exactly one node is missing. For both sides of a split
// to shrink, each must see its own baseline minus one, which needs A = B = N-1
// with A+B = N, only true at N=2. This sweeps every split of every cluster size
// and confirms no configuration lets both sides reach quorum, given a floor above
// half.
func TestAutoShrinkNeverAllowsTwoLeaders(t *testing.T) {
	for n := 2; n <= 12; n++ {
		floor := n/2 + 1

		// Each side runs its own tracker, seeded at the full cluster size, then
		// settles at what it can see for a long time.
		electionSeeing := func(visible int) *LeaderElection {
			bt := baseline.New(stability, shrinkDwell, true, nil)
			full := clustertest.Nodes(n)
			start := time.Now()
			bt.Observe(full, start)
			for i := 1; i <= 40; i++ {
				bt.Observe(full[:visible], start.Add(time.Duration(i)*(shrinkDwell+time.Millisecond)))
			}
			return &LeaderElection{
				config:   &Config{MinClusterSize: floor},
				baseline: bt,
			}
		}

		for a := 0; a <= n; a++ {
			bSide := n - a
			leA := electionSeeing(a)
			leB := electionSeeing(bSide)

			aElects := a >= leA.calculateQuorumForNodes(a)
			bElects := bSide >= leB.calculateQuorumForNodes(bSide)
			if aElects && bElects {
				t.Errorf("N=%d floor=%d split %d/%d: both sides elect (baselines %d/%d)",
					n, floor, a, bSide, leA.baseline.Size(), leB.baseline.Size())
			}
		}
	}
}

// SetClusterSize moves the pinned baseline and the minimum while the election
// runs (a deliberate resize), in both directions, without waiting for evidence
// of departures.
func TestSetClusterSize(t *testing.T) {
	le := &LeaderElection{config: &Config{}, baseline: baseline.New(stability, longDwell, false, nil)}
	le.SetClusterSize(5, 3)
	le.baseline.Observe(clustertest.Nodes(5), time.Now())
	if b, q := le.BaselineSize(), le.WriteQuorum(); b != 5 || q != 3 {
		t.Fatalf("5 nodes: baseline %d, write quorum %d", b, q)
	}
	// shrink by one: the baseline follows at once, though five nodes were seen
	le.SetClusterSize(4, 3)
	if b, q := le.BaselineSize(), le.WriteQuorum(); b != 4 || q != 3 {
		t.Fatalf("shrunk to 4: baseline %d, write quorum %d", b, q)
	}
	le.baseline.Observe(clustertest.Nodes(4), time.Now())
	le.SetClusterSize(3, 2)
	le.baseline.Observe(clustertest.Nodes(3), time.Now())
	if b, q := le.BaselineSize(), le.WriteQuorum(); b != 3 || q != 2 {
		t.Fatalf("shrunk to 3: baseline %d, write quorum %d", b, q)
	}
	// grow by one before the new node is seen: pinned at the new size
	le.SetClusterSize(4, 3)
	le.baseline.Observe(clustertest.Nodes(3), time.Now())
	if b, q := le.BaselineSize(), le.WriteQuorum(); b != 4 || q != 3 {
		t.Fatalf("grown to 4: baseline %d, write quorum %d", b, q)
	}
	if got := le.calculateQuorumForNodes(3); got != 3 {
		t.Fatalf("grown to 4, 3 visible: %d needed to lead", got)
	}
}

// ResetQuorumBaseline discards the tracked baseline so the next observation
// re-derives it from what is visible.
func TestResetQuorumBaseline(t *testing.T) {
	le := &LeaderElection{config: &Config{}, baseline: baseline.New(stability, longDwell, false, nil)}
	nodes := clustertest.Nodes(5)
	le.baseline.Observe(nodes, time.Now())
	le.baseline.Forget(nodes[3].ID)
	le.baseline.Forget(nodes[4].ID)
	if got := le.BaselineSize(); got != 3 {
		t.Fatalf("expected 3 after forgetting two, got %d", got)
	}

	le.ResetQuorumBaseline()
	if got := le.BaselineSize(); got != 0 {
		t.Fatalf("reset should clear the baseline, got %d", got)
	}
	le.baseline.Observe(nodes, time.Now())
	if got := le.BaselineSize(); got != 5 {
		t.Errorf("expected a re-seed at the visible 5, got %d", got)
	}
}
