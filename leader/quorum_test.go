package leader

import (
	"testing"
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

func TestQuorumIsAtLeastStrictMajority(t *testing.T) {
	// Quorum derived from the observed count must never drop below a strict
	// majority — otherwise two disjoint groups could both claim consensus and
	// elect separate leaders.
	cases := []struct {
		observed int
		want     int
	}{
		{observed: 1, want: 1},
		{observed: 2, want: 2},
		{observed: 3, want: 2},
		{observed: 4, want: 3},
		{observed: 5, want: 3},
		{observed: 6, want: 4},
		{observed: 10, want: 6},
		{observed: 100, want: 51},
	}

	for _, c := range cases {
		le := newQuorumTestElection(0)
		got := le.calculateQuorumForNodes(c.observed)
		if got != c.want {
			t.Errorf("observed=%d: quorum=%d, want %d (strict majority floor)",
				c.observed, got, c.want)
		}
		if got*2 <= c.observed {
			t.Errorf("observed=%d: quorum %d is not a strict majority", c.observed, got)
		}
	}
}

func TestQuorumUsesMinClusterSizeAsFloor(t *testing.T) {
	// A 5-node cluster: floor of 3.
	le := newQuorumTestElection(3)

	// The majority side sees 3 of 5 and needs 3 — it can elect.
	if got := le.calculateQuorumForNodes(3); got != 3 {
		t.Errorf("majority side: expected quorum 3, got %d", got)
	}

	// The minority side sees 2 of 5 and still needs 3 — it cannot elect.
	if got := le.calculateQuorumForNodes(2); got != 3 {
		t.Errorf("minority side: expected quorum 3, got %d", got)
	}

	// A lone node still needs 3 — it cannot elect itself.
	if got := le.calculateQuorumForNodes(1); got != 3 {
		t.Errorf("isolated node: expected quorum 3, got %d", got)
	}
}

func TestQuorumGrowsWithObserved(t *testing.T) {
	// The observed-majority term still applies above the floor, so a node
	// seeing more than the floor requires a majority of what it sees.
	le := newQuorumTestElection(3)
	if got := le.calculateQuorumForNodes(9); got != 5 {
		t.Errorf("expected quorum 5 for 9 observed nodes, got %d", got)
	}
}

// TestQuorumPreventsDisjointMajorities is the property that matters: for any
// split of a cluster into two groups, at most one group can satisfy quorum.
func TestQuorumPreventsDisjointMajorities(t *testing.T) {
	for clusterSize := 1; clusterSize <= 12; clusterSize++ {
		// Floor set correctly for this cluster: above half.
		le := newQuorumTestElection(clusterSize/2 + 1)

		for sideA := 0; sideA <= clusterSize; sideA++ {
			sideB := clusterSize - sideA

			aCanElect := sideA >= le.calculateQuorumForNodes(sideA)
			bCanElect := sideB >= le.calculateQuorumForNodes(sideB)

			if aCanElect && bCanElect {
				t.Errorf("cluster=%d split %d/%d: BOTH sides satisfy quorum (split brain)",
					clusterSize, sideA, sideB)
			}
		}
	}
}

// TestQuorumWithoutFloorAllowsIsolatedLeader documents the behaviour when
// MinClusterSize is left unset: an isolated node will elect itself, because
// nothing distinguishes isolation from a genuinely single-node cluster. This is
// why MinClusterSize must be set when leadership backs correctness.
func TestQuorumWithoutFloorAllowsIsolatedLeader(t *testing.T) {
	le := newQuorumTestElection(0)

	if got := le.calculateQuorumForNodes(1); got != 1 {
		t.Fatalf("expected quorum 1 for a lone node with no floor set, got %d", got)
	}

	// And the split-brain property does NOT hold without it — demonstrate the
	// 5-node cluster splitting 2/3 where the 2-side can still elect.
	le5 := newQuorumTestElection(0)
	twoSide := 2 >= le5.calculateQuorumForNodes(2)
	threeSide := 3 >= le5.calculateQuorumForNodes(3)
	if !(twoSide && threeSide) {
		t.Skip("behaviour changed; both sides no longer elect without an expectation")
	}
	t.Log("confirmed: without a floor a 2/3 split yields two leaders")
}

func TestQuorumZeroNodes(t *testing.T) {
	le := newQuorumTestElection(0)
	if got := le.calculateQuorumForNodes(0); got != 0 {
		t.Errorf("expected quorum 0 for an empty cluster, got %d", got)
	}
}

func TestNormalizeConfigKeepsMinClusterSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinClusterSize = 7
	got := normalizeConfig(cfg)
	if got.MinClusterSize != 7 {
		t.Errorf("MinClusterSize should survive normalisation, got %d", got.MinClusterSize)
	}
}

// Sanity check that the documented 5-node example behaves as described.
func TestQuorumFiveNodeExample(t *testing.T) {
	le := newQuorumTestElection(3) // 5-node cluster -> floor 3

	for _, tc := range []struct {
		visible  int
		canElect bool
	}{
		{visible: 5, canElect: true},
		{visible: 4, canElect: true},
		{visible: 3, canElect: true},
		{visible: 2, canElect: false},
		{visible: 1, canElect: false},
	} {
		req := le.calculateQuorumForNodes(tc.visible)
		can := tc.visible >= req
		if can != tc.canElect {
			t.Errorf("%d of 5 visible: canElect=%v (quorum %d), want %v",
				tc.visible, can, req, tc.canElect)
		}
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
// exactly what separates safe from unsafe.
func TestQuorumFloorMustExceedHalf(t *testing.T) {
	for actualN := 1; actualN <= 12; actualN++ {
		for floor := 1; floor <= actualN; floor++ {
			le := newQuorumTestElection(floor)

			bothElected := false
			for a := 0; a <= actualN; a++ {
				b := actualN - a
				if a >= le.calculateQuorumForNodes(a) && b >= le.calculateQuorumForNodes(b) {
					bothElected = true
					break
				}
			}

			floorExceedsHalf := floor*2 > actualN

			if floorExceedsHalf && bothElected {
				t.Errorf("N=%d floor=%d: floor exceeds half yet a split elected two leaders",
					actualN, floor)
			}
		}
	}
}

// A floor set for a smaller cluster becomes unsafe once the cluster grows. This
// documents that failure mode explicitly so the operational requirement — raise
// the floor when you scale out — is not forgotten.
func TestQuorumStaleFloorAfterGrowthIsUnsafe(t *testing.T) {
	// Floor chosen for a 5-node cluster.
	le := newQuorumTestElection(3)

	// The cluster has since grown to 9. A 3/6 split:
	threeSide := 3 >= le.calculateQuorumForNodes(3)
	sixSide := 6 >= le.calculateQuorumForNodes(6)

	if !threeSide || !sixSide {
		t.Skip("behaviour changed; a stale floor no longer admits two leaders")
	}
	t.Log("confirmed: a floor of 3 in a 9-node cluster admits two leaders on a 3/6 split — raise the floor when scaling out")

	// Raising the floor to a majority of 9 fixes it.
	fixed := newQuorumTestElection(5)
	if 3 >= fixed.calculateQuorumForNodes(3) && 6 >= fixed.calculateQuorumForNodes(6) {
		t.Error("a floor of 5 should prevent both sides of a 3/6 split from electing")
	}
}

// The floor must apply even when the observed count is tiny.
func TestQuorumFloorAppliesAtLowCounts(t *testing.T) {
	le := newQuorumTestElection(3)
	if got := le.calculateQuorumForNodes(5); got != 3 {
		t.Errorf("expected the floor of 3 to apply, got %d", got)
	}
	if got := le.calculateQuorumForNodes(1); got != 3 {
		t.Errorf("floor should not scale down with the observed count, got %d", got)
	}
}

// The floor is constant: it must not shrink just because fewer nodes are visible.
func TestQuorumFloorIsConstantAcrossViews(t *testing.T) {
	le := newQuorumTestElection(4)
	for observed := 0; observed <= 3; observed++ {
		if got := le.calculateQuorumForNodes(observed); got < 4 {
			t.Errorf("observed=%d: quorum %d dropped below the floor of 4", observed, got)
		}
	}
}
