package leader_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/leader"
)

// End-to-end checks for Config.ExpectedClusterSize: the pinned baseline must
// survive graceful leaves (Stop leaves), auto-shrink and restarts.

func expElection(n *blNode, expected int, dwell time.Duration) {
	ec := leader.DefaultConfig()
	ec.LeaderCheckInterval = 100 * time.Millisecond
	ec.LeaderTimeout = time.Second
	ec.MinClusterSize = 0 // the pin alone must supply the protection
	ec.StabilityPeriod = 300 * time.Millisecond
	ec.ShrinkDwell = dwell
	ec.ExpectedClusterSize = expected
	n.election = leader.NewLeaderElection(n.cluster, ec)
	n.election.Start()
}

func expBuild(t *testing.T, basePort, count, expected int, dwell time.Duration) []*blNode {
	t.Helper()
	seed := fmt.Sprintf("127.0.0.1:%d", basePort)
	nodes := make([]*blNode, 0, count)
	for i := 0; i < count; i++ {
		c, ct := blCluster(t, fmt.Sprintf("127.0.0.1:%d", basePort+i))
		c.Start()
		if i > 0 {
			if err := c.Join([]string{seed}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}
		nodes = append(nodes, &blNode{cluster: c, transport: ct, label: fmt.Sprintf("n%d", i)})
	}
	for _, n := range nodes {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) && n.cluster.NumAliveNodes() < count {
			time.Sleep(50 * time.Millisecond)
		}
		if n.cluster.NumAliveNodes() < count {
			t.Fatalf("%s only sees %d of %d nodes", n.label, n.cluster.NumAliveNodes(), count)
		}
	}
	for _, n := range nodes {
		expElection(n, expected, dwell)
	}
	return nodes
}

func liveTeardown(nodes []*blNode) {
	var live []*blNode
	for _, n := range nodes {
		if n.cluster != nil {
			live = append(live, n)
		}
	}
	blTeardown(live)
}

func waitAgreedLeader(t *testing.T, nodes []*blNode, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		seen := map[gossip.NodeID]int{}
		all := true
		for _, n := range nodes {
			if !n.election.HasLeader() {
				all = false
				break
			}
			seen[n.election.GetLeaderID()]++
		}
		if all && len(seen) == 1 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("nodes failed to agree on a leader")
}

func assertPinned(t *testing.T, n *blNode, baseline, quorum int) {
	t.Helper()
	if got := n.election.BaselineSize(); got != baseline {
		t.Errorf("%s: baseline %d, want %d", n.label, got, baseline)
	}
	if got := n.election.QuorumSize(); got < quorum {
		t.Errorf("%s: quorum %d, want at least %d", n.label, got, quorum)
	}
	if got := n.election.WriteQuorum(); got != quorum {
		t.Errorf("%s: write quorum %d, want %d", n.label, got, quorum)
	}
}

// Graceful leaves (including the Leave that a clean shutdown performs) must
// not lower a pinned baseline, and neither may the auto-shrink dwell. The last
// survivor therefore cannot lead on its own.
func TestClusterExpectedSizeLeaveDoesNotShrink(t *testing.T) {
	const dwell = 600 * time.Millisecond
	nodes := expBuild(t, 20300, 3, 3, dwell)
	defer liveTeardown(nodes)

	for _, n := range nodes {
		waitBaseline(t, n, 3, 10*time.Second)
	}
	waitAgreedLeader(t, nodes, 15*time.Second)

	nodes[2].retire()
	// Long enough for the leave to propagate and for several shrink dwells.
	time.Sleep(4 * dwell)
	for _, n := range nodes[:2] {
		assertPinned(t, n, 3, 2)
	}
	// Two of three is still a majority: leadership survives.
	waitAgreedLeader(t, nodes[:2], 15*time.Second)

	nodes[1].retire()
	time.Sleep(4 * dwell)
	assertPinned(t, nodes[0], 3, 2)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && nodes[0].election.HasLeader() {
		time.Sleep(100 * time.Millisecond)
	}
	if nodes[0].election.HasLeader() || nodes[0].election.IsLeader() {
		t.Fatal("the lone survivor of a pinned 3-node cluster must not hold leadership")
	}
}

// A node that restarts and can, for the moment, see only itself must come up
// with the pinned baseline rather than seeding one from its lonely view — and
// therefore must not elect itself before it has rejoined.
func TestClusterExpectedSizeRestart(t *testing.T) {
	nodes := expBuild(t, 20310, 3, 3, time.Hour)
	defer liveTeardown(nodes)

	for _, n := range nodes {
		waitBaseline(t, n, 3, 10*time.Second)
	}
	waitAgreedLeader(t, nodes, 15*time.Second)

	// Clean shutdown (Stop + Leave), then restart on the same address with no
	// peers yet.
	nodes[2].retire()
	c, ct := blCluster(t, "127.0.0.1:20312")
	c.Start()
	nodes[2] = &blNode{cluster: c, transport: ct, label: "n2-restarted"}
	expElection(nodes[2], 3, time.Hour)

	assertPinned(t, nodes[2], 3, 2)
	time.Sleep(1500 * time.Millisecond) // several election checks
	if nodes[2].election.IsLeader() || nodes[2].election.HasLeader() {
		t.Fatal("a restarted node alone elected a leader despite the pinned baseline")
	}

	// The survivors kept their pinned baseline through the leave.
	for _, n := range nodes[:2] {
		assertPinned(t, n, 3, 2)
	}

	// Rejoin: everyone converges on one leader with the baseline still 3.
	if err := c.Join([]string{"127.0.0.1:20310"}); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	waitAgreedLeader(t, nodes, 15*time.Second)
	for _, n := range nodes {
		assertPinned(t, n, 3, 2)
	}
}

// Without ExpectedClusterSize the existing adaptive behaviour is unchanged,
// and WriteQuorum tracks the adaptive baseline.
func TestClusterWriteQuorumUnpinned(t *testing.T) {
	nodes := blBuild(t, 20320, 3, 0)
	defer liveTeardown(nodes)

	for _, n := range nodes {
		waitBaseline(t, n, 3, 10*time.Second)
		if q := n.election.WriteQuorum(); q != 2 {
			t.Errorf("%s: write quorum %d, want 2", n.label, q)
		}
	}
	nodes[2].retire()
	for _, n := range nodes[:2] {
		waitBaseline(t, n, 2, 20*time.Second)
		if q := n.election.WriteQuorum(); q != 2 {
			t.Errorf("%s: write quorum %d after a leave from 3, want 2", n.label, q)
		}
	}
}
