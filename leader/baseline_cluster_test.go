package leader_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/internal/clustertest"
	"github.com/paularlott/gossip/leader"
	"github.com/stretchr/testify/require"
)

// End-to-end checks that the quorum baseline is wired to real cluster events:
// a graceful leave lowers it, a crash does not, ForgetNode gives the operator a
// way to reclaim the availability a crash costs, and Config.ExpectedClusterSize
// pins it. The arithmetic itself is tested against the tracker directly (see
// internal/baseline), so these tests only need one cluster per behaviour.

type blNode struct {
	*clustertest.Node
	election *leader.LeaderElection
}

// blConfig is an election config with brisk checks; dwell is the auto-shrink
// dwell (zero derives it from the cluster's failure detection).
func blConfig(minSize int, dwell time.Duration) *leader.Config {
	ec := leader.DefaultConfig()
	ec.LeaderCheckInterval = 100 * time.Millisecond
	ec.LeaderTimeout = time.Second
	ec.MinClusterSize = minSize
	ec.StabilityPeriod = 300 * time.Millisecond
	ec.ShrinkDwell = dwell
	return ec
}

func startElection(n *blNode, ec *leader.Config) {
	n.election = leader.NewLeaderElection(n.Cluster, ec)
	n.election.Start()
	n.OnStop(n.election.Stop)
}

// blBuild starts count joined nodes, each running an election with ec.
func blBuild(t *testing.T, count int, ec *leader.Config) []*blNode {
	t.Helper()
	nodes := make([]*blNode, 0, count)
	for _, n := range clustertest.Build(t, count) {
		bn := &blNode{Node: n}
		startElection(bn, ec)
		nodes = append(nodes, bn)
	}
	return nodes
}

func waitBaseline(t *testing.T, n *blNode, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return n.election.BaselineSize() == want },
		20*time.Second, 50*time.Millisecond, "%s baseline is %d, expected %d", n.Label, n.election.BaselineSize(), want)
}

// waitDetected waits until the node sees exactly alive alive nodes: the crashed
// ones have been detected.
func waitDetected(t *testing.T, n *blNode, alive int) {
	t.Helper()
	require.Eventually(t, func() bool { return n.Cluster.NumAliveNodes() == alive },
		20*time.Second, 50*time.Millisecond, "%s never settled at %d alive nodes", n.Label, alive)
}

func waitAgreedLeader(t *testing.T, nodes []*blNode) {
	t.Helper()
	require.Eventually(t, func() bool {
		seen := map[gossip.NodeID]int{}
		for _, n := range nodes {
			if !n.election.HasLeader() {
				return false
			}
			seen[n.election.GetLeaderID()]++
		}
		return len(seen) == 1
	}, 15*time.Second, 100*time.Millisecond, "nodes failed to agree on a leader")
}

// A graceful Leave() is an explicit signal, so surviving nodes lower their
// baseline and with it the quorum. Without ExpectedClusterSize the write quorum
// follows the baseline too.
func TestClusterGracefulLeaveLowersBaseline(t *testing.T) {
	nodes := blBuild(t, 5, blConfig(2, 0))

	for _, n := range nodes {
		waitBaseline(t, n, 5)
		require.Equal(t, 3, n.election.QuorumSize(), "%s: quorum for a 5-node cluster", n.Label)
		require.Equal(t, 3, n.election.WriteQuorum(), "%s: write quorum for a 5-node cluster", n.Label)
	}

	for _, victim := range nodes[3:] {
		victim.Retire()
	}

	survivors := nodes[:3]
	for _, n := range survivors {
		waitBaseline(t, n, 3)
		require.Equal(t, 2, n.election.QuorumSize(), "%s: quorum after draining to 3", n.Label)
		require.Equal(t, 2, n.election.WriteQuorum(), "%s: write quorum after draining to 3", n.Label)
	}

	// And leadership must survive the resize.
	waitAgreedLeader(t, survivors)
}

// A hard stop produces no announcement, so the baseline must hold: a crash is
// indistinguishable from a partition and quorum stays conservative. Two nodes
// lost at once is a shortfall auto-shrink must not absorb either, however short
// its dwell.
func TestClusterCrashDoesNotLowerBaseline(t *testing.T) {
	nodes := blBuild(t, 5, blConfig(2, time.Second))
	for _, n := range nodes {
		waitBaseline(t, n, 5)
	}

	nodes[3].Crash()
	nodes[4].Crash()
	survivors := nodes[:3]
	for _, n := range survivors {
		waitDetected(t, n, 3)
	}

	// Two shrink dwells and several stability periods after the loss was detected.
	require.Never(t, func() bool {
		for _, n := range survivors {
			if n.election.BaselineSize() != 5 || n.election.QuorumSize() != 3 {
				return true
			}
		}
		return false
	}, 2*time.Second, 100*time.Millisecond, "a silent loss moved the baseline; it must hold at 5")
}

// One ForgetNode call must reach the whole cluster, since visiting every node to
// shrink a large cluster is not workable, and it gives back the availability a
// crash cost.
func TestClusterForgetNodeBroadcastsToAll(t *testing.T) {
	nodes := blBuild(t, 5, blConfig(2, time.Hour)) // auto-shrink effectively off
	for _, n := range nodes {
		waitBaseline(t, n, 5)
	}

	deadID := nodes[4].ID()
	nodes[4].Crash()
	survivors := nodes[:4]
	for _, n := range survivors {
		waitDetected(t, n, 4)
		// Baseline holds after a silent loss, as designed.
		require.Equal(t, 5, n.election.BaselineSize(), n.Label)
	}

	// A single call on ONE node propagates to the rest.
	require.True(t, survivors[0].election.ForgetNode(deadID), "ForgetNode should have applied locally")

	for _, n := range survivors {
		waitBaseline(t, n, 4)
		require.Equal(t, 3, n.election.QuorumSize(), "%s: quorum after the forget", n.Label)
		require.Eventually(t, func() bool { return n.Cluster.GetNode(deadID) == nil },
			10*time.Second, 50*time.Millisecond, "%s: the forgotten node should be gone from the node list", n.Label)
	}
}

func TestClusterForgetNodeLocalDoesNotBroadcast(t *testing.T) {
	nodes := blBuild(t, 3, blConfig(2, time.Hour))
	for _, n := range nodes {
		waitBaseline(t, n, 3)
	}

	deadID := nodes[2].ID()
	nodes[2].Crash()
	waitDetected(t, nodes[0], 2)

	require.True(t, nodes[0].election.ForgetNodeLocal(deadID))
	require.Equal(t, 2, nodes[0].election.BaselineSize(), "n0 should have discounted locally")

	// A broadcast would have arrived within a few gossip rounds.
	require.Never(t, func() bool { return nodes[1].election.BaselineSize() != 3 },
		time.Second, 100*time.Millisecond, "n1 should be unaffected by a local-only forget")
}

// A forget, local or broadcast, naming a node this cluster has never seen (or
// the local node itself) must be rejected outright. Every accepted forget spends
// a baseline decrement, so accepting unknown IDs would let any member (or any
// bug) suppress quorum by minting IDs.
func TestClusterForgetNodeRejectsInvalid(t *testing.T) {
	nodes := blBuild(t, 2, blConfig(0, time.Hour))
	for _, n := range nodes {
		waitBaseline(t, n, 2)
	}

	stranger := gossip.NodeID(uuid.New())
	for _, n := range nodes {
		require.False(t, n.election.ForgetNodeLocal(stranger), "%s: unknown node, local", n.Label)
		require.False(t, n.Cluster.ForgetNode(stranger), "%s: unknown node, cluster", n.Label)
		require.False(t, n.election.ForgetNodeLocal(n.ID()), "%s: itself, local", n.Label)
		require.False(t, n.Cluster.ForgetNode(n.ID()), "%s: itself, cluster", n.Label)
		require.NotNil(t, n.Cluster.GetNode(n.ID()), "%s: the local node should still be present", n.Label)
	}

	// The broadcast variant is equally inert: no peer's baseline may move.
	// (ForgetNode with the caller's own ID is deliberately not asserted: it
	// returns false locally but is still broadcast, and the peers apply it.)
	require.False(t, nodes[0].election.ForgetNode(stranger))
	require.Never(t, func() bool {
		return nodes[0].election.BaselineSize() != 2 || nodes[1].election.BaselineSize() != 2
	}, time.Second, 100*time.Millisecond, "baseline moved on an invalid forget; it must hold at 2")
}

// Crashing nodes one at a time, with a gap, walks the baseline down without any
// operator involvement.
func TestClusterAutoShrinkStepwise(t *testing.T) {
	nodes := blBuild(t, 5, blConfig(2, time.Second))
	for _, n := range nodes {
		waitBaseline(t, n, 5)
	}

	nodes[4].Crash()
	for _, n := range nodes[:4] {
		waitBaseline(t, n, 4)
		require.Equal(t, 3, n.election.QuorumSize(), "%s: quorum for a baseline of 4", n.Label)
	}

	nodes[3].Crash()
	for _, n := range nodes[:3] {
		waitBaseline(t, n, 3)
		require.Equal(t, 2, n.election.QuorumSize(), "%s: quorum for a baseline of 3", n.Label)
	}
}

// --- Config.ExpectedClusterSize: the pinned baseline survives graceful leaves,
// auto-shrink and restarts ---

func pinnedConfig(dwell time.Duration) *leader.Config {
	ec := blConfig(0, dwell) // the pin alone must supply the protection
	ec.ExpectedClusterSize = 3
	return ec
}

func isPinned(n *blNode, baseline, quorum int) bool {
	return n.election.BaselineSize() == baseline &&
		n.election.QuorumSize() >= quorum &&
		n.election.WriteQuorum() == quorum
}

// Graceful leaves (including the Leave that a clean shutdown performs) must
// not lower a pinned baseline, and neither may the auto-shrink dwell. The last
// survivor therefore cannot lead on its own.
func TestClusterExpectedSizeLeaveDoesNotShrink(t *testing.T) {
	const dwell = 400 * time.Millisecond
	nodes := blBuild(t, 3, pinnedConfig(dwell))
	for _, n := range nodes {
		waitBaseline(t, n, 3)
	}
	waitAgreedLeader(t, nodes)

	holdsPin := func(nodes []*blNode) bool {
		for _, n := range nodes {
			if !isPinned(n, 3, 2) {
				return false
			}
		}
		return true
	}

	nodes[2].Retire()
	// Long enough for the leave to propagate and for several shrink dwells.
	require.Never(t, func() bool { return !holdsPin(nodes[:2]) }, 3*dwell, 50*time.Millisecond)
	// Two of three is still a majority: leadership survives.
	waitAgreedLeader(t, nodes[:2])

	nodes[1].Retire()
	require.Never(t, func() bool { return !holdsPin(nodes[:1]) }, 3*dwell, 50*time.Millisecond)
	require.Eventually(t, func() bool { return !nodes[0].election.HasLeader() },
		10*time.Second, 100*time.Millisecond, "the lone survivor of a pinned 3-node cluster must lose leadership")
	require.False(t, nodes[0].election.IsLeader())
}

// A node that restarts and can, for the moment, see only itself must come up
// with the pinned baseline rather than seeding one from its lonely view, and
// therefore must not elect itself before it has rejoined.
func TestClusterExpectedSizeRestart(t *testing.T) {
	ec := pinnedConfig(time.Hour)
	nodes := blBuild(t, 3, ec)
	for _, n := range nodes {
		waitBaseline(t, n, 3)
	}
	waitAgreedLeader(t, nodes)

	// Clean shutdown, then restart on the same address with no peers yet.
	nodes[2].Retire()
	nodes[2] = &blNode{Node: clustertest.NewNode(t, "n2-restarted", clustertest.WithAddr(nodes[2].Addr))}
	startElection(nodes[2], ec)

	require.True(t, isPinned(nodes[2], 3, 2), "the restarted node's baseline is not pinned")
	require.Never(t, func() bool { return nodes[2].election.IsLeader() || nodes[2].election.HasLeader() },
		500*time.Millisecond, 50*time.Millisecond, "a restarted node alone elected a leader despite the pinned baseline")

	// The survivors kept their pinned baseline through the leave.
	for _, n := range nodes[:2] {
		require.True(t, isPinned(n, 3, 2), n.Label)
	}

	// Rejoin: everyone converges on one leader with the baseline still 3.
	nodes[2].Join(t, nodes[0].Node)
	waitAgreedLeader(t, nodes)
	for _, n := range nodes {
		require.True(t, isPinned(n, 3, 2), n.Label)
	}
}
