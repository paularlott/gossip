package leader_test

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/internal/clustertest"
	"github.com/paularlott/gossip/leader"
	"github.com/stretchr/testify/require"
)

// A scoped election (MetadataCriteria) counts what an unscoped one does: alive
// nodes only. A matching node that has gone suspect is not a candidate (its node
// group still tracks it until it is declared dead).
func TestScopedElectionLeavesOutSuspectNodes(t *testing.T) {
	nodes := clustertest.Build(t, 3, clustertest.WithMeta(map[string]string{"role": "worker"}))

	cfg := leader.DefaultConfig()
	cfg.MetadataCriteria = map[string]string{"role": "worker"}
	el := leader.NewLeaderElection(nodes[0].Cluster, cfg)
	el.Start()
	t.Cleanup(el.Stop)

	victim := nodes[2]
	isCandidate := func() bool {
		for _, n := range el.Candidates() {
			if n.ID == victim.ID() {
				return true
			}
		}
		return false
	}
	require.Eventually(t, isCandidate, 10*time.Second, 50*time.Millisecond, "the third worker never became a candidate")

	victim.Net.Cut() // silent: it goes suspect, then dead

	// Suspect lasts until DeadNodeTimeout, far longer than the poll interval.
	deadline := time.Now().Add(10 * time.Second)
	for {
		n := nodes[0].Cluster.GetNode(victim.ID())
		require.NotNil(t, n)
		if n.Suspect() {
			require.True(t, el.GetNodeGroup().Contains(victim.ID()), "the node group should still track a suspect node")
			require.False(t, isCandidate(), "a suspect node is a candidate of the scoped election")
			return
		}
		require.Falsef(t, n.DeadOrLeft(), "the node went %v without being observed suspect", n.GetObservedState())
		require.True(t, time.Now().Before(deadline), "the node never went suspect")
		time.Sleep(10 * time.Millisecond)
	}
}
