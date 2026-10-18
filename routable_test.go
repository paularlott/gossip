package gossip

import (
	"testing"

	"github.com/google/uuid"
)

func TestNodeRoutable(t *testing.T) {
	cases := map[NodeState]bool{
		NodeAlive:   true,
		NodeSuspect: true,
		NodeLeaving: false,
		NodeDead:    false,
		NodeRemoved: false,
	}
	for state, want := range cases {
		n := newNode(NodeID(uuid.New()), "addr")
		n.observedState = state
		if got := n.Routable(); got != want {
			t.Errorf("Routable() in state %s = %v, want %v", state, got, want)
		}
	}
}

func TestClusterRoutableNodes(t *testing.T) {
	c := newTestCluster(t)

	want := map[NodeID]bool{c.localNode.ID: true}
	for _, state := range []NodeState{NodeAlive, NodeAlive, NodeSuspect, NodeLeaving, NodeDead} {
		n := newNode(NodeID(uuid.New()), "addr")
		n.observedState = state
		c.nodes.addIfNotExists(n)
		if state == NodeAlive || state == NodeSuspect {
			want[n.ID] = true
		}
	}

	got := c.RoutableNodes()
	if len(got) != len(want) {
		t.Fatalf("RoutableNodes() returned %d nodes, want %d", len(got), len(want))
	}
	for _, n := range got {
		if !want[n.ID] {
			t.Errorf("RoutableNodes() returned %s in state %s", n.ID, n.GetObservedState())
		}
		if !n.Routable() {
			t.Errorf("RoutableNodes() returned a node that is not Routable(): %s", n.GetObservedState())
		}
	}
}
