package gossip

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetNodesByTag(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) { c.Tags = []string{"taga", "tagb"} })
	node1 := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8001", []string{"taga", "tagb"})
	node2 := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8002", []string{"tagb", "tagc"})
	node3 := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8003", []string{"tagc"})
	dead := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8004", []string{"taga"})
	dead.observedState = NodeDead
	for _, n := range []*Node{node1, node2, node3, dead} {
		c.nodes.addOrUpdate(n)
	}

	assert.Equal(t, []string{"taga", "tagb"}, c.LocalNode().GetTags(), "the configured tags")
	assert.ElementsMatch(t, []*Node{c.localNode, node1}, c.GetNodesByTag("taga"), "a dead node is not listed")
	assert.ElementsMatch(t, []*Node{c.localNode, node1, node2}, c.GetNodesByTag("tagb"))
	assert.ElementsMatch(t, []*Node{node2, node3}, c.GetNodesByTag("tagc"))
	assert.Empty(t, c.GetNodesByTag("tagd"))
}

// Picking from a large cluster the few nodes that have a tag finds them, however rare, and never returns more than
// there are.
func TestGetRandomNodesWithTagSparse(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) { c.Tags = []string{"rare"} })
	rare := 1 // the local node
	for i := range 1000 {
		tag := "common"
		if i%100 == 0 {
			tag = "rare"
			rare++
		}
		c.nodes.addOrUpdate(newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8000", []string{tag}))
	}

	for _, tt := range []struct {
		tag  string
		k    int
		want int
	}{
		{"rare", 8, 8},
		{"rare", 20, rare},
		{"common", 50, 50},
	} {
		got := c.nodes.getRandomNodesWithTag(tt.k, tt.tag, nil)
		assert.Len(t, got, tt.want, "%d of %q", tt.k, tt.tag)
		for _, n := range got {
			assert.True(t, n.HasTag(tt.tag))
		}
	}
}

func TestGetRandomNodesWithTagExclude(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) { c.Tags = []string{"test"} })
	ids := make([]NodeID, 10)
	for i := range ids {
		ids[i] = NodeID(uuid.New())
		c.nodes.addOrUpdate(newNodeWithTags(ids[i], "127.0.0.1:8000", []string{"test"}))
	}

	got := c.nodes.getRandomNodesWithTag(5, "test", ids[:3])

	assert.Len(t, got, 5, "from the 7 not excluded and the local node")
	for _, n := range got {
		assert.NotContains(t, ids[:3], n.ID)
	}
}

// Joining brings the node's tags across, both ways.
func TestTagTransferOnJoin(t *testing.T) {
	a := startLiveCluster(t, func(c *Config) { c.Tags = []string{"taga", "tagb"} })
	b := startLiveCluster(t, func(c *Config) { c.Tags = []string{"tagb", "tagc"} })

	joinAndWait(t, a, b)

	bOnA, aOnB := a.GetNode(b.LocalNode().ID), b.GetNode(a.LocalNode().ID)
	require.NotNil(t, bOnA)
	require.NotNil(t, aOnB)
	assert.ElementsMatch(t, []string{"tagb", "tagc"}, bOnA.GetTags())
	assert.ElementsMatch(t, []string{"taga", "tagb"}, aOnB.GetTags())
	assert.Len(t, a.GetNodesByTag("taga"), 1)
	assert.Len(t, a.GetNodesByTag("tagb"), 2)
	assert.Len(t, a.GetNodesByTag("tagc"), 1)
	assert.Len(t, b.GetNodesByTag("tagc"), 1)
}
