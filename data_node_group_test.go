package gossip

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A DataNodeGroup keeps data of its own for each member, made by the initializer when the node joins the group (zero
// when there is none).
func TestDataNodeGroupData(t *testing.T) {
	west := map[string]string{"zone": "us-west"}

	t.Run("initializer", func(t *testing.T) {
		c := newUnitCluster(t)
		var added []*TestData
		dng := NewDataNodeGroup(c, west, &DataNodeGroupOptions[TestData]{
			DataInitializer: func(n *Node) *TestData { return &TestData{Value: "init-" + n.AdvertisedAddr(), Counter: 42} },
			OnNodeAdded:     func(_ *Node, d *TestData) { added = append(added, d) },
		})
		defer dng.Close()

		n := clusterNode(c, NodeAlive, "zone", "us-west")

		require.Equal(t, &TestData{Value: "init-127.0.0.1:8001", Counter: 42}, dng.GetNodeData(n.ID))
		assert.Equal(t, []*TestData{dng.GetNodeData(n.ID)}, added, "the callback gets the node's data")
		assert.Nil(t, dng.GetNodeData(NodeID{}), "a node that is not a member has no data")
	})

	t.Run("zero value without an initializer", func(t *testing.T) {
		c := newUnitCluster(t)
		dng := NewDataNodeGroup(c, west, &DataNodeGroupOptions[TestData]{})
		defer dng.Close()
		n := clusterNode(c, NodeAlive, "zone", "us-west")
		assert.Equal(t, &TestData{}, dng.GetNodeData(n.ID))
	})

	t.Run("update and read back", func(t *testing.T) {
		c := newUnitCluster(t)
		dng := NewDataNodeGroup(c, west, &DataNodeGroupOptions[TestData]{
			DataInitializer: func(*Node) *TestData { return &TestData{Value: "initial"} },
		})
		defer dng.Close()
		a, b := clusterNode(c, NodeAlive, "zone", "us-west"), clusterNode(c, NodeAlive, "zone", "us-west")

		require.NoError(t, dng.UpdateNodeData(a.ID, func(n *Node, d *TestData) error {
			assert.Same(t, a, n)
			d.Value, d.Counter = "updated", 100
			return nil
		}))
		assert.Equal(t, &TestData{Value: "updated", Counter: 100}, dng.GetNodeData(a.ID))
		assert.ElementsMatch(t, []*TestData{{Value: "updated", Counter: 100}, {Value: "initial"}}, dng.GetDataNodes())
		assert.Equal(t, &TestData{Value: "initial"}, dng.GetNodeData(b.ID), "the other node's data is untouched")

		boom := errors.New("boom")
		assert.Same(t, boom, dng.UpdateNodeData(b.ID, func(*Node, *TestData) error { return boom }), "the updater's error is returned")
		assert.NoError(t, dng.UpdateNodeData(NodeID{}, func(*Node, *TestData) error { return boom }), "a non-member is not updated, and not an error")
	})

	// A change to a suspect node's metadata still reaches OnNodeUpdated: suspect nodes stay in the group.
	t.Run("suspect node updates", func(t *testing.T) {
		c := newUnitCluster(t)
		updated := 0
		dng := NewDataNodeGroup(c, map[string]string{"role": "gateway"}, &DataNodeGroupOptions[int]{
			OnNodeUpdated: func(*Node, *int) { updated++ },
		})
		defer dng.Close()
		n := clusterNode(c, NodeAlive, "role", "gateway")
		c.nodes.updateState(n.ID, NodeSuspect)
		require.True(t, dng.Contains(n.ID))

		n.metadata.SetString("version", "2.0")
		c.nodes.notifyMetadataChanged(n)

		assert.Equal(t, 1, updated)
	})

	t.Run("data is dropped with the node", func(t *testing.T) {
		c := newUnitCluster(t)
		dng := NewDataNodeGroup(c, west, &DataNodeGroupOptions[TestData]{})
		defer dng.Close()
		n := clusterNode(c, NodeAlive, "zone", "us-west")
		c.nodes.updateState(n.ID, NodeDead)
		assert.Nil(t, dng.GetNodeData(n.ID))
		assert.Empty(t, dng.GetDataNodes())
	})
}

// A NodeGroup needs no options.
func TestNodeGroupNilOptions(t *testing.T) {
	c := newUnitCluster(t)
	west := map[string]string{"zone": "us-west"}

	ng := NewNodeGroup(c, west, nil)
	defer ng.Close()
	n := clusterNode(c, NodeAlive, "zone", "us-west")
	assert.True(t, ng.Contains(n.ID))
}

// A nil options struct is allowed, as it is for NewNodeGroup.
func TestDataNodeGroupNilOptions(t *testing.T) {
	c := newUnitCluster(t)
	g := NewDataNodeGroup[int](c, map[string]string{"zone": "us-west"}, nil)
	defer g.Close()

	n := clusterNode(c, NodeAlive, "zone", "us-west")
	assert.True(t, g.Contains(n.ID))
}

// OnNodeAdded may call back into the group without deadlocking on the shard lock.
func TestDataNodeGroupAddedCallbackMayReenter(t *testing.T) {
	c := newUnitCluster(t)
	var g *DataNodeGroup[int]
	counts := make(chan int, 4)
	g = NewDataNodeGroup[int](c, map[string]string{"zone": "us-west"}, &DataNodeGroupOptions[int]{
		OnNodeAdded: func(n *Node, _ *int) { counts <- g.Count(); g.Contains(n.ID) },
	})
	defer g.Close()

	done := make(chan struct{})
	go func() { clusterNode(c, NodeAlive, "zone", "us-west"); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: OnNodeAdded re-entering the group")
	}
	assert.Equal(t, 1, <-counts)
}

// The data initializer may call back into the group without deadlocking.
func TestDataNodeGroupInitializerMayReenter(t *testing.T) {
	c := newUnitCluster(t)
	var g *DataNodeGroup[int]
	g = NewDataNodeGroup[int](c, map[string]string{"zone": "us-west"}, &DataNodeGroupOptions[int]{
		DataInitializer: func(n *Node) *int { v := g.Count(); g.Contains(n.ID); return &v },
	})
	defer g.Close()

	done := make(chan struct{})
	go func() { clusterNode(c, NodeAlive, "zone", "us-west"); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: data initializer re-entering the group")
	}
	assert.Equal(t, 1, g.Count())
}
