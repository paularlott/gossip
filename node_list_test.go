package gossip

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeListAddAndRemove(t *testing.T) {
	c := newUnitCluster(t)
	nl := c.nodes
	id := NodeID(uuid.New())

	n := newNode(id, "127.0.0.1:1")
	assert.Same(t, n, nl.addIfNotExists(n))
	assert.Same(t, n, nl.addIfNotExists(newNode(id, "127.0.0.1:2")), "a node we hold is returned, not replaced")
	assert.Same(t, n, nl.get(id))
	assert.Nil(t, nl.get(NodeID(uuid.New())))

	replacement := newNode(id, "127.0.0.1:3")
	replacement.observedState = NodeSuspect
	require.True(t, nl.addOrUpdate(replacement))
	assert.Same(t, replacement, nl.get(id), "addOrUpdate replaces")
	assert.Equal(t, 1, nl.getSuspectCount())
	assert.Equal(t, 1, nl.getAliveCount(), "the local node")

	nl.remove(id)
	assert.Nil(t, nl.get(id))
	assert.Equal(t, 0, nl.getSuspectCount())
	assert.True(t, replacement.Removed(), "a removed node is marked so")

	nl.remove(c.localNode.ID)
	assert.NotNil(t, nl.get(c.localNode.ID), "the local node cannot be removed")
}

func TestNodeListRemoveIfInState(t *testing.T) {
	c := newUnitCluster(t)
	n := clusterNode(c, NodeAlive)

	assert.False(t, c.nodes.removeIfInState(n.ID, []NodeState{NodeDead}), "not in that state")
	assert.NotNil(t, c.nodes.get(n.ID))
	assert.True(t, c.nodes.removeIfInState(n.ID, []NodeState{NodeDead, NodeAlive}))
	assert.Nil(t, c.nodes.get(n.ID))
	assert.False(t, c.nodes.removeIfInState(n.ID, []NodeState{NodeAlive}), "already gone")
	assert.False(t, c.nodes.removeIfInState(c.localNode.ID, []NodeState{NodeAlive}), "the local node")
}

func TestNodeListUpdateState(t *testing.T) {
	c := newUnitCluster(t)
	n := clusterNode(c, NodeAlive)
	var prev []NodeState
	c.HandleNodeStateChangeFunc(func(_ *Node, p NodeState) { prev = append(prev, p) })

	assert.True(t, c.nodes.updateState(n.ID, NodeAlive), "the state it has")
	assert.Empty(t, prev, "no change, no notification")

	counts := func() [4]int {
		return [4]int{c.nodes.getAliveCount(), c.nodes.getSuspectCount(), c.nodes.getLeavingCount(), c.nodes.getDeadCount()}
	}
	for _, step := range []struct {
		to   NodeState
		want [4]int // alive, suspect, leaving, dead
	}{
		{NodeSuspect, [4]int{1, 1, 0, 0}},
		{NodeLeaving, [4]int{1, 0, 1, 0}},
		{NodeDead, [4]int{1, 0, 0, 1}},
		{NodeAlive, [4]int{2, 0, 0, 0}},
	} {
		assert.True(t, c.nodes.updateState(n.ID, step.to))
		assert.Equal(t, step.to, n.GetObservedState())
		assert.Equal(t, step.want, counts(), "after %s", step.to)
	}
	assert.Equal(t, []NodeState{NodeAlive, NodeSuspect, NodeLeaving, NodeDead}, prev, "each change names the state it left")

	assert.False(t, c.nodes.updateState(NodeID(uuid.New()), NodeDead), "unknown node")
}

// A node that is removed is announced as such.
func TestNodeRemovalIsAnnounced(t *testing.T) {
	c := newUnitCluster(t)
	var removed *Node
	c.HandleNodeStateChangeFunc(func(n *Node, _ NodeState) {
		if n.Removed() {
			removed = n
		}
	})
	n := clusterNode(c, NodeAlive)
	c.nodes.remove(n.ID)
	assert.Same(t, n, removed)
}

func TestNodeListQueries(t *testing.T) {
	c := newUnitCluster(t)
	nl := c.nodes
	var alive, suspect, dead []*Node
	for i := range 6 {
		alive = append(alive, clusterNode(c, NodeAlive))
		if i%2 == 0 {
			alive[i].tags = []string{"web"}
		}
	}
	for range 3 {
		suspect = append(suspect, clusterNode(c, NodeSuspect))
	}
	for range 2 {
		dead = append(dead, clusterNode(c, NodeDead))
	}
	aliveAndLocal := append([]*Node{c.localNode}, alive...)

	t.Run("random nodes are in the states asked for", func(t *testing.T) {
		got := nl.getRandomNodesInStates(100, []NodeState{NodeSuspect, NodeDead}, nil)
		assert.ElementsMatch(t, append(append([]*Node{}, suspect...), dead...), got)
		assert.Len(t, nl.getRandomNodesInStates(3, []NodeState{NodeAlive}, nil), 3, "at most k")
		assert.Len(t, nl.getRandomNodesInStates(100, []NodeState{NodeAlive}, nil), len(aliveAndLocal), "fewer when there are fewer")
		assert.Empty(t, nl.getRandomNodesInStates(5, []NodeState{}, nil), "no states")
		assert.Empty(t, nl.getRandomNodesInStates(0, []NodeState{NodeAlive}, nil), "k of 0")
		assert.Empty(t, nl.getRandomNodesInStates(-1, []NodeState{NodeAlive}, nil), "negative k")
	})

	t.Run("random nodes leave out the excluded", func(t *testing.T) {
		exclude := []NodeID{c.localNode.ID, alive[0].ID, alive[1].ID}
		for range 20 {
			for _, n := range nl.getRandomNodes(100, exclude) {
				assert.NotContains(t, exclude, n.ID)
				assert.True(t, n.Routable(), "alive or suspect")
			}
		}
		assert.Len(t, nl.getRandomNodes(100, exclude), len(alive)-2+len(suspect))
	})

	t.Run("random nodes with a tag", func(t *testing.T) {
		got := nl.getRandomNodesWithTag(100, "web", []NodeID{alive[0].ID})
		assert.ElementsMatch(t, []*Node{alive[2], alive[4]}, got)
		assert.Empty(t, nl.getRandomNodesWithTag(100, "nothing", nil))
	})

	t.Run("by tag", func(t *testing.T) {
		assert.ElementsMatch(t, []*Node{alive[0], alive[2], alive[4]}, nl.getByTag("web"))
	})

	t.Run("all in states", func(t *testing.T) {
		assert.ElementsMatch(t, aliveAndLocal, nl.getAllInStates([]NodeState{NodeAlive}))
		assert.Len(t, nl.getAllInStates([]NodeState{NodeAlive, NodeDead}), len(aliveAndLocal)+len(dead))
		assert.Len(t, nl.getAll(), len(aliveAndLocal)+len(suspect)+len(dead))
		assert.Equal(t, len(nl.getAll()), c.NumNodes())
	})

	t.Run("for all in states stops when asked", func(t *testing.T) {
		count := 0
		nl.forAllInStates([]NodeState{NodeAlive}, func(*Node) bool { count++; return true })
		assert.Equal(t, len(aliveAndLocal), count)
		count = 0
		nl.forAllInStates([]NodeState{NodeAlive}, func(*Node) bool { count++; return count < 3 })
		assert.Equal(t, 3, count)
	})
}

// The lists by state are cached; every change to the nodes is seen by the next call.
func TestNodeListStateCache(t *testing.T) {
	c := newUnitCluster(t)
	nl := c.nodes
	for range 4 {
		clusterNode(c, NodeAlive)
	}
	alive := func() int { return len(nl.getCachedNodesInStates([]NodeState{NodeAlive})) }

	require.Equal(t, 5, alive())
	assert.Equal(t, 5, alive(), "the cached list")

	n := clusterNode(c, NodeAlive)
	assert.Equal(t, 6, alive(), "after an add")
	nl.updateState(n.ID, NodeSuspect)
	assert.Equal(t, 5, alive(), "after a state change")
	nl.updateState(n.ID, NodeAlive)
	nl.remove(n.ID)
	assert.Equal(t, 5, alive(), "after a removal")
}

func TestStateSetToKey(t *testing.T) {
	assert.Equal(t, "", stateSetToKey(nil))
	assert.Equal(t, "1", stateSetToKey([]NodeState{NodeAlive}))
	assert.Equal(t, "1,2", stateSetToKey([]NodeState{NodeSuspect, NodeAlive}), "sorted")
	assert.Equal(t, "1,2", stateSetToKey([]NodeState{NodeAlive, NodeSuspect}), "the order given does not matter")
	assert.Equal(t, "1,2,3", stateSetToKey([]NodeState{NodeDead, NodeAlive, NodeSuspect}))
	assert.NotEqual(t, stateSetToKey([]NodeState{NodeAlive}), stateSetToKey([]NodeState{NodeSuspect}))
}

func TestNodeListEventHandlers(t *testing.T) {
	c := newUnitCluster(t)
	nl := c.nodes
	var states []NodeState
	var changed []*Node
	stateID := nl.OnNodeStateChange(func(_ *Node, prev NodeState) { states = append(states, prev) })
	metaID := nl.OnNodeMetadataChange(func(n *Node) { changed = append(changed, n) })

	n := clusterNode(c, NodeAlive)
	nl.updateState(n.ID, NodeSuspect)
	nl.notifyMetadataChanged(n)

	assert.Equal(t, []NodeState{NodeUnknown, NodeAlive}, states)
	assert.Equal(t, []*Node{n}, changed)

	assert.True(t, nl.RemoveStateChangeHandler(stateID))
	assert.True(t, nl.RemoveMetadataChangeHandler(metaID))
	assert.False(t, nl.RemoveStateChangeHandler(stateID), "already removed")
	assert.False(t, nl.RemoveMetadataChangeHandler(HandlerID(uuid.New())), "never added")

	nl.updateState(n.ID, NodeDead)
	nl.notifyMetadataChanged(n)
	assert.Len(t, states, 2)
	assert.Len(t, changed, 1)
}
