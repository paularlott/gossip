package gossip

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paularlott/gossip/hlc"
)

// NodeListShard represents a single shard of the node list
type nodeListShard struct {
	mutex   sync.RWMutex
	nodes   map[NodeID]*Node               // All nodes in this shard
	byState map[NodeState]map[NodeID]*Node // Nodes indexed by state
}

// NodeList manages a collection of nodes in the cluster
type nodeList struct {
	cluster    *Cluster
	shardCount int
	shardMask  uint32
	shards     []*nodeListShard
	randSource *rand.Rand // For thread-safe random selection

	// Atomic counters for quick access without traversing the map
	aliveCount   atomic.Int64
	suspectCount atomic.Int64
	leavingCount atomic.Int64
	deadCount    atomic.Int64
}

// NewNodeList creates a new node list
func newNodeList(c *Cluster) *nodeList {
	source := rand.New(rand.NewSource(time.Now().UnixNano()))
	nl := &nodeList{
		cluster:    c,
		shardCount: c.config.NodeShardCount,
		shardMask:  uint32(c.config.NodeShardCount - 1),
		randSource: source,
	}

	// Initialize shards
	nl.shards = make([]*nodeListShard, c.config.NodeShardCount)
	for i := 0; i < c.config.NodeShardCount; i++ {
		nl.shards[i] = &nodeListShard{
			nodes:   make(map[NodeID]*Node),
			byState: make(map[NodeState]map[NodeID]*Node),
		}

		// Initialize maps for each state
		nl.shards[i].byState[NodeAlive] = make(map[NodeID]*Node)
		nl.shards[i].byState[NodeSuspect] = make(map[NodeID]*Node)
		nl.shards[i].byState[NodeLeaving] = make(map[NodeID]*Node)
		nl.shards[i].byState[NodeDead] = make(map[NodeID]*Node)
	}

	return nl
}

// getShard returns the appropriate shard for a node ID
func (nl *nodeList) getShard(nodeID NodeID) *nodeListShard {
	// FNV-1a hash for better distribution - inlined for performance
	idBytes := nodeID[:]

	hash := uint32(2166136261) // FNV offset basis

	for _, b := range idBytes {
		hash ^= uint32(b)
		hash *= 16777619 // FNV prime
	}

	return nl.shards[hash&nl.shardMask]
}

func (nl *nodeList) add(node *Node, updateExisting bool) bool {
	shard := nl.getShard(node.ID)

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	existing, exists := shard.nodes[node.ID]
	if exists {
		if !updateExisting {
			return false // Node already exists and we don't want to update it
		}

		// Update existing node - track state changes for counter updates
		oldState := existing.state

		// Remove from old state map
		delete(shard.byState[oldState], node.ID)

		// Update node
		shard.nodes[node.ID] = node

		// Add to new state map
		shard.byState[node.state][node.ID] = node

		// Update counters
		nl.updateCountersForStateChange(oldState, node.state)

		// If old state was leaving or dead, trigger event listener
		if oldState == NodeLeaving || oldState == NodeDead ||
			(oldState == NodeSuspect && node.state == NodeAlive) {
			nl.cluster.notifyNodeStateChanged(node, oldState)
		}
	} else {
		// New node
		shard.nodes[node.ID] = node

		// Add to state map
		shard.byState[node.state][node.ID] = node

		// Update counters
		switch node.state {
		case NodeAlive:
			nl.aliveCount.Add(1)
		case NodeSuspect:
			nl.suspectCount.Add(1)
		case NodeLeaving:
			nl.leavingCount.Add(1)
		case NodeDead:
			nl.deadCount.Add(1)
		}

		// Trigger event listener
		nl.cluster.notifyNodeStateChanged(node, NodeUnknown)
	}

	return true
}

func (nl *nodeList) addIfNotExists(node *Node) bool {
	return nl.add(node, false)
}

func (nl *nodeList) addOrUpdate(node *Node) bool {
	return nl.add(node, true)
}

// Remove removes a node from the list
func (nl *nodeList) remove(nodeID NodeID) {
	nl.removeIfInState(nodeID, []NodeState{NodeAlive, NodeSuspect, NodeLeaving, NodeDead})
}

func (nl *nodeList) removeIfInState(nodeID NodeID, states []NodeState) bool {
	if nodeID == nl.cluster.localNode.ID {
		return false
	}

	shard := nl.getShard(nodeID)

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	node, exists := shard.nodes[nodeID]
	if exists {
		// Check if node is in one of the specified states
		for _, state := range states {
			if node.state == state {

				// Update counters
				switch node.state {
				case NodeAlive:
					nl.aliveCount.Add(-1)
				case NodeSuspect:
					nl.suspectCount.Add(-1)
				case NodeLeaving:
					nl.leavingCount.Add(-1)
				case NodeDead:
					nl.deadCount.Add(-1)
				}

				// Remove from state map
				delete(shard.byState[node.state], nodeID)

				// Remove from nodes map
				delete(shard.nodes, nodeID)

				return true
			}
		}
	}

	return false
}

// Get returns a node by ID
func (nl *nodeList) get(nodeID NodeID) *Node {
	shard := nl.getShard(nodeID)

	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	return shard.nodes[nodeID]
}

// UpdateState updates the state of a node
func (nl *nodeList) updateState(nodeID NodeID, state NodeState) bool {
	// Never allow the local node to be marked as Suspect or Dead
	if nodeID == nl.cluster.localNode.ID {
		// Only allow Alive or Leaving for local node
		if state != NodeAlive && state != NodeLeaving {
			nl.cluster.config.Logger.
				Field("rejected_state", state.String()).
				Debugf("Attempted to set invalid state for local node, ignoring")
			return false
		}
	}

	shard := nl.getShard(nodeID)

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if node, exists := shard.nodes[nodeID]; exists {
		oldState := node.state

		// Skip if state hasn't changed
		if oldState == state {
			return true
		}

		// Remove from old state map
		delete(shard.byState[oldState], nodeID)

		// Update node state
		node.state = state
		node.stateChangeTime = hlc.Now()

		// Add to new state map
		shard.byState[state][nodeID] = node

		// Update counters
		nl.updateCountersForStateChange(oldState, state)

		// Trigger event
		nl.cluster.notifyNodeStateChanged(node, oldState)

		// If state anything but alive clear address cache
		if state != NodeAlive {
			node.address.Clear()
		}

		return true
	}
	return false
}

// Helper to update counters when a node's state changes
func (nl *nodeList) updateCountersForStateChange(oldState, newState NodeState) {
	// Update state-specific counters
	switch oldState {
	case NodeAlive:
		nl.aliveCount.Add(-1)
	case NodeSuspect:
		nl.suspectCount.Add(-1)
	case NodeLeaving:
		nl.leavingCount.Add(-1)
	case NodeDead:
		nl.deadCount.Add(-1)
	}

	switch newState {
	case NodeAlive:
		nl.aliveCount.Add(1)
	case NodeSuspect:
		nl.suspectCount.Add(1)
	case NodeLeaving:
		nl.leavingCount.Add(1)
	case NodeDead:
		nl.deadCount.Add(1)
	}
}

// GetRandomNodesInStates returns up to k random nodes in the specified states, excluding specified IDs
func (nl *nodeList) getRandomNodesInStates(k int, states []NodeState, excludeIDs []NodeID) []*Node {
	if len(states) == 0 || k <= 0 {
		return []*Node{}
	}

	// Build exclusion set
	var excludeSet map[NodeID]struct{}
	excludeSetSize := len(excludeIDs)
	if excludeSetSize > 0 {
		excludeSet = make(map[NodeID]struct{}, len(excludeIDs))
		for _, id := range excludeIDs {
			excludeSet[id] = struct{}{}
		}
	}

	// Initialize reservoir with capacity k
	result := make([]*Node, 0, k)
	// Track total eligible nodes seen
	nodesSeen := 0

	for _, shard := range nl.shards {
		shard.mutex.RLock()

		// Process each requested state
		for _, state := range states {
			if nodesInState, exists := shard.byState[state]; exists {
				for nodeID, node := range nodesInState {
					// Skip excluded nodes
					if excludeSetSize > 0 {
						if _, excluded := excludeSet[nodeID]; excluded {
							continue
						}
					}

					// Increment count of eligible nodes
					nodesSeen++

					if len(result) < k {
						// Phase 1: Fill the reservoir until we have k items
						result = append(result, node)
					} else {
						// Phase 2: Replace items with decreasing probability
						j := nl.randSource.Intn(nodesSeen)
						if j < k {
							// Replace the item at index j
							result[j] = node
						}
					}
				}
			}
		}

		shard.mutex.RUnlock()
	}

	return result
}

// GetRandomNodes returns up to k random live nodes (Alive or Suspect)
func (nl *nodeList) getRandomNodes(k int, excludeIDs []NodeID) []*Node {
	return nl.getRandomNodesInStates(k, []NodeState{NodeAlive, NodeSuspect}, excludeIDs)
}

// GetAliveCount returns the number of nodes with state NodeAlive
func (nl *nodeList) getAliveCount() int {
	return int(nl.aliveCount.Load())
}

// GetSuspectCount returns the number of nodes with state nodeSuspect
func (nl *nodeList) getSuspectCount() int {
	return int(nl.suspectCount.Load())
}

// GetLeavingCount returns the number of nodes with state nodeLeaving
func (nl *nodeList) getLeavingCount() int {
	return int(nl.leavingCount.Load())
}

// GetDeadCount returns the number of nodes with state nodeDead
func (nl *nodeList) getDeadCount() int {
	return int(nl.deadCount.Load())
}

// forAllInState executes a function for all nodes in the specified states
// The callback function can return false to stop iteration
func (nl *nodeList) forAllInStates(states []NodeState, callback func(*Node) bool) {
	for _, shard := range nl.shards {
		shard.mutex.RLock()

		// Collect nodes from all specified states
		nodesCopy := make([]*Node, 0)
		for _, state := range states {
			if nodesInState, exists := shard.byState[state]; exists {
				for _, node := range nodesInState {
					nodesCopy = append(nodesCopy, node)
				}
			}
		}

		shard.mutex.RUnlock()

		// Execute callback on each node
		for _, node := range nodesCopy {
			if !callback(node) {
				return // Stop iteration if callback returns false
			}
		}
	}
}

func (nl *nodeList) getAllInStates(states []NodeState) []*Node {
	nodes := make([]*Node, 0)

	nl.forAllInStates(states, func(node *Node) bool {
		nodes = append(nodes, node)
		return true
	})

	return nodes
}

// getAll returns all nodes in the cluster
func (nl *nodeList) getAll() []*Node {
	allNodes := make([]*Node, 0, nl.getAliveCount()+nl.getSuspectCount()+nl.getLeavingCount()+nl.getDeadCount())

	// Iterate through all shards
	for _, shard := range nl.shards {
		shard.mutex.RLock()

		// Append all nodes in this shard to the result slice
		for _, node := range shard.nodes {
			allNodes = append(allNodes, node)
		}

		shard.mutex.RUnlock()
	}

	return allNodes
}

// RecalculateCounters rebuilds all counters
func (nl *nodeList) recalculateCounters() {
	var alive, suspect, leaving, dead int64

	// Iterate through all shards
	for _, shard := range nl.shards {
		shard.mutex.RLock()

		alive += int64(len(shard.byState[NodeAlive]))
		suspect += int64(len(shard.byState[NodeSuspect]))
		leaving += int64(len(shard.byState[NodeLeaving]))
		dead += int64(len(shard.byState[NodeDead]))

		shard.mutex.RUnlock()
	}

	nl.aliveCount.Store(alive)
	nl.suspectCount.Store(suspect)
	nl.leavingCount.Store(leaving)
	nl.deadCount.Store(dead)
}

// getRandomNodesForGossip returns nodes prioritizing recent state changes
func (nl *nodeList) getRandomNodesForGossip(k int, excludeIDs []NodeID) []*Node {
	// Get recent state changes first (alive, suspect, leaving, recent dead)
	recentNodes := nl.getRandomNodesInStates(k*3/4, []NodeState{NodeAlive, NodeSuspect, NodeLeaving}, excludeIDs)

	// Fill remaining slots with dead nodes (for consistency)
	if len(recentNodes) < k {
		deadNodes := nl.getRandomNodesInStates(k-len(recentNodes), []NodeState{NodeDead}, excludeIDs)
		recentNodes = append(recentNodes, deadNodes...)
	}

	return recentNodes
}
