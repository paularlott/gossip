package gossip

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// NodeListShard represents a single shard of the node list
type nodeListShard struct {
	mutex   sync.RWMutex
	nodes   map[NodeID]*Node               // All nodes in this shard
	byState map[NodeState]map[NodeID]*Node // Nodes indexed by state
}

// NodeList manages a collection of nodes in the cluster
type nodeList struct {
	config     *Config
	shardCount int
	shardMask  uint32
	shards     []*nodeListShard
	randSource *rand.Rand // For thread-safe random selection

	// Atomic counters for quick access without traversing the map
	totalCount   atomic.Int64
	aliveCount   atomic.Int64
	suspectCount atomic.Int64
	leavingCount atomic.Int64
	deadCount    atomic.Int64
	liveCount    atomic.Int64 // Alive or Suspect nodes
}

// NewNodeList creates a new node list
func newNodeList(config *Config) *nodeList {
	source := rand.New(rand.NewSource(time.Now().UnixNano()))
	nl := &nodeList{
		config:     config,
		shardCount: config.NodeShardCount,
		shardMask:  uint32(config.NodeShardCount - 1),
		randSource: source,
	}

	// Initialize shards
	nl.shards = make([]*nodeListShard, config.NodeShardCount)
	for i := 0; i < config.NodeShardCount; i++ {
		nl.shards[i] = &nodeListShard{
			nodes:   make(map[NodeID]*Node),
			byState: make(map[NodeState]map[NodeID]*Node),
		}

		// Initialize maps for each state
		nl.shards[i].byState[nodeAlive] = make(map[NodeID]*Node)
		nl.shards[i].byState[nodeSuspect] = make(map[NodeID]*Node)
		nl.shards[i].byState[nodeLeaving] = make(map[NodeID]*Node)
		nl.shards[i].byState[nodeDead] = make(map[NodeID]*Node)
	}

	return nl
}

// getShard returns the appropriate shard for a node ID
func (nl *nodeList) getShard(nodeID NodeID) *nodeListShard {
	// Simple hash of NodeID to determine shard
	// Assuming NodeID is a type that can be converted to bytes
	var hash uint32

	// Mix bytes from the NodeID to create a hash
	// Assuming NodeID is 16 bytes (UUID)
	for i := 0; i < len(nodeID); i += 4 {
		h := uint32(nodeID[i])
		if i+1 < len(nodeID) {
			h |= uint32(nodeID[i+1]) << 8
		}
		if i+2 < len(nodeID) {
			h |= uint32(nodeID[i+2]) << 16
		}
		if i+3 < len(nodeID) {
			h |= uint32(nodeID[i+3]) << 24
		}
		hash ^= h
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
	} else {
		// New node
		shard.nodes[node.ID] = node

		// Add to state map
		shard.byState[node.state][node.ID] = node

		// Update counters
		nl.totalCount.Add(1)
		switch node.state {
		case nodeAlive:
			nl.aliveCount.Add(1)
		case nodeSuspect:
			nl.suspectCount.Add(1)
		case nodeLeaving:
			nl.leavingCount.Add(1)
		case nodeDead:
			nl.deadCount.Add(1)
		}
		if node.state == nodeAlive || node.state == nodeSuspect {
			nl.liveCount.Add(1)
		}

		// Trigger event listener if configured
		if nl.config.EventListener != nil {
			nl.config.EventListener.OnNodeJoined(node)
		}
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
	shard := nl.getShard(nodeID)

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	node, exists := shard.nodes[nodeID]
	if exists {
		// Update counters
		nl.totalCount.Add(-1)
		switch node.state {
		case nodeAlive:
			nl.aliveCount.Add(-1)
		case nodeSuspect:
			nl.suspectCount.Add(-1)
		case nodeLeaving:
			nl.leavingCount.Add(-1)
		case nodeDead:
			nl.deadCount.Add(-1)
		}
		if node.state == nodeAlive || node.state == nodeSuspect {
			nl.liveCount.Add(-1)
		}

		// Remove from state map
		delete(shard.byState[node.state], nodeID)

		// Remove from nodes map
		delete(shard.nodes, nodeID)
	}
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
		node.stateChangeTime = time.Now()

		// Add to new state map
		shard.byState[state][nodeID] = node

		// Update counters
		nl.updateCountersForStateChange(oldState, state)

		// Trigger event listener if configured
		if nl.config.EventListener != nil {
			switch state {
			case nodeLeaving:
				nl.config.EventListener.OnNodeLeft(node)
			case nodeDead:
				nl.config.EventListener.OnNodeDead(node)
			default:
				nl.config.EventListener.OnNodeStateChanged(node, oldState)
			}
		}

		return true
	}
	return false
}

// Helper to update counters when a node's state changes
func (nl *nodeList) updateCountersForStateChange(oldState, newState NodeState) {
	// Update state-specific counters
	switch oldState {
	case nodeAlive:
		nl.aliveCount.Add(-1)
	case nodeSuspect:
		nl.suspectCount.Add(-1)
	case nodeLeaving:
		nl.leavingCount.Add(-1)
	case nodeDead:
		nl.deadCount.Add(-1)
	}

	switch newState {
	case nodeAlive:
		nl.aliveCount.Add(1)
	case nodeSuspect:
		nl.suspectCount.Add(1)
	case nodeLeaving:
		nl.leavingCount.Add(1)
	case nodeDead:
		nl.deadCount.Add(1)
	}

	// Update live count (nodes in Alive or Suspect state)
	oldLive := oldState == nodeAlive || oldState == nodeSuspect
	newLive := newState == nodeAlive || newState == nodeSuspect
	if oldLive && !newLive {
		nl.liveCount.Add(-1)
	} else if !oldLive && newLive {
		nl.liveCount.Add(1)
	}
}

func (nl *nodeList) getRandomNodesInStates(k int, states []NodeState, excludeIDs []NodeID) []*Node {
	if len(states) == 0 {
		return []*Node{}
	}

	// Build exclusion set
	var excludeSet map[NodeID]struct{}
	if len(excludeIDs) > 0 {
		excludeSet = make(map[NodeID]struct{}, len(excludeIDs))
		for _, id := range excludeIDs {
			excludeSet[id] = struct{}{}
		}
	}

	// Collect nodes matching requested states
	candidateNodes := make([]*Node, 0)

	for _, shard := range nl.shards {
		shard.mutex.RLock()

		// Directly iterate through the requested states
		for _, state := range states {
			if nodesInState, exists := shard.byState[state]; exists {
				for nodeID, node := range nodesInState {
					if excludeSet != nil {
						if _, excluded := excludeSet[nodeID]; excluded {
							continue
						}
					}
					candidateNodes = append(candidateNodes, node)
				}
			}
		}

		shard.mutex.RUnlock()
	}

	// If we have zero nodes, return empty slice
	count := len(candidateNodes)
	if count == 0 {
		return []*Node{}
	}

	// Shuffle and return results
	nl.randSource.Shuffle(count, func(i, j int) {
		candidateNodes[i], candidateNodes[j] = candidateNodes[j], candidateNodes[i]
	})

	if k >= count {
		return candidateNodes
	}
	return candidateNodes[:k]
}

// GetRandomLiveNodes returns up to k random live nodes (Alive or Suspect)
func (nl *nodeList) getRandomLiveNodes(k int, excludeIDs []NodeID) []*Node {
	return nl.getRandomNodesInStates(k, []NodeState{nodeAlive, nodeSuspect}, excludeIDs)
}

// GetRandomNodes returns up to k random nodes in any state
func (nl *nodeList) getRandomNodes(k int, excludeIDs []NodeID) []*Node {
	return nl.getRandomNodesInStates(k, []NodeState{nodeAlive, nodeSuspect, nodeLeaving, nodeDead}, excludeIDs)
}

// GetTotalCount returns the total number of nodes in the cluster
func (nl *nodeList) getTotalCount() int {
	return int(nl.totalCount.Load())
}

// GetAliveCount returns the number of nodes with state nodeAlive
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

// GetLiveCount returns the number of live nodes (Alive or Suspect)
func (nl *nodeList) getLiveCount() int {
	return int(nl.liveCount.Load())
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
	allNodes := make([]*Node, 0, nl.getTotalCount())

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
	var total, alive, suspect, leaving, dead, live int64

	// Iterate through all shards
	for _, shard := range nl.shards {
		shard.mutex.RLock()

		total += int64(len(shard.nodes))
		alive += int64(len(shard.byState[nodeAlive]))
		suspect += int64(len(shard.byState[nodeSuspect]))
		leaving += int64(len(shard.byState[nodeLeaving]))
		dead += int64(len(shard.byState[nodeDead]))

		shard.mutex.RUnlock()
	}

	// Live nodes are those in Alive or Suspect state
	live = alive + suspect

	nl.totalCount.Store(total)
	nl.aliveCount.Store(alive)
	nl.suspectCount.Store(suspect)
	nl.leavingCount.Store(leaving)
	nl.deadCount.Store(dead)
	nl.liveCount.Store(live)
}
