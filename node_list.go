package gossip

import (
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/paularlott/gossip/hlc"
)

// NodeListShard represents a single shard of the node list
type nodeListShard struct {
	mutex sync.RWMutex
	nodes map[NodeID]*Node // All nodes in this shard
}

// NodeList manages a collection of nodes in the cluster
type nodeList struct {
	cluster    *Cluster
	shardCount int
	shardMask  uint32
	shards     []*nodeListShard

	// State counters (updated atomically)
	aliveCount   atomic.Int64
	suspectCount atomic.Int64
	leavingCount atomic.Int64
	deadCount    atomic.Int64

	// Cache for state-based node lists
	cacheMutex sync.RWMutex
	stateCache map[string][]*Node // key: sorted states string
}

// Add cache key generation
func stateSetToKey(states []NodeState) string {
	if len(states) == 0 {
		return ""
	}

	// Sort states for consistent key
	sorted := make([]NodeState, len(states))
	copy(sorted, states)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	// Build key string
	var key strings.Builder
	for i, state := range sorted {
		if i > 0 {
			key.WriteByte(',')
		}
		key.WriteString(strconv.Itoa(int(state)))
	}
	return key.String()
}

// NewNodeList creates a new node list
func newNodeList(c *Cluster) *nodeList {
	nl := &nodeList{
		cluster:    c,
		shardCount: c.config.NodeShardCount,
		shardMask:  uint32(c.config.NodeShardCount - 1),
		stateCache: make(map[string][]*Node),
	}

	// Initialize shards
	nl.shards = make([]*nodeListShard, c.config.NodeShardCount)
	for i := 0; i < c.config.NodeShardCount; i++ {
		nl.shards[i] = &nodeListShard{nodes: make(map[NodeID]*Node)}
	}

	return nl
}

// getShard returns the appropriate shard for a node ID
func (nl *nodeList) getShard(nodeID NodeID) *nodeListShard {
	// UUID v7 has random bits in the last 8 bytes, use more of them
	idBytes := nodeID[:]
	// Use FNV-1a hash for better distribution
	hash := uint32(2166136261)
	for i := 8; i < 16; i++ {
		hash ^= uint32(idBytes[i])
		hash *= 16777619
	}
	return nl.shards[hash&nl.shardMask]
}

func (nl *nodeList) add(node *Node, updateExisting bool) bool {
	shard := nl.getShard(node.ID)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if existing, exists := shard.nodes[node.ID]; exists {
		if !updateExisting {
			return false
		}
		// Preserve certain runtime timestamps if newer than incoming (defensive)
		if existing.lastMessageTime.After(node.lastMessageTime) {
			node.lastMessageTime = existing.lastMessageTime
		}
		if existing.lastMetadataTime.After(node.lastMetadataTime) {
			node.lastMetadataTime = existing.lastMetadataTime
		}
		oldState := existing.state
		shard.nodes[node.ID] = node
		if oldState != node.state {
			nl.updateCountersForStateChange(oldState, node.state)
			nl.cluster.notifyNodeStateChanged(node, oldState)
		}
		return true
	}

	// New node
	shard.nodes[node.ID] = node
	nl.updateCountersForStateChange(NodeUnknown, node.state)
	nl.cluster.notifyNodeStateChanged(node, NodeUnknown)
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

	if node, exists := shard.nodes[nodeID]; exists {
		for _, s := range states {
			if node.state == s {
				delete(shard.nodes, nodeID)
				nl.updateCountersForStateChange(node.state, NodeUnknown)
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

// UpdateState updates the state of a node using a local timestamp
func (nl *nodeList) updateState(nodeID NodeID, newState NodeState) bool {
	// Never allow the local node to be marked as Suspect or Dead
	if nodeID == nl.cluster.localNode.ID {
		if newState != NodeAlive && newState != NodeLeaving {
			nl.cluster.config.Logger.
				Field("rejected_state", newState.String()).
				Debugf("Attempted to set invalid state for local node, ignoring")
			return false
		}
	}

	shard := nl.getShard(nodeID)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if node, exists := shard.nodes[nodeID]; exists {
		oldState := node.state
		if oldState == newState { // idempotent
			return true
		}
		node.state = newState
		node.stateChangeTime = hlc.Now()
		nl.updateCountersForStateChange(oldState, newState)
		nl.cluster.notifyNodeStateChanged(node, oldState)
		if newState != NodeAlive { // Clear cached resolved address to force re-resolution later
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

	nl.invalidateStateCache()
}

// Add cache invalidation method
func (nl *nodeList) invalidateStateCache() {
	nl.cacheMutex.Lock()
	defer nl.cacheMutex.Unlock()

	// Clear cache and bump version
	nl.stateCache = make(map[string][]*Node)
}

// Get cached or build node list for states
func (nl *nodeList) getCachedNodesInStates(states []NodeState) []*Node {
	key := stateSetToKey(states)
	if key == "" {
		return []*Node{}
	}

	// Try cache first
	nl.cacheMutex.RLock()
	if cached, exists := nl.stateCache[key]; exists {
		result := make([]*Node, len(cached))
		copy(result, cached)
		nl.cacheMutex.RUnlock()
		return result
	}
	nl.cacheMutex.RUnlock()

	// Cache miss - build the list
	nl.cacheMutex.Lock()
	defer nl.cacheMutex.Unlock()

	// Double-check after acquiring write lock
	if cached, exists := nl.stateCache[key]; exists {
		result := make([]*Node, len(cached))
		copy(result, cached)
		return result
	}

	// Build state set for O(1) lookup
	stateSet := make(map[NodeState]struct{}, len(states))
	for _, s := range states {
		stateSet[s] = struct{}{}
	}

	// Collect all nodes in these states
	var allNodes []*Node
	for _, shard := range nl.shards {
		shard.mutex.RLock()
		for _, node := range shard.nodes {
			if _, ok := stateSet[node.state]; ok {
				allNodes = append(allNodes, node)
			}
		}
		shard.mutex.RUnlock()
	}

	// Cache the result
	nl.stateCache[key] = allNodes

	// Return copy to prevent external modification
	result := make([]*Node, len(allNodes))
	copy(result, allNodes)
	return result
}

// GetRandomNodesInStates returns up to k random nodes in the specified states, excluding specified IDs
func (nl *nodeList) getRandomNodesInStates(k int, states []NodeState, excludeIDs []NodeID) []*Node {
	if len(states) == 0 || k <= 0 {
		return []*Node{}
	}

	// Get cached nodes for these states
	allCandidates := nl.getCachedNodesInStates(states)

	// Apply exclusions (not cached since excludeIDs vary)
	if len(excludeIDs) > 0 {
		excludeSet := make(map[NodeID]struct{}, len(excludeIDs))
		for _, id := range excludeIDs {
			excludeSet[id] = struct{}{}
		}

		// Filter out excluded nodes
		filtered := allCandidates[:0] // reuse slice
		for _, node := range allCandidates {
			if _, excluded := excludeSet[node.ID]; !excluded {
				filtered = append(filtered, node)
			}
		}
		allCandidates = filtered
	}

	// Return all if we don't have enough
	if len(allCandidates) <= k {
		return allCandidates
	}

	// Fisher-Yates shuffle and take first k
	for i := len(allCandidates) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		allCandidates[i], allCandidates[j] = allCandidates[j], allCandidates[i]
	}

	return allCandidates[:k]
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
func (nl *nodeList) forAllInStates(states []NodeState, cb func(*Node) bool) {
	// Get cached nodes for these states
	nodes := nl.getCachedNodesInStates(states)

	// Iterate through cached results
	for _, node := range nodes {
		if !cb(node) {
			return
		}
	}
}

func (nl *nodeList) getAllInStates(states []NodeState) []*Node {
	return nl.getCachedNodesInStates(states)
}

// getAll returns all nodes in the cluster
func (nl *nodeList) getAll() []*Node {
	return nl.getCachedNodesInStates([]NodeState{NodeAlive, NodeSuspect, NodeLeaving, NodeDead})
}

// RecalculateCounters rebuilds all counters
func (nl *nodeList) recalculateCounters() {
	var alive, suspect, leaving, dead int64
	for _, shard := range nl.shards {
		shard.mutex.RLock()
		for _, n := range shard.nodes {
			switch n.state {
			case NodeAlive:
				alive++
			case NodeSuspect:
				suspect++
			case NodeLeaving:
				leaving++
			case NodeDead:
				dead++
			}
		}
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

// Query: nodes whose state last changed before the provided timestamp (useful for purging long-dead nodes)
func (nl *nodeList) getNodesWithStateChangeBefore(ts hlc.Timestamp, states []NodeState) []*Node {
	res := make([]*Node, 0)
	nl.forAllInStates(states, func(n *Node) bool {
		if n.stateChangeTime.Before(ts) {
			res = append(res, n)
		}
		return true
	})
	return res
}

// Query: nodes whose last activity (any message) was before timestamp; states filter optional (nil => any state)
func (nl *nodeList) getNodesWithLastActivityBefore(ts hlc.Timestamp, states []NodeState) []*Node {
	res := make([]*Node, 0)
	var stateSet map[NodeState]struct{}
	if len(states) > 0 {
		stateSet = make(map[NodeState]struct{}, len(states))
		for _, s := range states {
			stateSet[s] = struct{}{}
		}
	}
	for _, shard := range nl.shards {
		shard.mutex.RLock()
		for _, n := range shard.nodes {
			if stateSet != nil {
				if _, ok := stateSet[n.state]; !ok {
					continue
				}
			}
			if n.lastMessageTime.Before(ts) {
				res = append(res, n)
			}
		}
		shard.mutex.RUnlock()
	}
	return res
}

// Query: nodes whose metadata timestamp is older than the given timestamp (used to trigger manual sync)
func (nl *nodeList) getNodesWithOldMetadata(ts hlc.Timestamp) []*Node {
	res := make([]*Node, 0)
	for _, shard := range nl.shards {
		shard.mutex.RLock()
		for _, n := range shard.nodes {
			if n.lastMetadataTime.Before(ts) {
				res = append(res, n)
			}
		}
		shard.mutex.RUnlock()
	}
	return res
}

// Update a node's metadata timestamp if newer (returns true if accepted)
func (nl *nodeList) updateMetadataTimestamp(nodeID NodeID, ts hlc.Timestamp) bool {
	shard := nl.getShard(nodeID)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if n, ok := shard.nodes[nodeID]; ok {
		if ts.After(n.lastMetadataTime) {
			n.lastMetadataTime = ts
			return true
		}
	}
	return false
}

// Convenience: sorted list of alive nodes by last activity oldest first (useful for probing)
func (nl *nodeList) getLeastRecentlyActiveAlive(limit int) []*Node {
	all := nl.getAllInStates([]NodeState{NodeAlive, NodeSuspect})
	sort.Slice(all, func(i, j int) bool { return all[i].lastMessageTime < all[j].lastMessageTime })
	if limit > 0 && len(all) > limit {
		return all[:limit]
	}
	return all
}
