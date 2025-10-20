package gossip

import (
	"math/rand"
	"slices"
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
	cluster    *Cluster // Reference to parent cluster
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

	// Event handlers
	stateChangeHandlers    *EventHandlers[NodeStateChangeHandler]
	metadataChangeHandlers *EventHandlers[NodeMetadataChangeHandler]
}

// Add cache key generation
func stateSetToKey(states []NodeState) string {
	// Fast paths for common cases
	switch len(states) {
	case 0:
		return ""
	case 1:
		return strconv.Itoa(int(states[0]))
	case 2:
		// Fast path for 2 states (most common: NodeAlive, NodeSuspect)
		a, b := states[0], states[1]
		if a > b {
			a, b = b, a
		}
		return strconv.Itoa(int(a)) + "," + strconv.Itoa(int(b))
	}

	// General case for 3+ states (rare)
	sorted := make([]NodeState, len(states))
	copy(sorted, states)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	// Build key string
	var key strings.Builder
	key.Grow(len(states) * 3) // Pre-allocate: ~2 digits + comma per state
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
		cluster:                c,
		shardCount:             c.config.NodeShardCount,
		shardMask:              uint32(c.config.NodeShardCount - 1),
		stateCache:             make(map[string][]*Node),
		stateChangeHandlers:    NewEventHandlers[NodeStateChangeHandler](),
		metadataChangeHandlers: NewEventHandlers[NodeMetadataChangeHandler](),
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

	if existing, exists := shard.nodes[node.ID]; exists {
		if !updateExisting {
			shard.mutex.Unlock()
			return false
		}

		oldState := existing.observedState
		shard.nodes[node.ID] = node

		shard.mutex.Unlock()

		if oldState != node.observedState {
			nl.updateCountersForStateChange(oldState, node.observedState)
			nl.notifyNodeStateChanged(node, oldState)
		}
		return true
	}

	// New node
	shard.nodes[node.ID] = node

	shard.mutex.Unlock()

	nl.updateCountersForStateChange(NodeUnknown, node.observedState)
	nl.notifyNodeStateChanged(node, NodeUnknown)
	return true
}

/**
 * Add the node if it doesn't exist, if adding a node then it returns the added node; else the existing node.
 */
func (nl *nodeList) addIfNotExists(node *Node) *Node {
	shard := nl.getShard(node.ID)
	shard.mutex.Lock()

	if existing, exists := shard.nodes[node.ID]; exists {
		oldState := existing.observedState
		shard.mutex.Unlock()

		if oldState != node.observedState {
			nl.updateCountersForStateChange(oldState, node.observedState)
			nl.notifyNodeStateChanged(existing, oldState)
		}
		return existing
	}

	shard.nodes[node.ID] = node
	shard.mutex.Unlock()

	nl.updateCountersForStateChange(NodeUnknown, node.observedState)
	nl.notifyNodeStateChanged(node, NodeUnknown)

	return node
}

func (nl *nodeList) addOrUpdate(node *Node) bool {
	return nl.add(node, true)
}

// Remove removes a node from the list
func (nl *nodeList) remove(nodeID NodeID) {
	nl.removeIfInState(nodeID, []NodeState{NodeAlive, NodeSuspect, NodeLeaving, NodeDead})
}

func (nl *nodeList) removeIfInState(nodeID NodeID, states []NodeState) bool {
	// Cannot remove local node
	if nl.isLocalNode(nodeID) {
		return false
	}

	shard := nl.getShard(nodeID)
	shard.mutex.Lock()

	node, exists := shard.nodes[nodeID]
	if !exists {
		shard.mutex.Unlock()
		return false
	}

	var matchedState NodeState
	var found bool
	for _, s := range states {
		if node.observedState == s {
			matchedState = s
			found = true
			break
		}
	}

	if !found {
		shard.mutex.Unlock()
		return false
	}

	// Remove the node
	delete(shard.nodes, nodeID)

	// Update node state while still holding lock
	node.mu.Lock()
	node.observedState = NodeRemoved
	node.observedStateTime = hlc.Now()
	node.mu.Unlock()

	// Release lock before callbacks
	shard.mutex.Unlock()

	nl.updateCountersForStateChange(matchedState, NodeRemoved)
	nl.notifyNodeStateChanged(node, matchedState)
	return true
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
	shard := nl.getShard(nodeID)

	shard.mutex.Lock()
	node, exists := shard.nodes[nodeID]
	if !exists {
		shard.mutex.Unlock()
		return false
	}

	node.mu.Lock()
	oldState := node.observedState
	if oldState == newState {
		node.mu.Unlock()
		shard.mutex.Unlock()
		return true
	}

	node.observedState = newState
	node.observedStateTime = hlc.Now()
	node.mu.Unlock()
	shard.mutex.Unlock()

	// Clear cached resolved address to force re-resolution later
	node.address.Clear()

	nl.updateCountersForStateChange(oldState, newState)
	nl.notifyNodeStateChanged(node, oldState)

	return true
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
			if _, ok := stateSet[node.observedState]; ok {
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
// If tag is not nil, only nodes with the specified tag are returned
func (nl *nodeList) getRandomNodesInStates(k int, states []NodeState, excludeIDs []NodeID) []*Node {
	return nl.getRandomNodesInStatesWithTag(k, states, excludeIDs, nil)
}

// getRandomNodesInStatesWithTag returns up to k random nodes in the specified states, excluding specified IDs
// If tag is not nil, only nodes with the specified tag are returned
func (nl *nodeList) getRandomNodesInStatesWithTag(k int, states []NodeState, excludeIDs []NodeID, tag *string) []*Node {
	if len(states) == 0 || k <= 0 {
		return []*Node{}
	}

	// Get cached nodes for these states
	allCandidates := nl.getCachedNodesInStates(states)

	// Build exclusion set for O(1) lookup
	var excludeSet map[NodeID]struct{}
	if len(excludeIDs) > 0 {
		excludeSet = make(map[NodeID]struct{}, len(excludeIDs))
		for _, id := range excludeIDs {
			excludeSet[id] = struct{}{}
		}
	}

	// Filter nodes based on exclusions and tag
	// Pre-allocate with estimated capacity to reduce reallocations
	estimatedCapacity := k
	if tag != nil {
		// If filtering by tag, we'll likely find fewer nodes
		estimatedCapacity = min(k*2, len(allCandidates)/10)
	}
	if estimatedCapacity > len(allCandidates) {
		estimatedCapacity = len(allCandidates)
	}
	filtered := make([]*Node, 0, estimatedCapacity)

	for _, node := range allCandidates {
		// Skip excluded nodes
		if excludeSet != nil {
			if _, excluded := excludeSet[node.ID]; excluded {
				continue
			}
		}

		// Skip nodes without the tag if tag filter is specified
		if tag != nil && !node.HasTag(*tag) {
			continue
		}

		filtered = append(filtered, node)
	}

	// Return all if we don't have enough
	if len(filtered) <= k {
		return slices.Clip(filtered)
	}

	// Fisher-Yates shuffle and take first k
	for i := len(filtered) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		filtered[i], filtered[j] = filtered[j], filtered[i]
	}

	return slices.Clip(filtered[:k])
}

// GetRandomNodes returns up to k random live nodes (Alive or Suspect)
func (nl *nodeList) getRandomNodes(k int, excludeIDs []NodeID) []*Node {
	return nl.getRandomNodesInStates(k, []NodeState{NodeAlive, NodeSuspect}, excludeIDs)
}

// GetRandomNodesWithTag returns up to k random live nodes (Alive or Suspect) that have the specified tag
func (nl *nodeList) getRandomNodesWithTag(k int, tag string, excludeIDs []NodeID) []*Node {
	return nl.getRandomNodesInStatesWithTag(k, []NodeState{NodeAlive, NodeSuspect}, excludeIDs, &tag)
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

// getByTag returns all nodes that have the specified tag
func (nl *nodeList) getByTag(tag string) []*Node {
	// Get cached nodes in alive/suspect states
	allNodes := nl.getCachedNodesInStates([]NodeState{NodeAlive, NodeSuspect})

	// Filter by tag without shuffling (we want all matching nodes)
	filtered := make([]*Node, 0, len(allNodes)/10) // Estimate ~10% will match
	for _, node := range allNodes {
		if node.HasTag(tag) {
			filtered = append(filtered, node)
		}
	}

	// Trim unused capacity to avoid returning slice with excess memory
	return slices.Clip(filtered)
}

// Add event handler registration methods
func (nl *nodeList) OnNodeStateChange(handler NodeStateChangeHandler) HandlerID {
	return nl.stateChangeHandlers.Add(handler)
}

func (nl *nodeList) OnNodeMetadataChange(handler NodeMetadataChangeHandler) HandlerID {
	return nl.metadataChangeHandlers.Add(handler)
}

func (nl *nodeList) RemoveStateChangeHandler(id HandlerID) bool {
	return nl.stateChangeHandlers.Remove(id)
}

func (nl *nodeList) RemoveMetadataChangeHandler(id HandlerID) bool {
	return nl.metadataChangeHandlers.Remove(id)
}

func (nl *nodeList) notifyNodeStateChanged(node *Node, prevState NodeState) {
	nl.stateChangeHandlers.ForEach(func(handler NodeStateChangeHandler) {
		go handler(node, prevState)
	})
}

func (nl *nodeList) notifyMetadataChanged(node *Node) {
	nl.metadataChangeHandlers.ForEach(func(handler NodeMetadataChangeHandler) {
		go handler(node)
	})
}

// isLocalNode checks if the given node ID is the local node
func (nl *nodeList) isLocalNode(nodeID NodeID) bool {
	return nl.cluster != nil && nl.cluster.localNode != nil && nl.cluster.localNode.ID == nodeID
}
