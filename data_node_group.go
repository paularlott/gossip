package gossip

import (
	"math/rand"
	"strings"
	"sync"
)

// DataNodeGroup groups nodes based on meta data and stores custom data with the nodes.
type DataNodeGroup[T any] struct {
	cluster          *Cluster
	metadataCriteria map[string]string
	shardCount       int
	shardMask        uint32
	shards           []*dataNodeGroupShard[T]

	// Handlers
	stateHandlerId    HandlerID
	metadataHandlerId HandlerID

	// Callbacks for node events
	onNodeAdded   func(node *Node, data *T) // Called when node matches criteria and is added
	onNodeRemoved func(node *Node, data *T) // Called when node no longer matches or is dead
	onNodeUpdated func(node *Node, data *T) // Called when node metadata changes but still matches

	// Function to initialize custom data for a node
	dataInitializer func(node *Node) *T
}

// dataNodeGroupShard represents a shard for the data node group
type dataNodeGroupShard[T any] struct {
	mutex sync.RWMutex
	// Map of node ID to both node pointer and custom data
	nodes map[NodeID]*NodeWithData[T]
}

// NodeWithData pairs a node with its custom data
type NodeWithData[T any] struct {
	Node *Node
	Data *T
}

// DataNodeGroupOptions contains configuration options
type DataNodeGroupOptions[T any] struct {
	// Callbacks
	OnNodeAdded   func(node *Node, data *T)
	OnNodeRemoved func(node *Node, data *T)
	OnNodeUpdated func(node *Node, data *T)

	// Function to initialize custom data for a node
	DataInitializer func(node *Node) *T
}

// NewDataNodeGroup creates a new data node group
func NewDataNodeGroup[T any](
	cluster *Cluster,
	criteria map[string]string,
	options *DataNodeGroupOptions[T],
) *DataNodeGroup[T] {
	// Use same shard count as cluster's nodeList for consistency
	shardCount := cluster.config.NodeShardCount

	dng := &DataNodeGroup[T]{
		cluster:          cluster,
		metadataCriteria: criteria,
		shardCount:       shardCount,
		shardMask:        uint32(shardCount - 1),
		shards:           make([]*dataNodeGroupShard[T], shardCount),
		onNodeAdded:      options.OnNodeAdded,
		onNodeRemoved:    options.OnNodeRemoved,
		onNodeUpdated:    options.OnNodeUpdated,
		dataInitializer:  options.DataInitializer,
	}

	// Initialize shards
	for i := 0; i < shardCount; i++ {
		dng.shards[i] = &dataNodeGroupShard[T]{
			nodes: make(map[NodeID]*NodeWithData[T]),
		}
	}

	// Register handlers for node state and metadata changes
	dng.stateHandlerId = cluster.HandleNodeStateChangeFunc(dng.handleNodeStateChange)
	dng.metadataHandlerId = cluster.HandleNodeMetadataChangeFunc(dng.handleNodeMetadataChange)

	// Populate group with existing nodes
	dng.initializeWithExistingNodes()

	return dng
}

// Close unregisters event handlers to allow for garbage collection
func (dng *DataNodeGroup[T]) Close() {
	dng.cluster.RemoveNodeStateChangeHandler(dng.stateHandlerId)
	dng.cluster.RemoveNodeMetadataChangeHandler(dng.metadataHandlerId)
}

// initializeWithExistingNodes adds all existing matching nodes to the group
func (dng *DataNodeGroup[T]) initializeWithExistingNodes() {
	nodes := dng.cluster.nodes.getAllInStates([]NodeState{NodeAlive, NodeSuspect})
	for _, node := range nodes {
		if dng.nodeMatchesCriteria(node) {
			dng.addNode(node)
		}
	}
}

// nodeMatchesCriteria checks if a node matches all the metadata criteria
func (dng *DataNodeGroup[T]) nodeMatchesCriteria(node *Node) bool {
	for key, expectedValue := range dng.metadataCriteria {
		if !node.Metadata.Exists(key) {
			return false
		}

		if expectedValue[0] == MetadataContainsPrefix[0] {
			if !strings.Contains(node.Metadata.GetString(key), expectedValue[1:]) {
				return false
			}
		} else if expectedValue != MetadataAnyValue && expectedValue != node.Metadata.GetString(key) {
			return false
		}
	}
	return true
}

// handleNodeStateChange processes node state changes
func (dng *DataNodeGroup[T]) handleNodeStateChange(node *Node, prevState NodeState) {
	if node.Alive() || node.Suspect() {
		// Node is now alive, check if it matches criteria
		if dng.nodeMatchesCriteria(node) {
			dng.addNode(node)
		}
	} else if prevState == NodeAlive || prevState == NodeSuspect {
		// Node is no longer alive, remove it
		dng.removeNode(node)
	}
}

// handleNodeMetadataChange processes node metadata changes
func (dng *DataNodeGroup[T]) handleNodeMetadataChange(node *Node) {
	if !node.Alive() {
		return
	}

	// Get the appropriate shard
	shard := dng.getShard(node.ID)

	// Check if the node is currently in the group
	shard.mutex.RLock()
	nodeData, exists := shard.nodes[node.ID]
	shard.mutex.RUnlock()

	matches := dng.nodeMatchesCriteria(node)

	if matches && !exists {
		// Node now matches but wasn't in the group before
		dng.addNode(node)
	} else if !matches && exists {
		// Node no longer matches but was in the group before
		dng.removeNode(node)
	} else if matches && exists {
		// Node still matches but metadata changed, trigger update callback
		// Refresh stored node pointer to the canonical one
		shard.mutex.Lock()
		nodeData.Node = node
		shard.mutex.Unlock()
		if dng.onNodeUpdated != nil {
			dng.onNodeUpdated(node, nodeData.Data)
		}
	}
}

// addNode adds a node to the appropriate shard
func (dng *DataNodeGroup[T]) addNode(node *Node) {
	shard := dng.getShard(node.ID)

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	// Check if node already exists
	if _, exists := shard.nodes[node.ID]; exists {
		return
	}

	// Create custom data for this node
	var data *T
	if dng.dataInitializer != nil {
		data = dng.dataInitializer(node)
	} else {
		// Create a zero value if no initializer is provided
		var zeroValue T
		data = &zeroValue
	}

	// Add node with its data
	shard.nodes[node.ID] = &NodeWithData[T]{
		Node: node,
		Data: data,
	}

	// Trigger callback
	if dng.onNodeAdded != nil {
		dng.onNodeAdded(node, data)
	}
}

// removeNode removes a node from the appropriate shard
func (dng *DataNodeGroup[T]) removeNode(node *Node) {
	shard := dng.getShard(node.ID)

	shard.mutex.Lock()

	// Get the node data before removing
	nodeData, exists := shard.nodes[node.ID]
	if !exists {
		shard.mutex.Unlock()
		return
	}

	// Remove the node
	delete(shard.nodes, node.ID)
	shard.mutex.Unlock()

	// Trigger callback after releasing the lock
	if dng.onNodeRemoved != nil {
		dng.onNodeRemoved(node, nodeData.Data)
	}
}

// GetNodes returns all alive nodes in this group, excluding specified node IDs
func (dng *DataNodeGroup[T]) GetNodes(excludeIDs []NodeID) []*Node {
	var result []*Node

	// Build exclusion set
	var excludeSet map[NodeID]struct{}
	excludeSetSize := len(excludeIDs)
	if excludeSetSize > 0 {
		excludeSet = make(map[NodeID]struct{}, len(excludeIDs))
		for _, id := range excludeIDs {
			excludeSet[id] = struct{}{}
		}
	}

	for _, shard := range dng.shards {
		shard.mutex.RLock()

		for nodeID, nodeData := range shard.nodes {
			// Skip excluded nodes
			if excludeSetSize > 0 {
				if _, excluded := excludeSet[nodeID]; excluded {
					continue
				}
			}

			result = append(result, nodeData.Node)
		}

		shard.mutex.RUnlock()
	}

	return result
}

// GetNodeData returns a node's custom data if it exists in the group
func (dng *DataNodeGroup[T]) GetNodeData(nodeID NodeID) *T {
	shard := dng.getShard(nodeID)

	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	if nodeData, exists := shard.nodes[nodeID]; exists {
		return nodeData.Data
	}

	return nil
}

// UpdateNodeData allows updating a node's custom data safely
func (dng *DataNodeGroup[T]) UpdateNodeData(nodeID NodeID, updateFn func(*Node, *T) error) error {
	shard := dng.getShard(nodeID)

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if nodeData, exists := shard.nodes[nodeID]; exists {
		return updateFn(nodeData.Node, nodeData.Data)
	}

	return nil
}

// GetDataNodes returns all alive nodes custom data
func (dng *DataNodeGroup[T]) GetDataNodes() []*T {
	var result []*T

	for _, shard := range dng.shards {
		shard.mutex.RLock()

		for _, nodeData := range shard.nodes {
			result = append(result, nodeData.Data)
		}

		shard.mutex.RUnlock()
	}

	return result
}

// Contains checks if a node with the given ID is in this group
func (dng *DataNodeGroup[T]) Contains(nodeID NodeID) bool {
	shard := dng.getShard(nodeID)

	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	_, exists := shard.nodes[nodeID]
	return exists
}

// Count returns the number of alive nodes in this group
func (dng *DataNodeGroup[T]) Count() int {
	count := 0

	for _, shard := range dng.shards {
		shard.mutex.RLock()
		count += len(shard.nodes)
		shard.mutex.RUnlock()
	}

	return count
}

// getShard returns the appropriate shard for a node ID
func (dng *DataNodeGroup[T]) getShard(nodeID NodeID) *dataNodeGroupShard[T] {
	// FNV-1a hash for better distribution - inlined for performance
	idBytes := nodeID[:]

	hash := uint32(2166136261) // FNV offset basis

	for _, b := range idBytes {
		hash ^= uint32(b)
		hash *= 16777619 // FNV prime
	}

	return dng.shards[hash&dng.shardMask]
}

// SendToPeers sends a message to all peers in the group and if necessary gossips to random peers.
func (dng *DataNodeGroup[T]) SendToPeers(msgType MessageType, data interface{}) error {
	zoneNodes := dng.GetNodes([]NodeID{dng.cluster.localNode.ID})

	rand.Shuffle(len(zoneNodes), func(i, j int) {
		zoneNodes[i], zoneNodes[j] = zoneNodes[j], zoneNodes[i]
	})

	err := dng.cluster.SendToPeers(zoneNodes, msgType, data)
	if err != nil {
		return err
	}

	return nil
}

// SendToPeersReliable sends a message to all peers in the group reliably and if necessary gossips to random peers.
func (dng *DataNodeGroup[T]) SendToPeersReliable(msgType MessageType, data interface{}) error {
	zoneNodes := dng.GetNodes([]NodeID{dng.cluster.localNode.ID})

	rand.Shuffle(len(zoneNodes), func(i, j int) {
		zoneNodes[i], zoneNodes[j] = zoneNodes[j], zoneNodes[i]
	})

	err := dng.cluster.SendToPeersReliable(zoneNodes, msgType, data)
	if err != nil {
		return err
	}

	return nil
}
