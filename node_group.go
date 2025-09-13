package gossip

import (
	"math/rand"
	"sync"
)

const (
	MetadataAnyValue       = "*"
	MetadataContainsPrefix = "~"
)

// NodeGroup represents a group of nodes that match specific metadata criteria
type NodeGroup struct {
	cluster          *Cluster
	metadataCriteria map[string]string
	shardCount       int
	shardMask        uint32
	shards           []*nodeGroupShard
	stateHandlerId   HandlerID
	metaHandlerId    HandlerID
	onNodeAdded      func(*Node)
	onNodeRemoved    func(*Node)
}

// nodeGroupShard represents a single shard of the node group
type nodeGroupShard struct {
	mutex sync.RWMutex
	// Map of node ID to node pointer
	nodes map[NodeID]*Node
}

type NodeGroupOptions struct {
	OnNodeAdded   func(*Node)
	OnNodeRemoved func(*Node)
}

// NewNodeGroup creates a new node group that tracks nodes with matching metadata
func NewNodeGroup(cluster *Cluster, criteria map[string]string, opts *NodeGroupOptions) *NodeGroup {
	validateNodeGroupParams(cluster, criteria)

	// Use same shard count as cluster's nodeList for consistency
	shardCount := cluster.config.NodeShardCount

	ng := &NodeGroup{
		cluster:          cluster,
		metadataCriteria: criteria,
		shardCount:       shardCount,
		shardMask:        uint32(shardCount - 1),
		shards:           make([]*nodeGroupShard, shardCount),
	}

	if opts != nil {
		ng.onNodeAdded = opts.OnNodeAdded
		ng.onNodeRemoved = opts.OnNodeRemoved
	}

	// Initialize shards
	for i := 0; i < shardCount; i++ {
		ng.shards[i] = &nodeGroupShard{
			nodes: make(map[NodeID]*Node),
		}
	}

	// Register handlers for node state and metadata changes
	ng.stateHandlerId = cluster.HandleNodeStateChangeFunc(ng.handleNodeStateChange)
	ng.metaHandlerId = cluster.HandleNodeMetadataChangeFunc(ng.handleNodeMetadataChange)

	// Populate group with existing nodes
	ng.initializeWithExistingNodes()

	return ng
}

func (ng *NodeGroup) Close() {
	// Unregister event handlers
	ng.cluster.RemoveNodeStateChangeHandler(ng.stateHandlerId)
	ng.cluster.RemoveNodeMetadataChangeHandler(ng.metaHandlerId)

	// Clear all shards
	for _, shard := range ng.shards {
		shard.mutex.Lock()
		shard.nodes = make(map[NodeID]*Node) // Clear nodes
		shard.mutex.Unlock()
	}
}

// initializeWithExistingNodes adds all existing matching nodes to the group
func (ng *NodeGroup) initializeWithExistingNodes() {
	nodes := ng.cluster.nodes.getAllInStates([]NodeState{NodeAlive, NodeSuspect})
	for _, node := range nodes {
		if ng.nodeMatchesCriteria(node) {
			ng.addNode(node)
		}
	}
}

// nodeMatchesCriteria checks if a node matches all the metadata criteria
func (ng *NodeGroup) nodeMatchesCriteria(node *Node) bool {
	return nodeMatchesCriteria(node, ng.metadataCriteria)
}

// handleNodeStateChange processes node state changes
func (ng *NodeGroup) handleNodeStateChange(node *Node, prevState NodeState) {
	if node.Alive() || node.Suspect() {
		// Node is now alive, check if it matches criteria
		if ng.nodeMatchesCriteria(node) {
			ng.addNode(node)
		} else {
			ng.removeNode(node)
		}
	} else {
		// Node is no longer alive, remove it
		ng.removeNode(node)
	}
}

// handleNodeMetadataChange processes node metadata changes
func (ng *NodeGroup) handleNodeMetadataChange(node *Node) {
	if !node.Alive() {
		return
	}

	// Check if the node matches the criteria after metadata change
	shard := ng.getShard(node.ID)
	shard.mutex.RLock()
	_, exists := shard.nodes[node.ID]
	shard.mutex.RUnlock()

	matches := ng.nodeMatchesCriteria(node)

	// Update group membership based on criteria match
	if matches && !exists {
		ng.addNode(node)
	} else if !matches && exists {
		ng.removeNode(node)
	} else if matches && exists {
		// Node still matches; refresh the stored pointer to keep in sync with node list
		shard.mutex.Lock()
		shard.nodes[node.ID] = node
		shard.mutex.Unlock()
	}
}

// addNode adds a node to the appropriate shard
func (ng *NodeGroup) addNode(node *Node) {
	shard := ng.getShard(node.ID)
	shard.mutex.Lock()
	shard.nodes[node.ID] = node
	shard.mutex.Unlock()

	if ng.onNodeAdded != nil {
		ng.onNodeAdded(node)
	}
}

// removeNode removes a node from the appropriate shard
func (ng *NodeGroup) removeNode(node *Node) {
	shard := ng.getShard(node.ID)
	shard.mutex.Lock()
	delete(shard.nodes, node.ID)
	shard.mutex.Unlock()

	if ng.onNodeRemoved != nil {
		ng.onNodeRemoved(node)
	}
}

// GetNodes returns all nodes in this group, excluding specified node IDs
func (ng *NodeGroup) GetNodes(excludeIDs []NodeID) []*Node {
	// Estimate capacity to reduce allocations
	estimatedSize := ng.Count() - len(excludeIDs)
	if estimatedSize < 0 {
		estimatedSize = 0
	}
	result := make([]*Node, 0, estimatedSize)

	// Build exclusion set
	var excludeSet map[NodeID]struct{}
	excludeSetSize := len(excludeIDs)
	if excludeSetSize > 0 {
		excludeSet = make(map[NodeID]struct{}, len(excludeIDs))
		for _, id := range excludeIDs {
			excludeSet[id] = struct{}{}
		}
	}

	for _, shard := range ng.shards {
		shard.mutex.RLock()

		for nodeID, node := range shard.nodes {
			// Skip excluded nodes
			if excludeSetSize > 0 {
				if _, excluded := excludeSet[nodeID]; excluded {
					continue
				}
			}

			result = append(result, node)
		}

		shard.mutex.RUnlock()
	}

	return result
}

// Contains checks if a node with the given ID is in this group
func (ng *NodeGroup) Contains(nodeID NodeID) bool {
	shard := ng.getShard(nodeID)
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	_, exists := shard.nodes[nodeID]
	return exists
}

// Count returns the number of nodes in this group
func (ng *NodeGroup) Count() int {
	count := 0

	for _, shard := range ng.shards {
		shard.mutex.RLock()
		count += len(shard.nodes)
		shard.mutex.RUnlock()
	}

	return count
}

// getShard returns the appropriate shard for a node ID
func (ng *NodeGroup) getShard(nodeID NodeID) *nodeGroupShard {
	return ng.shards[fnvHash(nodeID)&ng.shardMask]
}

// SendToPeers sends a message to all peers in the group and if necessary gossips to random peers.
func (ng *NodeGroup) SendToPeers(msgType MessageType, data interface{}) error {
	zoneNodes := ng.GetNodes([]NodeID{ng.cluster.localNode.ID})

	rand.Shuffle(len(zoneNodes), func(i, j int) {
		zoneNodes[i], zoneNodes[j] = zoneNodes[j], zoneNodes[i]
	})

	return ng.cluster.SendToPeers(zoneNodes, msgType, data)
}

// SendToPeersReliable sends a message to all peers in the group reliably and if necessary gossips to random peers.
func (ng *NodeGroup) SendToPeersReliable(msgType MessageType, data interface{}) error {
	zoneNodes := ng.GetNodes([]NodeID{ng.cluster.localNode.ID})

	rand.Shuffle(len(zoneNodes), func(i, j int) {
		zoneNodes[i], zoneNodes[j] = zoneNodes[j], zoneNodes[i]
	})

	return ng.cluster.SendToPeersReliable(zoneNodes, msgType, data)
}
