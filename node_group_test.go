package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/logger"
)

func TestNewNodeGroup(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{
		"zone": "us-west",
		"type": "worker",
	}

	var addedNodes []*Node
	var removedNodes []*Node
	var mu sync.Mutex

	opts := &NodeGroupOptions{
		OnNodeAdded: func(node *Node) {
			mu.Lock()
			addedNodes = append(addedNodes, node)
			mu.Unlock()
		},
		OnNodeRemoved: func(node *Node) {
			mu.Lock()
			removedNodes = append(removedNodes, node)
			mu.Unlock()
		},
	}

	ng := NewNodeGroup(cluster, criteria, opts)
	defer ng.Close()

	if ng.cluster != cluster {
		t.Error("Cluster not set correctly")
	}
	if len(ng.metadataCriteria) != 2 {
		t.Errorf("Expected 2 criteria, got %d", len(ng.metadataCriteria))
	}
	if ng.shardCount != cluster.config.NodeShardCount {
		t.Errorf("Expected shard count %d, got %d", cluster.config.NodeShardCount, ng.shardCount)
	}
}

func TestNewNodeGroupNilOptions(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	if ng.onNodeAdded != nil || ng.onNodeRemoved != nil {
		t.Error("Callbacks should be nil when options is nil")
	}
}

func TestNodeGroupMetadataCriteria(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	// Create test nodes with different metadata
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	node1.metadata.SetString("type", "worker")
	cluster.nodes.addOrUpdate(node1)

	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	node2.metadata.SetString("zone", "us-east")
	node2.metadata.SetString("type", "worker")
	cluster.nodes.addOrUpdate(node2)

	node3 := newNode(NodeID(uuid.New()), "127.0.0.1:8003")
	node3.metadata.SetString("zone", "us-west")
	node3.metadata.SetString("type", "manager")
	cluster.nodes.addOrUpdate(node3)

	// Test exact match criteria
	criteria := map[string]string{
		"zone": "us-west",
		"type": "worker",
	}

	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	// Should only match node1
	if ng.Count() != 1 {
		t.Errorf("Expected 1 node, got %d", ng.Count())
	}
	if !ng.Contains(node1.ID) {
		t.Error("Node1 should be in group")
	}
	if ng.Contains(node2.ID) {
		t.Error("Node2 should not be in group")
	}
	if ng.Contains(node3.ID) {
		t.Error("Node3 should not be in group")
	}
}

func TestNodeGroupAnyValueCriteria(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	node1.metadata.SetString("type", "worker")
	cluster.nodes.addOrUpdate(node1)

	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	node2.metadata.SetString("zone", "us-east")
	node2.metadata.SetString("type", "manager")
	cluster.nodes.addOrUpdate(node2)

	// Test any value criteria
	criteria := map[string]string{
		"zone": MetadataAnyValue, // Accept any zone
		"type": "worker",
	}

	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	// Should only match node1 (has type=worker)
	if ng.Count() != 1 {
		t.Errorf("Expected 1 node, got %d", ng.Count())
	}
	if !ng.Contains(node1.ID) {
		t.Error("Node1 should be in group")
	}
}

func TestNodeGroupContainsCriteria(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("tags", "production,web,frontend")
	cluster.nodes.addOrUpdate(node1)

	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	node2.metadata.SetString("tags", "staging,api,backend")
	cluster.nodes.addOrUpdate(node2)

	// Test contains criteria
	criteria := map[string]string{
		"tags": MetadataContainsPrefix + "web", // Contains "web"
	}

	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	// Should only match node1
	if ng.Count() != 1 {
		t.Errorf("Expected 1 node, got %d", ng.Count())
	}
	if !ng.Contains(node1.ID) {
		t.Error("Node1 should be in group")
	}
}

func TestNodeGroupMissingMetadata(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	// Missing "type" metadata
	cluster.nodes.addOrUpdate(node1)

	criteria := map[string]string{
		"zone": "us-west",
		"type": "worker",
	}

	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	// Should not match node1 (missing required metadata)
	if ng.Count() != 0 {
		t.Errorf("Expected 0 nodes, got %d", ng.Count())
	}
}

func TestNodeGroupStateChanges(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	// Create node with matching metadata
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")

	// Initially not in group
	if ng.Contains(node1.ID) {
		t.Error("Node should not be in group initially")
	}

	// Simulate state change to alive - should add to group
	ng.handleNodeStateChange(node1, NodeUnknown)
	if !ng.Contains(node1.ID) {
		t.Error("Node should be in group after state change to alive")
	}

	// Simulate state change to dead - should remove from group
	node1.observedState = NodeDead
	ng.handleNodeStateChange(node1, NodeAlive)
	if ng.Contains(node1.ID) {
		t.Error("Node should not be in group after state change to dead")
	}
}

func TestNodeGroupMetadataChanges(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	var addedNodes []*Node
	var removedNodes []*Node
	var mu sync.Mutex

	opts := &NodeGroupOptions{
		OnNodeAdded: func(node *Node) {
			mu.Lock()
			addedNodes = append(addedNodes, node)
			mu.Unlock()
		},
		OnNodeRemoved: func(node *Node) {
			mu.Lock()
			removedNodes = append(removedNodes, node)
			mu.Unlock()
		},
	}

	// Create node that doesn't match initially
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-east")
	cluster.nodes.addOrUpdate(node1)

	// Create NodeGroup after node exists (won't match initially)
	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, opts)
	defer ng.Close()

	// Change metadata to match criteria
	node1.metadata.SetString("zone", "us-west")
	ng.handleNodeMetadataChange(node1)

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	addedCount := len(addedNodes)
	mu.Unlock()

	if addedCount != 1 {
		t.Errorf("Expected 1 added node, got %d", addedCount)
	}

	// Change metadata to not match
	node1.metadata.SetString("zone", "us-central")
	ng.handleNodeMetadataChange(node1)

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	removedCount := len(removedNodes)
	mu.Unlock()

	if removedCount != 1 {
		t.Errorf("Expected 1 removed node, got %d", removedCount)
	}
}

func TestNodeGroupMetadataChangeNonAliveNode(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	// Create dead node
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.updateState(node1.ID, NodeDead)

	// Metadata change on dead node should be ignored
	ng.handleNodeMetadataChange(node1)

	if ng.Count() != 0 {
		t.Errorf("Expected 0 nodes, got %d", ng.Count())
	}
}

func TestNodeGroupGetNodes(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	// Add matching nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	ng.addNode(node1)

	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	node2.metadata.SetString("zone", "us-west")
	ng.addNode(node2)

	node3 := newNode(NodeID(uuid.New()), "127.0.0.1:8003")
	node3.metadata.SetString("zone", "us-west")
	ng.addNode(node3)

	// Test getting all nodes
	nodes := ng.GetNodes(nil)
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// Test excluding nodes
	excludeIDs := []NodeID{node1.ID, node3.ID}
	nodes = ng.GetNodes(excludeIDs)
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(nodes))
	}
	if nodes[0].ID != node2.ID {
		t.Error("Wrong node returned")
	}

	// Test excluding all nodes
	excludeIDs = []NodeID{node1.ID, node2.ID, node3.ID}
	nodes = ng.GetNodes(excludeIDs)
	if len(nodes) != 0 {
		t.Errorf("Expected 0 nodes, got %d", len(nodes))
	}
}

func TestNodeGroupContains(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")

	// Initially empty
	if ng.Contains(node1.ID) {
		t.Error("Should not contain node1 initially")
	}

	// Add node1
	ng.addNode(node1)
	if !ng.Contains(node1.ID) {
		t.Error("Should contain node1 after adding")
	}
	if ng.Contains(node2.ID) {
		t.Error("Should not contain node2")
	}

	// Remove node1
	ng.removeNode(node1)
	if ng.Contains(node1.ID) {
		t.Error("Should not contain node1 after removing")
	}
}

func TestNodeGroupCount(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	// Initially empty
	if ng.Count() != 0 {
		t.Errorf("Expected count 0, got %d", ng.Count())
	}

	// Add nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")

	ng.addNode(node1)
	if ng.Count() != 1 {
		t.Errorf("Expected count 1, got %d", ng.Count())
	}

	ng.addNode(node2)
	if ng.Count() != 2 {
		t.Errorf("Expected count 2, got %d", ng.Count())
	}

	// Remove node
	ng.removeNode(node1)
	if ng.Count() != 1 {
		t.Errorf("Expected count 1, got %d", ng.Count())
	}
}

func TestNodeGroupGetShard(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	nodeID := NodeID(uuid.New())
	shard1 := ng.getShard(nodeID)
	shard2 := ng.getShard(nodeID)

	// Same node ID should always return same shard
	if shard1 != shard2 {
		t.Error("Same node ID should return same shard")
	}

	// Shard should be within bounds
	found := false
	for _, s := range ng.shards {
		if s == shard1 {
			found = true
			break
		}
	}
	if !found {
		t.Error("Returned shard not found in shards array")
	}
}

func TestNodeGroupSendToPeers(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	// Add some nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	ng.addNode(node1)

	// Test SendToPeers
	err := ng.SendToPeers(UserMsg, "test message")
	if err != nil {
		t.Errorf("SendToPeers failed: %v", err)
	}

	// Test SendToPeersReliable
	err = ng.SendToPeersReliable(UserMsg, "test message")
	if err != nil {
		t.Errorf("SendToPeersReliable failed: %v", err)
	}
}

func TestNodeGroupClose(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)

	// Add some nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	ng.addNode(node1)

	if ng.Count() != 1 {
		t.Errorf("Expected count 1, got %d", ng.Count())
	}

	// Close should clear all nodes
	ng.Close()

	// Verify shards are cleared
	totalNodes := 0
	for _, shard := range ng.shards {
		shard.mutex.RLock()
		totalNodes += len(shard.nodes)
		shard.mutex.RUnlock()
	}

	if totalNodes != 0 {
		t.Errorf("Expected 0 nodes after close, got %d", totalNodes)
	}
}

func TestNodeGroupConcurrentAccess(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	const numGoroutines = 10
	const numOperations = 50

	var wg sync.WaitGroup

	// Concurrent add/remove operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
				node.metadata.SetString("zone", "us-west")

				ng.addNode(node)
				ng.Contains(node.ID)
				ng.Count()
				ng.GetNodes(nil)
				ng.removeNode(node)
			}
		}(i)
	}

	wg.Wait()
}

func TestNodeGroupAddNodeTwice(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")

	// Add node twice
	ng.addNode(node1)
	ng.addNode(node1)

	// Should only be counted once
	if ng.Count() != 1 {
		t.Errorf("Expected count 1, got %d", ng.Count())
	}
}

func TestNodeGroupRemoveNonExistentNode(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	ng := NewNodeGroup(cluster, criteria, nil)
	defer ng.Close()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")

	// Remove node that was never added (should not panic)
	ng.removeNode(node1)

	if ng.Count() != 0 {
		t.Errorf("Expected count 0, got %d", ng.Count())
	}
}

func createTestCluster(t *testing.T) *Cluster {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create test cluster: %v", err)
	}

	return cluster
}
