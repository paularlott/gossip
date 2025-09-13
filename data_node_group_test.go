package gossip

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type TestData struct {
	Value   string
	Counter int
}

func TestNewDataNodeGroup(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{
		"zone": "us-west",
		"type": "worker",
	}

	var addedNodes []*Node
	var addedData []*TestData
	var removedNodes []*Node
	var updatedNodes []*Node
	var mu sync.Mutex

	opts := &DataNodeGroupOptions[TestData]{
		OnNodeAdded: func(node *Node, data *TestData) {
			mu.Lock()
			addedNodes = append(addedNodes, node)
			addedData = append(addedData, data)
			mu.Unlock()
		},
		OnNodeRemoved: func(node *Node, data *TestData) {
			mu.Lock()
			removedNodes = append(removedNodes, node)
			mu.Unlock()
		},
		OnNodeUpdated: func(node *Node, data *TestData) {
			mu.Lock()
			updatedNodes = append(updatedNodes, node)
			mu.Unlock()
		},
		DataInitializer: func(node *Node) *TestData {
			return &TestData{
				Value:   "init-" + node.ID.String()[:8],
				Counter: 1,
			}
		},
	}

	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	if dng.cluster != cluster {
		t.Error("Cluster not set correctly")
	}
	if len(dng.metadataCriteria) != 2 {
		t.Errorf("Expected 2 criteria, got %d", len(dng.metadataCriteria))
	}
	if dng.shardCount != cluster.config.NodeShardCount {
		t.Errorf("Expected shard count %d, got %d", cluster.config.NodeShardCount, dng.shardCount)
	}
}

func TestDataNodeGroupNilOptions(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	if dng.onNodeAdded != nil || dng.onNodeRemoved != nil || dng.onNodeUpdated != nil {
		t.Error("Callbacks should be nil when not provided in options")
	}
}

func TestDataNodeGroupMetadataCriteria(t *testing.T) {
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

	opts := &DataNodeGroupOptions[TestData]{
		DataInitializer: func(node *Node) *TestData {
			return &TestData{Value: "test", Counter: 0}
		},
	}

	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Should only match node1
	if dng.Count() != 1 {
		t.Errorf("Expected 1 node, got %d", dng.Count())
	}
	if !dng.Contains(node1.ID) {
		t.Error("Node1 should be in group")
	}
	if dng.Contains(node2.ID) {
		t.Error("Node2 should not be in group")
	}
	if dng.Contains(node3.ID) {
		t.Error("Node3 should not be in group")
	}
}

func TestDataNodeGroupAnyValueCriteria(t *testing.T) {
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

	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Should only match node1 (has type=worker)
	if dng.Count() != 1 {
		t.Errorf("Expected 1 node, got %d", dng.Count())
	}
	if !dng.Contains(node1.ID) {
		t.Error("Node1 should be in group")
	}
}

func TestDataNodeGroupContainsCriteria(t *testing.T) {
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

	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Should only match node1
	if dng.Count() != 1 {
		t.Errorf("Expected 1 node, got %d", dng.Count())
	}
	if !dng.Contains(node1.ID) {
		t.Error("Node1 should be in group")
	}
}

func TestDataNodeGroupStateChanges(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	var addedNodes []*Node
	var removedNodes []*Node
	var mu sync.Mutex

	opts := &DataNodeGroupOptions[TestData]{
		OnNodeAdded: func(node *Node, data *TestData) {
			mu.Lock()
			addedNodes = append(addedNodes, node)
			mu.Unlock()
		},
		OnNodeRemoved: func(node *Node, data *TestData) {
			mu.Lock()
			removedNodes = append(removedNodes, node)
			mu.Unlock()
		},
		DataInitializer: func(node *Node) *TestData {
			return &TestData{Value: "test", Counter: 0}
		},
	}

	criteria := map[string]string{"zone": "us-west"}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Create node with matching metadata
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	cluster.nodes.addOrUpdate(node1)

	// Simulate state change to alive
	dng.handleNodeStateChange(node1, NodeUnknown)

	time.Sleep(10 * time.Millisecond) // Allow callback to execute

	mu.Lock()
	addedCount := len(addedNodes)
	mu.Unlock()

	if addedCount != 1 {
		t.Errorf("Expected 1 added node, got %d", addedCount)
	}

	// Simulate state change to dead
	cluster.nodes.updateState(node1.ID, NodeDead)
	dng.handleNodeStateChange(node1, NodeAlive)

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	removedCount := len(removedNodes)
	mu.Unlock()

	if removedCount != 1 {
		t.Errorf("Expected 1 removed node, got %d", removedCount)
	}
}

func TestDataNodeGroupMetadataChanges(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	var addedNodes []*Node
	var removedNodes []*Node
	var updatedNodes []*Node
	var mu sync.Mutex

	opts := &DataNodeGroupOptions[TestData]{
		OnNodeAdded: func(node *Node, data *TestData) {
			mu.Lock()
			addedNodes = append(addedNodes, node)
			mu.Unlock()
		},
		OnNodeRemoved: func(node *Node, data *TestData) {
			mu.Lock()
			removedNodes = append(removedNodes, node)
			mu.Unlock()
		},
		OnNodeUpdated: func(node *Node, data *TestData) {
			mu.Lock()
			updatedNodes = append(updatedNodes, node)
			mu.Unlock()
		},
		DataInitializer: func(node *Node) *TestData {
			return &TestData{Value: "test", Counter: 0}
		},
	}

	criteria := map[string]string{"zone": "us-west"}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Create node that doesn't match initially
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-east")
	cluster.nodes.addOrUpdate(node1)

	// Change metadata to match criteria
	node1.metadata.SetString("zone", "us-west")
	dng.handleNodeMetadataChange(node1)

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	addedCount := len(addedNodes)
	mu.Unlock()

	if addedCount != 1 {
		t.Errorf("Expected 1 added node, got %d", addedCount)
	}

	// Change metadata but still match (should trigger update)
	node1.metadata.SetString("extra", "value")
	dng.handleNodeMetadataChange(node1)

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	updatedCount := len(updatedNodes)
	mu.Unlock()

	if updatedCount != 1 {
		t.Errorf("Expected 1 updated node, got %d", updatedCount)
	}

	// Change metadata to not match
	node1.metadata.SetString("zone", "us-central")
	dng.handleNodeMetadataChange(node1)

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	removedCount := len(removedNodes)
	mu.Unlock()

	if removedCount != 1 {
		t.Errorf("Expected 1 removed node, got %d", removedCount)
	}
}

func TestDataNodeGroupMetadataChangeNonAliveNode(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Create dead node
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.updateState(node1.ID, NodeDead)

	// Metadata change on dead node should be ignored
	dng.handleNodeMetadataChange(node1)

	if dng.Count() != 0 {
		t.Errorf("Expected 0 nodes, got %d", dng.Count())
	}
}

func TestDataNodeGroupGetNodes(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{
		DataInitializer: func(node *Node) *TestData {
			return &TestData{Value: "test", Counter: 0}
		},
	}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Add matching nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	dng.addNode(node1)

	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	node2.metadata.SetString("zone", "us-west")
	dng.addNode(node2)

	node3 := newNode(NodeID(uuid.New()), "127.0.0.1:8003")
	node3.metadata.SetString("zone", "us-west")
	dng.addNode(node3)

	// Test getting all nodes
	nodes := dng.GetNodes(nil)
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// Test excluding nodes
	excludeIDs := []NodeID{node1.ID, node3.ID}
	nodes = dng.GetNodes(excludeIDs)
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(nodes))
	}
	if nodes[0].ID != node2.ID {
		t.Error("Wrong node returned")
	}
}

func TestDataNodeGroupGetNodeData(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{
		DataInitializer: func(node *Node) *TestData {
			return &TestData{
				Value:   "init-" + node.ID.String()[:8],
				Counter: 42,
			}
		},
	}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	dng.addNode(node1)

	// Test getting existing node data
	data := dng.GetNodeData(node1.ID)
	if data == nil {
		t.Fatal("Expected node data, got nil")
	}
	if data.Counter != 42 {
		t.Errorf("Expected counter 42, got %d", data.Counter)
	}
	if !strings.HasPrefix(data.Value, "init-") {
		t.Errorf("Expected value to start with 'init-', got %s", data.Value)
	}

	// Test getting non-existent node data
	nonExistentID := NodeID(uuid.New())
	data = dng.GetNodeData(nonExistentID)
	if data != nil {
		t.Error("Expected nil for non-existent node")
	}
}

func TestDataNodeGroupUpdateNodeData(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{
		DataInitializer: func(node *Node) *TestData {
			return &TestData{Value: "initial", Counter: 0}
		},
	}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	dng.addNode(node1)

	// Test updating existing node data
	err := dng.UpdateNodeData(node1.ID, func(node *Node, data *TestData) error {
		data.Value = "updated"
		data.Counter = 100
		return nil
	})
	if err != nil {
		t.Errorf("UpdateNodeData failed: %v", err)
	}

	// Verify update
	data := dng.GetNodeData(node1.ID)
	if data.Value != "updated" {
		t.Errorf("Expected value 'updated', got %s", data.Value)
	}
	if data.Counter != 100 {
		t.Errorf("Expected counter 100, got %d", data.Counter)
	}

	// Test updating non-existent node (should not error)
	nonExistentID := NodeID(uuid.New())
	err = dng.UpdateNodeData(nonExistentID, func(node *Node, data *TestData) error {
		return nil
	})
	if err != nil {
		t.Errorf("UpdateNodeData for non-existent node should not error: %v", err)
	}
}

func TestDataNodeGroupGetDataNodes(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{
		DataInitializer: func(node *Node) *TestData {
			return &TestData{Value: "test", Counter: 1}
		},
	}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Add nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	dng.addNode(node1)

	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	node2.metadata.SetString("zone", "us-west")
	dng.addNode(node2)

	// Get all data
	dataNodes := dng.GetDataNodes()
	if len(dataNodes) != 2 {
		t.Errorf("Expected 2 data nodes, got %d", len(dataNodes))
	}

	for _, data := range dataNodes {
		if data.Value != "test" || data.Counter != 1 {
			t.Errorf("Unexpected data values: %+v", data)
		}
	}
}

func TestDataNodeGroupContains(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")

	// Initially empty
	if dng.Contains(node1.ID) {
		t.Error("Should not contain node1 initially")
	}

	// Add node1
	dng.addNode(node1)
	if !dng.Contains(node1.ID) {
		t.Error("Should contain node1 after adding")
	}
	if dng.Contains(node2.ID) {
		t.Error("Should not contain node2")
	}

	// Remove node1
	dng.removeNode(node1)
	if dng.Contains(node1.ID) {
		t.Error("Should not contain node1 after removing")
	}
}

func TestDataNodeGroupCount(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Initially empty
	if dng.Count() != 0 {
		t.Errorf("Expected count 0, got %d", dng.Count())
	}

	// Add nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")

	dng.addNode(node1)
	if dng.Count() != 1 {
		t.Errorf("Expected count 1, got %d", dng.Count())
	}

	dng.addNode(node2)
	if dng.Count() != 2 {
		t.Errorf("Expected count 2, got %d", dng.Count())
	}

	// Remove node
	dng.removeNode(node1)
	if dng.Count() != 1 {
		t.Errorf("Expected count 1, got %d", dng.Count())
	}
}

func TestDataNodeGroupGetShard(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	nodeID := NodeID(uuid.New())
	shard1 := dng.getShard(nodeID)
	shard2 := dng.getShard(nodeID)

	// Same node ID should always return same shard
	if shard1 != shard2 {
		t.Error("Same node ID should return same shard")
	}

	// Shard should be within bounds
	found := false
	for _, s := range dng.shards {
		if s == shard1 {
			found = true
			break
		}
	}
	if !found {
		t.Error("Returned shard not found in shards array")
	}
}

func TestDataNodeGroupSendToPeers(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	// Add some nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	dng.addNode(node1)

	// Test SendToPeers
	err := dng.SendToPeers(UserMsg, "test message")
	if err != nil {
		t.Errorf("SendToPeers failed: %v", err)
	}

	// Test SendToPeersReliable
	err = dng.SendToPeersReliable(UserMsg, "test message")
	if err != nil {
		t.Errorf("SendToPeersReliable failed: %v", err)
	}
}

func TestDataNodeGroupClose(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)

	// Add some nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	dng.addNode(node1)

	if dng.Count() != 1 {
		t.Errorf("Expected count 1, got %d", dng.Count())
	}

	// Close should unregister handlers but not clear nodes (unlike NodeGroup)
	dng.Close()

	// Nodes should still be there
	if dng.Count() != 1 {
		t.Errorf("Expected count 1 after close, got %d", dng.Count())
	}
}

func TestDataNodeGroupDefaultDataInitializer(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{
		// No DataInitializer provided
	}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node1.metadata.SetString("zone", "us-west")
	dng.addNode(node1)

	// Should create zero value
	data := dng.GetNodeData(node1.ID)
	if data == nil {
		t.Fatal("Expected data, got nil")
	}
	if data.Value != "" || data.Counter != 0 {
		t.Errorf("Expected zero values, got %+v", data)
	}
}

func TestDataNodeGroupAddNodeTwice(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")

	// Add node twice
	dng.addNode(node1)
	dng.addNode(node1)

	// Should only be counted once
	if dng.Count() != 1 {
		t.Errorf("Expected count 1, got %d", dng.Count())
	}
}

func TestDataNodeGroupRemoveNonExistentNode(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")

	// Remove node that was never added (should not panic)
	dng.removeNode(node1)

	if dng.Count() != 0 {
		t.Errorf("Expected count 0, got %d", dng.Count())
	}
}

func TestDataNodeGroupConcurrentAccess(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	criteria := map[string]string{"zone": "us-west"}
	opts := &DataNodeGroupOptions[TestData]{
		DataInitializer: func(node *Node) *TestData {
			return &TestData{Value: "test", Counter: 0}
		},
	}
	dng := NewDataNodeGroup(cluster, criteria, opts)
	defer dng.Close()

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

				dng.addNode(node)
				dng.Contains(node.ID)
				dng.Count()
				dng.GetNodes(nil)
				dng.GetNodeData(node.ID)
				dng.UpdateNodeData(node.ID, func(n *Node, d *TestData) error {
					d.Counter++
					return nil
				})
				dng.removeNode(node)
			}
		}(i)
	}

	wg.Wait()
}