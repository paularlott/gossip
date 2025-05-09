package gossip

import (
	"reflect"
	"testing"
	"time"

	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/websocket"

	"github.com/google/uuid"
)

// Helper function to create a test cluster
func createTestCluster(t *testing.T) *Cluster {
	config := DefaultConfig()
	// Make sharding predictable for tests
	config.NodeShardCount = 8
	config.MsgCodec = codec.NewJsonCodec()
	config.WebsocketProvider = websocket.NewGorillaProvider(5*time.Second, true, "")
	config.AllowInsecureWebsockets = true
	config.SocketTransportEnabled = false
	config.BindAddr = "ws://127.0.0.1:8080/gossip"

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create test cluster: %v", err)
	}

	// Remove own node from the cluster
	cluster.nodes.remove(cluster.localNode.ID)

	return cluster
}

// Helper to create test nodes with specific IDs and states
func createTestNode(id string, state NodeState) *Node {
	uuid, _ := uuid.Parse(id)
	nodeID := NodeID(uuid)

	return &Node{
		ID:              nodeID,
		state:           state,
		stateChangeTime: time.Now(),
	}
}

func TestNodeList_AddAndGet(t *testing.T) {
	cluster := createTestCluster(t)
	nl := cluster.nodes

	// Create test node
	node := createTestNode("11111111-1111-1111-1111-111111111111", NodeAlive)

	// Test adding a new node
	if !nl.addIfNotExists(node) {
		t.Errorf("Failed to add new node")
	}

	// Verify node was added
	retrievedNode := nl.get(node.ID)
	if retrievedNode == nil {
		t.Errorf("Failed to retrieve added node")
	}

	// Check node properties
	if retrievedNode.ID != node.ID {
		t.Errorf("Retrieved node has incorrect ID")
	}

	if retrievedNode.state != NodeAlive {
		t.Errorf("Retrieved node has incorrect state, got %v, want %v", retrievedNode.state, NodeAlive)
	}

	// Try to add the same node again with addIfNotExists
	if nl.addIfNotExists(node) {
		t.Errorf("addIfNotExists should return false for existing node")
	}

	// Modify the node and use addOrUpdate
	modifiedNode := createTestNode("11111111-1111-1111-1111-111111111111", NodeSuspect)
	if !nl.addOrUpdate(modifiedNode) {
		t.Errorf("addOrUpdate should return true when updating existing node")
	}

	// Check that node state was updated
	retrievedNode = nl.get(node.ID)
	if retrievedNode.state != NodeSuspect {
		t.Errorf("Node state not updated, got %v, want %v", retrievedNode.state, NodeSuspect)
	}
}

func TestNodeList_Remove(t *testing.T) {
	cluster := createTestCluster(t)
	nl := cluster.nodes

	// Create and add test nodes
	node1 := createTestNode("11111111-1111-1111-1111-111111111111", NodeAlive)
	node2 := createTestNode("22222222-2222-2222-2222-222222222222", NodeSuspect)

	nl.addIfNotExists(node1)
	nl.addIfNotExists(node2)

	// Verify initial state
	if nl.getAliveCount()+nl.getSuspectCount() != 2 {
		t.Errorf("Expected total count of 2, got %d", nl.getAliveCount()+nl.getSuspectCount())
	}

	// Test removing a node
	nl.remove(node1.ID)

	// Verify node was removed
	if nl.get(node1.ID) != nil {
		t.Errorf("Node should be removed but still exists")
	}

	if nl.getAliveCount()+nl.getSuspectCount() != 1 {
		t.Errorf("Expected total count of 1 after removal, got %d", nl.getAliveCount()+nl.getSuspectCount())
	}

	// Test removeIfInState with matching state
	nl.removeIfInState(node2.ID, []NodeState{NodeSuspect})
	if nl.get(node2.ID) != nil {
		t.Errorf("Node should be removed by removeIfInState but still exists")
	}

	// Test removeIfInState with non-matching state
	node3 := createTestNode("33333333-3333-3333-3333-333333333333", NodeAlive)
	nl.addIfNotExists(node3)

	if nl.removeIfInState(node3.ID, []NodeState{NodeSuspect, NodeDead}) {
		t.Errorf("removeIfInState should return false for non-matching state")
	}

	if nl.get(node3.ID) == nil {
		t.Errorf("Node should not be removed when state doesn't match")
	}
}

func TestNodeList_UpdateState(t *testing.T) {
	cluster := createTestCluster(t)
	nl := cluster.nodes

	// Create and add a test node
	node := createTestNode("11111111-1111-1111-1111-111111111111", NodeAlive)
	nl.addIfNotExists(node)

	// Verify initial counters
	if nl.getAliveCount() != 1 || nl.getSuspectCount() != 0 {
		t.Errorf("Initial counters incorrect, alive=%d, suspect=%d", nl.getAliveCount(), nl.getSuspectCount())
	}

	// Update state
	if !nl.updateState(node.ID, NodeSuspect) {
		t.Errorf("Failed to update node state")
	}

	// Verify node state was updated
	updatedNode := nl.get(node.ID)
	if updatedNode.state != NodeSuspect {
		t.Errorf("Node state not updated, got %v, want %v", updatedNode.state, NodeSuspect)
	}

	// Verify counters were updated
	if nl.getAliveCount() != 0 || nl.getSuspectCount() != 1 {
		t.Errorf("Counters not updated correctly, alive=%d, suspect=%d", nl.getAliveCount(), nl.getSuspectCount())
	}

	// Try updating with same state - should return true but not change anything
	prevTime := updatedNode.stateChangeTime
	time.Sleep(10 * time.Millisecond) // Ensure time would change if updated

	if !nl.updateState(node.ID, NodeSuspect) {
		t.Errorf("updateState should return true even when state doesn't change")
	}

	// Verify stateChangeTime wasn't updated since state didn't change
	afterNode := nl.get(node.ID)
	if afterNode.stateChangeTime != prevTime {
		t.Errorf("stateChangeTime shouldn't be updated when state doesn't change")
	}

	// Update to dead state
	nl.updateState(node.ID, NodeDead)

	// Verify live count is updated
	if nl.getAliveCount() != 0 && nl.getSuspectCount() != 0 {
		t.Errorf("Live count should be 0 after node marked dead, got %d", nl.getAliveCount()+nl.getSuspectCount())
	}
}

func TestNodeList_GetRandomNodesInStates(t *testing.T) {
	cluster := createTestCluster(t)
	nl := cluster.nodes

	// Create nodes in different states
	aliveNode1 := createTestNode("11111111-1111-1111-1111-111111111111", NodeAlive)
	aliveNode2 := createTestNode("22222222-2222-2222-2222-222222222222", NodeAlive)
	suspectNode := createTestNode("33333333-3333-3333-3333-333333333333", NodeSuspect)
	deadNode := createTestNode("44444444-4444-4444-4444-444444444444", NodeDead)

	nl.addIfNotExists(aliveNode1)
	nl.addIfNotExists(aliveNode2)
	nl.addIfNotExists(suspectNode)
	nl.addIfNotExists(deadNode)

	// Get only alive nodes
	aliveNodes := nl.getRandomNodesInStates(10, []NodeState{NodeAlive}, nil)
	if len(aliveNodes) != 2 {
		t.Errorf("Expected 2 alive nodes, got %d", len(aliveNodes))
	}

	// Get alive and suspect nodes (live nodes)
	liveNodes := nl.getRandomLiveNodes(10, nil)
	if len(liveNodes) != 3 {
		t.Errorf("Expected 3 live nodes, got %d", len(liveNodes))
	}

	// Get all nodes
	allNodes := nl.getRandomNodes(10, nil)
	if len(allNodes) != 4 {
		t.Errorf("Expected 4 nodes total, got %d", len(allNodes))
	}

	// Get nodes with exclusion
	excludeIDs := []NodeID{aliveNode1.ID}
	nodesWithExclusion := nl.getRandomNodesInStates(10, []NodeState{NodeAlive}, excludeIDs)
	if len(nodesWithExclusion) != 1 {
		t.Errorf("Expected 1 alive node after exclusion, got %d", len(nodesWithExclusion))
	}

	// Test limit
	limitedNodes := nl.getRandomNodesInStates(2, []NodeState{NodeAlive, NodeSuspect, NodeDead}, nil)
	if len(limitedNodes) != 2 {
		t.Errorf("Expected 2 nodes with limit, got %d", len(limitedNodes))
	}

	// Test empty state list
	emptyStateNodes := nl.getRandomNodesInStates(10, []NodeState{}, nil)
	if len(emptyStateNodes) != 0 {
		t.Errorf("Expected 0 nodes with empty state list, got %d", len(emptyStateNodes))
	}
}

func TestNodeList_ForAllInStates(t *testing.T) {
	cluster := createTestCluster(t)
	nl := cluster.nodes

	// Add test nodes
	node1 := createTestNode("11111111-1111-1111-1111-111111111111", NodeAlive)
	node2 := createTestNode("22222222-2222-2222-2222-222222222222", NodeAlive)
	node3 := createTestNode("33333333-3333-3333-3333-333333333333", NodeSuspect)

	nl.addIfNotExists(node1)
	nl.addIfNotExists(node2)
	nl.addIfNotExists(node3)

	// Test forAllInStates with counting
	count := 0
	nl.forAllInStates([]NodeState{NodeAlive}, func(node *Node) bool {
		count++
		return true
	})

	if count != 2 {
		t.Errorf("forAllInStates should have counted 2 alive nodes, got %d", count)
	}

	// Test early termination
	visited := 0
	nl.forAllInStates([]NodeState{NodeAlive, NodeSuspect}, func(node *Node) bool {
		visited++
		return visited < 2 // Stop after visiting one node
	})

	if visited != 2 {
		t.Errorf("forAllInStates should have stopped after 2 visits, visited %d", visited)
	}

	// Test getAllInStates
	aliveNodes := nl.getAllInStates([]NodeState{NodeAlive})
	if len(aliveNodes) != 2 {
		t.Errorf("getAllInStates should return 2 alive nodes, got %d", len(aliveNodes))
	}

	// Test getAll
	allNodes := nl.getAll()
	if len(allNodes) != 3 {
		t.Errorf("getAll should return all 3 nodes, got %d", len(allNodes))
	}
}

func TestNodeList_RecalculateCounters(t *testing.T) {
	cluster := createTestCluster(t)
	nl := cluster.nodes

	// Add nodes of different states
	nl.addIfNotExists(createTestNode("11111111-1111-1111-1111-111111111111", NodeAlive))
	nl.addIfNotExists(createTestNode("22222222-2222-2222-2222-222222222222", NodeAlive))
	nl.addIfNotExists(createTestNode("33333333-3333-3333-3333-333333333333", NodeSuspect))
	nl.addIfNotExists(createTestNode("44444444-4444-4444-4444-444444444444", NodeDead))
	nl.addIfNotExists(createTestNode("55555555-5555-5555-5555-555555555555", NodeLeaving))

	// Manually corrupt the counters
	nl.aliveCount.Store(0)
	nl.suspectCount.Store(0)
	nl.leavingCount.Store(0)
	nl.deadCount.Store(0)

	// Run recalculation
	nl.recalculateCounters()

	// Verify counters are restored correctly
	if nl.getAliveCount() != 2 {
		t.Errorf("Alive count incorrect, got %d, want 2", nl.getAliveCount())
	}

	if nl.getSuspectCount() != 1 {
		t.Errorf("Suspect count incorrect, got %d, want 1", nl.getSuspectCount())
	}

	if nl.getLeavingCount() != 1 {
		t.Errorf("Leaving count incorrect, got %d, want 1", nl.getLeavingCount())
	}

	if nl.getDeadCount() != 1 {
		t.Errorf("Dead count incorrect, got %d, want 1", nl.getDeadCount())
	}
}

func TestNodeList_GetShard(t *testing.T) {
	cluster := createTestCluster(t)
	nl := cluster.nodes

	// Create nodes with deterministically different IDs
	nodeID1 := NodeID(uuid.New())
	nodeID2 := NodeID(uuid.New())

	// Get shards
	shard1 := nl.getShard(nodeID1)
	shard2 := nl.getShard(nodeID2)

	// Verify that shard selection is deterministic
	shard1Again := nl.getShard(nodeID1)

	if shard1 != shard1Again {
		t.Errorf("Shard selection should be deterministic for the same node ID")
	}

	// This might sometimes fail in extremely rare cases due to hash collisions
	// but it should catch obvious issues with the sharding algorithm
	if nl.shardCount > 1 && reflect.DeepEqual(nodeID1, nodeID2) == false && shard1 == shard2 {
		t.Logf("Note: Different node IDs mapped to same shard - this is possible but unlikely")
	}
}

func TestNodeList_ShardDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping distribution test in short mode")
	}

	cluster := createTestCluster(t)
	nl := cluster.nodes

	// Create a map to count nodes per shard
	shardCounts := make(map[*nodeListShard]int)

	// Generate a reasonable number of random UUIDs and check their shard distribution
	for i := 0; i < 1000; i++ {
		randomID := NodeID(uuid.New())
		shard := nl.getShard(randomID)
		shardCounts[shard]++
	}

	// The expected distribution would be approximately even
	expected := 1000 / nl.shardCount
	tolerance := int(float64(expected) * 0.3) // Allow 30% deviation

	for shard, count := range shardCounts {
		if count < expected-int(tolerance) || count > expected+int(tolerance) {
			t.Errorf("Shard distribution appears uneven. Shard %p has %d nodes (expected ~%d±%d)",
				shard, count, expected, int(tolerance))
		}
	}
}
