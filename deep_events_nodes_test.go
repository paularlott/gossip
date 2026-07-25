package gossip

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

// ============================================================================
// EventHandlers: Add, Remove, ForEach
// ============================================================================

func TestEventHandlersAddRemove(t *testing.T) {
	handlers := NewEventHandlers[func()]()

	called1, called2 := false, false

	id1 := handlers.Add(func() { called1 = true })
	id2 := handlers.Add(func() { called2 = true })

	handlers.ForEach(func(fn func()) { fn() })

	if !called1 || !called2 {
		t.Error("Both handlers should have been called")
	}

	// Remove first handler
	if !handlers.Remove(id1) {
		t.Error("Should succeed removing existing handler")
	}

	called1, called2 = false, false
	handlers.ForEach(func(fn func()) { fn() })

	if called1 {
		t.Error("Removed handler should not be called")
	}
	if !called2 {
		t.Error("Second handler should still be called")
	}

	// Remove second
	if !handlers.Remove(id2) {
		t.Error("Should succeed removing second handler")
	}

	// Remove non-existent
	if handlers.Remove(id1) {
		t.Error("Should fail removing already-removed handler")
	}
}

func TestEventHandlersForEachEmptyCollection(t *testing.T) {
	handlers := NewEventHandlers[func()]()

	// Should not panic on empty
	handlers.ForEach(func(fn func()) { fn() })
}

func TestEventHandlersConcurrent(t *testing.T) {
	handlers := NewEventHandlers[func()]()

	var wg sync.WaitGroup
	const numGoroutines = 10

	// Concurrent adds
	ids := make(chan HandlerID, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := handlers.Add(func() {})
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)

	// Concurrent ForEach while removing
	var ids2 []HandlerID
	for id := range ids {
		ids2 = append(ids2, id)
	}

	for _, id := range ids2 {
		wg.Add(1)
		go func(id HandlerID) {
			defer wg.Done()
			handlers.Remove(id)
		}(id)
	}
	wg.Wait()
}

// ============================================================================
// messageHistory
// ============================================================================

func TestMessageHistoryContainsAndRecord(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 16
	config.MsgHistoryMaxAge = 1 * time.Second
	config.MsgHistoryGCInterval = 100 * time.Millisecond

	history := newMessageHistory(config)
	defer history.stop()

	senderID := NodeID(uuid.New())
	msgID := MessageID(hlc.Now())

	// Not in history initially
	if history.contains(senderID, msgID) {
		t.Error("New message should not be in history")
	}

	// Record it
	history.recordMessage(senderID, msgID)

	// Now it should be there
	if !history.contains(senderID, msgID) {
		t.Error("Message should be in history after recording")
	}

	// Different message should not be there
	otherMsgID := MessageID(hlc.Now())
	if history.contains(senderID, otherMsgID) {
		t.Error("Different message should not be in history")
	}
}

func TestMessageHistoryPruning(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 16
	config.MsgHistoryMaxAge = 50 * time.Millisecond
	config.MsgHistoryGCInterval = 50 * time.Millisecond

	history := newMessageHistory(config)
	defer history.stop()

	senderID := NodeID(uuid.New())
	msgID := MessageID(hlc.Now())
	history.recordMessage(senderID, msgID)

	if !history.contains(senderID, msgID) {
		t.Error("Message should be in history")
	}

	// Wait for pruning
	time.Sleep(200 * time.Millisecond)

	if history.contains(senderID, msgID) {
		t.Error("Message should have been pruned after MaxAge")
	}
}

func TestMessageHistorySharding(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 16
	config.MsgHistoryMaxAge = 10 * time.Second
	config.MsgHistoryGCInterval = 10 * time.Second

	history := newMessageHistory(config)
	defer history.stop()

	// Add many messages from many senders
	for i := 0; i < 100; i++ {
		senderID := NodeID(uuid.New())
		msgID := MessageID(hlc.Now())
		history.recordMessage(senderID, msgID)

		if !history.contains(senderID, msgID) {
			t.Fatalf("Message %d should be in history", i)
		}
	}
}

// ============================================================================
// nodeList: comprehensive tests
// ============================================================================

func TestNodeListAddRemoveGet(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")

	// Add
	cluster.nodes.addOrUpdate(node)
	if cluster.nodes.get(nodeID) == nil {
		t.Error("Node should be added")
	}

	// Update existing
	updatedNode := newNode(nodeID, "127.0.0.1:8001")
	updatedNode.observedState = NodeSuspect
	cluster.nodes.addOrUpdate(updatedNode)

	got := cluster.nodes.get(nodeID)
	if got.GetObservedState() != NodeSuspect {
		t.Error("Node state should be updated")
	}

	// Remove
	cluster.nodes.remove(nodeID)
	if cluster.nodes.get(nodeID) != nil {
		t.Error("Node should be removed")
	}
}

func TestNodeListRemoveLocalNodeProtected(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Cannot remove local node
	cluster.nodes.remove(cluster.localNode.ID)

	if cluster.nodes.get(cluster.localNode.ID) == nil {
		t.Error("Local node should be protected from removal")
	}
}

func TestNodeListRemoveIfInStateDeep(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Try removing alive node when filtering for dead
	removed := cluster.nodes.removeIfInState(nodeID, []NodeState{NodeDead})
	if removed {
		t.Error("Should not remove alive node when filtering for dead")
	}

	// Remove alive node
	removed = cluster.nodes.removeIfInState(nodeID, []NodeState{NodeAlive})
	if !removed {
		t.Error("Should remove alive node when filtering for alive")
	}

	// Remove non-existent
	removed = cluster.nodes.removeIfInState(nodeID, []NodeState{NodeAlive})
	if removed {
		t.Error("Should not remove non-existent node")
	}
}

func TestNodeListUpdateState(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Same state - no-op
	if !cluster.nodes.updateState(nodeID, NodeAlive) {
		t.Error("Update to same state should return true")
	}

	// Different state
	if !cluster.nodes.updateState(nodeID, NodeSuspect) {
		t.Error("Update to different state should return true")
	}

	if cluster.nodes.get(nodeID).GetObservedState() != NodeSuspect {
		t.Error("State should be updated")
	}

	// Non-existent node
	if cluster.nodes.updateState(NodeID(uuid.New()), NodeDead) {
		t.Error("Should return false for non-existent node")
	}
}

func TestNodeListGetRandomNodes(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add 20 nodes
	for i := 0; i < 20; i++ {
		n := newNode(NodeID(uuid.New()), "127.0.0.1:800"+string(rune('0'+i%10)))
		cluster.nodes.addOrUpdate(n)
	}

	// Get random subset
	nodes := cluster.nodes.getRandomNodes(5, []NodeID{cluster.localNode.ID})
	if len(nodes) > 5 {
		t.Errorf("Should return at most 5 nodes, got %d", len(nodes))
	}

	// Verify exclusion
	for _, n := range nodes {
		if n.ID == cluster.localNode.ID {
			t.Error("Excluded node should not appear in results")
		}
	}
}

func TestNodeListGetRandomNodesWithTag(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add tagged and untagged nodes
	for i := 0; i < 10; i++ {
		n := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
		if i%2 == 0 {
			n = newNode(NodeID(uuid.New()), "127.0.0.1:8001", "web")
		}
		cluster.nodes.addOrUpdate(n)
	}

	// Get only web-tagged nodes
	nodes := cluster.nodes.getRandomNodesWithTag(10, "web", nil)
	for _, n := range nodes {
		if !n.HasTag("web") {
			t.Error("All returned nodes should have 'web' tag")
		}
	}
}

func TestNodeListGetRandomNodesInStatesEmpty(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Empty states
	nodes := cluster.nodes.getRandomNodesInStates(5, []NodeState{}, nil)
	if len(nodes) != 0 {
		t.Error("Empty states should return empty")
	}

	// k <= 0
	nodes = cluster.nodes.getRandomNodesInStates(0, []NodeState{NodeAlive}, nil)
	if len(nodes) != 0 {
		t.Error("k=0 should return empty")
	}
}

func TestNodeListForAllInStatesDeep(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	for i := 0; i < 5; i++ {
		n := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
		cluster.nodes.addOrUpdate(n)
	}

	count := 0
	cluster.nodes.forAllInStates([]NodeState{NodeAlive}, func(n *Node) bool {
		count++
		return true
	})

	if count != 6 { // 5 added + local
		t.Errorf("Expected 6 alive nodes, iterated %d", count)
	}

	// Test early termination
	count = 0
	cluster.nodes.forAllInStates([]NodeState{NodeAlive}, func(n *Node) bool {
		count++
		return count < 3
	})

	if count != 3 {
		t.Errorf("Expected to stop after 3, iterated %d", count)
	}
}

func TestNodeListGetByTag(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	n1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001", "api")
	n2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002", "web")
	n3 := newNode(NodeID(uuid.New()), "127.0.0.1:8003", "api", "web")
	cluster.nodes.addOrUpdate(n1)
	cluster.nodes.addOrUpdate(n2)
	cluster.nodes.addOrUpdate(n3)

	apiNodes := cluster.nodes.getByTag("api")
	if len(apiNodes) != 2 {
		t.Errorf("Expected 2 api-tagged nodes, got %d", len(apiNodes))
	}
}

func TestNodeListCounters(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Initial: 2 alive (local + node)
	if cluster.nodes.getAliveCount() != 2 {
		t.Errorf("Expected 2 alive, got %d", cluster.nodes.getAliveCount())
	}

	// Transition to suspect
	cluster.nodes.updateState(nodeID, NodeSuspect)
	if cluster.nodes.getSuspectCount() != 1 {
		t.Errorf("Expected 1 suspect, got %d", cluster.nodes.getSuspectCount())
	}
	if cluster.nodes.getAliveCount() != 1 {
		t.Errorf("Expected 1 alive, got %d", cluster.nodes.getAliveCount())
	}

	// Transition to leaving
	cluster.nodes.updateState(nodeID, NodeLeaving)
	if cluster.nodes.getLeavingCount() != 1 {
		t.Errorf("Expected 1 leaving, got %d", cluster.nodes.getLeavingCount())
	}

	// Transition to dead
	cluster.nodes.updateState(nodeID, NodeDead)
	if cluster.nodes.getDeadCount() != 1 {
		t.Errorf("Expected 1 dead, got %d", cluster.nodes.getDeadCount())
	}
}

func TestNodeListStateCacheInvalidation(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Get cached list
	alive := cluster.nodes.getAllInStates([]NodeState{NodeAlive})
	initialCount := len(alive)

	// Add another node - cache should be invalidated
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	cluster.nodes.addOrUpdate(node2)

	alive = cluster.nodes.getAllInStates([]NodeState{NodeAlive})
	if len(alive) != initialCount+1 {
		t.Errorf("Expected %d alive after cache invalidation, got %d", initialCount+1, len(alive))
	}
}

// ============================================================================
// stateSetToKey
// ============================================================================

func TestStateSetToKey(t *testing.T) {
	// Empty
	if stateSetToKey([]NodeState{}) != "" {
		t.Error("Empty states should produce empty key")
	}

	// Single state
	key := stateSetToKey([]NodeState{NodeAlive})
	if key != "1" { // NodeAlive = 1
		t.Errorf("Single state key: expected '1', got '%s'", key)
	}

	// Two states (sorted)
	key = stateSetToKey([]NodeState{NodeSuspect, NodeAlive})
	expected := "1,2" // sorted: Alive(1), Suspect(2)
	if key != expected {
		t.Errorf("Two state key: expected '%s', got '%s'", expected, key)
	}

	// Three+ states
	key = stateSetToKey([]NodeState{NodeDead, NodeAlive, NodeSuspect})
	expected = "1,2,3" // sorted
	if key != expected {
		t.Errorf("Three state key: expected '%s', got '%s'", expected, key)
	}
}

// ============================================================================
// Node: methods
// ============================================================================

func TestNodeMethods(t *testing.T) {
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")

	// State checks
	if !node.Alive() {
		t.Error("New node should be alive")
	}
	if node.Suspect() {
		t.Error("New node should not be suspect")
	}
	if node.DeadOrLeft() {
		t.Error("New node should not be dead or left")
	}
	if node.Removed() {
		t.Error("New node should not be removed")
	}

	// Observable state
	if node.GetObservedState() != NodeAlive {
		t.Error("New node should have alive observed state")
	}

	// Activity tracking
	oldActivity := node.getLastActivity()
	time.Sleep(1 * time.Millisecond)
	node.updateLastActivity()
	newActivity := node.getLastActivity()
	if !newActivity.After(oldActivity) {
		t.Error("Last activity should be updated")
	}

	// Address operations
	if !node.IsAddressEmpty() {
		t.Error("Initial address should be empty")
	}

	node.SetAddress(Address{IP: net.ParseIP("127.0.0.1"), Port: 8001})
	if node.IsAddressEmpty() {
		t.Error("Address should be set")
	}

	addr := node.GetAddress()
	if !addr.IP.Equal(net.ParseIP("127.0.0.1")) || addr.Port != 8001 {
		t.Errorf("Address mismatch: %+v", addr)
	}

	node.ClearAddress()
	if !node.IsAddressEmpty() {
		t.Error("Address should be cleared")
	}

	// Advertise address
	if node.AdvertisedAddr() != "127.0.0.1:8001" {
		t.Errorf("Expected advertise addr 127.0.0.1:8001, got %s", node.AdvertisedAddr())
	}
}

func TestNodeTags(t *testing.T) {
	// No tags
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	tags := node.GetTags()
	if len(tags) != 0 {
		t.Errorf("Expected empty tags, got %v", tags)
	}
	if node.HasTag("web") {
		t.Error("Should not have tag 'web'")
	}

	// With tags
	node = newNode(NodeID(uuid.New()), "127.0.0.1:8001", "web", "api")
	tags = node.GetTags()
	if len(tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(tags))
	}
	if !node.HasTag("web") {
		t.Error("Should have tag 'web'")
	}
	if !node.HasTag("api") {
		t.Error("Should have tag 'api'")
	}
	if node.HasTag("db") {
		t.Error("Should not have tag 'db'")
	}

	// Tags are copied
	tags[0] = "modified"
	if node.HasTag("modified") {
		t.Error("Tags should be a defensive copy")
	}
}

func TestNodeNewWithTags(t *testing.T) {
	// Backward-compat wrapper
	node := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8001", []string{"tag1", "tag2"})
	if !node.HasTag("tag1") || !node.HasTag("tag2") {
		t.Error("Tags should be set via newNodeWithTags")
	}
}

// ============================================================================
// NodeState: String()
// ============================================================================

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{NodeUnknown, "Unknown"},
		{NodeAlive, "Alive"},
		{NodeSuspect, "Suspect"},
		{NodeDead, "Dead"},
		{NodeLeaving, "Leaving"},
		{NodeRemoved, "Removed"},
		{NodeState(99), "Unknown"}, // unknown value
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.state.String() != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, tt.state.String())
			}
		})
	}
}

// ============================================================================
// NodeID: String()
// ============================================================================

func TestNodeIDString(t *testing.T) {
	u := uuid.New()
	nodeID := NodeID(u)
	if nodeID.String() != u.String() {
		t.Errorf("NodeID.String() should match uuid.String()")
	}
}
