package gossip

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

func newTestCluster(t *testing.T) *Cluster {
	config := DefaultConfig()
	config.NodeShardCount = 4
	config.MsgCodec = codec.NewJsonCodec()
	config.Transport = &mockTransport{}
	// Minimal transport / codec placeholders for tests
	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	return cluster
}

// Using built-in JSON codec for tests

func TestNodeListAddAndGet(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	id := NodeID(uuid.New())
	n := newNode(id, "127.0.0.1:1000")
	if !nl.addIfNotExists(n) {
		t.Fatal("expected addIfNotExists true")
	}
	if nl.addIfNotExists(n) {
		t.Fatal("expected second addIfNotExists false")
	}

	g := nl.get(id)
	if g == nil || g.ID != id {
		t.Fatalf("expected to retrieve node")
	}
}

func TestNodeListStateTransitions(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	id := NodeID(uuid.New())
	n := newNode(id, "127.0.0.1:2000")
	nl.addIfNotExists(n)

	if !nl.updateState(id, NodeSuspect, nil) {
		t.Fatalf("failed to mark suspect")
	}
	if nl.getSuspectCount() != 1 {
		t.Fatalf("suspect count mismatch")
	}
	if !nl.updateState(id, NodeDead, nil) {
		t.Fatalf("failed to mark dead")
	}
	if nl.getDeadCount() != 1 {
		t.Fatalf("dead count mismatch")
	}
}

func TestNodeListRemoveIfInState(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes
	id := NodeID(uuid.New())
	n := newNode(id, "addr")
	nl.addIfNotExists(n)
	// +1 for local node
	if nl.getAliveCount() != 2 {
		t.Fatalf("alive count 2 expected, got %d", nl.getAliveCount())
	}
	if !nl.removeIfInState(id, []NodeState{NodeAlive}) {
		t.Fatalf("expected removal")
	}
	if nl.getAliveCount() != 1 {
		t.Fatalf("alive count should be 1 (local node), got %d", nl.getAliveCount())
	}
}

// --- Benchmarks ---

// helper to populate a cluster with N nodes
func populateCluster(t testing.TB, count int) (*Cluster, []NodeID) {
	c := newTestCluster(&testing.T{})
	ids := make([]NodeID, 0, count)
	for i := 0; i < count; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "node")
		// Spread timestamps a little
		n.lastMessageTime = hlc.Now()
		c.nodes.addIfNotExists(n)
		ids = append(ids, id)
	}
	return c, ids
}

func BenchmarkNodeListAdd(b *testing.B) {
	c := newTestCluster(&testing.T{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		c.nodes.addIfNotExists(n)
	}
}

func BenchmarkNodeListUpdateState(b *testing.B) {
	c, ids := populateCluster(b, 10_000)
	// Ensure all start Alive
	for _, id := range ids {
		_ = c.nodes.updateState(id, NodeAlive, nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i%len(ids)]
		// Toggle Alive/Suspect to exercise counter changes
		if i%2 == 0 {
			c.nodes.updateState(id, NodeSuspect, nil)
		} else {
			c.nodes.updateState(id, NodeAlive, nil)
		}
	}
}

func BenchmarkNodeListGetRandomNodes(b *testing.B) {
	c, _ := populateCluster(b, 25_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.nodes.getRandomNodes(32, nil)
	}
}

func BenchmarkNodeListConcurrentRandom(b *testing.B) {
	c, _ := populateCluster(b, 40_000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.nodes.getRandomNodes(16, nil)
		}
	})
}

// Additional comprehensive tests for maximum coverage

func TestNodeListGetShard(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	id1 := NodeID(uuid.New())
	id2 := NodeID(uuid.New())

	shard1 := nl.getShard(id1)
	shard2 := nl.getShard(id2)

	if shard1 == nil || shard2 == nil {
		t.Fatal("getShard should not return nil")
	}

	// Same ID should return same shard
	if nl.getShard(id1) != shard1 {
		t.Fatal("Same ID should return same shard")
	}
}

func TestNodeListAddOrUpdate(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	id := NodeID(uuid.New())
	n1 := newNode(id, "addr1")
	n1.state = NodeAlive

	// First add
	if !nl.addOrUpdate(n1) {
		t.Fatal("addOrUpdate should return true for new node")
	}

	// Update existing
	n2 := newNode(id, "addr2")
	n2.state = NodeSuspect
	if !nl.addOrUpdate(n2) {
		t.Fatal("addOrUpdate should return true for existing node")
	}

	// Verify state was updated
	node := nl.get(id)
	if node.state != NodeSuspect {
		t.Fatalf("Expected NodeSuspect, got %v", node.state)
	}
}

func TestNodeListRemove(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	id := NodeID(uuid.New())
	n := newNode(id, "addr")
	nl.addIfNotExists(n)

	// Remove node
	nl.remove(id)

	// Should not exist anymore
	if nl.get(id) != nil {
		t.Fatal("Node should be removed")
	}

	// Cannot remove local node
	nl.remove(c.localNode.ID)
	if nl.get(c.localNode.ID) == nil {
		t.Fatal("Local node should not be removable")
	}
}

func TestNodeListGetRandomNodesInStates(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	// Add nodes in different states
	aliveNodes := make([]NodeID, 5)
	for i := 0; i < 5; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		n.state = NodeAlive
		nl.addIfNotExists(n)
		aliveNodes[i] = id
	}

	suspectNodes := make([]NodeID, 3)
	for i := 0; i < 3; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		n.state = NodeSuspect
		nl.addIfNotExists(n)
		suspectNodes[i] = id
	}

	// Get random alive nodes
	nodes := nl.getRandomNodesInStates(3, []NodeState{NodeAlive}, nil)
	if len(nodes) > 3 {
		t.Fatalf("Expected at most 3 nodes, got %d", len(nodes))
	}

	for _, node := range nodes {
		if node.state != NodeAlive {
			t.Fatalf("Expected NodeAlive, got %v", node.state)
		}
	}

	// Test with exclusions
	excludeIDs := []NodeID{aliveNodes[0], aliveNodes[1]}
	nodes = nl.getRandomNodesInStates(10, []NodeState{NodeAlive}, excludeIDs)

	for _, node := range nodes {
		for _, excludeID := range excludeIDs {
			if node.ID == excludeID {
				t.Fatal("Excluded node should not be returned")
			}
		}
	}

	// Test empty states
	nodes = nl.getRandomNodesInStates(5, []NodeState{}, nil)
	if len(nodes) != 0 {
		t.Fatal("Empty states should return empty slice")
	}

	// Test k <= 0
	nodes = nl.getRandomNodesInStates(0, []NodeState{NodeAlive}, nil)
	if len(nodes) != 0 {
		t.Fatal("k=0 should return empty slice")
	}
}

func TestNodeListForAllInStates(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	// Add nodes in different states
	for i := 0; i < 3; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		n.state = NodeAlive
		nl.addIfNotExists(n)
	}

	for i := 0; i < 2; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		n.state = NodeSuspect
		nl.addIfNotExists(n)
	}

	// Count alive nodes
	count := 0
	nl.forAllInStates([]NodeState{NodeAlive}, func(node *Node) bool {
		count++
		return true
	})

	if count != nl.getAliveCount() {
		t.Fatalf("Expected %d alive nodes, counted %d", nl.getAliveCount(), count)
	}

	// Test early termination
	count = 0
	nl.forAllInStates([]NodeState{NodeAlive}, func(node *Node) bool {
		count++
		return count < 2 // Stop after 2
	})

	if count != 2 {
		t.Fatalf("Expected early termination at 2, got %d", count)
	}
}

func TestNodeListGetAllInStates(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	// Add nodes in different states
	for i := 0; i < 3; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		n.state = NodeAlive
		nl.addIfNotExists(n)
	}

	for i := 0; i < 2; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		n.state = NodeDead
		nl.addIfNotExists(n)
	}

	// Get all alive nodes
	aliveNodes := nl.getAllInStates([]NodeState{NodeAlive})
	if len(aliveNodes) != nl.getAliveCount() {
		t.Fatalf("Expected %d alive nodes, got %d", nl.getAliveCount(), len(aliveNodes))
	}

	// Get all dead nodes
	deadNodes := nl.getAllInStates([]NodeState{NodeDead})
	if len(deadNodes) != nl.getDeadCount() {
		t.Fatalf("Expected %d dead nodes, got %d", nl.getDeadCount(), len(deadNodes))
	}

	// Get multiple states
	allNodes := nl.getAllInStates([]NodeState{NodeAlive, NodeDead})
	expected := nl.getAliveCount() + nl.getDeadCount()
	if len(allNodes) != expected {
		t.Fatalf("Expected %d nodes, got %d", expected, len(allNodes))
	}
}

func TestNodeListGetAll(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	// Add nodes in different states
	for i := 0; i < 2; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		n.state = NodeAlive
		nl.addIfNotExists(n)
	}

	for i := 0; i < 1; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		n.state = NodeSuspect
		nl.addIfNotExists(n)
	}

	allNodes := nl.getAll()
	expected := nl.getAliveCount() + nl.getSuspectCount() + nl.getLeavingCount() + nl.getDeadCount()
	if len(allNodes) != expected {
		t.Fatalf("Expected %d total nodes, got %d", expected, len(allNodes))
	}
}

func TestNodeListEventHandlers(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	var stateChanges []NodeState
	var metadataChanges []*Node
	var mu sync.Mutex

	// Register handlers
	stateID := nl.OnNodeStateChange(func(node *Node, prevState NodeState) {
		mu.Lock()
		stateChanges = append(stateChanges, prevState)
		mu.Unlock()
	})

	metaID := nl.OnNodeMetadataChange(func(node *Node) {
		mu.Lock()
		metadataChanges = append(metadataChanges, node)
		mu.Unlock()
	})

	// Add node to trigger state change
	id := NodeID(uuid.New())
	n := newNode(id, "addr")
	nl.addIfNotExists(n)

	// Update state to trigger handler
	nl.updateState(id, NodeSuspect, nil)

	// Trigger metadata change
	nl.notifyMetadataChanged(n)

	// Wait for async handlers
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if len(stateChanges) == 0 {
		t.Fatal("State change handler not called")
	}
	if len(metadataChanges) == 0 {
		t.Fatal("Metadata change handler not called")
	}
	mu.Unlock()

	// Remove handlers
	if !nl.RemoveStateChangeHandler(stateID) {
		t.Fatal("Failed to remove state change handler")
	}
	if !nl.RemoveMetadataChangeHandler(metaID) {
		t.Fatal("Failed to remove metadata change handler")
	}

	// Remove non-existent handlers
	fakeID := HandlerID(uuid.New())
	if nl.RemoveStateChangeHandler(fakeID) {
		t.Fatal("Should not remove non-existent handler")
	}
	if nl.RemoveMetadataChangeHandler(fakeID) {
		t.Fatal("Should not remove non-existent handler")
	}
}

func TestNodeListStateCache(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	// Add nodes
	for i := 0; i < 5; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		n.state = NodeAlive
		nl.addIfNotExists(n)
	}

	// First call should populate cache
	nodes1 := nl.getCachedNodesInStates([]NodeState{NodeAlive})

	// Second call should use cache
	nodes2 := nl.getCachedNodesInStates([]NodeState{NodeAlive})

	if len(nodes1) != len(nodes2) {
		t.Fatal("Cached results should be consistent")
	}

	// Modify state to invalidate cache
	if len(nodes1) > 0 {
		nl.updateState(nodes1[0].ID, NodeSuspect, nil)
	}

	// Cache should be invalidated
	nodes3 := nl.getCachedNodesInStates([]NodeState{NodeAlive})
	if len(nodes3) >= len(nodes1) {
		t.Fatal("Cache should be invalidated after state change")
	}
}

func TestNodeListStateSetToKey(t *testing.T) {
	// Test empty states
	key := stateSetToKey([]NodeState{})
	if key != "" {
		t.Fatal("Empty states should return empty key")
	}

	// Test single state
	key = stateSetToKey([]NodeState{NodeAlive})
	if key == "" {
		t.Fatal("Single state should return non-empty key")
	}

	// Test multiple states - order should not matter
	key1 := stateSetToKey([]NodeState{NodeAlive, NodeSuspect})
	key2 := stateSetToKey([]NodeState{NodeSuspect, NodeAlive})
	if key1 != key2 {
		t.Fatal("State order should not affect key")
	}

	// Test different states should produce different keys
	key3 := stateSetToKey([]NodeState{NodeAlive})
	key4 := stateSetToKey([]NodeState{NodeSuspect})
	if key3 == key4 {
		t.Fatal("Different states should produce different keys")
	}
}

func TestNodeListUpdateCountersForStateChange(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	initialAlive := nl.getAliveCount()
	initialSuspect := nl.getSuspectCount()

	// Simulate state change from Unknown to Alive
	nl.updateCountersForStateChange(NodeUnknown, NodeAlive)

	if nl.getAliveCount() != initialAlive+1 {
		t.Fatal("Alive count should increase")
	}

	// Simulate state change from Alive to Suspect
	nl.updateCountersForStateChange(NodeAlive, NodeSuspect)

	if nl.getAliveCount() != initialAlive {
		t.Fatal("Alive count should decrease")
	}
	if nl.getSuspectCount() != initialSuspect+1 {
		t.Fatal("Suspect count should increase")
	}
}

// Additional benchmarks for comprehensive coverage

func BenchmarkNodeListGetShard(b *testing.B) {
	c := newTestCluster(&testing.T{})
	nl := c.nodes
	id := NodeID(uuid.New())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.getShard(id)
	}
}

func BenchmarkNodeListGet(b *testing.B) {
	c, ids := populateCluster(b, 10000)
	nl := c.nodes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.get(ids[i%len(ids)])
	}
}

func BenchmarkNodeListGetAllInStates(b *testing.B) {
	c, _ := populateCluster(b, 10000)
	nl := c.nodes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.getAllInStates([]NodeState{NodeAlive, NodeSuspect})
	}
}

func BenchmarkNodeListForAllInStates(b *testing.B) {
	c, _ := populateCluster(b, 10000)
	nl := c.nodes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.forAllInStates([]NodeState{NodeAlive}, func(*Node) bool {
			return true
		})
	}
}

func BenchmarkNodeListStateCounters(b *testing.B) {
	c, _ := populateCluster(b, 1000)
	nl := c.nodes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.getAliveCount()
		nl.getSuspectCount()
		nl.getLeavingCount()
		nl.getDeadCount()
	}
}

func BenchmarkNodeListConcurrentStateUpdates(b *testing.B) {
	c, ids := populateCluster(b, 1000)
	nl := c.nodes

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := ids[rand.Intn(len(ids))]
			state := []NodeState{NodeAlive, NodeSuspect, NodeDead}[rand.Intn(3)]
			nl.updateState(id, state, nil)
		}
	})
}
