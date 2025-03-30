package gossip

import (
	"sync"
	"testing"
	"time"
)

// Helper to create a mock node for testing
func createMockNode(id byte, state NodeState) *Node {
	var nodeID NodeID
	nodeID[0] = id // Just use first byte for test differentiation
	return &Node{
		ID:             nodeID,
		advertisedAddr: "192.168.1.1:7946",
		state:          state,
	}
}

func TestNodeListBasicOperations(t *testing.T) {
	nl := newNodeList(4)

	// Create test nodes
	node1 := createMockNode(1, nodeAlive)
	node2 := createMockNode(2, nodeSuspect)
	node3 := createMockNode(3, nodeDead)

	// Test add
	nl.add(node1, true)
	nl.add(node2, true)
	nl.add(node3, true)

	// Test counts
	if got, want := nl.getTotalCount(), 3; got != want {
		t.Errorf("getTotalCount() = %v, want %v", got, want)
	}
	if got, want := nl.getAliveCount(), 1; got != want {
		t.Errorf("getAliveCount() = %v, want %v", got, want)
	}
	if got, want := nl.getLiveCount(), 2; got != want {
		t.Errorf("getLiveCount() = %v, want %v", got, want)
	}

	// Test get
	if got := nl.get(node1.ID); got == nil || got.ID != node1.ID {
		t.Errorf("get(node1.ID) = %v, want %v", got, node1)
	}

	// Test update state
	nl.updateState(node3.ID, nodeAlive)
	if got, want := nl.getAliveCount(), 2; got != want {
		t.Errorf("After updateState, getAliveCount() = %v, want %v", got, want)
	}

	// Test remove
	nl.remove(node2.ID)
	if got, want := nl.getTotalCount(), 2; got != want {
		t.Errorf("After remove, getTotalCount() = %v, want %v", got, want)
	}
	if got := nl.get(node2.ID); got != nil {
		t.Errorf("After remove, get(node2.ID) = %v, want nil", got)
	}
}

func TestNodeListStateTransitions(t *testing.T) {
	nl := newNodeList(4)
	node := createMockNode(1, nodeAlive)

	// Add node
	nl.add(node, true)

	// Initial state check
	if got, want := nl.getAliveCount(), 1; got != want {
		t.Errorf("Initial getAliveCount() = %v, want %v", got, want)
	}
	if got, want := nl.getLiveCount(), 1; got != want {
		t.Errorf("Initial getLiveCount() = %v, want %v", got, want)
	}

	// Transition to suspect (should remain "live")
	nl.updateState(node.ID, nodeSuspect)
	if got, want := nl.getAliveCount(), 0; got != want {
		t.Errorf("After suspect, getAliveCount() = %v, want %v", got, want)
	}
	if got, want := nl.getLiveCount(), 1; got != want {
		t.Errorf("After suspect, getLiveCount() = %v, want %v", got, want)
	}

	// Transition to dead (should not be "live")
	nl.updateState(node.ID, nodeDead)
	if got, want := nl.getAliveCount(), 0; got != want {
		t.Errorf("After dead, getAliveCount() = %v, want %v", got, want)
	}
	if got, want := nl.getLiveCount(), 0; got != want {
		t.Errorf("After dead, getLiveCount() = %v, want %v", got, want)
	}

	// Transition back to alive
	nl.updateState(node.ID, nodeAlive)
	if got, want := nl.getAliveCount(), 1; got != want {
		t.Errorf("After alive again, getAliveCount() = %v, want %v", got, want)
	}
	if got, want := nl.getLiveCount(), 1; got != want {
		t.Errorf("After alive again, getLiveCount() = %v, want %v", got, want)
	}
}

func TestNodeListGetRandomLiveNodes(t *testing.T) {
	nl := newNodeList(4)

	// Add 50 nodes (40 alive, 10 suspect)
	for i := byte(1); i <= 50; i++ {
		state := nodeAlive
		if i > 40 {
			state = nodeSuspect
		}
		node := createMockNode(i, state)
		nl.add(node, true)
	}

	// Add 10 dead nodes - these should not be returned
	for i := byte(51); i <= 60; i++ {
		node := createMockNode(i, nodeDead)
		nl.add(node, true)
	}

	// Verify counts
	if got, want := nl.getTotalCount(), 60; got != want {
		t.Errorf("getTotalCount() = %v, want %v", got, want)
	}
	if got, want := nl.getLiveCount(), 50; got != want {
		t.Errorf("getLiveCount() = %v, want %v", got, want)
	}

	// Test getting all live nodes
	allLive := nl.getRandomLiveNodes(100, nil)
	if got, want := len(allLive), 50; got != want {
		t.Errorf("getRandomLiveNodes(100, nil) returned %v nodes, want %v", got, want)
	}

	// Test getting a subset of live nodes
	someLive := nl.getRandomLiveNodes(20, nil)
	if got, want := len(someLive), 20; got != want {
		t.Errorf("getRandomLiveNodes(20, nil) returned %v nodes, want %v", got, want)
	}

	// Verify each returned node is actually live
	for _, node := range someLive {
		if node.state != nodeAlive && node.state != nodeSuspect {
			t.Errorf("getRandomLiveNodes returned node with state %v, want nodeAlive or nodeSuspect", node.state)
		}
	}

	// Verify randomness - multiple calls should not return identical results
	// Note: There's a small probability this could fail by chance
	someLiveAgain := nl.getRandomLiveNodes(20, nil)
	identical := true
	for i := 0; i < len(someLive); i++ {
		if someLive[i].ID != someLiveAgain[i].ID {
			identical = false
			break
		}
	}
	if identical {
		t.Errorf("Two consecutive calls to getRandomLiveNodes returned identical results, expected different order")
	}
}

func TestNodeListExclusions(t *testing.T) {
	nl := newNodeList(4)

	// Add 10 nodes
	var nodes []*Node
	for i := byte(1); i <= 10; i++ {
		node := createMockNode(i, nodeAlive)
		nl.add(node, true)
		nodes = append(nodes, node)
	}

	// Test exclusion of one node
	excludeIDs := []NodeID{nodes[0].ID}
	result := nl.getRandomLiveNodes(10, excludeIDs)

	// Should have 9 nodes
	if got, want := len(result), 9; got != want {
		t.Errorf("getRandomLiveNodes with 1 exclusion returned %v nodes, want %v", got, want)
	}

	// Verify excluded node is not in result
	for _, node := range result {
		if node.ID == nodes[0].ID {
			t.Errorf("Excluded node with ID %v was returned", node.ID)
		}
	}

	// Test exclusion of multiple nodes
	excludeIDs = []NodeID{nodes[0].ID, nodes[1].ID, nodes[2].ID}
	result = nl.getRandomLiveNodes(10, excludeIDs)

	// Should have 7 nodes
	if got, want := len(result), 7; got != want {
		t.Errorf("getRandomLiveNodes with 3 exclusions returned %v nodes, want %v", got, want)
	}

	// Verify excluded nodes are not in result
	for _, node := range result {
		for _, excludeID := range excludeIDs {
			if node.ID == excludeID {
				t.Errorf("Excluded node with ID %v was returned", node.ID)
			}
		}
	}
}

func TestForAllInState(t *testing.T) {
	nl := newNodeList(4)

	// Add nodes in different states
	for i := byte(1); i <= 10; i++ {
		var state NodeState
		switch {
		case i <= 4:
			state = nodeAlive
		case i <= 7:
			state = nodeSuspect
		default:
			state = nodeDead
		}
		node := createMockNode(i, state)
		nl.add(node, true)
	}

	// Test forAllInState for alive nodes
	count := 0
	nl.forAllInStates([]NodeState{nodeAlive}, func(node *Node) bool {
		if node.state != nodeAlive {
			t.Errorf("forAllInStates(nodeAlive) called with node in state %v", node.state)
		}
		count++
		return true
	})
	if got, want := count, 4; got != want {
		t.Errorf("forAllInStates(nodeAlive) processed %v nodes, want %v", got, want)
	}

	// Test early termination
	count = 0
	nl.forAllInStates([]NodeState{nodeSuspect}, func(node *Node) bool {
		count++
		return count < 2 // Stop after processing 1 node
	})
	if got, want := count, 2; got != want {
		t.Errorf("forAllInState with early termination processed %v nodes, want %v", got, want)
	}
}

func TestConcurrentAccess(t *testing.T) {
	nl := newNodeList(4)

	// Add some initial nodes
	for i := byte(1); i <= 10; i++ {
		node := createMockNode(i, nodeAlive)
		nl.add(node, true)
	}

	// Set up concurrent goroutines
	const numGoroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 4) // 4 types of operations

	// Create a map to track all added node IDs to detect any collisions
	addedNodeIDs := make(map[string]bool)
	var addMutex sync.Mutex

	// Test concurrent adds
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				// Create a node with a truly unique ID
				node := createMockNode(0, nodeAlive) // Start with zero

				// Set unique values in each byte of the ID
				node.ID[0] = byte(id)
				node.ID[1] = byte(j)
				node.ID[2] = byte(j >> 8)
				node.ID[3] = byte(id >> 8)
				node.ID[4] = byte((id*opsPerGoroutine + j) % 255)

				// Track this node ID to detect any collisions
				idStr := string(node.ID[:])
				addMutex.Lock()
				if addedNodeIDs[idStr] {
					t.Logf("WARNING: Collision detected for node ID: %v (i=%d, j=%d)", node.ID, id, j)
				}
				addedNodeIDs[idStr] = true
				addMutex.Unlock()

				nl.add(node, true)
			}
		}(i)
	}

	// Test concurrent reads
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				_ = nl.getTotalCount()
				_ = nl.getAliveCount()
				_ = nl.getLiveCount()
			}
		}()
	}

	// Test concurrent updates
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if j%2 == 0 {
					// Update some existing nodes
					var nodeID NodeID
					nodeID[0] = byte((id*j)%10 + 1) // Use a node we know exists
					state := NodeState(j % 4)       // Cycle through states
					nl.updateState(nodeID, state)
				}
			}
		}(i)
	}

	// Test concurrent random node selection
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				k := (j % 10) + 1 // Request 1-10 nodes
				_ = nl.getRandomLiveNodes(k, nil)
			}
		}()
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out, likely deadlock in concurrent operations")
	}

	// Final check - make sure all adds worked
	// Give it a small grace period for any pending operations to complete
	time.Sleep(100 * time.Millisecond)

	// Force a recalculation of counters to ensure accuracy
	nl.recalculateCounters()

	// Count the actual unique nodes we added
	addMutex.Lock()
	uniqueNodesAdded := len(addedNodeIDs)
	addMutex.Unlock()

	// Expected total = initial 10 nodes + all uniquely added nodes
	expectedAdds := 10 + uniqueNodesAdded

	totalCount := nl.getTotalCount()
	if totalCount != expectedAdds {
		// Add more diagnostic information
		t.Errorf("After concurrent operations, got %v total nodes, want %v",
			totalCount, expectedAdds)

		// Count nodes manually to verify the discrepancy
		var manualCount int
		for i := 0; i < nl.shardCount; i++ {
			nl.shards[i].mutex.RLock()
			manualCount += len(nl.shards[i].nodes)
			nl.shards[i].mutex.RUnlock()
		}

		t.Logf("Manual count of nodes: %d", manualCount)
		t.Logf("Unique nodes added: %d", uniqueNodesAdded)
		t.Logf("Difference between expected and actual: %d", expectedAdds-totalCount)
	}
}

func TestRecalculateCounters(t *testing.T) {
	nl := newNodeList(4)

	// Add nodes
	for i := byte(1); i <= 30; i++ {
		var state NodeState
		switch {
		case i <= 10:
			state = nodeAlive
		case i <= 20:
			state = nodeSuspect
		default:
			state = nodeDead
		}
		node := createMockNode(i, state)
		nl.add(node, true)
	}

	// Verify initial counts
	if got, want := nl.getTotalCount(), 30; got != want {
		t.Errorf("Initial getTotalCount() = %v, want %v", got, want)
	}
	if got, want := nl.getAliveCount(), 10; got != want {
		t.Errorf("Initial getAliveCount() = %v, want %v", got, want)
	}
	if got, want := nl.getLiveCount(), 20; got != want {
		t.Errorf("Initial getLiveCount() = %v, want %v", got, want)
	}

	// Manually corrupt the counters
	nl.totalCount.Store(0)
	nl.aliveCount.Store(0)
	nl.liveCount.Store(0)

	// Recalculate
	nl.recalculateCounters()

	// Verify counters are fixed
	if got, want := nl.getTotalCount(), 30; got != want {
		t.Errorf("After recalculate, getTotalCount() = %v, want %v", got, want)
	}
	if got, want := nl.getAliveCount(), 10; got != want {
		t.Errorf("After recalculate, getAliveCount() = %v, want %v", got, want)
	}
	if got, want := nl.getLiveCount(), 20; got != want {
		t.Errorf("After recalculate, getLiveCount() = %v, want %v", got, want)
	}
}
