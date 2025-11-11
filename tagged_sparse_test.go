package gossip

import (
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/logger"
)

// TestGetRandomNodesWithTagSparse tests the edge case where many nodes exist but only a few have the tag
// This verifies that the implementation doesn't use a k*multiplier approach which would fail
func TestGetRandomNodesWithTagSparse(t *testing.T) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Tags = []string{"rare"}
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create 1000 nodes, but only 10 have the "rare" tag
	nodesWithTag := 0
	for i := 0; i < 1000; i++ {
		var node *Node
		if i%100 == 0 {
			// Every 100th node gets the "rare" tag
			node = newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8000", []string{"rare"})
			nodesWithTag++
		} else {
			// Other nodes get "common" tag
			node = newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8000", []string{"common"})
		}
		node.observedState = NodeAlive
		cluster.nodes.addOrUpdate(node)
	}

	t.Logf("Created 1000 nodes, %d have the 'rare' tag", nodesWithTag+1) // +1 for local node

	// Request 8 random nodes with "rare" tag
	// The old k*3 approach would request 24 nodes and likely find 0 with the rare tag (24/1000 = 2.4% chance vs 1% distribution)
	// The new approach scans all nodes and filters, so it should find up to 8
	randomRareNodes := cluster.nodes.getRandomNodesWithTag(8, "rare", []NodeID{})

	t.Logf("Requested 8 nodes with 'rare' tag, got %d", len(randomRareNodes))

	// We should get 8 nodes (or 11 if all rare nodes are available)
	if len(randomRareNodes) < 8 {
		t.Errorf("Expected at least 8 nodes with 'rare' tag, got %d", len(randomRareNodes))
	}

	// Verify all returned nodes have the "rare" tag
	for _, node := range randomRareNodes {
		if !node.HasTag("rare") {
			t.Errorf("Node %s doesn't have 'rare' tag", node.ID.String()[:8])
		}
	}

	// Test requesting more nodes than available
	randomRareNodes = cluster.nodes.getRandomNodesWithTag(20, "rare", []NodeID{})
	t.Logf("Requested 20 nodes with 'rare' tag, got %d (expected 11: 10 created + 1 local)", len(randomRareNodes))

	// Should get all 11 nodes with the rare tag (10 created + 1 local node)
	if len(randomRareNodes) != nodesWithTag+1 {
		t.Errorf("Expected %d nodes with 'rare' tag, got %d", nodesWithTag+1, len(randomRareNodes))
	}

	// Test with common tag - should easily find 50 nodes
	randomCommonNodes := cluster.nodes.getRandomNodesWithTag(50, "common", []NodeID{})
	t.Logf("Requested 50 nodes with 'common' tag, got %d", len(randomCommonNodes))

	if len(randomCommonNodes) != 50 {
		t.Errorf("Expected 50 nodes with 'common' tag, got %d", len(randomCommonNodes))
	}

	// Verify all returned nodes have the "common" tag
	for _, node := range randomCommonNodes {
		if !node.HasTag("common") {
			t.Errorf("Node %s doesn't have 'common' tag", node.ID.String()[:8])
		}
	}
}

// TestGetRandomNodesWithTagExclude tests that exclusions work correctly with tag filtering
func TestGetRandomNodesWithTagExclude(t *testing.T) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Tags = []string{"test"}
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create 10 nodes with the "test" tag
	nodeIDs := make([]NodeID, 10)
	for i := 0; i < 10; i++ {
		nodeIDs[i] = NodeID(uuid.New())
		node := newNodeWithTags(nodeIDs[i], "127.0.0.1:8000", []string{"test"})
		node.observedState = NodeAlive
		cluster.nodes.addOrUpdate(node)
	}

	// Request 5 nodes excluding the first 3
	excludeIDs := nodeIDs[:3]
	randomNodes := cluster.nodes.getRandomNodesWithTag(5, "test", excludeIDs)

	t.Logf("Requested 5 nodes with 'test' tag excluding 3, got %d", len(randomNodes))

	// Should get 5 nodes (from the 7 remaining + 1 local = 8 total)
	if len(randomNodes) != 5 {
		t.Errorf("Expected 5 nodes, got %d", len(randomNodes))
	}

	// Verify none of the excluded nodes are in the result
	for _, node := range randomNodes {
		for _, excludeID := range excludeIDs {
			if node.ID == excludeID {
				t.Errorf("Excluded node %s was returned", node.ID.String()[:8])
			}
		}
	}
}
