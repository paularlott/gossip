package gossip

import (
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/logger"
)

// TestNodeHasTag tests the Node.HasTag method
func TestNodeHasTag(t *testing.T) {
	node := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8000", []string{"taga", "tagb", "tagc"})

	tests := []struct {
		tag      string
		expected bool
	}{
		{"taga", true},
		{"tagb", true},
		{"tagc", true},
		{"tagd", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			result := node.HasTag(tt.tag)
			if result != tt.expected {
				t.Errorf("HasTag(%q) = %v, expected %v", tt.tag, result, tt.expected)
			}
		})
	}
}

// TestNodeHasTagEmpty tests HasTag with empty tags
func TestNodeHasTagEmpty(t *testing.T) {
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8000")

	if node.HasTag("taga") {
		t.Error("Node with no tags should not match any tag")
	}
}

// TestPacketTagField tests that packet tag field is properly set and cleared
func TestPacketTagField(t *testing.T) {
	packet := NewPacket()
	defer packet.Release()

	// Initially nil
	if packet.Tag != nil {
		t.Error("New packet should have nil tag")
	}

	// Set tag
	tag := "test-tag"
	packet.Tag = &tag
	if packet.Tag == nil || *packet.Tag != "test-tag" {
		t.Error("Packet tag should be set correctly")
	}

	// Release and get new packet
	packet.Release()
	packet2 := NewPacket()
	defer packet2.Release()

	// Should be nil after release and reuse
	if packet2.Tag != nil {
		t.Error("Reused packet should have nil tag")
	}
}

// TestConfigTags tests that tags are properly set from config
func TestConfigTags(t *testing.T) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Tags = []string{"taga", "tagb"}
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Verify tags are set on local node
	if len(cluster.LocalNode().GetTags()) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(cluster.LocalNode().GetTags()))
	}

	if !cluster.LocalNode().HasTag("taga") {
		t.Error("Local node should have tag 'taga'")
	}

	if !cluster.LocalNode().HasTag("tagb") {
		t.Error("Local node should have tag 'tagb'")
	}

	if cluster.LocalNode().HasTag("tagc") {
		t.Error("Local node should not have tag 'tagc'")
	}
}

// TestSendTagged tests sending tagged messages
func TestSendTagged(t *testing.T) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Tags = []string{"taga"}
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Test SendTagged
	err = cluster.SendTagged("taga", UserMsg, "test message")
	if err != nil {
		t.Errorf("Failed to send tagged message: %v", err)
	}

	// Test SendTaggedReliable
	err = cluster.SendTaggedReliable("taga", UserMsg, "test message")
	if err != nil {
		t.Errorf("Failed to send tagged reliable message: %v", err)
	}
}

// TestGetNodesByTag tests retrieving nodes by tag
func TestGetNodesByTag(t *testing.T) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Tags = []string{"taga", "tagb"}
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add some test nodes
	node1 := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8001", []string{"taga", "tagb"})
	cluster.nodes.addOrUpdate(node1)

	node2 := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8002", []string{"tagb", "tagc"})
	cluster.nodes.addOrUpdate(node2)

	node3 := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8003", []string{"tagc"})
	cluster.nodes.addOrUpdate(node3)

	// Test getting nodes by taga (should include local node and node1)
	nodesWithTagA := cluster.GetNodesByTag("taga")
	if len(nodesWithTagA) != 2 {
		t.Errorf("Expected 2 nodes with tag 'taga', got %d", len(nodesWithTagA))
	}

	// Test getting nodes by tagb (should include local node, node1, and node2)
	nodesWithTagB := cluster.GetNodesByTag("tagb")
	if len(nodesWithTagB) != 3 {
		t.Errorf("Expected 3 nodes with tag 'tagb', got %d", len(nodesWithTagB))
	}

	// Test getting nodes by tagc (should include node2 and node3)
	nodesWithTagC := cluster.GetNodesByTag("tagc")
	if len(nodesWithTagC) != 2 {
		t.Errorf("Expected 2 nodes with tag 'tagc', got %d", len(nodesWithTagC))
	}

	// Test getting nodes by non-existent tag
	nodesWithTagD := cluster.GetNodesByTag("tagd")
	if len(nodesWithTagD) != 0 {
		t.Errorf("Expected 0 nodes with tag 'tagd', got %d", len(nodesWithTagD))
	}
}

// TestTaggedMessageRouting tests tag filtering in message handling
func TestTaggedMessageRouting(t *testing.T) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Tags = []string{"taga", "tagb"}
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add test nodes with different tags
	// Node1: has taga, tagb
	node1 := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8001", []string{"taga", "tagb"})
	cluster.nodes.addOrUpdate(node1)

	// Node2: has taga, tagb, tagc
	node2 := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8002", []string{"taga", "tagb", "tagc"})
	cluster.nodes.addOrUpdate(node2)

	// Node3: has tagc only
	node3 := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8003", []string{"tagc"})
	cluster.nodes.addOrUpdate(node3)

	// Test: Create a packet with tag 'tagc'
	tagc := "tagc"
	packet := NewPacket()
	packet.Tag = &tagc
	packet.SenderID = cluster.LocalNode().ID
	packet.MessageType = UserMsg
	packet.MessageID = MessageID(1)
	packet.TTL = 3
	packet.codec = config.MsgCodec
	defer packet.Release()

	// Get nodes that should receive 'tagc' messages
	// Should only get node2 and node3 (not local node or node1)
	nodesWithTagC := cluster.GetNodesByTag("tagc")

	expectedCount := 2 // node2 and node3
	if len(nodesWithTagC) != expectedCount {
		t.Errorf("Expected %d nodes with tag 'tagc', got %d", expectedCount, len(nodesWithTagC))
	}

	// Verify specific nodes
	hasNode2 := false
	hasNode3 := false
	for _, n := range nodesWithTagC {
		if n.ID == node2.ID {
			hasNode2 = true
		}
		if n.ID == node3.ID {
			hasNode3 = true
		}
	}

	if !hasNode2 {
		t.Error("Node2 should be in the list (has tagc)")
	}
	if !hasNode3 {
		t.Error("Node3 should be in the list (has tagc)")
	}

	// Test: Get nodes with 'taga'
	// Should get local node, node1, and node2
	nodesWithTagA := cluster.GetNodesByTag("taga")
	expectedCountA := 3
	if len(nodesWithTagA) != expectedCountA {
		t.Errorf("Expected %d nodes with tag 'taga', got %d", expectedCountA, len(nodesWithTagA))
	}
}

// TestTaggedMessageWithNoMatchingNodes tests behavior when no nodes have the tag
func TestTaggedMessageWithNoMatchingNodes(t *testing.T) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Tags = []string{"taga"}
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Send message with tag that no other node has
	// Should not fail, but will have no recipients
	err = cluster.SendTagged("nonexistent-tag", UserMsg, "test message")
	if err != nil {
		t.Errorf("SendTagged should not fail when no nodes have the tag: %v", err)
	}
}

// TestUntaggedMessageBackwardCompatibility tests that untagged messages work as before
func TestUntaggedMessageBackwardCompatibility(t *testing.T) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Tags = []string{"taga"}
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Regular Send should work without tags
	err = cluster.Send(UserMsg, "test message")
	if err != nil {
		t.Errorf("Regular Send should work: %v", err)
	}

	// Regular SendReliable should work without tags
	err = cluster.SendReliable(UserMsg, "test message")
	if err != nil {
		t.Errorf("Regular SendReliable should work: %v", err)
	}
}
