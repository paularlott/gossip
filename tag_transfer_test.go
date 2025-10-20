package gossip

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/logger"
)

// TestTagTransferOnJoin verifies that node tags are properly transferred when nodes join the cluster
func TestTagTransferOnJoin(t *testing.T) {
	// Create first cluster with tags
	config1 := DefaultConfig()
	config1.BindAddr = "127.0.0.1:18001"
	config1.AdvertiseAddr = "127.0.0.1:18001"
	config1.Tags = []string{"taga", "tagb"}
	config1.Transport = NewSocketTransport(config1)
	config1.MsgCodec = codec.NewShamatonMsgpackCodec()
	config1.Logger = logger.NewNullLogger()

	cluster1, err := NewCluster(config1)
	if err != nil {
		t.Fatalf("Failed to create cluster1: %v", err)
	}
	cluster1.Start()
	defer cluster1.Stop()

	// Verify cluster1 has the expected tags
	if len(cluster1.LocalNode().GetTags()) != 2 {
		t.Errorf("Cluster1 should have 2 tags, got %d", len(cluster1.LocalNode().GetTags()))
	}
	if !cluster1.LocalNode().HasTag("taga") || !cluster1.LocalNode().HasTag("tagb") {
		t.Error("Cluster1 should have tags 'taga' and 'tagb'")
	}

	// Create second cluster with different tags
	config2 := DefaultConfig()
	config2.BindAddr = "127.0.0.1:18002"
	config2.AdvertiseAddr = "127.0.0.1:18002"
	config2.Tags = []string{"tagb", "tagc"}
	config2.Transport = NewSocketTransport(config2)
	config2.MsgCodec = codec.NewShamatonMsgpackCodec()
	config2.Logger = logger.NewNullLogger()

	cluster2, err := NewCluster(config2)
	if err != nil {
		t.Fatalf("Failed to create cluster2: %v", err)
	}
	cluster2.Start()
	defer cluster2.Stop()

	// Verify cluster2 has the expected tags
	if len(cluster2.LocalNode().GetTags()) != 2 {
		t.Errorf("Cluster2 should have 2 tags, got %d", len(cluster2.LocalNode().GetTags()))
	}
	if !cluster2.LocalNode().HasTag("tagb") || !cluster2.LocalNode().HasTag("tagc") {
		t.Error("Cluster2 should have tags 'tagb' and 'tagc'")
	}

	// Join cluster2 to cluster1
	err = cluster2.Join([]string{"127.0.0.1:18001"})
	if err != nil {
		t.Fatalf("Failed to join clusters: %v", err)
	}

	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)

	// Verify cluster1 knows about cluster2 and has its tags
	node2InCluster1 := cluster1.GetNode(cluster2.LocalNode().ID)
	if node2InCluster1 == nil {
		t.Fatal("Cluster1 should know about cluster2")
	}

	if len(node2InCluster1.GetTags()) != 2 {
		t.Errorf("Node2 in cluster1 should have 2 tags, got %d: %v", len(node2InCluster1.GetTags()), node2InCluster1.GetTags())
	}

	if !node2InCluster1.HasTag("tagb") {
		t.Error("Node2 in cluster1 should have tag 'tagb'")
	}

	if !node2InCluster1.HasTag("tagc") {
		t.Error("Node2 in cluster1 should have tag 'tagc'")
	}

	// Verify cluster2 knows about cluster1 and has its tags
	node1InCluster2 := cluster2.GetNode(cluster1.LocalNode().ID)
	if node1InCluster2 == nil {
		t.Fatal("Cluster2 should know about cluster1")
	}

	if len(node1InCluster2.GetTags()) != 2 {
		t.Errorf("Node1 in cluster2 should have 2 tags, got %d: %v", len(node1InCluster2.GetTags()), node1InCluster2.GetTags())
	}

	if !node1InCluster2.HasTag("taga") {
		t.Error("Node1 in cluster2 should have tag 'taga'")
	}

	if !node1InCluster2.HasTag("tagb") {
		t.Error("Node1 in cluster2 should have tag 'tagb'")
	}

	// Test GetNodesByTag on both clusters
	tagaNodes1 := cluster1.GetNodesByTag("taga")
	if len(tagaNodes1) != 1 {
		t.Errorf("Cluster1 should see 1 node with tag 'taga', got %d", len(tagaNodes1))
	}

	tagbNodes1 := cluster1.GetNodesByTag("tagb")
	if len(tagbNodes1) != 2 {
		t.Errorf("Cluster1 should see 2 nodes with tag 'tagb', got %d", len(tagbNodes1))
	}

	tagcNodes1 := cluster1.GetNodesByTag("tagc")
	if len(tagcNodes1) != 1 {
		t.Errorf("Cluster1 should see 1 node with tag 'tagc', got %d", len(tagcNodes1))
	}

	tagcNodes2 := cluster2.GetNodesByTag("tagc")
	if len(tagcNodes2) != 1 {
		t.Errorf("Cluster2 should see 1 node with tag 'tagc', got %d", len(tagcNodes2))
	}
}
