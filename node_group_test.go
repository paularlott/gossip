package gossip

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/codec"
)

func newNodeGroupTestCluster(t *testing.T) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SocketTransportEnabled = false
	cfg.WebsocketProvider = nil
	cfg.MsgCodec = codec.NewJsonCodec()
	cfg.GossipInterval = 100 * time.Millisecond
	cfg.StateSyncInterval = 200 * time.Millisecond

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	c.Start()
	return c
}

func TestNodeGroup_MembershipByMetadata(t *testing.T) {
	c := newNodeGroupTestCluster(t)
	defer c.Stop()

	// Create nodes
	n1 := newNode(NodeID{1}, "")
	n2 := newNode(NodeID{2}, "")
	n3 := newNode(NodeID{3}, "")
	c.nodes.addOrUpdate(n1)
	c.nodes.addOrUpdate(n2)
	c.nodes.addOrUpdate(n3)

	n1.metadata.SetString("zone", "us-east")
	n2.metadata.SetString("zone", "us-west")
	n3.metadata.SetString("zone", "us-east")

	ng := NewNodeGroup(c, map[string]string{"zone": "us-east"}, nil)
	defer ng.Close()

	// Allow handler registration to process initial nodes
	time.Sleep(20 * time.Millisecond)

	if ng.Count() != 2 {
		t.Fatalf("expected 2 nodes in group, got %d", ng.Count())
	}
	if !ng.Contains(n1.ID) || !ng.Contains(n3.ID) {
		t.Fatalf("expected group to contain n1 and n3")
	}

	// Change metadata to remove n3
	n3.metadata.SetString("zone", "us-west")
	c.notifyMetadataChanged(n3)
	time.Sleep(20 * time.Millisecond)

	if ng.Count() != 1 || !ng.Contains(n1.ID) || ng.Contains(n3.ID) {
		t.Fatalf("expected only n1 to remain in group")
	}
}
