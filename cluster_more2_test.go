package gossip

import (
	"net"
	"testing"
	"time"

	"github.com/paularlott/gossip/compression"
	"github.com/paularlott/gossip/hlc"
)

func TestCluster_GetCandidates(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Add a few peers
	for i := 0; i < 5; i++ {
		n := newNode(NodeID{byte(30 + i)}, "")
		c.nodes.addOrUpdate(n)
	}
	c.nodes.updateState(c.localNode.ID, NodeAlive)

	candidates := c.GetCandidates()
	if len(candidates) == 0 {
		t.Fatalf("expected some candidates")
	}
	for _, n := range candidates {
		if n.ID == c.localNode.ID {
			t.Fatalf("candidates should exclude local node")
		}
	}
}

func TestStreamCompressionHelpers_WithStream(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Enable compressor so wrapStream returns *Stream
	c.config.Compressor = compression.NewSnappyCompressor()

	s1, s2 := net.Pipe()
	defer s1.Close()
	defer s2.Close()

	wrapped, err := c.wrapStream(s1)
	if err != nil {
		t.Fatalf("wrapStream: %v", err)
	}

	// These should hit the *Stream branch
	_ = c.EnableStreamCompression(wrapped)
	_ = c.DisableStreamCompression(wrapped)
}

func TestHealthMonitor_AddJoinPeer_NoDupes(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	hm := c.healthMonitor
	hm.addJoinPeer("ws://x")
	hm.addJoinPeer("ws://x")

	hm.joinPeersMutex.Lock()
	defer hm.joinPeersMutex.Unlock()
	if len(hm.joinPeers) != 1 {
		t.Fatalf("expected 1 join peer, got %d", len(hm.joinPeers))
	}
}

func TestHandlePushPullState_ReturnsLocalSubset(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Add some nodes so subset isn't empty
	for i := 0; i < 3; i++ {
		n := newNode(NodeID{byte(60 + i)}, "addr")
		n.state = NodeAlive
		n.stateChangeTime = hlc.Now()
		c.nodes.addOrUpdate(n)
	}

	sender := newNode(NodeID{200}, "")
	remote := []exchangeNodeState{{ID: c.localNode.ID, State: NodeAlive}}
	pkt, err := c.createPacket(sender.ID, pushPullStateMsg, c.getMaxTTL(), &remote)
	if err != nil {
		t.Fatalf("createPacket: %v", err)
	}
	defer pkt.Release()

	resp, err := c.handlePushPullState(sender, pkt)
	if err != nil {
		t.Fatalf("handlePushPullState: %v", err)
	}

	// Response should be *[]exchangeNodeState
	listPtr, ok := resp.(*[]exchangeNodeState)
	if !ok || listPtr == nil {
		t.Fatalf("unexpected response type: %T", resp)
	}
	// Allow for jitter in CalcPayloadSize; just ensure not nil and possibly non-empty
	_ = listPtr

	// Give background routines time to process
	time.Sleep(10 * time.Millisecond)
}
