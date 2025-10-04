package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
)

func TestClusterPublicAPIs(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Test LocalNode
	if cluster.LocalNode() == nil {
		t.Error("LocalNode() returned nil")
	}

	// Test LocalMetadata
	if cluster.LocalMetadata() == nil {
		t.Error("LocalMetadata() returned nil")
	}

	// Add some test nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	node3 := newNode(NodeID(uuid.New()), "127.0.0.1:8003")
	node3.observedState = NodeSuspect
	node4 := newNode(NodeID(uuid.New()), "127.0.0.1:8004")
	node4.observedState = NodeDead

	cluster.nodes.addOrUpdate(node1)
	cluster.nodes.addOrUpdate(node2)
	cluster.nodes.addOrUpdate(node3)
	cluster.nodes.addOrUpdate(node4)

	// Test Nodes
	nodes := cluster.Nodes()
	if len(nodes) < 4 {
		t.Errorf("Expected at least 4 nodes, got %d", len(nodes))
	}

	// Test AliveNodes
	aliveNodes := cluster.AliveNodes()
	if len(aliveNodes) < 2 {
		t.Errorf("Expected at least 2 alive nodes, got %d", len(aliveNodes))
	}

	// Test GetNode
	retrievedNode := cluster.GetNode(node1.ID)
	if retrievedNode == nil || retrievedNode.ID != node1.ID {
		t.Error("GetNode failed to retrieve node")
	}

	// Test GetNodeByIDString
	retrievedNode = cluster.GetNodeByIDString(node1.ID.String())
	if retrievedNode == nil || retrievedNode.ID != node1.ID {
		t.Error("GetNodeByIDString failed to retrieve node")
	}

	// Test invalid ID string
	if cluster.GetNodeByIDString("invalid-uuid") != nil {
		t.Error("Expected nil for invalid UUID string")
	}

	// Test NumNodes
	if cluster.NumNodes() < 4 {
		t.Errorf("Expected at least 4 nodes, got %d", cluster.NumNodes())
	}

	// Test NumAliveNodes
	if cluster.NumAliveNodes() < 2 {
		t.Errorf("Expected at least 2 alive nodes, got %d", cluster.NumAliveNodes())
	}

	// Test NumSuspectNodes
	if cluster.NumSuspectNodes() < 1 {
		t.Errorf("Expected at least 1 suspect node, got %d", cluster.NumSuspectNodes())
	}

	// Test NumDeadNodes
	if cluster.NumDeadNodes() < 1 {
		t.Errorf("Expected at least 1 dead node, got %d", cluster.NumDeadNodes())
	}

	// Test NodeIsLocal
	if !cluster.NodeIsLocal(cluster.localNode) {
		t.Error("NodeIsLocal failed for local node")
	}
	if cluster.NodeIsLocal(node1) {
		t.Error("NodeIsLocal returned true for remote node")
	}

	// Test GetCandidates
	candidates := cluster.GetCandidates()
	if len(candidates) == 0 {
		t.Error("GetCandidates returned empty list")
	}

	// Test Logger
	if cluster.Logger() == nil {
		t.Error("Logger() returned nil")
	}

	// Test NodesToIDs
	ids := cluster.NodesToIDs([]*Node{node1, node2})
	if len(ids) != 2 {
		t.Errorf("Expected 2 IDs, got %d", len(ids))
	}
}

func TestHandlerRegistration(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Test HandleFunc
	const testMsg MessageType = UserMsg + 1
	err = cluster.HandleFunc(testMsg, func(sender *Node, packet *Packet) error {
		return nil
	})
	if err != nil {
		t.Errorf("HandleFunc failed: %v", err)
	}

	// Test invalid message type
	err = cluster.HandleFunc(MessageType(1), func(sender *Node, packet *Packet) error {
		return nil
	})
	if err == nil {
		t.Error("Expected error for reserved message type")
	}

	// Test HandleFuncWithReply
	const testReplyMsg MessageType = UserMsg + 2
	err = cluster.HandleFuncWithReply(testReplyMsg, func(sender *Node, packet *Packet) (interface{}, error) {
		return map[string]string{"reply": "data"}, nil
	})
	if err != nil {
		t.Errorf("HandleFuncWithReply failed: %v", err)
	}

	// Test invalid message type for reply handler
	err = cluster.HandleFuncWithReply(MessageType(1), func(sender *Node, packet *Packet) (interface{}, error) {
		return nil, nil
	})
	if err == nil {
		t.Error("Expected error for reserved message type")
	}

	// Test UnregisterMessageType
	if !cluster.UnregisterMessageType(testMsg) {
		t.Error("UnregisterMessageType failed")
	}

	// Test unregister invalid type
	if cluster.UnregisterMessageType(MessageType(1)) {
		t.Error("Expected false for reserved message type")
	}
}

func TestEventHandlers(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Test HandleNodeStateChangeFunc
	var mu sync.Mutex
	stateChangeCalled := false
	handlerID := cluster.HandleNodeStateChangeFunc(func(node *Node, prevState NodeState) {
		mu.Lock()
		stateChangeCalled = true
		mu.Unlock()
	})

	// Trigger state change
	testNode := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(testNode)
	cluster.nodes.updateState(testNode.ID, NodeSuspect)

	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	if !stateChangeCalled {
		t.Error("State change handler was not called")
	}
	mu.Unlock()

	// Test RemoveNodeStateChangeHandler
	if !cluster.RemoveNodeStateChangeHandler(handlerID) {
		t.Error("RemoveNodeStateChangeHandler failed")
	}

	// Test HandleNodeMetadataChangeFunc
	metadataChangeCalled := false
	metadataHandlerID := cluster.HandleNodeMetadataChangeFunc(func(node *Node) {
		mu.Lock()
		metadataChangeCalled = true
		mu.Unlock()
	})

	// Trigger metadata change
	testNode.metadata.set("key", "value")
	cluster.nodes.notifyMetadataChanged(testNode)

	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	if !metadataChangeCalled {
		t.Error("Metadata change handler was not called")
	}
	mu.Unlock()

	// Test RemoveNodeMetadataChangeHandler
	if !cluster.RemoveNodeMetadataChangeHandler(metadataHandlerID) {
		t.Error("RemoveNodeMetadataChangeHandler failed")
	}

	// Test HandleGossipFunc
	gossipCalled := false
	gossipHandlerID := cluster.HandleGossipFunc(func() {
		mu.Lock()
		gossipCalled = true
		mu.Unlock()
	})

	// Manually trigger gossip handlers
	cluster.gossipEventHandlers.ForEach(func(handler GossipHandler) {
		handler()
	})

	mu.Lock()
	if !gossipCalled {
		t.Error("Gossip handler was not called")
	}
	mu.Unlock()

	// Test RemoveGossipHandler
	if !cluster.RemoveGossipHandler(gossipHandlerID) {
		t.Error("RemoveGossipHandler failed")
	}
}

func TestAddressString(t *testing.T) {
	tests := []struct {
		name     string
		addr     Address
		expected string
	}{
		{
			name:     "IP and Port",
			addr:     Address{IP: []byte{127, 0, 0, 1}, Port: 8080},
			expected: "127.0.0.1:8080",
		},
		{
			name:     "URL only",
			addr:     Address{URL: "http://example.com"},
			expected: "http://example.com",
		},
		{
			name:     "Empty address",
			addr:     Address{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.addr.String()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestClusterCalculationEdgeCases(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Test with only local node (alive count = 1)
	fanout := cluster.CalcFanOut()
	if fanout < 0 {
		t.Errorf("Expected non-negative fanout, got %d", fanout)
	}

	ttl := cluster.getMaxTTL()
	if ttl < 0 {
		t.Errorf("Expected non-negative TTL, got %d", ttl)
	}

	payloadSize := cluster.CalcPayloadSize(0)
	if payloadSize != 0 {
		t.Errorf("Expected payload size 0 for 0 items, got %d", payloadSize)
	}

	// Add nodes and test scaling
	for i := 0; i < 10; i++ {
		node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
		cluster.nodes.addOrUpdate(node)
	}

	fanout = cluster.CalcFanOut()
	if fanout <= 0 {
		t.Errorf("Expected positive fanout, got %d", fanout)
	}

	ttl = cluster.getMaxTTL()
	if ttl <= 0 {
		t.Errorf("Expected positive TTL, got %d", ttl)
	}

	payloadSize = cluster.CalcPayloadSize(10)
	if payloadSize <= 0 {
		t.Errorf("Expected positive payload size, got %d", payloadSize)
	}
}

func TestBroadcastQueueFull(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.SendQueueSize = 1 // Very small queue

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Fill the queue
	for i := 0; i < 10; i++ {
		packet := NewPacket()
		packet.TTL = 5
		cluster.enqueuePacketForBroadcast(packet, TransportBestEffort, nil, nil)
	}

	// Give time for queue to process
	time.Sleep(10 * time.Millisecond)
}

func TestJoinQueueFull(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.JoinQueueSize = 1 // Very small queue

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Try to overflow join queue
	for i := 0; i < 10; i++ {
		req := &joinRequest{nodeAddr: "127.0.0.1:8001"}
		select {
		case cluster.joinQueue <- req:
		default:
			// Queue full, expected
		}
	}
}
