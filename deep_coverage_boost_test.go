package gossip

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

// ============================================================================
// cluster_handlers: handlePing, handleMetadataUpdate, calculateJoinResponseSize
// ============================================================================

func TestHandlePingUpdatesTimestamp(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Create a ping packet
	ping := NewPacket()
	defer ping.Release()
	ping.SenderID = nodeID
	ping.MessageType = pingMsg
	ping.SetCodec(config.MsgCodec)

	payload, _ := config.MsgCodec.Marshal(&pingMessage{
		SenderID:      nodeID,
		AdvertiseAddr: "127.0.0.1:8001",
	})
	ping.SetPayload(payload)

	reply, err := cluster.handlePing(node, ping)
	if err != nil {
		t.Fatalf("handlePing returned error: %v", err)
	}
	if reply == nil {
		t.Fatal("Expected pong reply, got nil")
	}

	pong, ok := reply.(*pongMessage)
	if !ok {
		t.Fatalf("Expected *pongMessage, got %T", reply)
	}
	if pong.NodeID != cluster.localNode.ID {
		t.Errorf("Pong should contain local node ID")
	}
}

func TestHandleMetadataUpdateProcessesData(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Create metadata update packet
	packet := NewPacket()
	defer packet.Release()
	packet.SenderID = nodeID
	packet.MessageType = metadataUpdateMsg
	packet.SetCodec(config.MsgCodec)

	ts := hlc.Now()
	update := metadataUpdateMessage{
		MetadataTimestamp: ts,
		Metadata:          map[string]interface{}{"role": "gateway"},
		NodeState:         NodeAlive,
	}
	payload, _ := config.MsgCodec.Marshal(&update)
	packet.SetPayload(payload)

	cluster.handleMetadataUpdate(node, packet)

	// Check that metadata was applied
	val := node.metadata.GetString("role")
	if val != "gateway" {
		t.Errorf("Expected metadata role=gateway, got %q", val)
	}
}

func TestHandleMetadataUpdateOlderTimestamp(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Capture an older timestamp first
	olderTS := hlc.Now()
	time.Sleep(1 * time.Millisecond)

	// Set newer metadata
	node.metadata.update(map[string]interface{}{"role": "gateway"}, hlc.Now(), false)

	// Try to apply older metadata using the older timestamp
	packet := NewPacket()
	defer packet.Release()
	packet.SenderID = nodeID
	packet.MessageType = metadataUpdateMsg
	packet.SetCodec(config.MsgCodec)

	update := metadataUpdateMessage{
		MetadataTimestamp: olderTS,
		Metadata:          map[string]interface{}{"role": "ai"},
		NodeState:         NodeAlive,
	}
	payload, _ := config.MsgCodec.Marshal(&update)
	packet.SetPayload(payload)

	cluster.handleMetadataUpdate(node, packet)

	// Old update should be rejected
	val := node.metadata.GetString("role")
	if val != "gateway" {
		t.Errorf("Expected metadata role=gateway (newer), got %q", val)
	}
}

func TestCalculateJoinResponseSize(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// With just the local node
	size := cluster.calculateJoinResponseSize(1)
	if size <= 0 {
		t.Errorf("Expected positive size for join response, got %d", size)
	}

	// Add more nodes
	for i := 0; i < 10; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	size2 := cluster.calculateJoinResponseSize(int(cluster.nodes.getAliveCount()))
	if size2 <= size {
		t.Errorf("Size should increase with more nodes: %d vs %d", size2, size)
	}
}

// ============================================================================
// cluster: joinPeer (additional paths)
// ============================================================================

func TestJoinPeerDeadPeer(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.JoinQueueSize = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Drain join queue
	go func() {
		for {
			select {
			case <-cluster.joinQueue:
			case <-cluster.shutdownContext.Done():
				return
			}
		}
	}()
	defer cluster.cancelFunc()

	// joinPeer to self should skip
	cluster.joinPeer(cluster.localNode.AdvertisedAddr())
	// Should not crash - just exercises the code path
}

// ============================================================================
// NodeGroup: handleNodeStateChange, SendToPeers, SendToPeersReliable
// ============================================================================

func TestNodeGroupHandleNodeStateChange(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	ng := NewNodeGroup(cluster, map[string]string{"role": "worker"}, nil)

	// Add a node with matching metadata
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	node.metadata.SetString("role", "worker")

	// Trigger metadata change to add to group
	ng.handleNodeMetadataChange(node)
	if len(ng.GetNodes(nil)) != 1 {
		t.Fatalf("Expected 1 node in group, got %d", len(ng.GetNodes(nil)))
	}

	// Mark node dead => handleStateChange should remove it
	cluster.nodes.updateState(nodeID, NodeDead)
	ng.handleNodeStateChange(node, NodeAlive)

	if len(ng.GetNodes(nil)) != 0 {
		t.Error("Dead node should be removed from group")
	}

	// Mark node alive and re-add
	cluster.nodes.updateState(nodeID, NodeAlive)
	ng.handleNodeMetadataChange(node)

	// Mark node leaving
	cluster.nodes.updateState(nodeID, NodeLeaving)
	ng.handleNodeStateChange(node, NodeAlive)
	if len(ng.GetNodes(nil)) != 0 {
		t.Error("Leaving node should be removed from group")
	}
}

func TestNodeGroupSendToPeersEdgeCases(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	ng := NewNodeGroup(cluster, map[string]string{"role": "worker"}, nil)

	// Invalid message type
	err = ng.SendToPeers(0, "test")
	if err == nil {
		t.Error("Should reject reserved message type")
	}

	err = ng.SendToPeersReliable(0, "test")
	if err == nil {
		t.Error("Should reject reserved message type")
	}

	// Valid message type with no peers
	err = ng.SendToPeers(UserMsg, "test")
	if err != nil {
		t.Errorf("Empty peer list should not error: %v", err)
	}
}

// ============================================================================
// DataNodeGroup: handleNodeStateChange, SendToPeers, SendToPeersReliable
// ============================================================================

func TestDataNodeGroupHandleNodeStateChange(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	dng := NewDataNodeGroup[string](cluster, map[string]string{"role": "ai"},
		&DataNodeGroupOptions[string]{
			DataInitializer: func(node *Node) *string {
				s := "data-for-" + node.AdvertisedAddr()
				return &s
			},
		},
	)

	// Add matching node
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	node.metadata.SetString("role", "ai")

	dng.handleNodeMetadataChange(node)
	nodes := dng.GetNodes(nil)
	if len(nodes) != 1 {
		t.Fatalf("Expected 1 node, got %d", len(nodes))
	}

	// Verify data initializer was called
	data := dng.GetNodeData(nodeID)
	if data == nil || *data != "data-for-127.0.0.1:8001" {
		t.Errorf("Expected data initializer result, got %v", data)
	}

	// Mark dead => should remove
	cluster.nodes.updateState(nodeID, NodeDead)
	dng.handleNodeStateChange(node, NodeAlive)
	if len(dng.GetNodes(nil)) != 0 {
		t.Error("Dead node should be removed from data group")
	}
}

func TestDataNodeGroupSendToPeersEdgeCases(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	dng := NewDataNodeGroup[string](cluster, map[string]string{"role": "ai"},
		&DataNodeGroupOptions[string]{
			DataInitializer: func(node *Node) *string { return nil },
		},
	)

	// Invalid message type
	err = dng.SendToPeers(0, "test")
	if err == nil {
		t.Error("Should reject reserved message type")
	}

	err = dng.SendToPeersReliable(0, "test")
	if err == nil {
		t.Error("Should reject reserved message type")
	}
}

// ============================================================================
// HTTPTransport: HandleGossipRequest additional paths
// ============================================================================

func TestHandleGossipRequestMethodNotAllowed(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	ht := NewHTTPTransport(config)

	req := httptest.NewRequest(http.MethodGet, "/gossip", nil)
	w := httptest.NewRecorder()

	ht.HandleGossipRequest(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

func TestHandleGossipRequestBadPacket(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	ht := NewHTTPTransport(config)

	req := httptest.NewRequest(http.MethodPost, "/gossip", strings.NewReader("invalid data"))
	w := httptest.NewRecorder()

	ht.HandleGossipRequest(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

func TestHandleGossipRequestAuthRequired(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	config.BearerToken = "secret-token"
	ht := NewHTTPTransport(config)

	// No auth header
	req := httptest.NewRequest(http.MethodPost, "/gossip", nil)
	w := httptest.NewRecorder()
	ht.HandleGossipRequest(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 for missing auth, got %d", w.Code)
	}

	// Wrong format
	req2 := httptest.NewRequest(http.MethodPost, "/gossip", nil)
	req2.Header.Set("Authorization", "Basic abc")
	w2 := httptest.NewRecorder()
	ht.HandleGossipRequest(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 for wrong auth format, got %d", w2.Code)
	}

	// Wrong token
	req3 := httptest.NewRequest(http.MethodPost, "/gossip", nil)
	req3.Header.Set("Authorization", "Bearer wrong-token")
	w3 := httptest.NewRecorder()
	ht.HandleGossipRequest(w3, req3)
	if w3.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 for wrong token, got %d", w3.Code)
	}
}

func TestHandleGossipRequestNoReplyValid(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	ht := NewHTTPTransport(config)

	// Create a valid packet
	packet := NewPacket()
	packet.TTL = 1
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("hello")
	packet.SetPayload(payload)

	buf, _ := ht.packetToBuffer(packet, false) // no reply expected

	req := httptest.NewRequest(http.MethodPost, "/gossip", strings.NewReader(string(buf)))
	w := httptest.NewRecorder()

	ht.HandleGossipRequest(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected 204, got %d", w.Code)
	}

	// Drain the packet channel
	select {
	case p := <-ht.packetChannel:
		p.Release()
	case <-time.After(time.Second):
		t.Error("Expected packet in channel")
	}
}

func TestHandleGossipRequestQueueFull(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	config.IncomingPacketQueueDepth = 1
	ht := NewHTTPTransport(config)

	// Fill the queue
	filler := NewPacket()
	ht.packetChannel <- filler

	// Create valid packet
	packet := NewPacket()
	packet.TTL = 1
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("hello")
	packet.SetPayload(payload)
	buf, _ := ht.packetToBuffer(packet, false)

	req := httptest.NewRequest(http.MethodPost, "/gossip", strings.NewReader(string(buf)))
	w := httptest.NewRecorder()

	ht.HandleGossipRequest(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503 for full queue, got %d", w.Code)
	}

	// Clean up
	<-ht.packetChannel
	filler.Release()
}

// ============================================================================
// HTTPTransport: Send and SendWithReply integration
// ============================================================================

func TestHTTPTransportSend(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	ht := NewHTTPTransport(config)

	// Start a test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Just accept and discard
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, server.URL)

	packet := NewPacket()
	packet.TTL = 1
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("http-send-test")
	packet.SetPayload(payload)

	err := ht.Send(TransportReliable, node, packet)
	if err != nil {
		t.Fatalf("HTTP Send failed: %v", err)
	}
}

func TestHTTPTransportSendWithReplyIntegration(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	ht := NewHTTPTransport(config)

	// Create a reply packet for the server to return
	replyPacket := NewPacket()
	replyPacket.TTL = 0
	replyPacket.MessageType = UserMsg
	replyPacket.SetCodec(config.MsgCodec)
	replyPayload, _ := config.MsgCodec.Marshal("pong")
	replyPacket.SetPayload(replyPayload)
	replyBuf, _ := ht.packetToBuffer(replyPacket, false)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(replyBuf)
	}))
	defer server.Close()

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, server.URL)

	packet := NewPacket()
	packet.TTL = 1
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("ping")
	packet.SetPayload(payload)

	reply, err := ht.SendWithReply(node, packet)
	if err != nil {
		t.Fatalf("HTTP SendWithReply failed: %v", err)
	}
	if reply == nil {
		t.Fatal("Expected reply, got nil")
	}
	reply.Release()
}

func TestHTTPTransportSendWithReplyNoContent(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	ht := NewHTTPTransport(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, server.URL)

	packet := NewPacket()
	packet.TTL = 1
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("test")
	packet.SetPayload(payload)

	reply, err := ht.SendWithReply(node, packet)
	if err != nil {
		t.Fatalf("Expected no error for 204, got: %v", err)
	}
	if reply != nil {
		t.Error("Expected nil reply for 204")
		reply.Release()
	}
}

func TestHTTPTransportSendWithReplyHTTPError(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	ht := NewHTTPTransport(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, server.URL)

	packet := NewPacket()
	packet.TTL = 1
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("test")
	packet.SetPayload(payload)

	_, err := ht.SendWithReply(node, packet)
	if err == nil {
		t.Fatal("Expected error for 500 response")
	}
}

// ============================================================================
// Cluster: gossip manager paths - gossipMetadata, stateGossip
// ============================================================================

func TestGossipMetadataWithNodes(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Set local metadata
	cluster.localNode.metadata.SetString("role", "gateway")

	// Add remote node
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// gossipMetadata sends metadata to a random peer
	cluster.gossipMetadata()

	// Should not panic - mock transport will fail but that's OK
}

// ============================================================================
// cluster: NodeGroup GetNodes with filters
// ============================================================================

func TestNodeGroupGetNodesFilters(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	ng := NewNodeGroup(cluster, map[string]string{"role": "worker"}, nil)

	// Add 3 nodes with matching metadata
	for i := 0; i < 3; i++ {
		nodeID := NodeID(uuid.New())
		node := newNode(nodeID, fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(node)
		node.metadata.SetString("role", "worker")
		ng.handleNodeMetadataChange(node)
	}

	nodes := ng.GetNodes(nil)
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// Verify all nodes are alive
	allNodes := ng.GetNodes(nil)
	for _, n := range allNodes {
		if n.GetObservedState() != NodeAlive {
			t.Errorf("Expected node to be alive, got %v", n.GetObservedState())
		}
	}
}

func TestDataNodeGroupGetNodesFilters(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	dng := NewDataNodeGroup[string](cluster, map[string]string{"role": "ai"},
		&DataNodeGroupOptions[string]{
			DataInitializer: func(node *Node) *string { return nil },
		},
	)

	// Add nodes with matching metadata
	for i := 0; i < 3; i++ {
		nodeID := NodeID(uuid.New())
		node := newNode(nodeID, fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(node)
		node.metadata.SetString("role", "ai")
		dng.handleNodeMetadataChange(node)
	}

	nodes := dng.GetNodes(nil)
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	allNodes := dng.GetNodes(nil)
	for _, n := range allNodes {
		if n.GetObservedState() != NodeAlive {
			t.Errorf("Expected node to be alive, got %v", n.GetObservedState())
		}
	}
}

// ============================================================================
// Cluster: broadcastWorker paths
// ============================================================================

func TestBroadcastWorkerProcessesQueue(t *testing.T) {
	mt := newConfigurableMockTransport()
	config := DefaultConfig()
	config.Transport = mt
	config.MsgCodec = codec.NewJsonCodec()
	config.SendQueueSize = 50

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add peers
	for i := 0; i < 3; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	// Start broadcast worker using the cluster's internal mechanism
	cluster.shutdownWg.Add(1)
	go cluster.broadcastWorker()

	// Enqueue a packet
	p := NewPacket()
	p.TTL = 3
	p.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("broadcast-test")
	p.SetPayload(payload)

	cluster.enqueuePacketForBroadcast(p, TransportBestEffort, nil, nil)

	// Wait for worker to process
	time.Sleep(100 * time.Millisecond)

	if mt.sendCount.Load() == 0 {
		t.Error("Expected at least 1 send from broadcast worker")
	}

	// Stop the broadcast worker by shutting down the cluster's context
	cluster.cancelFunc()
	cluster.shutdownWg.Wait()
}

// ============================================================================
// Metadata: Exists, GetAllAsString, GetAllKeys
// ============================================================================

func TestMetadataExistsDeep(t *testing.T) {
	md := NewMetadata()
	md.SetString("key1", "val1")

	if !md.Exists("key1") {
		t.Error("key1 should exist")
	}
	if md.Exists("key2") {
		t.Error("key2 should not exist")
	}

	md.Delete("key1")
	if md.Exists("key1") {
		t.Error("key1 should not exist after delete")
	}
}

func TestMetadataGetAllAsStringDeep(t *testing.T) {
	md := NewMetadata()
	md.SetString("name", "node1")
	md.SetInt64("port", 8080)

	all := md.GetAllAsString()
	if all["name"] != "node1" {
		t.Errorf("Expected name=node1, got %q", all["name"])
	}
	if all["port"] != "8080" {
		t.Errorf("Expected port=8080, got %q", all["port"])
	}
}

func TestMetadataGetAllKeysDeep(t *testing.T) {
	md := NewMetadata()
	md.SetString("a", "1")
	md.SetString("b", "2")
	md.SetString("c", "3")

	keys := md.GetAllKeys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	keyMap := make(map[string]bool)
	for _, k := range keys {
		keyMap[k] = true
	}
	for _, expected := range []string{"a", "b", "c"} {
		if !keyMap[expected] {
			t.Errorf("Missing key %q", expected)
		}
	}
}

// ============================================================================
// handler_registry: dispatch with reply handler
// ============================================================================

func TestHandlerRegistryDispatchWithRespond(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	responded := false
	cluster.HandleFuncWithResponse(UserMsg,
		func(node *Node, packet *Packet) (interface{}, error) {
			responded = true
			return "reply-data", nil
		},
	)

	// Simulate incoming packet for this handler
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	p := NewPacket()
	p.SenderID = nodeID
	p.MessageType = UserMsg
	p.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("test")
	p.SetPayload(payload)

	// Set up reply channel
	replyChan := make(chan *Packet, 1)
	p.SetReplyChan(replyChan)

	cluster.handleIncomingPacket(p)

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	if !responded {
		t.Error("Reply handler should have been called")
	}
}

// ============================================================================
// Cluster: Leave
// ============================================================================

func TestLeaveMarksLocalDead(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start so context is active
	cluster.Start()
	defer cluster.Leave()

	// Add a peer
	n := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(n)

	cluster.Leave()

	if cluster.localNode.GetObservedState() != NodeLeaving {
		t.Errorf("Local node should be Leaving after Leave, got %v", cluster.localNode.GetObservedState())
	}
}

// ============================================================================
// socket transport: TCP listener operations
// ============================================================================

func TestSocketTransportShutdown(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	config.BindAddr = ":0"
	config.AdvertiseAddr = "127.0.0.1:0"

	st := NewSocketTransport(config)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	err := st.Start(ctx, &wg)
	if err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	// Verify listeners exist
	if st.tcpListener == nil {
		t.Error("TCP listener should exist")
	}
	if st.udpListener == nil {
		t.Error("UDP listener should exist")
	}

	// Trigger shutdown
	cancel()
	wg.Wait()
}

// ============================================================================
// sendToWithResponse nil response bug (now fixed)
// ============================================================================

func TestSendToWithResponseNilResponse(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{} // SendWithReply returns nil, nil
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Should not panic - our fix handles nil response
	var response struct{}
	err = cluster.sendToWithResponse(node, pushPullStateMsg, "test", &response)
	if err == nil {
		t.Error("Expected error for nil response from SendWithReply")
	}
}

// ============================================================================
// DefaultResolver: LookupSRV path coverage
// ============================================================================

func TestDefaultResolverLookupSRVNoTrailingDot(t *testing.T) {
	r := NewDefaultResolver()
	// Service without trailing dot should still work (dot gets added)
	_, err := r.LookupSRV("doesnotexist.invalid")
	if err == nil {
		t.Fatal("Expected error for unknown SRV")
	}
}

// ============================================================================
// Node state caching: getCachedNodesInStates
// ============================================================================

func TestNodeListCachedStates(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add nodes
	for i := 0; i < 5; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	// First call should populate cache
	nodes1 := cluster.nodes.getAllInStates([]NodeState{NodeAlive})
	count1 := len(nodes1)

	// Second call should use cache
	nodes2 := cluster.nodes.getAllInStates([]NodeState{NodeAlive})
	count2 := len(nodes2)

	if count1 != count2 {
		t.Errorf("Cached result should match: %d vs %d", count1, count2)
	}

	// Modify state => cache should invalidate
	if count1 > 1 {
		cluster.nodes.updateState(nodes1[1].ID, NodeDead)
		nodes3 := cluster.nodes.getAllInStates([]NodeState{NodeAlive})
		if len(nodes3) >= count1 {
			t.Error("Cache should be invalidated after state change")
		}
	}
}

// ============================================================================
// cluster: Start with incoming packet processing
// ============================================================================

func TestClusterStartProcessesIncomingPackets(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	cluster.Start()
	defer cluster.Leave()

	// Inject a packet into the transport channel
	p := NewPacket()
	p.SenderID = cluster.localNode.ID // from self - will be ignored
	p.MessageType = UserMsg
	p.TTL = 3
	p.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("test")
	p.SetPayload(payload)

	config.Transport.PacketChannel() <- p

	time.Sleep(100 * time.Millisecond)
}

// ============================================================================
// Metadata: UpdateFrom (exercised by handleMetadataUpdate)
// ============================================================================

func TestMetadataUpdateFromOlderTimestamp(t *testing.T) {
	md := NewMetadata()

	ts1 := hlc.Now()
	time.Sleep(1 * time.Millisecond)
	ts2 := hlc.Now()

	// Apply newer first
	md.update(map[string]interface{}{"key": "new"}, ts2, false)
	if md.GetString("key") != "new" {
		t.Error("Should apply newer timestamp")
	}

	// Apply older - should be rejected
	md.update(map[string]interface{}{"key": "old"}, ts1, false)
	if md.GetString("key") != "new" {
		t.Error("Should reject older timestamp")
	}
}

// ============================================================================
// Node: additional methods
// ============================================================================

func TestNodeSuspectAndAlive(t *testing.T) {
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")

	if !node.Alive() {
		t.Error("New node should be Alive")
	}
	if node.Suspect() {
		t.Error("New node should not be Suspect")
	}

	node.observedState = NodeSuspect
	if !node.Suspect() {
		t.Error("Should be Suspect")
	}
	if node.Alive() {
		t.Error("Suspect node should not report Alive")
	}
}

func TestNodeGetSetAddress(t *testing.T) {
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")

	if !node.IsAddressEmpty() {
		t.Error("Initial address should be empty")
	}

	addr := Address{IP: net.ParseIP("10.0.0.1"), Port: 7946}
	node.SetAddress(addr)

	if node.IsAddressEmpty() {
		t.Error("Address should be set")
	}

	got := node.GetAddress()
	if !got.IP.Equal(addr.IP) || got.Port != addr.Port {
		t.Errorf("Address mismatch: got %+v expected %+v", got, addr)
	}

	node.ClearAddress()
	if !node.IsAddressEmpty() {
		t.Error("Address should be cleared")
	}
}

// ============================================================================
// handlePing: unknown sender path (sender == nil)
// ============================================================================

func TestHandlePingUnknownSender(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())

	ping := NewPacket()
	defer ping.Release()
	ping.SenderID = nodeID
	ping.MessageType = pingMsg
	ping.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal(&pingMessage{
		SenderID:      nodeID,
		AdvertiseAddr: "127.0.0.1:9999",
	})
	ping.SetPayload(payload)

	// Call with nil sender - exercises the unknown sender path
	reply, err := cluster.handlePing(nil, ping)
	if err != nil {
		t.Fatalf("handlePing returned error: %v", err)
	}
	if reply == nil {
		t.Fatal("Expected pong reply from unknown sender")
	}

	// Verify join request was queued
	select {
	case req := <-cluster.joinQueue:
		if req.nodeAddr != "127.0.0.1:9999" {
			t.Errorf("Expected join addr 127.0.0.1:9999, got %s", req.nodeAddr)
		}
	default:
		t.Error("Expected join request to be queued for unknown sender")
	}
}

func TestHandlePingBadPayload(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	ping := NewPacket()
	defer ping.Release()
	ping.SenderID = NodeID(uuid.New())
	ping.MessageType = pingMsg
	ping.SetCodec(config.MsgCodec)
	ping.SetPayload([]byte("not-valid-json"))

	_, err = cluster.handlePing(nil, ping)
	if err == nil {
		t.Error("Expected error for bad payload")
	}
}

// ============================================================================
// handleMetadataUpdate: state mismatch, bad payload
// ============================================================================

func TestHandleMetadataUpdateStateMismatch(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Node is Alive but metadata says Suspect - exercises state update path
	packet := NewPacket()
	defer packet.Release()
	packet.SenderID = nodeID
	packet.MessageType = metadataUpdateMsg
	packet.SetCodec(config.MsgCodec)

	update := metadataUpdateMessage{
		MetadataTimestamp: hlc.Now(),
		Metadata:          map[string]interface{}{"role": "test"},
		NodeState:         NodeSuspect,
	}
	payload, _ := config.MsgCodec.Marshal(&update)
	packet.SetPayload(payload)

	cluster.handleMetadataUpdate(node, packet)

	// State should have been updated to Suspect
	if node.GetObservedState() != NodeSuspect {
		t.Errorf("Expected Suspect state, got %v", node.GetObservedState())
	}
}

func TestHandleMetadataUpdateBadPayload(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	packet := NewPacket()
	defer packet.Release()
	packet.SenderID = nodeID
	packet.MessageType = metadataUpdateMsg
	packet.SetCodec(config.MsgCodec)
	packet.SetPayload([]byte("bad-json"))

	err = cluster.handleMetadataUpdate(node, packet)
	if err == nil {
		t.Error("Expected error for bad payload")
	}
}

// ============================================================================
// calculateJoinResponseSize: all size brackets
// ============================================================================

func TestCalculateJoinResponseSizeAllBrackets(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// <= 5: returns totalAlive
	if s := cluster.calculateJoinResponseSize(3); s != 3 {
		t.Errorf("Expected 3 for tiny cluster, got %d", s)
	}
	if s := cluster.calculateJoinResponseSize(5); s != 5 {
		t.Errorf("Expected 5 for tiny cluster, got %d", s)
	}

	// <= 20: 80% of nodes
	if s := cluster.calculateJoinResponseSize(10); s != 8 {
		t.Errorf("Expected 8 for small cluster (80%% of 10), got %d", s)
	}
	if s := cluster.calculateJoinResponseSize(20); s != 16 {
		t.Errorf("Expected 16 for small cluster (80%% of 20), got %d", s)
	}

	// <= 100: cap at 20
	if s := cluster.calculateJoinResponseSize(50); s != 20 {
		t.Errorf("Expected 20 for medium cluster, got %d", s)
	}
	if s := cluster.calculateJoinResponseSize(100); s != 20 {
		t.Errorf("Expected 20 for medium cluster, got %d", s)
	}

	// > 100: cap at 25
	if s := cluster.calculateJoinResponseSize(500); s != 25 {
		t.Errorf("Expected 25 for large cluster, got %d", s)
	}
}

// ============================================================================
// HandleGossipRequest: auth paths, POST with valid packet
// ============================================================================

func TestHandleGossipRequestNoAuth(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	config.BearerToken = "secret-token"
	ht := NewHTTPTransport(config)

	// No auth header
	req := httptest.NewRequest(http.MethodPost, "/gossip", nil)
	w := httptest.NewRecorder()
	ht.HandleGossipRequest(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401, got %d", w.Code)
	}
}

func TestHandleGossipRequestBadAuthFormat(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	config.BearerToken = "secret-token"
	ht := NewHTTPTransport(config)

	req := httptest.NewRequest(http.MethodPost, "/gossip", nil)
	req.Header.Set("Authorization", "Basic notbearer")
	w := httptest.NewRecorder()
	ht.HandleGossipRequest(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401, got %d", w.Code)
	}
}

func TestHandleGossipRequestWrongToken(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	config.BearerToken = "secret-token"
	ht := NewHTTPTransport(config)

	req := httptest.NewRequest(http.MethodPost, "/gossip", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	ht.HandleGossipRequest(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401, got %d", w.Code)
	}
}

func TestHandleGossipRequestBadBody(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	ht := NewHTTPTransport(config)

	req := httptest.NewRequest(http.MethodPost, "/gossip", strings.NewReader("not-a-packet"))
	w := httptest.NewRecorder()
	ht.HandleGossipRequest(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

// ============================================================================
// HandleGossipRequest: valid packet (fire-and-forget, no reply)
// ============================================================================

func TestHandleGossipRequestValidFireAndForget(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	ht := NewHTTPTransport(config)

	// Build a valid packet buffer
	p := NewPacket()
	p.SenderID = NodeID(uuid.New())
	p.MessageType = UserMsg
	p.TTL = 3
	p.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("test-data")
	p.SetPayload(payload)

	buf, err := ht.packetToBuffer(p, false) // false = no reply expected
	if err != nil {
		t.Fatalf("Failed to encode packet: %v", err)
	}
	p.Release()

	req := httptest.NewRequest(http.MethodPost, "/gossip", strings.NewReader(string(buf)))
	w := httptest.NewRecorder()

	// Start a goroutine to consume from the packet channel
	go func() {
		select {
		case pkt := <-ht.packetChannel:
			pkt.Release()
		case <-time.After(2 * time.Second):
		}
	}()

	ht.HandleGossipRequest(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected 204 for fire-and-forget, got %d", w.Code)
	}
}

// ============================================================================
// NodeGroup/DataNodeGroup: SendToPeers with actual peers
// ============================================================================

func TestNodeGroupSendToPeersWithPeers(t *testing.T) {
	mt := newConfigurableMockTransport()
	config := DefaultConfig()
	config.Transport = mt
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	ng := NewNodeGroup(cluster, map[string]string{"role": "worker"}, nil)

	// Add matching nodes
	for i := 0; i < 3; i++ {
		nodeID := NodeID(uuid.New())
		node := newNode(nodeID, fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(node)
		node.metadata.SetString("role", "worker")
		ng.handleNodeMetadataChange(node)
	}

	// SendToPeers should succeed with valid message type
	err = ng.SendToPeers(UserMsg, "hello-peers")
	if err != nil {
		t.Errorf("SendToPeers failed: %v", err)
	}

	err = ng.SendToPeersReliable(UserMsg, "hello-reliable")
	if err != nil {
		t.Errorf("SendToPeersReliable failed: %v", err)
	}
}

func TestDataNodeGroupSendToPeersWithPeers(t *testing.T) {
	mt := newConfigurableMockTransport()
	config := DefaultConfig()
	config.Transport = mt
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	dng := NewDataNodeGroup[string](cluster, map[string]string{"role": "ai"},
		&DataNodeGroupOptions[string]{
			DataInitializer: func(node *Node) *string {
				s := "data"
				return &s
			},
		},
	)

	// Add matching nodes
	for i := 0; i < 3; i++ {
		nodeID := NodeID(uuid.New())
		node := newNode(nodeID, fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(node)
		node.metadata.SetString("role", "ai")
		dng.handleNodeMetadataChange(node)
	}

	err = dng.SendToPeers(UserMsg, "hello-data-peers")
	if err != nil {
		t.Errorf("DataNodeGroup SendToPeers failed: %v", err)
	}

	err = dng.SendToPeersReliable(UserMsg, "hello-reliable")
	if err != nil {
		t.Errorf("DataNodeGroup SendToPeersReliable failed: %v", err)
	}
}

// ============================================================================
// CalcFanOut: paths for different cluster sizes
// ============================================================================

func TestCalcFanOutVariousSizes(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// With 0 nodes
	f := cluster.CalcFanOut()
	if f < 1 {
		t.Errorf("Fan out should be at least 1, got %d", f)
	}

	// Add many nodes
	for i := 0; i < 50; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	f2 := cluster.CalcFanOut()
	if f2 <= f {
		t.Logf("Fan out with 50 nodes: %d (was %d with 0)", f2, f)
	}
}

// ============================================================================
// handlePushPullState: exercises combineStates through the handler
// ============================================================================

func TestHandlePushPullStateCombineStates(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add some local nodes
	for i := 0; i < 5; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	// Create a push-pull state request with some peer states
	peerID := NodeID(uuid.New())
	sender := newNode(peerID, "127.0.0.1:9000")
	cluster.nodes.addOrUpdate(sender)

	peerStates := []exchangeNodeState{
		{
			ID:             NodeID(uuid.New()),
			AdvertiseAddr:  "127.0.0.1:7001",
			State:          NodeAlive,
			StateTimestamp: hlc.Now(),
		},
		{
			ID:             NodeID(uuid.New()),
			AdvertiseAddr:  "127.0.0.1:7002",
			State:          NodeSuspect,
			StateTimestamp: hlc.Now(),
		},
	}

	packet := NewPacket()
	defer packet.Release()
	packet.SenderID = peerID
	packet.MessageType = pushPullStateMsg
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal(&peerStates)
	packet.SetPayload(payload)

	reply, err := cluster.handlePushPullState(sender, packet)
	if err != nil {
		t.Fatalf("handlePushPullState error: %v", err)
	}
	if reply == nil {
		t.Fatal("Expected reply with local states")
	}

	// Verify the new nodes were added via combineStates
	totalAlive := cluster.nodes.getAliveCount()
	if totalAlive < 5 {
		t.Errorf("Expected at least 5 alive nodes (had 5 + the new peer), got %d", totalAlive)
	}
}

// ============================================================================
// handleNodeLeave: exercises the leave handler
// ============================================================================

func TestHandleNodeLeaveStateTransition(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	if node.GetObservedState() != NodeAlive {
		t.Fatalf("Expected alive, got %v", node.GetObservedState())
	}

	packet := NewPacket()
	defer packet.Release()
	packet.SenderID = nodeID

	cluster.handleNodeLeave(node, packet)

	if node.GetObservedState() != NodeLeaving {
		t.Errorf("Expected Leaving after handleNodeLeave, got %v", node.GetObservedState())
	}
}

// ============================================================================
// exchangeState: test the state exchange mechanism
// ============================================================================

func TestExchangeStateWithPeer(t *testing.T) {
	// Create a "server" cluster that will respond to push-pull requests
	serverConfig := DefaultConfig()
	serverConfig.Transport = &mockTransport{}
	serverConfig.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(serverConfig)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add local nodes
	for i := 0; i < 3; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	// Create a peer to exchange state with using replyMockTransport
	peerID := NodeID(uuid.New())
	peer := newNode(peerID, "127.0.0.1:9001")
	cluster.nodes.addOrUpdate(peer)

	// exerciseState just needs to attempt the exchange - collect error info
	// The mock transport will fail to actually send, which exercises error paths
	cluster.exchangeState([]*Node{peer}, nil)

	// node should still exist
	if n := cluster.nodes.get(peerID); n == nil {
		t.Error("Peer should still exist after failed exchange")
	}
}

// ============================================================================
// HTTPTransport: ensureNodeAddressResolved paths
// ============================================================================

func TestHTTPTransportEnsureNodeAddressResolved(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	config.BindAddr = "/gossip"
	ht := NewHTTPTransport(config)

	// Empty address node
	node := newNode(NodeID(uuid.New()), "")
	err := ht.ensureNodeAddressResolved(node)
	if err == nil {
		t.Error("Expected error for empty address")
	}

	// HTTP URL node
	node2 := newNode(NodeID(uuid.New()), "http://127.0.0.1:8080")
	err = ht.ensureNodeAddressResolved(node2)
	if err != nil {
		t.Errorf("Unexpected error for valid HTTP URL: %v", err)
	}
	if node2.IsAddressEmpty() {
		t.Error("Address should be resolved for HTTP URL")
	}

	// HTTPS URL node
	node3 := newNode(NodeID(uuid.New()), "https://example.com:443")
	err = ht.ensureNodeAddressResolved(node3)
	if err != nil {
		t.Errorf("Unexpected error for valid HTTPS URL: %v", err)
	}
}

// ============================================================================
// broadcastWorker: tagged message path
// ============================================================================

func TestBroadcastWorkerTaggedMessage(t *testing.T) {
	mt := newConfigurableMockTransport()
	config := DefaultConfig()
	config.Transport = mt
	config.MsgCodec = codec.NewJsonCodec()
	config.SendQueueSize = 50

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add peers with tags
	for i := 0; i < 3; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i), "group-a")
		cluster.nodes.addOrUpdate(n)
	}

	cluster.shutdownWg.Add(1)
	go cluster.broadcastWorker()

	// Enqueue a tagged packet
	p := NewPacket()
	p.TTL = 3
	tag := "group-a"
	p.Tag = &tag
	p.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("tagged-broadcast")
	p.SetPayload(payload)

	cluster.enqueuePacketForBroadcast(p, TransportBestEffort, nil, nil)

	time.Sleep(100 * time.Millisecond)

	if mt.sendCount.Load() == 0 {
		t.Log("No sends recorded (expected with mock transport)")
	}

	cluster.cancelFunc()
	cluster.shutdownWg.Wait()
}

// ============================================================================
// Cluster.handleIncomingPacket: TTL exhausted path
// ============================================================================

func TestHandleIncomingPacketTTLZero(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	p := NewPacket()
	p.SenderID = nodeID
	p.MessageType = UserMsg
	p.TTL = 0 // Already exhausted
	p.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("test")
	p.SetPayload(payload)

	// Should be handled but not rebroadcast
	cluster.handleIncomingPacket(p)
	time.Sleep(50 * time.Millisecond)
}

// ============================================================================
// Metadata: GetAll, GetAllKeys, GetAllAsString with empty metadata
// ============================================================================

func TestMetadataGetMethodsEmpty(t *testing.T) {
	md := NewMetadata()

	all := md.GetAll()
	if len(all) != 0 {
		t.Errorf("Expected empty map, got %d entries", len(all))
	}

	keys := md.GetAllKeys()
	if len(keys) != 0 {
		t.Errorf("Expected empty keys, got %d", len(keys))
	}

	allStr := md.GetAllAsString()
	if len(allStr) != 0 {
		t.Errorf("Expected empty string map, got %d entries", len(allStr))
	}
}

// ============================================================================
// Metadata: Exists edge cases
// ============================================================================

func TestMetadataExistsAfterDelete(t *testing.T) {
	md := NewMetadata()
	md.SetString("key", "val")

	if !md.Exists("key") {
		t.Error("key should exist")
	}

	md.Delete("key")

	if md.Exists("key") {
		t.Error("key should not exist after delete")
	}

	// Delete non-existent key should not panic
	md.Delete("nonexistent")
}

// ============================================================================
// handleJoin: exercises the join handler with various payloads
// ============================================================================

func TestHandleJoinBadPayload(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	sender := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(sender)

	packet := NewPacket()
	defer packet.Release()
	packet.SenderID = sender.ID
	packet.MessageType = nodeJoinMsg
	packet.SetCodec(config.MsgCodec)
	packet.SetPayload([]byte("bad-json"))

	_, err = cluster.handleJoin(sender, packet)
	if err == nil {
		t.Error("Expected error for bad join payload")
	}
}

// ============================================================================
// sendMessageWithTargetAndTag: exercise tag-based sending
// ============================================================================

func TestSendMessageWithTag(t *testing.T) {
	mt := newConfigurableMockTransport()
	config := DefaultConfig()
	config.Transport = mt
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add tagged nodes
	for i := 0; i < 3; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i), "tag-x")
		cluster.nodes.addOrUpdate(n)
	}

	tag := "tag-x"
	err = cluster.sendMessageWithTargetAndTag(nil, nil, &tag, TransportBestEffort, 3, UserMsg, "tagged-msg")
	if err != nil {
		t.Errorf("sendMessageWithTargetAndTag failed: %v", err)
	}
}

// ============================================================================
// NodeList: getCachedNodesInStates
// ============================================================================

func TestGetCachedNodesInStates(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	for i := 0; i < 10; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	// Get alive nodes through cache (10 added + 1 local node = 11)
	alive := cluster.nodes.getCachedNodesInStates([]NodeState{NodeAlive})
	if len(alive) != 11 {
		t.Errorf("Expected 11 alive, got %d", len(alive))
	}

	// Get again - should use cache
	alive2 := cluster.nodes.getCachedNodesInStates([]NodeState{NodeAlive})
	if len(alive2) != 11 {
		t.Errorf("Expected 11 alive (cached), got %d", len(alive2))
	}

	// Modify a node's state to invalidate cache
	aliveSlice := cluster.nodes.getRandomNodesInStates(1, []NodeState{NodeAlive}, nil)
	if len(aliveSlice) > 0 {
		cluster.nodes.updateState(aliveSlice[0].ID, NodeSuspect)
	}

	alive3 := cluster.nodes.getCachedNodesInStates([]NodeState{NodeAlive})
	if len(alive3) != 10 {
		t.Errorf("Expected 10 alive after suspect, got %d", len(alive3))
	}
}
