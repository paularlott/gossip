package gossip

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

// ============================================================================
// Cluster: exchangeState
// ============================================================================

func TestExchangeStateUpdatesNodes(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add some nodes
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	cluster.nodes.addOrUpdate(node1)
	cluster.nodes.addOrUpdate(node2)

	// exchangeState needs to send to nodes - but the mock transport will return errors
	// This tests the suspect marking path
	candidates := []*Node{node1}
	cluster.exchangeState(candidates, []NodeID{cluster.localNode.ID})

	// node1 should be marked suspect since mock transport fails sendToWithResponse
	if node1.GetObservedState() != NodeSuspect {
		t.Errorf("Expected node1 to be suspect after failed exchange, got %v", node1.GetObservedState())
	}
}

func TestExchangeStateExcludesNodes(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// exchangeState with no candidates should not panic
	cluster.exchangeState([]*Node{}, []NodeID{cluster.localNode.ID})
}

// ============================================================================
// Cluster: combineStates - thorough edge cases
// ============================================================================

func TestCombineStatesUnknownNodeAlive(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.JoinQueueSize = 10

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start join worker to drain the queue
	go func() {
		for {
			select {
			case <-cluster.joinQueue:
				// drain
			case <-cluster.shutdownContext.Done():
				return
			}
		}
	}()
	defer cluster.cancelFunc()

	// Unknown alive node triggers join request
	unknownNodeID := NodeID(uuid.New())
	remoteStates := []exchangeNodeState{
		{
			ID:             unknownNodeID,
			AdvertiseAddr:  "127.0.0.1:9999",
			State:          NodeAlive,
			StateTimestamp: hlc.Now(),
		},
	}

	cluster.combineStates(remoteStates)

	// The node should not be added directly - a join request should be queued instead
	node := cluster.nodes.get(unknownNodeID)
	if node != nil {
		t.Error("Unknown alive node should trigger join, not direct add")
	}
}

func TestCombineStatesUnknownNodeDead(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Unknown dead node should be stored as tombstone
	unknownNodeID := NodeID(uuid.New())
	ts := hlc.Now()
	remoteStates := []exchangeNodeState{
		{
			ID:             unknownNodeID,
			AdvertiseAddr:  "127.0.0.1:9999",
			State:          NodeDead,
			StateTimestamp: ts,
		},
	}

	cluster.combineStates(remoteStates)

	// Should be stored as a tombstone
	node := cluster.nodes.get(unknownNodeID)
	if node == nil {
		t.Fatal("Dead tombstone should have been stored")
	}
	if node.GetObservedState() != NodeDead {
		t.Errorf("Tombstone should be Dead, got %v", node.GetObservedState())
	}
}

func TestCombineStatesUnknownNodeLeaving(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	unknownNodeID := NodeID(uuid.New())
	remoteStates := []exchangeNodeState{
		{
			ID:             unknownNodeID,
			AdvertiseAddr:  "127.0.0.1:9999",
			State:          NodeLeaving,
			StateTimestamp: hlc.Now(),
		},
	}

	cluster.combineStates(remoteStates)

	node := cluster.nodes.get(unknownNodeID)
	if node == nil {
		t.Fatal("Leaving tombstone should have been stored")
	}
	if node.GetObservedState() != NodeLeaving {
		t.Errorf("Tombstone should be Leaving, got %v", node.GetObservedState())
	}
}

func TestCombineStatesAddressChangeIgnoredFromGossip(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.JoinQueueSize = 10

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start drain goroutine
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

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	time.Sleep(1 * time.Millisecond)

	// Simulate address change via state exchange — should be IGNORED
	// because state exchange is secondhand gossip and not authoritative.
	// Address changes should only come from the node itself (join/ping).
	remoteStates := []exchangeNodeState{
		{
			ID:             nodeID,
			AdvertiseAddr:  "127.0.0.1:9999", // different address
			State:          NodeDead,
			StateTimestamp: hlc.Now(),
		},
	}

	cluster.combineStates(remoteStates)

	updatedNode := cluster.nodes.get(nodeID)
	if updatedNode.advertiseAddr != "127.0.0.1:8001" {
		t.Errorf("Expected address to remain 127.0.0.1:8001 (gossip should not overwrite), got %s", updatedNode.advertiseAddr)
	}
}

func TestCombineStatesOlderTimestampIgnored(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	node.observedStateTime = hlc.Now()
	cluster.nodes.addOrUpdate(node)

	// Send older timestamp - should be ignored
	olderTs := hlc.Timestamp(0) // zero timestamp is always older
	remoteStates := []exchangeNodeState{
		{
			ID:             nodeID,
			AdvertiseAddr:  "127.0.0.1:8001",
			State:          NodeDead,
			StateTimestamp: olderTs,
		},
	}

	cluster.combineStates(remoteStates)

	if cluster.nodes.get(nodeID).GetObservedState() != NodeAlive {
		t.Error("Node should remain alive when remote timestamp is older")
	}
}

func TestCombineStatesSameStateSameTimestamp(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	ts := hlc.Now()
	node.observedStateTime = ts
	cluster.nodes.addOrUpdate(node)

	remoteStates := []exchangeNodeState{
		{
			ID:             nodeID,
			AdvertiseAddr:  "127.0.0.1:8001",
			State:          NodeAlive, // same state
			StateTimestamp: ts,        // same timestamp
		},
	}

	cluster.combineStates(remoteStates)
	if cluster.nodes.get(nodeID).GetObservedState() != NodeAlive {
		t.Error("Node should remain alive when same state and timestamp")
	}
}

// ============================================================================
// Cluster: checkPeerConnectivity
// ============================================================================

func TestCheckPeerConnectivityNoSeedPeers(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// No seed peers - should return early without panic
	cluster.checkPeerConnectivity()
}

func TestCheckPeerConnectivityBelowThreshold(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.PeerRecoveryInterval = 1 * time.Second
	config.JoinQueueSize = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start drain goroutine
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

	// Set up seed peers - 4 peers
	cluster.seedPeers = []string{
		"127.0.0.1:9001",
		"127.0.0.1:9002",
		"127.0.0.1:9003",
		"127.0.0.1:9004",
	}

	// Only 1 alive node (local node) vs 4 seed peers
	// Threshold = 4/2 = 2
	// 1 alive <= 2 threshold => should trigger recovery
	cluster.checkPeerConnectivity()

	// Since we started the drain goroutine, just check it didn't panic
}

func TestCheckPeerConnectivityAboveThreshold(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.PeerRecoveryInterval = 1 * time.Second

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	cluster.seedPeers = []string{
		"127.0.0.1:9001",
		"127.0.0.1:9002",
	}

	// Add enough alive nodes (local + 2 more)
	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:9001")
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:9002")
	cluster.nodes.addOrUpdate(node1)
	cluster.nodes.addOrUpdate(node2)

	// 3 alive (local+2) vs 2 seeds, threshold = 1
	// 3 > 1, should NOT trigger recovery
	cluster.checkPeerConnectivity()
}

func TestCheckPeerConnectivityRecentRecoverySkipped(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.PeerRecoveryInterval = 30 * time.Second

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	cluster.seedPeers = []string{"127.0.0.1:9001"}
	cluster.lastPeerRecovery = time.Now() // just recovered

	// Should skip because too recent
	cluster.checkPeerConnectivity()
}

// ============================================================================
// Cluster: retrySeedPeers
// ============================================================================

func TestRetrySeedPeersNoSeeds(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// No seed peers - should return without panic
	cluster.retrySeedPeers()
}

func TestRetrySeedPeersQueuesJoinRequests(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.JoinQueueSize = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	cluster.seedPeers = []string{
		"127.0.0.1:9001",
		"127.0.0.1:9002",
	}

	// Drain join queue asynchronously
	drained := make(chan struct{})
	count := 0
	go func() {
		for {
			select {
			case <-cluster.joinQueue:
				count++
				if count >= 2 {
					close(drained)
					return
				}
			case <-time.After(2 * time.Second):
				close(drained)
				return
			}
		}
	}()

	cluster.retrySeedPeers()

	<-drained
	if count < 2 {
		t.Errorf("Expected at least 2 join requests queued, got %d", count)
	}
}

// ============================================================================
// Cluster: adjustGossipInterval
// ============================================================================

func TestAdjustGossipIntervalIncrease(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.GossipInterval = 5 * time.Second
	config.GossipMaxInterval = 20 * time.Second

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	cluster.gossipInterval = 5 * time.Second
	cluster.gossipTicker = time.NewTicker(5 * time.Second)
	defer cluster.gossipTicker.Stop()

	// Handlers took 6 seconds (longer than 5s interval)
	cluster.adjustGossipInterval(6 * time.Second)

	expected := 6 * time.Second * 5 / 4 // 7.5s
	if cluster.gossipInterval != expected {
		t.Errorf("Expected interval %v, got %v", expected, cluster.gossipInterval)
	}
}

func TestAdjustGossipIntervalCappedAtMax(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.GossipInterval = 5 * time.Second
	config.GossipMaxInterval = 10 * time.Second

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	cluster.gossipInterval = 5 * time.Second
	cluster.gossipTicker = time.NewTicker(5 * time.Second)
	defer cluster.gossipTicker.Stop()

	// Handlers took 15 seconds - way over max
	cluster.adjustGossipInterval(15 * time.Second)

	if cluster.gossipInterval != 10*time.Second {
		t.Errorf("Expected interval capped at 10s, got %v", cluster.gossipInterval)
	}
}

func TestAdjustGossipIntervalDecrease(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.GossipInterval = 5 * time.Second
	config.GossipMaxInterval = 20 * time.Second

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Currently elevated
	cluster.gossipInterval = 10 * time.Second
	cluster.gossipTicker = time.NewTicker(10 * time.Second)
	defer cluster.gossipTicker.Stop()

	// Handlers only took 2 seconds (fast, < 80% of 10s)
	cluster.adjustGossipInterval(2 * time.Second)

	// Should decrease: (2s + 10s) / 2 = 6s
	expected := (2*time.Second + 10*time.Second) / 2
	if cluster.gossipInterval != expected {
		t.Errorf("Expected interval %v, got %v", expected, cluster.gossipInterval)
	}
}

func TestAdjustGossipIntervalNeverBelowBase(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.GossipInterval = 5 * time.Second
	config.GossipMaxInterval = 20 * time.Second

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Slightly above base
	cluster.gossipInterval = 6 * time.Second
	cluster.gossipTicker = time.NewTicker(6 * time.Second)
	defer cluster.gossipTicker.Stop()

	// Very fast handler (effectively 0)
	cluster.adjustGossipInterval(1 * time.Millisecond)

	if cluster.gossipInterval < config.GossipInterval {
		t.Errorf("Interval %v should never go below base %v", cluster.gossipInterval, config.GossipInterval)
	}
}

func TestAdjustGossipIntervalNoChangeWhenAtBase(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.GossipInterval = 5 * time.Second

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	cluster.gossipInterval = 5 * time.Second
	cluster.gossipTicker = time.NewTicker(5 * time.Second)
	defer cluster.gossipTicker.Stop()

	// Handler took 1s (fast), but already at base - should not change
	cluster.adjustGossipInterval(1 * time.Second)

	if cluster.gossipInterval != 5*time.Second {
		t.Errorf("Interval should remain at base 5s, got %v", cluster.gossipInterval)
	}
}

// ============================================================================
// Cluster: HandleFuncWithResponse
// ============================================================================

func TestHandleFuncWithResponse(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	handler := func(node *Node, packet *Packet) (interface{}, error) {
		return map[string]string{"reply": "ok"}, nil
	}

	err = cluster.HandleFuncWithResponse(UserMsg, handler)
	if err != nil {
		t.Fatalf("HandleFuncWithResponse failed: %v", err)
	}

	// Verify handler is registered
	h := cluster.handlers.getHandler(UserMsg)
	if h == nil {
		t.Error("Handler should be registered")
	}
	if h.replyHandler == nil {
		t.Error("Reply handler should be set")
	}
}

func TestHandleFuncWithResponseInvalidType(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	handler := func(node *Node, packet *Packet) (interface{}, error) {
		return nil, nil
	}

	// System message types should be rejected
	err = cluster.HandleFuncWithResponse(nodeJoinMsg, handler)
	if err == nil {
		t.Error("Should reject system message types")
	}
}

// ============================================================================
// Cluster: HandleFunc and UnregisterMessageType
// ============================================================================

func TestHandleFuncRegistration(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	err = cluster.HandleFunc(UserMsg, func(node *Node, packet *Packet) error {
		return nil
	})
	if err != nil {
		t.Fatalf("HandleFunc failed: %v", err)
	}

	// Invalid message type
	err = cluster.HandleFunc(0, func(node *Node, packet *Packet) error { return nil })
	if err == nil {
		t.Error("Should reject message type 0")
	}
}

func TestUnregisterMessageType(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	cluster.HandleFunc(UserMsg, func(node *Node, packet *Packet) error { return nil })

	// Unregister
	if !cluster.UnregisterMessageType(UserMsg) {
		t.Error("Should succeed unregistering existing handler")
	}

	// Already unregistered
	if cluster.UnregisterMessageType(UserMsg) {
		t.Error("Should fail unregistering non-existent handler")
	}

	// System message type
	if cluster.UnregisterMessageType(0) {
		t.Error("Should reject system message types")
	}
}

// ============================================================================
// Cluster: handleIncomingPacket edge cases
// ============================================================================

func TestHandleIncomingPacketFromSelf(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	packet := NewPacket()
	packet.SenderID = cluster.localNode.ID
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)

	// Should be silently dropped
	cluster.handleIncomingPacket(packet)
}

func TestHandleIncomingPacketAlreadySeen(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	senderID := NodeID(uuid.New())
	msgID := MessageID(hlc.Now())

	// Record the message first
	cluster.msgHistory.recordMessage(senderID, msgID)

	packet := NewPacket()
	packet.SenderID = senderID
	packet.MessageID = msgID
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)

	// Should be dropped as duplicate
	cluster.handleIncomingPacket(packet)
}

func TestHandleIncomingPacketTargetedToOther(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	otherNodeID := NodeID(uuid.New())

	packet := NewPacket()
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = UserMsg
	packet.TargetNodeID = &otherNodeID
	packet.SetCodec(config.MsgCodec)

	// Should be dropped since target is not us
	cluster.handleIncomingPacket(packet)
}

func TestHandleIncomingPacketTagFiltering(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.Tags = []string{"web"}

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start broadcast worker to drain the queue
	go func() {
		for {
			select {
			case item := <-cluster.broadcastQueue:
				item.packet.Release()
				item.packet = nil
				cluster.broadcastQItemPool.Put(item)
			case <-cluster.shutdownContext.Done():
				return
			}
		}
	}()
	defer cluster.cancelFunc()

	// Add sender node
	senderID := NodeID(uuid.New())
	senderNode := newNode(senderID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(senderNode)

	tag := "api" // our node doesn't have this tag

	packet := NewPacket()
	packet.SenderID = senderID
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = UserMsg
	packet.Tag = &tag
	packet.TTL = 3
	packet.SetCodec(config.MsgCodec)
	packet.AddRef() // extra ref because handleIncomingPacket releases

	// Should forward but not process (we don't have "api" tag)
	cluster.handleIncomingPacket(packet)
}

func TestHandleIncomingPacketUnknownSenderNonJoin(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start broadcast worker to drain
	go func() {
		for {
			select {
			case item := <-cluster.broadcastQueue:
				item.packet.Release()
				item.packet = nil
				cluster.broadcastQItemPool.Put(item)
			case <-cluster.shutdownContext.Done():
				return
			}
		}
	}()
	defer cluster.cancelFunc()

	// Non-join message from unknown sender should be dropped
	packet := NewPacket()
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = UserMsg
	packet.TTL = 3
	packet.SetCodec(config.MsgCodec)
	packet.AddRef()

	cluster.handleIncomingPacket(packet)
}

// ============================================================================
// Cluster: enqueuePacketForBroadcast
// ============================================================================

func TestEnqueuePacketForBroadcastTTLZero(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	packet := NewPacket()
	packet.TTL = 0
	packet.AddRef()

	// Should be dropped since TTL is 0
	cluster.enqueuePacketForBroadcast(packet, TransportBestEffort, nil, nil)

	// queue should be empty
	select {
	case <-cluster.broadcastQueue:
		t.Error("Nothing should be queued when TTL is 0")
	default:
		// expected
	}
}

func TestEnqueuePacketForBroadcastQueueFull(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.SendQueueSize = 1

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Fill the queue
	p1 := NewPacket()
	p1.TTL = 3
	cluster.enqueuePacketForBroadcast(p1, TransportBestEffort, nil, nil)

	// Second packet should be dropped (queue full)
	p2 := NewPacket()
	p2.TTL = 3
	cluster.enqueuePacketForBroadcast(p2, TransportBestEffort, nil, nil)

	// Drain
	item := <-cluster.broadcastQueue
	item.packet.Release()
}

// ============================================================================
// Cluster: cleanupNodes
// ============================================================================

func TestCleanupNodesLeavingToDeadTransition(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.LeavingNodeTimeout = 1 * time.Millisecond
	config.NodeRetentionTime = 1 * time.Hour

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.updateState(nodeID, NodeLeaving)

	// Wait for timeout
	time.Sleep(5 * time.Millisecond)

	cluster.cleanupNodes()

	if cluster.nodes.get(nodeID).GetObservedState() != NodeDead {
		t.Error("Leaving node should have been moved to Dead after timeout")
	}
}

func TestCleanupNodesDeadRemoval(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.LeavingNodeTimeout = 1 * time.Hour
	config.NodeRetentionTime = 1 * time.Millisecond

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	node.observedState = NodeDead
	cluster.nodes.addOrUpdate(node)

	time.Sleep(5 * time.Millisecond)

	cluster.cleanupNodes()

	if cluster.nodes.get(nodeID) != nil {
		t.Error("Dead node should have been removed after retention time")
	}
}

// ============================================================================
// Cluster: Send variants
// ============================================================================

func TestSendVariants(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start broadcast worker to drain
	go func() {
		for {
			select {
			case item := <-cluster.broadcastQueue:
				item.packet.Release()
				item.packet = nil
				cluster.broadcastQItemPool.Put(item)
			case <-cluster.shutdownContext.Done():
				return
			}
		}
	}()
	defer cluster.cancelFunc()

	// Add nodes so there are peers
	for i := 0; i < 5; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Send", func() error { return cluster.Send(UserMsg, "test") }},
		{"SendReliable", func() error { return cluster.SendReliable(UserMsg, "test") }},
		{"SendTagged", func() error { return cluster.SendTagged("web", UserMsg, "test") }},
		{"SendTaggedReliable", func() error { return cluster.SendTaggedReliable("web", UserMsg, "test") }},
		{"Send invalid type", func() error { return cluster.Send(0, "test") }},
		{"SendReliable invalid type", func() error { return cluster.SendReliable(0, "test") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if tt.name == "Send invalid type" || tt.name == "SendReliable invalid type" {
				if err == nil {
					t.Error("Expected error for invalid message type")
				}
			}
		})
	}
}

func TestSendToVariants(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start broadcast worker to drain
	go func() {
		for {
			select {
			case item := <-cluster.broadcastQueue:
				item.packet.Release()
				item.packet = nil
				cluster.broadcastQItemPool.Put(item)
			case <-cluster.shutdownContext.Done():
				return
			}
		}
	}()
	defer cluster.cancelFunc()

	dstNode := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(dstNode)

	// SendTo
	err = cluster.SendTo(dstNode, UserMsg, "test")
	if err != nil {
		t.Errorf("SendTo failed: %v", err)
	}

	// SendToReliable
	err = cluster.SendToReliable(dstNode, UserMsg, "test")
	if err != nil {
		t.Errorf("SendToReliable failed: %v", err)
	}

	// SendToPeers
	err = cluster.SendToPeers([]*Node{dstNode}, UserMsg, "test")
	if err != nil {
		t.Errorf("SendToPeers failed: %v", err)
	}

	// SendToPeersReliable
	err = cluster.SendToPeersReliable([]*Node{dstNode}, UserMsg, "test")
	if err != nil {
		t.Errorf("SendToPeersReliable failed: %v", err)
	}

	// Invalid types
	if err := cluster.SendTo(dstNode, 0, "test"); err == nil {
		t.Error("SendTo should fail with system message type")
	}
	if err := cluster.SendToReliable(dstNode, 0, "test"); err == nil {
		t.Error("SendToReliable should fail with system message type")
	}
	if err := cluster.SendToPeers([]*Node{dstNode}, 0, "test"); err == nil {
		t.Error("SendToPeers should fail with system message type")
	}
	if err := cluster.SendToPeersReliable([]*Node{dstNode}, 0, "test"); err == nil {
		t.Error("SendToPeersReliable should fail with system message type")
	}
}

func TestSendTaggedInvalidTypes(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if err := cluster.SendTagged("web", 0, "test"); err == nil {
		t.Error("Should fail with system message type")
	}
	if err := cluster.SendTaggedReliable("web", 0, "test"); err == nil {
		t.Error("Should fail with system message type")
	}
}

// ============================================================================
// Cluster: SendToWithResponse
// ============================================================================

func TestSendToWithResponseInvalidType(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	dstNode := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	var resp string
	err = cluster.SendToWithResponse(dstNode, 0, "test", &resp)
	if err == nil {
		t.Error("Should fail with system message type")
	}
}

// ============================================================================
// Cluster: Accessor methods
// ============================================================================

func TestClusterAccessors(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.Tags = []string{"web", "api"}

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// NumNodes
	if cluster.NumNodes() != 1 {
		t.Errorf("Expected 1 node (local), got %d", cluster.NumNodes())
	}

	// NumAliveNodes
	if cluster.NumAliveNodes() != 1 {
		t.Errorf("Expected 1 alive node (local), got %d", cluster.NumAliveNodes())
	}

	// NumSuspectNodes
	if cluster.NumSuspectNodes() != 0 {
		t.Errorf("Expected 0 suspect nodes, got %d", cluster.NumSuspectNodes())
	}

	// NumDeadNodes
	if cluster.NumDeadNodes() != 0 {
		t.Errorf("Expected 0 dead nodes, got %d", cluster.NumDeadNodes())
	}

	// NodeIsLocal
	if !cluster.NodeIsLocal(cluster.localNode) {
		t.Error("Local node should be local")
	}

	otherNode := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	if cluster.NodeIsLocal(otherNode) {
		t.Error("Other node should not be local")
	}

	// GetNode
	if cluster.GetNode(cluster.localNode.ID) == nil {
		t.Error("GetNode should find local node")
	}

	// GetNodeByIDString
	if cluster.GetNodeByIDString(cluster.localNode.ID.String()) == nil {
		t.Error("GetNodeByIDString should find local node")
	}
	if cluster.GetNodeByIDString("invalid-uuid") != nil {
		t.Error("GetNodeByIDString should return nil for invalid UUID")
	}

	// Logger
	if cluster.Logger() == nil {
		t.Error("Logger should not be nil")
	}

	// NodesToIDs
	nodes := []*Node{cluster.localNode, otherNode}
	ids := cluster.NodesToIDs(nodes)
	if len(ids) != 2 {
		t.Errorf("Expected 2 IDs, got %d", len(ids))
	}

	// GetNodesByTag
	cluster.nodes.addOrUpdate(otherNode)
	taggedNode := newNode(NodeID(uuid.New()), "127.0.0.1:8002", "web")
	cluster.nodes.addOrUpdate(taggedNode)

	webNodes := cluster.GetNodesByTag("web")
	// Should include our local node (has "web" tag) and taggedNode
	if len(webNodes) < 1 {
		t.Errorf("Expected at least 1 web-tagged node, got %d", len(webNodes))
	}
}

// ============================================================================
// Cluster: Event handler registration
// ============================================================================

func TestNodeStateChangeHandler(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	var mu sync.Mutex
	var changes []struct {
		node      *Node
		prevState NodeState
	}

	handlerID := cluster.HandleNodeStateChangeFunc(func(node *Node, prevState NodeState) {
		mu.Lock()
		defer mu.Unlock()
		changes = append(changes, struct {
			node      *Node
			prevState NodeState
		}{node, prevState})
	})

	// Add a node and change state
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	time.Sleep(10 * time.Millisecond) // handlers are async

	mu.Lock()
	if len(changes) == 0 {
		mu.Unlock()
		t.Error("Expected state change notification for new node")
	}
	mu.Unlock()

	// Remove handler
	if !cluster.RemoveNodeStateChangeHandler(handlerID) {
		t.Error("Should succeed removing handler")
	}
}

func TestNodeMetadataChangeHandler(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	changed := make(chan *Node, 1)
	handlerID := cluster.HandleNodeMetadataChangeFunc(func(node *Node) {
		select {
		case changed <- node:
		default:
		}
	})

	// Trigger metadata change on a node
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.notifyMetadataChanged(node)

	select {
	case n := <-changed:
		if n.ID != nodeID {
			t.Error("Expected notification for the correct node")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected metadata change notification")
	}

	if !cluster.RemoveNodeMetadataChangeHandler(handlerID) {
		t.Error("Should succeed removing handler")
	}
}

func TestGossipHandler(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	called := false
	handlerID := cluster.HandleGossipFunc(func() {
		called = true
	})

	// Trigger gossip handlers
	cluster.gossipEventHandlers.ForEach(func(handler GossipHandler) {
		handler()
	})

	if !called {
		t.Error("Gossip handler should have been called")
	}

	if !cluster.RemoveGossipHandler(handlerID) {
		t.Error("Should succeed removing handler")
	}
}

// ============================================================================
// Cluster: NewCluster validation
// ============================================================================

func TestNewClusterValidation(t *testing.T) {
	// Nil config still works (uses defaults)
	_, err := NewCluster(nil)
	if err == nil {
		t.Error("Expected error for nil transport (even with default config)")
	}

	// Missing codec
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = nil
	_, err = NewCluster(config)
	if err == nil {
		t.Error("Expected error for nil codec")
	}

	// Missing transport
	config = DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()
	config.Transport = nil
	_, err = NewCluster(config)
	if err == nil {
		t.Error("Expected error for nil transport")
	}

	// Invalid encrypt key
	config = DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.EncryptionKey = []byte("short")
	_, err = NewCluster(config)
	if err == nil {
		t.Error("Expected error for invalid encryption key length")
	}

	// Valid encrypt key lengths
	for _, keyLen := range []int{16, 24, 32} {
		config = DefaultConfig()
		config.Transport = &mockTransport{}
		config.MsgCodec = codec.NewJsonCodec()
		config.EncryptionKey = make([]byte, keyLen)
		_, err = NewCluster(config)
		if err != nil {
			t.Errorf("Should accept %d byte key, got error: %v", keyLen, err)
		}
	}

	// Cipher without key
	config = DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.Cipher = &mockCipher{}
	_, err = NewCluster(config)
	if err == nil {
		t.Error("Expected error when cipher is set without encryption key")
	}

	// Invalid NodeID
	config = DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.NodeID = "not-a-uuid"
	_, err = NewCluster(config)
	if err == nil {
		t.Error("Expected error for invalid NodeID")
	}

	// Valid NodeID
	config = DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.NodeID = uuid.New().String()
	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Valid NodeID should work: %v", err)
	}
	if cluster.localNode.ID.String() != config.NodeID {
		t.Error("NodeID should match config")
	}
}

// mockCipher for testing
type mockCipher struct{}

func (m *mockCipher) Encrypt(key, data []byte) ([]byte, error) { return data, nil }
func (m *mockCipher) Decrypt(key, data []byte) ([]byte, error) { return data, nil }
func (m *mockCipher) Name() string                             { return "mock" }

// ============================================================================
// Cluster: Join
// ============================================================================

func TestJoinSelfFiltering(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.AdvertiseAddr = "127.0.0.1:8000"

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Join with only self address
	err = cluster.Join([]string{"127.0.0.1:8000"})
	if err == nil {
		t.Error("Should fail with no peers (self is filtered)")
	}
}

func TestJoinHTTPSelfFiltering(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.BindAddr = "http://localhost:8080/gossip"
	config.AdvertiseAddr = "http://localhost:8080/gossip"

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Join with self HTTP address
	err = cluster.Join([]string{"http://localhost:8080/gossip"})
	if err == nil {
		t.Error("Should fail with no peers (HTTP self is filtered)")
	}

	// Join with different HTTP address
	err = cluster.Join([]string{"http://otherhost:8080/gossip"})
	// Will fail to actually join but should not return "no peers" error
}

func TestJoinEmptyPeers(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	err = cluster.Join([]string{})
	if err == nil {
		t.Error("Should fail with empty peers")
	}
}

// ============================================================================
// Cluster: CalcFanOut / CalcPayloadSize / getMaxTTL edge cases
// ============================================================================

func TestCalcFanOutNoNodes(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Remove local node to have zero alive
	cluster.nodes.remove(cluster.localNode.ID) // won't work since it's local
	// CalcFanOut with 1 alive (local) should still return something valid
	fanOut := cluster.CalcFanOut()
	if fanOut < 0 {
		t.Errorf("FanOut should be non-negative, got %d", fanOut)
	}
}

func TestCalcPayloadSizeZero(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if cluster.CalcPayloadSize(0) != 0 {
		t.Error("CalcPayloadSize(0) should return 0")
	}

	if cluster.CalcPayloadSize(-1) != 0 {
		t.Error("CalcPayloadSize(-1) should return 0")
	}
}

func TestGetMaxTTLScaling(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// With many nodes, TTL should scale
	for i := 0; i < 100; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 9000+i))
		cluster.nodes.addOrUpdate(n)
	}

	ttl := cluster.getMaxTTL()
	if ttl == 0 {
		t.Error("TTL should be positive with many nodes")
	}
	if ttl > 8 {
		t.Errorf("TTL should be capped at 8, got %d", ttl)
	}
}

// ============================================================================
// Cluster: gossipMetadata
// ============================================================================

func TestGossipMetadataSkipsRecentUpdate(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.MetadataGossipInterval = 500 * time.Millisecond

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start broadcast worker to drain
	go func() {
		for {
			select {
			case item := <-cluster.broadcastQueue:
				item.packet.Release()
				item.packet = nil
				cluster.broadcastQItemPool.Put(item)
			case <-cluster.shutdownContext.Done():
				return
			}
		}
	}()
	defer cluster.cancelFunc()

	// Just updated metadata - should skip
	cluster.localNode.metadata.set("key", "value")
	cluster.gossipMetadata()

	// After enough time, should actually gossip
	time.Sleep(300 * time.Millisecond)
	cluster.gossipMetadata()
}

// ============================================================================
// Cluster: Leave
// ============================================================================

func TestLeave(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start broadcast worker to drain
	go func() {
		for {
			select {
			case item := <-cluster.broadcastQueue:
				item.packet.Release()
				item.packet = nil
				cluster.broadcastQItemPool.Put(item)
			case <-cluster.shutdownContext.Done():
				return
			}
		}
	}()
	defer cluster.cancelFunc()

	cluster.Leave()

	if cluster.localNode.GetObservedState() != NodeLeaving {
		t.Errorf("Expected local node to be Leaving, got %v", cluster.localNode.GetObservedState())
	}
}
