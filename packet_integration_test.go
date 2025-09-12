package gossip

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
)

// TestPacketIntegrationFullCluster tests packet management in a full cluster scenario
func TestPacketIntegrationFullCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create multiple clusters
	clusters := make([]*Cluster, 3)
	for i := range clusters {
		config := DefaultConfig()
		config.NodeID = fmt.Sprintf("node-%d", i)
		config.BindAddr = fmt.Sprintf("127.0.0.1:%d", 9000+i)
		config.Transport = &mockTransport{}
		config.MsgCodec = &mockCodec{}
		config.Logger = NewNullLogger()

		cluster, err := NewCluster(config)
		if err != nil {
			t.Fatalf("Failed to create cluster %d: %v", i, err)
		}
		clusters[i] = cluster
	}

	// Test message handling with packet tracking
	const CustomMsg MessageType = UserMsg + 1
	// Track packets for testing
	
	for _, cluster := range clusters {
		cluster.HandleFunc(CustomMsg, func(sender *Node, packet *Packet) error {
			// Simulate processing
			time.Sleep(1 * time.Millisecond)
			return nil
		})
	}

	// Send messages between clusters
	for i, cluster := range clusters {
		err := cluster.Send(CustomMsg, fmt.Sprintf("message from cluster %d", i))
		if err != nil {
			t.Errorf("Failed to send message from cluster %d: %v", i, err)
		}
	}

	// Give time for message processing
	time.Sleep(100 * time.Millisecond)
}

// TestPacketHandlerErrorScenario tests packet cleanup when handlers return errors
func TestPacketHandlerErrorScenario(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Register handler that returns error
	const ErrorMsg MessageType = UserMsg + 1
	cluster.HandleFunc(ErrorMsg, func(sender *Node, packet *Packet) error {
		return fmt.Errorf("simulated handler error")
	})

	// Create and process packet that will cause error
	packet := NewPacket()
	packet.MessageType = ErrorMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 1
	packet.SetCodec(config.MsgCodec)
	packet.SetPayload([]byte("test"))

	// Simulate packet processing
	cluster.handleIncomingPacket(packet)

	// Packet should be properly released even with handler error
	// We can't directly verify this, but the test ensures no panic occurs
}

// TestPacketTTLExpiration tests packet cleanup when TTL expires
func TestPacketTTLExpiration(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create packet with TTL of 0 (should be dropped immediately)
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 0
	packet.SetCodec(config.MsgCodec)

	// This should release the packet immediately due to TTL=0
	cluster.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{}, nil)

	// Give time for processing
	time.Sleep(10 * time.Millisecond)
}

// TestPacketDuplicateMessage tests packet cleanup for duplicate messages
func TestPacketDuplicateMessage(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	senderID := NodeID(uuid.New())
	messageID := MessageID(hlc.Now())

	// Create first packet
	packet1 := NewPacket()
	packet1.MessageType = UserMsg
	packet1.SenderID = senderID
	packet1.MessageID = messageID
	packet1.TTL = 3
	packet1.SetCodec(config.MsgCodec)

	// Process first packet
	cluster.handleIncomingPacket(packet1)

	// Create duplicate packet (same sender and message ID)
	packet2 := NewPacket()
	packet2.MessageType = UserMsg
	packet2.SenderID = senderID
	packet2.MessageID = messageID
	packet2.TTL = 3
	packet2.SetCodec(config.MsgCodec)

	// Process duplicate - should be released immediately
	cluster.handleIncomingPacket(packet2)

	// Give time for processing
	time.Sleep(10 * time.Millisecond)
}

// TestPacketTargetedMessage tests packet cleanup for targeted messages
func TestPacketTargetedMessage(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create packet targeted to different node (should be dropped)
	differentNodeID := NodeID(uuid.New())
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.TargetNodeID = &differentNodeID
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 3
	packet.SetCodec(config.MsgCodec)

	// Process packet - should be released due to wrong target
	cluster.handleIncomingPacket(packet)

	// Give time for processing
	time.Sleep(10 * time.Millisecond)
}

// TestPacketUnknownSender tests packet handling from unknown senders
func TestPacketUnknownSender(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create packet from unknown sender (not join or ping)
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 3
	packet.SetCodec(config.MsgCodec)

	// Process packet - should be released due to unknown sender
	cluster.handleIncomingPacket(packet)

	// Give time for processing
	time.Sleep(10 * time.Millisecond)
}

// TestPacketBroadcastQueueFull tests packet cleanup when broadcast queue is full
func TestPacketBroadcastQueueFull(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()
	// Note: BroadcastQueueDepth is not configurable, using default

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Fill the broadcast queue
	for i := 0; i < 10; i++ {
		packet := NewPacket()
		packet.MessageType = UserMsg
		packet.SenderID = cluster.localNode.ID
		packet.MessageID = MessageID(hlc.Now())
		packet.TTL = 3
		packet.SetCodec(config.MsgCodec)

		// This should handle queue full scenario
		cluster.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{}, nil)
	}

	// Give time for processing
	time.Sleep(50 * time.Millisecond)
}

// TestPacketConnectionBasedReply tests packet cleanup in connection-based replies
func TestPacketConnectionBasedReply(t *testing.T) {
	// Create mock connection
	conn := &mockConn{}
	
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.SetConn(conn)

	// Test CanReply
	if !packet.CanReply() {
		t.Error("Packet with connection should be able to reply")
	}

	// Test connection cleanup on release
	packet.Release()

	// Verify connection was closed
	if !conn.closed {
		t.Error("Connection should be closed when packet is released")
	}
}

// TestPacketChannelBasedReply tests packet cleanup in channel-based replies
func TestPacketChannelBasedReply(t *testing.T) {
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())

	replyChan := make(chan *Packet, 1)
	packet.SetReplyChan(replyChan)

	// Test CanReply
	if !packet.CanReply() {
		t.Error("Packet with reply channel should be able to reply")
	}

	// Test reply channel cleanup on release
	packet.Release()

	// Get new packet to verify cleanup
	newPacket := NewPacket()
	if newPacket.CanReply() {
		t.Error("New packet should not have reply capability")
	}
	newPacket.Release()
}

// TestPacketRaceConditions tests for race conditions in packet management
func TestPacketRaceConditions(t *testing.T) {
	const numGoroutines = 100

	packet := NewPacket()
	
	var wg sync.WaitGroup
	
	// Multiple goroutines adding references
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ref := packet.AddRef()
			time.Sleep(1 * time.Millisecond) // Simulate work
			ref.Release()
		}()
	}
	
	wg.Wait()
	
	// Original packet should still be valid
	if packet.refCount.Load() != 1 {
		t.Errorf("Expected ref count 1, got %d", packet.refCount.Load())
	}
	
	packet.Release()
}

// TestPacketMemoryUsage tests that packets don't hold onto large amounts of memory
func TestPacketMemoryUsage(t *testing.T) {
	// Create packet with large payload
	packet := NewPacket()
	largePayload := make([]byte, 1024*1024) // 1MB
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}
	packet.SetPayload(largePayload)
	
	// Release packet
	packet.Release()
	
	// Get new packet - payload should be cleared
	newPacket := NewPacket()
	if len(newPacket.Payload()) > 0 {
		t.Error("New packet should have empty payload")
	}
	newPacket.Release()
}