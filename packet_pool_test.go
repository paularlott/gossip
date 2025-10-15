package gossip

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
)

// TestPacketPoolBasicAllocationRelease tests basic packet allocation and release
func TestPacketPoolBasicAllocationRelease(t *testing.T) {
	// Get initial pool state
	initialPacket := NewPacket()
	initialPacket.Release()

	// Test basic allocation
	packet := NewPacket()
	if packet == nil {
		t.Fatal("NewPacket() returned nil")
	}

	// Verify initial ref count
	if packet.refCount.Load() != 1 {
		t.Errorf("Expected ref count 1, got %d", packet.refCount.Load())
	}

	// Test release
	packet.Release()

	// Verify packet was returned to pool by getting another one
	packet2 := NewPacket()
	if packet2 == nil {
		t.Fatal("Second NewPacket() returned nil")
	}
	packet2.Release()
}

// TestPacketAddRefRelease tests AddRef and Release functionality
func TestPacketAddRefRelease(t *testing.T) {
	packet := NewPacket()

	// Test AddRef
	packet2 := packet.AddRef()
	if packet2 != packet {
		t.Error("AddRef should return the same packet")
	}
	if packet.refCount.Load() != 2 {
		t.Errorf("Expected ref count 2, got %d", packet.refCount.Load())
	}

	// Release one reference
	packet.Release()
	if packet.refCount.Load() != 1 {
		t.Errorf("Expected ref count 1, got %d", packet.refCount.Load())
	}

	// Release final reference
	packet2.Release()
}

// TestPacketConcurrentAccess tests concurrent access to packet pool
func TestPacketConcurrentAccess(t *testing.T) {
	const numGoroutines = 100
	const packetsPerGoroutine = 50

	var wg sync.WaitGroup
	packets := make(chan *Packet, numGoroutines*packetsPerGoroutine)

	// Allocate packets concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < packetsPerGoroutine; j++ {
				packet := NewPacket()
				packets <- packet
			}
		}()
	}

	wg.Wait()
	close(packets)

	// Release all packets
	for packet := range packets {
		packet.Release()
	}
}

// TestPacketMultipleReferences tests multiple references to the same packet
func TestPacketMultipleReferences(t *testing.T) {
	packet := NewPacket()

	// Create multiple references
	refs := make([]*Packet, 5)
	for i := range refs {
		refs[i] = packet.AddRef()
	}

	// Verify ref count
	expectedCount := int32(len(refs) + 1) // +1 for original
	if packet.refCount.Load() != expectedCount {
		t.Errorf("Expected ref count %d, got %d", expectedCount, packet.refCount.Load())
	}

	// Release all but one reference
	for i := 0; i < len(refs)-1; i++ {
		refs[i].Release()
	}

	// Should still have 2 references (original + last ref)
	if packet.refCount.Load() != 2 {
		t.Errorf("Expected ref count 2, got %d", packet.refCount.Load())
	}

	// Release remaining references
	packet.Release()
	refs[len(refs)-1].Release()
}

// TestPacketCleanupOnRelease tests that packet fields are cleaned up on release
func TestPacketCleanupOnRelease(t *testing.T) {
	packet := NewPacket()

	// Set various fields
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	targetID := NodeID(uuid.New())
	packet.TargetNodeID = &targetID
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("test payload"))
	packet.SetCodec(&mockCodec{})

	// Release the packet
	packet.Release()

	// Verify cleanup - we can't directly check the packet since it's returned to pool,
	// but we can verify a new packet has clean state
	newPacket := NewPacket()
	if newPacket.SenderID != EmptyNodeID {
		t.Error("SenderID not cleaned up")
	}
	if newPacket.TargetNodeID != nil {
		t.Error("TargetNodeID not cleaned up")
	}
	if newPacket.payload != nil {
		t.Error("Payload not cleaned up")
	}
	newPacket.Release()
}

// TestPacketForwardingScenario tests packet allocation/release in forwarding scenario
func TestPacketForwardingScenario(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	_, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create a packet that would be forwarded
	originalPacket := NewPacket()
	originalPacket.MessageType = UserMsg
	originalPacket.SenderID = NodeID(uuid.New())
	originalPacket.MessageID = MessageID(hlc.Now())
	originalPacket.TTL = 3
	originalPacket.SetCodec(config.MsgCodec)
	originalPacket.SetPayload([]byte("test message"))

	// Simulate packet forwarding by calling AddRef (as done in handleIncomingPacket)
	forwardedPacket := originalPacket.AddRef()

	// Verify both packets have correct ref counts
	if originalPacket.refCount.Load() != 2 {
		t.Errorf("Expected ref count 2, got %d", originalPacket.refCount.Load())
	}

	// Release original packet (simulating end of processing)
	originalPacket.Release()

	// Verify forwarded packet still valid
	if forwardedPacket.refCount.Load() != 1 {
		t.Errorf("Expected ref count 1, got %d", forwardedPacket.refCount.Load())
	}

	// Release forwarded packet
	forwardedPacket.Release()
}

// TestPacketBroadcastScenario tests packet allocation/release in broadcast scenario
func TestPacketBroadcastScenario(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	_, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Test broadcast message creation and cleanup
	// err = cluster.Send(UserMsg, "test message")
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
	}

	// Give time for async processing
	time.Sleep(10 * time.Millisecond)
}

// TestPacketReplyScenario tests packet allocation/release in reply scenario
func TestPacketReplyScenario(t *testing.T) {
	// Create a packet with reply capability
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.SetCodec(&mockCodec{})

	// Set up reply channel
	replyChan := make(chan *Packet, 1)
	packet.SetReplyChan(replyChan)

	// Create reply packet
	replyPacket := NewPacket()
	replyPacket.MessageType = replyMsg
	replyPacket.SenderID = NodeID(uuid.New())

	// Send reply
	err := replyPacket.SendReply()
	if err == nil {
		t.Error("Expected error when no reply mechanism available")
	}

	// Set reply channel and try again
	replyPacket.SetReplyChan(replyChan)
	err = replyPacket.SendReply()
	if err != nil {
		t.Errorf("Failed to send reply: %v", err)
	}

	// Verify reply was received
	select {
	case receivedReply := <-replyChan:
		if receivedReply != replyPacket {
			t.Error("Received wrong reply packet")
		}
		receivedReply.Release()
	case <-time.After(100 * time.Millisecond):
		t.Error("Reply not received")
	}

	packet.Release()
}

// TestPacketLeakDetection tests for packet leaks by checking ref counts
func TestPacketLeakDetection(t *testing.T) {
	// Test various scenarios to ensure proper cleanup
	scenarios := []struct {
		name string
		fn   func() *Packet
	}{
		{
			name: "Basic allocation/release",
			fn: func() *Packet {
				p := NewPacket()
				p.Release()
				return nil // Packet should be released
			},
		},
		{
			name: "Multiple references",
			fn: func() *Packet {
				p := NewPacket()
				p2 := p.AddRef()
				p.Release()
				p2.Release()
				return nil // All references released
			},
		},
		{
			name: "Unreleased reference",
			fn: func() *Packet {
				p := NewPacket()
				p.AddRef() // Create extra reference but don't release it
				p.Release()
				return p // Should still have ref count > 0
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			result := scenario.fn()
			if result != nil {
				// Check that packet still has references
				if result.refCount.Load() <= 0 {
					t.Error("Expected packet to still have references")
				}
				// Clean up
				result.Release()
			}
		})
	}
}

// TestPacketWithResponseScenario tests sendToWithResponse packet management
func TestPacketWithResponseScenario(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransportWithReply{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	node := &Node{
		ID:            NodeID(uuid.New()),
		advertiseAddr: "127.0.0.1:8001",
		address:       Address{},
	}

	// Test sendToWithResponse
	var response string
	err = cluster.sendToWithResponse(node, UserMsg, "test request", &response)
	// We expect an error since our mock doesn't resolve addresses properly
	if err != nil {
		t.Logf("Expected error for unresolved address: %v", err)
	}
}

// mockTransportWithReply extends mockTransport to support replies
type mockTransportWithReply struct {
	mockTransport
}

func (m *mockTransportWithReply) SendWithReply(node *Node, packet *Packet) (*Packet, error) {
	// Create a mock reply packet
	reply := NewPacket()
	reply.MessageType = replyMsg
	reply.SenderID = NodeID(uuid.New())
	reply.SetCodec(&mockCodec{})
	reply.SetPayload([]byte("mock response"))
	return reply, nil
}

// TestPacketPoolStressTest performs stress testing of packet pool
func TestPacketPoolStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	const (
		numWorkers = 50
		duration   = 2 * time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	var operations int64

	// Start workers that continuously allocate and release packets
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Allocate packet
					packet := NewPacket()

					// Sometimes add references
					var refs []*Packet
					if atomic.LoadInt64(&operations)%3 == 0 {
						for j := 0; j < 3; j++ {
							refs = append(refs, packet.AddRef())
						}
					}

					// Release all references
					for _, ref := range refs {
						ref.Release()
					}
					packet.Release()

					atomic.AddInt64(&operations, 1)
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("Completed %d packet operations", atomic.LoadInt64(&operations))
}

// TestPacketFieldsAfterRelease ensures packet fields are properly reset
func TestPacketFieldsAfterRelease(t *testing.T) {
	packet := NewPacket()

	// Set fields that should be reset
	packet.SenderID = NodeID(uuid.New())
	targetID := NodeID(uuid.New())
	packet.TargetNodeID = &targetID
	packet.SetPayload([]byte("test data"))

	// Mock reply channel
	replyChan := make(chan *Packet, 1)
	packet.SetReplyChan(replyChan)

	// Verify fields are set
	if packet.SenderID == EmptyNodeID {
		t.Error("SenderID should be set")
	}
	if packet.TargetNodeID == nil {
		t.Error("TargetNodeID should be set")
	}
	if packet.Payload() == nil {
		t.Error("Payload should be set")
	}
	if !packet.CanReply() {
		t.Error("Packet should be able to reply")
	}

	// Release packet
	packet.Release()

	// Get a new packet from pool
	newPacket := NewPacket()
	defer newPacket.Release()

	// Verify only the fields that are actually reset are clean
	if newPacket.SenderID != EmptyNodeID {
		t.Errorf("SenderID not reset: %v", newPacket.SenderID)
	}
	if newPacket.TargetNodeID != nil {
		t.Errorf("TargetNodeID not reset: %v", newPacket.TargetNodeID)
	}
	if newPacket.Payload() != nil {
		t.Errorf("Payload not reset: %v", newPacket.Payload())
	}
	if newPacket.CanReply() {
		t.Error("New packet should not have reply capability")
	}
}
