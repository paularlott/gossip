package gossip

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Mock implementations for testing
type mockTransport struct {
	ch chan *Packet
}

func (m *mockTransport) Name() string { return "mock" }
func (m *mockTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	m.ch = make(chan *Packet)
	return nil
}
func (m *mockTransport) Send(transportType TransportType, node *Node, packet *Packet) error {
	return nil
}
func (m *mockTransport) SendWithReply(node *Node, packet *Packet) (*Packet, error) { return nil, nil }
func (m *mockTransport) PacketChannel() chan *Packet {
	if m.ch == nil {
		m.ch = make(chan *Packet)
	}
	return m.ch
}

func TestNodeCleanup(t *testing.T) {
	config := DefaultConfig()
	config.NodeCleanupInterval = 100 * time.Millisecond
	config.LeavingNodeTimeout = 50 * time.Millisecond
	config.NodeRetentionTime = 100 * time.Millisecond
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create test nodes
	leavingNode := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	leavingNode.observedState = NodeLeaving
	cluster.nodes.addOrUpdate(leavingNode)

	deadNode := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	deadNode.observedState = NodeDead
	cluster.nodes.addOrUpdate(deadNode)

	// Verify nodes exist
	if cluster.nodes.getLeavingCount() != 1 {
		t.Errorf("Expected 1 leaving node, got %d", cluster.nodes.getLeavingCount())
	}
	if cluster.nodes.getDeadCount() != 1 {
		t.Errorf("Expected 1 dead node, got %d", cluster.nodes.getDeadCount())
	}

	// Wait for leaving node timeout
	time.Sleep(60 * time.Millisecond)
	cluster.cleanupNodes()

	// Leaving node should now be dead
	if cluster.nodes.getLeavingCount() != 0 {
		t.Errorf("Expected 0 leaving nodes, got %d", cluster.nodes.getLeavingCount())
	}
	if cluster.nodes.getDeadCount() != 2 {
		t.Errorf("Expected 2 dead nodes, got %d", cluster.nodes.getDeadCount())
	}

	// Wait for retention timeout
	time.Sleep(110 * time.Millisecond)
	cluster.cleanupNodes()

	// Dead nodes should be removed
	if cluster.nodes.getDeadCount() != 0 {
		t.Errorf("Expected 0 dead nodes, got %d", cluster.nodes.getDeadCount())
	}
}

func TestNodeRemoveFlag(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Track removal notifications
	var removedNode *Node
	cluster.HandleNodeStateChangeFunc(func(node *Node, prevState NodeState) {
		if node.Removed() {
			removedNode = node
		}
	})

	// Create and add test node
	testNode := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	//testNode.state = NodeDead
	cluster.nodes.addOrUpdate(testNode)

	// Remove the node
	cluster.nodes.remove(testNode.ID)

	// Give time for async notification
	time.Sleep(10 * time.Millisecond)

	// Verify removal flag was set and notification sent
	if removedNode == nil {
		t.Error("Expected removal notification")
	}
	if removedNode != nil && !removedNode.Removed() {
		t.Error("Expected Remove flag to be true")
	}
}
