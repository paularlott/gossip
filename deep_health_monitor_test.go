package gossip

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

// ============================================================================
// HealthMonitor: processHealthCheck - DirectPing
// ============================================================================

func TestProcessHealthCheckDirectPingSuccess(t *testing.T) {
	// Create a cluster with a mock transport that can respond to pings
	config := DefaultConfig()
	mt := &replyMockTransport{
		pongResponse: &pongMessage{
			NodeID:            NodeID(uuid.New()),
			AdvertiseAddr:     "127.0.0.1:8001",
			MetadataTimestamp: hlc.Now(),
			Metadata:          map[string]interface{}{},
			NodeState:         NodeAlive,
		},
		codec: codec.NewJSONCodec(),
	}
	config.Transport = mt
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Process a direct ping check
	task := HealthCheckTask{
		NodeID:    nodeID,
		TaskType:  DirectPing,
		Timestamp: hlc.Now(),
	}

	cluster.healthMonitor.processHealthCheck(task)

	// Node should still be alive
	if node.GetObservedState() != NodeAlive {
		t.Errorf("Expected node to remain alive, got %v", node.GetObservedState())
	}
}

func TestProcessHealthCheckDirectPingFailure(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{} // fails SendWithReply
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	task := HealthCheckTask{
		NodeID:    nodeID,
		TaskType:  DirectPing,
		Timestamp: hlc.Now(),
	}

	cluster.healthMonitor.processHealthCheck(task)

	// Node should be suspect
	if node.GetObservedState() != NodeSuspect {
		t.Errorf("Expected node to be suspect after failed ping, got %v", node.GetObservedState())
	}
}

// ============================================================================
// HealthMonitor: processHealthCheck - SuspectRetry
// ============================================================================

func TestProcessHealthCheckSuspectRetrySuccess(t *testing.T) {
	config := DefaultConfig()
	mt := &replyMockTransport{
		pongResponse: &pongMessage{
			NodeID:            NodeID(uuid.New()),
			AdvertiseAddr:     "127.0.0.1:8001",
			MetadataTimestamp: hlc.Now(),
			Metadata:          map[string]interface{}{},
			NodeState:         NodeAlive,
		},
		codec: codec.NewJSONCodec(),
	}
	config.Transport = mt
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.updateState(nodeID, NodeSuspect)

	task := HealthCheckTask{
		NodeID:    nodeID,
		TaskType:  SuspectRetry,
		Timestamp: hlc.Now(),
	}

	cluster.healthMonitor.processHealthCheck(task)

	// Node should recover to alive
	if node.GetObservedState() != NodeAlive {
		t.Errorf("Expected node to recover to alive, got %v", node.GetObservedState())
	}
}

func TestProcessHealthCheckSuspectRetryFailureNotYetDead(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.DeadNodeTimeout = 1 * time.Hour // long timeout so it stays suspect

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.updateState(nodeID, NodeSuspect)

	task := HealthCheckTask{
		NodeID:    nodeID,
		TaskType:  SuspectRetry,
		Timestamp: hlc.Now(),
	}

	cluster.healthMonitor.processHealthCheck(task)

	// Should remain suspect (not enough time for dead timeout)
	if node.GetObservedState() != NodeSuspect {
		t.Errorf("Expected node to remain suspect, got %v", node.GetObservedState())
	}
}

func TestProcessHealthCheckSuspectRetryFailureMarkDead(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.DeadNodeTimeout = 1 * time.Millisecond

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.updateState(nodeID, NodeSuspect)

	// Wait for deadline to pass
	time.Sleep(5 * time.Millisecond)

	task := HealthCheckTask{
		NodeID:    nodeID,
		TaskType:  SuspectRetry,
		Timestamp: hlc.Now(),
	}

	cluster.healthMonitor.processHealthCheck(task)

	// Should be marked dead now
	if node.GetObservedState() != NodeDead {
		t.Errorf("Expected node to be dead after timeout, got %v", node.GetObservedState())
	}
}

// ============================================================================
// HealthMonitor: processHealthCheck - DeadNodeRetry
// ============================================================================

func TestProcessHealthCheckDeadNodeRetrySuccess(t *testing.T) {
	config := DefaultConfig()
	mt := &replyMockTransport{
		pongResponse: &pongMessage{
			NodeID:            NodeID(uuid.New()),
			AdvertiseAddr:     "127.0.0.1:8001",
			MetadataTimestamp: hlc.Now(),
			Metadata:          map[string]interface{}{},
			NodeState:         NodeAlive,
		},
		codec: codec.NewJSONCodec(),
	}
	config.Transport = mt
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.updateState(nodeID, NodeDead)

	task := HealthCheckTask{
		NodeID:    nodeID,
		TaskType:  DeadNodeRetry,
		Timestamp: hlc.Now(),
	}

	cluster.healthMonitor.processHealthCheck(task)

	// Node should recover!
	if node.GetObservedState() != NodeAlive {
		t.Errorf("Expected dead node to recover, got %v", node.GetObservedState())
	}
}

func TestProcessHealthCheckDeadNodeRetryStillDead(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{} // fails
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.updateState(nodeID, NodeDead)

	task := HealthCheckTask{
		NodeID:    nodeID,
		TaskType:  DeadNodeRetry,
		Timestamp: hlc.Now(),
	}

	cluster.healthMonitor.processHealthCheck(task)

	// Should remain dead
	if node.GetObservedState() != NodeDead {
		t.Errorf("Expected node to remain dead, got %v", node.GetObservedState())
	}
}

func TestProcessHealthCheckNonexistentNode(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	task := HealthCheckTask{
		NodeID:    NodeID(uuid.New()), // doesn't exist
		TaskType:  DirectPing,
		Timestamp: hlc.Now(),
	}

	// Should return without panic
	cluster.healthMonitor.processHealthCheck(task)
}

// ============================================================================
// HealthMonitor: pingNode - address update
// ============================================================================

func TestPingNodeAddressUpdate(t *testing.T) {
	config := DefaultConfig()
	mt := &replyMockTransport{
		pongResponse: &pongMessage{
			NodeID:            NodeID(uuid.New()),
			AdvertiseAddr:     "127.0.0.1:9999", // changed address!
			MetadataTimestamp: hlc.Now(),
			Metadata:          map[string]interface{}{"key": "val"},
			NodeState:         NodeAlive,
		},
		codec: codec.NewJSONCodec(),
	}
	config.Transport = mt
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	success := cluster.healthMonitor.pingNode(node)
	if !success {
		t.Fatal("Ping should have succeeded")
	}

	// Address should have been updated
	if node.advertiseAddr != "127.0.0.1:9999" {
		t.Errorf("Expected address to be updated to 127.0.0.1:9999, got %s", node.advertiseAddr)
	}

	// Metadata should have been updated
	if node.metadata.GetString("key") != "val" {
		t.Error("Metadata should have been updated from pong response")
	}
}

// ============================================================================
// HealthMonitor: enqueueHealthCheck
// ============================================================================

func TestEnqueueHealthCheckQueueFull(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.HealthCheckQueueDepth = 1

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())

	// Fill the queue
	cluster.healthMonitor.enqueueHealthCheck(nodeID, DirectPing)

	// Second should be silently dropped (queue full)
	cluster.healthMonitor.enqueueHealthCheck(nodeID, DirectPing)

	// Drain
	<-cluster.healthMonitor.taskQueue
}

// ============================================================================
// HealthMonitor: scanNodes
// ============================================================================

func TestScanNodesSkipsLocalNode(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.SuspectTimeout = 1 * time.Millisecond
	config.HealthCheckQueueDepth = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Only local node should be alive, and scanner should skip it
	cluster.healthMonitor.scanNodes()

	// Queue should be empty
	select {
	case <-cluster.healthMonitor.taskQueue:
		t.Error("Should not enqueue health check for local node")
	default:
		// expected
	}
}

func TestScanNodesEnqueuesSuspectCandidates(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.SuspectTimeout = 1 * time.Millisecond
	config.HealthCheckQueueDepth = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add a node with old activity timestamp
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Wait for suspect timeout
	time.Sleep(5 * time.Millisecond)

	cluster.healthMonitor.scanNodes()

	// Should have enqueued a health check
	select {
	case task := <-cluster.healthMonitor.taskQueue:
		if task.NodeID != nodeID {
			t.Error("Health check should be for the stale node")
		}
		if task.TaskType != DirectPing {
			t.Error("Should be a DirectPing task")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected health check to be enqueued")
	}
}

// ============================================================================
// HealthMonitor: retryDeadNodes
// ============================================================================

func TestRetryDeadNodesWithinRetryWindow(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.MaxDeadNodeRetryTime = 1 * time.Hour
	config.HealthCheckQueueDepth = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.updateState(nodeID, NodeDead)

	cluster.healthMonitor.retryDeadNodes()

	// Should have enqueued a retry
	select {
	case task := <-cluster.healthMonitor.taskQueue:
		if task.TaskType != DeadNodeRetry {
			t.Error("Expected DeadNodeRetry task")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected dead node retry to be enqueued")
	}
}

func TestRetryDeadNodesExceedsRetryWindow(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.MaxDeadNodeRetryTime = 1 * time.Millisecond
	config.HealthCheckQueueDepth = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.updateState(nodeID, NodeDead)

	// Wait for retry window to expire
	time.Sleep(5 * time.Millisecond)

	cluster.healthMonitor.retryDeadNodes()

	// Should NOT have enqueued
	select {
	case <-cluster.healthMonitor.taskQueue:
		t.Error("Should not retry dead nodes past MaxDeadNodeRetryTime")
	default:
		// expected
	}
}

// ============================================================================
// HealthMonitor: retrySuspectNodes
// ============================================================================

func TestRetrySuspectNodes(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.HealthCheckQueueDepth = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	cluster.nodes.updateState(nodeID, NodeSuspect)

	cluster.healthMonitor.retrySuspectNodes()

	select {
	case task := <-cluster.healthMonitor.taskQueue:
		if task.TaskType != SuspectRetry {
			t.Error("Expected SuspectRetry task")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected suspect retry to be enqueued")
	}
}

// ============================================================================
// replyMockTransport - a mock transport that returns canned pong responses
// ============================================================================

type replyMockTransport struct {
	pongResponse *pongMessage
	codec        codec.Serializer
	ch           chan *Packet
}

func (t *replyMockTransport) Name() string { return "reply-mock" }
func (t *replyMockTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	t.ch = make(chan *Packet, 100)
	return nil
}
func (t *replyMockTransport) PacketChannel() chan *Packet {
	if t.ch == nil {
		t.ch = make(chan *Packet, 100)
	}
	return t.ch
}
func (t *replyMockTransport) Send(transportType TransportType, node *Node, packet *Packet) error {
	return nil
}
func (t *replyMockTransport) SendWithReply(node *Node, packet *Packet) (*Packet, error) {
	reply := NewPacket()
	reply.MessageType = replyMsg
	reply.SenderID = node.ID
	reply.MessageID = MessageID(hlc.Now())
	reply.SetCodec(t.codec)

	payload, err := t.codec.Marshal(t.pongResponse)
	if err != nil {
		return nil, err
	}
	reply.SetPayload(payload)

	return reply, nil
}
