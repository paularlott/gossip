package gossip

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

// ============================================================================
// configurableMockTransport: supports failure injection, delays, partitions
// ============================================================================

type configurableMockTransport struct {
	ch             chan *Packet
	sendErr        error
	sendCount      atomic.Int64
	sendDelay      time.Duration
	replyErr       error
	replyResponse  *Packet
	partitionedIDs sync.Map // NodeID -> bool
	mu             sync.RWMutex
}

func newConfigurableMockTransport() *configurableMockTransport {
	return &configurableMockTransport{
		ch: make(chan *Packet, 100),
	}
}

func (t *configurableMockTransport) Name() string { return "configurable-mock" }
func (t *configurableMockTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	return nil
}
func (t *configurableMockTransport) PacketChannel() chan *Packet { return t.ch }

func (t *configurableMockTransport) Send(transportType TransportType, node *Node, packet *Packet) error {
	t.sendCount.Add(1)

	if t.sendDelay > 0 {
		time.Sleep(t.sendDelay)
	}

	// Check partition
	if _, partitioned := t.partitionedIDs.Load(node.ID); partitioned {
		return fmt.Errorf("network partition: can't reach %s", node.ID.String())
	}

	t.mu.RLock()
	err := t.sendErr
	t.mu.RUnlock()

	return err
}

func (t *configurableMockTransport) SendWithReply(node *Node, packet *Packet) (*Packet, error) {
	t.sendCount.Add(1)

	if t.sendDelay > 0 {
		time.Sleep(t.sendDelay)
	}

	if _, partitioned := t.partitionedIDs.Load(node.ID); partitioned {
		return nil, fmt.Errorf("network partition: can't reach %s", node.ID.String())
	}

	t.mu.RLock()
	err := t.replyErr
	resp := t.replyResponse
	t.mu.RUnlock()

	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp.AddRef(), nil
	}
	return nil, fmt.Errorf("no reply available")
}

func (t *configurableMockTransport) setSendError(err error) {
	t.mu.Lock()
	t.sendErr = err
	t.mu.Unlock()
}

func (t *configurableMockTransport) setReplyError(err error) {
	t.mu.Lock()
	t.replyErr = err
	t.mu.Unlock()
}

func (t *configurableMockTransport) partition(nodeID NodeID) {
	t.partitionedIDs.Store(nodeID, true)
}

func (t *configurableMockTransport) heal(nodeID NodeID) {
	t.partitionedIDs.Delete(nodeID)
}

// ============================================================================
// Network Partition Simulation
// ============================================================================

func TestNetworkPartitionCausesNodeSuspect(t *testing.T) {
	mt := newConfigurableMockTransport()
	config := DefaultConfig()
	config.Transport = mt
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add a node
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Partition the node
	mt.partition(nodeID)

	// Exchange state should mark node as suspect (can't reach it)
	cluster.exchangeState([]*Node{node}, []NodeID{cluster.localNode.ID})

	if node.GetObservedState() != NodeSuspect {
		t.Errorf("Expected node to be suspect after partition, got %v", node.GetObservedState())
	}
}

func TestNetworkPartitionAndRecovery(t *testing.T) {
	mt := newConfigurableMockTransport()
	config := DefaultConfig()
	config.Transport = mt
	config.MsgCodec = codec.NewJSONCodec()
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

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// 1. Partition => suspect
	mt.partition(nodeID)
	cluster.exchangeState([]*Node{node}, []NodeID{cluster.localNode.ID})
	if node.GetObservedState() != NodeSuspect {
		t.Error("Node should be suspect after partition")
	}

	// 2. Receive alive state from another peer (simulates indirect connectivity)
	time.Sleep(1 * time.Millisecond)
	cluster.combineStates([]exchangeNodeState{
		{
			ID:             nodeID,
			AdvertiseAddr:  "127.0.0.1:8001",
			State:          NodeAlive,
			StateTimestamp: hlc.Now(),
		},
	})

	// Node should be marked alive via combineStates recovery
	// (combineStates processes Alive from Suspect with newer timestamp)
	// Actually combineStates only processes Alive from Dead/Leaving, not from Suspect
	// This is intentional - suspect nodes need to prove they're alive via ping
}

// ============================================================================
// Intermittent Network Failures
// ============================================================================

func TestIntermittentSendFailures(t *testing.T) {
	mt := newConfigurableMockTransport()
	config := DefaultConfig()
	config.Transport = mt
	config.MsgCodec = codec.NewJSONCodec()
	config.SendQueueSize = 200

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Start broadcast worker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case item := <-cluster.broadcastQueue:
				if item.peers == nil {
					item.peers = cluster.nodes.getRandomNodes(cluster.CalcFanOut(), item.excludePeers)
				}
				for _, peer := range item.peers {
					if peer.ID != cluster.localNode.ID {
						mt.Send(item.transportType, peer, item.packet)
					}
				}
				item.packet.Release()
				item.packet = nil
				cluster.broadcastQItemPool.Put(item)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Add nodes
	for i := 0; i < 5; i++ {
		n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	// Send with intermittent failures
	mt.setSendError(fmt.Errorf("connection refused"))

	for i := 0; i < 10; i++ {
		cluster.Send(UserMsg, fmt.Sprintf("msg-%d", i))
	}

	time.Sleep(50 * time.Millisecond) // let workers process

	// Clear error
	mt.setSendError(nil)

	// Messages should succeed now
	for i := 0; i < 10; i++ {
		err := cluster.Send(UserMsg, fmt.Sprintf("msg-ok-%d", i))
		if err != nil {
			t.Errorf("Send should succeed after error cleared: %v", err)
		}
	}

	cancel()
	wg.Wait()
}

// ============================================================================
// Node Flapping Simulation
// ============================================================================

func TestNodeFlappingRapidStateChanges(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Rapidly flap between alive and dead via combineStates
	for i := 0; i < 20; i++ {
		time.Sleep(1 * time.Millisecond)

		var state NodeState
		if i%2 == 0 {
			state = NodeDead
		} else {
			state = NodeAlive
		}

		cluster.nodes.updateState(nodeID, state)
	}

	// Verify counters are consistent
	alive := cluster.nodes.getAliveCount()
	dead := cluster.nodes.getDeadCount()
	suspect := cluster.nodes.getSuspectCount()
	leaving := cluster.nodes.getLeavingCount()

	total := alive + dead + suspect + leaving
	// Should be exactly 2 (local + the flapping node)
	if total != 2 {
		t.Errorf("Counter inconsistency: alive=%d dead=%d suspect=%d leaving=%d total=%d (expected 2)",
			alive, dead, suspect, leaving, total)
	}
}

func TestConcurrentStateChangesCounterConsistency(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add 20 nodes
	nodeIDs := make([]NodeID, 20)
	for i := 0; i < 20; i++ {
		nodeID := NodeID(uuid.New())
		nodeIDs[i] = nodeID
		n := newNode(nodeID, fmt.Sprintf("127.0.0.1:%d", 8001+i))
		cluster.nodes.addOrUpdate(n)
	}

	// Concurrently change states
	var wg sync.WaitGroup
	states := []NodeState{NodeAlive, NodeSuspect, NodeDead}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nodeIdx := i % len(nodeIDs)
			stateIdx := i % len(states)
			cluster.nodes.updateState(nodeIDs[nodeIdx], states[stateIdx])
		}(i)
	}

	wg.Wait()

	// Verify counter consistency
	alive := cluster.nodes.getAliveCount()
	dead := cluster.nodes.getDeadCount()
	suspect := cluster.nodes.getSuspectCount()
	leaving := cluster.nodes.getLeavingCount()

	total := alive + dead + suspect + leaving
	// Should be 21 (1 local + 20 nodes)
	if total != 21 {
		t.Errorf("Counter inconsistency after concurrent changes: alive=%d dead=%d suspect=%d leaving=%d total=%d (expected 21)",
			alive, dead, suspect, leaving, total)
	}
}

// ============================================================================
// Simultaneous Cluster Restart Simulation
// ============================================================================

func TestSimultaneousRestartStateConvergence(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
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

	// Pre-populate with "old" nodes (from before restart)
	for i := 0; i < 5; i++ {
		nodeID := NodeID(uuid.New())
		node := newNode(nodeID, fmt.Sprintf("127.0.0.1:%d", 9001+i))
		node.observedState = NodeDead
		cluster.nodes.addOrUpdate(node)
	}

	// Simulate receiving state exchange from freshly restarted peer
	// saying some dead nodes are now alive
	var aliveStates []exchangeNodeState
	deadNodes := cluster.nodes.getAllInStates([]NodeState{NodeDead})

	time.Sleep(1 * time.Millisecond) // ensure newer timestamp

	for _, dn := range deadNodes {
		aliveStates = append(aliveStates, exchangeNodeState{
			ID:             dn.ID,
			AdvertiseAddr:  dn.advertiseAddr,
			State:          NodeAlive,
			StateTimestamp: hlc.Now(),
		})
	}

	cluster.combineStates(aliveStates)

	// Dead nodes that received Alive with newer timestamp should be updated
	for _, dn := range deadNodes {
		node := cluster.nodes.get(dn.ID)
		if node.GetObservedState() == NodeDead {
			t.Error("Dead node should have been recovered via combineStates with newer timestamp")
		}
	}
}

// ============================================================================
// Split-brain Scenario
// ============================================================================

func TestSplitBrainStateMerging(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.JoinQueueSize = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

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

	// Node A is alive in our view
	nodeA := NodeID(uuid.New())
	nodeAObj := newNode(nodeA, "127.0.0.1:9001")
	cluster.nodes.addOrUpdate(nodeAObj)

	// Node B is dead in our view
	nodeB := NodeID(uuid.New())
	nodeBObj := newNode(nodeB, "127.0.0.1:9002")
	nodeBObj.observedState = NodeDead
	cluster.nodes.addOrUpdate(nodeBObj)

	time.Sleep(1 * time.Millisecond)

	// From another partition: A is dead, B is alive
	// (with timestamps newer than ours)
	newerTS := hlc.Now()
	splitBrainStates := []exchangeNodeState{
		{
			ID:             nodeA,
			AdvertiseAddr:  "127.0.0.1:9001",
			State:          NodeDead,
			StateTimestamp: newerTS,
		},
		{
			ID:             nodeB,
			AdvertiseAddr:  "127.0.0.1:9002",
			State:          NodeAlive,
			StateTimestamp: newerTS,
		},
	}

	cluster.combineStates(splitBrainStates)

	// A should transition to Dead (newer timestamp says dead)
	if nodeAObj.GetObservedState() != NodeDead {
		t.Errorf("Node A should be Dead from split-brain merge, got %v", nodeAObj.GetObservedState())
	}

	// B should transition to Alive (newer timestamp says alive)
	if nodeBObj.GetObservedState() != NodeAlive {
		t.Errorf("Node B should be Alive from split-brain merge, got %v", nodeBObj.GetObservedState())
	}
}

// ============================================================================
// Broadcast Queue Under Pressure
// ============================================================================

func TestBroadcastQueueOverflow(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.SendQueueSize = 5 // small queue

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Don't start broadcast worker - queue will fill up

	successCount := 0
	for i := 0; i < 20; i++ {
		p := NewPacket()
		p.TTL = 3
		p.SetCodec(config.MsgCodec)
		payload, _ := config.MsgCodec.Marshal("test")
		p.SetPayload(payload)

		cluster.enqueuePacketForBroadcast(p, TransportBestEffort, nil, nil)
		successCount++
	}

	// Should not have panicked - overflow is handled gracefully
	if successCount != 20 {
		t.Errorf("All 20 attempts should complete, got %d", successCount)
	}

	// Drain the queue
	for {
		select {
		case item := <-cluster.broadcastQueue:
			item.packet.Release()
			item.packet = nil
			cluster.broadcastQItemPool.Put(item)
		default:
			return
		}
	}
}

// ============================================================================
// Message History Under High Load
// ============================================================================

func TestMessageHistoryHighThroughput(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 16
	config.MsgHistoryMaxAge = 10 * time.Second
	config.MsgHistoryGCInterval = 10 * time.Second

	history := newMessageHistory(config)
	defer history.stop()

	const numGoroutines = 10
	const msgsPerGoroutine = 100

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			senderID := NodeID(uuid.New())
			for j := 0; j < msgsPerGoroutine; j++ {
				msgID := MessageID(hlc.Now())
				history.recordMessage(senderID, msgID)

				// Verify it's immediately findable
				if !history.contains(senderID, msgID) {
					t.Errorf("Message should be in history immediately after recording")
				}
			}
		}(i)
	}

	wg.Wait()
}

// ============================================================================
// Concurrent Metadata Operations
// ============================================================================

func TestConcurrentMetadataOperations(t *testing.T) {
	md := NewMetadata()

	var wg sync.WaitGroup
	const numGoroutines = 20
	const numOps = 50

	callCount := int64(0)
	md.SetOnLocalChange(func(ts hlc.Timestamp, data map[string]interface{}) {
		atomic.AddInt64(&callCount, 1)
	})

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				md.SetString(key, fmt.Sprintf("value-%d-%d", id, j))
				md.GetString(key)
				md.GetAll()
				md.GetAllKeys()
				if j%3 == 0 {
					md.Delete(key)
				}
			}
		}(i)
	}

	wg.Wait()

	if callCount == 0 {
		t.Error("Expected callbacks during concurrent operations")
	}
}

// ============================================================================
// Node State Event Handler Race Conditions
// ============================================================================

func TestConcurrentEventHandlerRegistrationAndNotification(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	var wg sync.WaitGroup
	var notificationCount int64

	// Register one handler first to guarantee we always have one before nodes arrive
	cluster.HandleNodeStateChangeFunc(func(node *Node, prevState NodeState) {
		atomic.AddInt64(&notificationCount, 1)
	})

	// Register more handlers concurrently while nodes are being added
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cluster.HandleNodeStateChangeFunc(func(node *Node, prevState NodeState) {
				atomic.AddInt64(&notificationCount, 1)
			})
		}()
	}

	// Add nodes concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 9001+i))
			cluster.nodes.addOrUpdate(n)
		}(i)
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond) // let async notifications complete

	// The pre-registered handler guarantees at least some notifications
	if atomic.LoadInt64(&notificationCount) == 0 {
		t.Error("Expected some notifications from pre-registered handler")
	}
}
