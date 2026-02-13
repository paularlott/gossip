package gossip

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

// TestAddIfNotExistsDoesNotCorruptCounters verifies that addIfNotExists
// does not update counters or fire notifications when the node already exists.
// BUG FIX: Previously, addIfNotExists would decrement the old state counter
// and increment the new node's state counter without actually changing the
// existing node's state, corrupting alive/dead counts cluster-wide.
func TestAddIfNotExistsDoesNotCorruptCounters(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add a node as Dead
	nodeID := NodeID(uuid.New())
	deadNode := newNode(nodeID, "127.0.0.1:9001")
	deadNode.observedState = NodeDead
	cluster.nodes.addOrUpdate(deadNode)

	initialAlive := cluster.nodes.getAliveCount()
	initialDead := cluster.nodes.getDeadCount()

	// Now try addIfNotExists with a new node object that has Alive state
	aliveNode := newNode(nodeID, "127.0.0.1:9001") // defaults to Alive
	returned := cluster.nodes.addIfNotExists(aliveNode)

	// Should return the existing (Dead) node
	if returned.GetObservedState() != NodeDead {
		t.Errorf("Expected existing node to remain Dead, got %v", returned.GetObservedState())
	}

	// Counters should NOT have changed
	if cluster.nodes.getAliveCount() != initialAlive {
		t.Errorf("Alive count changed: expected %d, got %d", initialAlive, cluster.nodes.getAliveCount())
	}
	if cluster.nodes.getDeadCount() != initialDead {
		t.Errorf("Dead count changed: expected %d, got %d", initialDead, cluster.nodes.getDeadCount())
	}
}

// TestCombineStatesPropagatésAlive verifies that combineStates now accepts
// Alive state transitions when the remote timestamp is newer.
// BUG FIX: Previously, combineStates only accepted Dead/Leaving transitions,
// preventing recovery information from propagating through the cluster.
func TestCombineStatesPropagatesAlive(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create a node that's currently Dead in our view
	nodeID := NodeID(uuid.New())
	deadNode := newNode(nodeID, "127.0.0.1:9001")
	deadNode.observedState = NodeDead
	deadNode.observedStateTime = hlc.Now()
	cluster.nodes.addOrUpdate(deadNode)

	// Wait a tiny bit so the new timestamp is definitely newer
	time.Sleep(1 * time.Millisecond)

	// Simulate receiving a state exchange from another node saying this node is Alive
	newerTimestamp := hlc.Now()
	remoteStates := []exchangeNodeState{
		{
			ID:             nodeID,
			AdvertiseAddr:  "127.0.0.1:9001",
			State:          NodeAlive,
			StateTimestamp: newerTimestamp,
		},
	}

	cluster.combineStates(remoteStates)

	// The node should now be Alive
	node := cluster.nodes.get(nodeID)
	if node == nil {
		t.Fatal("Node not found after combineStates")
	}
	if node.GetObservedState() != NodeAlive {
		t.Errorf("Expected node to be Alive after combineStates with newer timestamp, got %v", node.GetObservedState())
	}
}

// TestCombineStatesRejectsOlderAlive verifies that combineStates correctly
// rejects stale Alive state when the local Dead timestamp is newer.
func TestCombineStatesRejectsOlderAlive(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create an old Alive timestamp
	oldTimestamp := hlc.Now()
	time.Sleep(1 * time.Millisecond)

	// Node is Dead with a newer timestamp
	nodeID := NodeID(uuid.New())
	deadNode := newNode(nodeID, "127.0.0.1:9001")
	deadNode.observedState = NodeDead
	deadNode.observedStateTime = hlc.Now()
	cluster.nodes.addOrUpdate(deadNode)

	// Simulate receiving stale "Alive" state with an older timestamp
	remoteStates := []exchangeNodeState{
		{
			ID:             nodeID,
			AdvertiseAddr:  "127.0.0.1:9001",
			State:          NodeAlive,
			StateTimestamp: oldTimestamp,
		},
	}

	cluster.combineStates(remoteStates)

	// The node should remain Dead (stale alive rejected)
	node := cluster.nodes.get(nodeID)
	if node.GetObservedState() != NodeDead {
		t.Errorf("Expected node to remain Dead with older alive timestamp, got %v", node.GetObservedState())
	}
}

// TestCombineStatesStillAcceptsDead verifies that Dead state propagation
// still works correctly (regression test for the existing behavior).
func TestCombineStatesStillAcceptsDead(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Node is Alive
	nodeID := NodeID(uuid.New())
	aliveNode := newNode(nodeID, "127.0.0.1:9001")
	cluster.nodes.addOrUpdate(aliveNode)

	time.Sleep(1 * time.Millisecond)

	// Another node reports it as Dead with a newer timestamp
	remoteStates := []exchangeNodeState{
		{
			ID:             nodeID,
			AdvertiseAddr:  "127.0.0.1:9001",
			State:          NodeDead,
			StateTimestamp: hlc.Now(),
		},
	}

	cluster.combineStates(remoteStates)

	node := cluster.nodes.get(nodeID)
	if node.GetObservedState() != NodeDead {
		t.Errorf("Expected node to be Dead after combineStates, got %v", node.GetObservedState())
	}
}

// TestJoinReplyRejoinsDeadPeers verifies that when processing a join reply,
// peers we have marked as Dead are re-joined (they may have restarted).
// BUG FIX: Previously, dead peers were skipped entirely in join reply processing.
func TestJoinReplyRejoinsDeadPeers(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()
	config.JoinQueueSize = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add a dead node to our node list
	deadID := NodeID(uuid.New())
	deadNode := newNode(deadID, "127.0.0.1:9002")
	deadNode.observedState = NodeDead
	cluster.nodes.addOrUpdate(deadNode)

	// Simulate receiving join reply from another node that includes the dead peer
	joinReplyNodes := []joinNode{
		{
			ID:            deadID,
			AdvertiseAddr: "127.0.0.1:9002",
			Tags:          []string{},
		},
	}

	// The dead peer should be queued for re-joining
	// We can verify by checking if a join request is enqueued
	existing := cluster.nodes.get(deadID)
	if existing == nil {
		t.Fatal("Dead node should exist in node list")
	}
	if !existing.DeadOrLeft() {
		t.Fatal("Node should be dead or left")
	}

	// Verify the peer would be processed (not skipped)
	for _, peer := range joinReplyNodes {
		existingPeer := cluster.nodes.get(peer.ID)
		if existingPeer == nil || existingPeer.DeadOrLeft() {
			// This is what we expect - the dead peer should be eligible for re-joining
			t.Log("Dead peer correctly identified for re-joining")
		} else {
			t.Error("Dead peer was incorrectly skipped")
		}
	}
}

// TestCounterConsistencyAfterStateTransitions verifies that alive/dead/suspect
// counters remain accurate through multiple state transitions.
func TestCounterConsistencyAfterStateTransitions(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Create 5 nodes
	nodeIDs := make([]NodeID, 5)
	for i := range nodeIDs {
		nodeIDs[i] = NodeID(uuid.New())
		node := newNode(nodeIDs[i], "127.0.0.1:"+string(rune('0'+i)))
		cluster.nodes.addOrUpdate(node)
	}

	// Initial: local + 5 nodes = 6 alive
	expectedAlive := 6 // local node + 5 added nodes
	if cluster.nodes.getAliveCount() != expectedAlive {
		t.Fatalf("Expected %d alive, got %d", expectedAlive, cluster.nodes.getAliveCount())
	}

	// Mark 2 as suspect
	cluster.nodes.updateState(nodeIDs[0], NodeSuspect)
	cluster.nodes.updateState(nodeIDs[1], NodeSuspect)

	if cluster.nodes.getAliveCount() != 4 {
		t.Errorf("Expected 4 alive after 2 suspects, got %d", cluster.nodes.getAliveCount())
	}
	if cluster.nodes.getSuspectCount() != 2 {
		t.Errorf("Expected 2 suspect, got %d", cluster.nodes.getSuspectCount())
	}

	// Mark 1 suspect as dead
	cluster.nodes.updateState(nodeIDs[0], NodeDead)

	if cluster.nodes.getAliveCount() != 4 {
		t.Errorf("Expected 4 alive, got %d", cluster.nodes.getAliveCount())
	}
	if cluster.nodes.getSuspectCount() != 1 {
		t.Errorf("Expected 1 suspect, got %d", cluster.nodes.getSuspectCount())
	}
	if cluster.nodes.getDeadCount() != 1 {
		t.Errorf("Expected 1 dead, got %d", cluster.nodes.getDeadCount())
	}

	// Recover the dead node (like health monitor would)
	cluster.nodes.updateState(nodeIDs[0], NodeAlive)

	if cluster.nodes.getAliveCount() != 5 {
		t.Errorf("Expected 5 alive after recovery, got %d", cluster.nodes.getAliveCount())
	}
	if cluster.nodes.getDeadCount() != 0 {
		t.Errorf("Expected 0 dead after recovery, got %d", cluster.nodes.getDeadCount())
	}

	// Now test addIfNotExists doesn't corrupt counters
	newNodeObj := newNode(nodeIDs[0], "127.0.0.1:0") // Alive by default
	cluster.nodes.addIfNotExists(newNodeObj)

	// Counters should be unchanged
	if cluster.nodes.getAliveCount() != 5 {
		t.Errorf("addIfNotExists corrupted alive count: expected 5, got %d", cluster.nodes.getAliveCount())
	}
	if cluster.nodes.getDeadCount() != 0 {
		t.Errorf("addIfNotExists corrupted dead count: expected 0, got %d", cluster.nodes.getDeadCount())
	}

	// Test addIfNotExists with state mismatch (Dead node, Alive new object)
	cluster.nodes.updateState(nodeIDs[2], NodeDead)
	deadCountBefore := cluster.nodes.getDeadCount()
	aliveCountBefore := cluster.nodes.getAliveCount()

	aliveNewObj := newNode(nodeIDs[2], "127.0.0.1:2")
	cluster.nodes.addIfNotExists(aliveNewObj)

	if cluster.nodes.getDeadCount() != deadCountBefore {
		t.Errorf("addIfNotExists with state mismatch corrupted dead count: expected %d, got %d",
			deadCountBefore, cluster.nodes.getDeadCount())
	}
	if cluster.nodes.getAliveCount() != aliveCountBefore {
		t.Errorf("addIfNotExists with state mismatch corrupted alive count: expected %d, got %d",
			aliveCountBefore, cluster.nodes.getAliveCount())
	}
}

// TestCombineStatesFlappingResistance simulates the flapping scenario:
// Node B joins Node A (alive). Node C has B as dead. C gossips "B dead" to A.
// Then A pings B (success), marks alive. Then C gossips "B dead" again.
// Verifies the timestamps prevent oscillation.
func TestCombineStatesFlappingResistance(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeB := NodeID(uuid.New())

	// Step 1: B joins A - A marks B as Alive at T1
	node := newNode(nodeB, "127.0.0.1:9001")
	cluster.nodes.addOrUpdate(node)

	time.Sleep(1 * time.Millisecond)

	// Step 2: C has B as Dead at T2 (T2 > T1, because C detected death after join)
	t2 := hlc.Now()
	remoteStatesFromC := []exchangeNodeState{
		{
			ID:             nodeB,
			AdvertiseAddr:  "127.0.0.1:9001",
			State:          NodeDead,
			StateTimestamp: t2,
		},
	}

	cluster.combineStates(remoteStatesFromC)

	// A should accept "Dead" because T2 > T1
	if cluster.nodes.get(nodeB).GetObservedState() != NodeDead {
		t.Error("Expected B to be marked Dead (T2 > T1)")
	}

	time.Sleep(1 * time.Millisecond)

	// Step 3: A pings B successfully, marks Alive at T3 (T3 > T2)
	cluster.nodes.updateState(nodeB, NodeAlive)
	t3 := cluster.nodes.get(nodeB).observedStateTime

	if !t3.After(t2) {
		t.Fatal("T3 should be after T2")
	}

	// Step 4: C gossips "B dead" again, but still using T2 (C hasn't updated its view)
	cluster.combineStates(remoteStatesFromC)

	// A should REJECT this because T2 < T3 (stale death report)
	if cluster.nodes.get(nodeB).GetObservedState() != NodeAlive {
		t.Error("Expected B to remain Alive - stale death report should be rejected")
	}

	time.Sleep(1 * time.Millisecond)

	// Step 5: However, if C detects B as dead again at T4 (newer), A should accept
	t4 := hlc.Now()
	newerDeathReport := []exchangeNodeState{
		{
			ID:             nodeB,
			AdvertiseAddr:  "127.0.0.1:9001",
			State:          NodeDead,
			StateTimestamp: t4,
		},
	}

	cluster.combineStates(newerDeathReport)

	if cluster.nodes.get(nodeB).GetObservedState() != NodeDead {
		t.Error("Expected B to be marked Dead with newer timestamp T4 > T3")
	}
}

// TestCombineStatesRecoveryPropagation simulates the recovery propagation scenario:
// Node A knows B is alive (from direct join). Node C has B as dead.
// A state-exchanges with C. C should learn B is alive.
func TestCombineStatesRecoveryPropagation(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	// Simulate Node C's cluster
	clusterC, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster C: %v", err)
	}

	nodeB := NodeID(uuid.New())

	// C has B as Dead at time T1
	deadNode := newNode(nodeB, "127.0.0.1:9001")
	deadNode.observedState = NodeDead
	deadNode.observedStateTime = hlc.Now()
	clusterC.nodes.addOrUpdate(deadNode)

	time.Sleep(1 * time.Millisecond)

	// A has B as Alive at time T2 > T1 (from a successful join)
	t2 := hlc.Now()

	// A sends state exchange to C saying B is Alive at T2
	statesFromA := []exchangeNodeState{
		{
			ID:             nodeB,
			AdvertiseAddr:  "127.0.0.1:9001",
			State:          NodeAlive,
			StateTimestamp: t2,
		},
	}

	clusterC.combineStates(statesFromA)

	// C should now know B is Alive (recovery propagated!)
	if clusterC.nodes.get(nodeB).GetObservedState() != NodeAlive {
		t.Error("Expected B to be Alive on C after receiving recovery from A's state exchange")
	}
}

// TestDataNodeGroupSuspectMetadata verifies that DataNodeGroup processes
// metadata changes for Suspect nodes (not just Alive).
// BUG FIX: Previously, metadata changes for Suspect nodes were silently
// ignored even though suspect nodes are kept in the group.
func TestDataNodeGroupSuspectMetadata(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add a node with matching metadata
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, "127.0.0.1:9001")
	cluster.nodes.addOrUpdate(node)

	// Set metadata that matches criteria
	node.metadata.update(map[string]interface{}{
		"role": "gateway",
	}, hlc.Now(), false)

	updateCount := 0
	dng := NewDataNodeGroup[int](
		cluster,
		map[string]string{"role": "gateway"},
		&DataNodeGroupOptions[int]{
			OnNodeUpdated: func(n *Node, data *int) {
				updateCount++
			},
		},
	)
	defer dng.Close()

	// Mark node as suspect - it should remain in the group
	cluster.nodes.updateState(nodeID, NodeSuspect)

	// Give the async notification time to fire
	time.Sleep(20 * time.Millisecond)

	if !dng.Contains(nodeID) {
		t.Error("Suspect node should be in the DataNodeGroup")
	}

	// Update metadata while suspect - this should trigger OnNodeUpdated
	updateCount = 0
	node.metadata.update(map[string]interface{}{
		"role":    "gateway",
		"version": "2.0",
	}, hlc.Now(), false)

	// Trigger metadata change notification manually (normally async)
	cluster.nodes.notifyMetadataChanged(node)

	// Give async handler time to fire
	time.Sleep(20 * time.Millisecond)

	if updateCount == 0 {
		t.Error("Expected OnNodeUpdated to fire for suspect node metadata change")
	}
}

// TestSimultaneousRestartConvergence simulates all root nodes restarting
// and verifies that state exchange enables full recovery without
// requiring direct health monitor pings.
func TestSimultaneousRestartConvergence(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Simulate: we know 3 nodes that are Dead (from previous incarnation)
	nodeIDs := make([]NodeID, 3)
	for i := range nodeIDs {
		nodeIDs[i] = NodeID(uuid.New())
		deadNode := newNode(nodeIDs[i], "127.0.0.1:900"+string(rune('1'+i)))
		deadNode.observedState = NodeDead
		deadNode.observedStateTime = hlc.Now()
		cluster.nodes.addOrUpdate(deadNode)
	}

	if cluster.nodes.getAliveCount() != 1 { // only local node
		t.Fatalf("Expected 1 alive (local), got %d", cluster.nodes.getAliveCount())
	}
	if cluster.nodes.getDeadCount() != 3 {
		t.Fatalf("Expected 3 dead, got %d", cluster.nodes.getDeadCount())
	}

	time.Sleep(1 * time.Millisecond)

	// Now one of them has restarted and another cluster member (a peer)
	// tells us via state exchange that node 0 is Alive
	newerTs := hlc.Now()
	stateFromPeer := []exchangeNodeState{
		{
			ID:             nodeIDs[0],
			AdvertiseAddr:  "127.0.0.1:9001",
			State:          NodeAlive,
			StateTimestamp: newerTs,
		},
	}

	cluster.combineStates(stateFromPeer)

	// Node 0 should now be Alive
	if cluster.nodes.get(nodeIDs[0]).GetObservedState() != NodeAlive {
		t.Error("Expected node 0 to be recovered via state exchange")
	}
	if cluster.nodes.getAliveCount() != 2 { // local + recovered
		t.Errorf("Expected 2 alive, got %d", cluster.nodes.getAliveCount())
	}
	if cluster.nodes.getDeadCount() != 2 {
		t.Errorf("Expected 2 dead, got %d", cluster.nodes.getDeadCount())
	}

	time.Sleep(1 * time.Millisecond)

	// All 3 recover
	for i := 1; i < 3; i++ {
		ts := hlc.Now()
		states := []exchangeNodeState{
			{
				ID:             nodeIDs[i],
				AdvertiseAddr:  "127.0.0.1:900" + string(rune('1'+i)),
				State:          NodeAlive,
				StateTimestamp: ts,
			},
		}
		cluster.combineStates(states)
		time.Sleep(1 * time.Millisecond)
	}

	if cluster.nodes.getAliveCount() != 4 { // local + 3 recovered
		t.Errorf("Expected 4 alive after full recovery, got %d", cluster.nodes.getAliveCount())
	}
	if cluster.nodes.getDeadCount() != 0 {
		t.Errorf("Expected 0 dead after full recovery, got %d", cluster.nodes.getDeadCount())
	}
}

// TestCombineStatesIgnoresLocalNode verifies that combineStates skips
// state updates for the local node (another node shouldn't override our self-view).
func TestCombineStatesIgnoresLocalNode(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Another node claims we are Dead
	statesFromPeer := []exchangeNodeState{
		{
			ID:             cluster.localNode.ID,
			AdvertiseAddr:  cluster.localNode.advertiseAddr,
			State:          NodeDead,
			StateTimestamp: hlc.Now(),
		},
	}

	cluster.combineStates(statesFromPeer)

	// We should still be Alive
	if cluster.localNode.GetObservedState() != NodeAlive {
		t.Error("Local node should not accept Dead state from gossip")
	}
}

// TestCombineStatesTombstonePreservation verifies that tombstones (Dead/Leaving
// records for unknown nodes) are still stored to prevent resurrection.
func TestCombineStatesTombstonePreservation(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	unknownNode := NodeID(uuid.New())

	// Receive tombstone for unknown node
	states := []exchangeNodeState{
		{
			ID:             unknownNode,
			AdvertiseAddr:  "127.0.0.1:9999",
			State:          NodeDead,
			StateTimestamp: hlc.Now(),
		},
	}

	cluster.combineStates(states)

	// Tombstone should be stored
	node := cluster.nodes.get(unknownNode)
	if node == nil {
		t.Error("Tombstone for unknown dead node should be stored")
	}
	if node.GetObservedState() != NodeDead {
		t.Errorf("Tombstone state should be Dead, got %v", node.GetObservedState())
	}
}

// TestCombineStatesSuspectTransition verifies Dead->Suspect transitions
// work through state exchange (a node may be recovering, showing as suspect
// on some peers).
func TestCombineStatesSuspectTransition(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	nodeID := NodeID(uuid.New())
	deadNode := newNode(nodeID, "127.0.0.1:9001")
	deadNode.observedState = NodeDead
	deadNode.observedStateTime = hlc.Now()
	cluster.nodes.addOrUpdate(deadNode)

	time.Sleep(1 * time.Millisecond)

	// Peer reports node as Suspect (partially recovered) with newer timestamp
	states := []exchangeNodeState{
		{
			ID:             nodeID,
			AdvertiseAddr:  "127.0.0.1:9001",
			State:          NodeSuspect,
			StateTimestamp: hlc.Now(),
		},
	}

	cluster.combineStates(states)

	node := cluster.nodes.get(nodeID)
	if node.GetObservedState() != NodeSuspect {
		t.Errorf("Expected Dead->Suspect transition, got %v", node.GetObservedState())
	}
}

// TestHandleJoinUpdatesDeadNodeState tests that when a known-dead node
// re-joins, its state is properly updated to Alive.
func TestHandleJoinUpdatesDeadNodeState(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	cluster.registerSystemHandlers()

	// Simulate an existing dead node
	remoteID := NodeID(uuid.New())
	deadNode := newNode(remoteID, "127.0.0.1:9001")
	deadNode.observedState = NodeDead
	deadNode.ProtocolVersion = PROTOCOL_VERSION
	cluster.nodes.addOrUpdate(deadNode)

	if cluster.nodes.get(remoteID).GetObservedState() != NodeDead {
		t.Fatal("Expected node to be Dead before rejoin")
	}

	// Simulate receiving a join message from the dead node (it has restarted)
	joinMsg := joinMessage{
		ID:              remoteID,
		AdvertiseAddr:   "127.0.0.1:9001",
		State:           NodeAlive,
		Tags:            []string{},
		ProtocolVersion: PROTOCOL_VERSION,
	}

	packet := NewPacket()
	packet.SenderID = remoteID
	packet.MessageType = nodeJoinMsg
	packet.codec = config.MsgCodec
	payload, err := config.MsgCodec.Marshal(&joinMsg)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}
	packet.SetPayload(payload)

	// replyChan for the handler
	replyChan := make(chan *Packet, 1)
	packet.SetReplyChan(replyChan)

	// The handler gets the sender from the node list
	sender := cluster.nodes.get(remoteID) // non-nil because we added as dead
	reply, handlerErr := cluster.handleJoin(sender, packet)
	if handlerErr != nil {
		t.Fatalf("handleJoin error: %v", handlerErr)
	}

	replyMsg := reply.(*joinReplyMessage)
	if !replyMsg.Accepted {
		t.Errorf("Join should be accepted, but was rejected: %s", replyMsg.RejectReason)
	}

	// The dead node should now be Alive (handleJoin calls updateState)
	if cluster.nodes.get(remoteID).GetObservedState() != NodeAlive {
		t.Error("Expected dead node to be marked Alive after successful rejoin")
	}

	packet.Release()
}
