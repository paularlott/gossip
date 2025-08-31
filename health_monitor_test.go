package gossip

import (
	"testing"

	"github.com/google/uuid"
)

// Ensures a suspicion about an unknown node adds it as Suspect for convergence
func TestHealthMonitor_HandleSuspicion_AddsUnknownNode(t *testing.T) {
	cluster := createTestCluster(t)
	hm := newHealthMonitor(cluster)

	// Add a sender node
	sender := createTestNode("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", NodeAlive)
	cluster.nodes.addIfNotExists(sender)

	// Build a suspicion packet for an unknown target
	msg := &suspicionMessage{NodeID: mustParseNodeID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")}
	pkt, err := cluster.createPacket(sender.ID, suspicionMsg, 1, msg)
	if err != nil {
		t.Fatalf("createPacket failed: %v", err)
	}
	defer pkt.Release()

	if err := hm.handleSuspicion(sender, pkt); err != nil {
		t.Fatalf("handleSuspicion returned error: %v", err)
	}

	suspect := cluster.nodes.get(msg.NodeID)
	if suspect == nil {
		t.Fatalf("suspect node not added to cluster")
	}
	if suspect.state != NodeSuspect {
		t.Fatalf("suspect node state = %v, want %v", suspect.state, NodeSuspect)
	}
}

// Ensures a leaving message about an unknown node adds it as Leaving
func TestHealthMonitor_HandleLeaving_AddsUnknownNode(t *testing.T) {
	cluster := createTestCluster(t)
	hm := newHealthMonitor(cluster)

	// Add a sender node
	sender := createTestNode("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", NodeAlive)
	cluster.nodes.addIfNotExists(sender)

	// Build a leaving packet for an unknown target
	msg := &leavingMessage{NodeID: mustParseNodeID("cccccccc-cccc-cccc-cccc-cccccccccccc")}
	pkt, err := cluster.createPacket(sender.ID, leavingMsg, 1, msg)
	if err != nil {
		t.Fatalf("createPacket failed: %v", err)
	}
	defer pkt.Release()

	if err := hm.handleLeaving(sender, pkt); err != nil {
		t.Fatalf("handleLeaving returned error: %v", err)
	}

	n := cluster.nodes.get(msg.NodeID)
	if n == nil {
		t.Fatalf("leaving node not added to cluster")
	}
	if n.state != NodeLeaving {
		t.Fatalf("leaving node state = %v, want %v", n.state, NodeLeaving)
	}
}

// Ensures push/pull discovery can add suspect nodes we didn't know about
func TestHealthMonitor_CombineRemote_AddsSuspect(t *testing.T) {
	cluster := createTestCluster(t)
	hm := newHealthMonitor(cluster)

	// Sender known
	sender := createTestNode("dddddddd-dddd-dddd-dddd-dddddddddddd", NodeAlive)
	cluster.nodes.addIfNotExists(sender)

	unknownID := mustParseNodeID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
	remote := []exchangeNodeState{{
		ID:              unknownID,
		AdvertiseAddr:   "",
		State:           NodeSuspect,
		StateChangeTime: 0,
	}}

	hm.combineRemoteNodeState(sender, remote)

	n := cluster.nodes.get(unknownID)
	if n == nil {
		t.Fatalf("suspect node not discovered via push/pull")
	}
	if n.state != NodeSuspect {
		t.Fatalf("discovered node state = %v, want %v", n.state, NodeSuspect)
	}
}

// Dead nodes should be removed after DeadNodeRetentionPeriod
func TestHealthMonitor_CleanupDeadNodes_RemovesAfterRetention(t *testing.T) {
	cluster := createTestCluster(t)
	hm := newHealthMonitor(cluster)

	// Make retention effectively immediate for test
	hm.config.DeadNodeRetentionPeriod = 1

	dead := createTestNode("ffffffff-ffff-ffff-ffff-ffffffffffff", NodeDead)
	dead.stateChangeTime = 0 // very old
	cluster.nodes.addIfNotExists(dead)

	hm.cleanupDeadNodes()

	if cluster.nodes.get(dead.ID) != nil {
		t.Fatalf("dead node was not removed after retention period")
	}
}

// Leaving nodes should transition to Dead after SuspectRetentionPeriod
func TestHealthMonitor_ProcessLeavingNodes_DeadAfterRetention(t *testing.T) {
	cluster := createTestCluster(t)
	hm := newHealthMonitor(cluster)

	// Make retention effectively immediate for test
	hm.config.SuspectRetentionPeriod = 1

	leaving := createTestNode("12345678-1234-1234-1234-1234567890ab", NodeLeaving)
	leaving.stateChangeTime = 0 // very old
	cluster.nodes.addIfNotExists(leaving)

	hm.processLeavingNodes()

	n := cluster.nodes.get(leaving.ID)
	if n == nil || n.state != NodeDead {
		t.Fatalf("leaving node not marked dead after retention period")
	}
}

// Helper to parse NodeID quickly in tests
func mustParseNodeID(s string) NodeID {
	id, err := uuid.Parse(s)
	if err != nil {
		panic(err)
	}
	return NodeID(id)
}
