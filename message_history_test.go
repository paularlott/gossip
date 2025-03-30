package gossip

import (
	"testing"
	"time"
)

// MockNodeID creates a NodeID for testing
func mockNodeID(id byte) NodeID {
	nodeID := NodeID{}
	for i := range nodeID {
		nodeID[i] = id
	}
	return nodeID
}

func TestMessageHistoryBasic(t *testing.T) {
	config := &Config{
		MsgHistoryShardCount: 4,
		MsgHistoryMaxAge:     100 * time.Millisecond,
		MsgHistoryGCInterval: 50 * time.Millisecond,
	}

	mh := newMessageHistory(config)
	defer mh.stop()

	// Test recording and checking messages
	node1 := mockNodeID(1)
	node2 := mockNodeID(2)

	msg1 := MessageID{Timestamp: 100, Seq: 1}
	msg2 := MessageID{Timestamp: 200, Seq: 2}

	// Should not contain messages initially
	if mh.contains(node1, msg1) {
		t.Error("New history should not contain any messages")
	}

	// Record messages
	mh.recordMessage(node1, msg1)
	mh.recordMessage(node2, msg2)

	// Should contain recorded messages
	if !mh.contains(node1, msg1) {
		t.Error("History should contain recorded message node1:msg1")
	}
	if !mh.contains(node2, msg2) {
		t.Error("History should contain recorded message node2:msg2")
	}

	// Should not contain unrecorded messages
	if mh.contains(node1, msg2) {
		t.Error("History should not contain unrecorded message node1:msg2")
	}
	if mh.contains(node2, msg1) {
		t.Error("History should not contain unrecorded message node2:msg1")
	}
}

func TestMessageHistoryPruning(t *testing.T) {
	config := &Config{
		MsgHistoryShardCount: 4,
		MsgHistoryMaxAge:     100 * time.Millisecond,
		MsgHistoryGCInterval: 50 * time.Millisecond,
	}

	mh := newMessageHistory(config)
	defer mh.stop()

	node1 := mockNodeID(1)
	msg1 := MessageID{Timestamp: 100, Seq: 1}

	// Record message
	mh.recordMessage(node1, msg1)

	// Should contain message initially
	if !mh.contains(node1, msg1) {
		t.Error("History should contain recorded message")
	}

	// Wait for pruning (a bit more than maxAge + GCInterval)
	time.Sleep(200 * time.Millisecond)

	// Message should be pruned
	if mh.contains(node1, msg1) {
		t.Error("Message should have been pruned after maxAge")
	}
}

func TestMessageHistorySharding(t *testing.T) {
	config := &Config{
		MsgHistoryShardCount: 4,
		MsgHistoryMaxAge:     1 * time.Hour,
		MsgHistoryGCInterval: 1 * time.Hour,
	}

	mh := newMessageHistory(config)
	defer mh.stop()

	// Create different node IDs
	nodes := make([]NodeID, 10)
	for i := range nodes {
		nodes[i] = mockNodeID(byte(i))
	}

	// Add 100 messages per node
	for _, node := range nodes {
		for i := 0; i < 100; i++ {
			msg := MessageID{Timestamp: int64(i), Seq: 0}
			mh.recordMessage(node, msg)
		}
	}

	// Verify all messages are found
	for _, node := range nodes {
		for i := 0; i < 100; i++ {
			msg := MessageID{Timestamp: int64(i), Seq: 0}
			if !mh.contains(node, msg) {
				t.Errorf("Message should be found for node %v, msg %d", node, msg)
			}
		}
	}

	// Check that shards are reasonably balanced by inspecting the length
	// of entries in each shard
	counts := make([]int, mh.config.MsgHistoryShardCount)
	for i, shard := range mh.shards {
		shard.mutex.RLock()
		counts[i] = len(shard.entries)
		shard.mutex.RUnlock()
	}

	// Simple check that no shard is empty
	for i, count := range counts {
		if count == 0 {
			t.Errorf("Shard %d is empty, expected some entries", i)
		}
	}
}
