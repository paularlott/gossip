package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordMessage(t *testing.T) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())

	// Record message
	mh.recordMessage(nodeID, messageID)

	// Verify it was recorded
	if !mh.contains(nodeID, messageID) {
		t.Error("Message should be recorded")
	}
}

func TestContains(t *testing.T) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())

	// Should not contain before recording
	if mh.contains(nodeID, messageID) {
		t.Error("Message should not be contained before recording")
	}

	// Record and check
	mh.recordMessage(nodeID, messageID)
	if !mh.contains(nodeID, messageID) {
		t.Error("Message should be contained after recording")
	}

	// Different nodeID should not contain
	otherNodeID := NodeID(uuid.New())
	if mh.contains(otherNodeID, messageID) {
		t.Error("Different nodeID should not contain the message")
	}

	// Different messageID should not contain
	otherMessageID := MessageID(clock.Now())
	if mh.contains(nodeID, otherMessageID) {
		t.Error("Different messageID should not contain the message")
	}
}

func TestConcurrentAccess(t *testing.T) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	const numGoroutines = 10
	const numMessages = 100

	var wg sync.WaitGroup
	clock := hlc.NewClock()

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			nodeID := NodeID(uuid.New())
			for j := 0; j < numMessages; j++ {
				messageID := MessageID(clock.Now())
				mh.recordMessage(nodeID, messageID)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodeID := NodeID(uuid.New())
			messageID := MessageID(clock.Now())
			for j := 0; j < numMessages; j++ {
				mh.contains(nodeID, messageID)
			}
		}()
	}

	wg.Wait()
}

// Messages are forgotten once they are older than MsgHistoryMaxAge.
func TestPruneHistory(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryGCInterval = 5 * time.Millisecond
	config.MsgHistoryMaxAge = 20 * time.Millisecond
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	messageID := MessageID(hlc.Now())
	mh.recordMessage(nodeID, messageID)
	require.True(t, mh.contains(nodeID, messageID), "recorded")

	require.Eventually(t, func() bool { return !mh.contains(nodeID, messageID) }, 5*time.Second, 5*time.Millisecond,
		"the message was not pruned after its maximum age")
}

// Whatever the node id and message id, a recorded message is found again.
func TestMessageHistoryEdgeKeys(t *testing.T) {
	mh := newMessageHistory(DefaultConfig())
	defer mh.stop()

	for name, key := range map[string]messageKey{
		"empty node id":   {nodeID: NodeID(uuid.Nil), messageID: MessageID(hlc.Now())},
		"zero message id": {nodeID: NodeID(uuid.New()), messageID: 0},
	} {
		require.False(t, mh.contains(key.nodeID, key.messageID), name)
		mh.recordMessage(key.nodeID, key.messageID)
		require.True(t, mh.contains(key.nodeID, key.messageID), name)
	}
}

// Messages made a microsecond apart spread over the shards evenly, so no one shard (and lock) takes the load.
func TestMessageHistoryShardsShareTheLoad(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 8
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	base := time.Now()
	for i := range 800 {
		mh.recordMessage(nodeID, MessageID(hlc.FromTime(base.Add(time.Duration(i)*time.Microsecond))))
	}

	for i, shard := range mh.shards {
		shard.mutex.RLock()
		assert.Len(t, shard.entries, 100, "shard %d", i)
		shard.mutex.RUnlock()
	}
}

func TestStop(t *testing.T) {
	config := DefaultConfig()
	mh := newMessageHistory(config)

	// Stop should not panic
	mh.stop()

	// Multiple stops should not panic
	mh.stop()
}

func TestMessageHistoryMultipleNodes(t *testing.T) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())

	// Record same messageID for multiple nodes
	nodes := make([]NodeID, 10)
	for i := 0; i < 10; i++ {
		nodes[i] = NodeID(uuid.New())
		mh.recordMessage(nodes[i], messageID)
	}

	// All should be contained
	for i, nodeID := range nodes {
		if !mh.contains(nodeID, messageID) {
			t.Errorf("Node %d should contain messageID", i)
		}
	}

	// Different messageID should not be contained
	otherMessageID := MessageID(clock.Now())
	for i, nodeID := range nodes {
		if mh.contains(nodeID, otherMessageID) {
			t.Errorf("Node %d should not contain different messageID", i)
		}
	}
}
