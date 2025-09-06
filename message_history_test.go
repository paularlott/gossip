package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
)

func TestNewMessageHistory(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 8

	mh := newMessageHistory(config)

	if mh.config != config {
		t.Error("Config not set correctly")
	}

	if mh.shardMask != 7 { // 8-1
		t.Errorf("Expected shardMask 7, got %d", mh.shardMask)
	}

	if len(mh.shards) != 8 {
		t.Errorf("Expected 8 shards, got %d", len(mh.shards))
	}

	for i, shard := range mh.shards {
		if shard == nil {
			t.Errorf("Shard %d is nil", i)
		}
		if shard.entries == nil {
			t.Errorf("Shard %d entries map is nil", i)
		}
	}

	mh.stop()
}

func TestGetShard(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 4
	mh := newMessageHistory(config)
	defer mh.stop()

	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())

	shard := mh.getShard(messageID)
	if shard == nil {
		t.Error("getShard returned nil")
	}

	// Test that same messageID returns same shard
	shard2 := mh.getShard(messageID)
	if shard != shard2 {
		t.Error("Same messageID should return same shard")
	}
}

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

func TestPruneHistory(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryGCInterval = 10 * time.Millisecond
	config.MsgHistoryMaxAge = 50 * time.Millisecond
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())

	// Record message
	mh.recordMessage(nodeID, messageID)

	// Verify it exists
	if !mh.contains(nodeID, messageID) {
		t.Error("Message should exist before pruning")
	}

	// Wait for pruning to occur
	time.Sleep(100 * time.Millisecond)

	// Message should be pruned
	if mh.contains(nodeID, messageID) {
		t.Error("Message should be pruned after max age")
	}
}

func TestShardDistribution(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 4
	mh := newMessageHistory(config)
	defer mh.stop()

	clock := hlc.NewClock()
	shardCounts := make(map[*historyShard]int)

	// Generate many messages and check distribution
	for i := 0; i < 1000; i++ {
		messageID := MessageID(clock.Now())
		shard := mh.getShard(messageID)
		shardCounts[shard]++
	}

	// Should use all shards
	if len(shardCounts) != 4 {
		t.Errorf("Expected 4 shards to be used, got %d", len(shardCounts))
	}

	// Check reasonable distribution (each shard should have some messages)
	for shard, count := range shardCounts {
		if count == 0 {
			t.Errorf("Shard %p has no messages", shard)
		}
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

func TestMessageKeyUniqueness(t *testing.T) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID1 := NodeID(uuid.New())
	nodeID2 := NodeID(uuid.New())
	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())

	// Record same messageID for different nodes
	mh.recordMessage(nodeID1, messageID)
	mh.recordMessage(nodeID2, messageID)

	// Both should be contained
	if !mh.contains(nodeID1, messageID) {
		t.Error("Node1 message should be contained")
	}
	if !mh.contains(nodeID2, messageID) {
		t.Error("Node2 message should be contained")
	}
}

func TestEmptyHistory(t *testing.T) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())

	// Should not contain anything in empty history
	if mh.contains(nodeID, messageID) {
		t.Error("Empty history should not contain any messages")
	}
}

func TestLargeShardCount(t *testing.T) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 64
	mh := newMessageHistory(config)
	defer mh.stop()

	if len(mh.shards) != 64 {
		t.Errorf("Expected 64 shards, got %d", len(mh.shards))
	}

	if mh.shardMask != 63 { // 64-1
		t.Errorf("Expected shardMask 63, got %d", mh.shardMask)
	}
}

// Benchmarks

func BenchmarkRecordMessage(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messageID := MessageID(clock.Now())
		mh.recordMessage(nodeID, messageID)
	}
}

func BenchmarkContains(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())
	mh.recordMessage(nodeID, messageID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mh.contains(nodeID, messageID)
	}
}

func BenchmarkGetShard(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mh.getShard(messageID)
	}
}

func BenchmarkConcurrentRecordMessage(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		nodeID := NodeID(uuid.New())
		clock := hlc.NewClock()
		for pb.Next() {
			messageID := MessageID(clock.Now())
			mh.recordMessage(nodeID, messageID)
		}
	})
}

func BenchmarkConcurrentContains(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	// Pre-populate with some messages
	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()
	for i := 0; i < 1000; i++ {
		messageID := MessageID(clock.Now())
		mh.recordMessage(nodeID, messageID)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			messageID := MessageID(clock.Now())
			mh.contains(nodeID, messageID)
		}
	})
}

func BenchmarkShardCount16(b *testing.B) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 16
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messageID := MessageID(clock.Now())
		mh.recordMessage(nodeID, messageID)
	}
}

func BenchmarkShardCount64(b *testing.B) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 64
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messageID := MessageID(clock.Now())
		mh.recordMessage(nodeID, messageID)
	}
}

func BenchmarkMixedOperations(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messageID := MessageID(clock.Now())
		mh.recordMessage(nodeID, messageID)
		mh.contains(nodeID, messageID)
	}
}

// Test memory allocation patterns
func BenchmarkRecordMessageAllocs(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		messageID := MessageID(clock.Now())
		mh.recordMessage(nodeID, messageID)
	}
}

// Test with realistic gossip patterns
func BenchmarkGossipPattern(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	// Simulate 10 nodes
	nodeIDs := make([]NodeID, 10)
	for i := range nodeIDs {
		nodeIDs[i] = NodeID(uuid.New())
	}
	clock := hlc.NewClock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := nodeIDs[i%len(nodeIDs)]
		messageID := MessageID(clock.Now())

		// 80% record, 20% check (typical gossip pattern)
		if i%5 == 0 {
			mh.contains(nodeID, messageID)
		} else {
			mh.recordMessage(nodeID, messageID)
		}
	}
}
