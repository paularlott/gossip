package gossip

import (
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
)

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

func BenchmarkMessageHistoryShardSelection(b *testing.B) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 64
	mh := newMessageHistory(config)
	defer mh.stop()

	clock := hlc.NewClock()
	messageIDs := make([]MessageID, 1000)
	for i := 0; i < 1000; i++ {
		messageIDs[i] = MessageID(clock.Now())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mh.getShard(messageIDs[i%len(messageIDs)])
	}
}

func BenchmarkMessageHistoryContainsMiss(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messageID := MessageID(clock.Now())
		mh.contains(nodeID, messageID) // Always miss
	}
}

func BenchmarkMessageHistoryContainsHit(b *testing.B) {
	config := DefaultConfig()
	mh := newMessageHistory(config)
	defer mh.stop()

	nodeID := NodeID(uuid.New())
	clock := hlc.NewClock()
	messageID := MessageID(clock.Now())
	mh.recordMessage(nodeID, messageID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mh.contains(nodeID, messageID) // Always hit
	}
}

func BenchmarkMessageHistoryHighContentionSingleShard(b *testing.B) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 1 // Force contention
	mh := newMessageHistory(config)
	defer mh.stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		nodeID := NodeID(uuid.New())
		clock := hlc.NewClock()
		for pb.Next() {
			messageID := MessageID(clock.Now())
			mh.recordMessage(nodeID, messageID)
			mh.contains(nodeID, messageID)
		}
	})
}

func BenchmarkMessageHistoryLowContentionManyShards(b *testing.B) {
	config := DefaultConfig()
	config.MsgHistoryShardCount = 128 // Reduce contention
	mh := newMessageHistory(config)
	defer mh.stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		nodeID := NodeID(uuid.New())
		clock := hlc.NewClock()
		for pb.Next() {
			messageID := MessageID(clock.Now())
			mh.recordMessage(nodeID, messageID)
			mh.contains(nodeID, messageID)
		}
	})
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
