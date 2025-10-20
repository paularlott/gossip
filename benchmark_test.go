package gossip

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/logger"
)

// BenchmarkMessageBroadcast benchmarks broadcasting messages
func BenchmarkMessageBroadcast(b *testing.B) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Transport = NewSocketTransport(config)
	config.MsgCodec = codec.NewShamatonMsgpackCodec()
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}

	// Add some test nodes
	for i := 0; i < 100; i++ {
		node := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8000+i))
		node.observedState = NodeAlive
		cluster.nodes.addOrUpdate(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cluster.Send(UserMsg, []byte("test message"))
	}
}

// BenchmarkTaggedMessageBroadcast benchmarks tagged message broadcasting
func BenchmarkTaggedMessageBroadcast(b *testing.B) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Tags = []string{"bench"}
	config.Transport = NewSocketTransport(config)
	config.MsgCodec = codec.NewShamatonMsgpackCodec()
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}

	// Add 1000 nodes, 10% with "bench" tag
	for i := 0; i < 1000; i++ {
		var node *Node
		if i%10 == 0 {
			node = newNodeWithTags(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8000+i), []string{"bench"})
		} else {
			node = newNodeWithTags(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8000+i), []string{"other"})
		}
		node.observedState = NodeAlive
		cluster.nodes.addOrUpdate(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cluster.SendTagged("bench", UserMsg, []byte("test message"))
	}
}

// BenchmarkRandomNodeSelection benchmarks random node selection
func BenchmarkRandomNodeSelection(b *testing.B) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}

	// Add 1000 nodes
	for i := 0; i < 1000; i++ {
		node := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8000+i))
		node.observedState = NodeAlive
		cluster.nodes.addOrUpdate(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cluster.nodes.getRandomNodes(10, []NodeID{})
	}
}

// BenchmarkNodeListGetRandomNodesWithTag benchmarks tagged random node selection
func BenchmarkNodeListGetRandomNodesWithTag(b *testing.B) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}

	// Add 1000 nodes, 10% with target tag
	for i := 0; i < 1000; i++ {
		var node *Node
		if i%10 == 0 {
			node = newNodeWithTags(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8000+i), []string{"target"})
		} else {
			node = newNodeWithTags(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8000+i), []string{"other"})
		}
		node.observedState = NodeAlive
		cluster.nodes.addOrUpdate(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cluster.nodes.getRandomNodesWithTag(10, "target", []NodeID{})
	}
}

// BenchmarkNodeHasTag benchmarks the HasTag method
func BenchmarkNodeHasTag(b *testing.B) {
	node := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8000", []string{"tag1", "tag2", "tag3", "tag4", "tag5"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = node.HasTag("tag3")
	}
}

// BenchmarkGetNodesByTag benchmarks GetNodesByTag
func BenchmarkGetNodesByTag(b *testing.B) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}

	// Add 1000 nodes, 10% with target tag
	for i := 0; i < 1000; i++ {
		var node *Node
		if i%10 == 0 {
			node = newNodeWithTags(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8000+i), []string{"target"})
		} else {
			node = newNodeWithTags(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8000+i), []string{"other"})
		}
		node.observedState = NodeAlive
		cluster.nodes.addOrUpdate(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cluster.GetNodesByTag("target")
	}
}

// BenchmarkMetadataOperations benchmarks metadata set/get operations
func BenchmarkMetadataOperations(b *testing.B) {
	metadata := NewMetadata()

	b.Run("SetString", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			metadata.SetString("key", "value")
		}
	})

	b.Run("GetString", func(b *testing.B) {
		metadata.SetString("key", "value")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metadata.GetString("key")
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		for i := 0; i < 10; i++ {
			metadata.SetString(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metadata.GetAll()
		}
	})
}

// BenchmarkPacketPool benchmarks packet allocation/release
func BenchmarkPacketPool(b *testing.B) {
	b.Run("AllocateRelease", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			packet := NewPacket()
			packet.Release()
		}
	})
}

// BenchmarkMessageHistory benchmarks message history operations
func BenchmarkMessageHistory(b *testing.B) {
	config := DefaultConfig()
	config.Logger = logger.NewNullLogger()
	history := newMessageHistory(config)

	b.Run("Record", func(b *testing.B) {
		nodeID := NodeID(uuid.New())
		for i := 0; i < b.N; i++ {
			history.recordMessage(nodeID, MessageID(i))
		}
	})

	b.Run("Contains", func(b *testing.B) {
		nodeID := NodeID(uuid.New())
		history.recordMessage(nodeID, MessageID(12345))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = history.contains(nodeID, MessageID(12345))
		}
	})
}

// BenchmarkConcurrentNodeAccess benchmarks concurrent node list access
func BenchmarkConcurrentNodeAccess(b *testing.B) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}

	// Add 1000 nodes
	nodeIDs := make([]NodeID, 1000)
	for i := 0; i < 1000; i++ {
		nodeIDs[i] = NodeID(uuid.New())
		node := newNode(nodeIDs[i], fmt.Sprintf("127.0.0.1:%d", 8000+i))
		node.observedState = NodeAlive
		cluster.nodes.addOrUpdate(node)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Mix of operations
			switch i % 4 {
			case 0:
				_ = cluster.nodes.get(nodeIDs[i%len(nodeIDs)])
			case 1:
				_ = cluster.nodes.getRandomNodes(5, []NodeID{})
			case 2:
				_ = cluster.nodes.getAliveCount()
			case 3:
				_ = cluster.nodes.getAllInStates([]NodeState{NodeAlive})
			}
			i++
		}
	})
}

// BenchmarkShardDistribution benchmarks shard distribution
func BenchmarkShardDistribution(b *testing.B) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = logger.NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}

	nodeIDs := make([]NodeID, 10000)
	for i := 0; i < 10000; i++ {
		nodeIDs[i] = NodeID(uuid.New())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cluster.nodes.getShard(nodeIDs[i%len(nodeIDs)])
	}
}
