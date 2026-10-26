package gossip

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
)

// helper to populate a cluster with N nodes
func populateCluster(t testing.TB, count int) (*Cluster, []NodeID) {
	c := newUnitCluster(t)
	ids := make([]NodeID, 0, count)
	for i := 0; i < count; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "node")
		// Spread timestamps a little
		n.lastMessageTime = hlc.Now()
		c.nodes.addIfNotExists(n)
		ids = append(ids, id)
	}
	return c, ids
}

func BenchmarkNodeListAdd(b *testing.B) {
	c := newUnitCluster(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := NodeID(uuid.New())
		n := newNode(id, "addr")
		c.nodes.addIfNotExists(n)
	}
}

func BenchmarkNodeListUpdateState(b *testing.B) {
	c, ids := populateCluster(b, 10_000)
	// Ensure all start Alive
	for _, id := range ids {
		_ = c.nodes.updateState(id, NodeAlive)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i%len(ids)]
		// Toggle Alive/Suspect to exercise counter changes
		if i%2 == 0 {
			c.nodes.updateState(id, NodeSuspect)
		} else {
			c.nodes.updateState(id, NodeAlive)
		}
	}
}

func BenchmarkNodeListGetRandomNodes(b *testing.B) {
	c, _ := populateCluster(b, 25_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.nodes.getRandomNodes(32, nil)
	}
}

func BenchmarkNodeListConcurrentRandom(b *testing.B) {
	c, _ := populateCluster(b, 40_000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.nodes.getRandomNodes(16, nil)
		}
	})
}

func BenchmarkNodeListGetShard(b *testing.B) {
	c := newUnitCluster(b)
	nl := c.nodes
	id := NodeID(uuid.New())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.getShard(id)
	}
}

func BenchmarkNodeListGet(b *testing.B) {
	c, ids := populateCluster(b, 10000)
	nl := c.nodes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.get(ids[i%len(ids)])
	}
}

func BenchmarkNodeListGetAllInStates(b *testing.B) {
	c, _ := populateCluster(b, 10000)
	nl := c.nodes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.getAllInStates([]NodeState{NodeAlive, NodeSuspect})
	}
}

func BenchmarkNodeListForAllInStates(b *testing.B) {
	c, _ := populateCluster(b, 10000)
	nl := c.nodes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.forAllInStates([]NodeState{NodeAlive}, func(*Node) bool {
			return true
		})
	}
}

func BenchmarkNodeListStateCounters(b *testing.B) {
	c, _ := populateCluster(b, 1000)
	nl := c.nodes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nl.getAliveCount()
		nl.getSuspectCount()
		nl.getLeavingCount()
		nl.getDeadCount()
	}
}

func BenchmarkNodeListConcurrentStateUpdates(b *testing.B) {
	c, ids := populateCluster(b, 1000)
	nl := c.nodes

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := ids[rand.Intn(len(ids))]
			state := []NodeState{NodeAlive, NodeSuspect, NodeDead}[rand.Intn(3)]
			nl.updateState(id, state)
		}
	})
}
