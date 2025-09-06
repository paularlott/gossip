package gossip

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

func newTestCluster(t *testing.T) *Cluster {
	config := DefaultConfig()
	config.NodeShardCount = 4
	config.MsgCodec = codec.NewJsonCodec()
	config.SocketTransportEnabled = false // Disable network binding for tests using only in-memory structures
	// Minimal transport / codec placeholders for tests
	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	return cluster
}

// Using built-in JSON codec for tests

func TestNodeListAddAndGet(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	id := NodeID(uuid.New())
	n := newNode(id, "127.0.0.1:1000")
	if !nl.addIfNotExists(n) {
		t.Fatal("expected addIfNotExists true")
	}
	if nl.addIfNotExists(n) {
		t.Fatal("expected second addIfNotExists false")
	}

	g := nl.get(id)
	if g == nil || g.ID != id {
		t.Fatalf("expected to retrieve node")
	}
}

func TestNodeListStateTransitions(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	id := NodeID(uuid.New())
	n := newNode(id, "127.0.0.1:2000")
	nl.addIfNotExists(n)

	if !nl.updateState(id, NodeSuspect) {
		t.Fatalf("failed to mark suspect")
	}
	if nl.getSuspectCount() != 1 {
		t.Fatalf("suspect count mismatch")
	}
	if !nl.updateState(id, NodeDead) {
		t.Fatalf("failed to mark dead")
	}
	if nl.getDeadCount() != 1 {
		t.Fatalf("dead count mismatch")
	}
}

func TestNodeListRemove(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes
	id := NodeID(uuid.New())
	n := newNode(id, "addr")
	nl.addIfNotExists(n)
	// +1 for local node
	if nl.getAliveCount() != 2 {
		t.Fatalf("alive count 2 expected, got %d", nl.getAliveCount())
	}
	if !nl.removeIfInState(id, []NodeState{NodeAlive}) {
		t.Fatalf("expected removal")
	}
	if nl.getAliveCount() != 1 {
		t.Fatalf("alive count should be 1 (local node), got %d", nl.getAliveCount())
	}
}

func TestNodeListQueries(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes

	oldTs := hlc.Now()
	time.Sleep(10 * time.Millisecond)
	idOld := NodeID(uuid.New())
	oldNode := newNode(idOld, "old")
	oldNode.stateChangeTime = oldTs
	nl.addIfNotExists(oldNode)

	idNew := NodeID(uuid.New())
	newNodeObj := newNode(idNew, "new")
	nl.addIfNotExists(newNodeObj)

	cutoff := hlc.Now()
	older := nl.getNodesWithStateChangeBefore(cutoff, []NodeState{NodeAlive})
	found := false
	for _, n := range older {
		if n.ID == idOld {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected to find old node by state change time")
	}
}

func TestNodeListOldMetadata(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes
	id := NodeID(uuid.New())
	n := newNode(id, "meta")
	base := hlc.Now()
	n.lastMetadataTime = base
	nl.addIfNotExists(n)
	olderCutoff := hlc.Timestamp(uint64(base) - 1)
	if !nl.updateMetadataTimestamp(id, hlc.Timestamp(uint64(base)+5)) {
		t.Fatalf("expected metadata timestamp update")
	}
	// Cutoff after update -> node IS returned as stale (older than cutoff)
	res := nl.getNodesWithOldMetadata(hlc.Timestamp(uint64(base) + 10))
	found := false
	for _, node := range res {
		if node.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected node to be considered old with large cutoff")
	}
	// Cutoff just after original (base+1) but before update (base+5) -> node is newer than cutoff -> not returned
	res2 := nl.getNodesWithOldMetadata(hlc.Timestamp(uint64(base) + 1))
	for _, node := range res2 {
		if node.ID == id {
			t.Fatalf("node should not appear old with early cutoff")
		}
	}
	// Very old cutoff (older than original) -> node newer -> not returned
	res3 := nl.getNodesWithOldMetadata(olderCutoff)
	for _, node := range res3 {
		if node.ID == id {
			t.Fatalf("node should not appear old with very early cutoff")
		}
	}
}

func TestLeastRecentlyActiveAlive(t *testing.T) {
	c := newTestCluster(t)
	nl := c.nodes
	id1 := NodeID(uuid.New())
	n1 := newNode(id1, "a")
	n1.lastMessageTime = hlc.Now()
	nl.addIfNotExists(n1)
	time.Sleep(time.Microsecond)
	id2 := NodeID(uuid.New())
	n2 := newNode(id2, "b")
	n2.lastMessageTime = hlc.Now()
	nl.addIfNotExists(n2)
	// Ensure local node activity is newest so it doesn't appear first
	c.localNode.lastMessageTime = hlc.Now()
	list := nl.getLeastRecentlyActiveAlive(3)
	// First non-local node should be n1 (oldest among test nodes)
	foundOrder := make([]NodeID, 0)
	for _, n := range list {
		if n.ID != c.localNode.ID {
			foundOrder = append(foundOrder, n.ID)
		}
	}
	if len(foundOrder) == 0 || foundOrder[0] != id1 {
		t.Fatalf("expected first non-local node to be id1")
	}
}

// --- Benchmarks ---

// helper to populate a cluster with N nodes
func populateCluster(t testing.TB, count int) (*Cluster, []NodeID) {
	c := newTestCluster(&testing.T{})
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
	c := newTestCluster(&testing.T{})
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

func BenchmarkNodeListLeastRecentlyActive(b *testing.B) {
	c, _ := populateCluster(b, 20_000)
	// Randomize activity times
	for _, n := range c.nodes.getAll() {
		// Add some variability
		if rand.Intn(4) == 0 {
			n.lastMessageTime = hlc.Now()
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.nodes.getLeastRecentlyActiveAlive(64)
	}
}

func BenchmarkNodeListQueries(b *testing.B) {
	c, _ := populateCluster(b, 30_000)
	cutoff := hlc.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.nodes.getNodesWithStateChangeBefore(cutoff, []NodeState{NodeAlive, NodeSuspect})
		_ = c.nodes.getNodesWithOldMetadata(cutoff)
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
