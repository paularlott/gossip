package partition

import (
	"sync"
	"testing"
	"time"
)

// A node that lost its disk while another node is down cannot resync: a majority of the other nodes is not there to
// answer. With ForceResync (an operator's decision) it resyncs from the one that is, and the cluster writes again.
func TestDTForceResyncWithAnotherNodeDown(t *testing.T) {
	force := false
	c := dtStartCluster(t, dtOpts{Nodes: 3, Tweak: func(i int, cfg *Config) {
		if i == 1 && force {
			cfg.ForceResync = true
		}
	}})
	w := c.startWorkload(dtWLOpts{})
	time.Sleep(2 * time.Second)
	w.finish()
	c.settle()

	c.crash(0)
	c.crash(1)
	c.restart(1, false) // its disk lost, node 0 down: one other node answers, not a majority of the others
	m, _ := c.node(1)
	time.Sleep(3 * time.Second)
	dtRequire(t, m.resync.Load(), "node 1 left resync with only one other node answering")
	dtRequire(t, !m.Ready(), "node 1 became ready without a majority of the other nodes")

	c.crash(1)
	force = true
	c.restart(1, false)
	c.awaitReady(30*time.Second, 1)
	m, s1 := c.node(1)
	dtRequire(t, !m.resync.Load(), "node 1 still resyncing with ForceResync")
	_, s2 := c.node(2)
	for p := 0; p < c.parts; p++ {
		a, _ := s1.State(p)
		b, _ := s2.State(p)
		dtRequire(t, a.Committed.Seq == b.Committed.Seq, "partition %d: node 1 at %d, node 2 at %d", p, a.Committed.Seq, b.Committed.Seq)
	}
	c.awaitOwners(30*time.Second, 1, 2) // two of three: the cluster writes again

	force = false
	c.restart(0, true)
	c.awaitReady(30 * time.Second)
	c.settle()
	c.verify()
}

// resyncQuorum: enough of the other nodes that every majority includes one of them.
func TestResyncQuorum(t *testing.T) {
	for size, want := range map[int]int{1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 4} {
		if got := resyncQuorum(size); got != want {
			t.Errorf("size %d: %d, want %d", size, got, want)
		}
		// at most size-majority other nodes hold none of a committed write: one more answer includes a holder
		if resyncQuorum(size) > size-1 && size > 1 {
			t.Errorf("size %d: needs %d of only %d others", size, resyncQuorum(size), size-1)
		}
	}
}

// In a cluster of two, a node that lost its disk resyncs from the other one: every majority (both) includes it.
func TestDTTwoNodeLostDiskResyncs(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 2})
	w := c.startWorkload(dtWLOpts{})
	time.Sleep(2 * time.Second)
	w.finish()
	c.settle()
	c.crash(1)
	c.restart(1, false)
	c.awaitReady(30*time.Second, 1)
	m, _ := c.node(1)
	dtRequire(t, !m.resync.Load(), "node 1 still resyncing")
	c.awaitOwners(30 * time.Second)
	c.settle()
	c.verify()
}

// Nodes with different ClusterIDs ignore each other completely: a node set up with another data set, in the same gossip
// cluster, never receives, gives or merges anything; given the cluster's ID (and a fresh store) it joins normally.
func TestDTDifferentClusterIDsNeverMerge(t *testing.T) {
	other := true
	c := dtNewCluster(t, dtOpts{Nodes: 3, Tweak: func(i int, cfg *Config) {
		cfg.ClusterID = "data-set-a"
		if i == 2 && other {
			cfg.ClusterID = "data-set-b"
		}
	}})
	c.startAll()
	c.awaitReady(30*time.Second, 0, 1)
	c.awaitOwners(30*time.Second, 0, 1)
	w := c.startWorkload(dtWLOpts{Nodes: func() []int { return []int{0, 1} }})
	time.Sleep(3 * time.Second)
	w.finish()
	m2, s2 := c.node(2)
	dtRequire(t, !m2.Ready(), "a node of another data set became ready")
	for p := 0; p < c.parts; p++ {
		st, _ := s2.State(p)
		dtRequire(t, st.Committed.Seq == 0 && st.Pending == nil && st.Promised.IsZero(),
			"partition %d: the other data set's node took part: %+v", p, st)
		dtRequire(t, len(c.leaders(p)) == 1 && c.leaders(p)[0] != 2, "partition %d led by %v", p, c.leaders(p))
	}
	other = false
	c.crash(2)
	c.restart(2, false)
	c.awaitReady(30 * time.Second)
	c.settle()
	c.verify()
}

// A new data set forms around one seeded node without ForceResync: the nodes are all unformed, so the empty ones copy
// from the founder alone. Once formed, the recovery quorum applies again.
func TestDTFormationAroundOneNode(t *testing.T) {
	var mu sync.Mutex
	formed := map[int]bool{}
	c := dtNewCluster(t, dtOpts{Nodes: 3, Tweak: func(i int, cfg *Config) {
		cfg.Formed = func() bool { mu.Lock(); defer mu.Unlock(); return formed[i] }
		cfg.SetFormed = func() error { mu.Lock(); defer mu.Unlock(); formed[i] = true; return nil }
	}})
	c.startAll()
	c.awaitReady(30 * time.Second)
	c.awaitOwners(30 * time.Second)
	mu.Lock()
	dtRequire(t, formed[0] && formed[1] && formed[2], "a new cluster's nodes have not formed once it is ready: %v", formed)
	mu.Unlock()
	w := c.startWorkload(dtWLOpts{})
	time.Sleep(2 * time.Second)
	w.finish()
	c.settle()

	// Node 0 now plays the founder, the one node holding a data set it was given (a restore or a seed), unformed. The
	// others start with nothing, unformed.
	c.crash(1)
	c.crash(2)
	c.crash(0)
	mu.Lock()
	formed[0], formed[1], formed[2] = false, false, false
	mu.Unlock()
	c.restart(0, true)
	c.restart(1, false)
	c.restart(2, false)
	c.awaitReady(30 * time.Second)
	c.awaitOwners(30 * time.Second)
	c.settle()
	c.verify()
	mu.Lock()
	dtRequire(t, formed[0] && formed[1] && formed[2], "not formed after forming: %v", formed)
	mu.Unlock()

	// Two nodes that lose their data again need the quorum, which node 0 alone is not.
	c.crash(1)
	c.crash(2)
	mu.Lock()
	formed[1], formed[2] = false, false // (the application resets the marker with the data)
	mu.Unlock()
	c.restart(1, false)
	c.restart(2, false)
	time.Sleep(3 * time.Second)
	m1, _ := c.node(1)
	m2, _ := c.node(2)
	dtRequire(t, m1.resync.Load() && m2.resync.Load(), "two nodes that lost their data resynced from one intact node")
}
