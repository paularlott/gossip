package partition

// Destruction scenarios of the production-readiness hardening: resync from intact nodes only, the divergence alarm,
// eligibility under load, and forwarded writes that reach the owner close to their deadline.

import (
	"strings"
	"testing"
	"time"
)

// Two nodes lose their disks while the third, intact, is up. Neither may resync from the other: an answer from a node
// that is resyncing proves nothing, and only one intact node answers where two are needed, so both wait (DESIGN.md §7:
// an operator force-starts). Force-started, a node resyncs from the intact node alone; the other then has two intact
// nodes to resync from, and nothing committed is lost.
func TestDTTwoLostDisksNeedForceStart(t *testing.T) {
	force := false
	c := dtStartCluster(t, dtOpts{Nodes: 3, Tweak: func(i int, cfg *Config) {
		if i == 0 && force {
			cfg.ForceResync = true
		}
	}})
	w := c.startWorkload(dtWLOpts{})
	time.Sleep(2 * time.Second)
	w.finish()
	c.settle()

	c.crash(0)
	c.crash(1)
	c.restart(0, false)
	c.restart(1, false)
	time.Sleep(3 * time.Second)
	for _, i := range []int{0, 1} {
		m, _ := c.node(i)
		dtRequire(t, m.Resyncing() && !m.Ready(), "node %d resynced with only one intact node and one resyncing node answering", i)
	}

	c.crash(0)
	force = true
	c.restart(0, false)
	c.awaitReady(30*time.Second, 0)
	force = false
	m0, s0 := c.node(0)
	dtRequire(t, !m0.Resyncing(), "node 0 still resyncing with ForceResync")
	_, s2 := c.node(2)
	for p := 0; p < c.parts; p++ {
		a, b := s0.snap(p), s2.snap(p)
		dtRequire(t, a.St.Committed.Seq == b.St.Committed.Seq && len(a.Entries) == len(b.Entries),
			"partition %d: node 0 at %d, the intact node 2 at %d", p, a.St.Committed.Seq, b.St.Committed.Seq)
	}
	c.awaitReady(30*time.Second, 1) // node 1 now hears two intact nodes
	c.settle()
	c.verify()
}

// Every node goes down; two come back with their disks lost, one of them force-started, while the only intact node is
// still down. The forced node has nobody intact to resync from and must not complete from the empty node; once the intact
// node is back, both resync and nothing committed is lost.
func TestDTForceStartWithOnlyEmptyPeersRefuses(t *testing.T) {
	force := false
	c := dtStartCluster(t, dtOpts{Nodes: 3, Tweak: func(i int, cfg *Config) {
		if i == 0 && force {
			cfg.ForceResync = true
		}
	}})
	w := c.startWorkload(dtWLOpts{})
	time.Sleep(2 * time.Second)
	w.finish()
	c.settle()

	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	force = true
	c.restart(0, false)
	c.restart(1, false)
	time.Sleep(3 * time.Second)
	for _, i := range []int{0, 1} {
		m, _ := c.node(i)
		dtRequire(t, m.Resyncing() && !m.Ready(), "node %d completed a resync with no intact node answering", i)
	}
	for p := 0; p < c.parts; p++ {
		dtRequire(t, len(c.leaders(p)) == 0, "partition %d is led by %v with no intact node up", p, c.leaders(p))
	}
	c.restart(2, true)
	c.awaitReady(30 * time.Second)
	c.settle()
	c.verify()
}

// A follower comes back with committed data the owner does not have (a disk restored from the wrong place, reported
// intact). Once it has been ahead of every report of the owner for three OwnerTimeouts it is diverged: it no longer
// serves reads of the partition, never leads it, and none of what it holds reaches another node, not even when the owner
// is lost (the partition then waits, rather than take the follower's word for what is committed).
func TestDTDivergedFollowerWithdraws(t *testing.T) {
	const p = 0
	c := dtNewCluster(t, dtOpts{Nodes: 3, Parts: 1})
	order := c.rankOrder(p)
	a, g, f := order[0], order[1], order[2] // the owner, the next rank, the follower that diverges
	c.startAll()
	c.awaitReady(30 * time.Second)
	c.await(30*time.Second, "node A to own the partition", func() bool { return c.ownerOf(p) == a })
	for j := 0; j < 3; j++ {
		c.mustIncrement(a, p, dtKey(p, j), 10*time.Second)
	}
	c.awaitConverged(10 * time.Second)

	c.crash(f)
	_, sf := c.node(f)
	sf.memStore.mu.Lock()
	mp := sf.memStore.part(p)
	for j := uint64(1); j <= 3; j++ {
		seq := mp.st.Committed.Seq + 1
		mp.entries = append(mp.entries, memEntry{Seq: seq, Data: []byte(`{"k":"junk","v":1,"id":"junk"}`)})
		mp.st.Committed = Pos{Epoch: mp.st.Committed.Epoch, Seq: seq}
	}
	mp.st.Pending = nil
	sf.memStore.mu.Unlock()
	c.restart(f, true)

	mf, _ := c.node(f)
	c.await(mf.divergeAfter()+15*time.Second, "node F to find the partition diverged", func() bool {
		st, _ := mf.PartitionStatus(p)
		return st.Diverged
	})
	dtRequire(t, !mf.CanRead(p) && mf.Status().Divergences >= 1, "a diverged follower: CanRead %v, divergences %d", mf.CanRead(p), mf.Status().Divergences)
	noJunk := func(nodes ...int) {
		t.Helper()
		for _, i := range nodes {
			_, s := c.node(i)
			for _, e := range s.snap(p).Entries {
				dtRequire(t, !strings.Contains(string(e.Data), "junk"), "node %d holds the diverged node's data", i)
			}
		}
	}
	c.mustIncrement(a, p, dtKey(p, 0), 10*time.Second)
	noJunk(a, g)

	// The owner is lost: the next rank cannot take over with the diverged node alone (it gives no promise), and the
	// diverged node never leads.
	c.crash(a)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		dtRequire(t, !mf.IsOwner(p), "the diverged node took the partition over")
		time.Sleep(20 * time.Millisecond)
	}
	noJunk(g)
	c.restart(a, true)
	c.await(30*time.Second, "the owner or the next rank to lead again", func() bool {
		o := c.ownerOf(p)
		return o == a || o == g
	})
	c.mustIncrement(g, p, dtKey(p, 1), 15*time.Second)
	c.awaitConverged(15*time.Second, a, g)
	noJunk(a, g)
	st, _ := mf.PartitionStatus(p)
	dtRequire(t, st.Diverged && !st.CanRead && !st.Leading, "the diverged node: %+v", st)
	c.requireNoConflict()
}

// Under a workload, the node that leads most partitions becomes ineligible: it steps down at once, the others take over,
// it leads nothing while ineligible, and it serves again once eligible. Nothing acknowledged is lost.
func TestDTIneligibleOwnerUnderWorkload(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3, Parts: 6})
	w := c.startWorkload(dtWLOpts{})
	time.Sleep(time.Second)
	n, most := 0, -1
	for _, i := range c.upNodes() {
		k := 0
		for p := 0; p < c.parts; p++ {
			if m, _ := c.node(i); m.IsOwner(p) {
				k++
			}
		}
		if k > most {
			n, most = i, k
		}
	}
	m, _ := c.node(n)
	m.SetEligible(false)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for p := 0; p < c.parts; p++ {
			dtRequire(t, !m.IsOwner(p), "the ineligible node %d leads partition %d", n, p)
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.awaitOwners(15*time.Second, c.others(n)...)
	m.SetEligible(true)
	time.Sleep(time.Second)
	w.finish()
	c.settle()
	c.verify()
}

// Forwarded writes reach the owner with little of their requester's deadline left (the followers' requests to the owner
// are delayed by most of WriteTimeout; the owner's heartbeats and writes to them are not): the owner refuses those it
// could not finish in time, with nothing written, and runs the others. What every caller was told matches what is
// committed.
func TestDTForwardedWritesNearTheirDeadline(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3, Parts: 1})
	owner := c.ownerOf(0)
	for _, i := range c.others(owner) {
		c.Delay(i, owner, dtWriteTimeout-350*time.Millisecond, dtWriteTimeout-50*time.Millisecond)
	}
	w := c.startWorkload(dtWLOpts{Workers: 8, Readers: -1})
	// Wait for the refusals the test is about (and for some write to have been run) instead of a fixed window.
	c.await(30*time.Second, "a forwarded write refused near its deadline and another committed", func() bool {
		return c.ops.count(dtUnavailable) > 0 && c.ops.count(dtOK) > 0
	})
	w.finish()
	t.Logf("with delayed requests: %s", c.ops.summary())
	dtRequire(t, c.ops.count(dtUnavailable) > 0, "no forwarded write was refused near its deadline: %s", c.ops.summary())
	c.settle()
	c.verify()
}
