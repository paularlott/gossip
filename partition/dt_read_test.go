package partition

// Linearizable reads under faults. The workloads of the other scenarios also make reads, and
// verify checks them against the committed log (invariant 4 for reads); these scenarios aim the reads at the moments where a
// lagging or deposed node could answer wrongly.

import (
	"errors"
	"testing"
	"time"
)

// An owner that has been deposed without knowing it (cut off from the others, who have a new owner that committed a write)
// never returns the old value: its confirmation round finds no majority, so it fails the read, and once the network is
// back the read is answered with the new value. The node's own copy does lack the new write the whole time, so answering
// from it, as a local read does, would have returned the old value.
func TestDTDeposedOwnerNeverReturnsTheOldValue(t *testing.T) {
	// Node 0 takes long to notice that it has lost its majority (a long OwnerTimeout), so that it believes it owns the
	// partition for seconds after the others have replaced it.
	c := dtStartCluster(t, dtOpts{Nodes: 3, Parts: 16, Tweak: func(i int, cfg *Config) {
		if i == 0 {
			cfg.OwnerTimeout = 3 * time.Second
			cfg.StaleAfter = dtStale // CanRead's bound stays the suite's (invariant 5 is checked against it)
			cfg.Tuning.BackoffMax = dtBackoff
			cfg.WriteTimeout = 6 * time.Second
		}
	}})
	p := -1
	for q := 0; q < c.parts; q++ {
		if c.ownerOf(q) == 0 {
			p = q
			break
		}
	}
	dtRequire(t, p >= 0, "node 0 owns no partition")
	key := dtKey(p, 0)
	c.mustIncrement(0, p, key, 10*time.Second)
	c.awaitConverged(10 * time.Second)
	if r := c.read(0, p, key); !r.ok || r.v != 1 {
		t.Fatalf("read before the fault: %+v", r)
	}

	c.Isolate(0)
	rest := c.others(0)
	c.await(15*time.Second, "a new owner among the other nodes", func() bool {
		for _, i := range rest {
			if m, _ := c.node(i); m.IsOwner(p) {
				return true
			}
		}
		return false
	})
	op := c.mustIncrement(rest[0], p, key, dtLiveness) // committed at version 2 by the new owner, before the reads below
	m0, s0 := c.node(0)
	dtRequire(t, m0.IsOwner(p), "node 0 stepped down before the reads began: the scenario did not arise")
	dtRequire(t, s0.version(p, key) == op.exp, "node 0 already holds the new write (version %d)", s0.version(p, key))

	// Reads through the deposed owner, all begun after the new write was acknowledged: none may be answered.
	done := make(chan *dtReadOp, 4)
	for k := 0; k < 4; k++ {
		go func() { done <- c.read(0, p, key) }()
	}
	for k := 0; k < 4; k++ {
		if r := <-done; r.ok {
			t.Errorf("the deposed owner answered a read with version %d (seq %d), the committed version is %d", r.v, r.seq, op.exp+1)
		} else if !errors.Is(r.err, ErrUnavailable) {
			t.Errorf("read through the deposed owner: %v", r.err)
		}
	}
	dtRequire(t, !m0.IsOwner(p), "the deposed owner did not step down after its confirmation round failed")

	// Healed, the same read is answered, with the new value: routed to the new owner (or served by an owner that confirmed
	// itself).
	c.Heal()
	deadline := time.Now().Add(dtLiveness)
	for {
		r := c.read(0, p, key)
		if r.ok {
			if r.v < op.exp+1 {
				t.Fatalf("read after healing returned version %d, want at least %d", r.v, op.exp+1)
			}
			break
		}
		if time.Now().After(deadline) {
			c.dump()
			t.Fatalf("no read through node 0 succeeded after healing: %v", r.err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.settle()
	c.verify()
}

// Reads from every node, while the owners of the partitions are crashed and restarted and nodes are cut off in turn, are
// answered with everything committed before them or fail; verify checks every one against the committed log.
func TestDTReadsAcrossFailover(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	w := c.startWorkload(dtWLOpts{Workers: 3, Readers: 4, Pace: 10 * time.Millisecond})
	c.awaitCommits(10*time.Second, 20)
	for round := 0; round < 3; round++ {
		i := round % 3
		c.crash(i)
		c.awaitOwners(15 * time.Second) // the survivors have taken over the crashed node's partitions
		c.awaitCommits(10*time.Second, 20)
		c.restart(i, true)
		c.awaitReady(30*time.Second, i)
		c.awaitCommits(10*time.Second, 20)
		c.Cut((i + 1) % 3)
		// Long enough (more than dtStale+dtStaleSlack) for the monitor to catch a cut-off node that goes on reading.
		time.Sleep(1500 * time.Millisecond)
		c.Uncut((i + 1) % 3)
		c.awaitOwners(15 * time.Second)
		c.awaitCommits(10*time.Second, 20)
	}
	w.finish()
	c.settle()
	reads, ok := c.ops.allReads(), 0
	for _, r := range reads {
		if r.ok {
			ok++
		}
	}
	if ok == 0 {
		t.Error("no read succeeded")
	}
	t.Logf("%d reads, %d answered", len(reads), ok)
	c.verify()
}

// Reads concurrent with writes on the same keys, through every node, with no faults: every one is answered, and verify
// finds none behind a write that had returned.
func TestDTReadsAreCurrentWithoutFaults(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	w := c.startWorkload(dtWLOpts{Workers: 4, Readers: 4, Pace: 5 * time.Millisecond, Keys: 2})
	time.Sleep(4 * time.Second)
	w.finish()
	c.settle()
	for _, r := range c.ops.allReads() {
		if !r.ok {
			t.Errorf("read of %s via node %d failed without any fault: %v", r.key, r.via, r.err)
		}
	}
	c.verify()
}
