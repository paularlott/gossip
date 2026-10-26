package partition

// Destruction scenarios: one test per failure story, run on real clusters over faultnet. Each ends with settle (heal,
// restart what is down, liveness) and verify (invariants 1-5 against everything that was recorded).

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dtStartCluster starts every node and waits until all are ready and every partition has one owner.
func dtStartCluster(t *testing.T, o dtOpts) *dtCluster {
	t.Helper()
	c := dtNewCluster(t, o)
	c.startAll()
	c.awaitReady(30 * time.Second)
	c.awaitOwners(30 * time.Second)
	return c
}

// others lists the nodes other than i.
func (c *dtCluster) others(i int) []int {
	var out []int
	for j := range c.nodes {
		if j != i {
			out = append(out, j)
		}
	}
	return out
}

// committedIDs returns the operations in the committed log of the first node that is up, with their sequence numbers.
func (c *dtCluster) committedIDs() map[string]uint64 {
	out := map[string]uint64{}
	up := c.upNodes()
	if len(up) == 0 {
		return out
	}
	_, s := c.node(up[0])
	for p := 0; p < c.parts; p++ {
		for _, e := range s.snap(p).Entries {
			var d dtData
			if json.Unmarshal(e.Data, &d) == nil {
				out[d.ID] = e.Seq
			}
		}
	}
	return out
}

// pendingHas reports whether node i holds a pending write of p whose data names the operation.
func (c *dtCluster) pendingHas(i, p int, id string) bool {
	_, s := c.node(i)
	if s == nil {
		return false
	}
	st, err := s.State(p)
	return err == nil && st.Pending != nil && strings.Contains(string(st.Pending.Data), `"`+id+`"`)
}

// cutAfterPending arms node i's store so that the next change it prepares for p (as owner) cuts it off from the network
// before replicate runs: the write is stored on that node only (its own SetPending needs no network, and the sends find
// every link cut). The returned channel closes when it fires. Replication stores locally and sends in parallel,
// so the cut must happen in Prepare for the write not to leave.
func (c *dtCluster) cutAfterPending(i, p int) chan struct{} {
	_, s := c.node(i)
	fired := make(chan struct{})
	s.setPrepareHook(func(pp int, change []byte) {
		if pp != p {
			return
		}
		s.setPrepareHook(nil)
		c.Cut(i)
		close(fired)
	})
	return fired
}

// awaitMeshed waits until every node that is up has every other up node alive in its gossip view: from then on a node has
// had the chance to see all the others' metadata (sizes, cluster IDs), so a "never happens" window after it is meaningful.
func (c *dtCluster) awaitMeshed(limit time.Duration) {
	c.t.Helper()
	c.await(limit, "every node to see every other alive", func() bool {
		up := c.upNodes()
		for _, i := range up {
			c.mu.Lock()
			cl := c.nodes[i].cluster
			c.mu.Unlock()
			if cl == nil || len(cl.AliveNodes()) < len(up) {
				return false
			}
		}
		return true
	})
}

// incrementConcurrently makes one increment per (node, partition) at once and returns the operations.
func (c *dtCluster) incrementConcurrently(nodes, parts []int) []*dtOp {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var out []*dtOp
	for _, i := range nodes {
		for _, p := range parts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				op := c.increment(i, p, dtKey(p, 0))
				mu.Lock()
				out = append(out, op)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	return out
}

func dtRequire(t *testing.T, cond bool, format string, args ...any) {
	t.Helper()
	if !cond {
		t.Fatalf(format, args...)
	}
}

// The owner stores a write and is cut off before it replicates it: the write exists on one node only. The other two take
// over without it (they never heard of it), keep writing, and the cut-off node reports it cannot read.
func TestDTOwnerDiesAfterMinorityStored(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	const p = 1
	o := c.ownerOf(p)
	dtRequire(t, o >= 0, "no single owner of %d", p)
	for _, i := range []int{0, 1, 2} {
		c.mustIncrement(i, p, dtKey(p, 0), 10*time.Second)
	}

	fired := c.cutAfterPending(o, p)
	lost := c.increment(o, p, dtKey(p, 1))
	select {
	case <-fired:
	default:
		t.Fatal("the owner never stored the write as pending")
	}
	dtRequire(t, lost.res == dtUnknown, "write through the cut owner returned %s (%v), want unknown", lost.res, lost.err)
	dtRequire(t, c.pendingHas(o, p, lost.id), "the owner does not hold its write as pending")
	for _, i := range c.others(o) {
		dtRequire(t, !c.pendingHas(i, p, lost.id), "node %d holds the write the owner never sent", i)
	}

	// The survivors take over and write while the old owner is still cut off.
	rest := c.others(o)
	c.awaitOwners(15*time.Second, rest...)
	for _, i := range rest {
		c.mustIncrement(i, p, dtKey(p, 0), dtLiveness)
	}
	time.Sleep(dtStale + dtStaleSlack)
	m, _ := c.node(o)
	for q := 0; q < c.parts; q++ {
		dtRequire(t, !m.CanRead(q), "cut-off node %d still reports CanRead(%d)", o, q)
	}

	c.settle()
	if _, ok := c.committedIDs()[lost.id]; ok {
		t.Errorf("the write that never left the owner was committed")
	}
	c.verify()
}

// Both followers store the write but every acknowledgement is lost: the owner answers unknown and steps down. Once
// replies flow again a takeover must settle the write: both followers hold it, so it is committed on every node.
func TestDTAckLostThenHeal(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	const p = 2
	o := c.ownerOf(p)
	dtRequire(t, o >= 0, "no single owner of %d", p)
	c.mustIncrement(o, p, dtKey(p, 0), 10*time.Second)

	rest := c.others(o)
	for _, f := range rest {
		c.DropReply(o, f, 1.0)
	}
	op := c.increment(o, p, dtKey(p, 1))
	dtRequire(t, op.res == dtUnknown, "write with every ack lost returned %s (%v), want unknown", op.res, op.err)
	for _, f := range rest {
		c.await(5*time.Second, "a follower to hold the write", func() bool { return c.pendingHas(f, p, op.id) })
	}
	m, _ := c.node(o)
	dtRequire(t, !m.IsOwner(p), "the owner did not step down after an unknown outcome")

	c.Heal()
	c.settle()
	if _, ok := c.committedIDs()[op.id]; !ok {
		t.Errorf("the write held by both followers was not committed by the takeover")
	}
	c.verify()
}

// As above, but the owner crashes: the two followers take over, find the write pending on both and commit it. The old
// owner returns with its own pending copy of the write and converges.
func TestDTAckLostThenOwnerCrashes(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	const p = 3
	o := c.ownerOf(p)
	dtRequire(t, o >= 0, "no single owner of %d", p)
	c.mustIncrement(o, p, dtKey(p, 0), 10*time.Second)

	rest := c.others(o)
	for _, f := range rest {
		c.DropReply(o, f, 1.0)
	}
	op := c.increment(o, p, dtKey(p, 1))
	dtRequire(t, op.res == dtUnknown, "write with every ack lost returned %s (%v), want unknown", op.res, op.err)
	for _, f := range rest {
		c.await(5*time.Second, "a follower to hold the write", func() bool { return c.pendingHas(f, p, op.id) })
	}
	c.crash(o)
	c.Heal()

	c.awaitOwners(15*time.Second, rest...)
	c.await(10*time.Second, "the survivors to commit the write both held", func() bool {
		_, ok := c.committedIDs()[op.id] // reads the first up node: a survivor
		return ok
	})
	for _, f := range rest {
		c.mustIncrement(f, p, dtKey(p, 0), dtLiveness)
	}
	c.restart(o, true)
	c.settle()
	if _, ok := c.committedIDs()[op.id]; !ok {
		t.Errorf("the write held by both survivors was lost")
	}
	c.verify()
}

// The owner stores its write on its own disk at the same time as it sends it to the followers (replicate overlaps the two),
// so its SetPending can fail after the followers have stored the write. The client must hear unknown (never unavailable:
// the write may be on a majority), the owner must step down, and a takeover must settle the write: committed on every
// node or on none, the same everywhere. With followers that stored it the write is on a majority and is committed; with
// followers that never got it, on none.
func TestDTOwnerSetPendingFailsAfterFollowersStored(t *testing.T) {
	for _, tc := range []struct {
		name      string
		delivered bool // the followers get the write before the owner's SetPending fails
	}{{"followers stored it", true}, {"followers never got it", false}} {
		t.Run(tc.name, func(t *testing.T) {
			c := dtStartCluster(t, dtOpts{Nodes: 3})
			const p = 1
			o := c.ownerOf(p)
			dtRequire(t, o >= 0, "no single owner of %d", p)
			c.mustIncrement(o, p, dtKey(p, 0), 10*time.Second)
			rest := c.others(o)
			if !tc.delivered {
				for _, f := range rest {
					c.Block(o, f)
				}
			}

			_, so := c.node(o)
			self := c.nodes[o].id
			fired := make(chan string, 1)
			so.setPendingFault(func(pp int, w *Write) error {
				if pp != p || w.Pos.Epoch.Node != self {
					return nil
				}
				so.setPendingFault(nil)
				var d dtData
				if err := json.Unmarshal(w.Data, &d); err != nil {
					return err
				}
				if tc.delivered {
					// Hold the owner's own store call until every follower has stored the write, then fail it.
					c.await(10*time.Second, "the followers to store the write", func() bool {
						for _, f := range rest {
							if !c.pendingHas(f, p, d.ID) {
								return false
							}
						}
						return true
					})
				}
				fired <- d.ID
				return errors.New("dtStore: disk full")
			})

			op := c.increment(o, p, dtKey(p, 1))
			var id string
			select {
			case id = <-fired:
			default:
				t.Fatal("the owner's SetPending of the write was never called")
			}
			dtRequire(t, id == op.id, "the failed SetPending was for %s, the write is %s", id, op.id)
			dtRequire(t, op.res == dtUnknown, "write whose owner SetPending failed returned %s (%v), want unknown", op.res, op.err)
			dtRequire(t, !c.pendingHas(o, p, op.id), "the owner holds the write it failed to store")
			m, _ := c.node(o)
			dtRequire(t, !m.IsOwner(p), "the owner did not step down after its SetPending failed")
			_, s := c.node(o)
			st, err := s.State(p)
			dtRequire(t, err == nil && st.Committed.Seq == 1, "the owner committed the write (or lost one): %+v, %v", st, err)

			// A takeover settles it. Followers that hold it commit it, on every node; otherwise nobody does.
			c.settle()
			committed := 0
			for _, i := range c.upNodes() {
				_, s := c.node(i)
				for _, e := range s.snap(p).Entries {
					if strings.Contains(string(e.Data), `"`+op.id+`"`) {
						committed++
					}
				}
			}
			dtRequire(t, committed == 0 || committed == len(c.upNodes()),
				"the write is committed on %d of %d nodes", committed, len(c.upNodes()))
			if tc.delivered {
				dtRequire(t, committed == len(c.upNodes()), "the write both followers stored was not committed (%d of %d nodes)",
					committed, len(c.upNodes()))
			} else {
				dtRequire(t, committed == 0, "a write no follower received was committed on %d nodes", committed)
			}
			c.verify()
		})
	}
}

// The owner is cut off and keeps trying to write: none of its writes may succeed, while the other two take over and keep
// writing. After healing it converges.
func TestDTCutOffOwnerCannotWrite(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	const p = 1
	o := c.ownerOf(p)
	dtRequire(t, o >= 0, "no single owner of %d", p)
	c.mustIncrement(o, p, dtKey(p, 0), 10*time.Second)
	rest := c.others(o)

	c.Cut(o)
	attempts := 0
	progress := make(chan struct{})
	go func() { // the survivors write meanwhile
		defer close(progress)
		c.mustIncrement(rest[0], p, dtKey(p, 0), dtLiveness)
		c.mustIncrement(rest[1], p, dtKey(p, 0), dtLiveness)
	}()
	// Rounds of one write per partition through the cut-off node (each runs up to WriteTimeout, starting while it still
	// believes it owns its partitions), until the survivors have made their progress.
	for {
		for _, op := range c.incrementConcurrently([]int{o}, c.allParts()) {
			if op.res == dtOK {
				t.Fatalf("a cut-off node committed a write (partition %d, attempt %d)", op.p, attempts)
			}
			attempts++
		}
		done := false
		select {
		case <-progress:
			done = true
		default:
		}
		if done {
			break
		}
	}
	c.awaitOwners(15*time.Second, rest...)
	m, _ := c.node(o)
	c.await(dtStale+dtStaleSlack, "the cut-off node to stop reading", func() bool {
		for q := 0; q < c.parts; q++ {
			if m.CanRead(q) {
				return false
			}
		}
		return true
	})
	t.Logf("%d writes through the cut-off node, none committed", attempts)

	c.Uncut(o)
	c.settle()
	c.verify()
}

// Two nodes can each reach the third but not each other. Writes through the node that reaches both must keep working and
// two owners must never commit; tried for every such pair, both ways blocked and then one way only.
func TestDTAsymmetricReachability(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3, Parts: 6})
	w := c.startWorkload(dtWLOpts{})
	for _, bothWays := range []bool{true, false} {
		for z := 0; z < 3; z++ {
			x, y := (z+1)%3, (z+2)%3
			c.Block(x, y)
			if bothWays {
				c.Block(y, x)
			}
			t.Logf("nodes %d and %d cannot reach each other (both ways %v); node %d reaches both", x, y, bothWays, z)
			// Writes through z must resume within the bound, for every partition.
			t0 := time.Now()
			for p := 0; p < c.parts; p++ {
				c.mustIncrement(z, p, dtKey(p, 0), dtLiveness)
			}
			t.Logf("  node %d: writes to every partition committed within %v", z, time.Since(t0).Round(10*time.Millisecond))
			c.awaitCommits(10*time.Second, 5) // the workload goes on under the fault
			c.Heal()
			c.awaitOwners(15 * time.Second)
		}
	}
	w.finish()
	c.settle()
	c.verify()
}

// A node returns with a pending write of an old epoch: its owner stored a write, was cut off and crashed; the others took
// over and committed other writes at that position. The returning node must drop the stale write and converge.
func TestDTReturnWithOldPending(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	const p = 1
	o := c.ownerOf(p)
	dtRequire(t, o >= 0, "no single owner of %d", p)
	c.mustIncrement(o, p, dtKey(p, 0), 10*time.Second)
	rest := c.others(o)

	fired := c.cutAfterPending(o, p)
	stale := c.increment(o, p, dtKey(p, 1))
	<-fired
	dtRequire(t, stale.res == dtUnknown, "write through the cut owner returned %s, want unknown", stale.res)
	c.crash(o)
	c.Heal()
	c.awaitOwners(15*time.Second, rest...)
	for range 3 {
		for _, i := range rest {
			c.mustIncrement(i, p, dtKey(p, 0), dtLiveness)
		}
	}

	c.restart(o, true)
	_, s := c.node(o)
	st, _ := s.State(p)
	dtRequire(t, st.Pending != nil && strings.Contains(string(st.Pending.Data), stale.id), "the returning node lost its pending write on disk: %+v", st)
	c.settle()
	if _, ok := c.committedIDs()[stale.id]; ok {
		t.Errorf("the stale pending write was committed after the others had moved on")
	}
	if st, _ := s.State(p); st.Pending != nil && st.Pending.Pos.Seq != st.Committed.Seq+1 {
		t.Errorf("returning node left with pending %s at committed %s", st.Pending.Pos, st.Committed)
	}
	c.verify()

	// Second act: the others have not moved on when the owner returns (nothing else is written). Whether its pending write
	// is adopted by a takeover or overwritten, every node must end up the same.
	o2 := c.ownerOf(2)
	dtRequire(t, o2 >= 0, "no single owner of 2")
	fired = c.cutAfterPending(o2, 2)
	stale2 := c.increment(o2, 2, dtKey(2, 1))
	<-fired
	dtRequire(t, stale2.res == dtUnknown, "second write through the cut owner returned %s, want unknown", stale2.res)
	c.crash(o2)
	c.Heal()
	c.awaitOwners(15*time.Second, c.others(o2)...)
	c.restart(o2, true)
	c.settle()
	_, adopted := c.committedIDs()[stale2.id]
	t.Logf("stale write adopted by a takeover: %v", adopted)
	c.verify()
}

// A node restarts with its disk lost while the other two are up and a workload runs: it must resync, become ready and
// converge on its own, whichever node it is (owners of many partitions, or followers).
func TestDTRestartWithDataLost(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	w := c.startWorkload(dtWLOpts{})
	for x := 0; x < 3; x++ {
		c.awaitCommits(10*time.Second, 30)
		c.crash(x)
		c.awaitOwners(15*time.Second, c.others(x)...) // the others have taken over
		c.restart(x, false)
		m, _ := c.node(x)
		dtRequire(t, m.resync.Load(), "node %d restarted with a lost disk but is not resyncing", x)
		for p := 0; p < c.parts; p++ {
			dtRequire(t, !m.CanRead(p), "node %d serves reads of %d while resyncing", x, p)
		}
		c.awaitReady(30*time.Second, x)
	}
	w.finish()
	c.settle()
	c.verify()
}

// All three nodes crash (with the workload running) and restart with their data: writes resume with no operator action
// and nothing committed is lost. The second time one of them comes back with its disk lost.
func TestDTCrashAllRestartAll(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	w := c.startWorkload(dtWLOpts{})
	for round := 0; round < 2; round++ {
		c.awaitCommits(10*time.Second, 30)
		for i := range c.nodes {
			c.crash(i)
		}
		time.Sleep(500 * time.Millisecond)
		for i := range c.nodes {
			c.restart(i, round == 0 || i != 2)
		}
		c.awaitReady(30 * time.Second)
		c.awaitOwners(30 * time.Second)
	}
	c.awaitCommits(10*time.Second, 30)
	w.finish()
	c.settle()
	c.verify()
}

// A node that has never held anything joins a running cluster (two of three nodes up, writing) and converges.
func TestDTJoinEmpty(t *testing.T) {
	c := dtNewCluster(t, dtOpts{Nodes: 3})
	c.start(0)
	c.start(1)
	c.awaitReady(30*time.Second, 0, 1)
	c.awaitOwners(30*time.Second, 0, 1)
	w := c.startWorkload(dtWLOpts{})
	c.awaitCommits(10*time.Second, 30)
	c.start(2)
	c.awaitReady(30*time.Second, 2)
	_, s := c.node(2)
	for p := 0; p < c.parts; p++ {
		dtRequire(t, s.snap(p).St.Committed.Seq > 0, "the late node is ready but holds nothing of partition %d", p)
	}
	c.awaitCommits(10*time.Second, 30)
	w.finish()
	c.settle()
	c.verify()
}

// Nodes configured with different cluster sizes ignore each other and never commit together.
func TestDTSizeMismatch(t *testing.T) {
	// Two nodes of size 3 and one of size 5: the pair commits, the odd one out never becomes a candidate.
	c := dtNewCluster(t, dtOpts{Nodes: 3, Sizes: []int{3, 3, 5}})
	c.startAll()
	c.awaitReady(30*time.Second, 0, 1)
	c.awaitOwners(30*time.Second, 0, 1)
	for p := 0; p < c.parts; p++ {
		c.mustIncrement(0, p, dtKey(p, 0), 10*time.Second)
		c.mustIncrement(1, p, dtKey(p, 0), 10*time.Second)
	}
	// Once every node has seen the others' metadata, the odd one out tries to write: its attempts run up to WriteTimeout,
	// which is the window in which it could have found a majority.
	c.awaitMeshed(15 * time.Second)
	for _, op := range c.incrementConcurrently([]int{2}, c.allParts()) {
		if op.res == dtOK {
			t.Fatalf("the node of another size committed a write of partition %d", op.p)
		}
	}
	m2, s2 := c.node(2)
	dtRequire(t, !m2.Ready(), "the node of size 5 is ready")
	for p := 0; p < c.parts; p++ {
		dtRequire(t, !m2.IsOwner(p), "the node of size 5 owns %d", p)
		dtRequire(t, s2.snap(p).St.Committed.Seq == 0, "the node of size 5 committed something in %d", p)
	}
	c.awaitConverged(20*time.Second, 0, 1)
	c.verify(0, 1)

	// Three sizes that all differ: nobody finds a majority, nothing is ever committed.
	d := dtNewCluster(t, dtOpts{Nodes: 3, Sizes: []int{3, 5, 7}, Seed: 2})
	d.startAll()
	d.awaitMeshed(15 * time.Second)
	// The attempts run up to WriteTimeout after the nodes have seen each other: that is the window for "never".
	for _, op := range d.incrementConcurrently([]int{0, 1, 2}, []int{1}) {
		if op.res == dtOK {
			t.Fatalf("node %d committed a write in a cluster of three different sizes", op.via)
		}
	}
	for i := range d.nodes {
		m, _ := d.node(i)
		dtRequire(t, !m.Ready(), "node %d of mixed sizes is ready", i)
	}
	d.led.mu.Lock()
	defer d.led.mu.Unlock()
	for _, e := range d.led.bySeq {
		t.Fatalf("something was committed in a cluster of three different sizes: %+v", e)
	}
}

// The owner commits a write that only one follower has stored, everything crashes before that follower hears the write is
// committed, and the owner returns with its disk lost: the write is then committed on a disk that is gone and pending on one
// follower. The returned owner's takeover must still find it, however its promise round goes.
func TestDTLostDiskOwnerWithAckedWriteOnlyPendingElsewhere(t *testing.T) {
	const p = 1
	c := dtNewCluster(t, dtOpts{Nodes: 3})
	var slow atomic.Bool
	var oIdx atomic.Int32
	oIdx.Store(-1)
	c.opts.Tweak = func(i int, cfg *Config) {
		if slow.Load() && int32(i) != oIdx.Load() {
			cfg.Tuning.ReadyHoldDown = 4 * time.Second // the owner is the first to be ready again
		}
	}
	c.startAll()
	c.awaitReady(30 * time.Second)
	c.awaitOwners(30 * time.Second)
	o := c.ownerOf(p)
	dtRequire(t, o >= 0, "no single owner of %d", p)
	oIdx.Store(int32(o))
	f1, f2 := c.others(o)[0], c.others(o)[1]
	c.mustIncrement(o, p, dtKey(p, 0), 10*time.Second)
	c.awaitConverged(10 * time.Second)

	// f2 cannot hear the owner; the owner is cut off the moment it commits, before a heartbeat tells f1.
	c.Block(o, f2)
	_, so := c.node(o)
	so.setCommitHook(func(pp int, w *Write) {
		if pp == p && w.Pos.Epoch.Node == c.nodes[o].id {
			so.setCommitHook(nil)
			c.Cut(o)
		}
	})
	acked := c.mustIncrement(o, p, dtKey(p, 1), 10*time.Second)
	c.crash(o)
	c.crash(f1)
	c.crash(f2)
	c.Heal()
	dtRequire(t, c.pendingHas(f1, p, acked.id), "follower %d does not hold the acknowledged write as pending", f1)
	if st, _ := c.nodes[f2].store.State(p); st.Pending != nil {
		t.Fatalf("follower %d holds a pending write it was never sent", f2)
	}

	slow.Store(true)
	c.restart(o, false)
	c.restart(f1, true)
	c.restart(f2, true)
	// When the owner proposes itself, the promise of the follower that holds the write never arrives.
	_, sn := c.node(o)
	var prevCounter uint64
	for _, i := range []int{f1, f2} {
		st, _ := c.nodes[i].store.State(p)
		prevCounter = max(prevCounter, st.Promised.Counter)
	}
	// Only until the heal: a takeover by the owner after it (the first can fail and back off) must not cut the link again.
	var blockMu sync.Mutex
	healed := false
	sn.setPromisedHook(func(pp int, e Epoch) {
		blockMu.Lock()
		defer blockMu.Unlock()
		if !healed && pp == p && e.Node == c.nodes[o].id && e.Counter > prevCounter {
			c.Block(o, f1)
		}
	})
	c.awaitReady(30 * time.Second)
	blockMu.Lock()
	healed = true
	c.Heal()
	blockMu.Unlock()
	c.settle()
	if _, ok := c.committedIDs()[acked.id]; !ok {
		t.Errorf("write %s was acknowledged but the resynced owner's takeover overwrote it", acked.id)
	}
	c.verify()
}

// Five nodes split 2|3: only the side with three commits; after healing everything converges.
func TestDTFiveNodeSplit(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 5, Parts: 6})
	w := c.startWorkload(dtWLOpts{})
	c.awaitCommits(10*time.Second, 30)

	minority, majority := []int{0, 1}, []int{2, 3, 4}
	c.Split(minority, majority)
	splitAt := time.Now()
	// Writes through the majority side resume within the bound for every partition.
	for p := 0; p < c.parts; p++ {
		c.mustIncrement(majority[p%3], p, dtKey(p, 0), dtLiveness)
	}
	t.Logf("majority side writes to every partition %v after the split", time.Since(splitAt).Round(10*time.Millisecond))
	// Nothing written through the minority side may succeed once the split is in place.
	for _, op := range c.incrementConcurrently(minority, c.allParts()) {
		if op.res == dtOK {
			t.Fatalf("minority node %d committed a write of partition %d", op.via, op.p)
		}
	}
	// A minority node reading: it must have stopped within StaleAfter of losing contact.
	c.await(dtStale+dtStaleSlack, "the minority nodes to stop reading", func() bool {
		for _, i := range minority {
			m, _ := c.node(i)
			for p := 0; p < c.parts; p++ {
				if m.CanRead(p) {
					return false
				}
			}
		}
		return true
	})
	c.Heal()
	w.finish()
	c.settle()
	c.verify()
}

// An owner and one follower end up on the minority side of a split while no write goes through them: the owner stays the
// owner in its own eyes and keeps announcing itself to the follower. Once the majority side has a new owner that commits
// writes, neither of them may go on serving reads for more than StaleAfter (bounded staleness).
func TestDTMinoritySideStopsReading(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 5})
	const p = 1
	o := c.ownerOf(p)
	dtRequire(t, o >= 0, "no single owner of %d", p)
	c.mustIncrement(o, p, dtKey(p, 0), 10*time.Second)
	f := c.others(o)[0]
	minority := []int{o, f}
	var majority []int
	for i := range c.nodes {
		if i != o && i != f {
			majority = append(majority, i)
		}
	}
	c.awaitConverged(10 * time.Second)

	c.Split(minority, majority)
	// The majority side takes over and commits.
	c.mustIncrement(majority[0], p, dtKey(p, 0), dtLiveness)
	committedAt := time.Now()
	time.Sleep(dtStale + dtStaleSlack)
	for _, i := range minority {
		m, _ := c.node(i)
		if m.CanRead(p) {
			t.Errorf("minority node %d (owner %v) still reports CanRead(%d) %v after the majority side committed a write",
				i, m.IsOwner(p), p, time.Since(committedAt).Round(10*time.Millisecond))
		}
	}
	c.settle()
	c.verify()
}

// Messages are duplicated, delayed with jitter (so that they overtake each other) and some are lost, under the workload.
func TestDTDuplicatedAndDelayedMessages(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	w := c.startWorkload(dtWLOpts{})
	all := func(fn func(i, j int)) {
		for i := range c.nodes {
			for j := range c.nodes {
				if i != j {
					fn(i, j)
				}
			}
		}
	}
	// Each phase lasts until a number of writes have committed under it (a window counted in operations, not seconds).
	c.awaitCommits(10*time.Second, 20)
	all(func(i, j int) { c.Duplicate(i, j, 0.5) })
	c.awaitCommits(20*time.Second, 150)
	all(func(i, j int) { c.Duplicate(i, j, 0); c.Delay(i, j, 0, 120*time.Millisecond) })
	c.awaitCommits(20*time.Second, 100)
	all(func(i, j int) { c.Duplicate(i, j, 0.5); c.DropRate(i, j, 0.05); c.DropReply(i, j, 0.05) })
	c.awaitCommits(20*time.Second, 100)
	w.finish()
	c.settle()
	c.verify()
}

// A change forwarded to the owner over a link that cannot carry it (the request never leaves the node, as when the
// connection cannot be made) was certainly not made: the node retries it until WriteTimeout and answers ErrUnavailable,
// never ErrUnknown. A change whose request reaches the owner but whose answer is lost may have been made: ErrUnknown
// (and here it was).
func TestDTForwardedChangeThatNeverLeftIsUnavailable(t *testing.T) {
	c := dtStartCluster(t, dtOpts{Nodes: 3})
	const p = 1
	o := c.ownerOf(p)
	dtRequire(t, o >= 0, "no single owner of %d", p)
	f := c.others(o)[0]
	c.mustIncrement(o, p, dtKey(p, 0), 10*time.Second)
	c.awaitConverged(10 * time.Second)

	c.Block(f, o) // every request from f to the owner fails before it is sent
	notSent := c.increment(f, p, dtKey(p, 0))
	dtRequire(t, notSent.res == dtUnavailable, "a change whose request never left node %d returned %s (%v), want unavailable",
		f, notSent.res, notSent.err)
	c.Heal()

	c.DropReply(f, o, 1.0) // the owner gets the request, node f never gets the answer
	lost := c.increment(f, p, dtKey(p, 0))
	dtRequire(t, lost.res == dtUnknown, "a change whose answer was lost returned %s (%v), want unknown", lost.res, lost.err)
	c.Heal()

	c.settle()
	ids := c.committedIDs()
	if _, ok := ids[notSent.id]; ok {
		t.Error("the change that never left was committed")
	}
	if _, ok := ids[lost.id]; !ok {
		t.Error("the change whose answer was lost was not committed")
	}
	c.verify()
}
