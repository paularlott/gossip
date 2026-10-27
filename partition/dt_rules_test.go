package partition

// Deterministic scenarios for two protocol rules that no randomised destruction test caught when they were removed. Each
// forces its interleaving with store hooks and faultnet blocks rather than timing, and fails with a violation of
// invariant 2 (one Seq, one write) when its rule is taken out.

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/gossip"
)

// rankOrder returns the node indices of the cluster ordered by rendezvous rank for p, rank 0 first, as every node
// computes it while all are candidates. The node IDs are fixed when the cluster is created, so a scenario can choose
// its roles before anything starts.
func (c *dtCluster) rankOrder(p int) []int {
	ids := make([]gossip.NodeID, len(c.nodes))
	for i, n := range c.nodes {
		ids[i] = n.id
	}
	out := make([]int, len(c.nodes))
	for i, n := range c.nodes {
		r, _ := rendezvousRank(p, n.id, ids)
		out[r] = i
	}
	return out
}

// awaitOwner waits until node i leads p, and returns its epoch.
func (c *dtCluster) awaitOwner(i, p int, limit time.Duration) Epoch {
	c.t.Helper()
	var e Epoch
	c.await(limit, "node to lead the partition", func() bool {
		m, _ := c.node(i)
		var ok bool
		e, ok = m.leadingEpoch(p)
		return ok
	})
	return e
}

// requireNoConflict fails the test if any two nodes committed different writes at one Seq (invariant 2), as the ledger
// saw it.
func (c *dtCluster) requireNoConflict() {
	c.t.Helper()
	c.led.mu.Lock()
	conflicts := append([]string(nil), c.led.conflicts...)
	c.led.mu.Unlock()
	for _, v := range conflicts {
		c.t.Errorf("invariant 2: %s", v)
	}
	if len(conflicts) > 0 {
		c.dump()
		c.t.FailNow()
	}
}

// requireLogHas checks that every node that is up holds, at seq of p, the write of operation id.
func (c *dtCluster) requireLogHas(p int, seq uint64, id string) {
	c.t.Helper()
	for _, i := range c.upNodes() {
		_, s := c.node(i)
		sn := s.snap(p)
		if uint64(len(sn.Entries)) < seq || !strings.Contains(string(sn.Entries[seq-1].Data), `"`+id+`"`) {
			c.t.Errorf("node %d does not hold %s at seq %d of partition %d", i, id, seq, p)
		}
	}
}

// A follower refuses a write of an epoch below its promise. The old owner A stores its pending write w1 at seq s and is
// paused there (a store hook) before it sends it. Meanwhile A is cut off from C and from B's point of view silent, so C
// takes over at a higher epoch with B's promise and commits a different write w2 at s on {C, B}. Then A is released and
// its w1 reaches B, which has promised the higher epoch: B must refuse it. Without the refusal B acknowledges w1 (it
// already holds seq s, or stores it over w2), A commits it with that second copy and s has two different writes.
func TestDTFollowerRefusesWriteBelowPromise(t *testing.T) {
	const p = 0
	b := -1 // the follower that must never take over: only C does (set before the nodes start)
	c := dtNewCluster(t, dtOpts{Nodes: 3, Parts: 1, Tweak: func(i int, cfg *Config) {
		cfg.WriteTimeout = 5 * time.Second
		if i == b {
			cfg.Tuning.RankDelay = time.Hour
		}
	}})
	order := c.rankOrder(p)
	a, cn, bn := order[0], order[1], order[2] // old owner (rank 0), the taker-over, the swing follower
	b = bn
	c.startAll()
	c.awaitReady(30 * time.Second)
	c.await(30*time.Second, "node A to own the partition", func() bool { return c.ownerOf(p) == a })
	c.mustIncrement(a, p, dtKey(p, 0), 10*time.Second)
	c.awaitConverged(10 * time.Second)

	mgrA, storeA := c.node(a)
	e1, leading := mgrA.leadingEpoch(p)
	dtRequire(t, leading, "node A does not lead the partition")

	// A pauses once it has prepared its write, before replicate stores it as pending and sends it anywhere (replicate does
	// both at the same time, so the pause cannot come between them).
	paused := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseA := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseA) // runs before the cluster is closed, so a failed test cannot leave A blocked
	var fire sync.Once
	storeA.setPrepareHook(func(pp int, change []byte) {
		if pp != p {
			return
		}
		fire.Do(func() {
			close(paused)
			<-release
		})
	})
	res1 := make(chan *dtOp, 1)
	go func() { res1 <- c.increment(a, p, dtKey(p, 1)) }() // w1
	select {
	case <-paused:
	case <-time.After(10 * time.Second):
		t.Fatal("A never prepared its write")
	}

	// While A is held: cut A off from C (both ways) and stop A's heartbeats to B, so that B's loyalty to A lapses and
	// B grants C's promise. (A's heartbeats would otherwise keep B "sticky", refusing every other candidate.)
	c.Split([]int{a}, []int{cn})
	c.Block(a, bn)
	e2 := c.awaitOwner(cn, p, 20*time.Second)
	dtRequire(t, e1.Less(e2), "C took over at %s, not above A's %s", e2, e1)
	w2 := c.mustIncrement(cn, p, dtKey(p, 2), 10*time.Second)
	dtRequire(t, w2.pos.Epoch == e2, "w2 committed at %s, want epoch %s", w2.pos, e2)
	s := w2.pos.Seq

	_, storeB := c.node(bn)
	stB, _ := storeB.State(p)
	dtRequire(t, stB.Promised == e2, "B's promise is %s, want C's epoch %s", stB.Promised, e2)
	stA, _ := storeA.State(p)
	dtRequire(t, stA.Pending == nil && stA.Committed.Seq == s-1,
		"A holds pending %+v, committed %s; it should have prepared w1 at seq %d and stored nothing yet", stA.Pending, stA.Committed, s)

	// Release A: its write w1 (epoch e1, seq s) now reaches B, whose promise is e2.
	c.Unblock(a, bn)
	releaseA()
	var w1 *dtOp
	select {
	case w1 = <-res1:
	case <-time.After(15 * time.Second):
		t.Fatal("A's write never returned")
	}
	c.requireNoConflict()
	dtRequire(t, w1.res != dtOK, "A's write w1 (epoch %s) committed although B had promised %s", e1, e2)

	c.settle()
	if _, ok := c.committedIDs()[w1.id]; ok {
		t.Errorf("the write of the deposed owner was committed")
	}
	c.requireLogHas(p, s, w2.id)
	c.verify()
}

// settle commits a pending write only when its epoch is the owner's committed epoch. Owner A (epoch e1) sends write X at
// seq s, which only F stores: A cannot reach G, H, I and F's acknowledgement is lost, so A steps down and is cut off.
// G takes over at e2 with the promises of G, H and I (F is not asked, so X is never seen) and commits a different write
// Y at s. When F is reconnected, G's heartbeat tells it that (e2, s) is committed: F must not commit X (epoch e1); it
// keeps it until its catch-up to Y replaces it. A node that committed any pending write at or below the owner's committed
// Seq would commit X at s.
func TestDTSettleClearsPendingOfOtherEpoch(t *testing.T) {
	const p = 0
	taker := -1 // the only node besides the old owner that may take over (set before the nodes start)
	oldOwner := -1
	c := dtNewCluster(t, dtOpts{Nodes: 5, Parts: 1, Tweak: func(i int, cfg *Config) {
		cfg.WriteTimeout = 5 * time.Second
		if i != taker && i != oldOwner {
			cfg.Tuning.RankDelay = time.Hour
		}
	}})
	order := c.rankOrder(p)
	a, g, f := order[0], order[1], order[2] // old owner, taker-over, the follower that holds X
	h, i := order[3], order[4]
	oldOwner, taker = a, g
	c.startAll()
	c.awaitReady(30 * time.Second)
	c.await(30*time.Second, "node A to own the partition", func() bool { return c.ownerOf(p) == a })
	c.mustIncrement(a, p, dtKey(p, 0), 10*time.Second)
	c.awaitConverged(10 * time.Second)

	mgrA, _ := c.node(a)
	e1, leading := mgrA.leadingEpoch(p)
	dtRequire(t, leading, "node A does not lead the partition")

	// X reaches F only, and A is cut off the moment F has stored it (before it can do anything else, such as a takeover of
	// its own); F's acknowledgement is lost as well. G is kept from F, so its promise round does not see X.
	_, storeF := c.node(f)
	fired := make(chan struct{})
	selfA := c.nodes[a].id
	storeF.setHook(func(pp int, w *Write) {
		if pp != p || w.Pos.Epoch.Node != selfA {
			return
		}
		storeF.setHook(nil)
		c.Cut(a)
		close(fired)
	})
	c.Block(g, f)
	c.Block(a, g)
	c.Block(a, h)
	c.Block(a, i)
	c.DropReply(a, f, 1.0)
	x := c.increment(a, p, dtKey(p, 1))
	select {
	case <-fired:
	default:
		t.Fatal("F never stored X")
	}
	dtRequire(t, x.res == dtUnknown, "X returned %s (%v), want unknown", x.res, x.err)
	for _, n := range []int{a, f} {
		dtRequire(t, c.pendingHas(n, p, x.id), "node %d does not hold X as pending", n)
	}
	for _, n := range []int{g, h, i} {
		dtRequire(t, !c.pendingHas(n, p, x.id), "node %d holds X, which only F should have", n)
	}

	// G takes over without X and commits Y at the same Seq.
	e2 := c.awaitOwner(g, p, 20*time.Second)
	dtRequire(t, e1.Less(e2), "G took over at %s, not above A's %s", e2, e1)
	y := c.mustIncrement(g, p, dtKey(p, 2), 10*time.Second)
	dtRequire(t, y.pos.Epoch == e2, "Y committed at %s, want epoch %s", y.pos, e2)
	s := y.pos.Seq
	dtRequire(t, c.pendingHas(f, p, x.id), "F lost X before it was reconnected")
	stF, _ := storeF.State(p)
	dtRequire(t, stF.Pending.Pos == (Pos{Epoch: e1, Seq: s}) && stF.Committed.Seq == s-1,
		"F holds pending %s, committed %s; want pending (%s, %d) after committed seq %d", stF.Pending.Pos, stF.Committed, e1, s, s-1)

	// F hears from G: it must clear X and catch up to Y.
	c.Unblock(g, f)
	c.await(30*time.Second, "F to settle its pending write and reach seq "+y.id, func() bool {
		st, _ := storeF.State(p)
		return st.Pending == nil && st.Committed.Seq >= s
	})
	c.requireNoConflict()

	c.settle()
	if _, ok := c.committedIDs()[x.id]; ok {
		t.Errorf("X, held by a minority only, was committed")
	}
	c.requireLogHas(p, s, y.id)
	c.verify()
}
