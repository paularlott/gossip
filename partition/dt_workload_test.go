package partition

// Workload and invariant checks of the destruction tests. The workload is made of compare-and-swap increments ("key k:
// expect version v, set v+1") whose outcome is recorded, so that after a run the committed logs of every node can be
// checked against what every caller was told.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// dtRes is the outcome of one Write as the caller saw it.
type dtRes int

const (
	dtOK          dtRes = iota // nil: committed
	dtRefused                  // ErrRefused: nothing written
	dtUnknown                  // ErrUnknown: may or may not be committed
	dtUnavailable              // ErrUnavailable: nothing was replicated
	dtOther                    // any other error: nothing written
)

func (r dtRes) String() string {
	return [...]string{"ok", "refused", "unknown", "unavailable", "other"}[r]
}

// dtOp is one recorded increment.
type dtOp struct {
	id         string
	p          int
	key        string
	exp        int
	via        int
	start, end time.Time
	res        dtRes
	err        error
	pos        Pos    // Result.Pos of a write that returned nil
	reply      string // Result.Reply of a write that returned nil: Prepare returns the operation's ID
}

// dtReadOp is one recorded Manager.Read of a key: it returned the key's committed version v and the partition's committed
// sequence number seq (ok), or failed with err.
type dtReadOp struct {
	p          int
	key        string
	via        int
	start, end time.Time
	ok         bool
	v          int
	seq        uint64
	err        error
}

// dtOps is the record of every operation of a cluster.
type dtOps struct {
	mu    sync.Mutex
	list  []*dtOp
	reads []*dtReadOp
}

func (o *dtOps) addRead(r *dtReadOp) {
	o.mu.Lock()
	o.reads = append(o.reads, r)
	o.mu.Unlock()
}

func (o *dtOps) allReads() []*dtReadOp {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*dtReadOp(nil), o.reads...)
}

func (o *dtOps) add(op *dtOp) {
	o.mu.Lock()
	o.list = append(o.list, op)
	o.mu.Unlock()
}

func (o *dtOps) all() []*dtOp {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*dtOp(nil), o.list...)
}

func (o *dtOps) count(res dtRes) int {
	n := 0
	for _, op := range o.all() {
		if op.res == res {
			n++
		}
	}
	return n
}

// summary describes the outcomes recorded so far.
func (o *dtOps) summary() string {
	var c [5]int
	for _, op := range o.all() {
		c[op.res]++
	}
	return fmt.Sprintf("ok=%d refused=%d unknown=%d unavailable=%d other=%d", c[dtOK], c[dtRefused], c[dtUnknown], c[dtUnavailable], c[dtOther])
}

var dtOpSeq atomic.Int64

// dtKey names key j of partition p.
func dtKey(p, j int) string { return fmt.Sprintf("p%d/k%d", p, j) }

// increment makes one compare-and-swap increment of key in p through node via, expecting the version via has committed,
// and records the outcome. It returns nil if the node is down.
func (c *dtCluster) increment(via, p int, key string) *dtOp {
	mgr, store := c.node(via)
	if mgr == nil {
		return nil
	}
	op := &dtOp{id: fmt.Sprintf("op%d", dtOpSeq.Add(1)), p: p, key: key, via: via}
	op.exp = store.version(p, key)
	change, _ := json.Marshal(dtChange{K: key, Exp: op.exp, ID: op.id})
	ctx, cancel := context.WithTimeout(context.Background(), dtWriteTimeout+2*time.Second)
	defer cancel()
	op.start = time.Now()
	res, err := mgr.Write(ctx, p, change)
	op.end = time.Now()
	op.err = err
	switch {
	case err == nil:
		op.res = dtOK
		op.pos, op.reply = res.Pos, string(res.Reply)
	case errors.Is(err, ErrRefused):
		op.res = dtRefused
		var re *RefusedError
		if !errors.As(err, &re) || re.Code != "conflict" {
			c.violate("write %s: refusal %v does not carry the code the Store gave it", op.id, err)
		}
	case errors.Is(err, ErrUnknown):
		op.res = dtUnknown
	case errors.Is(err, ErrUnavailable):
		op.res = dtUnavailable
	default:
		op.res = dtOther
	}
	c.ops.add(op)
	return op
}

// read makes one linearizable Read of key in p through node via and records the outcome. It returns nil if the node is down.
func (c *dtCluster) read(via, p int, key string) *dtReadOp {
	mgr, _ := c.node(via)
	if mgr == nil {
		return nil
	}
	r := &dtReadOp{p: p, key: key, via: via}
	ctx, cancel := context.WithTimeout(context.Background(), dtWriteTimeout+2*time.Second)
	defer cancel()
	r.start = time.Now()
	b, err := mgr.Read(ctx, p, []byte(key))
	r.end = time.Now()
	if err != nil {
		r.err = err
		if !errors.Is(err, ErrUnavailable) {
			c.violate("read of %s through node %d failed with %v, want an answer or ErrUnavailable", key, via, err)
		}
	} else {
		var res dtReadResult
		if err := json.Unmarshal(b, &res); err != nil {
			c.violate("read of %s through node %d: undecodable answer %q", key, via, b)
		} else {
			r.ok, r.v, r.seq = true, res.V, res.Seq
		}
	}
	c.ops.addRead(r)
	return r
}

// mustIncrement retries increment through via (a refused, unknown or unavailable change is simply tried again with a fresh
// read) until one is committed or limit passes, in which case the test fails.
func (c *dtCluster) mustIncrement(via, p int, key string, limit time.Duration) *dtOp {
	c.t.Helper()
	deadline := time.Now().Add(limit)
	var last *dtOp
	for time.Now().Before(deadline) {
		op := c.increment(via, p, key)
		if op != nil && op.res == dtOK {
			return op
		}
		if op != nil {
			last = op
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.dump()
	if last != nil {
		c.t.Fatalf("no write of partition %d through node %d committed within %v (last: %s, %v)", p, via, limit, last.res, last.err)
	}
	c.t.Fatalf("no write of partition %d through node %d committed within %v", p, via, limit)
	return nil
}

func sortedInts[V any](m map[int]V) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// --- workload ----------------------------------------------------------------------------------------------------

// dtWLOpts configures a background workload.
type dtWLOpts struct {
	Workers int           // default 4
	Readers int           // goroutines making linearizable reads of the same keys (default 2; negative: none)
	Parts   []int         // partitions to write to (default: all)
	Keys    int           // keys per partition (default 3)
	Nodes   func() []int  // landing nodes (default: every node that is up)
	Pace    time.Duration // mean pause between a worker's operations (default 30ms)
	Seed    int64
}

type dtWorkload struct {
	c    *dtCluster
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

// startWorkload runs random compare-and-swap increments from several goroutines until stop is called.
func (c *dtCluster) startWorkload(o dtWLOpts) *dtWorkload {
	if o.Workers == 0 {
		o.Workers = 4
	}
	if len(o.Parts) == 0 {
		o.Parts = c.allParts()
	}
	if o.Keys == 0 {
		o.Keys = 3
	}
	if o.Readers == 0 {
		o.Readers = 2
	}
	if o.Pace == 0 {
		o.Pace = 30 * time.Millisecond
	}
	if o.Seed == 0 {
		o.Seed = c.opts.Seed
	}
	w := &dtWorkload{c: c, stop: make(chan struct{})}
	c.t.Cleanup(w.finish) // a test that fails midway must not leave the workload running
	for k := 0; k < o.Workers; k++ {
		rng := rand.New(rand.NewSource(o.Seed*100 + int64(k)))
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-w.stop:
					return
				case <-time.After(time.Duration(rng.Int63n(int64(2 * o.Pace)))):
				}
				nodes := c.upNodes()
				if o.Nodes != nil {
					nodes = o.Nodes()
				}
				if len(nodes) == 0 {
					continue
				}
				p := o.Parts[rng.Intn(len(o.Parts))]
				c.increment(nodes[rng.Intn(len(nodes))], p, dtKey(p, rng.Intn(o.Keys)))
			}
		}()
	}
	for k := 0; k < o.Readers; k++ {
		rng := rand.New(rand.NewSource(o.Seed*100 + 50 + int64(k)))
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-w.stop:
					return
				case <-time.After(time.Duration(rng.Int63n(int64(2 * o.Pace)))):
				}
				nodes := c.upNodes()
				if o.Nodes != nil {
					nodes = o.Nodes()
				}
				if len(nodes) == 0 {
					continue
				}
				p := o.Parts[rng.Intn(len(o.Parts))]
				c.read(nodes[rng.Intn(len(nodes))], p, dtKey(p, rng.Intn(o.Keys)))
			}
		}()
	}
	return w
}

// finish stops the workload and waits for its in-flight operations to return.
func (w *dtWorkload) finish() {
	w.once.Do(func() { close(w.stop) })
	w.wg.Wait()
}

// awaitCommits waits until n more writes have committed: a workload window counted in operations rather than wall time.
func (c *dtCluster) awaitCommits(limit time.Duration, n int) {
	c.t.Helper()
	want := c.ops.count(dtOK) + n
	c.await(limit, fmt.Sprintf("%d more committed writes", n), func() bool { return c.ops.count(dtOK) >= want })
}

// --- settling and checking -----------------------------------------------------------------------------------

// settle ends every fault, restarts every node that is down (with its data), waits for the cluster to be ready and then
// checks liveness: a write to every partition must commit within dtLiveness. Finally it waits for every node to hold the
// same committed data.
func (c *dtCluster) settle() {
	c.t.Helper()
	c.Heal()
	for i := range c.nodes {
		if !c.isUp(i) {
			c.restart(i, true)
		}
	}
	c.awaitReady(30 * time.Second)
	c.settled(dtLiveness)
}

// settled is the part of settle after the nodes are up and healed: liveness probes and convergence.
func (c *dtCluster) settled(bound time.Duration) {
	c.t.Helper()
	t0 := time.Now()
	c.awaitOwners(bound)
	up := c.upNodes()
	var wg sync.WaitGroup
	errs := make(chan string, c.parts)
	for p := 0; p < c.parts; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			via := up[p%len(up)]
			deadline := t0.Add(bound)
			var last *dtOp
			for time.Now().Before(deadline) {
				op := c.increment(via, p, dtKey(p, 0))
				if op != nil && op.res == dtOK {
					return
				}
				last = op
				time.Sleep(20 * time.Millisecond)
			}
			errs <- fmt.Sprintf("liveness: no write of partition %d through node %d committed within %v of healing (last %v)", p, via, bound, last)
		}()
	}
	wg.Wait()
	close(errs)
	failed := false
	for e := range errs {
		c.t.Error(e)
		failed = true
	}
	if failed {
		c.dump()
		c.t.FailNow()
	}
	c.awaitConverged(20 * time.Second)
}

// converged reports whether the given nodes hold the same committed data in every partition.
func (c *dtCluster) converged(up []int) bool {
	if len(up) == 0 {
		return true
	}
	for p := 0; p < c.parts; p++ {
		var ref dtPartSnap
		for k, i := range up {
			_, s := c.node(i)
			if s == nil {
				return false
			}
			sn := s.snap(p)
			if k == 0 {
				ref = sn
				continue
			}
			if sn.St.Committed.Seq != ref.St.Committed.Seq || len(sn.Entries) != len(ref.Entries) {
				return false
			}
			for e := range sn.Entries {
				if string(sn.Entries[e].Data) != string(ref.Entries[e].Data) {
					return false
				}
			}
		}
	}
	return true
}

func (c *dtCluster) awaitConverged(limit time.Duration, nodes ...int) {
	c.t.Helper()
	c.await(limit, "every node to hold the same committed data", func() bool {
		if len(nodes) == 0 {
			return c.converged(c.upNodes())
		}
		return c.converged(nodes)
	})
}

// verify checks the safety invariants against everything that was recorded: it must be called once the cluster is settled.
func (c *dtCluster) verify(nodes ...int) {
	t := c.t
	t.Helper()
	failures := 0
	fail := func(format string, args ...any) {
		failures++
		if failures <= 25 {
			t.Errorf(format, args...)
		}
	}

	c.vmu.Lock()
	for _, v := range c.violations {
		fail("%s", v)
	}
	c.vmu.Unlock()
	c.led.mu.Lock()
	for _, v := range c.led.conflicts {
		fail("invariant 2: %s", v)
	}
	ledger := make(map[[2]uint64]dtCommit, len(c.led.bySeq))
	for k, v := range c.led.bySeq {
		ledger[k] = v
	}
	c.led.mu.Unlock()

	up := nodes
	if len(up) == 0 {
		up = c.upNodes()
	}
	if len(up) == 0 {
		t.Fatal("verify: no node is up")
	}

	// Invariant 3 (and 2): every node holds the same contiguous committed log.
	logs := make([][]memEntry, c.parts)
	for p := 0; p < c.parts; p++ {
		for k, i := range up {
			_, s := c.node(i)
			sn := s.snap(p)
			if err := s.checkInvariant(); err != nil {
				fail("store invariant (node %d): %v", i, err)
			}
			if uint64(len(sn.Entries)) != sn.St.Committed.Seq {
				fail("partition %d node %d: %d entries but committed %s", p, i, len(sn.Entries), sn.St.Committed)
			}
			for e, en := range sn.Entries {
				if en.Seq != uint64(e+1) {
					fail("partition %d node %d: entry %d has seq %d", p, i, e, en.Seq)
					break
				}
			}
			if k == 0 {
				logs[p] = sn.Entries
				continue
			}
			ref := logs[p]
			if len(ref) != len(sn.Entries) {
				fail("invariant 3: partition %d: node %d holds %d writes, node %d holds %d", p, i, len(sn.Entries), up[0], len(ref))
				continue
			}
			for e := range ref {
				if string(ref[e].Data) != string(sn.Entries[e].Data) {
					fail("invariant 3/2: partition %d seq %d: node %d has %q, node %d has %q", p, e+1, i, sn.Entries[e].Data, up[0], ref[e].Data)
					break
				}
			}
		}
	}

	// Invariant 1 and 2 over time: whatever any node ever committed (even on a disk that is gone) is in the final log.
	for k, cm := range ledger {
		p, seq := int(k[0]), k[1]
		if p >= c.parts {
			continue
		}
		if seq == 0 || seq > uint64(len(logs[p])) {
			fail("invariant 1: partition %d seq %d (%q, committed by node %d at %s) is missing from the final log of %d writes",
				p, seq, cm.Data, cm.Node, cm.Pos, len(logs[p]))
			continue
		}
		if got := string(logs[p][seq-1].Data); got != cm.Data {
			fail("invariant 1/2: partition %d seq %d was committed as %q (node %d, %s) but the final log has %q", p, seq, cm.Data, cm.Node, cm.Pos, got)
		}
	}

	// Invariant 4: per key the committed versions are 1, 2, 3, ... in log order: no change was applied against stale
	// state. Also index the log by operation.
	type at struct {
		p   int
		seq uint64
	}
	where := map[string]at{}
	finalVer := map[string]int{}
	for p := 0; p < c.parts; p++ {
		ver := map[string]int{}
		for _, e := range logs[p] {
			var d dtData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				fail("partition %d seq %d: undecodable data %q", p, e.Seq, e.Data)
				continue
			}
			if d.V != ver[d.K]+1 {
				fail("invariant 4: partition %d seq %d: key %s went from version %d to %d (a change applied against stale state)", p, e.Seq, d.K, ver[d.K], d.V)
			}
			ver[d.K] = d.V
			if prev, dup := where[d.ID]; dup {
				fail("operation %s committed twice: partition %d seq %d and partition %d seq %d", d.ID, prev.p, prev.seq, p, e.Seq)
			}
			where[d.ID] = at{p, e.Seq}
		}
		for k, v := range ver {
			finalVer[k] = v
		}
	}

	// What every caller was told against what is committed.
	ops := c.ops.all()
	okN, unkN := map[string]int{}, map[string]int{}
	known := map[string]bool{}
	var oks []*dtOp
	for _, op := range ops {
		known[op.id] = true
		_, inLog := where[op.id]
		switch op.res {
		case dtOK:
			okN[op.key]++
			oks = append(oks, op)
			if !inLog {
				fail("invariant 1: write %s (partition %d, %s) returned nil but is not in the committed log", op.id, op.p, op.key)
			} else if a := where[op.id]; op.pos.Seq != a.seq || op.reply != op.id {
				fail("write %s (partition %d, %s) returned position %s and reply %q, but it is committed at seq %d", op.id, op.p, op.key,
					op.pos, op.reply, a.seq)
			}
		case dtUnknown:
			unkN[op.key]++
		default:
			if inLog {
				fail("write %s (partition %d, %s) returned %s (%v) but it was committed", op.id, op.p, op.key, op.res, op.err)
			}
		}
	}
	for id, a := range where {
		if !known[id] {
			fail("partition %d seq %d: committed write %s that no caller made", a.p, a.seq, id)
		}
	}
	keys := map[string]bool{}
	for k := range okN {
		keys[k] = true
	}
	for k := range unkN {
		keys[k] = true
	}
	for k := range finalVer {
		keys[k] = true
	}
	for _, k := range dtSortedKeys(keys) {
		if v := finalVer[k]; v < okN[k] || v > okN[k]+unkN[k] {
			fail("key %s: final value %d, but %d increments returned nil and %d unknown", k, v, okN[k], unkN[k])
		}
	}

	// Real-time order: a write that returned before another began has the lower sequence number.
	byPart := map[int][]*dtOp{}
	for _, op := range oks {
		byPart[op.p] = append(byPart[op.p], op)
	}
	for p, list := range byPart {
		sort.Slice(list, func(a, b int) bool { return list[a].start.Before(list[b].start) })
		for a := range list {
			for b := a + 1; b < len(list); b++ {
				if list[b].start.After(list[a].end) && where[list[b].id].seq < where[list[a].id].seq {
					fail("invariant 1 (order): partition %d: %s returned before %s began but has the higher seq (%d > %d)",
						p, list[a].id, list[b].id, where[list[a].id].seq, where[list[b].id].seq)
				}
			}
		}
	}

	// Invariant 4 for reads: a read returns at least every write committed before the read started. A write returned nil
	// at op.end, so it was committed by then, as write number where[op.id].seq of its partition, taking its key to version
	// op.exp+1 or higher; a read that began after op.end must see at least that sequence number and that version. (And
	// nothing from the future: no more than the final log holds.)
	readOK, readFail := 0, 0
	type seen struct {
		end time.Time
		seq uint64
		ver int
	}
	byKey := map[string][]seen{} // per key, the writes that returned nil, in the order they returned
	for _, p := range sortedInts(byPart) {
		list := byPart[p]
		sort.Slice(list, func(a, b int) bool { return list[a].end.Before(list[b].end) })
		for _, op := range list {
			byKey[op.key] = append(byKey[op.key], seen{op.end, where[op.id].seq, op.exp + 1})
		}
	}
	for _, r := range c.ops.allReads() {
		if !r.ok {
			readFail++
			continue
		}
		readOK++
		if r.p >= c.parts || r.seq > uint64(len(logs[r.p])) || r.v > finalVer[r.key] {
			fail("read of %s (partition %d, via node %d) returned version %d at seq %d, beyond the final log (%d writes, version %d)",
				r.key, r.p, r.via, r.v, r.seq, len(logs[r.p]), finalVer[r.key])
			continue
		}
		for _, w := range byPart[r.p] {
			if !w.end.Before(r.start) {
				break // sorted by end: the rest returned after the read began
			}
			if s := where[w.id].seq; r.seq < s {
				fail("invariant 4 (read): read of %s via node %d began at +%v, after write %s returned (seq %d), but saw committed seq %d",
					r.key, r.via, r.start.Sub(w.end).Round(time.Millisecond), w.id, s, r.seq)
				break
			}
		}
		for _, w := range byKey[r.key] {
			if !w.end.Before(r.start) {
				break
			}
			if r.v < w.ver {
				fail("invariant 4 (read): read of %s via node %d began after a write that took it to version %d returned, but returned version %d",
					r.key, r.via, w.ver, r.v)
				break
			}
		}
	}

	if failures > 0 {
		c.dump()
		t.Fatalf("%d invariant violations (ops: %s)", failures, c.ops.summary())
	}
	t.Logf("verified: %s; committed writes %d; reads ok=%d unavailable=%d", c.ops.summary(), len(where), readOK, readFail)
}
