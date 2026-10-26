package partition

// Reads on the owner (Manager.Read, PartReadMsg, Store.Read) and the leadership confirmation round
// behind them. The destruction suite's read scenarios are in dt_read_test.go.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// readData reads the committed data of p through n and splits it into entries.
func readData(t *testing.T, n *fakeNode, p int) []string {
	t.Helper()
	b, err := n.m.Read(t.Context(), p, []byte("data"))
	if err != nil {
		t.Fatalf("Read(%d) via %s: %v", p, n.id, err)
	}
	if len(b) == 0 {
		return nil
	}
	return strings.Split(string(b), ",")
}

// A read sees every write committed before it began, on whatever node the write and the read land, including nodes whose
// own copy is a message behind.
func TestReadAfterWriteOnAnotherNodeSeesIt(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	for i := 0; i < 30; i++ {
		w := fc.nodes[i%3]
		r := fc.nodes[(i+1)%3]
		change := fmt.Sprintf("w%d", i)
		if _, err := w.m.Write(t.Context(), 1, []byte(change)); err != nil {
			t.Fatal(err)
		}
		got := readData(t, r, 1)
		if len(got) != i+1 || got[i] != change {
			t.Fatalf("read via %s after write %d via %s: %v", r.id, i, w.id, got)
		}
	}
	// The same, from the owner itself.
	if got := readData(t, owner, 1); len(got) != 30 {
		t.Errorf("read on the owner: %d entries, want 30", len(got))
	}
}

// A read does not see a pending write, and changes nothing.
func TestReadDoesNotSeePendingAndChangesNothing(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	writeRetry(t, owner, 1, "a")
	converge(t, 1, fc.nodes...)
	before := owner.store.callLog()
	owner.store.seedPending(1, Write{Partition: 1, Pos: Pos{Epoch: owner.m.parts[1].myEpoch, Seq: 99}, Data: []byte("pending")})
	got := readData(t, owner, 1)
	if len(got) != 1 || got[0] != "a" {
		t.Errorf("read = %v, want [a]", got)
	}
	// Nothing mutating was called for the read (no SetPromised, SetPending, Commit, Import).
	after := owner.store.callLog()
	for _, c := range after[len(before):] {
		if strings.HasPrefix(c, "Commit") || strings.HasPrefix(c, "Import") || strings.HasPrefix(c, "SetPromised") {
			t.Errorf("a read called %s", c)
		}
	}
}

// A Store refusal travels like a change's: a *RefusedError with its code, locally and from another node; any other Store
// error is ErrUnavailable; a request for a partition out of range or a stopped Manager is an error at once.
func TestReadRefusalCodeAndStoreFailure(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.WriteTimeout = 400 * time.Millisecond })
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	var other *fakeNode
	for _, fn := range fc.nodes {
		if fn != owner {
			other = fn
		}
	}
	for name, n := range map[string]*fakeNode{"owner": owner, "non-owner": other} {
		b, err := n.m.Read(t.Context(), 1, []byte("conflict: moved"))
		var re *RefusedError
		if !errors.As(err, &re) || re.Code != "conflict" || re.Msg != "memStore: conflict: moved" || !errors.Is(err, ErrRefused) || b != nil {
			t.Errorf("%s: %q, %#v", name, b, err)
		}
		_, err = n.m.Read(t.Context(), 1, []byte("refuse: plain"))
		if !errors.As(err, &re) || re.Code != "" || !errors.Is(err, ErrRefused) {
			t.Errorf("%s: plain refusal: %#v", name, err)
		}
		if _, err = n.m.Read(t.Context(), 1, []byte("fail")); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: a failing Store.Read gave %v, want ErrUnavailable", name, err)
		}
		if _, err = n.m.Read(t.Context(), 99, nil); err == nil || errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: out of range partition: %v", name, err)
		}
	}
	// A failing Store.Read must not have made the owner step down: it is a Store problem, not a leadership one.
	if !owner.m.IsOwner(1) {
		t.Error("the owner stepped down because Store.Read failed")
	}
	other.m.Stop()
	if _, err := other.m.Read(t.Context(), 1, []byte("data")); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Read on a stopped Manager: %v", err)
	}
}

// A node that is not the owner and believes in the wrong owner is told where the owner is, and the read gets there.
func TestReadFollowsTheOwnerHint(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	writeRetry(t, owner, 1, "x")
	var wrong, asker *fakeNode
	for _, fn := range fc.nodes {
		switch {
		case fn == owner:
		case wrong == nil:
			wrong = fn
		default:
			asker = fn
		}
	}
	// The asker believes that the other follower owns p; that node answers notowner with the real owner.
	ps := asker.m.parts[1]
	ps.vmu.Lock()
	ps.owner, ps.ownerSeen = wrong.id, time.Now().Add(time.Minute) // fresh for as long as the test runs
	ps.vmu.Unlock()
	got := readData(t, asker, 1)
	if len(got) != 1 || got[0] != "x" {
		t.Errorf("read = %v", got)
	}
}

// Many forwarded reads against an owner that serves one at a time are all answered: busy is retried, never an error.
func TestReadsBusyAreRetried(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.Tuning.MaxConcurrentReads = 1 })
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	writeRetry(t, owner, 1, "x")
	var other *fakeNode
	for _, fn := range fc.nodes {
		if fn != owner {
			other = fn
		}
	}
	var wg sync.WaitGroup
	var failed atomic.Int64
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b, err := other.m.Read(t.Context(), 1, []byte("data")); err != nil || string(b) != "x" {
				failed.Add(1)
				t.Errorf("read: %q, %v", b, err)
			}
		}()
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("%d reads failed", failed.Load())
	}
}

// holdPartition takes p's mutex on node n (so its heartbeat handler cannot answer for p) until the returned function is
// called.
func holdPartition(t *testing.T, n *fakeNode, p int) (release func()) {
	t.Helper()
	if err := n.m.parts[p].mu.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	rel := func() { once.Do(n.m.parts[p].mu.unlock) }
	t.Cleanup(rel)
	return rel
}

// Concurrent reads share leadership rounds, and a read is served only by a round that started after it arrived: with the
// followers unable to answer, one read starts a round; the reads that arrive while it is in flight wait for the next one,
// which they all share. So 1 + 30 reads take exactly two rounds.
func TestReadsShareRoundsAndNeedOneStartedAfterArrival(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) {
		c.OwnerTimeout = 3 * time.Second // the held followers must not look dead
		c.WriteTimeout = 10 * time.Second
	})
	fc.startAll()
	fc.waitOwners(20 * time.Second)
	owner := fc.owners(1)[0]
	writeRetry(t, owner, 1, "x")
	var releases []func()
	for _, fn := range fc.nodes {
		if fn != owner {
			releases = append(releases, holdHeartbeats(t, fn))
		}
	}

	base := owner.m.readRounds.Load()
	results := make(chan error, 31)
	read := func() {
		b, err := owner.m.Read(t.Context(), 1, []byte("data"))
		if err == nil && string(b) != "x" {
			err = fmt.Errorf("read %q", b)
		}
		results <- err
	}
	go read()
	waitFor(t, 5*time.Second, "the first round to start", func() bool { return owner.m.readRounds.Load() == base+1 })
	for i := 0; i < 30; i++ {
		go read()
	}
	// Let them all arrive (they are blocked on the next round), and see that nothing completed: the round in flight started
	// before they came and cannot serve them (and the followers cannot answer it yet anyway).
	time.Sleep(150 * time.Millisecond)
	select {
	case err := <-results:
		t.Fatalf("a read completed while the followers could not answer: %v", err)
	default:
	}
	if got := owner.m.readRounds.Load(); got != base+1 {
		t.Fatalf("%d rounds started while the first was in flight, want 1", got-base)
	}
	for _, r := range releases {
		r()
	}
	for i := 0; i < 31; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("reads did not complete")
		}
	}
	if got := owner.m.readRounds.Load() - base; got != 2 {
		t.Errorf("31 reads took %d rounds, want 2 (one for the first read, one shared by the 30 that arrived during it)", got)
	}
}

// Reads that arrive one after the other each get their own round (nothing is served from an old confirmation), and
// concurrent ones share: far fewer rounds than reads.
func TestConcurrentReadsNeedFarFewerRoundsThanReads(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	writeRetry(t, owner, 1, "x")
	base := owner.m.readRounds.Load()
	for i := 0; i < 5; i++ {
		readData(t, owner, 1)
	}
	if got := owner.m.readRounds.Load() - base; got != 5 {
		t.Errorf("5 sequential reads took %d rounds, want 5", got)
	}
	base = owner.m.readRounds.Load()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readData(t, owner, 1)
		}()
	}
	wg.Wait()
	if got := owner.m.readRounds.Load() - base; got >= 100 {
		t.Errorf("200 concurrent reads took %d rounds", got)
	}
}

// A round that cannot get a majority fails, the owner steps down (as it would for lost heartbeats) and the read says so:
// unavailable at once when every other node is unreachable, and a refusal to answer, never a stale answer.
func TestReadRoundWithoutMajorityStepsDown(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	writeRetry(t, owner, 1, "x")
	fc.isolate(owner, true) // nobody detects it: the owner still believes it owns p
	resp := owner.m.handleRead(t.Context(), &readReq{Size: 3, P: 1, Req: []byte("data")})
	if resp.Status != statusUnavailable && resp.Status != statusNotOwner {
		t.Errorf("handleRead without a majority: %+v", resp)
	}
	if owner.m.IsOwner(1) {
		t.Error("the owner did not step down after a failed round")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, err := owner.m.Read(ctx, 1, []byte("data")); err == nil {
		// The isolated owner has no one to ask.
		t.Error("a read on an isolated node was answered")
	}
}

// An owner that was deposed without knowing it (a node holds a higher promise) is fenced by the round: it steps down and
// answers notowner; it never serves the read.
func TestReadRoundFencedOwnerStepsDown(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	writeRetry(t, owner, 1, "x")
	// Every follower promises an epoch above the owner's; the owner's own promise is untouched, as for an owner that has
	// not heard of the new epoch.
	high := Epoch{Counter: 1000, Node: nid(9)}
	for _, fn := range fc.nodes {
		if fn != owner {
			if err := fn.m.setPromised(1, high); err != nil {
				t.Fatal(err)
			}
		}
	}
	resp := owner.m.handleRead(t.Context(), &readReq{Size: 3, P: 1, Req: []byte("data")})
	if resp.Status != statusNotOwner {
		t.Errorf("handleRead by a fenced owner: %+v", resp)
	}
	if owner.m.IsOwner(1) {
		t.Error("a fenced owner did not step down")
	}
}

// Only an entry the receiver accepts counts as a confirmation: not one from a node in resync mode or one below its
// promise. The decision needs no partition mutex: an entry is accepted, at once, while the partition is busy.
func TestHeartbeatAcceptedOnlyForTheCurrentOwner(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	owner, f := fc.nodes[0], fc.nodes[1]
	e := ep(5, 1)
	hb := func(epoch Epoch) *heartbeatResp {
		return f.m.handleHeartbeat(owner.id, &heartbeatMsg{Size: 3, From: owner.id,
			Entries: []heartbeatEntry{{P: 1, Epoch: epoch}}})
	}
	if r := hb(e); !r.OK || len(r.Accepted) != 1 || r.Accepted[0] != 1 || len(r.Fenced) != 0 {
		t.Errorf("a plain entry: %+v", r)
	}
	// A higher promise: fenced, not accepted.
	if err := f.m.setPromised(1, ep(6, 2)); err != nil {
		t.Fatal(err)
	}
	if r := hb(e); !r.OK || len(r.Accepted) != 0 || len(r.Fenced) != 1 {
		t.Errorf("an entry below the promise: %+v", r)
	}
	// An owner at a higher epoch is already known: the lower one is stale and not accepted (and not fenced either).
	if r := hb(ep(7, 1)); len(r.Accepted) != 1 {
		t.Errorf("the higher entry: %+v", r)
	}
	if r := f.m.handleHeartbeat(fc.nodes[2].id, &heartbeatMsg{Size: 3, From: fc.nodes[2].id,
		Entries: []heartbeatEntry{{P: 1, Epoch: ep(6, 3)}}}); len(r.Accepted) != 0 {
		t.Errorf("a heartbeat of another node below the epoch of the owner known: %+v", r)
	}
	// Resync mode: the node has forgotten what it promised, so its answer proves nothing.
	f.m.resync.Store(true)
	if r := hb(ep(9, 1)); !r.OK || len(r.Accepted) != 0 {
		t.Errorf("a node in resync mode accepted: %+v", r)
	}
	f.m.resync.Store(false)
	// The partition's mutex is held (a long write, a takeover): the entry is still decided at once, from the cached
	// promise, and its settling waits for the partition's goroutine.
	rel := holdPartition(t, f, 1)
	start := time.Now()
	if r := hb(ep(9, 1)); len(r.Accepted) != 1 {
		t.Errorf("an entry of a busy partition: %+v", r)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("the heartbeat reply waited %v for the busy partition", d)
	}
	if err := f.m.setPromised(1, ep(10, 2)); err != nil {
		t.Fatal(err)
	}
	if r := hb(ep(9, 1)); len(r.Accepted) != 0 || len(r.Fenced) != 1 {
		t.Errorf("an entry below a promise given while the partition was busy: %+v", r)
	}
	rel()
}

// With a cluster of one, a read needs no one else.
func TestReadOnAClusterOfOne(t *testing.T) {
	fc := newFakeCluster(t, 1, 1, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	n := fc.nodes[0]
	writeRetry(t, n, 1, "solo")
	if got := readData(t, n, 1); len(got) != 1 || got[0] != "solo" {
		t.Errorf("read = %v", got)
	}
}

// A new owner that is still settling the last write of the owner before it (it re-proposes it, and holds the partition's
// mutex until that is committed) must not answer a read from before that write: the read waits for the takeover, and then sees
// the write, which the old owner may have acknowledged.
func TestReadWaitsForATakeoverToSettleTheLastWrite(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	a, b, c := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	// The dead owner (node 9) stored w1 on a majority (a and b) and may have acknowledged it; nobody committed it but
	// the owner itself.
	w1 := Write{Partition: 1, Pos: Pos{Epoch: ep(1, 9), Seq: 1}, Data: []byte("w1")}
	a.store.seedPending(1, w1)
	b.store.seedPending(1, w1)
	hold, held := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.store.mu.Lock()
	c.store.onSetPending = func(p int, w *Write) { // the settling write's own SetPending of the new owner
		once.Do(func() { close(held) })
		<-hold
	}
	c.store.mu.Unlock()

	took := make(chan error, 1)
	go func() { took <- c.m.takeover(t.Context(), 1) }()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the takeover never reached the settling write")
	}
	if !c.m.IsOwner(1) {
		t.Fatal("the new owner is not leading while it settles")
	}
	type result struct {
		b   []byte
		err error
	}
	got := make(chan result, 1)
	go func() {
		b, err := c.m.Read(t.Context(), 1, []byte("data"))
		got <- result{b, err}
	}()
	select {
	case r := <-got:
		t.Fatalf("a read was answered while the takeover was settling the last write: %q, %v", r.b, r.err)
	case <-time.After(200 * time.Millisecond):
	}
	close(hold)
	if err := <-took; err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-got:
		if r.err != nil || string(r.b) != "w1" {
			t.Errorf("read after the takeover = %q, %v, want w1", r.b, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read was not answered after the takeover")
	}
}
