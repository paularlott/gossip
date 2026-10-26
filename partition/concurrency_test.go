package partition

// Deferred handlers, request limits, worker availability, and replication overlap.

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip"
)

// Deferred handlers and request limits.

// fakeReplier records what a handler replied.
type fakeReplier struct {
	mu      sync.Mutex
	replies []any
	errs    []error
	done    chan struct{}
}

func newFakeReplier() *fakeReplier { return &fakeReplier{done: make(chan struct{}, 8)} }

func (r *fakeReplier) Reply(payload any, err error) {
	r.mu.Lock()
	r.replies = append(r.replies, payload)
	r.errs = append(r.errs, err)
	r.mu.Unlock()
	r.done <- struct{}{}
}

func (r *fakeReplier) wait(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("no reply")
	}
}

func (r *fakeReplier) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.replies)
}

func TestServeRunsWorkInAManagerGoroutineAndRepliesOnce(t *testing.T) {
	m := newFakeCluster(t, 1, 1, nil).nodes[0].m
	r := newFakeReplier()
	release := make(chan struct{})
	m.serve(r, m.reqSem, "refused", func() any {
		<-release
		return "answer"
	})
	// serve returned while the work still waits: the caller (a cluster worker) is free.
	if r.count() != 0 {
		t.Fatal("replied before the work was done")
	}
	if len(m.reqSem) != 1 {
		t.Errorf("the request does not hold a slot while it runs (%d)", len(m.reqSem))
	}
	close(release)
	r.wait(t)
	if r.replies[0] != "answer" || r.errs[0] != nil || r.count() != 1 {
		t.Errorf("%v %v", r.replies, r.errs)
	}
	waitFor(t, time.Second, "the slot to be released", func() bool { return len(m.reqSem) == 0 })
}

func TestServeRefusesBeyondTheLimitAndWhenStopping(t *testing.T) {
	m := newFakeCluster(t, 1, 1, func(i int, c *Config) { c.MaxConcurrentRequests = 2 }).nodes[0].m
	if cap(m.reqSem) != 2 {
		t.Fatalf("MaxConcurrentRequests = %d", cap(m.reqSem))
	}
	var ran atomic.Int32
	release := make(chan struct{})
	work := func() any { ran.Add(1); <-release; return "answer" }
	rs := []*fakeReplier{newFakeReplier(), newFakeReplier(), newFakeReplier()}
	for _, r := range rs {
		m.serve(r, m.reqSem, "refused", work)
	}
	// The third found the limit reached: refused at once, with the refusal and without running.
	rs[2].wait(t)
	if rs[2].replies[0] != "refused" || rs[2].errs[0] != nil {
		t.Errorf("over the limit: %v %v", rs[2].replies, rs[2].errs)
	}
	close(release)
	rs[0].wait(t)
	rs[1].wait(t)
	if got := ran.Load(); got != 2 {
		t.Errorf("%d works ran, want 2", got)
	}
	waitFor(t, time.Second, "the slots to be released", func() bool { return len(m.reqSem) == 0 })

	// A request without a semaphore (a change, bounded by its own) is not limited here.
	r := newFakeReplier()
	m.serve(r, nil, "refused", func() any { return "unbounded" })
	r.wait(t)
	if r.replies[0] != "unbounded" {
		t.Errorf("%v", r.replies)
	}

	// A Manager that is stopping refuses and gives its slot back.
	m.Stop()
	r = newFakeReplier()
	m.serve(r, m.reqSem, "refused", func() any { t.Error("work ran on a stopped Manager"); return nil })
	r.wait(t)
	if r.replies[0] != "refused" || len(m.reqSem) != 0 {
		t.Errorf("stopped: %v, %d slots held", r.replies, len(m.reqSem))
	}
}

// The handlers registered on a real cluster refuse over the limit with their "not OK" reply, never an acknowledgement,
// and answer again once there is room.
func TestRegisteredHandlersRefuseBeyondMaxConcurrentRequests(t *testing.T) {
	addrA, addrB := freeAddr(t), freeAddr(t)
	ca, cb := newRealCluster(t, addrA), newRealCluster(t, addrB)
	t.Cleanup(ca.Stop)
	t.Cleanup(cb.Stop)
	if err := cb.Join([]string{addrA}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "the two nodes to know each other", func() bool {
		return ca.GetNode(cb.LocalNode().ID) != nil && cb.GetNode(ca.LocalNode().ID) != nil
	})
	m, err := New(ca, Config{Partitions: 4, ClusterSize: 3, Store: newMemStore(), MaxConcurrentRequests: 2,
		HeartbeatInterval: time.Second, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	a := cb.GetNode(ca.LocalNode().ID)
	from := cb.LocalNode().ID
	e := Epoch{Counter: 5, Node: from}

	send := func(mt gossip.MessageType, req, resp any) {
		t.Helper()
		if err := cb.SendToWithResponse(a, mt, req, resp); err != nil {
			t.Fatalf("message %d: %v", mt, err)
		}
	}
	probe := func() (promise promiseResp, write writeResp, hb heartbeatResp, claim claimResp, state stateResp) {
		send(partPromiseMsg, &promiseReq{Size: 3, P: 1, Epoch: e}, &promise)
		send(partWriteMsg, &writeReq{Size: 3, Write: Write{Partition: 2, Pos: Pos{Epoch: e, Seq: 1}}}, &write)
		send(partHeartbeatMsg, &heartbeatMsg{Size: 3, From: from, Entries: []heartbeatEntry{{P: 3, Epoch: e}}}, &hb)
		send(partClaimMsg, &claimReq{Size: 3, P: 1}, &claim)
		send(partStateMsg, &stateReq{Size: 3}, &state)
		return
	}

	// Every slot taken: each request is refused.
	m.reqSem <- struct{}{}
	m.reqSem <- struct{}{}
	promise, write, hb, claim, state := probe()
	if promise.OK || promise.SizeMismatch {
		t.Errorf("promise over the limit: %+v", promise)
	}
	if write.OK || write.SizeMismatch {
		t.Errorf("write over the limit was acknowledged: %+v", write)
	}
	if hb.OK || hb.SizeMismatch {
		t.Errorf("heartbeat over the limit counted as delivered: %+v", hb)
	}
	if claim.OK {
		t.Errorf("claim over the limit: %+v", claim)
	}
	if len(state.Committed) != 0 || len(state.Promised) != 0 || state.SizeMismatch {
		t.Errorf("state over the limit answered: %+v", state)
	}
	if st, _ := m.store.State(1); st.Promised.Counter != 0 {
		t.Errorf("a refused promise was recorded: %+v", st)
	}
	if st, _ := m.store.State(2); st.Pending != nil {
		t.Errorf("a refused write was stored: %+v", st)
	}

	// Room again: served.
	<-m.reqSem
	<-m.reqSem
	promise, write, hb, _, state = probe()
	if !promise.OK || promise.Promised != e {
		t.Errorf("promise: %+v", promise)
	}
	if !write.OK {
		t.Errorf("write: %+v", write)
	}
	if !hb.OK {
		t.Errorf("heartbeat: %+v", hb)
	}
	if len(state.Committed) != 4 || len(state.Promised) != 4 {
		t.Errorf("state: %+v", state)
	}
	waitFor(t, 2*time.Second, "the slots to be released", func() bool { return len(m.reqSem) == 0 })
}

// Replication overlaps the owner's storage with sends to followers.

// The owner's SetPending runs at the same time as the sends: with it held up, the followers have stored the write, and the
// owner has committed nothing; it commits only after its own SetPending has returned.
func TestReplicateOverlapsOwnSetPending(t *testing.T) {
	fc := leader(t, 0)
	owner, f1, f2 := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releasePending := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releasePending)
	var once sync.Once
	owner.store.onSetPending = func(p int, w *Write) {
		once.Do(func() {
			close(entered)
			<-release
		})
	}

	res := make(chan *changeResp, 1)
	go func() {
		res <- owner.m.runChange(t.Context(), 1, []byte("x"), false)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the owner never began storing the write")
	}
	// The followers get the write while the owner's own SetPending is still running.
	waitFor(t, 2*time.Second, "both followers to store the write", func() bool {
		return f1.store.state(1).Pending != nil && f2.store.state(1).Pending != nil
	})
	time.Sleep(50 * time.Millisecond) // the acks have arrived by now; the owner must still not commit
	select {
	case r := <-res:
		t.Fatalf("the write finished before the owner's own SetPending returned: %+v", r)
	default:
	}
	if st := owner.store.state(1); st.Committed.Seq != 0 {
		t.Fatalf("the owner committed before its own SetPending returned: %+v", st)
	}
	owner.store.mu.Lock()
	for _, c := range owner.store.calls {
		if c == "Commit(1)" {
			t.Error("Commit was called before the owner's own SetPending returned")
		}
	}
	owner.store.mu.Unlock()

	releasePending()
	if r := <-res; r.Status != statusDone {
		t.Fatalf("%+v", r)
	}
	if d := owner.store.data(1); !slices.Equal(d, []string{"x"}) {
		t.Errorf("data = %v", d)
	}
	owner.store.mu.Lock()
	calls := slices.Clone(owner.store.calls)
	owner.store.mu.Unlock()
	last := func(name string) int { return slices.Index(calls, name) }
	if sp, c := last("SetPending(1)"), last("Commit(1)"); sp < 0 || c < 0 || sp > c {
		t.Errorf("order of the owner's store calls: %v", calls)
	}
}

// The owner's SetPending fails after both followers stored the write: the result is unknown, the owner steps down without
// committing, and a takeover settles the write: committed on every node.
func TestOwnerSetPendingFailsAfterFollowersStored(t *testing.T) {
	fc := leader(t, 0)
	owner, f1, f2 := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	owner.store.onSetPending = func(p int, w *Write) {
		// Hold the owner's own call until both followers hold the write, then fail it.
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
			if f1.store.state(1).Pending != nil && f2.store.state(1).Pending != nil {
				break
			}
		}
		owner.store.failOnce("SetPending", errors.New("disk"))
	}
	resp := owner.m.runChange(t.Context(), 1, []byte("x"), false)
	if resp.Status != statusUnknown {
		t.Fatalf("%+v, want unknown", resp)
	}
	if owner.m.IsOwner(1) {
		t.Error("still leading after its own SetPending failed")
	}
	if st := owner.store.state(1); st.Committed.Seq != 0 {
		t.Errorf("the owner committed a write it failed to store: %+v", st)
	}
	owner.store.onSetPending = nil

	// A takeover (by a follower) settles it.
	if err := f1.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	for i, fn := range fc.nodes {
		fn := fn
		waitFor(t, 5*time.Second, fmt.Sprintf("node %d to commit the write", i), func() bool {
			return slices.Equal(fn.store.data(1), []string{"x"})
		})
	}
}

// Incoming worker availability under load.

// With two incoming workers per node (the library's default is 8) and 64 concurrent writers, every write commits: no
// handler holds a worker while it waits for followers, so there are no timeouts (unknown or unavailable outcomes).
func TestManyWritersWithTwoIncomingWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a three node cluster")
	}
	bc := benchNewCluster(t, benchOptions{size: 3, parts: 16, heartbeat: 100 * time.Millisecond, workers: 2})
	const writers, each = 64, 6
	hot := bc.ownedBy(bc.nodes[0])[0]
	for _, phase := range []string{"spread", "one partition"} {
		var wg sync.WaitGroup
		var failed, done atomic.Int64
		start := time.Now()
		for g := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				n := bc.nodes[g%len(bc.nodes)]
				rng := rand.New(rand.NewPCG(uint64(g), 7))
				for range each {
					p := hot
					if phase == "spread" {
						p = rng.IntN(bc.parts)
					}
					if _, err := n.m.Write(t.Context(), p, benchPayload); err != nil {
						failed.Add(1)
						t.Errorf("%s: write %d via node %d: %v", phase, p, g%len(bc.nodes), err)
						return
					}
					done.Add(1)
				}
			}()
		}
		wg.Wait()
		t.Logf("%s: %d writes in %v, %d failed", phase, done.Load(), time.Since(start).Round(time.Millisecond), failed.Load())
		if done.Load() != writers*each {
			t.Errorf("%s: %d of %d writes committed", phase, done.Load(), writers*each)
		}
	}
}
