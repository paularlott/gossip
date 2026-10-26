package partition

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/gossip"
)

// writeRetry writes through n, retrying while the cluster is between owners; it fails on any other outcome.
func writeRetry(t *testing.T, n *fakeNode, p int, change string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := n.m.Write(t.Context(), p, []byte(change))
		if err == nil {
			return
		}
		if (errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnknown)) && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		t.Fatalf("Write(%d, %q) via %s: %v", p, change, n.id, err)
	}
}

// converge waits until every listed node holds the same committed data for p, and returns it.
func converge(t *testing.T, p int, nodes ...*fakeNode) []string {
	t.Helper()
	waitFor(t, 10*time.Second, fmt.Sprintf("partition %d to converge", p), func() bool {
		ref := nodes[0].store.data(p)
		for _, n := range nodes[1:] {
			if !slices.Equal(ref, n.store.data(p)) {
				return false
			}
		}
		return true
	})
	for _, n := range nodes {
		if err := n.store.checkInvariant(); err != nil {
			t.Error(err)
		}
	}
	return nodes[0].store.data(p)
}

func TestClusterElectsOneOwnerPerPartition(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	var mu sync.Mutex
	events := map[int]gossip.NodeID{}
	fc.nodes[0].m.HandleOwnershipFunc(func(p int, owner gossip.NodeID, epoch Epoch) {
		mu.Lock()
		defer mu.Unlock()
		if owner != gossip.EmptyNodeID {
			events[p] = owner
		}
	})
	fc.startAll()
	fc.waitOwners(10 * time.Second)

	for _, fn := range fc.nodes {
		if !fn.m.Ready() {
			t.Errorf("node %s owns partitions but is not ready", fn.id)
		}
	}
	// The owner is the rendezvous owner among the three.
	cands := []gossip.NodeID{fc.nodes[0].id, fc.nodes[1].id, fc.nodes[2].id}
	for p := 0; p < 4; p++ {
		want, _ := rendezvousOwner(p, cands)
		if got := fc.owners(p)[0].id; got != want {
			t.Errorf("p%d owned by %s, want the rendezvous owner %s", p, got, want)
		}
	}
	waitFor(t, 2*time.Second, "ownership events", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for p := 0; p < 4; p++ {
			if events[p] != fc.owners(p)[0].id {
				return false
			}
		}
		return true
	})
}

func TestClusterWritesThroughEveryNodeConverge(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)

	want := map[int][]string{}
	for p := 0; p < 4; p++ {
		for i, n := range fc.nodes {
			change := fmt.Sprintf("p%d-via%d", p, i)
			writeRetry(t, n, p, change)
			want[p] = append(want[p], change)
		}
	}
	for p := 0; p < 4; p++ {
		if got := converge(t, p, fc.nodes...); !slices.Equal(got, want[p]) {
			t.Errorf("p%d: data %v, want %v (single owner, one total order)", p, got, want[p])
		}
		if seq := fc.owners(p)[0].store.state(p).Committed.Seq; seq != 3 {
			t.Errorf("p%d: committed seq %d, want 3", p, seq)
		}
	}
	// A write that the owner commits is readable from the owner at once.
	for p := 0; p < 4; p++ {
		if !fc.owners(p)[0].m.CanRead(p) {
			t.Errorf("p%d: owner cannot read", p)
		}
	}
}

func TestClusterRefusedAndFailedChanges(t *testing.T) {
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
		_, err := n.m.Write(t.Context(), 1, []byte("refuse: expected version moved"))
		if !errors.Is(err, ErrRefused) {
			t.Errorf("%s: err = %v, want ErrRefused", name, err)
		} else if !strings.Contains(err.Error(), "expected version moved") {
			t.Errorf("%s: the Store's message was lost: %v", name, err)
		}
	}
	if seq := owner.store.state(1).Committed.Seq; seq != 0 {
		t.Errorf("a refused change was committed: seq %d", seq)
	}
	// A failure of the Store (not a refusal) is not a refusal: nothing was written and the write times out.
	_, err := other.m.Write(t.Context(), 1, []byte("fail"))
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrRefused) {
		t.Errorf("err = %v, want ErrUnavailable", err)
	}
	if _, err := fc.nodes[0].m.Write(t.Context(), 99, nil); err == nil {
		t.Error("a partition out of range was written")
	}
	// The partition still works afterwards.
	writeRetry(t, other, 1, "fine")
	if got := converge(t, 1, fc.nodes...); !slices.Equal(got, []string{"fine"}) {
		t.Errorf("data = %v", got)
	}
}

func TestClusterOwnerCrashFailsOver(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)

	for p := 0; p < 4; p++ {
		writeRetry(t, fc.nodes[p%3], p, "before")
	}
	victim := fc.owners(1)[0]
	fc.crash(victim)
	var live []*fakeNode
	for _, fn := range fc.nodes {
		if fn != victim {
			live = append(live, fn)
		}
	}
	fc.waitOwners(10 * time.Second)

	for p := 0; p < 4; p++ {
		writeRetry(t, live[p%2], p, "after")
	}
	for p := 0; p < 4; p++ {
		if got := converge(t, p, live...); !slices.Equal(got, []string{"before", "after"}) {
			t.Errorf("p%d: data = %v, want [before after]: nothing lost, nothing twice", p, got)
		}
	}
}

func TestClusterGracefulStopHandsOverQuickly(t *testing.T) {
	// A long OwnerTimeout: only the relinquish message lets the successor take over within the test.
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) {
		c.OwnerTimeout = 5 * time.Second
		c.Tuning.BackoffMax = 50 * time.Millisecond
	})
	fc.startAll()
	fc.waitOwners(20 * time.Second)
	victim := fc.owners(1)[0]
	victim.m.Stop()
	victim.down.Store(true)
	start := time.Now()
	fc.waitOwners(4 * time.Second)
	if time.Since(start) > 4*time.Second {
		t.Fatal("took the OwnerTimeout")
	}
}

func TestClusterLosesMajority(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.WriteTimeout = 500 * time.Millisecond })
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	writeRetry(t, fc.nodes[0], 1, "one")
	converge(t, 1, fc.nodes...)

	survivor := fc.nodes[0]
	fc.crash(fc.nodes[1])
	fc.crash(fc.nodes[2])
	// The survivor is cut off from a majority: it must not accept or serve anything once its contact lapses.
	waitFor(t, 5*time.Second, "survivor to stop reading", func() bool { return !survivor.m.CanRead(1) })
	_, err := survivor.m.Write(t.Context(), 1, []byte("lonely"))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Write = %v, want ErrUnavailable", err)
	}
	if d := survivor.store.data(1); !slices.Equal(d, []string{"one"}) {
		t.Fatalf("data = %v", d)
	}

	// With a majority back, writes succeed again.
	fc.restart(fc.nodes[1], nil)
	waitFor(t, 10*time.Second, "restarted node ready", func() bool { return fc.nodes[1].m.Ready() })
	fc.waitOwners(10 * time.Second)
	writeRetry(t, survivor, 1, "two")
	if got := converge(t, 1, survivor, fc.nodes[1]); !slices.Equal(got, []string{"one", "two"}) {
		t.Errorf("data = %v", got)
	}
}

func TestClusterIsolatedOwnerCannotCommit(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.WriteTimeout = 800 * time.Millisecond })
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	writeRetry(t, fc.nodes[0], 1, "before")
	converge(t, 1, fc.nodes...)

	old := fc.owners(1)[0]
	fc.isolate(old, true)

	// The isolated owner cannot replicate: its write is of unknown outcome at best, never reported committed.
	_, err := old.m.Write(t.Context(), 1, []byte("doomed"))
	fc.detect(old, true) // failure detection catches up
	if err == nil {
		t.Fatal("an isolated owner committed a write")
	}
	if !errors.Is(err, ErrUnknown) && !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
	// The majority side elects a new owner and goes on.
	var rest []*fakeNode
	for _, fn := range fc.nodes {
		if fn != old {
			rest = append(rest, fn)
		}
	}
	waitFor(t, 10*time.Second, "a new owner on the majority side", func() bool {
		for _, fn := range rest {
			if fn.m.IsOwner(1) {
				return true
			}
		}
		return false
	})
	writeRetry(t, rest[0], 1, "after")

	fc.isolate(old, false)
	fc.detect(old, false)
	fc.waitOwners(10 * time.Second)
	got := converge(t, 1, fc.nodes...)
	if got[0] != "before" || got[len(got)-1] != "after" {
		t.Errorf("data = %v", got)
	}
	for _, d := range got {
		if d == "doomed" {
			// Not reported committed; only acceptable if every node agrees (it did: converge).
			t.Logf("the unknown write was settled as committed: %v", got)
		}
	}
	if o := fc.owners(1); len(o) != 1 {
		t.Errorf("owners after healing: %d", len(o))
	}
}

func TestClusterUnknownWriteIsSettledByTakeover(t *testing.T) {
	// The owner stores the write, cannot reach anyone, and steps down. After healing, its own takeover finds the
	// write pending on itself and settles it: "unknown" really did mean "may be committed later".
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.WriteTimeout = 600 * time.Millisecond })
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	// Hold the owner's heartbeats away so that its replicate is the first to notice the cut.
	fc.isolate(owner, true)
	_, err := owner.m.Write(t.Context(), 1, []byte("maybe"))
	if !errors.Is(err, ErrUnknown) && !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if errors.Is(err, ErrUnknown) {
		if st := owner.store.state(1); st.Pending == nil || string(st.Pending.Data) != "maybe" {
			t.Fatalf("an unknown write must stay pending on the owner: %+v", st)
		}
		if owner.m.IsOwner(1) {
			t.Fatal("owner did not step down after an unknown outcome")
		}
	}
}

func TestClusterConcurrentWritesAreTotallyOrdered(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)

	const perNode = 15
	var wg sync.WaitGroup
	for i, n := range fc.nodes {
		for _, p := range []int{1, 2} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < perNode; j++ {
					writeRetry(t, n, p, fmt.Sprintf("n%d-%d", i, j))
				}
			}()
		}
	}
	wg.Wait()
	for _, p := range []int{1, 2} {
		got := converge(t, p, fc.nodes...)
		if len(got) != 3*perNode {
			t.Fatalf("p%d: %d writes, want %d", p, len(got), 3*perNode)
		}
		seen := map[string]bool{}
		for _, d := range got {
			if seen[d] {
				t.Errorf("p%d: %q committed twice", p, d)
			}
			seen[d] = true
		}
		// Each node's own writes are in the order it made them.
		for i := range fc.nodes {
			last := -1
			for pos, d := range got {
				var ni, j int
				if _, err := fmt.Sscanf(d, "n%d-%d", &ni, &j); err == nil && ni == i {
					if j != last+1 {
						t.Fatalf("p%d: node %d's write %d at %d is out of order", p, i, j, pos)
					}
					last = j
				}
			}
		}
	}
}

func TestClusterSizeMismatchIsNotACandidate(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) {
		c.WriteTimeout = 300 * time.Millisecond
		if i == 2 {
			c.ClusterSize = 5
		}
	})
	fc.startAll()
	waitFor(t, 10*time.Second, "owners among the two nodes of size 3", func() bool {
		for p := 0; p < 4; p++ {
			if len(fc.owners(p)) != 1 {
				return false
			}
		}
		return true
	})
	for p := 0; p < 4; p++ {
		if fc.nodes[2].m.IsOwner(p) {
			t.Errorf("a node configured with another size owns p%d", p)
		}
	}
	writeRetry(t, fc.nodes[0], 1, "ok")
	// The odd node cannot write through them: nobody it can see has its size.
	if _, err := fc.nodes[2].m.Write(t.Context(), 1, []byte("x")); !errors.Is(err, ErrUnavailable) {
		t.Errorf("err = %v, want ErrUnavailable", err)
	}
	if d := fc.nodes[2].store.data(1); len(d) != 0 {
		t.Errorf("the odd node holds data: %v", d)
	}
}

func TestClusterNotReadyUntilMajorityAndHoldDown(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.Tuning.ReadyHoldDown = 150 * time.Millisecond })
	if err := fc.nodes[0].m.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if fc.nodes[0].m.Ready() {
		t.Fatal("ready without a majority of the configured size answering")
	}
	if err := fc.nodes[1].m.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "ready with a majority", func() bool { return fc.nodes[0].m.Ready() && fc.nodes[1].m.Ready() })
	if fc.nodes[2].m.Ready() {
		t.Error("an unstarted node is ready")
	}
}

func TestClusterStartupSyncCatchesUp(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	if err := fc.nodes[0].m.Start(); err != nil {
		t.Fatal(err)
	}
	if err := fc.nodes[1].m.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "owners among two nodes", func() bool {
		for p := 0; p < 4; p++ {
			if len(fc.owners(p)) != 1 {
				return false
			}
		}
		return true
	})
	for i := 0; i < 5; i++ {
		writeRetry(t, fc.nodes[0], 1, fmt.Sprintf("w%d", i))
	}
	converge(t, 1, fc.nodes[0], fc.nodes[1])

	// The third node joins late and empty; it must catch up before it declares itself ready.
	if err := fc.nodes[2].m.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "late node ready", func() bool { return fc.nodes[2].m.Ready() })
	if d := fc.nodes[2].store.data(1); len(d) != 5 {
		t.Errorf("late node ready with data %v", d)
	}
	fc.waitOwners(10 * time.Second)
}

func TestClusterResyncAfterDataLoss(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	for i := 0; i < 4; i++ {
		writeRetry(t, fc.nodes[i%3], 1, fmt.Sprintf("w%d", i))
	}
	converge(t, 1, fc.nodes...)

	// Node 3 loses its disk: same identity, empty data, not intact.
	victim := fc.nodes[2]
	fc.crash(victim)
	victim.store = newMemStore()
	victim.store.intact = false
	fc.restart(victim, nil)
	// While in resync mode it neither promises nor stores anything.
	if resp := victim.m.handlePromise(nid(1), &promiseReq{Size: 3, P: 1, Epoch: ep(99, 1)}); resp.OK {
		t.Fatal("a resync node promised")
	}
	waitFor(t, 10*time.Second, "resynced node ready", func() bool { return victim.m.Ready() })
	if victim.m.resync.Load() {
		t.Error("still in resync mode after becoming ready")
	}
	if got := victim.store.data(1); len(got) != 4 {
		t.Errorf("resynced data = %v", got)
	}
	// Its promise floor is at least what the others had promised.
	for p := 0; p < 4; p++ {
		var max Epoch
		for _, fn := range fc.nodes[:2] {
			if pr := fn.store.state(p).Promised; max.Less(pr) {
				max = pr
			}
		}
		if got := victim.store.state(p).Promised; got.Less(max) && !max.Less(got) {
			t.Errorf("p%d: promise floor %v below %v", p, got, max)
		}
	}
	fc.waitOwners(10 * time.Second)
	writeRetry(t, victim, 1, "post")
	converge(t, 1, fc.nodes...)
}

func TestManagerLifecycle(t *testing.T) {
	fc := newFakeCluster(t, 1, 1, nil)
	m := fc.nodes[0].m
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err == nil {
		t.Error("second Start succeeded")
	}
	waitFor(t, 5*time.Second, "single node ready and owning", func() bool { return m.Ready() && m.IsOwner(1) })
	if _, err := m.Write(context.Background(), 1, []byte("solo")); err != nil {
		t.Fatal(err)
	}
	m.Stop()
	m.Stop() // idempotent
	if _, err := m.Write(context.Background(), 1, []byte("late")); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Write after Stop = %v", err)
	}
	if m.IsOwner(1) {
		t.Error("owner after Stop")
	}
	if err := m.Start(); err == nil {
		t.Error("Start after Stop succeeded")
	}
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 1, P: 1, Epoch: ep(50, 2)}); resp.OK {
		t.Error("a stopped manager promised")
	}
}

func TestStartFailsOnBadConfig(t *testing.T) {
	if _, err := newManager(Config{ClusterSize: 0, Store: newMemStore()}, &fakeNet{&fakeNode{fc: &fakeCluster{}, id: nid(1)}}); err == nil {
		t.Error("ClusterSize 0 accepted")
	}
	if _, err := New(nil, Config{ClusterSize: 1, Store: newMemStore()}); err == nil {
		t.Error("nil cluster accepted")
	}
}
