package partition

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteReturnsPositionAndReplyLocallyAndOverTheNetwork(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
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
		before := owner.store.state(1).Committed.Seq
		res, err := n.m.Write(t.Context(), 1, []byte("hello-"+name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if string(res.Reply) != "ok:hello-"+name {
			t.Errorf("%s: reply %q", name, res.Reply)
		}
		if res.Pos.Seq != before+1 || res.Pos.Epoch.Node != owner.id {
			t.Errorf("%s: position %s after %d writes by %s", name, res.Pos, before, owner.id)
		}
		if st := owner.store.state(1); st.Committed != res.Pos {
			t.Errorf("%s: owner committed %s, Write said %s", name, st.Committed, res.Pos)
		}
	}
	// On any error the result is zero.
	for name, n := range map[string]*fakeNode{"owner": owner, "non-owner": other} {
		res, err := n.m.Write(t.Context(), 1, []byte("conflict: "+name))
		if err == nil || res.Pos != (Pos{}) || res.Reply != nil {
			t.Errorf("%s: %+v, %v", name, res, err)
		}
	}
}

func TestRefusedErrorKeepsItsCodeLocallyAndOverTheNetwork(t *testing.T) {
	if err := Refuse("conflict", "version moved"); err.Error() != "version moved" || !errors.Is(err, ErrRefused) {
		t.Errorf("Refuse: %v", err)
	}
	if (&RefusedError{}).Error() != ErrRefused.Error() {
		t.Error("an empty RefusedError has no text")
	}
	fc := newFakeCluster(t, 3, 3, nil)
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
		_, err := n.m.Write(t.Context(), 1, []byte("conflict: moved"))
		var re *RefusedError
		if !errors.As(err, &re) || re.Code != "conflict" || re.Msg != "memStore: conflict: moved" || !errors.Is(err, ErrRefused) {
			t.Errorf("%s: err = %#v", name, err)
		}
		// An error that only wraps ErrRefused is refused too, without a code, and its text is kept.
		_, err = n.m.Write(t.Context(), 1, []byte("refuse: plain"))
		if !errors.As(err, &re) || re.Code != "" || !errors.Is(err, ErrRefused) || re.Error() == "" {
			t.Errorf("%s: plain refusal: %#v", name, err)
		}
	}
	if seq := owner.store.state(1).Committed.Seq; seq != 0 {
		t.Errorf("refused changes were committed: %d", seq)
	}
}

func TestWaitAppliedOnAFollowerReturnsPromptlyAfterAWrite(t *testing.T) {
	// A heartbeat interval far longer than the bound below: only the commit notice can make the follower apply the write
	// in time.
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) {
		c.HeartbeatInterval = 400 * time.Millisecond
		c.OwnerTimeout = 3 * time.Second
		c.Tuning.RankDelay = time.Second
		c.Tuning.BackoffMax = 40 * time.Millisecond
	})
	fc.startAll()
	fc.waitOwners(20 * time.Second)
	owner := fc.owners(1)[0]
	var followers []*fakeNode
	for _, fn := range fc.nodes {
		if fn != owner {
			followers = append(followers, fn)
		}
	}
	for i := 0; i < 5; i++ {
		res, err := owner.m.Write(t.Context(), 1, []byte(fmt.Sprintf("w%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for _, f := range followers {
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			err := f.m.WaitApplied(ctx, 1, res.Pos)
			cancel()
			if err != nil {
				t.Fatalf("write %d: follower %s: %v", i, f.id, err)
			}
			if st := f.store.state(1); st.Committed.Seq < res.Pos.Seq {
				t.Fatalf("WaitApplied returned before the write was applied: %s < %s", st.Committed, res.Pos)
			}
		}
		if d := time.Since(start); d > 100*time.Millisecond { // a quarter of a heartbeat interval
			t.Errorf("write %d: followers applied it after %v", i, d)
		}
	}
}

func TestWaitAppliedEdges(t *testing.T) {
	fc := newFakeCluster(t, 1, 1, nil)
	m, st := fc.nodes[0].m, fc.nodes[0].store
	st.seedCommitted(1, 3, ep(1, 1))
	if err := m.WaitApplied(t.Context(), 1, Pos{Seq: 3}); err != nil {
		t.Errorf("already applied: %v", err)
	}
	if err := m.WaitApplied(t.Context(), 1, Pos{}); err != nil {
		t.Errorf("the zero position: %v", err)
	}
	if err := m.WaitApplied(t.Context(), 99, Pos{Seq: 1}); err == nil {
		t.Error("a partition out of range")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := m.WaitApplied(ctx, 1, Pos{Seq: 4}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("not applied: %v", err)
	}
	// Sequence numbers decide: a write labelled with another epoch is the same write.
	if err := m.WaitApplied(t.Context(), 1, Pos{Epoch: ep(9, 9), Seq: 2}); err != nil {
		t.Errorf("another epoch label: %v", err)
	}

	// A commit and an import wake it; so does Stop (with ErrUnavailable).
	got := make(chan error, 3)
	for _, seq := range []uint64{4, 6} {
		go func() { got <- m.WaitApplied(t.Context(), 1, Pos{Seq: seq}) }()
	}
	time.Sleep(20 * time.Millisecond)
	if err := m.commit(1, &Write{Partition: 1, Pos: Pos{Epoch: ep(1, 1), Seq: 4}, Prev: st.state(1).Committed, Data: []byte("d4")}); err != nil {
		t.Fatal(err)
	}
	if err := <-got; err != nil {
		t.Errorf("after a commit: %v", err)
	}
	select {
	case err := <-got:
		t.Fatalf("seq 6 returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	m.Stop()
	if err := <-got; !errors.Is(err, ErrUnavailable) {
		t.Errorf("after Stop: %v", err)
	}
}

func TestCommitNoticeIsAHeartbeatEntry(t *testing.T) {
	e1 := ep(1, 2)
	t.Run("settles the pending write without a heartbeat", func(t *testing.T) {
		fc := newFakeCluster(t, 3, 3, nil)
		fc.attach()
		f := fc.nodes[0]
		f.store.seedCommitted(1, 3, e1)
		f.store.seedPending(1, Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("d4")})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		waited := make(chan error, 1)
		go func() { waited <- f.m.WaitApplied(ctx, 1, Pos{Epoch: e1, Seq: 4}) }()
		f.m.handleCommit(nid(2), &commitMsg{Size: 3, P: 1, Epoch: e1, Committed: Pos{Epoch: e1, Seq: 4}})
		if err := <-waited; err != nil {
			t.Fatal(err)
		}
		if st := f.store.state(1); st.Committed.Seq != 4 || st.Pending != nil {
			t.Errorf("state: %+v", st)
		}
		if o, ok := f.m.Owner(1); !ok || o != nid(2) {
			t.Errorf("owner = %v, %v: a notice names its sender as the owner, like a heartbeat", o, ok)
		}
	})
	t.Run("a node behind catches up", func(t *testing.T) {
		fc := newFakeCluster(t, 3, 3, nil)
		fc.attach()
		f, owner := fc.nodes[0], fc.nodes[1]
		owner.store.seedCommitted(1, 5, e1)
		f.store.seedCommitted(1, 2, e1)
		f.m.handleCommit(nid(2), &commitMsg{Size: 3, P: 1, Epoch: e1, Committed: Pos{Epoch: e1, Seq: 5}})
		f.m.wg.Wait()
		if st := f.store.state(1); st.Committed.Seq != 5 {
			t.Errorf("state: %+v", st)
		}
	})
	t.Run("is fenced like a heartbeat", func(t *testing.T) {
		fc := newFakeCluster(t, 3, 3, nil)
		fc.attach()
		f := fc.nodes[0]
		f.store.seedCommitted(1, 3, e1)
		f.store.seedPending(1, Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("d4")})
		f.store.SetPromised(1, ep(5, 3))
		f.m.handleCommit(nid(2), &commitMsg{Size: 3, P: 1, Epoch: e1, Committed: Pos{Epoch: e1, Seq: 4}})
		if st := f.store.state(1); st.Committed.Seq != 3 || st.Pending == nil {
			t.Errorf("a deposed owner's notice was applied: %+v", st)
		}
		if _, ok := f.m.Owner(1); ok {
			t.Error("a deposed owner's notice was believed")
		}
		// Another cluster size, a stopped manager and an epoch that is not the sender's are ignored.
		f.store.SetPromised(1, Epoch{})
		f.m.handleCommit(nid(2), &commitMsg{Size: 5, P: 1, Epoch: e1, Committed: Pos{Epoch: e1, Seq: 4}})
		f.m.handleCommit(nid(3), &commitMsg{Size: 3, P: 1, Epoch: e1, Committed: Pos{Epoch: e1, Seq: 4}})
		if st := f.store.state(1); st.Committed.Seq != 3 {
			t.Errorf("state: %+v", st)
		}
	})
	t.Run("the owner sends one after every commit", func(t *testing.T) {
		fc := leader(t, 0)
		f := fc.nodes[1]
		res, err := fc.nodes[0].m.Write(t.Context(), 1, []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := f.m.WaitApplied(ctx, 1, res.Pos); err != nil {
			t.Fatalf("the follower never applied the write: %v", err)
		}
	})
}

func TestBusyOwnerIsRetriedAndEveryWriteLands(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, func(i int, c *Config) { c.Tuning.MaxConcurrentChanges = 1 })
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	var landing []*fakeNode
	for _, fn := range fc.nodes {
		if fn != owner {
			landing = append(landing, fn)
		}
	}
	const writers, each = 16, 4
	var wg sync.WaitGroup
	var failed atomic.Int64
	start := time.Now()
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				if _, err := landing[w%len(landing)].m.Write(t.Context(), 1, []byte(fmt.Sprintf("w%d-%d", w, j))); err != nil {
					t.Errorf("writer %d write %d: %v", w, j, err)
					failed.Add(1)
					return
				}
			}
		}()
	}
	wg.Wait()
	if failed.Load() > 0 {
		return
	}
	busy := landing[0].busyReplies.Load() + landing[1].busyReplies.Load()
	t.Logf("%d writes in %v, %d busy replies", writers*each, time.Since(start), busy)
	if busy == 0 {
		t.Error("the owner was never busy: the limit was not exercised")
	}
	got := converge(t, 1, fc.nodes...)
	if len(got) != writers*each {
		t.Errorf("%d writes committed, want %d", len(got), writers*each)
	}
	seen := map[string]bool{}
	for _, d := range got {
		if seen[d] {
			t.Errorf("%q committed twice", d)
		}
		seen[d] = true
	}
}
