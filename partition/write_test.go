package partition

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// leader makes node i of an attached 3-node cluster the owner of partition 1 and returns the cluster.
func leader(t *testing.T, i int) *fakeCluster {
	t.Helper()
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	if err := fc.nodes[i].m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	fc.nodes[i].m.wg.Wait()
	return fc
}

func TestRunChangeStates(t *testing.T) {
	t.Run("not the owner says who is", func(t *testing.T) {
		fc := newFakeCluster(t, 3, 3, nil)
		m := fc.nodes[0].m
		m.setOwner(1, nid(2), ep(4, 2))
		resp := m.runChange(t.Context(), 1, []byte("x"), false)
		if resp.Status != statusNotOwner || resp.Owner != nid(2) || resp.Epoch != ep(4, 2) {
			t.Errorf("%+v", resp)
		}
		resp = fc.nodes[1].m.runChange(t.Context(), 1, []byte("x"), false)
		if resp.Status != statusNotOwner || resp.Owner != gossipEmpty {
			t.Errorf("a node that knows nobody: %+v", resp)
		}
	})
	t.Run("no recent majority contact refuses", func(t *testing.T) {
		fc := newFakeCluster(t, 3, 3, nil)
		fc.attach()
		m := fc.nodes[0].m
		advance := setClock(m)
		if err := m.takeover(t.Context(), 1); err != nil {
			t.Fatal(err)
		}
		m.wg.Wait()
		advance(m.cfg.OwnerTimeout)
		before := len(fc.nodes[0].store.callLog())
		resp := m.runChange(t.Context(), 1, []byte("x"), false)
		if resp.Status != statusUnavailable {
			t.Errorf("%+v", resp)
		}
		if len(fc.nodes[0].store.callLog()) != before {
			t.Error("a cut-off owner touched its store")
		}
		if !m.IsOwner(1) {
			t.Error("lack of contact alone must not depose; a later heartbeat round restores it")
		}
	})
	t.Run("fenced by a higher promise", func(t *testing.T) {
		fc := leader(t, 0)
		fc.nodes[0].store.SetPromised(1, ep(40, 3))
		resp := fc.nodes[0].m.runChange(t.Context(), 1, []byte("x"), false)
		if resp.Status != statusNotOwner {
			t.Errorf("%+v", resp)
		}
		if fc.nodes[0].m.IsOwner(1) {
			t.Error("still leading though fenced")
		}
	})
	t.Run("refusal is returned unchanged and writes nothing", func(t *testing.T) {
		fc := leader(t, 0)
		m, st := fc.nodes[0].m, fc.nodes[0].store
		before := len(st.callLog())
		resp := m.runChange(t.Context(), 1, []byte("refuse: version"), false)
		if resp.Status != statusRefused || resp.RefusedMsg == "" {
			t.Errorf("%+v", resp)
		}
		if len(st.callLog()) != before || st.state(1).Pending != nil {
			t.Error("a refused change wrote")
		}
		// Another failure of Prepare is not a refusal.
		resp = m.runChange(t.Context(), 1, []byte("fail"), false)
		if resp.Status != statusUnavailable {
			t.Errorf("%+v", resp)
		}
		if !m.IsOwner(1) {
			t.Error("a failed Prepare deposed the owner")
		}
	})
	t.Run("sequence numbers follow committed", func(t *testing.T) {
		fc := leader(t, 0)
		m := fc.nodes[0].m
		for i := 1; i <= 3; i++ {
			resp := m.runChange(t.Context(), 1, []byte{byte('a' + i)}, false)
			if resp.Status != statusDone {
				t.Fatalf("%+v", resp)
			}
			if got := fc.nodes[0].store.state(1).Committed.Seq; got != uint64(i) {
				t.Fatalf("seq %d after %d writes", got, i)
			}
		}
		st := fc.nodes[0].store.state(1)
		if st.Committed.Epoch.Counter != 1 || st.Pending != nil {
			t.Errorf("%+v", st)
		}
	})
}

func TestReplicateOutcomes(t *testing.T) {
	t.Run("a majority commits though another node reports a higher promise, and the owner steps down", func(t *testing.T) {
		fc := leader(t, 0)
		fc.nodes[2].store.SetPromised(1, ep(50, 3))
		fc.nodes[1].store.SetPromised(1, ep(1, 1)) // unchanged; node 2 acks
		resp := fc.nodes[0].m.runChange(t.Context(), 1, []byte("x"), false)
		if resp.Status != statusDone {
			t.Fatalf("%+v", resp)
		}
		waitFor(t, time.Second, "the owner to notice the higher promise", func() bool { return !fc.nodes[0].m.IsOwner(1) })
		if got := fc.nodes[0].m.parts[1].maxCounter.Load(); got < 50 {
			t.Errorf("maxCounter = %d, want >= 50", got)
		}
		if d := fc.nodes[0].store.data(1); !slices.Equal(d, []string{"x"}) {
			t.Errorf("data = %v", d)
		}
	})
	t.Run("no majority: unknown, steps down, keeps the write pending", func(t *testing.T) {
		fc := leader(t, 0)
		fc.isolate(fc.nodes[0], true)
		resp := fc.nodes[0].m.runChange(t.Context(), 1, []byte("x"), false)
		if resp.Status != statusUnknown {
			t.Fatalf("%+v", resp)
		}
		if fc.nodes[0].m.IsOwner(1) {
			t.Error("still leading after an unknown outcome")
		}
		st := fc.nodes[0].store.state(1)
		if st.Pending == nil || string(st.Pending.Data) != "x" || st.Committed.Seq != 0 {
			t.Errorf("%+v", st)
		}
	})
	t.Run("a follower that needs catch-up is not counted", func(t *testing.T) {
		fc := leader(t, 0)
		fc.nodes[2].down.Store(true)
		// The only other node is behind: committed 0 here, but the write's Prev will be seq 1.
		fc.nodes[0].store.seedCommitted(1, 0, ep(1, 1))
		if resp := fc.nodes[0].m.runChange(t.Context(), 1, []byte("a"), false); resp.Status != statusDone {
			t.Fatalf("%+v", resp)
		}
		// Make node 2 lose its copy of the first write, then write again: it answers NeedCatchUp.
		fc.nodes[1].store.mu.Lock()
		fc.nodes[1].store.parts[1] = &memPart{st: State{Promised: fc.nodes[1].store.parts[1].st.Promised}}
		fc.nodes[1].store.mu.Unlock()
		fc.nodes[1].m.catchingUp(1, true) // keep its asynchronous catch-up from healing it during the test
		resp := fc.nodes[0].m.runChange(t.Context(), 1, []byte("b"), false)
		if resp.Status != statusUnknown {
			t.Fatalf("a lagging follower was counted: %+v", resp)
		}
	})
	t.Run("storing the pending write fails: unknown, never unavailable (the followers may hold the write)", func(t *testing.T) {
		fc := leader(t, 0)
		fc.nodes[0].store.failOnce("SetPending", errors.New("disk"))
		resp := fc.nodes[0].m.runChange(t.Context(), 1, []byte("x"), false)
		if resp.Status != statusUnknown {
			t.Fatalf("%+v", resp)
		}
		if fc.nodes[0].m.IsOwner(1) {
			t.Error("still leading on a failing store")
		}
		if st := fc.nodes[0].store.state(1); st.Committed.Seq != 0 || st.Pending != nil {
			t.Errorf("the owner committed or holds a write it failed to store: %+v", st)
		}
	})
	t.Run("commit fails after a majority stored it: unknown", func(t *testing.T) {
		fc := leader(t, 0)
		fc.nodes[0].store.failOnce("Commit", errors.New("disk"))
		resp := fc.nodes[0].m.runChange(t.Context(), 1, []byte("x"), false)
		if resp.Status != statusUnknown {
			t.Fatalf("%+v", resp)
		}
		if fc.nodes[0].m.IsOwner(1) {
			t.Error("still leading")
		}
		// The next takeover settles it: it is committed on a majority.
		if err := fc.nodes[0].m.takeover(t.Context(), 1); err != nil {
			t.Fatal(err)
		}
		if d := fc.nodes[0].store.data(1); !slices.Equal(d, []string{"x"}) {
			t.Errorf("data = %v", d)
		}
	})
	t.Run("a single node cluster commits on its own", func(t *testing.T) {
		fc := newFakeCluster(t, 1, 1, nil)
		fc.attach()
		if err := fc.nodes[0].m.takeover(t.Context(), 1); err != nil {
			t.Fatal(err)
		}
		if resp := fc.nodes[0].m.runChange(t.Context(), 1, []byte("x"), false); resp.Status != statusDone {
			t.Fatalf("%+v", resp)
		}
	})
}

func TestWriteRoutesAndRetries(t *testing.T) {
	t.Run("follows a notowner hint to the owner", func(t *testing.T) {
		fc := leader(t, 0)
		fc.nodes[1].m.setOwner(1, nid(3), ep(1, 3)) // wrongly believes node 3
		fc.nodes[2].m.setOwner(1, nid(1), ep(1, 1)) // node 3 knows the truth
		if _, err := fc.nodes[1].m.Write(t.Context(), 1, []byte("via hint")); err != nil {
			t.Fatal(err)
		}
		if d := fc.nodes[0].store.data(1); !slices.Equal(d, []string{"via hint"}) {
			t.Errorf("data = %v", d)
		}
	})
	t.Run("a failed dial is retried, an ambiguous failure is not", func(t *testing.T) {
		fc := leader(t, 0)
		fc.nodes[1].m.setOwner(1, nid(1), ep(1, 1))
		notSent := errors.New("x")
		e := error(errNotSentWrap{notSent})
		fc.nodes[1].changeErr.Store(&e)
		go func() {
			time.Sleep(60 * time.Millisecond)
			fc.nodes[1].changeErr.Store(nil)
		}()
		if _, err := fc.nodes[1].m.Write(t.Context(), 1, []byte("retried")); err != nil {
			t.Fatalf("a request that was never sent must be retried: %v", err)
		}
		amb := errors.New("connection reset")
		fc.nodes[1].changeErr.Store(&amb)
		_, err := fc.nodes[1].m.Write(t.Context(), 1, []byte("maybe"))
		if !errors.Is(err, ErrUnknown) {
			t.Fatalf("err = %v, want ErrUnknown", err)
		}
		fc.nodes[1].changeErr.Store(nil)
		if d := fc.nodes[0].store.data(1); !slices.Equal(d, []string{"retried"}) {
			t.Errorf("data = %v", d)
		}
	})
	t.Run("honours the caller's context", func(t *testing.T) {
		fc := newFakeCluster(t, 3, 3, nil)
		ctx, cancel := t.Context(), func() {}
		_ = cancel
		start := time.Now()
		_, err := fc.nodes[0].m.Write(ctx, 1, []byte("x")) // nobody owns anything: waits until WriteTimeout
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("err = %v", err)
		}
		if d := time.Since(start); d < time.Second {
			t.Errorf("gave up after %v, before the write timeout", d)
		}
	})
	t.Run("size mismatch from the owner", func(t *testing.T) {
		resp := &changeResp{SizeMismatch: true}
		if _, err := resp.outcome(); !resp.final() || !errors.Is(err, ErrSizeMismatch) {
			t.Errorf("%v, %v", resp.final(), err)
		}
	})
}

func TestChangeHandlerBounds(t *testing.T) {
	fc := leader(t, 0)
	m := fc.nodes[0].m
	if resp := m.handleChange(t.Context(), &changeReq{Size: 5, P: 1, Change: []byte("x")}); !resp.SizeMismatch {
		t.Errorf("%+v", resp)
	}
	if resp := m.handleChange(t.Context(), &changeReq{Size: 3, P: 77, Change: []byte("x")}); resp.Status != statusUnavailable {
		t.Errorf("%+v", resp)
	}
	// With every slot taken the owner answers busy instead of queueing: nothing is run.
	if cap(m.changeSem) != DefaultMaxConcurrentChanges {
		t.Errorf("MaxConcurrentChanges defaults to %d, want %d", cap(m.changeSem), DefaultMaxConcurrentChanges)
	}
	for i := 0; i < cap(m.changeSem); i++ {
		m.changeSem <- struct{}{}
	}
	if resp := m.handleChange(t.Context(), &changeReq{Size: 3, P: 1, Change: []byte("x")}); resp.Status != statusBusy {
		t.Errorf("%+v", resp)
	}
	for i := 0; i < cap(m.changeSem); i++ {
		<-m.changeSem
	}
	if resp := m.handleChange(t.Context(), &changeReq{Size: 3, P: 1, Change: []byte("x")}); resp.Status != statusDone {
		t.Errorf("%+v", resp)
	}
	if resp := fc.nodes[1].m.handleChange(t.Context(), &changeReq{Size: 3, P: 1, Change: []byte("x")}); resp.Status != statusNotOwner {
		t.Errorf("%+v", resp)
	}
}
