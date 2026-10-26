package partition

import (
	"errors"
	"testing"
	"time"
)

// setClock fixes the Manager's clock and returns a function that moves it.
func setClock(m *Manager) func(d time.Duration) {
	now := time.Unix(1_000_000, 0)
	m.now = func() time.Time { return now }
	return func(d time.Duration) { now = now.Add(d) }
}

func TestPromiseRefusesLowerAndEqualEpochs(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m, st := fc.nodes[0].m, fc.nodes[0].store
	st.seedCommitted(1, 2, ep(1, 2))
	st.seedPending(1, Write{Pos: Pos{Epoch: ep(1, 2), Seq: 3}, Prev: Pos{Epoch: ep(1, 2), Seq: 2}, Data: []byte("p")})

	resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(5, 2)})
	if !resp.OK || resp.Promised != ep(5, 2) {
		t.Fatalf("a higher epoch must be promised: %+v", resp)
	}
	if resp.Committed != (Pos{Epoch: ep(1, 2), Seq: 2}) || resp.Pending == nil || string(resp.Pending.Data) != "p" {
		t.Errorf("the reply must carry committed and pending: %+v", resp)
	}
	if got := st.state(1).Promised; got != ep(5, 2) {
		t.Fatalf("promise not stored: %v", got)
	}

	for name, e := range map[string]Epoch{
		"equal":                  ep(5, 2),
		"lower counter":          ep(4, 3),
		"lower counter, high ID": ep(4, 9),
		"same counter, lower ID": ep(5, 1), // nid(1) sorts below nid(2)? checked below
	} {
		if name == "same counter, lower ID" && !e.Less(ep(5, 2)) {
			continue // the byte order of the test IDs is not the point
		}
		resp := m.handlePromise(nid(3), &promiseReq{Size: 3, P: 1, Epoch: e})
		if resp.OK || resp.Promised != ep(5, 2) {
			t.Errorf("%s: want refusal carrying the stored promise, got %+v", name, resp)
		}
		if got := st.state(1).Promised; got != ep(5, 2) {
			t.Errorf("%s: promise changed to %v", name, got)
		}
	}

	// Higher by counter, and higher by node ID at the same counter, are promised.
	if resp := m.handlePromise(nid(3), &promiseReq{Size: 3, P: 1, Epoch: ep(6, 3)}); !resp.OK {
		t.Errorf("counter 6 refused: %+v", resp)
	}
	hi, lo := ep(7, 9), ep(7, 1)
	if hi.Less(lo) {
		hi, lo = lo, hi
	}
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: lo}); !resp.OK {
		t.Errorf("%v refused: %+v", lo, resp)
	}
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: hi}); !resp.OK {
		t.Errorf("%v refused after %v: %+v", hi, lo, resp)
	}
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: lo}); resp.OK {
		t.Errorf("%v accepted after %v", lo, hi)
	}
	if got := m.parts[1].maxCounter.Load(); got != 7 {
		t.Errorf("maxCounter = %d, want 7 (raised from every request)", got)
	}
}

func TestPromiseSizeMismatchAndBadPartition(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m, st := fc.nodes[0].m, fc.nodes[0].store
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 5, P: 1, Epoch: ep(1, 2)}); !resp.SizeMismatch || resp.OK {
		t.Errorf("size mismatch: %+v", resp)
	}
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 99, Epoch: ep(1, 2)}); resp.OK {
		t.Errorf("a partition out of range was promised: %+v", resp)
	}
	if !st.state(1).Promised.IsZero() {
		t.Error("a refused promise was stored")
	}
}

func TestPromiseStickiness(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m, st := fc.nodes[0].m, fc.nodes[0].store
	advance := setClock(m)

	// Node 3 is the owner and was heard just now: a promise to node 2 is refused whatever its epoch.
	m.setOwner(1, nid(3), ep(2, 3))
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(9, 2)}); resp.OK {
		t.Fatalf("promised to a challenger of a live owner: %+v", resp)
	}
	if !st.state(1).Promised.IsZero() {
		t.Fatal("a refused promise was stored")
	}
	// ... but the owner itself may re-propose.
	if resp := m.handlePromise(nid(3), &promiseReq{Size: 3, P: 1, Epoch: ep(3, 3)}); !resp.OK {
		t.Fatalf("refused the owner's own proposal: %+v", resp)
	}
	// Another partition is not affected.
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 2, Epoch: ep(1, 2)}); !resp.OK {
		t.Fatalf("stickiness leaked to another partition: %+v", resp)
	}

	// Stickiness lapses when the owner has been silent for OwnerTimeout...
	advance(m.cfg.OwnerTimeout - time.Millisecond)
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(9, 2)}); resp.OK {
		t.Fatal("promised a moment before the owner timed out")
	}
	advance(time.Millisecond)
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(9, 2)}); !resp.OK {
		t.Fatalf("refused after the owner timed out: %+v", resp)
	}

	// ... or at once when it relinquishes.
	m.setOwner(3, nid(3), ep(2, 3))
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 3, Epoch: ep(9, 2)}); resp.OK {
		t.Fatal("promised to a challenger of a live owner")
	}
	m.handleRelinquish(nid(2), &relinquishMsg{Size: 3, P: 3, Epoch: ep(2, 3)}) // not the owner: ignored
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 3, Epoch: ep(9, 2)}); resp.OK {
		t.Fatal("a relinquish from a node that is not the owner lapsed the stickiness")
	}
	m.handleRelinquish(nid(3), &relinquishMsg{Size: 3, P: 3, Epoch: ep(2, 3)})
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 3, Epoch: ep(9, 2)}); !resp.OK {
		t.Fatalf("refused after the owner relinquished: %+v", resp)
	}
}

func TestPromiseInResyncMode(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m, st := fc.nodes[0].m, fc.nodes[0].store
	st.mu.Lock()
	st.part(1).st.Promised = ep(2, 2)
	st.mu.Unlock()
	m.resync.Store(true)
	before := len(st.callLog())
	resp := m.handlePromise(nid(3), &promiseReq{Size: 3, P: 1, Epoch: ep(9, 3)})
	if resp.OK || resp.Promised != ep(2, 2) {
		t.Fatalf("a node that is not intact must not promise: %+v", resp)
	}
	if got := st.state(1).Promised; got != ep(2, 2) {
		t.Fatalf("resync node recorded a promise: %v", got)
	}
	if calls := st.callLog(); len(calls) != before {
		t.Fatalf("resync node wrote to its store: %v", calls)
	}
}

func TestPromiseDurableBeforeReply(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m, st := fc.nodes[0].m, fc.nodes[0].store
	entered, release := make(chan struct{}), make(chan struct{})
	st.onSetPromised = func(p int, e Epoch) {
		close(entered)
		<-release
	}
	done := make(chan *promiseResp, 1)
	go func() { done <- m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(3, 2)}) }()

	<-entered
	select {
	case resp := <-done:
		t.Fatalf("replied before the promise was durable: %+v", resp)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	resp := <-done
	if !resp.OK {
		t.Fatalf("refused: %+v", resp)
	}
	if got := st.state(1).Promised; got != ep(3, 2) {
		t.Fatalf("reply sent but promise not stored: %v", got)
	}
}

func TestPromiseStoreFailureRefuses(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m, st := fc.nodes[0].m, fc.nodes[0].store
	st.failOnce("SetPromised", errors.New("disk"))
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(3, 2)}); resp.OK {
		t.Fatal("promised though the promise could not be stored")
	}
	st.failOnce("State", errors.New("disk"))
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(3, 2)}); resp.OK {
		t.Fatal("promised though the state could not be read")
	}
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(3, 2)}); !resp.OK {
		t.Fatal("refused after the faults cleared")
	}
}

func TestPromiseDeposesALeader(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m, st := fc.nodes[0].m, fc.nodes[0].store
	if err := st.SetPromised(1, ep(4, 1)); err != nil {
		t.Fatal(err)
	}
	m.becomeLeader(1, ep(4, 1), m.now())
	if !m.IsOwner(1) {
		t.Fatal("setup")
	}
	// The leader is sticky to others while it has majority contact.
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(5, 2)}); resp.OK {
		t.Fatalf("a leader with majority contact promised to a challenger: %+v", resp)
	}
	if !m.IsOwner(1) {
		t.Fatal("stepped down on a refused promise")
	}
	// Once it has lost contact it yields, and steps down.
	m.parts[1].vmu.Lock()
	m.parts[1].ownerSeen = m.now().Add(-time.Hour)
	m.parts[1].vmu.Unlock()
	if resp := m.handlePromise(nid(2), &promiseReq{Size: 3, P: 1, Epoch: ep(5, 2)}); !resp.OK {
		t.Fatalf("refused: %+v", resp)
	}
	if m.IsOwner(1) {
		t.Fatal("still leading after promising a higher epoch")
	}
	if _, ok := m.Owner(1); ok {
		t.Fatal("still believes in itself as owner")
	}
}
