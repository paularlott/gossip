package partition

import (
	"errors"
	"testing"
)

// follower builds a 3-node cluster and returns node 0 (the follower under test), with the owner's ID 2 and the
// follower's partition 1 holding `committed` writes (epoch e1) and, if pending > 0, a pending write at seq pending.
type followerSetup struct {
	fc *fakeCluster
	f  *fakeNode
}

func newFollower(t *testing.T, committed int, e1 Epoch) followerSetup {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	f := fc.nodes[0]
	f.store.seedCommitted(1, committed, e1)
	return followerSetup{fc, f}
}

func (s followerSetup) pos(e Epoch, seq uint64) Pos { return Pos{Epoch: e, Seq: seq} }

func (s followerSetup) write(e Epoch, seq uint64, prev Pos, data string) Write {
	return Write{Partition: 1, Pos: Pos{Epoch: e, Seq: seq}, Prev: prev, Data: []byte(data)}
}

func TestSettleTable(t *testing.T) {
	e1, e2 := ep(1, 2), ep(2, 2)
	cases := []struct {
		name        string
		pending     *Write // seq 4 over committed 3
		oc          Pos
		wantPending string // "", or data of the pending write that remains
		wantSeq     uint64 // committed seq after
	}{
		{"no pending: nothing", nil, Pos{Epoch: e1, Seq: 9}, "", 3},
		{"same epoch, seq <= OC: commit", &Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("p")},
			Pos{Epoch: e1, Seq: 4}, "", 4},
		{"same epoch, seq < OC: commit", &Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("p")},
			Pos{Epoch: e1, Seq: 7}, "", 4},
		{"other epoch, seq <= OC: clear", &Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("p")},
			Pos{Epoch: e2, Seq: 4}, "", 3},
		{"other epoch, seq < OC: clear", &Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("p")},
			Pos{Epoch: e2, Seq: 8}, "", 3},
		{"seq > OC (same epoch): keep", &Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("p")},
			Pos{Epoch: e1, Seq: 3}, "p", 3},
		{"seq > OC (other epoch): keep", &Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("p")},
			Pos{Epoch: e2, Seq: 3}, "p", 3},
		{"zero OC: keep", &Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("p")}, Pos{}, "p", 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newFollower(t, 3, e1)
			if c.pending != nil {
				s.f.store.seedPending(1, *c.pending)
			}
			s.f.m.settle(1, c.oc)
			st := s.f.store.state(1)
			if st.Committed.Seq != c.wantSeq {
				t.Errorf("committed seq = %d, want %d", st.Committed.Seq, c.wantSeq)
			}
			got := ""
			if st.Pending != nil {
				got = string(st.Pending.Data)
			}
			if got != c.wantPending {
				t.Errorf("pending = %q, want %q", got, c.wantPending)
			}
			if err := s.f.store.checkInvariant(); err != nil {
				t.Error(err)
			}
			if c.wantSeq == 4 {
				if d := s.f.store.data(1); len(d) != 4 || d[3] != "p" {
					t.Errorf("the committed pending write was not applied: %v", d)
				}
			}
		})
	}
}

func TestSettleCommitsOnTheLocalLabelOfThePreviousSeq(t *testing.T) {
	e1 := ep(1, 2)
	// A pending write whose Prev carries another epoch's label for the same Seq (see handleWrite) commits on this
	// node's own committed position, which is what Store.Commit checks.
	s := newFollower(t, 3, e1)
	s.f.store.seedPending(1, s.write(e1, 4, s.pos(ep(7, 3), 3), "relabelled"))
	s.f.m.settle(1, Pos{Epoch: e1, Seq: 4})
	if st := s.f.store.state(1); st.Committed != (Pos{Epoch: e1, Seq: 4}) || st.Pending != nil {
		t.Fatalf("%+v", st)
	}
	if d := s.f.store.data(1); d[3] != "relabelled" {
		t.Errorf("data %v", d)
	}
	// A pending write that does not follow the committed Seq is never committed (the invariant is broken; catch-up
	// is what fixes it).
	s = newFollower(t, 3, e1)
	s.f.store.seedPending(1, s.write(e1, 6, s.pos(e1, 5), "gap"))
	s.f.m.settle(1, Pos{Epoch: e1, Seq: 6})
	if st := s.f.store.state(1); st.Committed.Seq != 3 {
		t.Fatalf("%+v", st)
	}
}

func TestWriteHandlerRefusalsAndGrants(t *testing.T) {
	e1, e2, e3 := ep(1, 2), ep(2, 2), ep(3, 3)
	prev := Pos{Epoch: e1, Seq: 3}

	t.Run("size mismatch", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 4, Write: s.write(e2, 4, prev, "x")})
		if resp.OK || !resp.SizeMismatch {
			t.Errorf("%+v", resp)
		}
	})
	t.Run("resync mode", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		s.f.m.resync.Store(true)
		before := len(s.f.store.callLog())
		if resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 4, prev, "x")}); resp.OK {
			t.Errorf("%+v", resp)
		}
		if calls := s.f.store.callLog(); len(calls) != before {
			t.Errorf("resync node stored a write")
		}
	})
	t.Run("epoch below the promise", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		s.f.store.SetPromised(1, e3)
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 4, prev, "x")})
		if resp.OK || resp.Promised != e3 {
			t.Errorf("want refusal carrying the promise: %+v", resp)
		}
		if st := s.f.store.state(1); st.Pending != nil || st.Promised != e3 {
			t.Errorf("state changed: %+v", st)
		}
		if got := s.f.m.parts[1].maxCounter.Load(); got != 2 {
			t.Errorf("maxCounter = %d, want 2 (the epoch the write carried)", got)
		}
	})
	t.Run("epoch above the promise raises it", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		s.f.store.SetPromised(1, e1)
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 4, prev, "x"), OwnerCommitted: prev})
		if !resp.OK || resp.Committed != prev {
			t.Fatalf("%+v", resp)
		}
		st := s.f.store.state(1)
		if st.Promised != e2 || st.Pending == nil || string(st.Pending.Data) != "x" {
			t.Errorf("state: %+v", st)
		}
	})
	t.Run("epoch equal to the promise", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		s.f.store.SetPromised(1, e2)
		if resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 4, prev, "x"), OwnerCommitted: prev}); !resp.OK {
			t.Errorf("%+v", resp)
		}
	})
	t.Run("a higher epoch deposes a leader", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		s.f.store.SetPromised(1, ep(1, 1))
		s.f.m.becomeLeader(1, ep(1, 1), s.f.m.now())
		if resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 4, prev, "x"), OwnerCommitted: prev}); !resp.OK {
			t.Fatalf("%+v", resp)
		}
		if s.f.m.IsOwner(1) {
			t.Error("still leading after a write of a higher epoch")
		}
	})
	t.Run("malformed write", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		if resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 9, prev, "x"), OwnerCommitted: prev}); resp.OK {
			t.Errorf("a write that does not follow its predecessor was stored: %+v", resp)
		}
		if resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: Write{Partition: 99}}); resp.OK {
			t.Errorf("a partition out of range was stored: %+v", resp)
		}
	})
	t.Run("store failures refuse", func(t *testing.T) {
		for _, method := range []string{"State", "SetPromised", "SetPending"} {
			s := newFollower(t, 3, e1)
			s.f.store.failOnce(method, errors.New("disk"))
			if resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 4, prev, "x"), OwnerCommitted: prev}); resp.OK {
				t.Errorf("%s failed but the write was acknowledged", method)
			}
		}
	})
}

func TestWriteHandlerPendingAndCommittedTable(t *testing.T) {
	e1, e2 := ep(1, 2), ep(2, 3)
	c3 := Pos{Epoch: e1, Seq: 3}
	c4 := Pos{Epoch: e1, Seq: 4}

	t.Run("no pending, committed == prev: stored", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e1, 4, c3, "w4"), OwnerCommitted: c3})
		if !resp.OK || resp.NeedCatchUp {
			t.Fatalf("%+v", resp)
		}
		if st := s.f.store.state(1); st.Pending == nil || string(st.Pending.Data) != "w4" || st.Committed != c3 {
			t.Errorf("%+v", st)
		}
	})
	t.Run("pending of the owner's epoch within OC is committed first", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		s.f.store.seedPending(1, s.write(e1, 4, c3, "w4"))
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e1, 5, c4, "w5"), OwnerCommitted: c4})
		if !resp.OK || resp.Committed != c4 {
			t.Fatalf("%+v", resp)
		}
		st := s.f.store.state(1)
		if st.Committed != c4 || st.Pending == nil || string(st.Pending.Data) != "w5" {
			t.Errorf("%+v", st)
		}
		if d := s.f.store.data(1); len(d) != 4 || d[3] != "w4" {
			t.Errorf("data %v", d)
		}
	})
	t.Run("pending of another epoch within OC is cleared, then catch-up is needed", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		// The follower holds a never-committed write (e1,4); the committed write at 4 is the new owner's.
		s.f.store.seedPending(1, s.write(e1, 4, c3, "stale"))
		// Node 1's store (the source of the catch-up) holds the real history.
		src := s.fc.nodes[1].store
		src.seedCommitted(1, 3, e1)
		real4 := Pos{Epoch: e2, Seq: 4}
		if err := src.Commit(&Write{Partition: 1, Pos: real4, Prev: c3, Data: []byte("real4")}); err != nil {
			t.Fatal(err)
		}
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 5, real4, "w5"), OwnerCommitted: real4})
		if resp.OK || !resp.NeedCatchUp {
			t.Fatalf("want NeedCatchUp: %+v", resp)
		}
		if st := s.f.store.state(1); st.Pending != nil {
			t.Fatalf("the undecided stale pending write was kept: %+v", st.Pending)
		}
		s.f.m.wg.Wait() // the asynchronous catch-up
		st := s.f.store.state(1)
		if st.Committed != real4 {
			t.Fatalf("catch-up did not bring the committed write: %+v", st)
		}
		if d := s.f.store.data(1); len(d) != 4 || d[3] != "real4" {
			t.Errorf("data %v", d)
		}
		// The owner's retry now succeeds.
		if resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 5, real4, "w5"), OwnerCommitted: real4}); !resp.OK {
			t.Errorf("retry refused: %+v", resp)
		}
	})
	t.Run("pending beyond OC is kept by settle but replaced by the write at the same seq", func(t *testing.T) {
		s := newFollower(t, 3, e1)
		s.f.store.seedPending(1, s.write(e1, 4, c3, "old"))
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 4, c3, "new"), OwnerCommitted: c3})
		if !resp.OK {
			t.Fatalf("%+v", resp)
		}
		if st := s.f.store.state(1); st.Pending == nil || string(st.Pending.Data) != "new" || st.Pending.Pos.Epoch != e2 {
			t.Errorf("the write at the same seq must replace the pending one: %+v", st.Pending)
		}
		if err := s.f.store.checkInvariant(); err != nil {
			t.Error(err)
		}
	})
	t.Run("committed behind prev: NeedCatchUp, nothing stored", func(t *testing.T) {
		s := newFollower(t, 2, e1)
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e1, 5, c4, "w5"), OwnerCommitted: c4})
		if resp.OK || !resp.NeedCatchUp {
			t.Fatalf("%+v", resp)
		}
		s.f.m.wg.Wait() // the catch-up finds nothing to fetch from the empty source
		if st := s.f.store.state(1); st.Pending != nil {
			t.Errorf("stored a write it cannot apply: %+v", st.Pending)
		}
	})
	t.Run("already committed at the write's Seq: acknowledged, not stored again", func(t *testing.T) {
		// This node committed write 5 as the old owner; the new owner re-proposes it as (e2,5) on top of (e1,4).
		s := newFollower(t, 5, e1)
		before := len(s.f.store.callLog())
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 5, c4, "d5"), OwnerCommitted: c4})
		if !resp.OK || resp.NeedCatchUp {
			t.Fatalf("%+v", resp)
		}
		calls := s.f.store.callLog()[before:]
		if len(calls) != 1 || calls[0] != "SetPromised(1)" {
			t.Errorf("only the promise should be written: %v", calls)
		}
		if st := s.f.store.state(1); st.Pending != nil || st.Committed.Seq != 5 {
			t.Errorf("%+v", st)
		}
	})
	t.Run("committed beyond the write: refused", func(t *testing.T) {
		s := newFollower(t, 6, e1)
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 5, c4, "w5"), OwnerCommitted: c4})
		if resp.OK || resp.NeedCatchUp {
			t.Fatalf("%+v", resp)
		}
	})
	t.Run("same Seq as prev under another label: the write follows, on this node's label", func(t *testing.T) {
		// The new owner re-proposed write 4 as (e2,4); this node committed it as (e1,4). Write 5 names (e2,4).
		s := newFollower(t, 4, e1)
		other := Pos{Epoch: e2, Seq: 4}
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e2, 5, other, "w5"), OwnerCommitted: other})
		if !resp.OK || resp.NeedCatchUp {
			t.Fatalf("%+v", resp)
		}
		if st := s.f.store.state(1); st.Pending == nil || st.Pending.Prev != c4 {
			t.Fatalf("pending %+v, want Prev = this node's own (e1,4)", st.Pending)
		}
		// It commits (the Store checks Prev against its own committed position).
		s.f.m.settle(1, Pos{Epoch: e2, Seq: 5})
		if st := s.f.store.state(1); st.Committed.Seq != 5 {
			t.Errorf("%+v", st)
		}
	})
	t.Run("first write of a partition", func(t *testing.T) {
		s := newFollower(t, 0, e1)
		resp := s.f.m.handleWrite(nid(2), &writeReq{Size: 3, Write: s.write(e1, 1, Pos{}, "w1")})
		if !resp.OK {
			t.Fatalf("%+v", resp)
		}
	})
}
