package partition

// In-process tests of startup synchronization and majority contact.

import (
	"testing"
	"time"
)

// A node that lost its data adopts the highest-epoch pending write at the position after the committed one, and nothing
// else; a node that did not lose its data keeps what it has.
func TestResyncAdoptsPendingWrite(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	a, b, v := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	for _, n := range []*fakeNode{a, b} {
		n.store.seedCommitted(1, 2, ep(1, 1))
		n.store.seedCommitted(2, 3, ep(1, 1))
		if err := n.store.SetPromised(1, ep(5, 2)); err != nil {
			t.Fatal(err)
		}
	}
	// Partition 1: a holds a pending write at 3 (committed on the lost disk, as far as anyone knows); b holds an older
	// pending at 3. Partition 2: a holds a pending write at 2, which is already decided.
	a.store.seedPending(1, Write{Pos: Pos{Epoch: ep(5, 2), Seq: 3}, Prev: Pos{Epoch: ep(1, 1), Seq: 2}, Data: []byte("newer")})
	b.store.seedPending(1, Write{Pos: Pos{Epoch: ep(3, 2), Seq: 3}, Prev: Pos{Epoch: ep(1, 1), Seq: 2}, Data: []byte("older")})
	a.store.seedPending(2, Write{Pos: Pos{Epoch: ep(2, 2), Seq: 2}, Data: []byte("decided")})

	v.store.intact = false
	v.m.resync.Store(true)
	if !v.m.syncOnce() {
		t.Fatal("resync did not complete")
	}
	st := v.store.state(1)
	if st.Committed.Seq != 2 || st.Pending == nil || string(st.Pending.Data) != "newer" || st.Pending.Pos.Seq != 3 ||
		st.Pending.Pos.Epoch != ep(5, 2) || st.Pending.Prev != st.Committed || st.Pending.Partition != 1 {
		t.Errorf("partition 1 after resync: committed %s pending %+v, want the pending write of the highest epoch", st.Committed, st.Pending)
	}
	if st := v.store.state(2); st.Committed.Seq != 3 || st.Pending != nil {
		t.Errorf("partition 2 after resync: committed %s pending %+v, want no pending (already decided)", st.Committed, st.Pending)
	}
	if err := v.store.checkInvariant(); err != nil {
		t.Error(err)
	}
	// An intact node does not take anyone's pending write: it keeps its own.
	c := newFakeCluster(t, 3, 3, nil)
	c.attach()
	for _, n := range c.nodes[:2] {
		n.store.seedCommitted(1, 2, ep(1, 1))
	}
	c.nodes[0].store.seedPending(1, Write{Pos: Pos{Epoch: ep(5, 2), Seq: 3}, Data: []byte("theirs")})
	if !c.nodes[2].m.syncOnce() {
		t.Fatal("sync of an intact node did not complete")
	}
	if st := c.nodes[2].store.state(1); st.Pending != nil {
		t.Errorf("an intact node adopted a pending write: %+v", st.Pending)
	}
}

// An owner that has had no majority contact for OwnerTimeout steps down, so that followers it can still reach stop refreshing
// their view of it (bounded staleness).
func TestOwnerWithoutMajorityStepsDown(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	owner := fc.owners(1)[0]
	// Every link of the owner is cut but the membership view is kept (nobody detects the failure): it keeps believing it is
	// the owner until it notices that nobody answers.
	fc.isolate(owner, true)
	waitFor(t, 5*time.Second, "the owner without majority contact to step down", func() bool { return !owner.m.IsOwner(1) })
}
