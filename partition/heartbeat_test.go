package partition

import (
	"github.com/paularlott/gossip"
	"testing"
	"time"
)

func TestHeartbeatSetsOwnerAndFiresEvent(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	m := fc.nodes[0].m
	var got []ownershipEvent
	done := make(chan struct{}, 8)
	m.HandleOwnershipFunc(func(p int, owner gossip.NodeID, epoch Epoch) {
		got = append(got, ownershipEvent{p, owner, epoch})
		done <- struct{}{}
	})
	m.wg.Add(1)
	go func() { defer m.wg.Done(); m.eventLoop() }()

	hb := &heartbeatMsg{Size: 3, From: nid(2), Entries: []heartbeatEntry{{P: 1, Epoch: ep(3, 2), Committed: Pos{}}}}
	if resp := m.handleHeartbeat(nid(2), hb); !resp.OK {
		t.Fatalf("%+v", resp)
	}
	owner, ok := m.Owner(1)
	if !ok || owner != nid(2) {
		t.Fatalf("Owner = %v, %v", owner, ok)
	}
	if !m.CanRead(1) {
		t.Error("CanRead false right after a heartbeat")
	}
	<-done
	// The same heartbeat again is not a change.
	m.handleHeartbeat(nid(2), hb)
	// A new epoch from the same node is.
	hb.Entries[0].Epoch = ep(4, 2)
	m.handleHeartbeat(nid(2), hb)
	<-done
	select {
	case <-done:
		t.Error("an unchanged owner fired an event")
	case <-time.After(30 * time.Millisecond):
	}
	m.cancel()
	m.wg.Wait()
	if len(got) != 2 || got[0] != (ownershipEvent{1, nid(2), ep(3, 2)}) || got[1].epoch != ep(4, 2) {
		t.Errorf("events: %+v", got)
	}
	if m.parts[1].maxCounter.Load() != 4 {
		t.Errorf("maxCounter = %d", m.parts[1].maxCounter.Load())
	}
}

func TestHeartbeatIgnoredCases(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	m, st := fc.nodes[0].m, fc.nodes[0].store
	st.SetPromised(1, ep(5, 3))

	send := func(from int, e Epoch) *heartbeatResp {
		return m.handleHeartbeat(nid(from), &heartbeatMsg{Size: 3, From: nid(from), Entries: []heartbeatEntry{{P: 1, Epoch: e}}})
	}
	// A deposed owner: its epoch is below the promise.
	send(2, ep(4, 2))
	if _, ok := m.Owner(1); ok {
		t.Error("a deposed owner's heartbeat was believed")
	}
	// An entry whose epoch names another node than the sender.
	send(2, ep(6, 3))
	if _, ok := m.Owner(1); ok {
		t.Error("heartbeat from a node that is not the epoch's proposer was believed")
	}
	// Size mismatch.
	if resp := m.handleHeartbeat(nid(2), &heartbeatMsg{Size: 5, Entries: []heartbeatEntry{{P: 1, Epoch: ep(6, 2)}}}); !resp.SizeMismatch || resp.OK {
		t.Errorf("%+v", resp)
	}
	if _, ok := m.Owner(1); ok {
		t.Error("heartbeat of another cluster size was believed")
	}
	// A heartbeat for a partition out of range does no harm.
	m.handleHeartbeat(nid(2), &heartbeatMsg{Size: 3, Entries: []heartbeatEntry{{P: 99, Epoch: ep(6, 2)}}})

	// Valid, then a stale lower epoch from another node does not take the owner away.
	send(2, ep(7, 2))
	send(3, ep(6, 3))
	if o, _ := m.Owner(1); o != nid(2) {
		t.Errorf("owner = %v, want node 2 (higher epoch)", o)
	}
	// A higher one from another node does.
	send(3, ep(8, 3))
	if o, _ := m.Owner(1); o != nid(3) {
		t.Errorf("owner = %v, want node 3", o)
	}

	// A node that is not intact does not process heartbeats.
	m.resync.Store(true)
	send(2, ep(9, 2))
	if o, _ := m.Owner(1); o != nid(3) {
		t.Errorf("resync node changed its owner to %v", o)
	}
}

func TestHeartbeatSettlesPendingAndCatchesUp(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	f, owner := fc.nodes[0], fc.nodes[1]
	e1 := ep(1, 2)
	owner.store.seedCommitted(1, 6, e1)

	// The follower holds writes 1-3 committed and 4 pending (the owner's), and is behind on 5 and 6.
	f.store.seedCommitted(1, 3, e1)
	f.store.seedPending(1, Write{Pos: Pos{Epoch: e1, Seq: 4}, Prev: Pos{Epoch: e1, Seq: 3}, Data: []byte("d4")})
	hb := &heartbeatMsg{Size: 3, From: nid(2), Entries: []heartbeatEntry{{P: 1, Epoch: e1, Committed: Pos{Epoch: e1, Seq: 6}}}}
	f.m.handleHeartbeat(nid(2), hb)
	f.m.wg.Wait()
	st := f.store.state(1)
	if st.Committed != (Pos{Epoch: e1, Seq: 6}) || st.Pending != nil {
		t.Fatalf("state: %+v", st)
	}
	if d := f.store.data(1); len(d) != 6 {
		t.Errorf("data = %v", d)
	}
	if err := f.store.checkInvariant(); err != nil {
		t.Error(err)
	}

	// A follower that holds the pending write the heartbeat covers needs no catch-up: settle commits it.
	g := fc.nodes[2]
	g.store.seedCommitted(1, 5, e1)
	g.store.seedPending(1, Write{Pos: Pos{Epoch: e1, Seq: 6}, Prev: Pos{Epoch: e1, Seq: 5}, Data: []byte("d6")})
	exportsBefore := owner.store.exportCount()
	g.m.handleHeartbeat(nid(2), hb)
	g.m.wg.Wait()
	if st := g.store.state(1); st.Committed.Seq != 6 || st.Pending != nil {
		t.Fatalf("state: %+v", st)
	}
	if d := g.store.data(1); d[5] != "d6" {
		t.Errorf("data = %v", d)
	}
	if n := owner.store.exportCount(); n != exportsBefore {
		t.Errorf("the owner's store was exported %d more time(s): a heartbeat that only covers the pending write needs no catch-up", n-exportsBefore)
	}
}

func TestHeartbeatFromHigherEpochDeposesALeader(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	m := fc.nodes[0].m
	m.becomeLeader(1, ep(2, 1), m.now())
	// A lower epoch from another node changes nothing.
	m.handleHeartbeat(nid(2), &heartbeatMsg{Size: 3, Entries: []heartbeatEntry{{P: 1, Epoch: ep(1, 2)}}})
	if !m.IsOwner(1) {
		t.Fatal("deposed by a lower epoch")
	}
	m.handleHeartbeat(nid(2), &heartbeatMsg{Size: 3, Entries: []heartbeatEntry{{P: 1, Epoch: ep(3, 2)}}})
	if m.IsOwner(1) {
		t.Fatal("still leading under a higher epoch")
	}
	if o, ok := m.Owner(1); !ok || o != nid(2) {
		t.Fatalf("owner = %v, %v", o, ok)
	}
}

func TestCanReadBoundedStaleness(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m := fc.nodes[0].m
	advance := setClock(m)

	if m.CanRead(1) {
		t.Error("CanRead with no owner")
	}
	if m.CanRead(99) || m.CanRead(-1) {
		t.Error("CanRead out of range")
	}
	// A follower reads while it hears the owner.
	m.setOwner(1, nid(2), ep(1, 2))
	if !m.CanRead(1) {
		t.Error("follower cannot read just after a heartbeat")
	}
	advance(m.cfg.StaleAfter - time.Millisecond)
	if !m.CanRead(1) {
		t.Error("follower cannot read inside the bound")
	}
	advance(time.Millisecond)
	if m.CanRead(1) {
		t.Error("follower reads from a copy older than the bound")
	}
	// A new owner does not read before its takeover has settled (it counts majority contact only once the write it
	// settles is committed, or it found none to settle), and then reads while it has majority contact.
	m.becomeLeader(2, ep(1, 1), m.now())
	if m.CanRead(2) {
		t.Error("leader reads before its takeover settled")
	}
	m.touchMajority(2, ep(1, 1), m.now()) // a heartbeat round while the takeover settles counts for nothing
	if m.CanRead(2) {
		t.Error("leader reads before its takeover settled, after a heartbeat round")
	}
	m.takeoverSettled(2, ep(1, 1), m.now())
	if !m.CanRead(2) {
		t.Error("leader cannot read just after takeover")
	}
	advance(m.cfg.StaleAfter)
	if m.CanRead(2) {
		t.Error("leader cut off from a majority still reads")
	}
	m.touchMajority(2, ep(1, 1), m.now())
	if !m.CanRead(2) {
		t.Error("leader cannot read after fresh majority contact")
	}
	// A node that is not intact never serves.
	m.resync.Store(true)
	if m.CanRead(2) {
		t.Error("a node in resync mode reads")
	}
}

func TestRelinquishLapsesOwnerSeen(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	m := fc.nodes[0].m
	m.setOwner(1, nid(2), ep(3, 2))
	if _, ok := m.Owner(1); !ok {
		t.Fatal("setup")
	}
	m.handleRelinquish(nid(2), &relinquishMsg{Size: 5, P: 1, Epoch: ep(3, 2)}) // wrong size: ignored
	if _, ok := m.Owner(1); !ok {
		t.Fatal("a relinquish of another cluster size was honoured")
	}
	m.handleRelinquish(nid(2), &relinquishMsg{Size: 3, P: 1, Epoch: ep(2, 2)}) // older epoch: ignored
	if _, ok := m.Owner(1); !ok {
		t.Fatal("a relinquish of an older epoch was honoured")
	}
	m.handleRelinquish(nid(2), &relinquishMsg{Size: 3, P: 1, Epoch: ep(3, 2)})
	if _, ok := m.Owner(1); ok {
		t.Fatal("still believes in a relinquished owner")
	}
	// A heartbeat of that tenure that was still in flight must not bring the owner back; a later tenure does.
	hb := &heartbeatMsg{Size: 3, From: nid(2), Entries: []heartbeatEntry{{P: 1, Epoch: ep(3, 2)}}}
	m.handleHeartbeat(nid(2), hb)
	if _, ok := m.Owner(1); ok {
		t.Fatal("a late heartbeat revived a relinquished owner")
	}
	hb.Entries[0].Epoch = ep(4, 2)
	m.handleHeartbeat(nid(2), hb)
	if o, ok := m.Owner(1); !ok || o != nid(2) {
		t.Fatal("a new tenure of the same node was not believed")
	}
	m.handleRelinquish(nid(3), &relinquishMsg{Size: 3, P: 1, Epoch: ep(9, 2)}) // not from the epoch's node: ignored
	if _, ok := m.Owner(1); !ok {
		t.Fatal("a forged relinquish was honoured")
	}
	m.handleRelinquish(nid(2), &relinquishMsg{Size: 3, P: 1, Epoch: ep(4, 2)})
	if _, ownerless := m.trackOwnerless(1); !ownerless {
		t.Fatal("a relinquished partition is not ownerless")
	}
}
