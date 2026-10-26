package partition

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/paularlott/gossip"
)

func TestEpochOrdering(t *testing.T) {
	a, b := nid(1), nid(2)
	cases := []struct {
		x, y Epoch
		want int
	}{
		{Epoch{}, Epoch{}, 0},
		{Epoch{}, Epoch{Counter: 1}, -1},
		{Epoch{Counter: 1, Node: a}, Epoch{Counter: 1, Node: a}, 0},
		{Epoch{Counter: 1, Node: b}, Epoch{Counter: 2, Node: a}, -1}, // counter first
		{Epoch{Counter: 3, Node: a}, Epoch{Counter: 2, Node: b}, 1},
		{Epoch{Counter: 2, Node: a}, Epoch{Counter: 2, Node: b}, cmpID(a, b)}, // then node, byte-wise
		{Epoch{Counter: 0, Node: a}, Epoch{}, 1},                              // the zero Epoch is lowest
	}
	for _, c := range cases {
		if got := c.x.Compare(c.y); got != c.want {
			t.Errorf("%v.Compare(%v) = %d, want %d", c.x, c.y, got, c.want)
		}
		if got := c.y.Compare(c.x); got != -c.want {
			t.Errorf("%v.Compare(%v) = %d, want %d", c.y, c.x, got, -c.want)
		}
	}
	if !(Epoch{}).IsZero() || (Epoch{Counter: 1}).IsZero() {
		t.Error("IsZero")
	}
	if !(Epoch{Counter: 1}).Less(Epoch{Counter: 2}) || (Epoch{Counter: 2}).Less(Epoch{Counter: 2}) {
		t.Error("Less")
	}
}

func TestPosEquality(t *testing.T) {
	e := Epoch{Counter: 4, Node: nid(1)}
	if (Pos{Epoch: e, Seq: 3}) != (Pos{Epoch: e, Seq: 3}) {
		t.Error("equal positions must compare equal")
	}
	if (Pos{Epoch: e, Seq: 3}) == (Pos{Epoch: e, Seq: 4}) {
		t.Error("different sequences must differ")
	}
	if (Pos{Epoch: Epoch{Counter: 5, Node: nid(1)}, Seq: 3}) == (Pos{Epoch: e, Seq: 3}) {
		t.Error("different epochs must differ: same Seq, different write")
	}
	if !(Pos{}).IsZero() {
		t.Error("IsZero")
	}
}

func TestCountersRefuseAtMaxUint64(t *testing.T) {
	if v, err := increment(0); err != nil || v != 1 {
		t.Errorf("increment(0) = %d, %v", v, err)
	}
	if v, err := increment(math.MaxUint64 - 2); err != nil || v != math.MaxUint64-1 {
		t.Errorf("increment(Max-2) = %d, %v", v, err)
	}
	// The result must never be MaxUint64.
	for _, in := range []uint64{math.MaxUint64 - 1, math.MaxUint64} {
		if _, err := increment(in); !errors.Is(err, ErrExhausted) {
			t.Errorf("increment(%d) err = %v, want ErrExhausted", in, err)
		}
	}
}

func TestTakeoverRefusesExhaustedCounter(t *testing.T) {
	fc := newFakeCluster(t, 1, 1, nil)
	m := fc.nodes[0].m
	m.parts[1].raise(math.MaxUint64 - 1)
	if err := m.takeover(m.ctx, 1); !errors.Is(err, ErrExhausted) {
		t.Fatalf("takeover = %v, want ErrExhausted", err)
	}
	if m.IsOwner(1) {
		t.Error("took over with an exhausted counter")
	}
	if st := fc.nodes[0].store.state(1); !st.Promised.IsZero() {
		t.Errorf("promise recorded despite exhaustion: %v", st.Promised)
	}
}

func TestRunChangeRefusesExhaustedSeq(t *testing.T) {
	fc := newFakeCluster(t, 1, 1, nil)
	fn := fc.nodes[0]
	if err := fn.m.takeover(fn.m.ctx, 1); err != nil {
		t.Fatal(err)
	}
	epoch, _ := fn.m.leadingEpoch(1)
	fn.store.mu.Lock()
	fn.store.part(1).st.Committed = Pos{Epoch: epoch, Seq: math.MaxUint64 - 1}
	fn.store.mu.Unlock()
	if _, err := fn.m.Write(t.Context(), 1, []byte("x")); !errors.Is(err, ErrExhausted) {
		t.Fatalf("Write = %v, want ErrExhausted", err)
	}
	if st := fn.store.state(1); st.Pending != nil || st.Committed.Seq != math.MaxUint64-1 {
		t.Errorf("state changed: %+v", st)
	}
}

func TestPartitionOf(t *testing.T) {
	for _, parts := range []int{2, 3, 64, 1000} {
		seen := map[int]bool{}
		for i := 0; i < 5000; i++ {
			p := PartitionOf(fmt.Sprintf("key-%d", i), parts)
			if p < 1 || p >= parts {
				t.Fatalf("PartitionOf(%d partitions) = %d, want [1, %d)", parts, p, parts)
			}
			seen[p] = true
			if PartitionOf(fmt.Sprintf("key-%d", i), parts) != p {
				t.Fatal("not deterministic")
			}
		}
		if parts <= 64 && len(seen) != parts-1 {
			t.Errorf("%d partitions: only %d used", parts, len(seen))
		}
	}
	if PartitionOf("x", 1) != 0 || PartitionOf("x", 0) != 0 {
		t.Error("a single partition is partition 0")
	}
}

func TestRendezvousStability(t *testing.T) {
	var cands []gossip.NodeID
	for i := 1; i <= 5; i++ {
		cands = append(cands, nid(i))
	}
	const parts = 256
	owner := make([]gossip.NodeID, parts)
	count := map[gossip.NodeID]int{}
	for p := 0; p < parts; p++ {
		o, ok := rendezvousOwner(p, cands)
		if !ok {
			t.Fatal("no owner with candidates")
		}
		owner[p] = o
		count[o]++
	}
	for _, c := range cands {
		if count[c] == 0 {
			t.Errorf("node %s owns nothing of %d partitions", c, parts)
		}
	}
	// Removing one candidate moves only its partitions.
	removed := cands[2]
	rest := append(append([]gossip.NodeID{}, cands[:2]...), cands[3:]...)
	for p := 0; p < parts; p++ {
		o, _ := rendezvousOwner(p, rest)
		if owner[p] == removed {
			if o == removed {
				t.Fatalf("p%d still owned by the removed node", p)
			}
		} else if o != owner[p] {
			t.Errorf("p%d moved from %s to %s though its owner stayed", p, owner[p], o)
		}
	}
	// The order of the candidates does not matter, and none means no owner.
	rev := []gossip.NodeID{cands[4], cands[3], cands[2], cands[1], cands[0]}
	for p := 0; p < parts; p++ {
		if o, _ := rendezvousOwner(p, rev); o != owner[p] {
			t.Fatalf("p%d: owner depends on candidate order", p)
		}
	}
	if _, ok := rendezvousOwner(0, nil); ok {
		t.Error("an owner without candidates")
	}
}

func TestMajority(t *testing.T) {
	for size, want := range map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4, 7: 4} {
		if got := majority(size); got != want {
			t.Errorf("majority(%d) = %d, want %d", size, got, want)
		}
	}
}
