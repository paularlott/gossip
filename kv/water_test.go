package kv

import (
	"testing"
	"time"

	"github.com/paularlott/gossip"
)

func n(id byte) *gossip.Node {
	return gossip.NewTestNode(gossip.NodeID{id}, "127.0.0.1:1", nil)
}

func newTestWater(stability, dwell time.Duration) *waterTracker {
	tt := time.Now()
	w := newWaterTracker(stability, dwell, true, nil, func() time.Time { return tt })
	w.nowFn = func() time.Time { return tt }
	return w
}

func (w *waterTracker) advance(d time.Duration) {
	base := w.nowFn()
	w.nowFn = func() time.Time { return base.Add(d) }
}

func TestWaterSeedsImmediately(t *testing.T) {
	w := newTestWater(time.Minute, time.Minute)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, time.Now())
	if w.size() != 3 {
		t.Fatalf("seed = %d, want 3", w.size())
	}
}

func TestWaterGrowthNeedsStability(t *testing.T) {
	w := newTestWater(time.Minute, time.Minute)
	w.observe([]*gossip.Node{n(1)}, time.Now())

	// A fourth node appears but the count has not settled: no adoption.
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, time.Now())
	w.advance(time.Second)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, w.nowFn())
	if w.size() != 1 {
		t.Fatalf("mark rose before the stability period: %d", w.size())
	}

	// Steady for the full period: adopted.
	w.advance(time.Minute)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, w.nowFn())
	if w.size() != 3 {
		t.Fatalf("steady growth not adopted: %d", w.size())
	}
}

func TestWaterShrinkOnlyOneAtATimeAfterDwell(t *testing.T) {
	w := newTestWater(time.Second, time.Minute)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, time.Now())

	// One member missing, but not for the dwell yet: mark holds.
	w.observe([]*gossip.Node{n(1), n(2)}, time.Now())
	w.advance(30 * time.Second)
	w.observe([]*gossip.Node{n(1), n(2)}, w.nowFn())
	if w.size() != 3 {
		t.Fatalf("mark fell before the dwell elapsed: %d", w.size())
	}

	// Dwell elapsed: one step down.
	w.advance(time.Minute)
	w.observe([]*gossip.Node{n(1), n(2)}, w.nowFn())
	if w.size() != 2 {
		t.Fatalf("one-at-a-time shrink did not happen: %d", w.size())
	}
}

func TestWaterNeverShrinksOnLargerLoss(t *testing.T) {
	// Two members missing from a three-node group is indistinguishable from
	// a partition of that size: the mark must hold, whatever the dwell.
	w := newTestWater(time.Second, time.Minute)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, time.Now())

	w.observe([]*gossip.Node{n(1)}, time.Now())
	w.advance(10 * time.Minute)
	w.observe([]*gossip.Node{n(1)}, w.nowFn())
	if w.size() != 3 {
		t.Fatalf("mark followed a mass loss down: %d", w.size())
	}
}

func TestWaterAutoShrinkDisabled(t *testing.T) {
	w := newTestWater(time.Second, time.Minute)
	w.autoShrink = false
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, time.Now())

	w.observe([]*gossip.Node{n(1), n(2)}, time.Now())
	w.advance(10 * time.Minute)
	w.observe([]*gossip.Node{n(1), n(2)}, w.nowFn())
	if w.size() != 3 {
		t.Fatalf("mark shrank with auto-shrink disabled: %d", w.size())
	}
}

func TestWaterGracefulDepartureImmediateAndOnce(t *testing.T) {
	w := newTestWater(time.Second, time.Minute)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, time.Now())

	if !w.noteGracefulDeparture(gossip.NodeID{3}) {
		t.Fatal("first graceful departure not counted")
	}
	if w.size() != 2 {
		t.Fatalf("mark = %d after graceful departure, want 2", w.size())
	}

	// The same departure can never count twice.
	if w.noteGracefulDeparture(gossip.NodeID{3}) {
		t.Fatal("departure counted twice")
	}
	if w.size() != 2 {
		t.Fatalf("mark = %d after double departure", w.size())
	}

	// A node outside the store's scope is irrelevant.
	if w.noteGracefulDeparture(gossip.NodeID{9}) {
		t.Fatal("departure of a non-member counted")
	}
}

func TestWaterDepartedUnMarksOnReturn(t *testing.T) {
	w := newTestWater(time.Second, time.Minute)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, time.Now())

	w.noteGracefulDeparture(gossip.NodeID{3})
	if w.size() != 2 {
		t.Fatalf("mark = %d after departure, want 2", w.size())
	}

	// The node returns: it is un-marked and can depart again later without
	// having permanently spent its decrement.
	w.advance(time.Minute)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, w.nowFn())
	w.advance(time.Minute)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, w.nowFn())
	if w.size() != 3 {
		t.Fatalf("returning node not re-adopted: %d", w.size())
	}

	if !w.noteGracefulDeparture(gossip.NodeID{3}) {
		t.Fatal("second departure after a return not counted")
	}
	if w.size() != 2 {
		t.Fatalf("mark = %d after second departure, want 2", w.size())
	}
}

func TestWaterForget(t *testing.T) {
	w := newTestWater(time.Second, time.Minute)
	w.observe([]*gossip.Node{n(1), n(2), n(3)}, time.Now())

	if !w.forget(gossip.NodeID{2}) {
		t.Fatal("forget not counted")
	}
	if w.size() != 2 {
		t.Fatalf("mark = %d after forget, want 2", w.size())
	}
	if w.forget(gossip.NodeID{2}) {
		t.Fatal("forget counted twice")
	}
	if w.forget(gossip.NodeID{8}) {
		t.Fatal("forget of an unknown node counted")
	}
}

func TestWaterFloorIsOne(t *testing.T) {
	w := newTestWater(time.Second, time.Minute)
	w.observe([]*gossip.Node{n(1), n(2)}, time.Now())

	w.noteGracefulDeparture(gossip.NodeID{2})
	if w.size() != 1 {
		t.Fatalf("mark = %d, want 1", w.size())
	}
	// Departing the last member cannot push the mark below one.
	w.noteGracefulDeparture(gossip.NodeID{1})
	if w.size() != 1 {
		t.Fatalf("mark fell below the floor: %d", w.size())
	}
}
