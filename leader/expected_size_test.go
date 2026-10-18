package leader

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/internal/baseline"
)

// Unit checks for the pinned baseline (Config.ExpectedClusterSize), driven
// directly against the tracker.

func TestBaselineFloorReportedBeforeObservation(t *testing.T) {
	b := baseline.New(stability, longDwell, true, nil)
	b.SetFloor(5)
	if got := b.Size(); got != 5 {
		t.Fatalf("a pinned baseline should report the floor before any observation, got %d", got)
	}
}

// A node restarting alone must not seed a baseline of one.
func TestBaselineFloorSeedsOnRestart(t *testing.T) {
	b := baseline.New(stability, longDwell, true, nil)
	b.SetFloor(3)
	b.Observe(mkNodes(1), time.Now())
	if got := b.Size(); got != 3 {
		t.Fatalf("expected the restarted node's baseline to be pinned at 3, got %d", got)
	}
}

func TestBaselineFloorBlocksGracefulShrink(t *testing.T) {
	b := baseline.New(stability, longDwell, true, nil)
	b.SetFloor(3)
	nodes := mkNodes(5)
	b.Observe(nodes, time.Now())

	// Leaves above the floor still lower the baseline...
	b.NoteGracefulDeparture(nodes[0].ID)
	b.NoteGracefulDeparture(nodes[1].ID)
	if got := b.Size(); got != 3 {
		t.Fatalf("expected baseline 3 after two leaves from 5, got %d", got)
	}
	// ...but never below it.
	b.NoteGracefulDeparture(nodes[2].ID)
	b.NoteGracefulDeparture(nodes[3].ID)
	if got := b.Size(); got != 3 {
		t.Fatalf("leaves dropped the pinned baseline to %d", got)
	}
}

func TestBaselineFloorBlocksForgetAndReset(t *testing.T) {
	b := baseline.New(stability, longDwell, true, nil)
	b.SetFloor(3)
	nodes := mkNodes(3)
	b.Observe(nodes, time.Now())

	b.Forget(nodes[0].ID)
	if got := b.Size(); got != 3 {
		t.Fatalf("Forget dropped the pinned baseline to %d", got)
	}

	b.Reset()
	b.Observe(mkNodes(1), time.Now())
	if got := b.Size(); got != 3 {
		t.Fatalf("Reset lost the floor: baseline %d", got)
	}
}

func TestBaselineFloorBlocksAutoShrink(t *testing.T) {
	const dwell = 50 * time.Millisecond
	b := baseline.New(stability, dwell, true, nil)
	b.SetFloor(3)
	nodes := mkNodes(3)
	now := time.Now()
	b.Observe(nodes, now)

	// One node missing, steady for well past the dwell.
	b.Observe(nodes[:2], now.Add(time.Millisecond))
	b.Observe(nodes[:2], now.Add(10*dwell))
	b.Observe(nodes[:2], now.Add(20*dwell))
	if got := b.Size(); got != 3 {
		t.Fatalf("auto-shrink dropped the pinned baseline to %d", got)
	}
}

func TestBaselineFloorAllowsGrowthAndShrinkBackToFloor(t *testing.T) {
	const dwell = 50 * time.Millisecond
	b := baseline.New(stability, dwell, true, nil)
	b.SetFloor(3)
	now := time.Now()
	nodes := mkNodes(4)
	b.Observe(nodes[:3], now)

	// Growth above the floor is still adopted after the stability period.
	b.Observe(nodes, now.Add(time.Millisecond))
	b.Observe(nodes, now.Add(2*stability))
	if got := b.Size(); got != 4 {
		t.Fatalf("expected growth to 4, got %d", got)
	}

	// And it can come back down to the floor.
	b.NoteGracefulDeparture(nodes[3].ID)
	if got := b.Size(); got != 3 {
		t.Fatalf("expected the baseline back at the floor (3), got %d", got)
	}
}

func TestBaselineFloorUnsetKeepsAdaptiveBehaviour(t *testing.T) {
	b := baseline.New(stability, longDwell, true, nil)
	b.SetFloor(0)
	b.Observe(mkNodes(1), time.Now())
	if got := b.Size(); got != 1 {
		t.Fatalf("an unset floor should leave the baseline adaptive, got %d", got)
	}
	if got := b.Floor(); got != 0 {
		t.Fatalf("Floor() = %d, want 0", got)
	}
}
