package hlc

import (
	"sync"
	"testing"
	"time"
)

// fixedWall returns a wall-clock source frozen at t, so tests can hold the
// physical clock still and force the logical counter to do all the work.
func fixedWall(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// After witnessing a timestamp well ahead of local time, the clock must keep
// minting strictly increasing, unique values for far more than the 1024 the
// counter field can hold — the counter has to carry into the time field
// rather than wrap back to zero.
func TestNowStrictlyIncreasingAfterFutureWitness(t *testing.T) {
	clock := NewClock()
	future := FromTime(time.Now().Add(5 * time.Second))
	clock.Witness(future)

	const n = 5000 // > 4 * 2^CounterBits
	prev := future
	seen := make(map[Timestamp]struct{}, n)
	for i := 0; i < n; i++ {
		ts := clock.Now()
		if ts <= prev {
			t.Fatalf("call %d: Now()=%d (counter %d) not strictly greater than previous %d (counter %d)",
				i, ts, ts.Counter(), prev, prev.Counter())
		}
		if _, dup := seen[ts]; dup {
			t.Fatalf("call %d: duplicate timestamp %d", i, ts)
		}
		seen[ts] = struct{}{}
		prev = ts
	}
}

// The same guarantee with the physical clock frozen, so the test does not
// depend on how fast the loop runs relative to the wall clock.
func TestNowCarriesIntoTimeFieldWithFrozenWall(t *testing.T) {
	wall := time.Now()
	clock := &Clock{wall: fixedWall(wall)}

	first := clock.Now()
	prev := first
	for i := 0; i < 3*(1<<CounterBits); i++ {
		ts := clock.Now()
		if ts <= prev {
			t.Fatalf("call %d: Now()=%d not strictly greater than %d", i, ts, prev)
		}
		prev = ts
	}
	if got, want := uint64(prev)>>CounterBits, uint64(first)>>CounterBits+3; got != want {
		t.Errorf("expected the counter to carry three times into the time field: time field %d, want %d", got, want)
	}
}

// Witness documents that the next Now() is strictly greater than the
// witnessed value. That must hold when the witnessed counter is already at
// its maximum.
func TestWitnessCounterMaxThenNowIsGreater(t *testing.T) {
	clock := NewClock()
	ahead := FromTime(time.Now().Add(time.Second)) | Timestamp(counterMask)
	if ahead.Counter() != uint16(counterMask) {
		t.Fatalf("setup: counter %d, want %d", ahead.Counter(), counterMask)
	}

	clock.Witness(ahead)
	if next := clock.Now(); next <= ahead {
		t.Fatalf("Now()=%d after Witness(%d with counter %d) is not strictly greater", next, ahead, ahead.Counter())
	}
}

// A wall clock that steps backwards must not let Now() go backwards.
func TestNowMonotonicAcrossWallClockStepBack(t *testing.T) {
	base := time.Now()
	wall := base
	clock := &Clock{wall: func() time.Time { return wall }}

	before := clock.Now()
	wall = base.Add(-10 * time.Second)
	for i := 0; i < 2000; i++ {
		if ts := clock.Now(); ts <= before {
			t.Fatalf("call %d after step back: Now()=%d <= %d", i, ts, before)
		} else {
			before = ts
		}
	}
}

// Concurrent Now() while the clock is ahead of wall time (counter mode): every
// value is unique, each goroutine sees a strictly increasing sequence, and all
// exceed the witnessed timestamp.
func TestNowConcurrentWhileBehind(t *testing.T) {
	clock := NewClock()
	future := FromTime(time.Now().Add(5*time.Second)) | Timestamp(counterMask-3)
	clock.Witness(future)

	const goroutines = 16
	const per = 1000
	results := make([][]Timestamp, goroutines)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			out := make([]Timestamp, per)
			for i := range out {
				out[i] = clock.Now()
			}
			results[g] = out
		}(g)
	}
	wg.Wait()

	seen := make(map[Timestamp]struct{}, goroutines*per)
	for g, seq := range results {
		for i, ts := range seq {
			if ts <= future {
				t.Fatalf("goroutine %d call %d: %d not greater than witnessed %d", g, i, ts, future)
			}
			if i > 0 && ts <= seq[i-1] {
				t.Fatalf("goroutine %d call %d: %d not greater than %d", g, i, ts, seq[i-1])
			}
			if _, dup := seen[ts]; dup {
				t.Fatalf("duplicate timestamp %d", ts)
			}
			seen[ts] = struct{}{}
		}
	}
}

func TestPreEpochWallClockClampsToZero(t *testing.T) {
	pre := Epoch().Add(-time.Hour)
	if !BeforeEpoch(pre) {
		t.Fatal("BeforeEpoch should report a time before the epoch")
	}
	if BeforeEpoch(Epoch()) || BeforeEpoch(time.Now()) {
		t.Fatal("BeforeEpoch should be false at and after the epoch")
	}
	if got := FromTime(pre); got != 0 {
		t.Errorf("FromTime(pre-epoch) = %d, want 0", got)
	}

	clock := &Clock{wall: fixedWall(pre)}
	a := clock.Now()
	b := clock.Now()
	if a != 1 || b != 2 {
		t.Errorf("pre-epoch clock should degrade to a logical counter from zero, got %d, %d", a, b)
	}
	if !Timestamp(0).Time().Equal(Epoch()) {
		t.Errorf("timestamp zero should map to the epoch, got %v", Timestamp(0).Time())
	}
}

func TestTimestampAhead(t *testing.T) {
	now := time.Now()

	if d := FromTime(now.Add(-time.Second)).Ahead(now); d != 0 {
		t.Errorf("past timestamp reported %v ahead", d)
	}
	if d := FromTime(now).Ahead(now.Add(time.Microsecond)); d != 0 {
		t.Errorf("timestamp behind now reported %v ahead", d)
	}
	d := FromTime(now.Add(3 * time.Second)).Ahead(now)
	if d < 3*time.Second-time.Microsecond || d > 3*time.Second {
		t.Errorf("expected ~3s ahead, got %v", d)
	}
	// The counter does not contribute.
	if d2 := (FromTime(now.Add(3*time.Second)) | Timestamp(counterMask)).Ahead(now); d2 != d {
		t.Errorf("counter changed Ahead: %v vs %v", d2, d)
	}
}

func TestWitnessBounded(t *testing.T) {
	now := time.Now()
	clock := &Clock{wall: fixedWall(now)}

	// Within the bound: witnessed.
	near := FromTime(now.Add(2 * time.Second))
	if !clock.WitnessBounded(near, 5*time.Second) {
		t.Fatal("timestamp 2s ahead should be accepted with a 5s bound")
	}
	if next := clock.Now(); next <= near {
		t.Fatalf("accepted timestamp was not witnessed: Now()=%d <= %d", next, near)
	}

	// Beyond the bound: refused, clock untouched.
	before := clock.Now()
	far := FromTime(now.Add(24 * time.Hour))
	if clock.WitnessBounded(far, 5*time.Second) {
		t.Fatal("timestamp a day ahead should be refused with a 5s bound")
	}
	if next := clock.Now(); next >= far || next != before+1 {
		t.Fatalf("refused timestamp moved the clock: Now()=%d, before=%d, far=%d", next, before, far)
	}

	// Past timestamps are always accepted (and are a no-op).
	if !clock.WitnessBounded(FromTime(now.Add(-time.Hour)), 0) {
		t.Fatal("a past timestamp should be accepted")
	}

	// Zero bound refuses anything ahead of wall time.
	if clock.WitnessBounded(FromTime(now.Add(time.Millisecond)), 0) {
		t.Fatal("a zero bound should refuse a timestamp ahead of wall time")
	}
	// Negative bound behaves as zero.
	if clock.WitnessBounded(FromTime(now.Add(time.Millisecond)), -time.Second) {
		t.Fatal("a negative bound should refuse a timestamp ahead of wall time")
	}
}
