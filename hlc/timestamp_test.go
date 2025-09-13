package hlc

import (
	"sync"
	"testing"
	"time"
)

func TestNewClock(t *testing.T) {
	clock := NewClock()
	if clock == nil {
		t.Fatal("NewClock() returned nil")
	}
	if clock.last != 0 {
		t.Errorf("Expected initial last value to be 0, got %d", clock.last)
	}
}

func TestTimestampNow(t *testing.T) {
	clock := NewClock()

	// Test basic timestamp generation
	ts1 := clock.Now()
	ts2 := clock.Now()

	if ts1 == 0 {
		t.Error("Expected non-zero timestamp")
	}

	if !ts2.After(ts1) && !ts2.Equal(ts1) {
		t.Errorf("Expected ts2 (%d) to be after or equal to ts1 (%d)", ts2, ts1)
	}
}

func TestTimestampMonotonicity(t *testing.T) {
	clock := NewClock()

	var timestamps []Timestamp
	for i := 0; i < 1000; i++ {
		timestamps = append(timestamps, clock.Now())
	}

	// Verify monotonicity
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i].Before(timestamps[i-1]) {
			t.Errorf("Timestamp %d (%d) is before timestamp %d (%d)",
				i, timestamps[i], i-1, timestamps[i-1])
		}
	}
}

func TestTimestampConcurrency(t *testing.T) {
	clock := NewClock()
	const numGoroutines = 100
	const timestampsPerGoroutine = 100

	timestamps := make([][]Timestamp, numGoroutines)
	var wg sync.WaitGroup

	// Generate timestamps concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			timestamps[idx] = make([]Timestamp, timestampsPerGoroutine)
			for j := 0; j < timestampsPerGoroutine; j++ {
				timestamps[idx][j] = clock.Now()
			}
		}(i)
	}

	wg.Wait()

	// Collect all timestamps
	var allTimestamps []Timestamp
	for i := 0; i < numGoroutines; i++ {
		allTimestamps = append(allTimestamps, timestamps[i]...)
	}

	// Verify uniqueness
	timestampMap := make(map[Timestamp]bool)
	for _, ts := range allTimestamps {
		if timestampMap[ts] {
			t.Errorf("Duplicate timestamp found: %d", ts)
		}
		timestampMap[ts] = true
	}

	if len(timestampMap) != len(allTimestamps) {
		t.Errorf("Expected %d unique timestamps, got %d", len(allTimestamps), len(timestampMap))
	}
}

func TestTimestampComparison(t *testing.T) {
	clock := NewClock()

	ts1 := clock.Now()
	time.Sleep(1 * time.Millisecond) // Ensure different time
	ts2 := clock.Now()

	// Test Before
	if !ts1.Before(ts2) {
		t.Errorf("Expected ts1 (%d) to be before ts2 (%d)", ts1, ts2)
	}
	if ts2.Before(ts1) {
		t.Errorf("Expected ts2 (%d) not to be before ts1 (%d)", ts2, ts1)
	}

	// Test After
	if !ts2.After(ts1) {
		t.Errorf("Expected ts2 (%d) to be after ts1 (%d)", ts2, ts1)
	}
	if ts1.After(ts2) {
		t.Errorf("Expected ts1 (%d) not to be after ts2 (%d)", ts1, ts2)
	}

	// Test Equal
	if ts1.Equal(ts2) {
		t.Errorf("Expected ts1 (%d) not to equal ts2 (%d)", ts1, ts2)
	}
	if !ts1.Equal(ts1) {
		t.Errorf("Expected ts1 (%d) to equal itself", ts1)
	}
}

func TestTimestampTimeExtraction(t *testing.T) {
	clock := NewClock()

	beforeTime := time.Now()
	ts := clock.Now()
	afterTime := time.Now()

	extractedTime := ts.Time()

	// The extracted time should be within the window
	if extractedTime.Before(beforeTime.Add(-1 * time.Millisecond)) {
		t.Errorf("Extracted time %v is too early, expected after %v", extractedTime, beforeTime)
	}
	if extractedTime.After(afterTime.Add(1 * time.Millisecond)) {
		t.Errorf("Extracted time %v is too late, expected before %v", extractedTime, afterTime)
	}
}

func TestTimestampCounterExtraction(t *testing.T) {
	clock := NewClock()

	// Generate many timestamps quickly to trigger counter increments
	var counters []uint16
	for i := 0; i < 100; i++ {
		ts := clock.Now()
		counters = append(counters, ts.Counter())
	}

	// At least some should have non-zero counters due to rapid generation
	hasNonZeroCounter := false
	for _, counter := range counters {
		if counter > 0 {
			hasNonZeroCounter = true
			break
		}
		if uint64(counter) > counterMask {
			t.Errorf("Counter %d exceeds maximum value %d", counter, counterMask)
		}
	}

	// Note: This might occasionally fail on very fast systems, but should generally pass
	if !hasNonZeroCounter {
		t.Log("Warning: No non-zero counters found - this might indicate timestamps aren't being generated fast enough")
	}
}

func TestTimestampBitLayout(t *testing.T) {
	clock := NewClock()
	ts := clock.Now()

	// Verify the bit layout
	timeComponent := uint64(ts) >> CounterBits
	counterComponent := uint64(ts) & counterMask

	// Reconstruct timestamp
	reconstructed := (timeComponent << CounterBits) | counterComponent

	if reconstructed != uint64(ts) {
		t.Errorf("Bit layout verification failed: original=%d, reconstructed=%d", ts, reconstructed)
	}

	// Verify counter is within bounds
	if counterComponent > counterMask {
		t.Errorf("Counter component %d exceeds mask %d", counterComponent, counterMask)
	}
}

func TestDefaultClock(t *testing.T) {
	// Test the package-level Now() function
	ts1 := Now()
	ts2 := Now()

	if ts1 == 0 {
		t.Error("Expected non-zero timestamp from default clock")
	}

	if !ts2.After(ts1) && !ts2.Equal(ts1) {
		t.Errorf("Expected ts2 (%d) to be after or equal to ts1 (%d)", ts2, ts1)
	}
}

func TestTimestampCounterOverflow(t *testing.T) {
	clock := NewClock()

	// This test is tricky because we need to generate enough concurrent
	// timestamps to potentially overflow the counter
	const iterations = 2000

	var timestamps []Timestamp
	for i := 0; i < iterations; i++ {
		timestamps = append(timestamps, clock.Now())
	}

	// Check that all timestamps are unique and monotonic
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i].Before(timestamps[i-1]) {
			t.Errorf("Non-monotonic timestamps at index %d: %d < %d",
				i, timestamps[i], timestamps[i-1])
		}
		if timestamps[i].Equal(timestamps[i-1]) {
			t.Errorf("Duplicate timestamps at index %d: %d", i, timestamps[i])
		}
	}
}

func BenchmarkTimestampGeneration(b *testing.B) {
	clock := NewClock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = clock.Now()
	}
}

func BenchmarkTimestampGenerationParallel(b *testing.B) {
	clock := NewClock()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = clock.Now()
		}
	})
}

func BenchmarkTimestampComparison(b *testing.B) {
	clock := NewClock()
	ts1 := clock.Now()
	ts2 := clock.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ts1.Before(ts2)
	}
}
