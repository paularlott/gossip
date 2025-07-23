package hlc

import (
	"testing"
	"time"
)

func TestTimestampConstants(t *testing.T) {
	// Test that our constants make sense
	t.Logf("epochUnixNano: %d", epochUnixNano)
	t.Logf("timeBits: %d", timeBits)
	t.Logf("counterBits: %d", counterBits)
	t.Logf("maxTime: %d", (uint64(1)<<timeBits)-1)

	// Verify epoch is January 1, 2025
	expectedEpoch := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if epochUnixNano != expectedEpoch.UnixNano() {
		t.Errorf("Epoch mismatch: expected %d, got %d", expectedEpoch.UnixNano(), epochUnixNano)
	}
}

func TestTimestampTimeConversion(t *testing.T) {
	clock := NewClock()

	// Test with current time
	beforeTime := time.Now()
	ts := clock.Now()
	afterTime := time.Now()

	extractedTime := ts.Time()

	t.Logf("Before: %v (%d)", beforeTime, beforeTime.UnixNano())
	t.Logf("Timestamp: %d", uint64(ts))
	t.Logf("Extracted: %v (%d)", extractedTime, extractedTime.UnixNano())
	t.Logf("After: %v (%d)", afterTime, afterTime.UnixNano())

	// The extracted time should be between beforeTime and afterTime
	if extractedTime.Before(beforeTime) {
		t.Errorf("Extracted time %v is before creation time %v", extractedTime, beforeTime)
	}
	if extractedTime.After(afterTime) {
		t.Errorf("Extracted time %v is after creation time %v", extractedTime, afterTime)
	}
}

func TestTimestampRoundTrip(t *testing.T) {
	// Test specific times around the epoch
	testTimes := []time.Time{
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),             // Epoch start
		time.Date(2025, 1, 1, 0, 0, 0, 1, time.UTC),             // 1 nanosecond after epoch
		time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC),             // 1 second after epoch
		time.Date(2025, 6, 15, 12, 30, 45, 123456789, time.UTC), // Mid-year
	}

	for _, testTime := range testTimes {
		t.Run(testTime.Format(time.RFC3339Nano), func(t *testing.T) {
			// Calculate what the relative nanoseconds should be
			relNs := uint64(testTime.UnixNano() - epochUnixNano)

			// Create a timestamp with zero counter
			ts := Timestamp(relNs << counterBits)
			extractedTime := ts.Time()

			t.Logf("Original: %v (%d)", testTime, testTime.UnixNano())
			t.Logf("RelNs: %d", relNs)
			t.Logf("Timestamp: %d", uint64(ts))
			t.Logf("Extracted: %v (%d)", extractedTime, extractedTime.UnixNano())

			// The times should match exactly (ignoring counter bits)
			if !extractedTime.Equal(testTime) {
				t.Errorf("Round trip failed: expected %v, got %v", testTime, extractedTime)
			}
		})
	}
}

func TestTimestampEpochHandling(t *testing.T) {
	// Test timestamp at epoch
	epochTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts := Timestamp(0) // Zero timestamp should represent epoch
	extractedTime := ts.Time()

	t.Logf("Epoch time: %v (%d)", epochTime, epochTime.UnixNano())
	t.Logf("Zero timestamp extracted: %v (%d)", extractedTime, extractedTime.UnixNano())

	if !extractedTime.Equal(epochTime) {
		t.Errorf("Epoch conversion failed: expected %v, got %v", epochTime, extractedTime)
	}
}

func TestClockMonotonicity(t *testing.T) {
	clock := NewClock()

	var previous Timestamp
	for i := 0; i < 100; i++ {
		current := clock.Now()
		if i > 0 && !current.After(previous) {
			t.Errorf("Clock not monotonic: iteration %d, current %d not after previous %d", i, current, previous)
		}
		previous = current
	}
}

func TestTimestampBitManipulation(t *testing.T) {
	// Test that we can extract time and counter correctly
	testTime := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC) // 1 hour after epoch
	relNs := uint64(testTime.UnixNano() - epochUnixNano)

	// Create timestamp with specific counter value
	counter := uint64(42)
	ts := Timestamp((relNs << counterBits) | counter)

	// Extract time part (should ignore counter)
	extractedTime := ts.Time()
	expectedTime := time.Unix(0, int64(relNs)+epochUnixNano)

	t.Logf("Original time: %v", testTime)
	t.Logf("RelNs: %d", relNs)
	t.Logf("Counter: %d", counter)
	t.Logf("Timestamp: %d", uint64(ts))
	t.Logf("Extracted time: %v", extractedTime)
	t.Logf("Expected time: %v", expectedTime)

	if !extractedTime.Equal(expectedTime) {
		t.Errorf("Time extraction failed: expected %v, got %v", expectedTime, extractedTime)
	}
}

func TestDebugBitManipulation(t *testing.T) {
	// Let's debug exactly what's happening
	beforeTime := time.Now()
	ts := NewClock().Now()

	t.Logf("Before time: %d", beforeTime.UnixNano())
	t.Logf("Epoch: %d", epochUnixNano)
	t.Logf("Relative ns should be: %d", uint64(beforeTime.UnixNano())-epochUnixNano)

	// Extract what the timestamp thinks the relative ns is
	extractedRelNs := uint64(ts) >> counterBits
	t.Logf("Timestamp raw: %d", uint64(ts))
	t.Logf("Extracted rel ns: %d", extractedRelNs)
	t.Logf("Counter: %d", uint64(ts)&counterMask)

	// What time does this give us?
	reconstructedTime := time.Unix(0, int64(extractedRelNs)+epochUnixNano)
	t.Logf("Reconstructed time: %v (%d)", reconstructedTime, reconstructedTime.UnixNano())

	// Let's also check the Now() implementation
	now := uint64(time.Now().UnixNano())
	relNow := now - epochUnixNano
	t.Logf("Current time: %d", now)
	t.Logf("Current rel time: %d", relNow)
	t.Logf("Shifted rel time: %d", relNow<<counterBits)
}

func TestTypeConversions(t *testing.T) {
	now := time.Now()
	nowNs := now.UnixNano()
	nowUint := uint64(nowNs)

	t.Logf("Now as int64: %d", nowNs)
	t.Logf("Now as uint64: %d", nowUint)
	t.Logf("Epoch as int64: %d", epochUnixNano)
	t.Logf("Epoch as uint64: %d", uint64(epochUnixNano))

	// Test the subtraction both ways
	relNs1 := nowUint - uint64(epochUnixNano)
	relNs2 := uint64(nowNs - epochUnixNano)

	t.Logf("Method 1 (uint64 - uint64): %d", relNs1)
	t.Logf("Method 2 (uint64(int64 - int64)): %d", relNs2)

	if relNs1 != relNs2 {
		t.Errorf("Type conversion issue detected!")
	}
}

func TestDebugArithmetic(t *testing.T) {
	now := time.Now().UnixNano()

	// Test the old way (broken)
	nowUint := uint64(now)
	relNowBroken := uint64(nowUint - epochUnixNano) // This is wrong!

	// Test the correct way
	relNowFixed := uint64(now - epochUnixNano)

	t.Logf("Now (int64): %d", now)
	t.Logf("Now (uint64): %d", nowUint)
	t.Logf("Epoch: %d", epochUnixNano)
	t.Logf("Broken calculation: %d", relNowBroken)
	t.Logf("Fixed calculation: %d", relNowFixed)

	// The broken version will be completely wrong
	if relNowBroken == relNowFixed {
		t.Logf("Both calculations match (unexpected)")
	} else {
		t.Logf("Calculations differ as expected - broken: %d, fixed: %d", relNowBroken, relNowFixed)
	}
}
