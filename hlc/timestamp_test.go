package hlc

import (
	"testing"
	"time"
)

func TestTimestampConstants(t *testing.T) {
	// Test that our constants make sense
	t.Logf("epochUnixMicro: %d", epochUnixMicro)
	t.Logf("timeBits: %d", timeBits)
	t.Logf("counterBits: %d", counterBits)
	t.Logf("maxTime: %d", (uint64(1)<<timeBits)-1)

	// Verify epoch is January 1, 2025
	expectedEpoch := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if epochUnixMicro != expectedEpoch.UnixMicro() {
		t.Errorf("Epoch mismatch: expected %d, got %d", expectedEpoch.UnixMicro(), epochUnixMicro)
	}
}

func TestTimestampTimeConversion(t *testing.T) {
	clock := NewClock()

	// Test with current time
	beforeTime := time.Now()
	ts := clock.Now()
	afterTime := time.Now()

	extractedTime := ts.Time()

	t.Logf("Before: %v (%d)", beforeTime, beforeTime.UnixMicro())
	t.Logf("Timestamp: %d", uint64(ts))
	t.Logf("Extracted: %v (%d)", extractedTime, extractedTime.UnixMicro())
	t.Logf("After: %v (%d)", afterTime, afterTime.UnixMicro())

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
		time.Date(2025, 1, 1, 0, 0, 0, 1000, time.UTC),          // 1 microsecond after epoch
		time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC),             // 1 second after epoch
		time.Date(2025, 6, 15, 12, 30, 45, 123456000, time.UTC), // Mid-year
	}

	for _, testTime := range testTimes {
		t.Run(testTime.Format(time.RFC3339Nano), func(t *testing.T) {
			// Calculate what the relative microseconds should be
			relMicro := uint64(testTime.UnixMicro() - epochUnixMicro)

			// Create a timestamp with zero counter
			ts := Timestamp(relMicro << counterBits)
			extractedTime := ts.Time()

			t.Logf("Original: %v (%d)", testTime, testTime.UnixMicro())
			t.Logf("RelMicro: %d", relMicro)
			t.Logf("Timestamp: %d", uint64(ts))
			t.Logf("Extracted: %v (%d)", extractedTime, extractedTime.UnixMicro())

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

	t.Logf("Epoch time: %v (%d)", epochTime, epochTime.UnixMicro())
	t.Logf("Zero timestamp extracted: %v (%d)", extractedTime, extractedTime.UnixMicro())

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
	relMicro := uint64(testTime.UnixMicro() - epochUnixMicro)

	// Create timestamp with specific counter value
	counter := uint64(42)
	ts := Timestamp((relMicro << counterBits) | counter)

	// Extract time part (should ignore counter)
	extractedTime := ts.Time()
	expectedTime := time.UnixMicro(int64(relMicro) + epochUnixMicro)

	t.Logf("Original time: %v", testTime)
	t.Logf("RelMicro: %d", relMicro)
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

	t.Logf("Before time: %d", beforeTime.UnixMicro())
	t.Logf("Epoch: %d", epochUnixMicro)
	t.Logf("Relative micro should be: %d", uint64(beforeTime.UnixMicro())-epochUnixMicro)

	// Extract what the timestamp thinks the relative micro is
	extractedRelMicro := uint64(ts) >> counterBits
	t.Logf("Timestamp raw: %d", uint64(ts))
	t.Logf("Extracted rel micro: %d", extractedRelMicro)
	t.Logf("Counter: %d", uint64(ts)&counterMask)

	// What time does this give us?
	reconstructedTime := time.UnixMicro(int64(extractedRelMicro) + epochUnixMicro)
	t.Logf("Reconstructed time: %v (%d)", reconstructedTime, reconstructedTime.UnixMicro())

	// Let's also check the Now() implementation
	now := uint64(time.Now().UnixMicro())
	relNow := now - epochUnixMicro
	t.Logf("Current time: %d", now)
	t.Logf("Current rel time: %d", relNow)
	t.Logf("Shifted rel time: %d", relNow<<counterBits)
}

func TestTypeConversions(t *testing.T) {
	now := time.Now()
	nowMicro := now.UnixMicro()
	nowUint := uint64(nowMicro)

	t.Logf("Now as int64: %d", nowMicro)
	t.Logf("Now as uint64: %d", nowUint)
	t.Logf("Epoch as int64: %d", epochUnixMicro)
	t.Logf("Epoch as uint64: %d", uint64(epochUnixMicro))

	// Test the subtraction both ways
	relMicro1 := nowUint - uint64(epochUnixMicro)
	relMicro2 := uint64(nowMicro - epochUnixMicro)

	t.Logf("Method 1 (uint64 - uint64): %d", relMicro1)
	t.Logf("Method 2 (uint64(int64 - int64)): %d", relMicro2)

	if relMicro1 != relMicro2 {
		t.Errorf("Type conversion issue detected!")
	}
}

func TestDebugArithmetic(t *testing.T) {
	now := time.Now().UnixMicro()

	// Test the old way (broken)
	nowUint := uint64(now)
	relNowBroken := uint64(nowUint - epochUnixMicro) // This is wrong!

	// Test the correct way
	relNowFixed := uint64(now - epochUnixMicro)

	t.Logf("Now (int64): %d", now)
	t.Logf("Now (uint64): %d", nowUint)
	t.Logf("Epoch: %d", epochUnixMicro)
	t.Logf("Broken calculation: %d", relNowBroken)
	t.Logf("Fixed calculation: %d", relNowFixed)

	// The broken version will be completely wrong
	if relNowBroken == relNowFixed {
		t.Logf("Both calculations match (unexpected)")
	} else {
		t.Logf("Calculations differ as expected - broken: %d, fixed: %d", relNowBroken, relNowFixed)
	}
}

func TestTimeRangeCalculationMicroseconds(t *testing.T) {
	// Calculate the maximum time we can represent
	maxRelativeMicro := (uint64(1) << timeBits) - 1
	maxAbsoluteMicro := int64(maxRelativeMicro) + epochUnixMicro
	maxTime := time.UnixMicro(maxAbsoluteMicro)

	t.Logf("Max relative microseconds: %d", maxRelativeMicro)
	t.Logf("Max absolute microseconds: %d", maxAbsoluteMicro)
	t.Logf("Max representable time: %v", maxTime)

	// Convert to years
	microsecondsPerYear := 365.25 * 24 * 3600 * 1e6
	yearsFromEpoch := float64(maxRelativeMicro) / microsecondsPerYear
	t.Logf("Years from epoch: %.1f", yearsFromEpoch)

	// Check current usage
	now := time.Now()
	currentRelMicro := uint64(now.UnixMicro() - epochUnixMicro)
	usagePercent := float64(currentRelMicro) / float64(maxRelativeMicro) * 100

	t.Logf("Current time: %v", now)
	t.Logf("Current relative microseconds: %d", currentRelMicro)
	t.Logf("Current usage: %.6f%%", usagePercent)
}
