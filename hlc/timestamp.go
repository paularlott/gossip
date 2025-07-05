// Package hlc implements a Hybrid Logical Clock (HLC) for generating globally unique,
// monotonically increasing timestamps suitable for distributed systems. The HLC combines
// physical time (with nanosecond precision) and a logical counter to ensure causality
// even in the presence of clock skew or concurrent events. Timestamps are encoded as
// 64-bit unsigned integers, with 52 bits for time (relative to a fixed epoch 1st Jan 2025)
// and 12 bits for the logical counter. The Clock type provides thread-safe timestamp
// generation using atomic operations.

package hlc

import (
	"sync/atomic"
	"time"
)

const (
	timeBits    = 52
	counterBits = 12
	timeMask    = (uint64(1)<<timeBits - 1) << counterBits
	counterMask = uint64(1)<<counterBits - 1

	// Epoch: 1st Jan 2025, 00:00:00 UTC
	epochUnixNano = 1735689600000000000
)

// Timestamp represents a Hybrid Logical Clock timestamp.
type Timestamp uint64

// Clock is a thread-safe Hybrid Logical Clock using atomics.
type Clock struct {
	last uint64 // atomic
}

// NewClock creates a new HLC clock.
func NewClock() *Clock {
	return &Clock{}
}

// Now returns a new Timestamp based on the current time and HLC rules.
func (c *Clock) Now() Timestamp {
	for {
		now := uint64(time.Now().UnixNano())
		relNow := now - epochUnixNano
		last := atomic.LoadUint64(&c.last)
		lastTime := last >> counterBits
		lastCounter := last & counterMask

		var ts uint64
		if relNow > lastTime {
			ts = (relNow << counterBits)
		} else {
			ts = (lastTime << counterBits) | ((lastCounter + 1) & counterMask)
		}

		if atomic.CompareAndSwapUint64(&c.last, last, ts) {
			return Timestamp(ts)
		}
		// else: retry
	}
}

// Before returns true if ts is before other.
func (ts Timestamp) Before(other Timestamp) bool {
	return ts < other
}

// After returns true if ts is after other.
func (ts Timestamp) After(other Timestamp) bool {
	return ts > other
}

// Time extracts the time component as time.Time.
func (ts Timestamp) Time() time.Time {
	relNs := (uint64(ts) & timeMask) >> counterBits
	ns := relNs + epochUnixNano
	return time.Unix(0, int64(ns))
}

// Counter extracts the counter component.
func (ts Timestamp) Counter() uint16 {
	return uint16(uint64(ts) & counterMask)
}

var defaultClock = NewClock()

// Now returns a new Timestamp from the default Clock.
func Now() Timestamp {
	return defaultClock.Now()
}
