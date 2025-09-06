// Package hlc implements a Hybrid Logical Clock (HLC) for generating globally unique,
// monotonically increasing timestamps suitable for distributed systems. The HLC combines
// physical time (with microsecond precision) and a logical counter to ensure causality
// even in the presence of clock skew or concurrent events. Timestamps are encoded as
// 64-bit unsigned integers, with 54 bits for time (relative to a fixed epoch 1st Jan 2025)
// and 10 bits for the logical counter. The Clock type provides thread-safe timestamp
// generation using atomic operations.

package hlc

import (
	"sync/atomic"
	"time"
)

const (
	CounterBits = 10
	counterMask = uint64(1)<<CounterBits - 1

	// Epoch: 1st Jan 2025, 00:00:00 UTC (in microseconds)
	epochUnixMicro = 1735689600000000
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
		now := time.Now().UnixMicro()
		relNow := uint64(now - epochUnixMicro)
		last := atomic.LoadUint64(&c.last)
		lastTime := last >> CounterBits
		lastCounter := last & counterMask

		var ts uint64
		if relNow > lastTime {
			ts = (relNow << CounterBits)
		} else {
			ts = (lastTime << CounterBits) | ((lastCounter + 1) & counterMask)
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

// Equal returns true if ts is equal to other.
func (ts Timestamp) Equal(other Timestamp) bool {
	return ts == other
}

// Time extracts the time component as time.Time.
func (ts Timestamp) Time() time.Time {
	// Extract the time portion by shifting right to remove counter bits
	relMicro := uint64(ts) >> CounterBits

	// Convert back to absolute microseconds since Unix epoch
	absoluteMicro := int64(relMicro) + epochUnixMicro

	return time.UnixMicro(absoluteMicro)
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
