// Package hlc implements a Hybrid Logical Clock (HLC) for generating globally unique,
// monotonically increasing timestamps suitable for distributed systems. The HLC combines
// physical time (with microsecond precision) and a logical counter to ensure causality
// even in the presence of clock skew or concurrent events. Timestamps are encoded as
// 64-bit unsigned integers, with 54 bits for time (relative to a fixed epoch 1st Jan 2025)
// and 10 bits for the logical counter. The Clock type provides thread-safe timestamp
// generation using atomic operations; every timestamp it issues is strictly
// greater than the last, with the counter carrying into the time field when it
// fills. Wall clocks before the epoch clamp to zero (see BeforeEpoch).

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
//
// The zero value is ready to use.
type Clock struct {
	last uint64 // atomic

	// wall overrides the wall-clock source. Nil means time.Now. Unexported:
	// only the package's own tests substitute it.
	wall func() time.Time
}

// NewClock creates a new HLC clock.
func NewClock() *Clock {
	return &Clock{}
}

func (c *Clock) wallNow() time.Time {
	if c.wall != nil {
		return c.wall()
	}
	return time.Now()
}

// physical returns t as microseconds since the HLC epoch, shifted into the
// time field. A wall clock before the epoch is clamped to 0 rather than
// wrapping to an enormous unsigned value; use BeforeEpoch to detect it.
func physical(t time.Time) uint64 {
	rel := t.UnixMicro() - epochUnixMicro
	if rel < 0 {
		return 0
	}
	return uint64(rel) << CounterBits
}

// Now returns a new Timestamp based on the current time and HLC rules.
//
// Every call returns a value strictly greater than any value previously
// returned by Now or passed to Witness on this clock:
//
//	ts = max(physical_now, last+1)
//
// While the wall clock is ahead of the last issued timestamp the counter is
// zero. While it is not — many calls inside one microsecond, after Witness
// of a peer's timestamp that is ahead of local time, or after the wall clock
// steps backwards — the logical counter increments, and when it fills
// (2^CounterBits values) the carry flows into the time field. The time
// component of a timestamp can therefore run marginally ahead of wall time
// under sustained bursts, and runs ahead by the witnessed skew after
// Witness; it never goes backwards and never repeats.
//
// The wire format is unchanged: 54 bits of microseconds since the epoch
// followed by CounterBits of counter.
func (c *Clock) Now() Timestamp {
	for {
		phys := physical(c.wallNow())
		last := atomic.LoadUint64(&c.last)

		ts := phys
		if ts <= last {
			ts = last + 1 // counter increment; carries into the time field on overflow
		}

		if atomic.CompareAndSwapUint64(&c.last, last, ts) {
			return Timestamp(ts)
		}
		// else: retry
	}
}

// Witness advances the clock so that it has observed ts. After Witness
// returns, the next Now() is strictly greater than ts even when the local
// wall clock is behind the observed timestamp — the logical counter takes
// over (carrying into the time field as needed). Receiving nodes use this
// when merging remote timestamps so that a locally minted write always
// dominates a previously observed remote one.
//
// Witness is unbounded: a peer whose clock is far in the future (or a
// corrupted/poisoned version) drags this clock forward with it, and every
// timestamp minted afterwards inherits that offset. Where timestamps arrive
// from the network, prefer WitnessBounded.
func (c *Clock) Witness(ts Timestamp) {
	v := uint64(ts)
	for {
		last := atomic.LoadUint64(&c.last)
		if v <= last {
			return
		}
		if atomic.CompareAndSwapUint64(&c.last, last, v) {
			return
		}
	}
}

// WitnessBounded is Witness with a guard against far-future timestamps. If ts
// is more than maxAhead ahead of this clock's wall time (see Timestamp.Ahead)
// it is NOT witnessed and false is returned; otherwise ts is witnessed exactly
// as by Witness and true is returned (including when ts is already in the
// past, which is a no-op).
//
// The bound exists because Witness is sticky: a single poisoned version from
// next year, once witnessed, makes every node that sees it mint versions from
// next year, which in turn defeats any age-based cut-off computed from wall
// time (tombstone purging, retention, TTLs keyed on version). Choose maxAhead
// comfortably larger than the clock skew you tolerate between healthy nodes
// (seconds to a few minutes is typical). A maxAhead of zero or less refuses
// any timestamp whose time component is ahead of local wall time.
//
// Rejecting ts only declines to advance the clock; what to do with the entry
// that carried it (drop it, quarantine it, accept it without witnessing) is
// the caller's decision. Note that accepting such an entry without
// witnessing it means a local write minted immediately afterwards may not
// dominate it.
func (c *Clock) WitnessBounded(ts Timestamp, maxAhead time.Duration) (accepted bool) {
	if maxAhead < 0 {
		maxAhead = 0
	}
	if ts.Ahead(c.wallNow()) > maxAhead {
		return false
	}
	c.Witness(ts)
	return true
}

// Ahead reports how far the time component of ts is ahead of the wall time
// now, or 0 if it is not ahead. The logical counter is ignored, so the
// resolution is one microsecond. Use it to detect timestamps minted by a peer
// whose clock is fast, or that were poisoned, before witnessing them.
func (ts Timestamp) Ahead(now time.Time) time.Duration {
	if d := ts.Time().Sub(now); d > 0 {
		return d
	}
	return 0
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

// Uint64 returns the raw 64-bit encoded value of the timestamp.
func (ts Timestamp) Uint64() uint64 {
	return uint64(ts)
}

var defaultClock = NewClock()

// Now returns a new Timestamp from the default Clock.
func Now() Timestamp {
	return defaultClock.Now()
}

// FromTime converts a time.Time to an HLC Timestamp with counter set to 0.
// Times before the epoch (see Epoch) clamp to the zero timestamp.
func FromTime(t time.Time) Timestamp {
	return Timestamp(physical(t))
}

// Epoch returns the instant that timestamp time zero represents
// (1st Jan 2025, 00:00:00 UTC).
func Epoch() time.Time {
	return time.UnixMicro(epochUnixMicro).UTC()
}

// BeforeEpoch reports whether t is before the HLC epoch. A wall clock reading
// before the epoch cannot be represented: Clock.Now and FromTime clamp it to
// time zero, so timestamps stop tracking wall time (Now degrades to a pure
// logical counter). Callers should check BeforeEpoch(time.Now()) at start-up
// and refuse to run — or at least warn loudly — when it is true, since it
// almost always means an unset RTC or a badly misconfigured clock.
func BeforeEpoch(t time.Time) bool {
	return t.UnixMicro() < epochUnixMicro
}
