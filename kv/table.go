package kv

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/hlc"
)

// gcGrace absorbs clock skew between nodes before an expired entry may be
// forgotten: a replica with a fast clock must not drop an entry that is still
// live elsewhere and could be the last copy.
const gcGrace = 10 * time.Second

// table is the node's local replica of the store. It is leaderless: every
// node applies writes locally and merges remote entries with the LWW rules
// documented on Entry. Correctness of the merge depends only on entry
// contents, so pushes, gossip, and full syncs all funnel through apply.
type table struct {
	mu      sync.Mutex
	entries map[string]*Entry
	cfg     *Config
	clock   *hlc.Clock
	nowFn   func() time.Time
	selfID  gossip.NodeID
	closed  bool
}

func newTable(cfg *Config, selfID gossip.NodeID) *table {
	return &table{
		entries: make(map[string]*Entry),
		cfg:     cfg,
		clock:   cfg.Clock,
		nowFn:   cfg.NowFn,
		selfID:  selfID,
	}
}

// close stops the table accepting further merges. Reaping is driven by the
// store on the cluster's gossip event, so the table owns no timer of its own.
func (t *table) close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
}

// expired reports whether a live entry's TTL has lapsed.
func (e *Entry) expired(now time.Time) bool {
	return e.ExpiresAtMs != 0 && now.UnixMilli() > e.ExpiresAtMs
}

// live reports whether the entry currently holds a readable value.
func (e *Entry) live(now time.Time) bool {
	return !e.Tombstone && !e.expired(now)
}

// compareNodeID orders node IDs byte-wise; uuid.UUID is a [16]byte array.
func compareNodeID(a, b gossip.NodeID) int {
	return bytes.Compare(a[:], b[:])
}

// get returns a live value. Expiry is checked lazily: an entry past its TTL
// reads as missing without being removed, so peers can still learn of it and
// derive the expiry themselves.
func (t *table) get(key string) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	e, ok := t.entries[key]
	if !ok || !e.live(t.nowFn()) {
		return nil, false
	}
	return e.Value, true
}

// setLocal mints a versioned entry for a local write. The current entry's
// version is witnessed before minting, so the local clock — and therefore
// this write — is guaranteed to dominate whatever the table already holds.
func (t *table) setLocal(key string, value []byte, ttl time.Duration) *Entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	if cur, ok := t.entries[key]; ok {
		t.clock.Witness(hlc.Timestamp(cur.Version))
	}

	e := &Entry{
		Key:     key,
		Value:   value,
		Version: uint64(t.clock.Now()),
		Origin:  t.selfID,
	}
	if ttl > 0 {
		e.ExpiresAtMs = t.nowFn().Add(ttl).UnixMilli()
	}
	t.entries[key] = e
	return e
}

// deleteLocal mints a tombstone. It is written even when the key is absent
// locally: a peer may still hold a value the local node never saw, and only
// the tombstone prevents that value resurrecting on the next sync.
func (t *table) deleteLocal(key string) *Entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	if cur, ok := t.entries[key]; ok {
		t.clock.Witness(hlc.Timestamp(cur.Version))
	}

	e := &Entry{
		Key:         key,
		Version:     uint64(t.clock.Now()),
		Origin:      t.selfID,
		Tombstone:   true,
		DeletedAtMs: t.nowFn().UnixMilli(),
	}
	t.entries[key] = e
	return e
}

// deletePrefixLocal tombstones every live key under prefix, returning the
// batch for replication. Absent keys are skipped — unlike a plain Delete
// there is no resurrection to guard against for keys the table has never
// held, and skipping keeps bulk deletes from minting unbounded junk.
func (t *table) deletePrefixLocal(prefix string) []*Entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	now := t.nowFn()
	var out []*Entry
	for k, e := range t.entries {
		if !e.live(now) || !hasPrefix(k, prefix) {
			continue
		}
		t.clock.Witness(hlc.Timestamp(e.Version))
		tomb := &Entry{
			Key:         k,
			Version:     uint64(t.clock.Now()),
			Origin:      t.selfID,
			Tombstone:   true,
			DeletedAtMs: now.UnixMilli(),
		}
		t.entries[k] = tomb
		out = append(out, tomb)
	}
	return out
}

// apply merges one remote entry using the version-ordered rules. Returns
// false when the entry was dominated.
func (t *table) apply(in *Entry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.applyLocked(in)
}

func (t *table) applyLocked(in *Entry) bool {
	if t.closed || in == nil || in.Key == "" {
		return false
	}

	// Whatever arrives, the clock has now seen it: a local write minted
	// afterwards is guaranteed to beat it.
	t.clock.Witness(hlc.Timestamp(in.Version))

	cur, ok := t.entries[in.Key]
	if !ok {
		t.entries[in.Key] = in
		return true
	}

	switch {
	case in.Version > cur.Version:
		t.entries[in.Key] = in
		return true
	case in.Version < cur.Version:
		return false // dominated; stale write arriving late
	case in.Tombstone != cur.Tombstone:
		if in.Tombstone {
			t.entries[in.Key] = in // a delete cannot be resurrected
			return true
		}
		return false // a value cannot resurrect over its tombstone
	case in.Tombstone:
		// Two tombstones at the same version: keep the later delete time so
		// no node GCs earlier than any node saw.
		if in.DeletedAtMs > cur.DeletedAtMs {
			t.entries[in.Key] = in
		}
		return true
	default:
		// Two values at the same version: the higher origin wins, making the
		// outcome identical on every replica. Equal origin is the same write
		// redelivered — keep what we have.
		if compareNodeID(in.Origin, cur.Origin) > 0 {
			t.entries[in.Key] = in
			return true
		}
		return false
	}
}

// applyAll merges a batch of entries and reports how many were adopted.
func (t *table) applyAll(entries []*Entry) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	n := 0
	for _, e := range entries {
		if t.applyLocked(e) {
			n++
		}
	}
	return n
}

// keys returns the sorted live keys under prefix.
func (t *table) keys(prefix string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()
	var out []string
	for k, e := range t.entries {
		if e.live(now) && hasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// len reports the number of live keys.
func (t *table) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()
	n := 0
	for _, e := range t.entries {
		if e.live(now) {
			n++
		}
	}
	return n
}

// entryCount reports total entries including tombstones (diagnostics).
func (t *table) entryCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}

// tombstoneCount reports live tombstones (diagnostics).
func (t *table) tombstoneCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	n := 0
	for _, e := range t.entries {
		if e.Tombstone {
			n++
		}
	}
	return n
}

// snapshot exports the current view — live entries and unexpired tombstones —
// for a full-sync exchange. It omits exactly what reap would remove.
func (t *table) snapshot() []*Entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()
	out := make([]*Entry, 0, len(t.entries))
	for _, e := range t.entries {
		if t.reapable(e, now) {
			continue
		}
		ce := *e
		out = append(out, &ce)
	}
	return out
}

// reapable reports whether reap may forget the entry now.
func (t *table) reapable(e *Entry, now time.Time) bool {
	if e.Tombstone {
		return now.After(time.UnixMilli(e.DeletedAtMs).Add(t.cfg.TombstoneRetention))
	}
	if e.ExpiresAtMs == 0 {
		return false // no expiry: lives until deleted
	}
	return now.After(time.UnixMilli(e.ExpiresAtMs).Add(gcGrace))
}

// reap clears expired entries and GCs tombstones so the map does not grow
// without bound. Called by the store on the cluster's gossip event; expiry
// itself is also checked lazily on every read, so reaping is memory hygiene,
// not correctness.
func (t *table) reap() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()
	for k, e := range t.entries {
		if t.reapable(e, now) {
			delete(t.entries, k)
		}
	}
}

// hasPrefix matches keys under a prefix; the empty prefix matches everything.
func hasPrefix(key, prefix string) bool {
	return len(prefix) == 0 || (len(key) >= len(prefix) && key[:len(prefix)] == prefix)
}
