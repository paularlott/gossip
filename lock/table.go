package lock

import (
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

// entry is a single lock-table record held in the local replica store.
type entry struct {
	owner      gossip.NodeID
	token      Token
	held       bool      // false once released (tombstone)
	expiresAt  time.Time // meaningful while held
	releasedAt time.Time // meaningful once released
}

// expired reports whether a held entry's lease has lapsed.
func (e *entry) expired(now time.Time) bool {
	return !e.held || now.After(e.expiresAt)
}

func (e *entry) remaining(now time.Time) time.Duration {
	d := e.expiresAt.Sub(now)
	if d < 0 {
		return 0
	}
	return d
}

func (e *entry) toWire(key string) replicaEntry {
	w := replicaEntry{Key: key, Token: e.token, Held: e.held}
	if e.held {
		w.ExpiresAtMs = e.expiresAt.UnixMilli()
		w.Owner = e.owner
	} else {
		w.ReleasedAtMs = e.releasedAt.UnixMilli()
	}
	return w
}

func fromWire(w replicaEntry) *entry {
	e := &entry{token: w.Token, held: w.Held}
	if w.Held {
		e.owner = w.Owner
		e.expiresAt = time.UnixMilli(w.ExpiresAtMs)
	} else {
		e.releasedAt = time.UnixMilli(w.ReleasedAtMs)
	}
	return e
}

// gcGrace absorbs clock skew between nodes before an expired grant is allowed
// to be forgotten: a replica with a fast clock must not drop an entry that is
// still live elsewhere and could be the last copy.
const gcGrace = 10 * time.Second

// table is the node's replica store of lock state. Whoever currently holds
// leadership serves reads and writes against it; everyone else receives pushed
// and gossiped entries and answers recovery queries with it.
//
// # Authority and recovery
//
// A table refuses to grant until its node has been elected leader AND completed
// recovery: merging replica state from every live peer, deadline-bounded. There
// is deliberately no time-based warm-up gate — recovery replaces it. An acked
// mutation is on at least W nodes, so a new leader that hears from the live
// replicas knows every acked grant; the residual cases (all replicas of a grant
// unreachable at recovery time) are bounded by holder extends and fencing
// tokens, per the safety contract in the README.
//
// # Merge ordering
//
// Entries merge by token: higher token wins; equal token prefers the tombstone;
// equal token held-vs-held takes the later expiry. Token ordering comes from
// the single election domain, so a later leader's entries always dominate a
// zombie predecessor's regardless of wall clocks.
type table struct {
	mu      sync.Mutex
	entries map[string]*entry
	counter uint64 // per-term counter for fencing tokens

	term uint64
	// serving is true only while this node is the leader and has completed
	// recovery for the current term. Grants are refused otherwise.
	serving bool

	maxTTL time.Duration // bounds tombstone retention
	nowFn  func() time.Time

	closed bool
}

func newTable(maxTTL time.Duration) *table {
	return &table{
		entries: make(map[string]*entry),
		maxTTL:  maxTTL,
		nowFn:   time.Now,
	}
}

// close stops the table accepting further merges. Reaping is driven by the
// pool on the cluster's gossip event, so the table owns no timer of its own.
func (t *table) close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
}

// now returns the injectable clock.
func (t *table) now() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nowFn()
}

// assumeLeadership records the term under which this table will serve and
// closes service until recovery completes.
func (t *table) assumeLeadership(term uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if term == t.term && t.serving {
		return // already serving this term
	}

	t.term = term
	t.counter = 0
	t.serving = false
}

// openForService marks recovery complete; grants may proceed.
func (t *table) openForService() {
	t.mu.Lock()
	t.serving = true
	t.mu.Unlock()
}

// relinquishLeadership stops the table serving. The entries are deliberately
// kept: they are this node's replica view, needed by a future recovery and
// kept current by pushed and gossiped entries.
func (t *table) relinquishLeadership() {
	t.mu.Lock()
	t.serving = false
	t.mu.Unlock()
}

func (t *table) servingNow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.serving
}

// currentTerm returns the term this table last assumed.
func (t *table) currentTerm() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.term
}

// tryAcquire grants a lock if the key is free, returning the resulting entry
// for replication. A re-entrant acquire by the current owner refreshes the
// expiry and keeps the token.
func (t *table) tryAcquire(key string, owner gossip.NodeID, ttl time.Duration) (replicaEntry, bool, string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()

	if !t.serving {
		return replicaEntry{}, false, reasonRecovering
	}

	if e, ok := t.entries[key]; ok && !e.expired(now) {
		if e.owner == owner {
			e.expiresAt = now.Add(ttl)
			return e.toWire(key), true, ""
		}
		return replicaEntry{}, false, "held by another node"
	}

	t.counter++
	tok := Token{Term: t.term, Counter: t.counter}
	t.entries[key] = &entry{
		owner:     owner,
		token:     tok,
		held:      true,
		expiresAt: now.Add(ttl),
	}

	return t.entries[key].toWire(key), true, ""
}

// tombstone converts a held grant into a release record when the token
// matches, returning the tombstone for replication. Releasing an absent or
// expired key succeeds silently (idempotent).
func (t *table) release(key string, token Token) (replicaEntry, bool, string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()

	e, ok := t.entries[key]
	if !ok || e.expired(now) {
		if ok {
			t.entries[key] = &entry{token: e.token, held: false, releasedAt: now}
		}
		return replicaEntry{}, true, ""
	}
	if !e.token.Equal(token) {
		return replicaEntry{}, false, "token mismatch"
	}

	e.held = false
	e.owner = gossip.EmptyNodeID
	e.expiresAt = time.Time{}
	e.releasedAt = now

	return e.toWire(key), true, ""
}

// extend refreshes a held lock's expiry when the token matches, returning the
// updated entry for replication.
func (t *table) extend(key string, token Token, ttl time.Duration) (replicaEntry, bool, string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()

	e, ok := t.entries[key]
	if !ok || e.expired(now) {
		return replicaEntry{}, false, "not held"
	}
	if !e.token.Equal(token) {
		return replicaEntry{}, false, "token mismatch"
	}

	e.expiresAt = now.Add(ttl)
	return e.toWire(key), true, ""
}

// releaseByOwner tombstones every live lock held by a node, used when a holder
// dies so its locks free immediately rather than waiting out their TTLs.
func (t *table) releaseByOwner(owner gossip.NodeID) []replicaEntry {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()

	var out []replicaEntry
	for k, e := range t.entries {
		if e.held && !e.expired(now) && e.owner == owner {
			e.held = false
			e.owner = gossip.EmptyNodeID
			e.expiresAt = time.Time{}
			e.releasedAt = now
			out = append(out, e.toWire(k))
		}
	}
	return out
}

// query reports a key's current state. ready is false when the table is not
// currently the serving authority, in which case callers must not treat the
// key as free.
func (t *table) query(key string) (held bool, owner gossip.NodeID, token Token, remainingMs int64, ready bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.serving {
		return false, gossip.EmptyNodeID, Token{}, 0, false
	}

	now := t.nowFn()
	e, ok := t.entries[key]
	if !ok || e.expired(now) {
		return false, gossip.EmptyNodeID, Token{}, 0, true
	}

	return true, e.owner, e.token, e.remaining(now).Milliseconds(), true
}

// applyEntry merges one entry from a peer (push, gossip, or recovery) using the
// token-ordered rules. Returns false when the entry was dominated.
func (t *table) applyEntry(w replicaEntry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.applyEntryLocked(w)
}

func (t *table) applyEntryLocked(w replicaEntry) bool {
	if t.closed || w.Key == "" {
		return false
	}

	incoming := fromWire(w)
	cur, ok := t.entries[w.Key]
	if !ok {
		t.entries[w.Key] = incoming
		t.advanceCounterLocked(incoming.token)
		return true
	}

	switch {
	case incoming.token.After(cur.token):
		t.entries[w.Key] = incoming
		t.advanceCounterLocked(incoming.token)
		return true
	case cur.token.After(incoming.token):
		return false // dominated; stale write arriving late
	case !incoming.held && cur.held:
		t.entries[w.Key] = incoming // tombstone beats its grant
		return true
	case incoming.held && !cur.held:
		return false // already released; a grant cannot resurrect
	default:
		// Equal token, same state. For held entries the later expiry wins:
		// extends serialize at the leader, so its deadlines are comparable.
		// For tombstones keep the later release time (GCs no earlier than
		// any node saw).
		if incoming.held {
			if incoming.expiresAt.After(cur.expiresAt) {
				cur.expiresAt = incoming.expiresAt
			}
		} else if incoming.releasedAt.After(cur.releasedAt) {
			cur.releasedAt = incoming.releasedAt
		}
		return true
	}
}

// advanceCounterLocked keeps the local counter past any recovered token from
// the current term so freshly minted tokens cannot collide.
func (t *table) advanceCounterLocked(tok Token) {
	if tok.Term == t.term && tok.Counter > t.counter {
		t.counter = tok.Counter
	}
}

// applyAll merges a batch of entries (a recovery response) and reports how
// many were adopted.
func (t *table) applyAll(entries []replicaEntry) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	n := 0
	for _, w := range entries {
		if t.applyEntryLocked(w) {
			n++
		}
	}
	return n
}

// snapshot exports the current view — live grants and unexpired tombstones —
// for a recovery query. Tombstones travel so that a released grant cannot be
// resurrected by a node that never saw the release.
func (t *table) snapshot() []replicaEntry {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()
	out := make([]replicaEntry, 0, len(t.entries))
	for k, e := range t.entries {
		if e.held && now.After(e.expiresAt.Add(gcGrace)) {
			continue // long expired; safe to omit
		}
		if !e.held && now.After(e.releasedAt.Add(t.maxTTL)) {
			continue // tombstone past its GC bound
		}
		out = append(out, e.toWire(k))
	}
	return out
}

// count reports the number of live locks (used by tests and diagnostics).
func (t *table) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()
	n := 0
	for _, e := range t.entries {
		if e.held && !e.expired(now) {
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

// reap clears expired grants and GCs tombstones so the map does not grow
// without bound. Called by the pool on the cluster's gossip event; expiry
// itself is also checked lazily on every read, so reaping is memory hygiene,
// not correctness.
func (t *table) reap() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.nowFn()
	for k, e := range t.entries {
		// Grants: keep until expiry plus a skew-absorbing grace.
		if e.held && now.After(e.expiresAt.Add(gcGrace)) {
			delete(t.entries, k)
			continue
		}
		// Tombstones: keep until any grant they killed is provably expired
		// everywhere (see replicaEntry.ReleasedAtMs).
		if !e.held && now.After(e.releasedAt.Add(t.maxTTL)) {
			delete(t.entries, k)
		}
	}
}
