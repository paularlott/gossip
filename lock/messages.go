package lock

import "github.com/paularlott/gossip"

// Token is a fencing token: an ordered pair of (election term, per-term counter).
//
// Ordering is lexicographic — term first, then counter. Because the term comes
// from leader election it increases on every leadership change, and the counter
// is monotonic within a single leader's tenure. That gives a strict total order
// across leaders with no dependence on wall-clock synchronisation.
//
// The token is also the version key for replica merging, which is why the
// ordering matters beyond fencing: an entry from a later term always dominates
// an entry from an earlier one, so a zombie leader's writes can never displace
// a legitimate successor's.
type Token struct {
	Term    uint64 `msgpack:"t" json:"t"`
	Counter uint64 `msgpack:"c" json:"c"`
}

// After reports whether t is strictly later than other.
func (t Token) After(other Token) bool {
	if t.Term != other.Term {
		return t.Term > other.Term
	}
	return t.Counter > other.Counter
}

// Before reports whether t is strictly earlier than other.
func (t Token) Before(other Token) bool { return other.After(t) }

// Equal reports whether the two tokens are identical.
func (t Token) Equal(other Token) bool {
	return t.Term == other.Term && t.Counter == other.Counter
}

// IsZero reports whether this is the zero token (never issued).
func (t Token) IsZero() bool { return t.Term == 0 && t.Counter == 0 }

// String renders the token as "term.counter".
func (t Token) String() string {
	return itoa(t.Term) + "." + itoa(t.Counter)
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// replicaEntry is one lock-table entry as it travels between nodes — in push
// acks, gossip fan-out, and recovery responses.
//
// An entry is an immutable fact about a mutation: a grant mints a token and an
// expiry; a release is the same token marked Held=false (a tombstone); an
// extend is the same token with a later expiry. Receivers merge by token:
//
//  1. Higher token wins (a later term or a later counter dominates).
//  2. Equal token: released beats held (a tombstone kills exactly its grant).
//  3. Equal token, both held: the later expiry wins (extends serialize at the
//     leader, so its deadlines are comparable without cross-node clocks).
type replicaEntry struct {
	Key string `msgpack:"k" json:"k"`

	// Owner is the node holding the grant. Zero on tombstones.
	Owner gossip.NodeID `msgpack:"o,omitempty" json:"o,omitempty"`

	Token Token `msgpack:"t" json:"t"`

	// Held is false once the lock has been released (tombstone).
	Held bool `msgpack:"h" json:"h"`

	// ExpiresAtMs is the grant's expiry (unix milliseconds), meaningful while Held.
	ExpiresAtMs int64 `msgpack:"e,omitempty" json:"e,omitempty"`

	// ReleasedAtMs is when the release happened (unix milliseconds), set on
	// tombstones. Tombstones are garbage collected at ReleasedAtMs + MaxTTL:
	// by then any copy of the grant they killed is provably expired, because
	// a grant's expiry is at most MaxTTL after it was made, which is at most
	// MaxTTL before its release.
	ReleasedAtMs int64 `msgpack:"r,omitempty" json:"r,omitempty"`
}

// acquireRequest asks the leader to grant a lock.
type acquireRequest struct {
	PoolName    string        `msgpack:"p" json:"p"`
	Key         string        `msgpack:"k" json:"k"`
	TTLMs       int64         `msgpack:"ttl" json:"ttl"`
	RequesterID gossip.NodeID `msgpack:"rid" json:"rid"`
}

// acquireResponse is the leader's verdict on an acquire.
type acquireResponse struct {
	Granted bool   `msgpack:"g" json:"g"`
	Token   Token  `msgpack:"t" json:"t"`
	Reason  string `msgpack:"r,omitempty" json:"r,omitempty"`
}

// releaseRequest asks the leader to release a held lock.
type releaseRequest struct {
	PoolName string `msgpack:"p" json:"p"`
	Key      string `msgpack:"k" json:"k"`
	Token    Token  `msgpack:"t" json:"t"`
}

// releaseResponse confirms a release.
type releaseResponse struct {
	Released bool   `msgpack:"r" json:"r"`
	Reason   string `msgpack:"rs,omitempty" json:"rs,omitempty"`
}

// extendRequest asks the leader to refresh a lock's TTL.
type extendRequest struct {
	PoolName string `msgpack:"p" json:"p"`
	Key      string `msgpack:"k" json:"k"`
	Token    Token  `msgpack:"t" json:"t"`
	TTLMs    int64  `msgpack:"ttl" json:"ttl"`
}

// extendResponse confirms an extend.
type extendResponse struct {
	Extended bool   `msgpack:"e" json:"e"`
	Reason   string `msgpack:"r,omitempty" json:"r,omitempty"`
}

// queryRequest asks the leader about a key's current state.
type queryRequest struct {
	PoolName string `msgpack:"p" json:"p"`
	Key      string `msgpack:"k" json:"k"`
}

// queryResponse reports a key's current state.
type queryResponse struct {
	Held   bool          `msgpack:"h" json:"h"`
	Owner  gossip.NodeID `msgpack:"o,omitempty" json:"o,omitempty"`
	Token  Token         `msgpack:"t,omitempty" json:"t,omitempty"`
	TTLMs  int64         `msgpack:"ttl,omitempty" json:"ttl,omitempty"`
	Reason string        `msgpack:"r,omitempty" json:"r,omitempty"`
}

// replicaPush carries entries from the leader to a replica and expects an
// ack, making the write durable. Used on the W-write path; batching lets a
// holder's death release all its locks in one round trip.
type replicaPush struct {
	PoolName string         `msgpack:"p" json:"p"`
	Entries  []replicaEntry `msgpack:"e" json:"e"`
}

// replicaAck confirms a replica applied a pushed entry.
type replicaAck struct {
	Applied bool `msgpack:"a" json:"a"`
}

// replicaGossipBroadcast carries a payload-sized batch of entries as
// fire-and-forget gossip, spreading entries beyond the W replicas so that
// recovery merges find copies even when the original replica set has thinned.
// Never load-bearing for correctness — the anti-entropy re-gossip heals lost
// deliveries.
type replicaGossipBroadcast struct {
	PoolName string         `msgpack:"p" json:"p"`
	Entries  []replicaEntry `msgpack:"e" json:"e"`
}

// stateQueryRequest asks a node for its current replica state. Sent by a newly
// elected leader to every live peer during recovery.
type stateQueryRequest struct {
	PoolName string `msgpack:"p" json:"p"`
}

// stateQueryResponse is a node's current replica view, including tombstones.
type stateQueryResponse struct {
	Entries []replicaEntry `msgpack:"e" json:"e"`
}
