package kv

import "github.com/paularlott/gossip"

// Entry is one key-value record as it travels between nodes — in write
// pushes, gossip fan-out, and full-sync exchanges — and as it is stored in
// the local table. Values are opaque bytes; nothing in the protocol inspects
// them.
//
// An entry is an immutable fact about a mutation, ordered by Version (an HLC
// timestamp). Receivers merge by version:
//
//  1. Higher version wins (a strictly later write dominates).
//  2. Equal version: a tombstone beats a value — a delete cannot be
//     resurrected by a same-instant set.
//  3. Equal version, same kind: the higher Origin node ID wins. Two nodes
//     can only mint equal HLCs by counter coincidence, and this rule makes
//     the outcome identical on every replica regardless of arrival order.
type Entry struct {
	Key string `msgpack:"k" json:"k"`

	// Value is the stored bytes; nil on tombstones.
	Value []byte `msgpack:"v,omitempty" json:"v,omitempty"`

	// Version is the HLC timestamp of the write that produced this entry.
	Version uint64 `msgpack:"ts" json:"ts"`

	// Origin is the node that minted the Version, used only to break
	// equal-version ties deterministically.
	Origin gossip.NodeID `msgpack:"o,omitempty" json:"o,omitempty"`

	// Tombstone is true once the key has been deleted. Tombstones are kept
	// for TombstoneRetention so a stale replica's copy of the value cannot
	// resurrect after the delete.
	Tombstone bool `msgpack:"d" json:"d"`

	// ExpiresAtMs is the entry's expiry (unix milliseconds), 0 = no expiry.
	// Expiry is derived from this field by anyone holding the entry, so TTL
	// lapses need no tombstone to converge.
	ExpiresAtMs int64 `msgpack:"e,omitempty" json:"e,omitempty"`

	// DeletedAtMs is when the delete happened (unix milliseconds), set on
	// tombstones for garbage collection.
	DeletedAtMs int64 `msgpack:"r,omitempty" json:"r,omitempty"`
}

// writePush carries entries from a writer to a peer and expects an ack,
// making the write durable. Used on the W-write path.
type writePush struct {
	StoreName string   `msgpack:"p" json:"p"`
	Entries   []*Entry `msgpack:"e" json:"e"`
}

// writeAck confirms a peer applied a pushed batch.
type writeAck struct {
	Applied bool `msgpack:"a" json:"a"`
}

// gossipBroadcast carries a payload-sized batch of entries as fire-and-forget
// gossip, spreading entries beyond the W replicas so full-sync merges find
// copies even when the original replica set has thinned. Never load-bearing
// for correctness — the anti-entropy re-gossip heals lost deliveries.
type gossipBroadcast struct {
	StoreName string   `msgpack:"p" json:"p"`
	Entries   []*Entry `msgpack:"e" json:"e"`
}

// fullSyncRequest asks a member for its current view and simultaneously
// offers the requester's — the exchange is bidirectional, so both sides
// converge on the union rather than only the joiner learning.
type fullSyncRequest struct {
	StoreName string   `msgpack:"p" json:"p"`
	Entries   []*Entry `msgpack:"e" json:"e"`
}

// fullSyncResponse is a member's current view, live entries and live
// tombstones alike.
type fullSyncResponse struct {
	Entries []*Entry `msgpack:"e" json:"e"`
}
