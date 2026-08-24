package kv

// StoreSnapshot is the complete state of a store at a point in time, handed
// to a Persister to encode and store however it wants.
type StoreSnapshot struct {
	// Store is the store's Config.Name, for implementations that share
	// storage between several stores and want to partition by name.
	Store string

	// Entries is the store's current view: live entries and live tombstones
	// alike. The entries are copies; implementations may retain them.
	Entries []*Entry
}

// Persister optionally backs the store with long-term storage: a local file
// (written atomically — temp file, then rename), an object store, a database
// — anything that can hold a snapshot. When set, the store periodically
// snapshots itself and calls Save on a dedicated goroutine, so a slow
// Persister never blocks writes, reads, or the gossip machinery; a snapshot
// in flight also never piles up behind a slow one (later triggers skip while
// one runs). At construction the store calls Load and seeds its table from
// the result before its first peer sync, restoring state after a restart or
// a full group outage.
//
// The encoding is entirely the implementation's choice — JSON, msgpack, a
// database row per entry — with one correctness requirement: **every field
// of every Entry must survive the round trip unchanged.** Entries merge by
// (Version, Origin, Tombstone); an implementation that drops Origin makes
// equal-version tie-breaks nondeterministic after restore, one that drops
// DeletedAtMs breaks tombstone garbage collection, and one that drops
// tombstones outright resurrects every deleted key from storage. The Entry
// type carries json and msgpack tags, so a standard serialiser captures
// everything; hand-rolled encoders must be checked against the full field
// list.
//
// A Load error, a nil snapshot, or a snapshot whose Store name does not
// match logs a warning and starts the store empty — peers repopulate it
// via sync.
//
// Snapshots are periodic, not a write-ahead log: an acknowledged write is
// durable on WriteReplicas in-memory nodes, and the snapshot narrows the
// full-group-outage loss window to the writes since the last one.
type Persister interface {
	Save(snap *StoreSnapshot) error
	Load() (*StoreSnapshot, error)
}
