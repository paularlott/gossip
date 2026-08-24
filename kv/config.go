package kv

import (
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/hlc"
)

// Message types for the KV protocol, allocated from the library's central
// reserved registry in packet.go (kv block: ReservedMsgsStart+20..+22).
const (
	kvWritePushMsg = gossip.KVWritePushMsg
	kvGossipMsg    = gossip.KVGossipMsg
	kvFullSyncMsg  = gossip.KVFullSyncMsg
)

// DefaultMaxValueSize caps a single value at 64KB: merge correctness never
// depends on value contents (they are opaque), and bounded values protect
// gossip packet sizes and the full-sync exchange.
const DefaultMaxValueSize = 64 * 1024

// Config holds configuration for a store.
type Config struct {
	// Name identifies this store. All nodes participating in the same logical
	// store must use the same name. Multiple stores with different names can
	// coexist on one cluster. Default: "default".
	Name string

	// WriteReplicas is W: the number of nodes — the writer included — that
	// must durably hold a write before Set acknowledges it to the caller.
	// Default: 2 (the writer plus one peer).
	//
	// Every write is pushed to peers in parallel and Set fails closed with
	// ErrWriteQuorum if the required acks do not arrive within
	// ReplicationTimeout. An acknowledged write therefore survives the loss
	// of any W-1 nodes at that instant; a fire-and-forget gossip fan-out then
	// spreads the entry further, raising steady-state replication towards the
	// whole group.
	//
	// The requirement is measured against the store's water mark — the group
	// size adopted under the shared baseline rules (growth after a stability
	// period; shrinkage only on graceful leaves, one-at-a-time after a long
	// dwell, or Store.Forget). A store that starts alone accepts local-only
	// writes; a store partitioned away from a group it has seen refuses
	// writes rather than silently splitting the store's history; a group
	// scaled down one node at a time (or drained gracefully) re-opens for
	// writes. Degrading applies to the adopted group size, never to peer
	// failures.
	WriteReplicas int

	// StabilityPeriod is how long the observed member count must hold steady
	// before it is trusted as the group's real size and may raise the water
	// mark. It must comfortably exceed failure detection, and it stops a
	// transient member from ratcheting the write bar permanently. Zero
	// derives it as 2 x Cluster.DeadNodeTimeout().
	StabilityPeriod time.Duration

	// ShrinkDwell is how long the group must sit at exactly one member below
	// the water mark before the mark follows it down. This lets a group
	// shrink without operator involvement while remaining split-safe (see
	// the baseline tracker). Any larger loss is left alone: it cannot be told apart from
	// a partition of that size. Zero derives it as 4 x
	// Cluster.DeadNodeTimeout(). Set AutoShrinkDisabled to turn it off.
	ShrinkDwell time.Duration

	// AutoShrinkDisabled stops the water mark from ever following the
	// observed count downward on its own. With this set, shrinking requires
	// a graceful leave or an explicit Store.Forget.
	AutoShrinkDisabled bool

	// Persister optionally backs the store with long-term storage (see
	// persist.go). Nil — the default — is memory-only. When set, note that
	// TombstoneRetention must exceed the worst-case node downtime: a node
	// restored from a snapshot taken before a delete, rejoining after the
	// cluster has reaped that delete's tombstone, would otherwise resurrect
	// the deleted key.
	Persister Persister

	// SnapshotWrites is how many table changes (local writes plus adopted
	// remote entries) may accumulate before a snapshot is saved. A store
	// with no changes never touches the Persister. Default: 1000.
	SnapshotWrites int

	// SnapshotInterval bounds the age of unsaved changes: a dirty store
	// snapshots at least this often, a clean one never. Default: 30s.
	SnapshotInterval time.Duration

	// ReplicationTimeout bounds one round of write acks. A write that cannot
	// collect its acks within this budget is retried once against replacement
	// peers and then refused. Default: 500ms.
	ReplicationTimeout time.Duration

	// SyncTimeout bounds the full-sync exchange used at catch-up — how long
	// the joining node waits for peers to answer before giving up on this
	// tick (later ticks retry). Default: 5s.
	SyncTimeout time.Duration

	// MaxValueSize is the hard cap on a single value in bytes. Larger writes
	// are rejected with ErrValueTooLarge before any replication is attempted.
	// Default: 64KB.
	MaxValueSize int

	// MaxKeys caps the number of live keys in this store; a Set that would
	// introduce a new key beyond the cap is rejected with ErrTooManyKeys.
	// Overwriting existing keys is always allowed, as is setting a key after
	// its slot is freed by a delete or expiry. 0 means unlimited.
	// Default: 0.
	MaxKeys int

	// MaxTTL is the longest per-entry TTL accepted by Set. A ttl of 0 or less
	// means "no expiry". Default: 24h.
	MaxTTL time.Duration

	// TombstoneRetention is how long a delete tombstone is kept before
	// garbage collection. A replica that missed a delete and reconnects after
	// the tombstone has been reaped could resurrect the deleted value, so
	// this must comfortably exceed the longest plausible partition between
	// group members. Anti-entropy re-spreads live tombstones continuously,
	// so correctly connected peers converge long before it elapses.
	// Default: 10m.
	TombstoneRetention time.Duration

	// Clock overrides the HLC clock used to version writes. Intended for
	// tests; production callers leave it nil.
	Clock *hlc.Clock

	// NowFn overrides the wall clock used for TTL expiry and GC. Intended
	// for tests; production callers leave it nil.
	NowFn func() time.Time
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Name:               "default",
		WriteReplicas:      2,
		ReplicationTimeout: 500 * time.Millisecond,
		SyncTimeout:        5 * time.Second,
		MaxValueSize:       DefaultMaxValueSize,
		MaxKeys:            0,
		MaxTTL:             24 * time.Hour,
		TombstoneRetention: 10 * time.Minute,
		SnapshotWrites:     1000,
		SnapshotInterval:   30 * time.Second,
	}
}

// validate returns a normalised copy, applying defaults for zero-value fields.
func (c *Config) validate() *Config {
	defaults := DefaultConfig()
	if c == nil {
		return defaults
	}

	out := *c

	if out.Name == "" {
		out.Name = defaults.Name
	}
	if out.WriteReplicas <= 0 {
		out.WriteReplicas = defaults.WriteReplicas
	}
	if out.ReplicationTimeout <= 0 {
		out.ReplicationTimeout = defaults.ReplicationTimeout
	}
	if out.SyncTimeout <= 0 {
		out.SyncTimeout = defaults.SyncTimeout
	}
	if out.MaxValueSize <= 0 {
		out.MaxValueSize = defaults.MaxValueSize
	}
	if out.MaxKeys < 0 {
		out.MaxKeys = 0
	}
	if out.MaxTTL <= 0 {
		out.MaxTTL = defaults.MaxTTL
	}
	if out.TombstoneRetention <= 0 {
		out.TombstoneRetention = defaults.TombstoneRetention
	}
	if out.SnapshotWrites <= 0 {
		out.SnapshotWrites = defaults.SnapshotWrites
	}
	if out.SnapshotInterval <= 0 {
		out.SnapshotInterval = defaults.SnapshotInterval
	}
	if out.Clock == nil {
		out.Clock = hlc.NewClock()
	}
	if out.NowFn == nil {
		out.NowFn = time.Now
	}

	return &out
}
