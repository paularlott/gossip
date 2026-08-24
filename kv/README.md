# Replicated KV Store

A leaderless, eventually-consistent replicated key-value store. Every node
holds a full replica and serves local reads; writes are versioned with an HLC
timestamp and made durable on `WriteReplicas` (W) nodes before `Set`
acknowledges, then spread across the whole scope by gossip and an
anti-entropy sweep. Conflicting writes are resolved identically on every
replica by deterministic last-writer-wins rules, with tombstoned deletes that
cannot be resurrected by stale replicas.

## Design

```
Set / Delete  →  apply locally under a fresh HLC version
             →  push to W-1 peers in parallel, wait for acks          (durable)
             →  fire-and-forget fan-out to the scope's members        (spread)

anti-entropy rides the cluster's gossip event (the library's own
state exchange, always running): catch-up until synced, re-gossip after;
membership events trigger immediate total sweeps on top

restarted / late-joining node ──► bidirectional full sync: it offers its
                                  snapshot and merges the answer
```

There is no leader, no coordinator, and no per-key ownership: any member may
write anything at any time. That is the point — the store targets transient,
cache-grade state (knot's plugin store) where availability and zone-locality
matter more than strong co-ordination. The cost is spelled out below: no
atomic read-modify-write. Callers that need co-ordination should take a lock
from the `lock` package first.

### Merge rules

Every entry carries `(Version, Origin, Tombstone)`. Merges are commutative
and identical on every replica:

1. **Higher version wins** — Version is an HLC timestamp, so a write minted
   after another node's write dominates it regardless of arrival order (the
   table witnesses every incoming version into its clock before minting).
2. **Equal version: tombstone beats value** — a delete cannot be resurrected
   by a same-instant set.
3. **Equal version, same kind: higher Origin node ID wins** — two nodes can
   only mint equal HLCs by counter coincidence; this rule makes the outcome
   identical everywhere regardless of arrival order.

Values are opaque, capped bytes (`MaxValueSize`, default 64KB). Merge
correctness never depends on their contents.

### Deletes and tombstones

A delete is a write: it mints a fresh version and must be durable on W nodes
like any Set. Tombstones are retained for `TombstoneRetention` (default 10m)
so a replica that missed the delete — down, or partitioned — cannot
resurrect the value when it syncs. Retention must comfortably exceed the
longest plausible partition; anti-entropy re-spreads live tombstones
continuously, so correctly connected peers converge long before it elapses.

TTL expiry needs no tombstone: `ExpiresAtMs` travels inside the entry, so
every holder derives the same expiry at the same instant. Expired entries are
reaped after a skew-absorbing grace.

### Write durability and the adaptive water mark

`Set` acknowledges after the local write plus `min(W-1, water-1)` peer acks
within `ReplicationTimeout`, retried once against replacement peers. The
water mark is the adopted group size the bar is measured against — the same
shared tracker as the leader package's election baseline (internal/baseline),
fed by membership events rather than polling — and it moves only for
defensible reasons:

- **Growth is trusted after it settles**: the observed member count must hold
  steady for `StabilityPeriod` (default 2 × DeadNodeTimeout) before it raises
  the mark, so a transient member cannot ratchet the bar permanently.
- **A store that starts alone accepts writes** — its own copy is the whole
  replica set. This is the bootstrap allowance.
- **Graceful `Leave()` drops the mark immediately**: a departure broadcast is
  a positive signal from outside the failure domain, which a partitioned node
  cannot fake.
- **A silent loss (crash or partition) never lowers the bar directly.** The
  mark follows down only when exactly one member is missing and it has stayed
  missing for `ShrinkDwell` (default 4 × DeadNodeTimeout) — one step at a
  time, which is what keeps it split-safe. A larger loss is indistinguishable
  from a partition of that size, so the mark holds and the store refuses
  writes (`ErrWriteQuorum`) rather than silently splitting its history.
  Shrinking past such a loss needs graceful drains or `Store.Forget`.
- **A failed Set has not taken effect**: the local copy is compensated with a
  tombstone that dominates the half-written value everywhere. Deletes are the
  exception in kind — a delete that falls short of quorum *stands* (the safe
  direction), so `ErrWriteQuorum` from Delete means "not confirmed durable,
  will converge via anti-entropy", never "undone".

The net behaviour for a zone: lose one node of three and writes continue
(quorum still reachable); drain or scale down one node at a time and the bar
follows within a dwell; get partitioned away from the group and writes fail
closed until the partition heals or the group is genuinely gone.

### Optional persistence (snapshots)

Supply a `Persister` — anything that can hold an opaque blob (an atomically
written local file, an object store, a database) — and the store periodically
snapshots itself to it:

- **Debounced saves**: a snapshot is taken after `SnapshotWrites` table
  changes (local writes *plus* adopted remote entries, so pure replicas
  persist their view too) or `SnapshotInterval` of unsaved changes,
  whichever comes first. **A clean store never touches the Persister** — no
  write activity means no disk activity.
- **Saves run on their own goroutine**, single-flighted: a slow Persister
  skips later triggers rather than piling up, and nothing on the write path,
  read path, or gossip machinery ever waits on storage.
- **At startup the store seeds from the Persister before its first peer
  sync**, so the restore composes with the merge rules: a stale snapshot
  loses per-key to fresher cluster state, a snapshot newer than any peer
  propagates back out, and a full group outage restores to the freshest
  state any node had persisted — the union, resolved by version.
- **`Store.Snapshot()`** forces an immediate synchronous save (a no-op in
  memory-only mode); **`Close`** flushes a dirty store on the way down. A
  failed save restores the dirty count, so no change is silently considered
  persisted and the next trigger retries.

The blob is opaque and library-owned: a versioned binary encoding of the
entries, live tombstones included — a snapshot without tombstones would
resurrect every deleted key from disk on restore. Persisters that want an
inspectable format can decode and re-encode it with `DecodeSnapshot` /
`EncodeSnapshot` (see `examples/kvpersist` for a JSON-dumping Persister). A Load error or an
undecodable blob logs and starts empty; peers repopulate via sync.

**Not a write-ahead log.** An acknowledged write is durable on
`WriteReplicas` in-memory nodes; the snapshot narrows the full-outage loss
window to the writes since the last one.

**When persisting, raise `TombstoneRetention` beyond the worst-case node
downtime.** A node restored from a snapshot taken before a delete, rejoining
after the cluster has reaped that delete's tombstone, would otherwise
resurrect the deleted key — the same GC-grace tradeoff as any
tombstone-based store.

### Drivers: events plus the cluster's gossip cadence

The store owns no timers and polls nothing. Membership events (node state
and metadata changes) refresh the member view and feed the baseline
immediately; the cluster's own gossip event — which the library runs
continuously for its state exchange regardless of store activity — advances
the baseline's stability/dwell windows, reaps the table, checks the
debounced snapshot triggers, and runs the paced anti-entropy sweep. That
gossip cadence is the eventual-consistency backbone: fire-and-forget
deliveries lost to sub-detection partitions (no events, no send errors
exist) are healed by the next gossip round, exactly as in the lock package.

### Residual consistency semantics (documented, by design)

- **No atomic read-modify-write.** Concurrent writers to one key race and one
  wins; nothing corrupts. Per-writer key naming avoids most races;
  `Cluster.LockResource`-style locks cover the rest.
- **A netsplit heals by LWW**: both sides accept writes while partitioned
  (each side's quorum permitting) and the higher version wins per key on
  heal.
- **The store is memory-only.** It survives node restarts and crashes via
  replica copies and the join-time full sync; it is not a database. A full
  group outage loses the store — acceptable for transient data by design.

## Scope: cluster or group

A store is scoped by its `Membership`:

- `ClusterMembership{Cluster: c}` — every alive node of the cluster.
- `GroupMembership{Group: g}` — the members of a `NodeGroup`, i.e. the nodes
  whose metadata matches criteria such as `zone=eu-west`. Zone groups are
  small and co-located, so writes fan out to all members directly: minimal
  latency, and the whole zone holds every key.

Gossip, pushes, and full syncs are addressed only to the store's members — a
group-scoped store never leaks entries outside its group. Multiple stores
with different `Config.Name` coexist on one cluster.

**One name, one scope**: stores with the same name on the same cluster are
dispatched to by name, so do not reuse a name across different scopes on one
cluster (e.g. a cluster-wide `cache` and a group-scoped `cache`) — the
group's members would apply the cluster-wide store's entries.

## Usage

```go
cluster, _ := gossip.NewCluster(cfg)
cluster.Start()

// Cluster-wide store:
store := kv.NewStore(cluster, kv.ClusterMembership{Cluster: cluster}, kv.DefaultConfig())
defer store.Close()

// Or, scoped to a zone:
group := gossip.NewNodeGroup(cluster, map[string]string{"zone": "eu-west"}, nil)
store = kv.NewStore(cluster, kv.GroupMembership{Group: group}, &kv.Config{
    Name:          "plugin-cache",
    WriteReplicas: 2, // writer + 1 peer ack; a 3-node zone tolerates one down
})

_ = store.Set("p:demo:u:alice:theme", []byte("dark"), time.Minute)
if v, ok := store.Get("p:demo:u:alice:theme"); ok { /* ... */ }
_ = store.Delete("p:demo:u:alice:theme")
n, _ := store.DeletePrefix("p:demo:u:alice:") // bulk delete: scan + one tombstone batch
keys := store.Keys("p:demo:")
```

Key layout conventions (`p:<plugin>:u:<user>:<key>`) belong to the caller;
the store is a generic byte KV and deliberately knows nothing about users.

## API notes

| Call | Behaviour |
|---|---|
| `Get(key)` | Local read; deleted and TTL-expired keys read as missing. |
| `Set(key, value, ttl)` | Durable on W nodes before returning; `ttl <= 0` = no expiry. Fails closed with `ErrWriteQuorum` if acks fall short (then the write has not taken effect). |
| `Delete(key)` | Idempotent tombstone write; durable like Set. On quorum failure the delete stands and converges. |
| `DeletePrefix(prefix)` | Scans live keys under the prefix, replicates one tombstone batch; returns the count. |
| `Keys(prefix)` / `Len()` | Sorted live keys / live count — local view. |
| `Sync()` | One bidirectional full-sync exchange; also runs automatically on membership events and the gossip cadence. |
| `Synced()` | Whether an initial catch-up has completed (or there are no peers). |

Validation errors — `ErrKeyEmpty`, `ErrValueTooLarge`, `ErrTTLOutOfRange`,
`ErrTooManyKeys` (per-store `MaxKeys` cap), `ErrStoreClosed` — are returned
before any replication is attempted.

## Config defaults

| Field | Default | Notes |
|---|---|---|
| `Name` | `"default"` | Same name on every participating node. |
| `WriteReplicas` | `2` | Writer + 1 peer ack; measured against the water mark. |
| `StabilityPeriod` | `2 x DeadNodeTimeout` | How long growth must hold steady before raising the mark. |
| `ShrinkDwell` | `4 x DeadNodeTimeout` | How long exactly-one-missing must persist before the mark follows down. |
| `AutoShrinkDisabled` | `false` | Shrink only via graceful leave or `Store.Forget`. |
| `Persister` | `nil` | Optional long-term storage; nil is memory-only. |
| `SnapshotWrites` | `1000` | Table changes before a snapshot is saved. |
| `SnapshotInterval` | `30s` | Max age of unsaved changes; clean stores never save. |
| `ReplicationTimeout` | `500ms` | Per ack round; one retry against replacements. |
| `SyncTimeout` | `5s` | Bounds each full-sync exchange attempt. |
| `MaxValueSize` | `64KB` | Hard cap, enforced before replication. |
| `MaxKeys` | `0` (unlimited) | Live-key cap per store. |
| `MaxTTL` | `24h` | Upper bound on per-entry TTLs. |
| `TombstoneRetention` | `10m` | Exceed the longest plausible partition. |
