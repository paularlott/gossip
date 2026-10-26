# Partitioned Replication

Single-owner, majority-committed replication of partitioned data over gossip
membership. The data is split into a fixed number of partitions; each partition
has at most one owner at a time; every change to it goes through that owner,
which stores it on a majority of the **configured** cluster before it commits;
every node keeps a copy and serves reads from it, within a bounded staleness, or reads
through the owner when it needs the latest state (`Manager.Read`, linearizable). A node that
is not the owner takes part as a follower, and as a fallback: the nodes that could own a
partition are ranked, and the next rank takes over when the owner is lost.

What the data *is* belongs to the application. The package never looks inside a
write: it elects owners, orders and replicates opaque bytes, settles what a lost
owner left half done, catches nodes up, and tells the application when its local
copy may be read. The application supplies the durable side through the `Store`
interface (promises, a pending write, committed data, snapshots for catch-up).

For a complete, runnable application — a partitioned key-value server with a
durable file-backed `Store` and an HTTP API — see
[`examples/partition`](../examples/partition/). The protocol and the argument for its
safety are in [DESIGN.md](DESIGN.md).

## When to use it, and when to use kv

| | `partition` | [`kv`](../kv/) |
| --- | --- | --- |
| Writers | one owner per partition; others forward to it | any node, any time |
| Write is acknowledged | after a majority of the configured size holds it | after `WriteReplicas` nodes hold it |
| Conflicting writes | cannot happen: one owner orders them; the `Store` can refuse a change (compare-and-swap, uniqueness) | resolved afterwards, last writer wins by HLC |
| Read-modify-write | yes, `Store.Prepare` checks the change against the committed data | no |
| During a netsplit | the side with a majority of `ClusterSize` carries on; the other refuses writes | both sides accept writes (quorum permitting) and merge on heal |
| Lost owner | writes to its partitions wait for a takeover (seconds) | nothing to wait for |
| Cluster size | fixed, configured, never inferred from what is visible | adaptive |
| Data | the application's own durable `Store` | memory, optional snapshots |

Use `partition` for state where two writers disagreeing is a bug and where
losing an acknowledged write is not acceptable: accounts, configuration,
uniqueness constraints, allocation tables. Use `kv` for transient, cache-grade
state where availability and zone locality matter more than coordination. If one
node doing all the writes is enough, [`leader`](../leader/) is smaller still.

## Guarantees and limits

- **Majority durability.** A successful `Write` is committed after the owner and
  a majority of the configured cluster durably store it. Acknowledged writes
  survive minority failures, provided the application meets the
  [Store contract](#the-store-contract).
- **One ordered history per partition.** Changes go through one owner and are
  applied in sequence order. Different partitions can progress independently.
- **Read consistency is explicit.** `Manager.Read` is linearizable. Local reads
  guarded by `CanRead` may lag; `WaitApplied` gives local read-your-writes.
  See [Reads](#reads) for the tradeoffs.
- **Write errors describe the outcome.** `ErrRefused` and `ErrUnavailable` mean
  nothing was written. `ErrUnknown` means it may have committed: read to resolve
  the outcome or use application-level idempotency before retrying. The package
  does not retry a change once it may have been delivered.
- **Membership is configured.** Every node must use the same `ClusterSize`,
  `Partitions`, and data-set `ClusterID`. Majorities are never inferred from
  visible nodes. Different sizes or IDs do not exchange protocol state;
  `Write` can return `ErrSizeMismatch`. There is no membership-change protocol.
- **Availability requires a majority.** A minority cannot write or answer
  linearizable reads. Local reads stop after `StaleAfter` without fresh owner
  contact, or after lagging a reported commit point for that long. A deposed owner
  may serve local reads for up to `StaleAfter` after losing contact.
- **Deployment constraints.** Use one `Manager` per gossip cluster and a transport
  with streams. Changes and replies must fit the cluster's packet limit. Counters
  never wrap; exhaustion returns `ErrExhausted`.

The protocol rules and safety arguments are in [DESIGN.md](DESIGN.md).

### Recovery and operations

- **Choose the cluster size deliberately.** Three nodes tolerate one unavailable
  node; five tolerate two. Changing the configured size needs an application-level
  procedure that preserves quorum safety.
- **Lost or restored data must report `Store.Intact() == false`.** The node stays
  in resync mode until it has caught up from `ClusterSize − majority + 1` intact
  peers: two peers in a three-node cluster, three in a five-node cluster.
  Resyncing peers do not count. A node that incorrectly reports stale data intact
  can break the durability guarantee.
- **Insufficient recovery peers require an operator decision.** For one start,
  `ForceResync` allows recovery from at least one intact peer, accepting loss of
  writes held only by unavailable peers. It never recovers from resyncing peers
  alone. Bootstrapping empty nodes from one seeded or restored node also needs
  this explicit setting when the seed cannot meet the quorum; ensure that seed
  holds the intended data set. A peer's “forming” status never lowers the quorum.
- **Majority disk loss needs a backup.** The protocol cannot reconstruct data
  held only on lost disks; writes after the backup may be lost.
- **Divergence requires resync.** A node consistently ahead of its owner for
  three `OwnerTimeout`s withdraws from that partition until restart. Inspect
  `PartitionStatus(p).Diverged`, `Status().Divergences`, and the error log; restart
  with the data marked lost.

## Usage

```go
import "github.com/paularlott/gossip/partition"

// store implements partition.Store (see below). On every node of the cluster:
m, err := partition.New(cluster, partition.Config{
    ClusterSize: 3,        // required: the configured number of voting nodes
    ClusterID:   dataSetID, // the data set's name, the same on every node
    Store:       store,
})
if err != nil { /* bad configuration */ }
m.HandleOwnershipFunc(func(p int, owner gossip.NodeID, epoch partition.Epoch) { /* owner changed */ })
m.HandleResyncedFunc(func() { /* the node's lost data has been caught up: mark it trustworthy */ })
if err := m.Start(); err != nil { /* handlers could not be registered */ }
defer m.Stop()

// A write goes to the partition's owner wherever you are.
p := partition.PartitionOf("user:42", 64) // 1 + fnv64a(key) % 63; partition 0 is left for global data
res, err := m.Write(ctx, p, change)
var refused *partition.RefusedError
switch {
case err == nil:                                    // committed on a majority
    // res.Pos is the write's position, res.Reply what Store.Prepare returned for you
    err = m.WaitApplied(ctx, p, res.Pos)            // optional: until this node has applied it too
case errors.As(err, &refused):                      // the Store said no (refused.Code, refused.Msg); nothing was written
    // A stale expectation (compare-and-swap)? Let the local copy reach what the owner decided against, then re-read it.
    err = m.WaitApplied(ctx, p, refused.Pos)
case errors.Is(err, partition.ErrUnknown):          // may or may not have happened: read before retrying
case errors.Is(err, partition.ErrUnavailable):      // no owner or no majority right now; nothing was written
}

// A local read is your own copy, if it may be trusted: cheap, possibly a message behind.
if m.CanRead(p) {
    v := store.Get(...)   // your Store's own read method
}

// A read through the owner is the latest committed state, on any node: it runs Store.Read(p, req) on the owner,
// which confirms with a majority that it still owns p first.
answer, err := m.Read(ctx, p, req)
switch {
case err == nil:                                    // answer is what Store.Read returned
case errors.As(err, &refused):                      // Store.Read refused the request (refused.Code, refused.Msg)
case errors.Is(err, partition.ErrUnavailable):      // no owner reachable or no majority confirmed it, within WriteTimeout
}

m.Ready()      // caught up, so a candidate to own partitions (while eligible)
m.Resyncing()  // started without intact data and still catching up
m.IsOwner(p)   // this node holds p
m.Owner(p)     // who this node believes holds p
m.SetEligible(false)  // e.g. the clock guard fired: lead nothing (steps down at once), keep following
st, _ := m.PartitionStatus(p) // owner, epoch, positions, lag, catching up, diverged, can read: readiness and metrics
m.Status()     // the same for every partition, and the node's own state
```

`Start` must be called on every node with the same `Partitions`, `ClusterSize`,
and `ClusterID`. The cluster's transport must support streams (the socket and HTTP
transports do), because catch-up uses them. Only one `Manager` can run on a
cluster. Call `Start` after the cluster has been created and started; the node
joins the protocol by gossiping its metadata, so it can be called before or after
`Join`.

**The transport's deadline.** A node that is not the owner sends a change to the owner and waits for
the answer for up to `WriteTimeout` (less if the caller's context ends sooner). The cluster's transport
must let it wait that long: gossip's `Config.TCPDeadline` (2s by default) must be at least
`m.MinTransportDeadline()` (a little more than `WriteTimeout`, 5s by default). With a shorter transport
deadline a change that takes longer than it (the owner waited for the partition, or for a slow follower)
is reported `ErrUnknown` although the owner may still commit it. Either raise `TCPDeadline` (it also
bounds the cluster's own pings, so a node that hangs is suspected later) or lower `WriteTimeout`, and
check it at start:

```go
if gossipCfg.TCPDeadline < m.MinTransportDeadline() {
    return fmt.Errorf("TCPDeadline %v is below the partition manager's %v", gossipCfg.TCPDeadline, m.MinTransportDeadline())
}
```

Custom transports must distinguish requests that were never sent from uncertain
outcomes: an error wrapping `*net.OpError` with `Op == "dial"` is safe to retry.
Other transport errors may produce `ErrUnknown`. See
[write routing](DESIGN.md#5-writes) for the protocol rules.

A forwarded change or read carries its caller's deadline (as an absolute time, so the nodes' clocks must
agree to well within it). The owner starts it only if at least `2 × HeartbeatInterval` (at most half of
`WriteTimeout`) is left when it holds the partition, and otherwise refuses it with nothing done; the node
that forwards it does not send it at all with less than that left. Either way the caller gets
`ErrUnavailable`, not `ErrUnknown`. So give `Write` and `Read` contexts with more time than that; on the
owner itself a shorter context is fine. Once the owner has begun a write, it finishes it on a
`WriteTimeout` of its own, whatever became of the caller: the write is committed, or the owner steps
down and the next owner settles it. A `Write` on the owner itself returns only then.

**Eligibility.** `SetEligible(false)` takes the node out of ownership without stopping it: it steps down
from everything it leads (telling the others, so successors take over at once), publishes itself as not
ready so that nobody ranks it, and neither takes over nor claims; it still follows, acknowledges writes,
promises, serves catch-up and state, forwards changes and reads its own copy. Use it for whatever makes
the node unfit to lead for a while: a clock guard that finds the clock unreliable, a configuration it
refused, a drain before maintenance. `SetEligible(true)` undoes it.

**Status.** `PartitionStatus(p)` reports what this node knows of `p`: the owner and its epoch, whether it
leads, its committed position and the highest one the owner reported, how long it has trailed that (`Lag`),
whether a catch-up is running, whether `p` diverged here (see [Recovery and operations](#recovery-and-operations)) and `CanRead`.
`Status()` returns it for every partition with `Ready`, `Resyncing`, `Eligible` and the count of
divergences. A readiness check can require, say, `CanRead` on every partition; metrics can export the
lag and the divergences. Catch-ups that keep failing are logged at warn level (at most every 10s per
partition).

`Stop` steps down from every partition the node owns and tells the others, so
successors take over at once; it does not leave the cluster.

### Reads

Choose consistency per read:

- **A local read** (`CanRead(p)` and then the `Store`'s own read method) is answered from this node's copy
  without a message. It is as fresh as the node's last commit notice or heartbeat: after a `Write` on another
  node it can be a message behind (use `WaitApplied` for your own writes), and it can show a state that a
  newer write has already replaced. Use it for what tolerates that: display, listings, caches, lookups whose
  answer a later check can correct.
- **`Manager.Read(ctx, p, req)`** is linearizable: it returns at least every write committed before it was
  called, on any node, and never a state from before a write that had returned. Use it to decide on the latest
  state (a uniqueness or last-administrator check before a change, a lookup that a following change depends
  on); it is also the right read after an `ErrUnknown`, to find out what happened. It has a cost (below): a round
  trip from the owner to a majority of the cluster, shared by the reads that arrive while it runs, plus the hop to
  the owner when the caller is not the owner; and it needs a majority reachable, where a local read does not
  (`ErrUnavailable` otherwise). It does not take a decision for you: a read followed by a write is two steps,
  and another write can come between them. Put the check in `Store.Prepare`, which runs on the owner against
  the committed data and refuses with a code, when the decision and the change must be atomic; use `Read` when
  the decision is only the application's to make.

`Store.Read(p, req)` is the application's: it answers `req` (any bytes) from the committed data of `p`, changes
nothing, and returns the answer as bytes (or a refusal, made with `partition.Refuse`, which reaches the caller of
`Manager.Read` as a `*RefusedError` with its code, as for a change). It runs on the owner without the
partition's mutex, so like `State` and `Export` it must be safe to call while a write is committed.

**Results and refusals.** `Write` returns a `Result{Pos, Reply}`. `Pos` is the
committed write's position; `Reply` is whatever the owner's `Store.Prepare`
returned for the caller (a new version, a generated id), carried back unchanged
even when the caller is not the owner. On any error the `Result` is zero. A
refusal is a `*RefusedError{Code, Msg, Pos}` (`errors.Is(err, ErrRefused)` is true
too): the `Store` makes one with `partition.Refuse(code, msg)`, and the code
survives the trip from the owner, so the application never has to parse messages.
`Pos` is the owner's committed position when it refused, the state the refusal was
decided against. After `ErrRefused` for a stale expectation (a compare-and-swap whose
version had moved), `WaitApplied(ctx, p, refused.Pos)` before re-reading the local
copy: otherwise a node that is a message behind reads the same stale version again
and builds the retry on it.

## The Store contract

The application supplies persistence and data semantics through
[`Store`](store.go), which is the authoritative interface contract. Methods for
separate partitions run concurrently; `State`, `Read`, and `Export` may also run
while the same partition changes. Return consistent committed snapshots and
never expose pending writes through reads or exports.

| Method | Responsibility |
| --- | --- |
| `State(p)` | Return `Promised`, `Committed`, and `Pending` cheaply, reflecting completed calls. Returned writes are immutable to both parties. |
| `SetPromised(p, e)` | Durably record the promise before returning. |
| `SetPending(p, w)` | Durably record or clear the pending write before returning; this is the replication acknowledgement. |
| `Prepare(p, change)` | Validate against committed data and return deterministic write bytes plus an optional reply. Runs serially on the owner; must not change data. Refuse with `partition.Refuse(code, msg)`. |
| `Commit(w)` | Atomically apply the bytes, advance `Committed`, and clear the matching pending write. `w.Prev` is this node's own committed position. |
| `Read(p, req)` | Answer from committed data without changing it. A refusal reaches `Manager.Read` as a `*RefusedError`; other failures produce `ErrUnavailable`. |
| `Export(p, after)` | Return a consistent snapshot of committed changes after sequence `after` (0: all data), with its position. Return `ErrPurged` if the history is unavailable. |
| `Import(p, r, upTo, full)` | Apply the export, replace data when `full`, advance `Committed` only after complete input, and clear pending writes through `upTo.Seq`. Truncated input must fail. See `Store.Import` for the rules for batched imports. |
| `Intact()` | Report whether this node's stored state is trustworthy. Read once at `Start`; `HandleResyncedFunc` signals successful recovery. |

**Durability ordering is essential.** `SetPromised` and `SetPending` must be
crash-durable before returning, including all earlier `Commit` and `Import`
results. `Commit` and `Import` need not themselves flush before returning, but
must meet their atomicity requirements. A store that separates committed data
from protocol state must flush the data first. The reason for this ordering is
explained in the [durability safety argument](DESIGN.md#9-the-durability-contract-and-the-safety-argument).

Implementation rules:

- Key positions and export history by **`Seq`**, not the full `Pos`; the same
  write may have different epoch labels across nodes. The protocol explanation
  is in [DESIGN.md §6](DESIGN.md#6-followers-comparing-by-seq-and-catch-up).
- Pending writes follow `Committed.Seq` by one. A store may lazily treat a stored
  pending write at or below the committed sequence as absent.
- Retain tombstones for incremental exports, or return `ErrPurged` when that
  history is no longer available. Export formats must detect truncated input.
- Generate versions, timestamps, and IDs in `Prepare`, so every replica applies
  the same bytes. Return caller-specific results through `Result.Reply`.
- Mark recovered storage trustworthy in `HandleResyncedFunc`, registered before
  `Start`, so a later restart does not unnecessarily resync.

For a database-backed store, keep protocol state and application data in the
same database. Apply a commit and its position in one transaction; use durable
transactions for promises and pending writes, ensuring they also flush earlier
commits. Cache state for frequent `State` calls, and index records and tombstones
by the sequence that last changed them for incremental exports.

[`store_test.go`](store_test.go) provides an in-memory store with fault hooks;
[`examples/partition`](../examples/partition/) provides a file-backed store.

## Configuration

| Field | Default | |
| --- | --- | --- |
| `Partitions` | 64 | Number of partitions, `0 .. Partitions-1`. The same on every node. |
| `ClusterSize` | — (required, >= 1) | The configured number of voting nodes. The same on every node. |
| `ClusterID` | empty string | Data-set identity, chosen once and shared by every node. Empty is a valid identity. |
| `HeartbeatInterval` | 500ms | How often owners announce their partitions and the ownership loop runs. Also: a change that finds no owner yet is retried after a quarter of it. |
| `OwnerTimeout` | 3 × `HeartbeatInterval` | How long an owner may be silent before it is considered gone; also how long a node refuses to promise to a challenger of an owner it has heard from. |
| `BackoffMax` | `OwnerTimeout` | A node that finds it should own a partition waits a random time in `[0, BackoffMax)` before proposing. |
| `RankDelay` | 2 × `HeartbeatInterval` | The node of rank *r* takes over a partition that has had no owner for *r* × `RankDelay` (after the random backoff). Rank 0 acts at once. |
| `TakeoverBackoffMax` | 60s | After a takeover that did not complete a node leaves the partition alone for `OwnerTimeout`, doubling with each consecutive failure up to this. |
| `MaxConcurrentChanges` | 256 | Forwarded changes served at once. Excess requests receive "busy" and are retried with backoff. |
| `MaxChangesPerPartition` | 32 | Forwarded changes of one partition that its owner holds at once (running, or waiting for the partition, which runs one at a time), so that one hot partition cannot take all of `MaxConcurrentChanges`; more are answered "busy" as above. |
| `MaxConcurrentCatchUps` | 4 | Catch-ups a node runs at once in the background (followers that fell behind, the startup synchronisation); the others wait for a slot. A takeover's own catch-up does not wait. |
| `MaxConcurrentRequests` | 1024 | The other requests a node serves at once (promise, write, claim, state and heartbeat), which share this limit. Beyond it a request is refused at once with its "not OK" reply: a promise or write is not acknowledged, a heartbeat does not count as delivered, a state request is not answered. |
| `MaxConcurrentReads` | 1024 | Reads forwarded from other nodes that a node serves at once as owner, each in a goroutine of the `Manager` while the owner confirms its leadership; more are answered "busy" and retried as changes are. |
| `ReadyHoldDown` | 2 × `OwnerTimeout` | How long a caught-up node waits before it declares itself ready and becomes a candidate for ownership. |
| `WriteTimeout` | 5s | Bounds one `Write`, including waiting for a takeover, and one `Read`; and, separately, one replication on the owner, from its start. The transport's `TCPDeadline` must be at least `MinTransportDeadline()`, a little more than this (see "The transport's deadline"). |
| `StaleAfter` | `OwnerTimeout` | `CanRead`'s bound: how old the last sign of a live owner may be, and how long a follower may trail the committed position the owner reported. |
| `ForceResync` | false | For one start of a node that lost its data, when too few intact nodes are left to resync from: see [Recovery and operations](#recovery-and-operations). |
| `Established`, `SetEstablished` | nil | Optional durable participation marker. Informational for recovery; it never lowers the quorum. See [`Config`](config.go) for callback requirements. |
| `Store` | — (required) | The application's storage. |
| `Logger` | the cluster's | Log output. |

Takeover after a crash takes about `OwnerTimeout` (the owner has to go quiet) plus `RankDelay`
for the next rank, plus the random backoff in `[0, BackoffMax)` and a promise round; it does not
wait for the cluster's failure detector to drop the lost node from the membership.
See [ownership](DESIGN.md#3-ownership) for the timing rules and [Cost](#cost) for measurements.

## Protocol reference

[DESIGN.md](DESIGN.md) describes the state machine and its safety arguments:

- [Ownership and ranked fallback](DESIGN.md#3-ownership)
- [Takeover and pending-write settlement](DESIGN.md#4-takeover)
- [Write routing and replication](DESIGN.md#5-writes)
- [Follower state and catch-up](DESIGN.md#6-followers-comparing-by-seq-and-catch-up)
- [Startup and lost-disk recovery](DESIGN.md#7-startup-readiness-and-resync)
- [Linearizable read confirmation](DESIGN.md#8-reads)
- [Durability and quorum safety](DESIGN.md#9-the-durability-contract-and-the-safety-argument)

## Cost

Benchmarks are in `bench_test.go`; they run real clusters on localhost over the
socket transport with the in-memory store. Run them with

```shell
go test -run '^$' -bench 'BenchmarkPartition' -benchtime=1s ./partition/
go test -run '^$' -bench 'BenchmarkPartitionTakeover' -benchtime=3x ./partition/
```

Machine: Apple M5 Pro (15 cores), macOS, Go 1.27.3 darwin/arm64, three or five nodes
in one process talking over localhost sockets, 16 partitions, 64 byte changes, the
in-memory `Store` (so no disk cost unless a row says so). Treat the figures as orders
of magnitude, not as a specification. "p50" and "p99" are per-write latencies;
"writes/s" is aggregate.

**One write at a time** (3 nodes, 100ms heartbeat): a write that lands on the
partition's owner, and one that lands elsewhere and is sent to the owner first. The
owner's figure is also the sequential throughput of a single partition:

| | p50 | p99 | sequential throughput |
| --- | --- | --- | --- |
| lands on the owner | 56µs | 101µs | about 17,300 writes/s |
| lands on another node | 94µs | 126µs | about 10,500 writes/s |

(The figures include the commit notice the owner sends every follower after each commit.)

**Network latency** (3 nodes). `internal/faultnet` delays the request leg of an
exchange, so "1ms" means every request between two nodes takes 1ms longer, and a
request/response pays it once. A write on the owner waits for one round of
requests to the followers; a write elsewhere also pays the request to the owner,
so it costs two delays. 64 concurrent writers are spread over the nodes and over
all partitions:

| delay per request | owner p50 | non-owner p50 | 64 writers: writes/s | p50 | p99 |
| --- | --- | --- | --- | --- | --- |
| none | 56µs | 94µs | 25,500 | 2.0ms | 8.0ms |
| 1ms | 1.5ms | 2.9ms | 11,100 | 4.6ms | 19ms |
| 5ms | 6.4ms | 12.6ms | 2,100 | 24ms | 89ms |

**Reads** (3 nodes, 100ms heartbeat, `BenchmarkPartitionRead`; the request is the `Store`'s committed
sequence number, so the figures are the protocol's cost). A linearizable read is one round of heartbeat
requests to the followers (no durable `SetPending`, no `Commit`), plus the hop to the owner when it lands elsewhere. With 64 readers, reads
of a partition share rounds, so the owners run about half a round per read:

| | p50 | p99 | |
| --- | --- | --- | --- |
| lands on the owner | 59µs | 137µs | about 15,900 reads/s one at a time |
| lands on another node | 101µs | 148µs | about 9,600 reads/s one at a time |
| owner, 1ms per request | 1.5ms | 1.6ms | one delay: the round |
| other node, 1ms per request | 2.9ms | 3.0ms | two delays: the hop and the round |
| 64 readers over the 16 partitions | 1.9ms | 3.8ms | 29,500 reads/s, 0.54 rounds per read |

A local read costs nothing on the network; `Read` is for the decisions that cannot use a possibly lagging copy.

**Concurrent writers** (3 nodes, no delay), spread over all partitions, or all on one
partition (which the owner runs one write at a time):

| heartbeat | 8 writers, spread | 64 writers, spread | 8 writers, one partition |
| --- | --- | --- | --- |
| 100ms | 22,200/s (p99 0.8ms) | 25,400/s (p99 8.0ms) | 14,500/s |
| 500ms (the default) | 22,200/s (p99 0.8ms) | 25,500/s (p99 8.1ms) | 14,500/s |

The request handlers reply later and run in goroutines of the `Manager`, so no handler holds one of the cluster's
incoming workers while it waits for the followers, and the limits can be high: `MaxConcurrentChanges` is 256 and
`MaxConcurrentRequests` 1024. The cost is a goroutine hop per request, a few microseconds per write. With
`NumIncomingWorkers` at 2, 64 writers all commit (`TestManyWritersWithTwoIncomingWorkers`).

**3 nodes versus 5** (a majority of 2 of 3, or 3 of 5; 100ms heartbeat, no delay):

| | owner p50 | non-owner p50 | 64 writers |
| --- | --- | --- | --- |
| 3 nodes | 55µs | 94µs | 25,600/s |
| 5 nodes | 98µs | 120µs | 14,200/s |

**Cost of a durable write.** The benchmark's `Store` can sleep for every `SetPromised`
and `SetPending` (what a real `Store` pays for an fsync) and, separately, for every
`Commit` and `Import`. A write waits for the slower of the owner's `SetPending` and the
followers' (all of them run at the same time), not for their sum. Since a `Commit` need
not be durable, it adds nothing: a follower commits a write when the owner's commit
notice arrives, not as part of the next write. "durable commit" is what the same
`Store` costs if its `Commit` fsyncs as well:

| cost per fsync | | owner p50 | 64 writers |
| --- | --- | --- | --- |
| none | | 55µs | 25,200/s |
| 1ms | lazy `Commit` | 1.3ms | 12,100/s |
| 1ms | durable `Commit` | 3.9ms | 3,900/s |
| 4ms (a full fsync on a laptop SSD is about 3ms) | lazy `Commit` | 5.1ms | 2,900/s |
| 4ms | durable `Commit` | 14.7ms | 920/s |

So the `Store`'s durable write counts once, and throughput comes from partitions working in
parallel. A `Store` that batches durable writes across partitions (group commit) gains most.

**Takeover** (3 nodes; time until writes through a surviving node to every
partition of the lost owner succeed again; probes every 25ms; 16 partitions, of
which the lost node owned about a third). "crash" cuts the node off without announcing
anything, "stop" is `Manager.Stop`, which hands the partitions back:

| heartbeat | | first partition writable | all of them |
| --- | --- | --- | --- |
| 100ms | crash | 0.55s | 0.76s |
| 100ms | stop | 0.05s | 0.28s |
| 500ms | crash | 2.5s | 3.7s |
| 500ms | stop | 0.40s | 1.3s |

After a crash the next-ranked node takes over once the partition has been ownerless for
`RankDelay` (the owner was silent for `OwnerTimeout` first), after the random backoff in
`[0, BackoffMax)` per partition and a promise round: at most about `OwnerTimeout +
RankDelay + BackoffMax` plus a heartbeat interval, which is under a second at a 100ms
heartbeat and up to about 4.5s at the default 500ms. It does not wait for the cluster's
failure detector. The last partition takes longer than the first because of the random
backoff. A `Stop` skips all of it.
