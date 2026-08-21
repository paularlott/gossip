# Distributed Locks

Advisory distributed locks backed by leader election. A single elected leader
serialises every grant, release, and extend; each mutation is made durable on
`WriteReplicas` (W) nodes before it is acknowledged and spread to the rest of
the group by gossip. A newly elected leader recovers the table by merging
replica state from its peers — in seconds, not by waiting out a TTL window —
which is what makes failover fast even when locks legitimately run for minutes.

## Design

```
Acquire / Release / Extend / Query  →  current leader  (TCP request/response)

leader applies mutation ──► pushes entry to W-1 peers, waits for acks
                        └─► fire-and-forget fan-out to the group's candidates

every node, per gossip tick ──► catch-up pull until synced; re-gossip batches after

new leader elected ──► queries every live peer for its replica view, merges
```

The leader holds the authoritative table and is the only node that grants. All
mutations are ordered by it, which is what makes the merge rules (below)
well-defined. Everything else — including the leader's own future self — is a
replica holding a merge-ordered view.

### Why single-authority plus replication

A per-key coordinator determined by hashing against local membership views
double-grants during churn, because two nodes legitimately disagree about who
owns a key while views converge. A single elected authority removes that class
of bug structurally: there is exactly one grant authority, and joining confers
none. Replication then addresses the authority's weakness — losing it — without
reintroducing distributed granting.

### Fencing tokens

Tokens are `(term, counter)` pairs where the term comes from leader election
and increases on every leadership change. That gives a strict total order
**with no clock dependency** — a new leader's very first token outranks
everything its predecessor issued, no matter how high that got. Pass the token
to whatever resource the lock protects and have it reject anything carrying a
lower token than the highest it has seen.

Tokens are also the merge ordering for replicas (below), which is why they
matter even if you never fence a resource.

### Merge rules

Every node holds a replica store. Entries arrive via pushes, gossip, and
recovery, in any order, and merge by token:

1. **Higher token wins.** A write from a later term dominates everything from
   earlier terms — a zombie leader's late writes can never displace its
   successor's.
2. **Equal token: the tombstone wins.** A release carries exactly its grant's
   token, so it kills precisely that grant and nothing after it, regardless of
   arrival order.
3. **Equal token, both held: the later expiry wins.** Extends serialise at the
   leader, so its deadlines are comparable without cross-node clocks. (This is
   why merge ordering is the token, not a timestamp: a partitioned node's
   clock keeps ticking and a late physical timestamp must not beat a new
   term.)

### Recovery replaces the warm-up gate

When a node is elected leader it does not wait out a TTL window before
serving. It queries every live peer for its replica view (tombstones included),
merges by the rules above, and opens as soon as its peers answer — normally a
few round trips, capped by `RecoveryTimeout` for stragglers. Late responses
keep merging afterwards; token ordering makes them harmless. Locks held when
the old leader died survive with their original tokens.

The table refuses to grant until recovery completes, so decisions are never
made against incomplete state. `Query` likewise refuses rather than report a
false "not held" during that window.

### Anti-entropy: catch-up and re-gossip

Fire-and-forget gossip can lose a delivery, and a node that joins later never
sees the history at all. Two mechanisms close both gaps, both riding the
cluster's gossip event (`HandleGossipFunc`) — the same self-adjusting cadence
the cluster uses for its own state exchange — so the pool keeps no timer of
its own:

- **Catch-up.** Until a pool has synced once, each gossip tick pulls a full
  state query from its peers, so a late-joining node becomes a useful replica
  promptly rather than waiting for the next leadership change.
- **Re-gossip.** Thereafter each tick pushes a random, payload-sized batch of
  entries to the pool's candidates — the whole table to every peer when it
  fits one payload, one random peer per batch otherwise. Lost gossip heals on
  a later tick and steady-state replication rises towards the whole candidate
  set over time.

Neither is load-bearing for correctness — W-durability carries that — they
raise the recovery hit-rate and keep replica stores warm.

### Tombstone garbage collection

Releases are retained until `release time + MaxTTL`, then dropped. The bound is
provable: any grant the tombstone killed had at most `MaxTTL` to live from a
moment no later than the release, so by collection time every copy of it is
expired everywhere. Drop tombstones earlier and a lagging replica could
resurrect a released grant at the next merge.

## Safety contract

Mutual exclusion holds in the common case. In the pathological case — recovery
cannot reach any node holding a replica of a grant (W or more simultaneous
failures, or a replica partitioned away at merge time) — an overlap is
possible, is **bounded by the holder's extend interval**, and is always
detectable via token ordering. This is what separates the design from the
original hashing approach, where overlaps were silent.

Two practices make the contract tight:

- **Extend on an interval much shorter than the TTL.** A holder that cannot
  renew learns it lost the lock within one interval, not one TTL.
- **Fence the resource.** Record `Lock.Token()` where the lock is enforced and
  reject or preempt work carrying a lower token.

## Usage

```go
import (
    "github.com/paularlott/gossip/leader"
    "github.com/paularlott/gossip/lock"
)

// Set up the election (cluster-wide or NodeGroup-scoped)
ec := leader.DefaultConfig()
ec.MinClusterSize = 3 // majority of smallest cluster size you'll run
election := leader.NewLeaderElection(cluster, ec)
election.Start()
defer election.Stop()

// Create the pool
pool := lock.NewPool(cluster, election, &lock.Config{
    MinTTL:       time.Second,
    MaxTTL:       30 * time.Second,
    WriteReplicas: 2, // W: leader + 1 peer must hold every mutation
})
defer pool.Close()

// Non-blocking
lk, err := pool.TryAcquire("deploy-mutex", 30*time.Second)

// Blocking with context
lk, err = pool.Acquire(ctx, "deploy-mutex", 30*time.Second)

// Extend before expiry — do this on an interval much shorter than the TTL
err = lk.Extend(30 * time.Second)

// Release
err = lk.Release()

// Query from any node
held, owner, token, remaining, err := pool.Query("deploy-mutex")
```

`Query` answers from the leader's authoritative table. While a new leader is
still recovering the answer is genuinely unknown, so `Query` returns
`ErrWarmingUp` or `ErrNoLeader` rather than a misleading "not held". Both are
transient; retry or treat as "unknown".

## TTL Requirement

Every lock must have a TTL. There is no "infinite" lock option. This ensures
the system self-heals if a holder crashes without releasing. `Config` enforces
`MinTTL` and `MaxTTL` bounds. Long-lived locks (a five-minute GPU job, say) are
supported: take the lock with the full expected duration, or take it short and
`Extend` on an interval — which the safety contract recommends anyway.

`MaxTTL` no longer bounds failover time (recovery does), so it can be set to
the natural length of the work being protected. It still bounds how long an
orphaned lock can stay wedged and how long tombstones are retained.

## The W dial

`WriteReplicas` is the number of nodes — the leader included — that must durably
hold a mutation before it is acknowledged:

| W | Meaning |
|---|---------|
| 1 | Leader only. No extra round trip; every acked grant dies with the leader. Only for advisory hints. |
| 2 | Leader + 1 peer ack (default). Tolerates one crash at ack instant; gossip raises steady-state replication to the whole group within seconds. |
| 3+ | One further parallel ack each. Tolerates W-1 simultaneous crashes at ack instant, at the cost of sensitivity to slow peers. |

If the group has fewer nodes than W, the requirement degrades to what the group
can provide — a single-node group runs on the leader's copy alone, because
refusing all operations would serve nobody. Degrading applies to group size,
never to peer failures: if healthy peers exist and do not ack within
`ReplicationTimeout`, the operation is refused (and a blocking `Acquire`
retries). A grant that cannot be made durable is compensated with a tombstone
so it can never resurrect at a later recovery.

## NodeGroup Scoping

The pool follows whichever election you give it. A `MetadataCriteria`-scoped
election elects a group-specific leader, and the pool uses it:

```go
ec := leader.DefaultConfig()
ec.MinClusterSize = 2
ec.MetadataCriteria = map[string]string{"role": "worker"}

election := leader.NewLeaderElection(cluster, ec)
election.Start()

pool := lock.NewPool(cluster, election, &lock.Config{Name: "worker-locks"})
```

Replica targets and recovery queries are drawn from the election's candidate
list, so replication stays inside the group. Any node in the cluster can take
locks from the pool — it routes to the group's leader — but every node expected
to hold replicas should create the pool.

## Multiple Pools

Multiple pools with different names can coexist on one cluster, each with its
own scope and leader:

```go
globalPool := lock.NewPool(cluster, globalElection, &lock.Config{Name: "global"})
workerPool := lock.NewPool(cluster, workerElection, &lock.Config{Name: "workers"})
```

## Failure Modes

| Scenario | Behaviour |
|----------|-----------|
| Lock holder crashes | Leader detects the death and frees its locks immediately (tombstoned and replicated) |
| Leader crashes | New leader elected, recovers by merging replicas. Locks survive with their tokens; service resumes in seconds regardless of MaxTTL. |
| Leader retires gracefully | Same as a crash, minus the failure-detection delay. No handover step needed — replicas already hold everything. |
| Node joins | Nothing. Joining confers no authority. |
| Node leaves gracefully | Baseline quorum lowers by 1; a non-leader departing has no effect on lock state |
| Network partition | Minority cannot elect (quorum floor blocks it), so cannot grant. Majority continues. A partitioned zombie leader cannot commit: its replica pushes fail, so it fails closed. |
| All W replicas of a grant unreachable at recovery | The pathological case: overlap possible, bounded by holder extends, detectable via tokens. See the safety contract. |

## Ownership Model

Lock ownership is per-node, not per-goroutine. Two goroutines on the same node
both acquiring the same key will succeed with the same token — the lock excludes
other **nodes**, not other local callers. Combine with a local `sync.Mutex` if you
need goroutine-level exclusion within one process.

## Quorum and Split-Brain

Safety rests on the election never producing two simultaneous leaders. See the
[leader package](../leader/) for the quorum design (`MinClusterSize`, the
adaptive baseline, and `ForgetNode`). The lock pool inherits the election's
term as the first component of every fencing token, so the two packages share
one ordering domain.

## Configuration

```go
type Config struct {
    Name               string        // Pool name (default: "default")
    MinTTL             time.Duration // Minimum lock TTL (default: 1s)
    MaxTTL             time.Duration // Maximum lock TTL (default: 30s); also bounds tombstone retention
    RetryInterval      time.Duration // Retry delay for blocking Acquire (default: 50ms)
    WriteReplicas      int           // W (default: 2), see the dial
    ReplicationTimeout time.Duration // Deadline for one round of replica acks (default: 500ms)
    RecoveryTimeout    time.Duration // Cap on leader recovery merge (default: 5s)
}
```

## Lifecycle

`Close` every pool you create: it cancels the leadership subscription (a closed
pool is otherwise retained by its election), stops background work, and — when
the last pool on a cluster goes — unregisters the protocol handlers. A cluster
whose pools are never closed retains its registry entry, pinning the cluster,
for the life of the process.

## Example

See [examples/lock](../examples/lock) for an interactive CLI demo.
