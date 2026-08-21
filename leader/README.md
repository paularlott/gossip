# Leader Election

Quorum-based leader election over gossip membership, with an adaptive baseline
for split-brain protection that does not need re-tuning as the cluster grows.

## Usage

```go
import "github.com/paularlott/gossip/leader"

ec := leader.DefaultConfig()
ec.MinClusterSize = 3 // majority of the smallest cluster you will ever run
election := leader.NewLeaderElection(cluster, ec)
election.Start()
defer election.Stop()

election.HandleEventFunc(leader.BecameLeaderEvent, func(leader.EventType, gossip.NodeID) { ... })
election.HandleEventFunc(leader.LeaderLostEvent, func(leader.EventType, gossip.NodeID) { ... })
```

The leader is the eligible node with the lowest node ID; every election
increments a term, and heartbeats carry it so a higher term always wins. When
`MetadataCriteria` is set, only nodes matching all criteria participate and
follow — useful for electing a leader within a role-scoped group.

## How quorum is computed

```
quorum = max(MinClusterSize, baseline/2 + 1, majority(observed))
```

Three terms, each covering what the others cannot:

- **majority(observed)** — a strict majority of the nodes this node can see right
  now. Purely local: it only ever adds strictness, because on its own two sides
  of a partition each compute a majority of their own view and both pass.
- **baseline/2 + 1** — a majority of the cluster size this node has settled on.
  This is the adaptive term: it grows automatically as the cluster grows, which
  is what removes the need to re-tune `MinClusterSize` on scale-out.
- **MinClusterSize** — a constant floor. Because it does not depend on any node's
  view, it is a lower bound on every side's threshold: two sides of a split can
  only both qualify if `N >= 2*MinClusterSize`, so a floor above half the cluster
  makes two simultaneous leaders impossible.

Set `MinClusterSize` to the majority of the **smallest** cluster you will ever
run (`N/2+1`), and leave it alone as you scale. At 0 there is no floor and an
isolated node will elect itself — acceptable when leadership is only an
optimisation, never when it backs correctness such as distributed locks.

## The adaptive baseline

The baseline tracks the cluster's settled size:

- **Growth**: the observed eligible count must hold steady for
  `StabilityPeriod` (default `2× DeadNodeTimeout`) before it is adopted, so a
  count sampled mid-convergence — or a node that appears for two seconds —
  cannot ratchet quorum upward.
- **Graceful shrink**: a node that announces `Leave()` is a positive signal from
  outside the failure domain, so the baseline drops by one immediately. A
  partitioned node cannot fake this: a partitioned node cannot broadcast.
- **Silent loss**: a crash looks identical to a partition, so the baseline holds
  and quorum stays conservative. One missing node is absorbed automatically
  after `ShrinkDwell` (default `4× DeadNodeTimeout`) — the single-step
  restriction is what keeps that split-safe. A larger shortfall is never
  absorbed on its own; use graceful drains or `ForgetNode`.
- **Rejoin**: a departing node that comes back is un-marked, so rolling
  restarts do not leak decrements.

### Reclaiming availability after crashes

A crashed node cannot be distinguished from a partitioned one, so the cluster
keeps counting it. When an operator knows better, `ForgetNode` is the escape
hatch:

```go
election.ForgetNode(deadNodeID)     // broadcasts; one call reaches the cluster
election.ForgetNodeLocal(deadNodeID) // this node only
```

It discounts the node from the baseline and drops it from the local node list.
Only nodes the cluster already knows can be forgotten — a forget naming an ID
that was never a member is rejected, locally and on every peer, since each
accepted forget lowers quorum. Delivery of the broadcast is best-effort gossip;
a node that misses it keeps the higher baseline — the strict, safe direction.

## Diagnostics

- `BaselineSize()` — the cluster size quorum is currently measured against.
  When leadership cannot be established, look here first.
- `QuorumSize()` — how many eligible nodes are currently required.
- `Term()` / `LeaderTermSnapshot()` — the election term and a consistent
  (leader, term) pair; the term is what downstream consumers such as
  [distributed locks](../lock/) use for fencing tokens.
- `WatchLeadership(fn)` — a callback fired on every leadership change,
  including transitions the four public events do not cover (a term advancing
  while the same node stays leader, a leader silently becoming ineligible).
  The callback is invoked once with the current state on registration. This is
  how the lock pool reacts to failover without polling.

## Split-brain safety argument

For a split to produce two leaders, both sides must independently reach quorum.
With the floor above half the cluster, `A + B = N < 2*MinClusterSize` means at
least one side is below the floor. The baseline follows the same rule when it
shrinks: the auto-shrink step only applies at exactly one node missing, and two
sides of a partition each seeing "baseline minus one" requires `A = B = N-1`
with `A + B = N`, which only solves at `N = 2` — where the floor already blocks
both sides. The property is swept exhaustively in
`TestQuorumPreventsDisjointMajorities` and `TestAutoShrinkNeverAllowsTwoLeaders`.

## Message types

The election communicates over two message types, drawn from the library's
reserved range: `HeartbeatMessageType` (default `ReservedMsgsStart+1`) and
`ForgetMessageType` (default `ReservedMsgsStart+2`). Applications must define
their own message types from `UserMsg` upward, never from the reserved range —
a registration collision in the reserved range means a contract violation or a
library bug and fails fast at construction. Running several elections on one
cluster (cluster-wide plus group-scoped, say) requires distinct message types
for each.

## Small clusters

`MinClusterSize` interacts strongly with cluster size, and the choices are
genuinely different at the bottom end:

- **Single node (e.g. everything on one machine for testing):**
  `MinClusterSize = 1` is correct. The node elects itself, and the lock pool
  runs in its documented degraded mode with the leader's copy as the only
  replica.
- **Two nodes:** a quorum system cannot be both crash-tolerant and
  partition-safe at N=2 — this is why etcd and Consul recommend three. Pick
  the failure you prefer:
  - `MinClusterSize = 2` (safe): a node crash leaves the survivor unable to
    elect — the floor dominates and auto-shrink cannot lower it — so
    leadership (and anything built on it) is unavailable until `ForgetNode`
    marks the dead node. Nothing is ever double-granted.
  - `MinClusterSize = 1` (available): a node crash is absorbed after
    `ShrinkDwell`; but a 1/1 **partition** is absorbed the same way on both
    sides, and both elect. Only choose this if partitions are considered less
    likely than crashes and the guarded work tolerates rare duplication.
- **Three nodes:** the sane production floor. `MinClusterSize = 2`, one crash
  is survived with full safety, and no partition of three can field two
  leaders.

## Scaling note

Leader heartbeats are disseminated by gossip, so a heartbeat reaches the far
side of a large cluster after several propagation hops. Keep `LeaderTimeout`
comfortably above gossip convergence time for your cluster size; at hundreds of
nodes the 3s default becomes tight and should be raised alongside the probe
interval.

## Upgrading from the previous election

Behavioural changes to be aware of when deploying this version against a
cluster running the old code (rolling upgrades mix both, so read this first):

- **Quorum is now a strict majority of the observed count, floored by
  `MinClusterSize` and the adaptive baseline.** The old 60%-of-observed default
  produced slightly higher thresholds in clusters of 7+ nodes; a mixed cluster
  may elect where the old code would not. Safety is unchanged — a majority is
  still enforced — availability improves.
- **`SteppedDownEvent` now fires when a leader stops matching its
  `MetadataCriteria`**, where it previously went silent. Handlers see an
  accurate event they did not before.
- **Event handlers run outside the election's lock.** Previously a handler
  calling `IsLeader()` during an event deadlocked; that hazard is fixed, at the
  cost of events no longer being atomic with the state change under the lock.
- **`QuorumPercentage` is removed** (breaking, deliberate): majority-based
  quorum supersedes it. Delete the field from existing configs.

## Example

See [examples/leader](../examples/leader) for an interactive demo, and
[examples/lock](../examples/lock) for locks built on top of the election.
