# partition — design

How the `partition` package works: single-owner, majority-committed replication of partitioned data over gossip
membership. [README.md](README.md) is the user's guide (API, Store contract, configuration, cost); this document is the
protocol as built, for whoever maintains it. File names in brackets say where each part lives.

Two rules run through everything: **safety over liveness** (when a node is unsure it refuses) and **the cluster size is
configured, never inferred** (a majority is `ClusterSize/2+1` of the configured number, not of the nodes that are
visible).

## 1. State

A **partition** is an integer `0 .. Partitions-1`; the package never looks at what is in it. Every node keeps a copy of
every partition. Each partition has at most one **owner** at a time, and every change goes through the owner.

- **Epoch** `(Counter, Node)`: one tenure of ownership, ordered by Counter and then by Node. The zero Epoch is lowest.
- **Pos** `(Epoch, Seq)`: the position of a write. `Seq` is per partition, starts at 1, never resets and is the order of
  writes. **A Seq is the identity of a write**: whatever epoch label it carries, the same Seq is the same write (§6).
- **Write** `(Partition, Pos, Prev, Data)`: `Data` is opaque, made by `Store.Prepare` on the owner; `Prev` is the
  committed position the write follows.
- Counters never wrap: an operation that would make a Seq or Counter reach `MaxUint64` returns `ErrExhausted` and does
  nothing.

**Durable state** per partition, kept by the application's `Store`: `Promised` (the highest epoch promised), `Committed`
(the last committed write, applied to the data) and `Pending` (the one stored write that is not known to be committed).
The package keeps `Pending == nil || Pending.Pos.Seq == Committed.Seq+1`.

**Volatile state** per partition [manager.go]: the owner this node believes in (`owner`, `ownerEpoch`, `ownerSeen`, the
time of its last heartbeat); `leading` and `myEpoch` (this node completed a promise round at `myEpoch` and has not
stepped down), `settled` and `leaderSince` (the takeover has committed the write it settles; when its promise round
began); `maxCounter` (the highest Epoch counter seen for the partition anywhere); `lastMajority` (owner only, once
settled: when a write or heartbeat round last reached a majority); `relinquished` (the highest epoch whose owner said it
stepped down); `ownerlessSince`, `failures`, `backoffUntil` and `lastClaim` (§3); a **cache** of the durable state,
`promised` and `committed`, lower bounds of the Store's values raised after every `SetPromised`, `Commit` and `Import`
the package makes (and whenever it reads `State` under the mutex), loaded from `State` the first time it is needed; and
what the owner reports (§6): `ownerCommitted`, `hbCommitted`, the lag (`lagTarget`, `lagSince`), `aheadSince` and
`diverged`. Per node: `ready` (the startup synchronisation is done) and `eligible` (`SetEligible`).

**The partition mutex** `mu[p]` serialises everything that changes the partition's durable state: the promise and write
handlers, the settling of heartbeat entries, catch-up imports, takeover and replication. Takeover and replication wait for
the network with it held (a partition does one thing at a time; other partitions are not blocked). A request handler,
which runs in a goroutine of the Manager (§2), waits for it at most `OwnerTimeout` and refuses if it cannot get it.
**No handler waits for it on one of the cluster's incoming workers**, nor in the path of a heartbeat's reply: heartbeat
entries and commit notices are decided from the cache and the volatile state, and the settling they call for is queued
for a goroutine of the partition (§6). A second mutex per partition (`vmu`) guards the volatile fields and is never held
across I/O. Lock order: `mu`, then `vmu`.

## 2. Messages

Every message carries `Size`, the sender's `ClusterSize`, and `Cluster`, its `ClusterID` (the name of the data set); a
receiver configured differently in either answers `SizeMismatch` and otherwise ignores it, and only nodes publishing the
same size and ID in their metadata are ownership candidates. Two data sets therefore never exchange anything: positions
are compared by sequence number, so a merge of two data sets would otherwise go unnoticed. Message types are `gossip.PartHeartbeatMsg` … `gossip.PartReadMsg` (`ReservedMsgsStart + 30..39`)
[messages.go]. Requests are answered through `HandleFuncWithDeferredReply`: the handler decodes the request and hands the
work to a goroutine of the Manager, so no handler holds one of the cluster's incoming workers while it waits for the
Store, a mutex or other nodes [net.go]. The one-way messages (Relinquish, Commit) are handled on the worker and never
wait: a relinquish changes only volatile state, and a commit notice is decided like a heartbeat entry, its settling
queued (§6). A request over its concurrency limit, or arriving while the Manager stops, is answered at once with the "not
OK" reply, never with an acknowledgement.

| Message | Kind | Content | Section |
| --- | --- | --- | --- |
| Heartbeat | request | `Entries[(P, Epoch, Committed)]` → `OK`, `Fenced[(P, Promised)]`, `Accepted[P]` | §5, §6 |
| Promise | request | `(P, Epoch)` → `OK`, `Promised`, `Committed`, `Pending` | §4 |
| Write | request | `(Write, OwnerCommitted)` → `OK`, `Promised`, `Committed`, `NeedCatchUp` | §5, §6 |
| Change | request | `(P, Change, Deadline)` → `Status`, refusal code and message, `Pos`, `Reply`, owner hint | §5 |
| Read | request | `(P, Req, Deadline)` → as Change; `Reply` is the answer | §8 |
| State | request | `()` → `Intact`, `Forming`, `Committed[]`, `Promised[]`, `Pending[]` for every partition | §7 |
| Claim | request | `(P)` → `OK` | §3 |
| CatchUp | stream | `(P, After, Full)` → header `(UpTo, Err)`, then the `Store.Export` bytes | §6 |
| Relinquish | one way | `(P, Epoch)` | §3 |
| Commit | one way | `(P, Epoch, Committed)` | §5 |

A change's `Status` is done, refused, unknown, notowner, unavailable, exhausted or busy; all but done and unknown mean
that nothing was written. A refusal's `Pos` is the owner's committed position when it refused (`RefusedError.Pos`).
`Deadline` is the requester's deadline as Unix nanoseconds (§5). The heartbeat is a request only so that the owner learns that it arrived (the library's one-way
sends never report a failed delivery). `Heartbeat.From` is carried for compatibility; receivers use the packet's sender.
The metadata keys `partition.size`, `partition.cluster`, and `partition.ready` are published on the local node.

## 3. Ownership

**Candidates** for owning partitions are the alive nodes (this one included, if ready and eligible) whose `partition.size`
and `partition.cluster` match and whose `partition.ready` is true [membership.go]; a node publishes `ready` only while it is
both ready and eligible. For partition `p` they are ordered by `fnv64a(p, nodeID)`, highest first
(ties: the higher ID): **rank** 0 is the rendezvous owner, then 1, 2, … Every node computes the same order from the same
view, and when a node leaves only its partitions move.

A partition is **ownerless** at a node when the node does not lead it and has no live owner: none known, the owner's last
heartbeat or commit notice is older than `OwnerTimeout`, or the owner relinquished. `ownerlessSince` is when that began:
the expiry of the last owner's heartbeat, or when the node first noticed there was none; it is reset when an owner is seen.

Every `HeartbeatInterval` the ownership loop looks at each partition [takeover.go]:

- **Takeover.** A ready, eligible node of rank `r` takes over a partition that has been ownerless for `r × RankDelay`,
  after a random backoff in `[0, BackoffMax)` (the trigger is checked again after it), unless the partition is in backoff
  or diverged here. Rank 0
  acts at once; a crashed rank 0 that is still alive in gossip is replaced by rank 1 one `RankDelay` later, without waiting
  for the failure detector.
- **Claim.** A ready, eligible rank 0 node that sees a live owner other than itself (and is not in backoff, and the
  partition has not diverged here) sends it a Claim, at most once per `OwnerTimeout`, once it is **caught up**: no
  catch-up of the partition is running and it has committed at least what the owner reported in its last heartbeat (not
  its commit notices, which a node under load is always a moment behind). A claimant that is behind would hold the
  partition through a long catch-up in its takeover. The owner, if it leads the partition and the claimant is rank 0 among
  its own candidates,
  takes the mutex (so after any change in progress), steps down, relinquishes and answers OK. The claimant then finds the
  partition ownerless and takes it over in the ordinary way. A claim is not itself a takeover.
- **Backoff.** A takeover that does not complete (no majority of promises, catch-up failed, the settling write failed) puts
  the partition in backoff at that node: `OwnerTimeout`, doubling with each consecutive failure up to
  `TakeoverBackoffMax`; a success resets it. In backoff the node neither takes over nor claims. This bounds the churn when
  the top-ranked node can send but not receive: it claims, takes over, fails and backs off further each time, while the
  next rank keeps the partition. A takeover that asked nobody (its epoch was not above the stored promise) or ended
  because the Manager stopped is not a failure.
- **Stickiness.** A node refuses to promise to a challenger while it has heard from the partition's live owner within
  `OwnerTimeout`, so a slow heartbeat does not move a partition (§4).

**An owner never steps down because of rank.** It steps down when: a promise, write or heartbeat of a higher epoch reaches
it; a reply, a heartbeat's `Fenced` list or the stored promise shows a higher promise than its epoch; a replication fails
or cannot be told (§5); its Store fails; it has had no majority contact for `OwnerTimeout` (a takeover that is still
settling counts from its promise round); a read's confirmation round finds no majority (§8); it grants a claim; it becomes
ineligible (`SetEligible(false)`); the partition diverged here (§6); or the Manager stops. **Every step-down sends
Relinquish**`(P, Epoch)` to every alive node [manager.go `stepDown`]: a receiver whose believed owner is the sender clears
`ownerSeen` (stickiness lapses at once) and remembers the epoch in `relinquished`, so that a heartbeat of that tenure
still in flight is ignored. Without it a follower would refuse every successor's promise for `OwnerTimeout` after its
owner stopped leading. Stepping down is always safe:
followers refuse writes below their promise, so a node that leads in its own belief only fails to commit. The step-down on
lost majority matters for staleness: a follower on the owner's side of a partition would otherwise keep refreshing
`ownerSeen` from its heartbeats and keep serving local reads of data that the other side has moved on from.

## 4. Takeover

`takeover(p)` runs under `mu[p]` throughout, including while it waits for the network [takeover.go]:

1. Propose `e = (maxCounter+1, self)`. If `e` is not above the stored `Promised`, raise `maxCounter` and retry later.
   Otherwise record the promise durably (`SetPromised`).
2. Send Promise`(p, e)` to every other alive node. A node answers OK only if it is not resyncing, `e` is above its
   `Promised` and stickiness does not bind it; it records the promise durably **before** replying, steps down if it led
   below `e`, and reports its `Committed` and `Pending`. Refusals raise `maxCounter`. Wait for a majority of the configured
   size (self included), or every answer, or `OwnerTimeout`; without a majority the takeover fails.
3. `H` is the highest `Committed.Seq` among the answers (equal Seq is the same write). If this node is behind, catch up to
   `H` from an answerer that holds it (§6).
4. The write to settle is the pending write of the highest epoch among the answers' pending writes **at `H+1`**: it is the
   one an earlier owner most recently got as far as sending. Pending writes at or below `H` are already decided and are
   ignored.
5. Unless this node became ineligible or found the partition diverged meanwhile (then it stops, which is not a failure:
   it never writes at `e`), become the owner (`leading = true`, `myEpoch = e`, not `settled`). If there is a write to
   settle, replicate its `Data` at `(e, H+1)` with `Prev = H` and wait for it to commit; if it does not, step down and
   fail. Only then is the tenure `settled`, with majority contact from the settling write's start (or the promise round's,
   if there was nothing to settle), and does the mutex release: no change or read is served, and no local read
   (`CanRead`), before the previous owner's last write is settled. A heartbeat round that reaches a majority meanwhile
   counts for nothing. Announce with a heartbeat.

## 5. Writes

**Landing node** [write.go]. `Write` and `Read` share `route`: until `WriteTimeout` (or the caller's context, if it ends
sooner), send the request to this node if it leads, else to the owner it believes in, else to the rendezvous owner. A
request to another node carries the deadline. A node that is not the owner answers `notowner` with the owner it believes
in, tried next (once per attempt); an owner at a limit (`MaxConcurrentChanges`, `MaxChangesPerPartition`,
`MaxConcurrentReads`) answers `busy` and the request is sent again after a pause that starts at 1ms and doubles with each
busy answer in a row up to 50ms, with jitter (between half the pause and all of it); anything else waits a quarter of
`HeartbeatInterval`. Nothing is sent to another node once less than the deadline floor (below) is left: the owner would
refuse it, and a request that cannot arrive in time would turn an operation that never ran into `ErrUnknown`. A node
never forwards a request it receives. A change that was certainly not delivered (the transport's error wraps a
`*net.OpError` whose `Op` is "dial": the connection could not be made, or a transport wrapper refused to send it) is
retried, and ends in `ErrUnavailable` if no attempt gets through; one that may have been delivered is never retried and
ends in `ErrUnknown`, whereas a read changes nothing and is simply sent again.

**Deadlines.** The transport must let a request wait for its reply longer than `WriteTimeout`
(`Manager.MinTransportDeadline`, `WriteTimeout + HeartbeatInterval`; gossip's `TCPDeadline`), or a forwarded change the
owner still commits is reported `ErrUnknown`. A forwarded change or read runs on the owner with a context that ends at
the requester's deadline (and at most `WriteTimeout` after it arrived), which bounds its wait for the mutex; once the owner
holds the mutex it refuses (`unavailable`, nothing done) if less than the **deadline floor**, `2 × HeartbeatInterval` (at
most half of `WriteTimeout`), is left (`tooLate`), so it never begins a write it could not report back on. The deadline is
an absolute time, so nodes' clocks must agree to well within the floor; a clock far off makes forwarded requests fail
(`ErrUnavailable`), never unsafe. A change of this node's own caller has no floor: the caller waits for it.

**Owner** (`runChange`, under `mu[p]`): it must still lead, have had majority contact within `OwnerTimeout`, have time
left (above), and still hold `Promised == myEpoch` (otherwise it steps down: it was fenced; a Store that fails to answer
`State` also makes it step down). `Store.Prepare` checks the change against the committed data and returns the write's
`Data` and a reply for the caller, or a refusal (which carries `Committed` as its `Pos`). The write is
`(myEpoch, Committed.Seq+1)` with `Prev = Committed`.

**Replication** (`replicate`). It runs on its own `WriteTimeout` from its start, whatever is left of its caller's: a write
that was begun is committed, or its owner steps down so that a takeover settles it, never left because a caller gave up.
The owner sends the write, with its committed position, to every other alive node and, **at the same time**, stores it as
its own pending write (`SetPending`, durable). Its own acknowledgement counts once that call
has returned, so a write waits for the slower of the owner's durable `SetPending` and the followers', not for their sum. It
commits (`Store.Commit`) as soon as a majority including itself holds the write, never before its own `SetPending` has
returned, and does not return before it has either: a Store call cannot be abandoned, and a later one on the partition must
not overtake it. Followers that answer `NeedCatchUp` are not counted. Any answer showing a promise above the epoch ends the
tenure. If the owner's own `SetPending` fails, the write may already be stored on followers: the result is `unknown`,
never `unavailable`, and the owner steps down so that a takeover settles it. Likewise, when a majority did not answer in
time the write may or may not be committed, so the result is `unknown` and the owner steps down.

**Commit notice.** Right after committing, the owner tells every other alive node (Commit, one way, from a goroutine of the
Manager) the epoch and the committed position. A receiver treats it exactly like one heartbeat entry, so followers apply
the write within one message delay (and a goroutine hop) instead of at the next write or heartbeat. Notices may overtake
each other; each is idempotent.

**Heartbeats.** Every `HeartbeatInterval` each owner sends every other alive node one Heartbeat listing its partitions with
epoch and committed position [heartbeat.go]. A round whose sends were delivered to at least `Majority-1` others counts as
majority contact for the partitions it listed (`lastMajority`, and `ownerSeen` for the owner itself); a successful write
counts too. The round does not wait for slow nodes. A reply's `Fenced` entries, promises above the owner's epoch, make it
step down.

## 6. Followers, comparing by Seq, and catch-up

**Write handling** [follower.go]. A follower that is not resyncing, whose copy of the partition has not diverged, and that
can get the mutex refuses a write of an epoch below its promise, and a malformed one (its Seq does not follow its `Prev`). A write of a higher epoch first raises the promise
durably (and deposes this node if it led below it). The follower then **settles** its pending write against the owner's
committed position `oc` carried by the write (and by heartbeats and commit notices):

| Condition | Action | Why |
| --- | --- | --- |
| no pending, or `pending.Seq > oc.Seq` | nothing | nothing to decide, or not decided yet |
| `pending.Epoch == oc.Epoch`, `pending.Seq <= oc.Seq` | commit it (if it follows `Committed`) | an epoch's owner wrote one write per Seq and committed through `oc` |
| `pending.Epoch != oc.Epoch`, `pending.Seq <= oc.Seq` | clear it | that Seq is decided, possibly as a different write; catch-up brings the committed one |

Then it compares what it has committed with the write **by Seq, not by Pos**:

- committed Seq below `w.Prev.Seq`: this node is behind; answer `NeedCatchUp` and start a catch-up;
- committed Seq equal to `w.Seq`: this node already holds the write, committed under another epoch label; acknowledge
  (it is stored, which is all an ack says) and do not store it again;
- committed Seq above `w.Seq`: impossible, since a write stored by a majority needs each holder to have committed its
  predecessor; refuse;
- committed Seq equal to `w.Prev.Seq`: store it as the pending write, with `Prev` rewritten to this node's own label of
  that Seq (which is what `Store.Commit` checks), and acknowledge.

**Why Seq.** An owner can commit write *s* and lose the ownership before the others hear of it. They hold *s* only as
pending; the next owner's promise round (which need not include the old owner) finds that pending write and re-proposes
the same data as *s* under its own epoch. The same write is then committed at *s* under two positions on different nodes.
Comparing positions would call that a conflict. By the rule that equal Seq is the same write, it is not, so everything
that identifies a write (follower checks, takeover's `H`, `WaitApplied`, `Export`'s "changed after") uses the Seq; the
epoch in a Pos only says who proposed it.

**Heartbeat and commit-notice entries** (`onHeartbeatEntry`) are decided without the partition's mutex, under `vmu` only,
from the cached promise and the volatile state. An entry is *accepted* as the current owner's only if all hold: the sender
is the epoch's node; this node is not resyncing; its cached promise is not above the entry's epoch (otherwise the entry is
answered with a fence); the epoch is above `relinquished`; the epoch is not below that of the owner it believes in; and it
does not lead at a higher epoch (if it leads at a lower one it steps down). The cached promise is never below a promise
the node has answered: `setPromised` raises it after the durable `SetPromised` and before any reply, which is all the read
confirmation needs (§8). An accepted entry records the owner (`ownerSeen` is now, and an ownership event fires if the
owner or epoch changed) and the reported committed position, and **queues** the rest for the partition's goroutine
(`queueEntry`, `applyEntries`): one slot per partition that keeps the entry with the highest committed position, drained
by at most one goroutine, which takes the mutex, settles the pending write against the entry's committed position and
starts a catch-up if this node is behind. Settling later is as safe as at once: an owner's committed position is a fact
whenever it is looked at. Dropping an older queued entry loses nothing (the newer one decides all it would have, and the
next heartbeat repeats it). So a heartbeat's reply never waits for a busy partition, and the entries of one heartbeat do
not wait for each other.

**Staleness and divergence** [status.go]. Each accepted entry is a report of the owner's committed position. A follower
behind a report measures since when (`lagSince`, for the oldest report it has not reached: when it reaches it, the lag
restarts from the latest report still ahead, or ends); `CanRead` is false once that reaches `StaleAfter`, even while the
owner is heard, so a follower whose catch-ups keep failing stops serving. A follower **ahead** of every report for
`3 × OwnerTimeout` (`aheadSince`; a single report at or above its position resets it, so reports overtaken by commit
notices do not count) has committed writes the owner does not have. That cannot happen while the guarantees hold (§9); it
means lost data, a disk lost or wrongly restored with `Intact()` true, here or on a majority elsewhere. The partition is
then **diverged** at that node until it restarts: logged at error level and counted, `CanRead` false, no takeover or
claim, and it steps down if it led. It also gives no promise and acknowledges no write for the partition, serves no
catch-up of it and leaves it out of its state answers, because each of those would hand its unsupported writes to
another node (a promise round would take its committed position as `H`; an acknowledgement by Seq would count it as a
holder of a write it does not have). `Manager.PartitionStatus` and `Status` expose all of this.

**Catch-up** [catchup.go]. A node behind a committed position (from a write, an entry or the startup exchange) opens a
stream to a node that has it. At most `MaxConcurrentCatchUps` run at once in the background (asynchronous ones and the
startup sync's); a takeover's own does not wait for a slot. Background failures in a row are logged at warn level, at
most every 10s per partition. It asks for the changes after its own committed Seq; if the source answers `ErrPurged`, it
asks for everything. The stream is a header frame (4-byte length, JSON `(UpTo, Err)`) then the `Store.Export` bytes. The
receiver takes the mutex (unless the caller, a takeover, already holds it), re-reads its state and imports only if `UpTo`
is still ahead; otherwise the body is discarded. At most one asynchronous catch-up runs per partition; a request while one
is in flight is dropped, since a later entry asks again. **Committed data is identical on every node that has it**, so any
node is a safe source up to its own commit point: catch-up needs no fencing, takes no mutex on the source and never exports
pending writes.

## 7. Startup, readiness and resync

`Start` publishes `partition.size`, `partition.ready = false` and registers the handlers [sync.go]. If `Store.Intact()` is
false the node is in **resync mode**. The sync loop asks every alive node for its State until answers from a majority
including this node (in resync mode, `size − majority + 1` of the **other** nodes: enough that every majority includes
one of them, so 2 of 3, 1 of 2, 2 of 4, 3 of 5) are in, then for each partition catches up from a
node that reports the highest committed Seq. **Only intact answers count, and only they are used**: a State answer says
whether its node is resyncing (`Intact` false), and such a node may have forgotten promises and acknowledgements, so two
nodes that lost their data never resync from each other. `ForceResync` (an operator's, for one start) lowers the quorum
to one intact answer, never to none: with only resyncing nodes answering the node waits, and says so at warn level.

**Forming and established.** An intact node is *established* once it has given a promise (`setPromised`, made durable
through `Config.SetEstablished` before the promise is stored; a resyncing node raising its promise to what others report
does not count) or completed a startup sync with intact answers from a majority including itself; until then it is
*forming*, and its State answer says so. This is informational and never lowers the recovery quorum: a majority can
commit while a forming node is absent, so the forming node's intact data may still be stale. For example, two of three
nodes can commit a write before the third ever participates. If one of those two loses its disk while the other is
unreachable, recovering from the forming third node alone would lose that acknowledged write. Retaining or losing the
recovering node's established marker does not change that argument.

To bootstrap empty nodes around one seeded or restored node when the seed alone does not meet the resync quorum, the
operator must explicitly set `ForceResync` on the empty nodes for that start and ensure that the seed holds the intended
data set. After bootstrap, normal recovery uses the quorum again. With the hooks left nil every node counts as established.

When every partition is caught up it leaves resync mode (calling the
`HandleResyncedFunc` handlers), waits `ReadyHoldDown` and publishes `ready = true`; only then is the node a candidate. Any
node serves State and catch-up whether or not it is ready.

A node in resync mode **promises nothing, stores nothing, follows no owners and serves no reads**: its data was lost or
restored, so it may have forgotten promises it made and writes it acknowledged, and counting it would break the
majority-intersection argument (§9). In addition to catching up, it:

- raises each partition's `Promised` to the highest reported, and
- **adopts a pending write** (`adoptPending`): the pending write of the highest epoch at `Committed+1` among the answers,
  stored as its own pending. A write is committed once a majority holds it, and the lost disk may have been part of that
  majority; the only other holders may hold the write as pending only, because they learn that it is committed from the
  owner's next heartbeat or write. If the node came back without it, a takeover whose promise round happens to skip those
  holders would write something else at its position and the acknowledged write would be lost. Holding the write as its
  own pending, the node reports it in every promise it gives, as an intact node would. Among the answers of `size − majority + 1`
  other nodes there is always a holder of every write a majority stored: at most `size − majority` other nodes hold none.

## 8. Reads

**Local reads and staleness.** `CanRead(p)` is true while this node leads `p`, its takeover has settled (§4), and it has had
majority contact within `StaleAfter`; or, as a follower, while it has seen the owner's heartbeat within `StaleAfter`, has
not trailed a reported committed position for `StaleAfter` (§6), and `p` has not diverged here. It is false in resync
mode. A node that is cut off stops serving after that. The owner's copy is current; a follower's is as fresh as the last commit notice or heartbeat (a
message behind after a write, up to `HeartbeatInterval` if the notice was lost). A deposed owner that has not heard of it
can serve local reads for up to `StaleAfter`. `WaitApplied(p, pos)` returns once this node's committed Seq is at least
`pos.Seq`; it wakes on every local commit or import.

**Linearizable reads** (`Read`, the read-index method) [read.go]. Routed like a write, `runRead` on the owner:

1. takes `mu[p]` only to check that it still leads with `Promised == myEpoch` and to note `idx = Committed`, and releases
   it;
2. **confirms its leadership**: a round that started after the read arrived sends the partition's heartbeat entry to every
   other node and waits, for at most `OwnerTimeout`, until a majority including this node has **accepted** it (§6: no
   promise above the epoch when the receiver looked). A promise above the epoch in any answer fails the round and steps the
   owner down; so does the lack of a majority, which also relinquishes;
3. checks that it still leads at the same epoch, then runs `Store.Read` without the mutex.

Reads share rounds: at most one round runs per partition and at most one more waits; a read joins the waiting round (the
running one started before it arrived and proves nothing about it), so each read waits for at most two. A round confirms
the tenure it started in; a read that began in another tenure is answered `notowner`.

*Why accepted entries only.* A receiver answers OK to an entry it ignored for reasons that say nothing about its promise:
it is resyncing (and has forgotten its promises), it knows a higher owner, or the entry's tenure was relinquished.
Counting those would break the intersection argument below. `Accepted` says that the entry was taken as the current
owner's: the receiver looked at its promise after the entry arrived. It looks at the cached promise, not the Store's,
without the mutex; that is enough because the cache is raised before the node answers any promise or write of a higher
epoch, so a promise the receiver had not cached when it looked was given (answered) after it looked.

*Why the mutex.* A takeover sets `leading` before it has committed the write it settles, and holds the mutex until it has.
A read that did not wait for it could confirm leadership and answer from before a write the previous owner acknowledged.

*Why it is linearizable.* A write committed before the read arrived is applied here: this tenure committed it before it
answered, or an earlier tenure did, in which case the takeover settled it before releasing the mutex the read waited for.
Nothing newer than this tenure has committed anything: the round found a majority, this node included, holding no promise
above the epoch after the read arrived; a later owner needs promises from a majority, which includes one of those nodes,
and that node promised after it answered, so the later owner's takeover completed after the read arrived.

## 9. The durability contract and the safety argument

**Durability** (the `Store` documentation has the full text). `SetPromised` and `SetPending` are durable before they
return. `Commit` and `Import` are only atomic: a crash leaves the state from before or after the call. A write's safety
comes from its being stored as pending on a majority; a commit lost by a crash is redone from the pending write, which
every rule above handles (a follower commits it when the owner's position says so; a new owner settles it). So a write's
latency is one durable `SetPending`, and no `Commit`. One ordering is required: a durable `SetPending` or `SetPromised`
must also make everything committed or imported before it durable. With a single pending slot, a crash that kept a later
`SetPending` (at *n+1*) and lost the `Commit` of *n* would leave the node with `Committed` *n-1* and `Pending` *n+1*, and
write *n* on this node nowhere.

**One value per Seq.** A write at Seq *s* in epoch *e* is made only by the owner of *e*, which became owner through a
majority of durable promises, and is stored by a node only if the node's promise is not above *e*. Take the first write
committed at *s*, in epoch *e*, stored by a majority *M*. Any later owner, at *e' > e*, gathered promises from a majority
that intersects *M*. A node in the intersection either stored the write before promising *e'* (afterwards it would refuse
it), so it reports it as pending at *s* or as committed beyond it, or never stored it, which contradicts its being in *M*.
So the later owner either catches up past *s*, where the committed data is that write, or re-proposes the pending write of
the highest epoch at *s*, which by induction over epochs carries the same data. Hence every committed write at *s*,
whatever its epoch label, has the same `Data`. Earlier uncommitted proposals may differ, but cannot later be committed
as a competing value.

**No acknowledged write is lost.** `Write` returns only after a majority durably stored the write as pending and the owner
committed it. A node loses its copy only by: committing it (durable, or restored as pending by the ordering rule above);
having it replaced by a pending write of a higher epoch, which carries the same data; having it cleared by `settle`, which
happens only when that Seq is already decided and catch-up restores the committed write; or losing its disk, in which case
it resyncs without promising or storing anything and adopts the pending write (§7). A takeover therefore always meets a
holder, and the argument above applies. `ErrUnknown` leaves the same resolution to the next owner: the write is settled,
committed once or dropped, and the package never retries it.

**One owner commits.** Two nodes can believe they own a partition; only one can commit, because committing needs a
majority to store the write at an epoch they have not outpromised. An owner that has been outpromised learns it from a
reply, a fence, its stored promise or a higher epoch's message, and steps down.

Safety of writes and of linearizable reads does not depend on clocks. Timers drive liveness (timeouts, backoff) and
`CanRead`'s staleness bound.

## 10. Validation

Operational constraints and recovery decisions are documented in
[the user guide](README.md#recovery-and-operations). When changing a protocol rule,
keep its safety argument and focused tests together:

- Unit and in-process cluster tests exercise handler decisions, persistence
  ordering, deadlines, and concurrency with explicit store hooks.
- `dt_*_test.go` exercises real socket clusters with injected faults and a ledger
  that checks acknowledged-write survival, one value per sequence, convergence,
  operation ordering, and consistent reads. The recovery regression is in
  `recovery_test.go`; unit and socket tests cover different failure boundaries.
- `TestDTSoak` combines random network and storage faults. Replay its printed seed
  with `PARTITION_SOAK_SEED`; extend release runs with `PARTITION_SOAK_DURATION=3m`
  and vary cluster size with `PARTITION_SOAK_NODES` (at least 2).

Run the suite and race detector from the repository root:

```sh
go test ./... -count=1
go test -race ./... -count=1
PARTITION_SOAK_SEED=20261026 PARTITION_SOAK_DURATION=3m PARTITION_SOAK_NODES=5 go test -race ./partition -run '^TestDTSoak$' -count=1
```

The fault harness models storage durability; the application's actual `Store`
also needs crash-recovery validation on its deployment platform.
