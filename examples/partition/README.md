# Partition Example

A small partitioned key-value server built on the [`partition`](../../partition/) package: every key lives in one
partition, each partition has one owner at a time, and a write is committed once a majority of the configured cluster
holds it. Unlike the [kv example](../kv/), which is eventually consistent and keeps everything in memory, the data here
is on disk, writes have a single ordering per partition, and a node can be killed, restarted or even wiped without
losing an acknowledged write.

## What it shows

- **A durable `partition.Store`** on the standard library only (`store.go`): per partition a directory with the committed
  data and the protocol's state, every file replaced atomically (write a temporary file, fsync, rename, fsync the
  directory), so a crash never leaves a torn file. `SetPending` and `SetPromised` are durable before they return;
  `Commit` is not: the store uses the package's optional deferred durability, and a background flush writes the committed data.
- **Single-owner writes**: `PUT`, `DELETE` and a compare-and-swap (`If-Match`) go to the partition's owner wherever
  they land, and are acknowledged once a majority has stored them. The owner's `Prepare` decides the key's new version
  and the package carries it back to the node that took the request, which returns it as the `ETag`.
- **Local reads with bounded staleness**: `GET` is answered from the node's own copy while the package says the copy is
  recent enough (`CanRead`), and with `503` otherwise.
- **Linearizable reads**: `GET ?consistent=1` goes through `Manager.Read`: the partition's owner confirms with a
  majority that it still owns the partition and answers from `Store.Read`, so the answer includes every write that
  returned before the request, on any node, and a node that cannot reach a majority answers `503` instead of a stale
  value.
- **Takeover**: kill a node and its partitions move to the next-ranked node after a few seconds; restart it and it
  catches up, then claims its partitions back (the owner that borrowed them hands them over).
- **Resync after data loss**: wipe a node's data directory and it refuses to take part (`Intact()` is false) until it has
  caught up from the others.

## Running

Build it once, then start three nodes on one machine, each in its own terminal. The first start of a brand new cluster
uses `-new-cluster`; the node IDs are fixed so that a restarted node is the same node.

```shell
cd examples/partition
go build -o kvpart .

./kvpart -bind 127.0.0.1:7001 -http 127.0.0.1:8001 -data /tmp/kvpart/n1 -new-cluster \
    -node-id 00000000-0000-4000-8000-000000000001 -peers 127.0.0.1:7002,127.0.0.1:7003
./kvpart -bind 127.0.0.1:7002 -http 127.0.0.1:8002 -data /tmp/kvpart/n2 -new-cluster \
    -node-id 00000000-0000-4000-8000-000000000002 -peers 127.0.0.1:7001,127.0.0.1:7003
./kvpart -bind 127.0.0.1:7003 -http 127.0.0.1:8003 -data /tmp/kvpart/n3 -new-cluster \
    -node-id 00000000-0000-4000-8000-000000000003 -peers 127.0.0.1:7001,127.0.0.1:7002
```

(`rm -rf /tmp/kvpart` first to start from nothing.) After a few seconds each node logs `ready`, and the partitions are
spread over the three:

```
INF [partition] took over partition.partition=3 partition.epoch=1@00000000 ...
INF [partition] ready
```

```shell
$ curl -s 'localhost:8001/status?brief'
{
  "node": "00000000-0000-4000-8000-000000000001",
  "ready": true,
  "resync": false,
  "owned": [0, 1, 4, 5, 8, 9, 12, 13]
}
```

(The spread of partitions over the nodes depends on their IDs; with these IDs node 1 owns eight of the sixteen and
nodes 2 and 3 four each. Your lists may be laid out differently.)

### Flags

| Flag | Default | |
| --- | --- | --- |
| `-bind` | `127.0.0.1:7001` | Address for gossip traffic (TCP and UDP); also what the other nodes use to reach this one |
| `-http` | `127.0.0.1:8001` | Address of the HTTP API |
| `-peers` | | Comma separated gossip addresses of the other nodes (this node's own address may be in the list) |
| `-cluster-size` | `3` | The configured number of nodes: the same on every node, never changed while running |
| `-partitions` | `16` | The number of partitions: the same on every node |
| `-data` | (required) | Data directory |
| `-new-cluster` | off | One of the first nodes of a brand new cluster: an empty data directory is trusted |
| `-resync` | off | The data was lost, restored from a backup or is suspect: catch up from the others first |
| `-node-id` | a new one each start | A UUID; give each node a fixed one |
| `-heartbeat` | `500ms` | Partition heartbeat interval; the owner timeout is three times this |
| `-log-level` | `info` | `trace`, `debug`, `info`, `warn` or `error` |

## Using it

Any node accepts any request. A write to a key goes to that key's owner, wherever it landed:

```shell
curl -i -X PUT localhost:8001/kv/greeting -d 'hello'        # 204, ETag: "1"; X-Partition tells which partition holds the key
curl -i localhost:8002/kv/greeting                           # 200, body: hello, ETag: "1" (a local read)
curl -i 'localhost:8002/kv/greeting?consistent=1'            # the same, read through the owner: current, even a moment after a PUT elsewhere
curl -i -X PUT localhost:8003/kv/greeting -d 'hello again'   # no precondition: always applied, 204, ETag: "2"
curl -i -X PUT localhost:8001/kv/greeting -d x -H 'If-Match: "1"'      # 412: the version is 2, not 1
curl -i -X PUT localhost:8001/kv/greeting -d x -H 'If-Match: "2"'      # 204: compare-and-swap succeeded
curl -i -X PUT localhost:8001/kv/new -d x -H 'If-None-Match: *'        # 204 the first time, 412 after
curl -i -X DELETE localhost:8001/kv/greeting                 # 204; a second DELETE is 404
curl -s localhost:8001/status                                # every partition: owner, epoch, committed sequence, keys
```

| Status | Meaning |
| --- | --- |
| `204` | Committed on a majority. A `PUT` carries the key's new version as its `ETag`. |
| `404` | No such key (`GET`, or `DELETE` of a missing key). |
| `412` | The `If-Match` / `If-None-Match` precondition failed; nothing was written. |
| `503` | Nothing was written (no owner reachable, no majority, or not ready) or, for `GET`, this node cannot serve the partition right now (`?consistent=1`: no owner reachable or no majority confirmed it). `Retry-After: 1`. |
| `504` | The outcome is unknown: the change was sent and the owner was lost; it may or may not have been applied. Read the key before retrying. |

Reads are local. The node that owns the key's partition always has the latest write. Any other node learns that a
write committed from the owner's commit notice, one message after the commit, so a `GET` on a node other than the
one that took a `PUT` can still, for that moment, return the old value (and if the notice is lost, until the owner's
next heartbeat, every `-heartbeat`, 500ms by default). The node that took the `PUT` waits (up to a second) until it
has applied the write itself, with `Manager.WaitApplied`, before it answers, so a `GET` there right afterwards
sees it. That is the bounded staleness: a node whose last sign of the owner is older than the owner timeout refuses
(`503`) instead of serving.

When a read has to be current (a decision that depends on the latest value), use `GET /kv/<key>?consistent=1`
instead. It is not answered from the node's copy: `Manager.Read` sends it to the key's owner, which first confirms
with a majority of the cluster that it still owns the partition (one round trip to the other nodes, shared by the
reads that arrive meanwhile) and then looks the key up (`Store.Read`). So it sees every write that returned before it
was sent, wherever either landed, and a node that was cut off from the cluster cannot answer it from old data; the price
is that round trip, plus the hop to the owner when the request lands elsewhere, and that it needs a majority of the
cluster reachable (a plain `GET` of the local copy does not, until the staleness bound runs out). A missing key is `404`.

## Trying the failure cases

Keep the three nodes running and write some keys through each, so there is something to lose:

```shell
for i in 1 2 3 4 5 6 7 8; do curl -s -X PUT localhost:800$((i % 3 + 1))/kv/key$i -d "value$i"; done
```

### Kill a node

Find the process of node 3 and kill it hard (`kill -9`), or press Ctrl+C in its terminal for a clean stop.

```shell
kill -9 $(pgrep -f 'bind 127.0.0.1:7003')
```

Keep writing through node 1 or 2:

```shell
for i in 1 2 3 4 5 6 7 8; do curl -s -o /dev/null -w "key$i %{http_code}\n" -X PUT localhost:8001/kv/key$i -d "after-kill-$i"; done
```

You should see: keys whose partitions node 3 did not own succeed at once (`204`). The requests for keys of its partitions
wait a few seconds, until the owner timeout has passed and the next-ranked node takes the partition over (the logs say
`took over`; it does not wait for the cluster's failure detector to drop the dead node), and then succeed (`204`): the
other two are a majority of three. A write waits for a takeover for up to five seconds; an outage that outlasts that returns `503` (nothing was
written; retry). `GET /status?brief` on node 1 and 2 now together list all sixteen partitions as `owned`. With a clean
stop (Ctrl+C) the node hands its partitions back and the gap is a fraction of a second.

### Restart it

Run node 3's command again, *without* `-new-cluster` (its data directory is trusted):

```shell
./kvpart -bind 127.0.0.1:7003 -http 127.0.0.1:8003 -data /tmp/kvpart/n3 \
    -node-id 00000000-0000-4000-8000-000000000003 -peers 127.0.0.1:7001,127.0.0.1:7002
```

You should see it log `partition manager started ... resync=false` and then `ready` after a few seconds;
`curl localhost:8003/kv/key1` returns `after-kill-1` (what it missed while it was down, fetched from the others by
catch-up); and once it is ready it claims its partitions back: the node that borrowed them logs `handed over on a claim`
for each and the returning node `took over`, and `GET`s of those partitions can answer `503` for a moment while they
change hands.

### Wipe a node and restart it

Stop node 2 and delete its data, then start it again with the same command, still without `-new-cluster`:

```shell
kill -9 $(pgrep -f 'bind 127.0.0.1:7002'); rm -rf /tmp/kvpart/n2
./kvpart -bind 127.0.0.1:7002 -http 127.0.0.1:8002 -data /tmp/kvpart/n2 \
    -node-id 00000000-0000-4000-8000-000000000002 -peers 127.0.0.1:7001,127.0.0.1:7003
```

You should see `resync=true` in its `partition manager started` line, then `resync complete` and `ready` in its log
(a second or so with this little data), `GET`s on it answering `503` until the resync is done (it has no data, so it
cannot promise or serve anything), and its keys back. It resyncs from a majority of the *other* nodes, so both other
nodes must be up: wipe a node while another is down and it waits. If you
restore a node's directory from a backup instead of wiping it, start it with `-resync` for the same effect.

Forgetting `-new-cluster` on the very first start of all three nodes also works, because they resync from each other,
but then all three must be up before any is ready. With `-new-cluster` any two are enough.

### Lose two nodes

Kill nodes 2 and 3. Node 1 keeps its data but is not a majority of three: every `PUT` waits five seconds and returns
`503`, and, after the owner timeout, so does every `GET` (a minority must not serve data that the majority might have
changed). Start one of them again and the cluster resumes. This is the cost of the guarantee that an acknowledged write is never lost, and
why `-cluster-size` is configured rather than inferred.

## How the store works

`store.go` is the whole durable side. The design choice is a **snapshot per partition**:

```
<data>/intact            the data directory's state can be trusted (see below)
<data>/p03/state.json    Promised and Pending: small, rewritten on every promise and every pending write
<data>/p03/data.json     Committed and every record, tombstones included, rewritten by a background flush
```

- **Commit** applies the write's records and the new committed position to the partition's data in memory in one
  atomic step and returns without making it durable (the package's optional deferred durability: a write is safe once a
  majority holds it as *pending*, and a commit a crash loses is redone from that). A background flush rewrites `data.json` (temporary file,
  fsync, rename: still atomic), once for a burst of commits. It costs time proportional to the partition's size per
  flush, which is fine here (and the number of partitions bounds it); an append-only log plus a periodic snapshot is
  the upgrade, and would change nothing outside `store.go`.
- **Order**: `SetPending` and `SetPromised` flush the committed data before they write `state.json`, so a crash never
  keeps a pending write (or a promise) and forgets the commit before it; the server also flushes when it stops. This
  ordering is what makes the deferred durability allowed: with a single pending slot, a lost commit followed by a
  durable pending write at the next sequence would leave the write on this node nowhere.
- **Pending** is cleared lazily: `Commit` does not touch `state.json`, and `State` hides a stored pending write whose
  sequence is not above the committed one. That saves a second fsync per commit and cannot be torn between two files.
- **Everything is also kept in memory**, so `State`, `Prepare`, `Export` and `GET` never read the disk. The store loads
  the directory when it opens, and removes temporary files left by a crash.
- **Export by sequence**: each record keeps the sequence of the write that last wrote it, so "changed after *n*" is
  a scan. A delete is a tombstone (a record with `del` set), so that it reaches a node that catches up. This store
  never purges them, so `Export` never returns `ErrPurged`; the price is that a deleted key stays on disk forever.
- **Versions** are decided by `Prepare` on the owner and travel inside the write, so every node applies the same bytes:
  a key's version counts its writes (tombstones included), and an `If-Match` compares with the live version (0 for a
  missing or deleted key).
- **Intact** is true when the data directory carries the `intact` marker, or when the node was started with
  `-new-cluster` and the directory is new; it is false for a node started with `-resync` or without a marker, which is
  how a wiped node knows to resync. `-resync` removes the marker, and the server writes it again from
  `HandleResyncedFunc`, when the node's resync is over, so a crash half way through a resync leaves the node needing
  one again.
- Each fsync matters: a durable write costs several milliseconds on a laptop SSD (a full fsync is about 3ms on an
  Apple SSD, two of them per file replacement). A write's critical path is one durable `SetPending`: the owner's, in parallel with one on each
  follower; `Commit` does not wait for the disk. Throughput comes from partitions working in parallel.

## Notes

- The gossip traffic here is neither encrypted nor compressed; see the other examples for how to configure that.
- Values are limited to 256KB: a change travels as one message to the owner and again to every follower.
- The `ETag` of a `PUT` is `Result.Reply`, the version the owner's `Prepare` decided; a `DELETE` has none. The status
  codes come from the `RefusedError` code the store refused with (`conflict`: 412, `not-found`: 404), not from message text.
- This is an example: a snapshot per partition, no authentication on the HTTP API, no cluster resizing.
- `go test -race ./examples/partition/` runs three servers in one process, writes through each, kills one, keeps
  writing, restarts it, wipes another and restarts that, and checks convergence through `GET`.
