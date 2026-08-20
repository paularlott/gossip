# Distributed Queue

A distributed message queue with at-least-once delivery built on top of the gossip cluster. Messages are coordinated via rendezvous hashing — the queue name deterministically maps to a coordinator node that holds the authoritative message state.

## Design

The queue uses a single-coordinator model per queue name (same as the lock package), with **push-based delivery**:

- A producer publishes to the coordinator via TCP request/response
- The coordinator holds pending messages in-memory
- Consumers **register** with the coordinator, declaring a prefetch limit
- The coordinator **pushes** messages to registered consumers as they arrive
- Consumers acknowledge (ack) after processing, which frees prefetch capacity
- Unacknowledged messages are redelivered after a visibility timeout
- If the coordinator dies, messages are handed off to the new coordinator
- If a consumer dies, its inflight messages are re-queued immediately

### Why push, not poll

An earlier design had consumers poll the coordinator every 50ms. That cost a fresh TCP connection per poll (~20/sec per node per queue even when completely idle) and added up to 50ms of latency to every message.

Push-based delivery removes both problems:

| | Poll (old) | Push (current) |
|---|---|---|
| Idle traffic | ~20 req/sec/node/queue | 1 heartbeat per `ConsumerHeartbeatInterval` (default 5s) |
| Delivery latency | 0–50ms of poll wait | one network hop (~1ms measured locally) |
| Scaling with queues | linear in queue count | flat |

The only periodic traffic is the consumer heartbeat, which also serves as the mechanism for re-attaching to a new coordinator after a membership change.

### Flow control (prefetch)

Each consumer declares a `Prefetch` limit (default 16). The coordinator tracks how many unacknowledged messages each consumer holds and never exceeds that limit, so:

- A slow consumer cannot be overwhelmed
- Work naturally flows to consumers with spare capacity
- Messages are distributed round-robin among consumers that have capacity

Pushes are answered as soon as the message is buffered locally — the coordinator does not wait for your handler to finish, so a long-running handler never blocks delivery to other consumers. Handlers run concurrently up to the prefetch limit.

### Handler panics

A panic inside a message handler is caught, converted to an error, and the message is nacked for redelivery. A bad handler cannot take down the node.

## Delivery Guarantees

- **At-least-once**: Messages are only removed after explicit acknowledgment. If a consumer fails, the message is redelivered.
- **FIFO per-queue**: Messages are delivered in the order they were published (within a single queue partition).
- **No duplicates in normal operation**: A message is delivered to exactly one consumer at a time. Duplicates can occur if a consumer processes a message but fails before acking.

## Usage

```go
import "github.com/paularlott/gossip/queue"

// Create a queue
q := queue.New(cluster, &queue.Config{
    Name:              "tasks",
    VisibilityTimeout: 30 * time.Second,
    MaxRetries:        3,
})
defer q.Close()

// Publish (fire-and-forget)
err := q.Publish(ctx, []byte("job payload"))

// Consume
q.Consume(func(msg *queue.Message) error {
    // Process msg.Payload
    fmt.Println("received:", string(msg.Payload))
    return nil // returning nil = ack
})
```

## Request/Reply

For RPC-style communication, use `Request` to publish a message and wait for a response from the consumer:

```go
// Producer side — blocks until worker replies or timeout
result, err := q.Request(ctx, []byte("prompt"), 30*time.Second)
if err != nil {
    log.Fatal(err)
}
fmt.Println("response:", string(result))

// Consumer side — reply to the caller
q.Consume(func(msg *queue.Message) error {
    result := process(msg.Payload)
    msg.Reply(result)
    return nil
})
```

Under the hood:
1. `Request` registers a correlation ID and publishes with reply metadata
2. The worker calls `msg.Reply(payload)` which sends directly to the caller's node
3. The caller's reply channel unblocks with the result

If the worker dies, the visibility timeout redelivers the message to another worker. The caller keeps waiting until the timeout or a new worker replies.

## Scoped Queues (NodeGroup)

Queues can be scoped to a subset of nodes:

```go
aiWorkers := gossip.NewNodeGroup(cluster, map[string]string{"role": "ai"}, nil)

q := queue.New(cluster, &queue.Config{
    Name:      "ai-tasks",
    NodeGroup: aiWorkers,
})
```

Only nodes in the group participate as coordinators and consumers. Any node in the cluster can publish.

## Multiple Queues

Multiple independent queues can coexist on the same cluster, each with a unique name:

```go
taskQueue := queue.New(cluster, &queue.Config{Name: "tasks"})
eventQueue := queue.New(cluster, &queue.Config{Name: "events"})
```

Message handlers are registered once per cluster and shared across all queues.

## Configuration

```go
type Config struct {
    Name                      string            // Queue name (default: "default")
    NodeGroup                 *gossip.NodeGroup // Scope to subset (nil = whole cluster)
    VisibilityTimeout         time.Duration     // Redelivery timeout (default: 30s)
    MaxRetries                int               // Max attempts before dead-letter (default: 3)
    MaxSize                   int               // Max pending messages, 0 = unlimited
    Prefetch                  int               // Max concurrent messages per consumer (default: 16)
    ConsumerHeartbeatInterval time.Duration     // Consumer re-announce interval (default: 5s)
    DeadLetterHandler         func(payload []byte, attempts int) // Called on max retries
}
```

Tuning notes:

- **`Prefetch`** trades throughput against fairness. Higher values keep a fast consumer busy; lower values spread work more evenly and reduce how much is stranded if a consumer dies. `1` gives strict one-at-a-time processing.
- **`ConsumerHeartbeatInterval`** bounds how long it takes a consumer to attach to a new coordinator after an unclean membership change (membership events trigger an immediate re-register, so this is only the fallback). Consumers are considered stale after three missed intervals.

## Failure Modes

| Scenario | Behaviour |
|----------|-----------|
| Consumer fails (returns error) | Message immediately re-queued for another attempt |
| Consumer handler panics | Panic contained, treated as an error, message re-queued |
| Consumer dies | Coordinator detects via health events, drops its registration and re-queues everything it held |
| Consumer stops heartbeating | Registration expires after 3 intervals; held messages re-queued |
| Consumer buffer full | Push is refused, message returns to the head of the queue and is retried |
| Push fails (network) | Consumer's registration is dropped and its messages re-queued |
| Consumer times out | Visibility timeout expires, message re-queued |
| Max retries exceeded | Message sent to DeadLetterHandler (or discarded if nil) |
| Coordinator crashes | Queue rehashes to a new coordinator. Messages held only in the crashed process are lost — they are in-memory and not replicated. |
| Handoff partially accepted | Messages are re-imported locally and retried by the periodic reconcile |
| Node joins | Coordinator recalculates, hands off messages if it's no longer coordinator |
| Node leaves (graceful) | `Close()` drains all messages to new coordinator before departing |
| Producer dies | Messages already on coordinator are processed normally |

## Coordinator Handoff

Same model as the lock package:
- On any membership change, each node recalculates whether it's still the coordinator
- If not, all messages (pending + inflight) are exported and sent to the new coordinator in a single batch
- On graceful shutdown (`Close()`), messages are drained to survivors before the node departs
- If a handoff fails, or the destination accepts fewer messages than were sent (for example it has no queue registered under that name yet), the messages are re-imported locally rather than dropped

Because queued messages have no TTL to fall back on, the handoff worker also **reconciles every 5 seconds** independently of membership events. This catches messages that a producer with a stale cluster view published to a coordinator that had already retired — without it those messages would sit unprocessed until the next membership change.

## Delivery Caveats

A few honest limitations to be aware of:

- **Messages are in-memory only.** They are not replicated and not persisted. If a coordinator process is killed (`SIGKILL`, panic, power loss) its pending and inflight messages are lost. Graceful shutdown drains them; abrupt termination does not.
- **`Close()` is the last chance to drain.** If the drain fails during `Close` (no reachable destination), the messages are gone.
- **Duplicates are possible.** At-least-once means a message may be processed more than once if a consumer completes work but dies before acking. Make handlers idempotent.
- **Ordering is per-queue, best-effort.** Redelivery pushes a failed message to the back of the queue, so a retried message is not re-delivered in its original position.

For workloads that need durable persistence across process restarts, use a purpose-built broker (NATS, Redis Streams, SQS). This queue is designed for in-cluster work distribution without external infrastructure.
