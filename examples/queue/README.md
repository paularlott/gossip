# Queue Example

Example of a distributed message queue using the gossip protocol. Demonstrates publishing, consuming, request/reply, and at-least-once delivery across multiple nodes.

## How It Works

The queue uses rendezvous hashing to assign a coordinator node that holds all pending messages. Consumers register with the coordinator, which then pushes messages to them as they arrive. Consumers ack after processing. If a consumer fails, the message is redelivered to another consumer.

Key properties:
- Push-based delivery — no polling, so an idle queue is silent and delivery is a single hop
- Prefetch flow control — a consumer never holds more than `--prefetch` messages at once
- At-least-once delivery — messages are only removed after ack
- Visibility timeout — unacked messages redeliver automatically
- Request/reply — producers can wait for a response from the worker
- Graceful handoff — queue state migrates when nodes join or leave

## Running

Start a 3-node cluster. Two nodes act as producers, one as a consumer:

### TCP/UDP

Terminal 1 (consumer):
```shell
go run . --port=8000 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --consume --prefetch=4
```

Terminal 2 (producer):
```shell
go run . --port=8001 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002
```

Terminal 3 (producer):
```shell
go run . --port=8002 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002
```

### HTTP

```shell
go run . --web-port=8080 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/ --consume
go run . --web-port=8081 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/
go run . --web-port=8082 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/
```

## Commands

| Command | Description |
|---------|-------------|
| `publish <message>` | Publish a fire-and-forget message |
| `request <message>` | Publish and wait for the consumer's reply (10s timeout) |
| `status` | Show local queue pending/inflight counts and registered consumers |
| `peers` | Show cluster members |
| `help` | Show all commands |

## Example Session

On the consumer node (Terminal 1):
```
> Consumer started, waiting for messages...
INFO Processing message id=abc123 payload="hello world" attempt=1
```

On a producer node (Terminal 2):
```
> publish hello world
Published: hello world

> request compute something
Sending request: compute something (waiting up to 10s)...
Reply: processed: compute something
```

## Multiple Consumers

You can run multiple consumer nodes — messages are distributed across them (one delivery per message):

```shell
go run . --port=8000 --peers=... --consume
go run . --port=8001 --peers=... --consume
go run . --port=8002 --peers=...  # producer only
```

If a consumer crashes, its inflight messages are redelivered to surviving consumers.
