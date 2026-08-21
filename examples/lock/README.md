# Lock Example

Interactive CLI demonstrating distributed advisory locks backed by leader election.

## Running

### TCP/UDP (3 nodes)

```shell
go run . --port=8000 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --min-nodes=2
go run . --port=8001 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --min-nodes=2
go run . --port=8002 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --min-nodes=2
```

### HTTP

```shell
go run . --web-port=8080 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/ --min-nodes=2
go run . --web-port=8081 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/ --min-nodes=2
go run . --web-port=8082 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/ --min-nodes=2
```

## Commands

| Command | Description |
|---------|-------------|
| `lock <key> <ttl_seconds>` | Acquire (blocks up to 10s) |
| `trylock <key> <ttl_seconds>` | Non-blocking acquire |
| `unlock <key>` | Release a held lock |
| `extend <key> <ttl_seconds>` | Refresh TTL |
| `query <key>` | Check who holds a key |
| `locks` | Show locally held locks |
| `leader` | Show current leader / warm-up state |
| `peers` | Show cluster members |

## Example Session

Node 1:
```
> trylock deploy 30
Acquired: key=deploy token=1.1
```

Node 2:
```
> trylock deploy 30
Not acquired: lock: not acquired

> query deploy
'deploy': owner=0196042b-... token=1.1 remaining=27.4s
```

Node 1:
```
> unlock deploy
Released: deploy
```

Node 2:
```
> trylock deploy 30
Acquired: key=deploy token=1.2
```
