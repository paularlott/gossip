# Leader Example

Example of a simple leader election using gossip protocol to keep the nodes in sync.

## Running

### TCP/UDP

```shell
# Start a WebSocket only node
go run . --port=8000 --peers=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23

# Start a node with both TCP/UDP and WebSocket support
go run . --port=8001 --peers=127.0.0.1:8000,127.0.0.1:8002,127.0.0.1:8003 --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3

# Start a node with only TCP/UDP support
go run . --port=8002 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8003 --node-id=01960a1c-6852-7da4-a237-e41868ca3960

# Start a 2nd node with only TCP/UDP support
go run . --port=8003 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --node-id=0196165d-b2f6-739e-8ada-93f7837234d1
```

### WebSocket

```shell
# Start a WebSocket only node
go run . --web-port=8080 --peers=ws://127.0.0.1:8081/ --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23

# Start a node with both TCP/UDP and WebSocket support
go run . --web-port=8081 --peers=ws://127.0.0.1:8080/ --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3

# Start a node with only TCP/UDP support
go run . --web-port=8082 --peers=ws://127.0.0.1:8081/ --node-id=01960a1c-6852-7da4-a237-e41868ca3960

# Start a 2nd node with only TCP/UDP support
go run . --web-port=8083 --peers=ws://127.0.0.1:8081/ --node-id=0196165d-b2f6-739e-8ada-93f7837234d1
```
