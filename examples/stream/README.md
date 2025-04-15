# Stream Example

Example to demonstrate opening a stream to another node and sending data between the two nodes.

## Running

### TCP/UDP

```shell
# Start a WebSocket only node
go run . --port=8000 --peers=127.0.0.1:8001 --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23

# Start a node with both TCP/UDP and WebSocket support
go run . --port=8001 --peers=127.0.0.1:8000 --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3
```

### WebSocket

```shell
# Start a WebSocket only node
go run . --web-port=8080 --peers=ws://127.0.0.1:8081/ --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23

# Start a node with both TCP/UDP and WebSocket support
go run . --web-port=8081 --peers=ws://127.0.0.1:8080/ --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3
```

Once running entering `peers` on any node will show the current state of the cluster.

Issuing the command `stream <nodeid> <message>` will send the message over a stream to the remote node, the node will then respond and both will output the message to the console.
