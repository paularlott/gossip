# KV Store Example

Example of a simple key-value store using gossip protocol to keep the data in sync between the peers.

## Running

### TCP/UDP

```shell
go run . --port=8000 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23
go run . --port=8001 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3
go run . --port=8002 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --node-id=01960a1c-6852-7da4-a237-e41868ca3960
go run . --port=8003 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --node-id=0196165d-b2f6-739e-8ada-93f7837234d1
```

### WebSocket

```shell
go run . --web-port=8080 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/ --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23
go run . --web-port=8081 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/  --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3
go run . --web-port=8082 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/  --node-id=01960a1c-6852-7da4-a237-e41868ca3960
go run . --web-port=8083 --peers=ws://127.0.0.1:8080/,ws://127.0.0.1:8081/,ws://127.0.0.1:8082/  --node-id=0196165d-b2f6-739e-8ada-93f7837234d1
```


Once running entering `peers` on any node will show the current state of the cluster.

### Commands

`keys` - List all keys in the store
`get <key>` - Get the value of a key
`set <key> <value>` - Set the value of a key
`delete <key>` - Delete a key
