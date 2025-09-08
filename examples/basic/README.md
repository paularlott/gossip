# Basic Example

Basic usage example, creates a cluster and joins nodes to it. Nodes can communicate over TCP/UDP or WebSocket.

## Running

### TCP/UDP

```shell
go run . --port=8000 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23
go run . --port=8001 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3
go run . --port=8002 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --node-id=01960a1c-6852-7da4-a237-e41868ca3960
go run . --port=8003 --peers=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 --node-id=0196165d-b2f6-739e-8ada-93f7837234d1
```

```shell
go run . --port=8000 --peers=127.0.0.1:8001 --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23
go run . --port=8001 --peers=127.0.0.1:8000 --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3
go run . --port=8002 --peers=127.0.0.1:8000 --node-id=01960a1c-6852-7da4-a237-e41868ca3960

go run . --web-port=8080 --peers=http://127.0.0.1:8081/ --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23
go run . --web-port=8081 --peers=http://127.0.0.1:8080/ --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3
go run . --web-port=8082 --peers=http://127.0.0.1:8080/ --node-id=01960a1c-6852-7da4-a237-e41868ca3960

```


### WebSocket

```shell
go run . --web-port=8080 --peers=http://127.0.0.1:8080/,http://127.0.0.1:8081/,http://127.0.0.1:8082/ --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23
go run . --web-port=8081 --peers=http://127.0.0.1:8080/,http://127.0.0.1:8081/,http://127.0.0.1:8082/  --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3
go run . --web-port=8082 --peers=http://127.0.0.1:8080/,http://127.0.0.1:8081/,http://127.0.0.1:8082/  --node-id=01960a1c-6852-7da4-a237-e41868ca3960
go run . --web-port=8083 --peers=http://127.0.0.1:8080/,http://127.0.0.1:8081/,http://127.0.0.1:8082/  --node-id=0196165d-b2f6-739e-8ada-93f7837234d1
```

Once running entering `peers` on any node will show the current state of the cluster.

Issuing `set-meta key value` on any node will set meta data fo the node the command is being run on, if everything is working correctly then running the `peers` command on any node will show the meta data.
