# Gossip Protocol Library

This library implements a Gossip Protocol in Go.

## Advertised / Join Addresses

The advertised and bind addresses of a node can be any of the following, these same addresses can be given as the join address when starting a new node:

- IP:port
- hostname:port
- hostname
- IP
- srv+service-name
- wss://hostname:port/endpoint
- ip:port|wss://hostname:port/endpoint

For websocket connections a websocket provider must be given within the configuration.

## Cluster Events

Join and leave events are triggered for self when the node joins or leaves the cluster, `cluster.NodeIsLocal(node)` can be used to check if the event is for the local node.

## Building

### Msgpack

The gossip library relies on msgpack modules to serialize and deserialize messages.

Supported msgpack modules:

- Shamaton
- Vmihailenco

To build the library with the Shamaton module, use the following command:

```shell
go build -tags msgpack_shamaton
```

To build the library with the Vmihailenco module, use the following command:

```shell
go build -tags msgpack_vmihailenco
```

Run tests:

```shell
cd examples/test
go run . --port=8000 --web-port=8080 --peers=127.0.0.1:8001 --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23
go run . --port=8001 --web-port=8081 --peers=127.0.0.1:8000 --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3

go run . --port=8000 --web-port=8080 --peers=wss://127.0.0.1:8081/ --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23
go run . --port=8001 --web-port=8081 --peers=wss://127.0.0.1:8080/ --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3

go run . --web-port=8080 --peers=ws://127.0.0.1:8081/ --node-id=0196042b-1caa-7ad9-9ea3-c57b2e189b23
go run . --web-port=8081 --peers=ws://127.0.0.1:8080/ --node-id=01960421-c1c4-7a06-87a0-970cf4c4dbd3

go run . --peers=127.0.0.1:8001 --port=8002 --node-id=01960a1c-6852-7da4-a237-e41868ca3960
```
