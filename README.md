# Gossip Protocol Library

This library implements a Gossip Protocol in Go.

## Advertised Address

The advertised address of a node can be any of the following, these same addresses can be given as the join address when starting a new node:

- IP:port
- hostname:port
- hostname
- IP
- srv+service-name
- https://hostname:port/endpoint (for example, https://example.com:8000/gossip) [TODO]

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



go run --tags msgpack_vmihailenco,msgpack_shamaton  examples/test/main.go  --peers 127.0.0.1:8000,127.0.0.1:8001