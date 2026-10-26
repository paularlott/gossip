# Gossip Protocol Library

A lightweight, Go-based library for implementing the gossip protocol in distributed systems. The library supports multiple transport mechanisms, including TCP, UDP, and HTTP, providing flexibility for a variety of use cases.

With its straightforward API, the library simplifies decentralized communication by enabling the creation and management of nodes, exchanging messages, and handling critical events like node joins and departures. Designed with reliability in mind, it continuously monitors node health within the cluster and seamlessly removes unreachable nodes, ensuring the system stays robust and adaptive.

The library leverages a hybrid approach to gossiping, combining **event-driven gossiping** and **periodic gossiping** to balance rapid updates and eventual consistency across the cluster:

- **Event-Driven Gossiping** swiftly propagates critical updates immediately after events occur, minimizing latency.
- **Periodic Gossiping** adds redundancy by disseminating updates at regular intervals, ensuring eventual consistency even if some nodes initially miss updates.

This flexible architecture supports the development of resilient distributed systems with efficient data sharing and robust fault tolerance.

## Features

- **Multiple Transport Support**: TCP, UDP, and HTTP connections
- **Message Security**: Optional encryption for message payloads
- **Compression**: Configurable message compression, support for Snappy is provide by default
- **Health Monitoring**: Automatic node health checking with direct and indirect pings
- **Flexible Codec Support**: Pluggable serialization with support for multiple msgpack implementations
- **Metadata Sharing**: Distribute custom node metadata across the cluster
- **Version Checking**: Application and protocol version compatibility verification
- **Direct Streams**: Node-to-node replies of any size for bulk data such as files or state transfers, over the socket and HTTP transports
- **Automatic Transport Selection**: UDP will be used wherever possible, however if the packet exceeds the MTU size, TCP will be used instead
- **[Distributed Locks](lock/)**: Advisory distributed locks backed by leader election, with fencing tokens and automatic failover
- **[Replicated KV Store](kv/)**: Leaderless, eventually-consistent key-value store with deterministic LWW merges, tombstoned deletes, TTLs, ack'd writes, and cluster- or group-scoped replication
- **[Leader Election](leader/)**: Quorum-based leader election with adaptive baseline and split-brain protection

## Installation

To install the library, use the following command:

```shell
go get github.com/paularlott/gossip
```

## Basic Usage

```go
package main

import (
  "fmt"
  "time"

  "github.com/paularlott/gossip"
  "github.com/paularlott/gossip/codec/shamaton"
  "github.com/paularlott/gossip/compression/snappy"
  "github.com/paularlott/gossip/encryption/aes"
)

func main() {
  // Create configuration
  config := gossip.DefaultConfig()
  config.NodeID = "01960f9b-72ca-7a51-9efa-47c12f42a138"       // Optional: auto-generated if not specified
  config.BindAddr = "127.0.0.1:8000"                           // Listen on TCP and UDP
  config.EncryptionKey = []byte("an-example-32-byte-key-123456789") // Optional: enables encryption (16, 24 or 32 bytes)
  config.Cipher = aes.New()                                    // Encryption algorithm
  config.MsgCodec = shamaton.New()                             // Message serialization
  config.Compressor = snappy.New()                             // Optional: enables compression

  // Create and start the cluster
  cluster, err := gossip.NewCluster(config)
  if err != nil {
    panic(err)
  }
	cluster.Start()
	defer cluster.Stop()

  // Join existing cluster (if any)
  err = cluster.Join([]string{"127.0.0.1:8001"})
  if err != nil {
    fmt.Println("Warning:", err)
  }

  // Register message handler
  const CustomMsg gossip.MessageType = gossip.UserMsg + 1
  cluster.HandleFunc(CustomMsg, func(sender *gossip.Node, packet *gossip.Packet) error {
    var message string
    if err := packet.UnmarshalPayload(&message); err != nil {
        return err
    }
    fmt.Printf("Received message from %s: %s\n", sender.ID, message)
    return nil
  })

  // Broadcast a message
  message := "Hello cluster!"
  cluster.Broadcast(CustomMsg, message)

  // Keep the application running
  select {}
}
```

## Message Handlers

Three kinds of handler receive the messages of a type, registered on the cluster:

- `HandleFunc(msgType, func(*Node, *Packet) error)` for a message sent to the cluster or to a node without waiting for an answer.
- `HandleFuncWithReply` (also `HandleFuncWithResponse`) for a request: the handler returns the reply, and the sender receives it from `SendToWithResponse` or `SendToWithResponseContext`.
- `HandleFuncWithDeferredReply` for a request that is answered later, from any goroutine.

The packet is pooled: decode it (`packet.Unmarshal`) inside the handler and do not keep it or its payload afterwards.

A `HandleFuncWithReply` handler runs on one of the cluster's incoming workers (`Config.NumIncomingWorkers`, 8 by default) until it returns, so a handler that waits for other nodes or for a disk occupies a worker for that time, and enough of them stop the node reading any other message. A deferred handler does not: it decodes the request, starts the work wherever it likes and returns, and answers through the `Replier` it was given when the work is done.

```go
const QueryMsg gossip.MessageType = gossip.UserMsg + 2

cluster.HandleFuncWithDeferredReply(QueryMsg, func(sender *gossip.Node, packet *gossip.Packet, r gossip.Replier) {
	var req Query
	if err := packet.Unmarshal(&req); err != nil { // decode first, before returning
		r.Reply(nil, err)
		return
	}
	go func() {
		result, err := lookup(req) // may take as long as it needs
		r.Reply(result, err)
	}()
})
```

`Reply(payload, nil)` sends the payload as the reply, exactly as the return value of a `HandleFuncWithReply` handler would be. `Reply(_, err)` with an error, or a nil payload, sends nothing (the requester gives up at its own deadline, as it does when a `HandleFuncWithReply` handler fails), and a reply to a requester that has already given up is dropped. `Reply` may be called from any goroutine and any time; the second call does nothing. The `Replier` holds the request packet until `Reply` is called, so call it on every path, with a nil payload when there is nothing to send. It works on the socket and HTTP transports, and for requests sent with `SendToWithResponseContext`. A message that is not a request (sent with `SendTo`, `Broadcast`) can also arrive at a deferred handler; `Reply` then only releases it.

## Address Formats

The gossip library supports multiple address formats for binding and connecting to the cluster:

- **IP:port** - Standard TCP/UDP address (e.g., 127.0.0.1:8000)
- **hostname:port** - DNS hostname with port, when multiple addresses are returned the node will attempt to connect to each address in turn assuming each is a node within the cluster
- **hostname** or **IP** - The default port will be used, for a hostname returning multiple addresses the node will attempt to connect to each address in turn assuming each is a node within the cluster
- **srv+service-name** - SRV DNS record lookup, when multiple addresses are returned the node will attempt to connect to each address in turn assuming each is a node within the cluster
- **https://hostname:port/endpoint** - HTTP connection
- **srv+https://hostname:port/endpoint** - Secure HTTP connection looking up the port number for the target

## Configuration Options

The `Config` struct provides extensive customization:

```go
config := gossip.DefaultConfig()

// Node identification
config.NodeID = "unique-node-id"               // Optional: auto-generated if not provided
config.BindAddr = "0.0.0.0:3500"               // Address to bind for listening
config.AdvertiseAddr = "192.168.1.1:3500"      // Address to advertise to peers (optional)

// Communication
config.EncryptionKey = []byte("an-example-32-byte-key-123456789") // Optional: enables encryption (16, 24 or 32 bytes)
config.Cipher = aes.New()                              // Encryption algorithm
config.PreviousEncryptionKeys = nil                    // Keys being rotated out: they decrypt, never encrypt
config.EncryptionKeyWindow = time.Hour                 // How long one derived traffic key is used (default 1h)
config.MaxPacketAge = 0                                // Refuse packets this far from the local clock (0 = off)
config.Compressor = snappy.New()                       // Enable payload compression using the provided compressor
config.CompressMinSize = 1024                          // Minimum size of a packet that will be considered for compression
config.Transport = gossip.NewSocketTransport(config)   // Socket based transport

// Custom dialer/listener (e.g. for overlay networks like Tailscale tsnet)
config.DialFunc = srv.Dial       // Custom TCP dial function, defaults to net.DialTimeout
config.ListenFunc = srv.Listen   // Custom TCP listen function, defaults to net.ListenTCP
                                 // Setting ListenFunc automatically enables ForceReliableTransport

// Logging
config.Logger = logger.NewNullLogger()                 // Default: no logging

// Streams
config.StreamIdleTimeout = 30 * time.Second  // How long a stream may wait for its next frame

// Request/response connection reuse (socket transport, Unix platforms)
config.TCPPoolSize = 16                      // Idle connections kept per peer; 0 dials for every request
config.TCPIdleTimeout = 60 * time.Second     // Server closes idle connections after this; clients reuse for half of it
```

`SendToWithResponse` reuses TCP connections per peer instead of dialing for every request, which avoids exhausting
ephemeral ports under load. A pooled connection is checked before use and discarded if the peer has closed it;
nothing is retried once a request has been sent, so a request is never delivered twice. Peers running an older
version close after one reply and are detected automatically (pooling to them is paused for a few minutes).

One-way reliable messages (`SendToReliable`, `SendReliable`, and any message too large for UDP) use the same pooled
connections, so a node under steady reliable traffic does not dial and close a connection per message. This applies to
peers that advertise it (current versions do, learnt from their join, ping and metadata messages); a peer that does not
gets a connection of its own per message, as before. A message that fails on a pooled connection that was closed
meanwhile is sent once more on a new connection, and a repeat the peer had in fact received is ignored by it.

### TCP-only operation

Set `config.ForceReliableTransport = true` to run the socket transport over TCP only: every message — gossip,
health probes and state exchange — is sent over TCP and no UDP listener is bound, so only the TCP port needs to be
reachable. Use it where UDP is blocked or unreliable (some container platforms, NAT'd or load-balanced networks).
All nodes in a cluster should use the same setting. Setting `ListenFunc` enables it automatically. See
`Example_tcpOnly` in `example_tcp_only_test.go`.

### Node helpers

`Node.Routable()` reports whether a node is alive or suspect — worth sending to — and `Cluster.RoutableNodes()`
returns all such nodes (the local node included). `AliveNodes()` remains the stricter set for quorum decisions.

## Deadlines and cancellation

`SendToWithResponse` is bounded by the transport's timeouts: `TCPDialTimeout` for each dial attempt and `TCPDeadline`
for writing the request and waiting for the reply. When the caller has a deadline of its own (a request handler, a
quorum round), pass it with `SendToWithResponseContext`: the dial, the write and the wait end at the context's
deadline or cancellation, whichever comes first, with the context's error.

```go
ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
defer cancel()
var resp Reply
err := cluster.SendToWithResponseContext(ctx, node, QueryMsg, &Query{Key: "k"}, &resp)
if errors.Is(err, context.DeadlineExceeded) {
	// the peer did not answer in time
}
```

`OpenStream` takes a context too, and it bounds the stream's dial as well as the reply. Stopping the cluster aborts
dials and exchanges in flight.

The built-in socket and HTTP transports implement `ContextTransport` (`SendWithReplyContext`). A custom transport that
implements only `Transport` keeps working unchanged: `SendToWithResponseContext` then stops waiting when the context
ends, and the exchange runs on until the transport's own timeouts.

## Direct Streams

Messages are limited to a single packet (`TCPMaxPacketSize`, 4MB by default) sent within `TCPDeadline`. For bulk data, a node can open a stream to another node: the request is sent as a packet and the reply, of any size, is read as an `io.Reader` from a direct connection. Streams are never gossiped.

```go
const FileMsg gossip.MessageType = gossip.UserMsg + 1

// Serving node: write the reply to w. Returning an error ends the stream
// with that error at the caller.
cluster.HandleStreamFunc(FileMsg, func(sender *gossip.Node, packet *gossip.Packet, w io.Writer) error {
	var req FileRequest
	if err := packet.Unmarshal(&req); err != nil {
		return err
	}
	f, err := os.Open(req.Path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	return err
})

// Calling node: read the reply as it arrives.
r, err := cluster.OpenStream(ctx, node, FileMsg, &FileRequest{Path: "data.bin"})
if err != nil {
	return err
}
defer r.Close()
_, err = io.Copy(dst, r) // io.EOF only once the whole reply has arrived
```

The reply travels in frames of up to 64KB. On the socket transport each frame is encrypted with the cluster key, as packets are; on the HTTP transport the connection's TLS protects it. A reader gets `io.EOF` only after a complete reply, the handler's error if it failed, and `io.ErrUnexpectedEOF` if the connection broke, so a partial reply is never mistaken for a whole one. Each frame must arrive within `StreamIdleTimeout`; cancelling the context passed to `OpenStream` aborts the stream. Streams are accepted only from known cluster members, and `OpenStream` returns `ErrStreamsUnsupported` on a transport without stream support.

## Encryption and Key Rotation

With an `EncryptionKey` and a cipher, every packet and stream frame is encrypted. With the AES cipher (any cipher
implementing `encryption.AEADCipher`) nodes of this version use a keyed format between themselves:

- **Traffic keys per window.** Each node derives a traffic key from the configured key for every
  `EncryptionKeyWindow` (default one hour, the same on every node), so no key encrypts more than a window's traffic.
  Clocks may differ by up to a window.
- **Authenticated header.** The packet flags are authenticated with the payload.
- **One byte of overhead.** A single byte in front of the ciphertext names the window and the key.
- **Upgrades.** Nodes advertise that they read the keyed format; a node that does not (an older release, or a
  cipher without an AEAD) is always sent the original format, and every node still reads the original format, so a
  cluster can be upgraded one node at a time.

The key of a running cluster is rotated with `Cluster.SetEncryptionKeys(current, previous...)` in three rounds over
every node, so no node ever receives traffic it cannot read:

1. add the new key as a previous key;
2. make the new key current and the old one previous;
3. once every node has done step 2, drop the old key.

`MaxPacketAge` (off by default) refuses packets whose message time is further than that from the local clock, and
keeps each message in the duplicate history until then, so a captured packet cannot be replayed once it has left the
history. Set it well above the clock skew your nodes can have.

## Logging

The library uses the [github.com/paularlott/logger](https://github.com/paularlott/logger) interface for logging. By default, a null logger is used which discards all log output. You can provide your own logger implementation by implementing the `logger.Logger` interface:

```go
type Logger interface {
    Trace(msg string, args ...any)
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
    Fatal(msg string, args ...any)
    With(key string, value any) Logger
    WithError(err error) Logger
    WithGroup(name string) Logger
}
```

The `Fatal` method logs the message and then calls `os.Exit(1)`, making it convenient for handling fatal errors without explicit exit calls.

### Using slog

The library provides examples using the slog implementation:

```go
import (
    "os"
    "github.com/paularlott/logger/slog"
)

// Create a console logger with colored output
logger := slog.New("debug", "console", os.Stderr)

// Configure the cluster with the logger
config := gossip.DefaultConfig()
config.Logger = logger
```

See the [examples/common/log.go](examples/common/log.go) file for a complete implementation using slog.

## Node States

Nodes in the cluster go through several states:

- **NodeUnknown** - Node state is unknown, nodes start in this state and change to NodeAlive when joining the cluster
- **NodeAlive** - Node is active and healthy
- **NodeSuspect** - Node might be unhealthy (pending confirmation)
- **NodeDead** - Node is confirmed dead
- **NodeLeaving** - Node is gracefully leaving the cluster

## Message Codecs

The library supports pluggable serialization. Each codec implementation lives in its own sub-package, so importing one does not pull in the others — Go's import system handles exclusion automatically, no build tags needed.

```go
import (
    "github.com/paularlott/gossip/codec/shamaton"
    // or: codec/vmihailenco
    // or: codec/hashicorp
    // or: codec (for JSON, which is stdlib)
)

// Shamaton msgpack (recommended — fastest and safest)
config.MsgCodec = shamaton.New()

// Vmihailenco msgpack
config.MsgCodec = vmihailenco.New()

// Hashicorp msgpack (ugorji/codec port)
config.MsgCodec = hashicorp.New()

// JSON (stdlib, no external dependency)
config.MsgCodec = codec.NewJSONCodec()
```

Similarly, compression implementations are split into sub-packages:

```go
import "github.com/paularlott/gossip/compression/snappy"

config.Compressor = snappy.New()
```

### Examples

The `examples` directory contains various examples demonstrating the library's capabilities. Each example is self-contained and can be run independently.

- **[basic](examples/basic)**: A basic usage example that creates a cluster and joins nodes to it. Nodes can communicate over TCP/UDP or HTTP.
- **[events](examples/events)**: Example that installs an event handler to display cluster events.
- **[usermessages](examples/usermessages)**: Example that demonstrates user defined message handling.
- **[kv](examples/kv)**: Example replicated key-value store built on the [kv package](kv/).
- **[kvpersist](examples/kvpersist)**: Example KV store with optional persistence, dumping snapshots to a readable JSON file and restoring from it on restart.
- **[leader](examples/leader)**: Example demonstrating leader election.
- **[lock](examples/lock)**: Example demonstrating distributed advisory locks.
