# Gossip Protocol Library

A lightweight, Go-based library for implementing the gossip protocol in distributed systems. The library supports multiple transport mechanisms, including TCP, UDP, and WebSocket, providing flexibility for a variety of use cases.

With its straightforward API, the library simplifies decentralized communication by enabling the creation and management of nodes, exchanging messages, and handling critical events like node joins and departures. Designed with reliability in mind, it continuously monitors node health within the cluster and seamlessly removes unreachable nodes, ensuring the system stays robust and adaptive.

The library leverages a hybrid approach to gossiping, combining **event-driven gossiping** and **periodic gossiping** to balance rapid updates and eventual consistency across the cluster:

- **Event-Driven Gossiping** swiftly propagates critical updates immediately after events occur, minimizing latency.
- **Periodic Gossiping** adds redundancy by disseminating updates at regular intervals, ensuring eventual consistency even if some nodes initially miss updates.

This flexible architecture supports the development of resilient distributed systems with efficient data sharing and robust fault tolerance.

## Features

- **Multiple Transport Support**: TCP, UDP, and WebSocket (both ws and wss) connections
- **Pluggable WebSocket Providers**: Support for both Gorilla and Coder WebSocket implementations
- **Message Security**: Optional encryption for message payloads
- **Compression**: Configurable message compression, support for Snappy is provide by default
- **Health Monitoring**: Automatic node health checking with direct and indirect pings
- **Flexible Codec Support**: Pluggable serialization with support for multiple msgpack implementations
- **Metadata Sharing**: Distribute custom node metadata across the cluster
- **Version Checking**: Application and protocol version compatibility verification
- **Automatic Transport Selection**: UDP will be used wherever possible, however if the packet exceeds the MTU size, TCP will be used instead
- **Node to Node Streams**: Support for streaming data between nodes over TCP and WebSocket connections

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
  "github.com/paularlott/gossip/codec"
  "github.com/paularlott/gossip/compression"
  "github.com/paularlott/gossip/websocket"
)

func main() {
  // Create configuration
  config := gossip.DefaultConfig()
  config.NodeID = "01960f9b-72ca-7a51-9efa-47c12f42a138"       // Optional: auto-generated if not specified
  config.BindAddr = "127.0.0.1:8000"                           // Listen on TCP and UDP
  config.EncryptionKey = "your-32-byte-key"                    // Optional: enables encryption
	config.Cipher = encryption.NewAESEncryptor()                 // Encryption algorithm
  config.MsgCodec = codec.NewShamatonMsgpackCodec()            // Message serialization
  config.Compressor = compression.NewSnappyCompressor()        // Optional: enables compression

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

## Address Formats

The gossip library supports multiple address formats for binding and connecting to the cluster:

- **IP:port** - Standard TCP/UDP address (e.g., 127.0.0.1:8000)
- **hostname:port** - DNS hostname with port, when multiple addresses are returned the node will attempt to connect to each address in turn assuming each is a node within the cluster
- **hostname** or **IP** - The default port will be used, for a hostname returning multiple addresses the node will attempt to connect to each address in turn assuming each is a node within the cluster
- **srv+service-name** - SRV DNS record lookup, when multiple addresses are returned the node will attempt to connect to each address in turn assuming each is a node within the cluster
- **ws://hostname:port/endpoint** - WebSocket connection
- **wss://hostname:port/endpoint** - Secure WebSocket connection

## Configuration Options

The `Config` struct provides extensive customization:

```go
config := gossip.DefaultConfig()

// Node identification
config.NodeID = "unique-node-id"               // Optional: auto-generated if not provided
config.BindAddr = "0.0.0.0:3500"               // Address to bind for listening
config.AdvertiseAddr = "192.168.1.1:3500"      // Address to advertise to peers (optional)

// Communication
config.EncryptionKey = "your-32-byte-key"              // Optional: enables encryption
config.Cipher = encryption.NewAESEncryptor()           // Encryption algorithm
config.Compressor = compression.NewSnappyCompressor()  // Enable payload compression using the provided compressor
config.CompressMinSize = 1024                          // Minimum size of a packet that will be considered for compression

// Networking, optional but if given allows use of WebSockets for transport and disables TCP/UDP
config.WebsocketProvider = websocket.NewGorillaProvider(5*time.Second, true, "")
config.AllowInsecureWebsockets = true
config.SocketTransportEnabled = false
```

## Node States

Nodes in the cluster go through several states:

- **NodeUnknown** - Node state is unknown, nodes start in this state and change to NodeAlive when joining the cluster
- **NodeAlive** - Node is active and healthy
- **NodeSuspect** - Node might be unhealthy (pending confirmation)
- **NodeDead** - Node is confirmed dead
- **NodeLeaving** - Node is gracefully leaving the cluster

## WebSocket Support

The library provides adapters for two WebSocket implementations allowing you to choose the one that fits with the rest of your application:

```go
// Using Gorilla WebSockets
config.WebsocketProvider = websocket.NewGorillaProvider(5*time.Second, true, "")

// Using Coder WebSockets
config.WebsocketProvider = websocket.NewCoderProvider(5*time.Second, true, "")
```

## Message Codecs

Multiple serialization options are available allowing you to choose the one that best fits your application:

```go
// Using Shamaton msgpack
config.MsgCodec = codec.NewShamatonMsgpackCodec()

// Using Vmihailenco msgpack
config.MsgCodec = codec.NewVmihailencoMsgpackCodec()

// Using JSON
config.MsgCodec = codec.NewJSONCodec()
```

### Examples

The `examples` directory contains various examples demonstrating the library's capabilities. Each example is self-contained and can be run independently.

- **[basic](examples/basic)**: A basic usage example that creates a cluster and joins nodes to it. Nodes can communicate over TCP/UDP or WebSocket.
- **[events](examples/events)**: Example that installs an event handler to display cluster events.
- **[usermessages](examples/usermessages)**: Example that demonstrates user defined message handling.
- **[kv](examples/kv)**: Example Key Value store.
- **[Stream](examples/stream)**: Example using the stream functions to pass data between nodes.
- **[leader](examples/leader)**: Example demonstrating leader election.
