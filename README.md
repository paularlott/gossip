# Gossip Protocol Library

A Go-based library designed for implementing gossip protocols in distributed systems, supporting multiple transport mechanisms including TCP, UDP, and WebSocket.

This library enables decentralized communication between nodes in distributed systems with ease. Its simple API allows you to create and manage nodes, exchange messages, and handle key events like node joins and departures. Built with robustness in mind, the library automatically monitors node health within the cluster and efficiently removes nodes that become unreachable.

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
  config.BindAddr = "127.0.0.1:8000|ws://127.0.0.1:8080"       // Listen on TCP and WebSocket
  config.EncryptionKey = "your-32-byte-key"                    // Optional: enables encryption
  config.MsgCodec = codec.NewShamatonMsgpackCodec()            // Message serialization
  config.Compressor = compression.NewSnappyCompressor()        // Optional: enables compression
  config.WebsocketProvider = websocket.NewGorillaProvider(5*time.Second, true, "")

  // Create and start the cluster
  cluster, err := gossip.NewCluster(config)
  if err != nil {
    panic(err)
  }
  defer cluster.Shutdown()

  // Join existing cluster (if any)
  err = cluster.Join([]string{"127.0.0.1:8001", "ws://127.0.0.1:8081"})
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
- **ip:port|ws://hostname:port/endpoint** - Multiple transport options combined, the TCP/UDP address must be first, followed by the WebSocket address. The node will favour TCP/UDP

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
config.Compressor = compression.NewSnappyCompressor()  // Enable payload compression using the provided compressor
config.CompressMinSize = 1024                          // Minimum size of a packet that will be considered for compression

// Networking, optional but if given allows use of WebSockets for transport
config.WebsocketProvider = websocket.NewGorillaProvider(5*time.Second, true, "")
```

## Node States

Nodes in the cluster go through several states:

- **nodeAlive** - Node is active and healthy
- **nodeSuspect** - Node might be unhealthy (pending confirmation)
- **nodeDead** - Node is confirmed dead
- **nodeLeaving** - Node is gracefully leaving the cluster

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
