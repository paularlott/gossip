package gossip

import (
	"context"
	"fmt"
	"sync"
)

type TransportType int

const (
	TransportBestEffort = iota
	TransportReliable
)

var (
	ErrNoTransportAvailable = fmt.Errorf("no transport available") // When there's no available transport between two nodes
)

// Transport defines the interface for packet-based communication
type Transport interface {
	// Get the transport's name
	Name() string

	// Start the transport server
	Start(ctx context.Context, wg *sync.WaitGroup) error

	// PacketChannel returns the channel for incoming packets
	PacketChannel() chan *Packet

	// Send sends a packet to specific node using the specified transport type
	Send(transportType TransportType, node *Node, packet *Packet) error

	// SendWithReply sends a packet and waits for a reply
	SendWithReply(node *Node, packet *Packet) (*Packet, error)
}
