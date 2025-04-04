package gossip

import (
	"github.com/google/uuid"
)

type NodeID uuid.UUID

func (n NodeID) String() string {
	return uuid.UUID(n).String()
}

// Indicates why we're selecting peers, allowing for purpose-specific optimizations
type peerSelectionPurpose int

const (
	purposeBroadcast     peerSelectionPurpose = iota // General message broadcast
	purposeStateExchange                             // Exchange node states
	purposeIndirectPing                              // Indirect ping requests
	purposeTTL                                       // TTL calculation
)

// EventListener defines the interface for receiving events
type EventListener interface {
	OnInit(cluster *Cluster)
	OnNodeJoined(node *Node)
	OnNodeLeft(node *Node)
	OnNodeDead(node *Node)
	OnNodeStateChanged(node *Node, prevState NodeState)
	OnNodeMetadataChanged(node *Node)
}

// Interface for decoupling the message serialization and deserialization
type MsgCodec interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

type CompressionCodec interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}
