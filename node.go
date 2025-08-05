package gossip

import (
	"sync/atomic"
	"time"

	"github.com/paularlott/gossip/hlc"
)

type NodeState uint8

const (
	NodeUnknown NodeState = iota
	NodeAlive
	NodeLeaving
	NodeDead
	NodeSuspect
)

func (ns NodeState) String() string {
	switch ns {
	case NodeUnknown:
		return "Unknown"
	case NodeAlive:
		return "Alive"
	case NodeLeaving:
		return "Leaving"
	case NodeDead:
		return "Dead"
	case NodeSuspect:
		return "Suspect"
	default:
		return "Unknown"
	}
}

type Node struct {
	ID                 NodeID
	advertiseAddr      string  // Raw advertise address (may contain SRV records, URLs, etc.)
	address            Address // Resolved address (IP/Port or WebSocket URL) - resolved locally when needed
	stateChangeTime    hlc.Timestamp
	state              NodeState
	lastActivity       atomic.Int64 // Timestamp of last message received
	Metadata           MetadataReader
	metadata           *Metadata
	ProtocolVersion    uint16
	ApplicationVersion string
}

func newNode(id NodeID, advertiseAddr string) *Node {
	metadata := NewMetadata()

	n := &Node{
		ID:              id,
		advertiseAddr:   advertiseAddr,
		address:         Address{}, // Empty until resolved
		stateChangeTime: hlc.Now(),
		state:           NodeAlive,
		Metadata:        metadata,
		metadata:        metadata,
	}

	n.lastActivity.Store(time.Now().UnixNano())

	return n
}

func (n *Node) updateLastActivity() {
	n.lastActivity.Store(time.Now().UnixNano())
}

func (n *Node) getLastActivity() time.Time {
	nano := n.lastActivity.Load()
	return time.Unix(0, nano)
}

func (node *Node) GetState() NodeState {
	return node.state
}

func (node *Node) GetAdvertisedAddress() string {
	return node.advertiseAddr
}

func (node *Node) DeadOrLeft() bool {
	return node.state == NodeDead || node.state == NodeLeaving
}

func (node *Node) Alive() bool {
	return node.state == NodeAlive
}

func (node *Node) Suspect() bool {
	return node.state == NodeSuspect
}
