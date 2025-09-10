package gossip

import (
	"github.com/paularlott/gossip/hlc"
)

type NodeState uint8

const (
	NodeUnknown NodeState = iota
	NodeAlive
	NodeSuspect
	NodeDead
	NodeLeaving
	NodeRemoved
)

func (ns NodeState) String() string {
	switch ns {
	case NodeUnknown:
		return "Unknown"
	case NodeAlive:
		return "Alive"
	case NodeSuspect:
		return "Suspect"
	case NodeDead:
		return "Dead"
	case NodeLeaving:
		return "Leaving"
	case NodeRemoved:
		return "Removed"
	default:
		return "Unknown"
	}
}

// Struct to hold our view of the state of a node within the cluster
type Node struct {
	ID                  NodeID
	advertiseAddr       string        // Raw advertise address (may contain SRV records, URLs, etc.)
	address             Address       // Resolved address (IP/Port or WebSocket URL) - resolved locally when needed
	localState          NodeState     // The local view of the node's state
	localStateTimestamp hlc.Timestamp // Local timestamp for the node's state (updated by the node)
	lastMessageTime     hlc.Timestamp // When we last received any message from this node (passive liveness check)
	Metadata            MetadataReader
	metadata            *Metadata
	ProtocolVersion     uint16
	ApplicationVersion  string
}

func newNode(id NodeID, advertiseAddr string) *Node {
	metadata := NewMetadata()

	now := hlc.Now()
	n := &Node{
		ID:                  id,
		advertiseAddr:       advertiseAddr,
		address:             Address{}, // Empty until resolved
		localState:          NodeAlive,
		localStateTimestamp: now,
		lastMessageTime:     now,
		Metadata:            metadata,
		metadata:            metadata,
	}

	return n
}

func (n *Node) updateLastActivity() {
	n.lastMessageTime = hlc.Now()
}

func (n *Node) getLastActivity() hlc.Timestamp {
	return n.lastMessageTime
}

func (node *Node) GetState() NodeState {
	return node.localState
}

func (node *Node) DeadOrLeft() bool {
	return node.localState == NodeDead || node.localState == NodeLeaving
}

func (node *Node) Alive() bool {
	return node.localState == NodeAlive
}

func (node *Node) Suspect() bool {
	return node.localState == NodeSuspect
}

func (node *Node) Removed() bool {
	return node.localState == NodeRemoved
}

// Address returns a pointer to the node's resolved address
func (node *Node) Address() *Address {
	return &node.address
}

// AdvertiseAddr returns the node's advertise address string
func (node *Node) AdvertiseAddr() string {
	return node.advertiseAddr
}
