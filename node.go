package gossip

import (
	"sync"

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
	ID                 NodeID
	advertiseAddr      string        // Raw advertise address (may contain SRV records, URLs, etc.)
	address            Address       // Resolved address (IP/Port or URL) - resolved locally when needed
	observedState      NodeState     // The local view of the node's state
	observedStateTime  hlc.Timestamp // Local timestamp for the node's state (updated by the node)
	lastMessageTime    hlc.Timestamp // When we last received any message from this node (passive liveness check)
	Metadata           MetadataReader
	metadata           *Metadata
	ProtocolVersion    uint16
	ApplicationVersion string
	mu                 sync.RWMutex // Protects observedState reads
}

func newNode(id NodeID, advertiseAddr string) *Node {
	metadata := NewMetadata()

	now := hlc.Now()
	n := &Node{
		ID:                id,
		advertiseAddr:     advertiseAddr,
		address:           Address{}, // Empty until resolved
		observedState:     NodeAlive,
		observedStateTime: now,
		lastMessageTime:   now,
		Metadata:          metadata,
		metadata:          metadata,
	}

	return n
}

func (n *Node) updateLastActivity() {
	n.lastMessageTime = hlc.Now()
}

func (n *Node) getLastActivity() hlc.Timestamp {
	return n.lastMessageTime
}

func (node *Node) GetObservedState() NodeState {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.observedState
}

func (node *Node) DeadOrLeft() bool {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.observedState == NodeDead || node.observedState == NodeLeaving
}

func (node *Node) Alive() bool {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.observedState == NodeAlive
}

func (node *Node) Suspect() bool {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.observedState == NodeSuspect
}

func (node *Node) Removed() bool {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.observedState == NodeRemoved
}

// Address returns a pointer to the node's resolved address
func (node *Node) Address() *Address {
	return &node.address
}

// AdvertiseAddr returns the node's advertise address string
func (node *Node) AdvertiseAddr() string {
	return node.advertiseAddr
}
