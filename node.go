package gossip

import (
	"sync/atomic"
	"time"
)

type NodeState uint8

const (
	nodeAlive NodeState = iota
	nodeLeaving
	nodeDead
	nodeSuspect
)

func (ns NodeState) String() string {
	switch ns {
	case nodeAlive:
		return "Alive"
	case nodeLeaving:
		return "Leaving"
	case nodeDead:
		return "Dead"
	case nodeSuspect:
		return "Suspect"
	default:
		return "Unknown"
	}
}

type Node struct {
	ID              NodeID
	address         Address
	stateChangeTime time.Time
	state           NodeState
	lastActivity    atomic.Int64 // Timestamp of last message received
	Metadata        MetadataReader
	metadata        *Metadata
}

func newNode(id NodeID, address Address) *Node {
	metadata := NewMetadata()

	n := &Node{
		ID:              id,
		address:         address,
		stateChangeTime: time.Now(),
		state:           nodeAlive,
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

func (node *Node) GetAddress() Address {
	return node.address
}

func (node *Node) DeadOrLeft() bool {
	return node.state == nodeDead || node.state == nodeLeaving
}

func (node *Node) Alive() bool {
	return node.state == nodeAlive
}

func (node *Node) Suspect() bool {
	return node.state == nodeSuspect
}

// Checks if this node shares at least one transport with the remote node.
func (node *Node) HasCompatibleTransport(remote *Node) bool {
	return (node.address.Port != 0 && remote.address.Port != 0) || (node.address.URL != "" && remote.address.URL != "")
}
