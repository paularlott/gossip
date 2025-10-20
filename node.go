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
	tags               []string      // Tags for tag-based message routing (immutable after creation)
	Metadata           MetadataReader
	metadata           *Metadata
	ProtocolVersion    uint16
	ApplicationVersion string
	mu                 sync.RWMutex // Protects observedState reads
}

// newNode creates a new node with optional tags
// Use newNode(id, addr) for no tags, or newNode(id, addr, tags...) for tags
func newNode(id NodeID, advertiseAddr string, tags ...string) *Node {
	metadata := NewMetadata()

	now := hlc.Now()
	n := &Node{
		ID:                id,
		advertiseAddr:     advertiseAddr,
		address:           Address{}, // Empty until resolved
		observedState:     NodeAlive,
		observedStateTime: now,
		lastMessageTime:   now,
		tags:              tags,
		Metadata:          metadata,
		metadata:          metadata,
	}

	return n
}

// newNodeWithTags is deprecated, use newNode with variadic tags instead
// Kept for backward compatibility during transition
func newNodeWithTags(id NodeID, advertiseAddr string, tags []string) *Node {
	return newNode(id, advertiseAddr, tags...)
}

func (n *Node) updateLastActivity() {
	n.mu.Lock()
	n.lastMessageTime = hlc.Now()
	n.mu.Unlock()
}

func (n *Node) getLastActivity() hlc.Timestamp {
	n.mu.RLock()
	defer n.mu.RUnlock()
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

// GetTags returns a copy of the node's tags.
// Tags are immutable (set at node creation), but we return a defensive copy
// to prevent accidental modification by callers.
// Note: For performance-critical paths, consider using HasTag() instead of GetTags()
func (node *Node) GetTags() []string {
	if node.tags == nil {
		return []string{}
	}
	// Return a copy to prevent external modification
	tagsCopy := make([]string, len(node.tags))
	copy(tagsCopy, node.tags)
	return tagsCopy
}

// HasTag returns true if the node has the specified tag
func (node *Node) HasTag(tag string) bool {
	for _, t := range node.tags {
		if t == tag {
			return true
		}
	}
	return false
}
