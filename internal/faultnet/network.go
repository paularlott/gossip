package faultnet

import (
	"fmt"
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

// Network holds the transports of many nodes by NodeID and applies faults
// across them.
type Network struct {
	mu    sync.Mutex
	nodes map[gossip.NodeID]*Transport
	addrs map[string]gossip.NodeID // advertise address -> node (SetAddress)
}

// NewNetwork returns an empty network.
func NewNetwork() *Network {
	return &Network{nodes: make(map[gossip.NodeID]*Transport), addrs: make(map[string]gossip.NodeID)}
}

// Add registers a node's transport. The ID is known once the cluster is
// created: build the transport, set it as Config.Transport, create the cluster,
// then Add(cluster.LocalNode().ID, transport).
func (n *Network) Add(id gossip.NodeID, t *Transport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = t
	t.network.Store(n)
}

// SetAddress records a node's advertise address, so that the faults aimed at the node also cover what other nodes send
// to that address before they know its ID (a join). Without it a Block, and so a Split, lets joins through.
func (n *Network) SetAddress(id gossip.NodeID, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.addrs[addr] = id
}

// idAt returns the node recorded at an advertise address.
func (n *Network) idAt(addr string) (gossip.NodeID, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	id, ok := n.addrs[addr]
	return id, ok
}

// Transport returns the transport registered for id, or nil.
func (n *Network) Transport(id gossip.NodeID) *Transport {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodes[id]
}

// must returns the transport for id and panics when there is none: a fault
// aimed at an unregistered node is a bug in the test.
func (n *Network) must(id gossip.NodeID) *Transport {
	if t := n.Transport(id); t != nil {
		return t
	}
	panic(fmt.Sprintf("faultnet: node %s is not registered in the network", id))
}

// Split severs every link between the two groups in both directions; nodes
// within a group still reach each other.
func (n *Network) Split(groupA, groupB []gossip.NodeID) {
	for _, a := range groupA {
		for _, b := range groupB {
			n.must(a).Block(b)
			n.must(b).Block(a)
		}
	}
}

// OneWay drops the packets from one node to another; the other direction
// still flows.
func (n *Network) OneWay(from, to gossip.NodeID) {
	n.must(from).Block(to)
}

// Isolate cuts the node off from everyone (Transport.Cut). Heal restores it.
func (n *Network) Isolate(id gossip.NodeID) {
	n.must(id).Cut()
}

// Delay delays the packets from one node to another (Transport.Delay).
func (n *Network) Delay(from, to gossip.NodeID, min, max time.Duration) {
	n.must(from).Delay(to, min, max)
}

// DropRate drops a fraction of the packets from one node to another.
func (n *Network) DropRate(from, to gossip.NodeID, p float64) {
	n.must(from).DropRate(to, p)
}

// DropReply loses a fraction of the replies to requests from one node to
// another: the request is handled by the peer, the caller sees ErrDropped
// (Transport.DropReplyRate).
func (n *Network) DropReply(from, to gossip.NodeID, p float64) {
	n.must(from).DropReplyRate(to, p)
}

// Duplicate duplicates a fraction of the packets from one node to another.
func (n *Network) Duplicate(from, to gossip.NodeID, p float64) {
	n.must(from).Duplicate(to, p)
}

// Heal clears every fault on every node.
func (n *Network) Heal() {
	n.mu.Lock()
	ts := make([]*Transport, 0, len(n.nodes))
	for _, t := range n.nodes {
		ts = append(ts, t)
	}
	n.mu.Unlock()
	for _, t := range ts {
		t.Heal()
	}
}
