package gossip

// This file provides exported helpers for tests in dependent modules. They let
// a test construct the inputs that the cluster's message handlers receive
// (a sender Node carrying metadata, e.g. a zone) without standing up a live
// cluster or transport. Packets can be built directly with NewPacket together
// with the exported Packet.SetCodec / Packet.SetPayload methods.

// NewTestNode builds a Node with the given id, advertise address and metadata.
// It is intended for use by tests in other modules that need to drive handlers
// which read node metadata — for example a handler that filters messages by the
// sender's "zone" metadata. The node is marked alive.
func NewTestNode(id NodeID, advertiseAddr string, metadata map[string]string) *Node {
	n := newNode(id, advertiseAddr)
	for k, v := range metadata {
		n.metadata.SetString(k, v)
	}
	return n
}
