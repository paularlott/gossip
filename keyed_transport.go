package gossip

import (
	"errors"
	"fmt"
)

// Capabilities a node advertises in its self-describing messages (join, ping, pong, metadata), so peers use them
// only with nodes known to have them.
const (
	// capKeepsConnections: after answering a request the node keeps the connection open for the next one, so a peer
	// may pool it. Peers that predate connection reuse close it, and reusing it would fail a request.
	capKeepsConnections uint32 = 1 << 0
)

// socketTransport is the cluster's socket transport, also when Transport wraps it (see Config.socket); nil for
// other transports.
func (c *Cluster) socketTransport() *SocketTransport {
	if st, ok := c.transport.(*SocketTransport); ok {
		return st
	}
	return c.config.socket
}

// capabilities is what this node advertises.
func (c *Cluster) capabilities() uint32 {
	if st := c.socketTransport(); st != nil {
		return st.capabilities()
	}
	return 0
}

// learn records what a node advertised about itself.
func (n *Node) learn(keyWindow, caps uint32) {
	n.keyWindow.Store(keyWindow)
	n.caps.Store(caps)
}

// keyWindow is the key window this node advertises to its peers, which then send it the keyed format.
func (c *Cluster) keyWindow() uint32 {
	if st := c.socketTransport(); st != nil {
		return st.encryptionWindow()
	}
	return 0
}

// SetEncryptionKeys replaces the cluster's encryption keys while it runs: current encrypts everything sent from now
// on, and current and previous all decrypt. To rotate the key of a running cluster without losing any traffic:
//
//  1. on every node, add the new key to previous;
//  2. on every node, make the new key current and the old one previous;
//  3. once every node has done step 2, remove the old key.
//
// The keys must be 16, 24 or 32 bytes, and the cluster must have been started with encryption.
func (c *Cluster) SetEncryptionKeys(current []byte, previous ...[]byte) error {
	st := c.socketTransport()
	if st == nil {
		return errors.New("the transport does not support replacing encryption keys")
	}
	if err := st.setEncryptionKeys(current, previous); err != nil {
		return fmt.Errorf("encryption keys not replaced: %w", err)
	}
	return nil
}
