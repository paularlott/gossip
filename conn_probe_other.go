//go:build !unix

package gossip

import "net"

// connAlive cannot inspect sockets on this platform, so request/response connections are not pooled.
func connAlive(net.Conn) (alive, ok bool) { return false, false }
