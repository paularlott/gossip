//go:build unix

package gossip

import (
	"net"
	"syscall"
	"time"
)

// connAlive reports whether an idle pooled connection is still usable, without blocking: a peer that closed it (an
// older version after its single reply, an idle timeout, a restart) has left EOF or a reset in the socket. ok is
// false when the connection does not expose its socket (e.g. a custom DialFunc), in which case it is not pooled.
func connAlive(c net.Conn) (alive, ok bool) {
	sc, isSC := c.(syscall.Conn)
	if !isSC {
		return false, false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return false, false
	}
	// The previous request's read deadline is still set; an expired deadline would fail the check below.
	if c.SetReadDeadline(time.Time{}) != nil {
		return false, true
	}
	var buf [1]byte
	err = raw.Read(func(fd uintptr) bool {
		for {
			_, _, err := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
			if err == syscall.EINTR { // interrupted (e.g. by the runtime's preemption signal): ask again
				continue
			}
			// Nothing to read is the healthy idle state. EOF, data the client is not expecting, or any other
			// error mean the connection must not carry another request.
			alive = err == syscall.EAGAIN || err == syscall.EWOULDBLOCK
			return true
		}
	})
	return alive && err == nil, true
}
