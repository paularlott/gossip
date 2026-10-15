package gossip

import (
	"net"
	"sync"
	"time"
)

// connPool keeps idle request/response connections per peer address so SendWithReply does not dial (and leave a
// socket in TIME_WAIT) for every call. A connection is used by one request at a time.
type connPool struct {
	mu      sync.Mutex
	max     int           // idle connections kept per address; 0 disables pooling
	maxIdle time.Duration // a connection idle longer than this is not reused (below the server's idle timeout)
	idle    map[string][]pooledConn
	noReuse map[string]time.Time // addresses whose peer closed a reused connection (an older server); not pooled until then
	closed  bool
}

type pooledConn struct {
	conn  net.Conn
	since time.Time
	uses  int // replies received on this connection
}

// noReuseFor is how long pooling stays off for an address after its peer refused a reused connection.
const noReuseFor = 5 * time.Minute

func newConnPool(max int, serverIdle time.Duration) *connPool {
	return &connPool{max: max, maxIdle: serverIdle / 2, idle: map[string][]pooledConn{}, noReuse: map[string]time.Time{}}
}

func (p *connPool) enabled(addr string) bool {
	if p.max <= 0 || addr == "" {
		return false
	}
	if until, ok := p.noReuse[addr]; ok {
		if time.Now().Before(until) {
			return false
		}
		delete(p.noReuse, addr)
	}
	return true
}

// get returns the most recently used idle connection for addr and how many replies it has carried, discarding any
// that have been idle too long.
func (p *connPool) get(addr string) (net.Conn, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.enabled(addr) {
		return nil, 0
	}
	conns := p.idle[addr]
	for len(conns) > 0 {
		c := conns[len(conns)-1]
		conns = conns[:len(conns)-1]
		if time.Since(c.since) < p.maxIdle {
			p.idle[addr] = conns
			return c.conn, c.uses
		}
		c.conn.Close()
	}
	delete(p.idle, addr)
	return nil, 0
}

// put returns a healthy connection to the pool, or closes it when the pool is full, disabled or closed.
func (p *connPool) put(addr string, conn net.Conn, uses int) {
	if _, ok := connAlive(conn); !ok { // cannot be checked before reuse
		conn.Close()
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.enabled(addr) || len(p.idle[addr]) >= p.max {
		conn.Close()
		return
	}
	p.idle[addr] = append(p.idle[addr], pooledConn{conn: conn, since: time.Now(), uses: uses})
}

// refuse turns pooling off for addr for a while and closes its idle connections.
func (p *connPool) refuse(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.noReuse[addr] = time.Now().Add(noReuseFor)
	for _, c := range p.idle[addr] {
		c.conn.Close()
	}
	delete(p.idle, addr)
}

func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for addr, conns := range p.idle {
		for _, c := range conns {
			c.conn.Close()
		}
		delete(p.idle, addr)
	}
}
