package gossip

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
)

// countingListener counts accepted connections and keeps them so a test can drop them.
type countingListener struct {
	net.Listener
	accepted atomic.Int64
	mu       sync.Mutex
	conns    []net.Conn
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
		l.mu.Lock()
		l.conns = append(l.conns, c)
		l.mu.Unlock()
	}
	return c, err
}

// dropAll closes every accepted connection from the server side, as a restarting peer would.
func (l *countingListener) dropAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		c.Close()
	}
}

func reuseConfig() *Config {
	return &Config{
		BindAddr:                 "127.0.0.1:0",
		IncomingPacketQueueDepth: 64,
		Logger:                   logger.NewNullLogger(),
		MsgCodec:                 codec.NewJSONCodec(),
		TCPDeadline:              5 * time.Second,
		UDPDeadline:              5 * time.Second,
		TCPDialTimeout:           5 * time.Second,
		UDPMaxPacketSize:         1024,
		TCPMaxPacketSize:         65536,
		TCPPoolSize:              16,
		TCPIdleTimeout:           60 * time.Second,
		ForceReliableTransport:   true,
	}
}

// startEchoServer starts a transport whose handler replies "echo:<payload>" after delay(n) for the n-th request.
func startEchoServer(t *testing.T, cfg *Config, delay func(n int64) time.Duration) (*SocketTransport, *countingListener, *Node, *atomic.Int64) {
	t.Helper()
	var ln *countingListener
	cfg.ListenFunc = func(network, addr string) (net.Listener, error) {
		l, err := net.Listen(network, addr)
		if err != nil {
			return nil, err
		}
		ln = &countingListener{Listener: l}
		return ln, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	st := NewSocketTransport(cfg)
	if err := st.Start(ctx, &wg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	var requests atomic.Int64
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case p := <-st.PacketChannel():
				n := requests.Add(1)
				go func(p *Packet) {
					if delay != nil {
						time.Sleep(delay(n))
					}
					if p.CanReply() {
						p.AddRef()
						p.payload = append([]byte("echo:"), p.payload...)
						if p.SendReply() != nil {
							p.Release()
						}
					}
					p.Release()
				}(p)
			}
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	node := newNode(NodeID(uuid.New()), addr.String())
	node.address = Address{IP: addr.IP, Port: addr.Port}
	node.caps.Store(capKeepsConnections) // as a current peer advertises
	return st, ln, node, &requests
}

func startClient(t *testing.T, cfg *Config) *SocketTransport {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	st := NewSocketTransport(cfg)
	if err := st.Start(ctx, &wg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })
	return st
}

func request(t *testing.T, client *SocketTransport, node *Node, payload string) (string, error) {
	p := NewPacket()
	defer p.Release()
	p.MessageType = UserMsg
	p.SenderID = NodeID(uuid.New())
	p.MessageID = MessageID(hlc.Now())
	p.TTL = 1
	p.SetCodec(client.config.MsgCodec)
	p.SetPayload([]byte(payload))
	reply, err := client.SendWithReply(node, p)
	if err != nil {
		return "", err
	}
	defer reply.Release()
	return string(reply.Payload()), nil
}

func TestSendWithReplyReusesConnection(t *testing.T) {
	_, ln, node, _ := startEchoServer(t, reuseConfig(), nil)
	client := startClient(t, reuseConfig())
	for i := range 100 {
		got, err := request(t, client, node, fmt.Sprint(i))
		if err != nil || got != fmt.Sprintf("echo:%d", i) {
			t.Fatalf("request %d: %q %v", i, got, err)
		}
	}
	if n := ln.accepted.Load(); n != 1 {
		t.Fatalf("expected 1 connection for 100 sequential requests, got %d", n)
	}
}

func TestSendWithReplyConcurrentReuse(t *testing.T) {
	_, ln, node, _ := startEchoServer(t, reuseConfig(), nil)
	client := startClient(t, reuseConfig())
	const workers, each = 32, 50
	var wg sync.WaitGroup
	errs := make(chan error, workers*each)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				msg := fmt.Sprintf("%d-%d", w, i)
				got, err := request(t, client, node, msg)
				if err != nil || got != "echo:"+msg {
					errs <- fmt.Errorf("%s: got %q err %v", msg, got, err)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// Connections beyond the pool size are closed after use, so allow some churn — but far fewer than one per call.
	if n := ln.accepted.Load(); n > workers*4 {
		t.Fatalf("too many connections for %d calls: %d", workers*each, n)
	}
}

// startOldServer emulates a peer that predates connection reuse: one request per connection, then close.
func startOldServer(t *testing.T) (*Node, *atomic.Int64) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	codecTransport := NewSocketTransport(reuseConfig())
	var accepted atomic.Int64
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func() {
				defer conn.Close()
				p, _, _, err := codecTransport.readPacketFlags(conn, 5*time.Second)
				if err != nil {
					return
				}
				p.payload = append([]byte("echo:"), p.payload...)
				codecTransport.writePacket(conn, p, false)
				p.Release()
			}()
		}
	}()
	addr := l.Addr().(*net.TCPAddr)
	node := newNode(NodeID(uuid.New()), addr.String())
	node.address = Address{IP: addr.IP, Port: addr.Port}
	return node, &accepted
}

// TestSendWithReplyFallsBackForOlderServer: a peer that does not advertise keeping connections open (one that
// predates connection reuse) gets a fresh connection per request, so no request is ever sent on a connection it has
// already closed. (Reusing one used to fail a request whenever its close arrived after the client's check.)
func TestSendWithReplyFallsBackForOlderServer(t *testing.T) {
	node, accepted := startOldServer(t)
	client := startClient(t, reuseConfig())
	for i := range 50 {
		got, err := request(t, client, node, fmt.Sprint(i))
		if err != nil || got != fmt.Sprintf("echo:%d", i) {
			t.Fatalf("request %d against older server: %q %v", i, got, err)
		}
	}
	if n := accepted.Load(); n != 50 {
		t.Fatalf("expected 50 connections, got %d", n)
	}
	if n := client.pool.size(); n != 0 {
		t.Fatalf("%d connections to an older server pooled", n)
	}
}

func TestSendWithReplyRecoversFromDroppedPooledConnection(t *testing.T) {
	_, ln, node, _ := startEchoServer(t, reuseConfig(), nil)
	client := startClient(t, reuseConfig())
	for i := range 3 {
		if _, err := request(t, client, node, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	ln.dropAll() // the peer drops its idle connections, as on a restart
	time.Sleep(50 * time.Millisecond)
	addr := node.GetAddress()
	if got, err := request(t, client, node, "after"); err != nil || got != "echo:after" {
		t.Fatalf("after drop: %q %v", got, err)
	}
	// A connection that had carried several replies is not mistaken for an older server.
	if !client.pool.enabled(addr.String()) {
		t.Fatal("pooling switched off after a dropped multi-use connection")
	}
}

// TestSendWithReplyNoReplyNotResent: a handler that finishes without replying leaves the server to time out and
// close the connection; the client must report an error, not send the processed request again.
func TestSendWithReplyNoReplyNotResent(t *testing.T) {
	cfg := reuseConfig()
	cfg.TCPDeadline = 300 * time.Millisecond
	var requests atomic.Int64
	var ln *countingListener
	cfg.ListenFunc = func(network, addr string) (net.Listener, error) {
		l, err := net.Listen(network, addr)
		ln = &countingListener{Listener: l}
		return ln, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	server := NewSocketTransport(cfg)
	if err := server.Start(ctx, &wg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case p := <-server.PacketChannel():
				if requests.Add(1) == 1 && p.CanReply() { // only the first request gets a reply
					p.AddRef()
					if p.SendReply() != nil {
						p.Release()
					}
				}
				p.Release()
			}
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	node := newNode(NodeID(uuid.New()), addr.String())
	node.address = Address{IP: addr.IP, Port: addr.Port}
	node.caps.Store(capKeepsConnections)
	client := startClient(t, reuseConfig())
	if _, err := request(t, client, node, "first"); err != nil {
		t.Fatal(err)
	}
	if _, err := request(t, client, node, "second"); err == nil {
		t.Fatal("expected an error when no reply is sent")
	}
	time.Sleep(500 * time.Millisecond)
	if n := requests.Load(); n != 2 {
		t.Fatalf("processed request was sent again: server saw %d requests", n)
	}
}

// TestSendWithReplyTimeoutNotRetried: a slow reply on a reused connection must not be sent again (the peer may
// still be processing it).
func TestSendWithReplyTimeoutNotRetried(t *testing.T) {
	_, _, node, requests := startEchoServer(t, reuseConfig(), func(n int64) time.Duration {
		if n == 2 {
			return 700 * time.Millisecond
		}
		return 0
	})
	cfg := reuseConfig()
	cfg.TCPDeadline = 300 * time.Millisecond
	client := startClient(t, cfg)
	if _, err := request(t, client, node, "fast"); err != nil {
		t.Fatal(err)
	}
	if _, err := request(t, client, node, "slow"); err == nil {
		t.Fatal("expected a timeout")
	}
	time.Sleep(time.Second)
	if n := requests.Load(); n != 2 {
		t.Fatalf("slow request was retried: server saw %d requests", n)
	}
}

func TestServerKeepsConnectionForSeveralRequests(t *testing.T) {
	// An unpooled client (older behaviour) still works against the new server.
	_, ln, node, _ := startEchoServer(t, reuseConfig(), nil)
	cfg := reuseConfig()
	cfg.TCPPoolSize = 0
	client := startClient(t, cfg)
	for i := range 3 {
		if got, err := request(t, client, node, fmt.Sprint(i)); err != nil || got != fmt.Sprintf("echo:%d", i) {
			t.Fatalf("%q %v", got, err)
		}
	}
	if n := ln.accepted.Load(); n != 3 {
		t.Fatalf("pooling disabled should dial per request, got %d connections", n)
	}
}

func TestServerIdleTimeoutClosesConnection(t *testing.T) {
	cfg := reuseConfig()
	cfg.TCPIdleTimeout = 200 * time.Millisecond
	_, ln, node, _ := startEchoServer(t, cfg, nil)
	client := startClient(t, reuseConfig()) // client keeps connections far longer than the server
	if _, err := request(t, client, node, "a"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // server closes the idle connection
	if got, err := request(t, client, node, "b"); err != nil || got != "echo:b" {
		t.Fatalf("after server idle close: %q %v", got, err)
	}
	if n := ln.accepted.Load(); n != 2 {
		t.Fatalf("expected a fresh connection after the idle close, got %d", n)
	}
}

// TestPooledConnectionReusedAfterDeadlinePassed: the read deadline from a connection's previous request must not
// make it look dead once it has been idle longer than TCPDeadline.
func TestPooledConnectionReusedAfterDeadlinePassed(t *testing.T) {
	_, ln, node, _ := startEchoServer(t, reuseConfig(), nil)
	cfg := reuseConfig()
	cfg.TCPDeadline = 200 * time.Millisecond
	client := startClient(t, cfg)
	if _, err := request(t, client, node, "a"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if got, err := request(t, client, node, "b"); err != nil || got != "echo:b" {
		t.Fatalf("%q %v", got, err)
	}
	addr := node.GetAddress()
	if n := ln.accepted.Load(); n != 1 || !client.pool.enabled(addr.String()) {
		t.Fatalf("connection not reused after its deadline passed: %d connections, pooling %v", n, client.pool.enabled(addr.String()))
	}
}

// size is the number of idle connections the pool holds.
func (p *connPool) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.idle {
		n += len(c)
	}
	return n
}
