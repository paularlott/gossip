package gossip

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/stretchr/testify/require"
)

// One-way reliable packets travel on pooled connections to peers that advertise capOneWayOnPooled (section E of the
// partition design): the server keeps reading after a one-way packet, the client takes a connection from the pool,
// writes, and puts it back.

const oneWayCaps = capKeepsConnections | capOneWayOnPooled

// oneWayRecorder is the packet consumer of a test server: it answers requests with "echo:<payload>" and records the
// one-way packets, checking how they arrive.
type oneWayRecorder struct {
	mu       sync.Mutex
	payloads []string
	held     []*Packet
	bad      []string
	hold     bool // keep one-way packets unreleased
}

func (r *oneWayRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.payloads)
}

// unique is the number of distinct payloads received.
func (r *oneWayRecorder) unique() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[string]bool{}
	for _, p := range r.payloads {
		seen[p] = true
	}
	return len(seen)
}

func (r *oneWayRecorder) releaseHeld() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.held {
		p.Release()
	}
	r.held = nil
}

// startOneWayServer starts a transport behind a counting listener, consuming its packets with a oneWayRecorder.
// The returned node advertises the capabilities caps.
func startOneWayServer(t *testing.T, cfg *Config, caps uint32) (*SocketTransport, *countingListener, *Node, *oneWayRecorder) {
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
	require.NoError(t, st.Start(ctx, &wg))
	rec := &oneWayRecorder{}
	t.Cleanup(func() { cancel(); wg.Wait(); rec.releaseHeld() })

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case p := <-st.PacketChannel():
				if p.awaitingReply() {
					p.AddRef()
					p.payload = append([]byte("echo:"), p.payload...)
					if p.SendReply() != nil {
						p.Release()
					}
					p.Release()
					continue
				}
				rec.mu.Lock()
				if p.conn != nil || !p.reliable || !p.CanReply() {
					rec.bad = append(rec.bad, string(p.payload))
				}
				rec.payloads = append(rec.payloads, string(p.payload))
				if rec.hold {
					rec.held = append(rec.held, p)
					p = nil
				}
				rec.mu.Unlock()
				if p != nil {
					p.Release()
				}
			}
		}
	}()

	addr := ln.Addr().(*net.TCPAddr)
	node := newNode(NodeID(uuid.New()), addr.String())
	node.address = Address{IP: addr.IP, Port: addr.Port}
	node.caps.Store(caps)
	return st, ln, node, rec
}

func oneWayPacket(client *SocketTransport, payload string) *Packet {
	p := NewPacket()
	p.MessageType = UserMsg
	p.SenderID = NodeID(uuid.New())
	p.MessageID = MessageID(hlc.Now())
	p.TTL = 1
	p.SetCodec(client.config.MsgCodec)
	p.SetPayload([]byte(payload))
	return p
}

// sendOneWay sends payload as a one-way reliable packet.
func sendOneWay(client *SocketTransport, node *Node, payload string) error {
	p := oneWayPacket(client, payload)
	defer p.Release()
	return client.Send(TransportReliable, node, p)
}

func requireDelivered(t *testing.T, rec *oneWayRecorder, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return rec.unique() >= want }, 10*time.Second, 5*time.Millisecond, "one-way packets not delivered")
	require.Equal(t, want, rec.count(), "a packet was delivered more than once")
	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Empty(t, rec.bad, "a one-way packet arrived owning its connection, or not marked reliable")
}

// Many one-way reliable sends reuse a small number of connections, one at a time each, however many senders there are.
func TestOneWayReliableReusesConnections(t *testing.T) {
	_, ln, node, rec := startOneWayServer(t, reuseConfig(), oneWayCaps)
	client := startClient(t, reuseConfig())

	const sequential = 200
	for i := range sequential {
		require.NoError(t, sendOneWay(client, node, fmt.Sprintf("seq-%d", i)))
	}
	requireDelivered(t, rec, sequential)
	require.EqualValues(t, 1, ln.accepted.Load(), "sequential sends should share one connection")
	require.Equal(t, 1, client.pool.size())

	const senders, each = 8, 50
	var wg sync.WaitGroup
	for s := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				if err := sendOneWay(client, node, fmt.Sprintf("par-%d-%d", s, i)); err != nil {
					t.Errorf("sender %d: %v", s, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	requireDelivered(t, rec, sequential+senders*each)
	require.LessOrEqual(t, ln.accepted.Load(), int64(senders), "at most one connection per concurrent sender")
	require.LessOrEqual(t, client.pool.size(), senders)
}

// Requests and one-way packets share pooled connections: every one-way packet arrives and every request is answered.
func TestOneWayInterleavedWithRequestsOnPooledConnection(t *testing.T) {
	_, ln, node, rec := startOneWayServer(t, reuseConfig(), oneWayCaps)
	client := startClient(t, reuseConfig())

	const rounds = 50
	for i := range rounds {
		require.NoError(t, sendOneWay(client, node, fmt.Sprintf("one-%d", i)))
		got, err := request(t, client, node, fmt.Sprintf("req-%d", i))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("echo:req-%d", i), got)
	}
	requireDelivered(t, rec, rounds)
	require.EqualValues(t, 1, ln.accepted.Load(), "one connection should carry both kinds")

	const workers, each = 8, 30
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				if err := sendOneWay(client, node, fmt.Sprintf("one-%d-%d", w, i)); err != nil {
					t.Errorf("one-way %d/%d: %v", w, i, err)
					return
				}
				got, err := request(t, client, node, fmt.Sprintf("req-%d-%d", w, i))
				if err != nil || got != fmt.Sprintf("echo:req-%d-%d", w, i) {
					t.Errorf("request %d/%d: %q %v", w, i, got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	requireDelivered(t, rec, rounds+workers*each)
	require.LessOrEqual(t, ln.accepted.Load(), int64(workers))
}

// A one-way packet no longer takes its connection: a handler may keep it as long as it likes (a deferred reply
// handler does, until it replies) and the connection carries on, and releasing the packet leaves the connection open.
func TestOneWayPacketDoesNotOwnConnection(t *testing.T) {
	_, ln, node, rec := startOneWayServer(t, reuseConfig(), oneWayCaps)
	rec.hold = true
	client := startClient(t, reuseConfig())

	for i := range 5 {
		require.NoError(t, sendOneWay(client, node, fmt.Sprint(i)))
	}
	requireDelivered(t, rec, 5)
	got, err := request(t, client, node, "while-held")
	require.NoError(t, err)
	require.Equal(t, "echo:while-held", got)

	rec.releaseHeld()
	got, err = request(t, client, node, "after-release")
	require.NoError(t, err)
	require.Equal(t, "echo:after-release", got)
	require.NoError(t, sendOneWay(client, node, "last"))
	requireDelivered(t, rec, 6)
	require.EqualValues(t, 1, ln.accepted.Load(), "the held and released packets must not have closed the connection")
}

// A peer that does not advertise the bit (an older one, or one not heard from yet) gets a fresh connection per
// one-way packet, which is closed after it, as before.
func TestOneWayToPeerWithoutBitUsesFreshConnections(t *testing.T) {
	t.Run("current server", func(t *testing.T) {
		_, ln, node, rec := startOneWayServer(t, reuseConfig(), capKeepsConnections)
		client := startClient(t, reuseConfig())
		const n = 20
		for i := range n {
			require.NoError(t, sendOneWay(client, node, fmt.Sprint(i)))
		}
		requireDelivered(t, rec, n)
		require.EqualValues(t, n, ln.accepted.Load())
		require.Equal(t, 0, client.pool.size())
	})
	t.Run("older server", func(t *testing.T) {
		node, accepted := startOldServer(t) // reads one packet, answers it, closes
		client := startClient(t, reuseConfig())
		const n = 20
		for i := range n {
			require.NoError(t, sendOneWay(client, node, fmt.Sprint(i)))
		}
		require.Eventually(t, func() bool { return accepted.Load() == n }, 5*time.Second, 5*time.Millisecond)
		require.Equal(t, 0, client.pool.size())
	})
}

// writeCountConn counts Write calls, and keeps SyscallConn (through the embedded TCPConn) so the pool can probe it.
type writeCountConn struct {
	*net.TCPConn
	writes atomic.Int64
}

func (c *writeCountConn) Write(b []byte) (int, error) {
	c.writes.Add(1)
	return c.TCPConn.Write(b)
}

func dialCounting(conns *[]*writeCountConn, mu *sync.Mutex) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		w := &writeCountConn{TCPConn: c.(*net.TCPConn)}
		mu.Lock()
		*conns = append(*conns, w)
		mu.Unlock()
		return w, nil
	}
}

// A pooled connection the peer has closed meanwhile (its idle timeout) is detected before use and nothing is written
// to it: the packet goes out on a new connection and arrives once.
func TestOneWayPooledConnectionClosedByPeerIsNotWritten(t *testing.T) {
	serverCfg := reuseConfig()
	serverCfg.TCPIdleTimeout = 100 * time.Millisecond
	_, ln, node, rec := startOneWayServer(t, serverCfg, oneWayCaps)

	var mu sync.Mutex
	var conns []*writeCountConn
	cfg := reuseConfig() // keeps connections far longer than the server does
	cfg.DialFunc = dialCounting(&conns, &mu)
	client := startClient(t, cfg)

	require.NoError(t, sendOneWay(client, node, "first"))
	requireDelivered(t, rec, 1)
	time.Sleep(300 * time.Millisecond) // the server closes the idle connection

	require.NoError(t, sendOneWay(client, node, "second"))
	requireDelivered(t, rec, 2)
	require.EqualValues(t, 2, ln.accepted.Load())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, conns, 2)
	require.EqualValues(t, 1, conns[0].writes.Load(), "the connection the peer had closed was written to")
	require.EqualValues(t, 1, conns[1].writes.Load())
}

// A write error is retried once, on a newly dialled connection. When the peer had in fact received the packet the
// repeat reaches the cluster a second time and is dropped: the handler runs exactly once.
func TestOneWayRetryAfterWriteErrorDeliversOnce(t *testing.T) {
	const marker = "retry-marker-1d1c"
	var armed atomic.Bool
	var mu sync.Mutex
	failed := 0
	markerConns := map[*net.TCPConn]int{}

	newNode := func(dial func(ctx context.Context, network, addr string) (net.Conn, error)) (*Cluster, *atomic.Int64) {
		addr := getFreeTCPAddress(t)
		cfg := DefaultConfig()
		cfg.BindAddr, cfg.AdvertiseAddr = addr, addr
		cfg.MsgCodec = codec.NewJSONCodec()
		cfg.Logger = logger.NewNullLogger()
		cfg.ForceReliableTransport = true
		cfg.DialFunc = dial
		cfg.Transport = NewSocketTransport(cfg)
		c, err := NewCluster(cfg)
		require.NoError(t, err)
		var got atomic.Int64
		require.NoError(t, c.HandleFunc(UserMsg+70, func(_ *Node, p *Packet) error {
			var s string
			if err := p.Unmarshal(&s); err == nil && s == marker {
				got.Add(1)
			}
			return nil
		}))
		c.Start()
		t.Cleanup(c.Stop)
		return c, &got
	}

	// a's connections deliver the marked packet in full and then report a write failure, once armed.
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &injectConn{TCPConn: c.(*net.TCPConn), marker: []byte(marker), armed: &armed, mu: &mu, failed: &failed, seen: markerConns}, nil
	}
	a, _ := newNode(dial)
	b, gotB := newNode(nil)
	require.NoError(t, b.Join([]string{a.LocalNode().AdvertisedAddr()}))
	require.Eventually(t, func() bool {
		nb := a.GetNode(b.LocalNode().ID)
		return nb != nil && nb.caps.Load()&capOneWayOnPooled != 0 && b.GetNode(a.LocalNode().ID) != nil
	}, 10*time.Second, 20*time.Millisecond, "the peers did not learn each other's capabilities")

	armed.Store(true)
	require.NoError(t, a.SendToReliable(a.GetNode(b.LocalNode().ID), UserMsg+70, marker))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return failed == 1 && len(markerConns) == 2
	}, 10*time.Second, 5*time.Millisecond, "the packet was not sent again on another connection after the write error")
	require.Eventually(t, func() bool { return gotB.Load() >= 1 }, 10*time.Second, 5*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	require.EqualValues(t, 1, gotB.Load(), "the repeated packet reached the handler twice")
}

// injectConn writes the packet carrying marker in full the first time it is armed, then closes the connection and
// reports an error, as a connection that failed after the peer had received the packet would.
type injectConn struct {
	*net.TCPConn
	marker []byte
	armed  *atomic.Bool
	mu     *sync.Mutex
	failed *int
	seen   map[*net.TCPConn]int
}

func (c *injectConn) Write(b []byte) (int, error) {
	if !bytes.Contains(b, c.marker) {
		return c.TCPConn.Write(b)
	}
	c.mu.Lock()
	c.seen[c.TCPConn]++
	c.mu.Unlock()
	if !c.armed.CompareAndSwap(true, false) {
		return c.TCPConn.Write(b)
	}
	n, err := c.TCPConn.Write(b)
	c.TCPConn.Close()
	c.mu.Lock()
	*c.failed++
	c.mu.Unlock()
	if err != nil {
		return n, err
	}
	return n, errors.New("injected write failure")
}

// A node that does not advertise the bit (a test hook stands in for an older build) and one that does deliver
// to each other both ways: one-way packets and requests.
func TestOneWayMixedVersionPairDeliversBothWays(t *testing.T) {
	const echoMsg, oneMsg = UserMsg + 71, UserMsg + 72
	type side struct {
		*Cluster
		got sync.Map // payload -> deliveries
		n   atomic.Int64
	}
	start := func(without uint32) *side {
		addr := getFreeTCPAddress(t)
		cfg := DefaultConfig()
		cfg.BindAddr, cfg.AdvertiseAddr = addr, addr
		cfg.MsgCodec = codec.NewJSONCodec()
		cfg.Logger = logger.NewNullLogger()
		cfg.ForceReliableTransport = true
		st := NewSocketTransport(cfg)
		st.withoutCaps = without
		cfg.Transport = st
		c, err := NewCluster(cfg)
		require.NoError(t, err)
		s := &side{Cluster: c}
		require.NoError(t, c.HandleFunc(oneMsg, func(_ *Node, p *Packet) error {
			var v string
			if err := p.Unmarshal(&v); err != nil {
				return err
			}
			s.got.Store(v, true)
			s.n.Add(1)
			return nil
		}))
		require.NoError(t, c.HandleFuncWithReply(echoMsg, func(_ *Node, p *Packet) (interface{}, error) {
			var v string
			if err := p.Unmarshal(&v); err != nil {
				return nil, err
			}
			return "echo:" + v, nil
		}))
		c.Start()
		t.Cleanup(c.Stop)
		return s
	}
	old, cur := start(capOneWayOnPooled), start(0)
	require.Equal(t, capKeepsConnections, old.capabilities())
	require.Equal(t, oneWayCaps, cur.capabilities())
	require.NoError(t, cur.Join([]string{old.LocalNode().AdvertisedAddr()}))
	require.Eventually(t, func() bool {
		a, b := old.GetNode(cur.LocalNode().ID), cur.GetNode(old.LocalNode().ID)
		return a != nil && b != nil && a.caps.Load() == oneWayCaps && b.caps.Load() == capKeepsConnections
	}, 10*time.Second, 20*time.Millisecond, "capabilities not learnt")

	const n = 30
	for i := range n {
		require.NoError(t, old.SendToReliable(old.GetNode(cur.LocalNode().ID), oneMsg, fmt.Sprintf("old-%d", i)))
		require.NoError(t, cur.SendToReliable(cur.GetNode(old.LocalNode().ID), oneMsg, fmt.Sprintf("cur-%d", i)))
		var r string
		require.NoError(t, old.SendToWithResponse(old.GetNode(cur.LocalNode().ID), echoMsg, fmt.Sprint(i), &r))
		require.Equal(t, fmt.Sprint("echo:", i), r)
		require.NoError(t, cur.SendToWithResponse(cur.GetNode(old.LocalNode().ID), echoMsg, fmt.Sprint(i), &r))
		require.Equal(t, fmt.Sprint("echo:", i), r)
	}
	require.Eventually(t, func() bool { return cur.n.Load() == n && old.n.Load() == n }, 10*time.Second, 10*time.Millisecond,
		"one-way packets not delivered both ways (to current %d, to old %d, want %d)", cur.n.Load(), old.n.Load(), n)
	for i := range n {
		_, ok1 := cur.got.Load(fmt.Sprintf("old-%d", i))
		_, ok2 := old.got.Load(fmt.Sprintf("cur-%d", i))
		require.True(t, ok1 && ok2, "packet %d missing", i)
	}
}

// The HTTP transport has no connections of its own to pool for one-way packets: it advertises nothing, and sends
// work as before.
func TestOneWayHTTPTransportUnaffected(t *testing.T) {
	env := newDeferredEnv(t, "http", 8, 5*time.Second)
	require.Zero(t, env.server.capabilities())
	require.Zero(t, env.client.capabilities())

	var got atomic.Int64
	require.NoError(t, env.server.HandleFunc(UserMsg+73, func(_ *Node, p *Packet) error {
		got.Add(1)
		return nil
	}))
	for i := range 20 {
		require.NoError(t, env.client.SendToReliable(env.node, UserMsg+73, fmt.Sprint(i)))
	}
	require.Eventually(t, func() bool { return got.Load() == 20 }, 10*time.Second, 10*time.Millisecond)
}
