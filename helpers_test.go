package gossip

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// sentPacket is a send recorded by a mockTransport.
type sentPacket struct {
	node      *Node
	transport TransportType
	reply     bool // sent with SendWithReply
	msgType   MessageType
	tag       *string
}

// mockTransport is a transport that sends nothing: it records what it was asked to send and answers as told. By
// default sends succeed and SendWithReply returns no reply (an error to the caller).
type mockTransport struct {
	mu      sync.Mutex
	ch      chan *Packet
	sent    []sentPacket
	sendErr error                                 // returned by Send and SendWithReply
	replyFn func(*Node, *Packet) (*Packet, error) // answers SendWithReply when set
}

func (m *mockTransport) Name() string { return "mock" }
func (m *mockTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	m.PacketChannel()
	return nil
}

func (m *mockTransport) PacketChannel() chan *Packet {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ch == nil {
		m.ch = make(chan *Packet, 100)
	}
	return m.ch
}

func (m *mockTransport) record(typ TransportType, node *Node, packet *Packet, reply bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, sentPacket{node: node, transport: typ, reply: reply, msgType: packet.MessageType, tag: packet.Tag})
	return m.sendErr
}

func (m *mockTransport) Send(typ TransportType, node *Node, packet *Packet) error {
	return m.record(typ, node, packet, false)
}

func (m *mockTransport) SendWithReply(node *Node, packet *Packet) (*Packet, error) {
	if err := m.record(TransportReliable, node, packet, true); err != nil {
		return nil, err
	}
	m.mu.Lock()
	fn := m.replyFn
	m.mu.Unlock()
	if fn == nil {
		return nil, nil
	}
	return fn(node, packet)
}

// sends returns the sends recorded so far.
func (m *mockTransport) sends() []sentPacket {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sentPacket(nil), m.sent...)
}

func (m *mockTransport) setSendError(err error) {
	m.mu.Lock()
	m.sendErr = err
	m.mu.Unlock()
}

// replyWith makes SendWithReply answer every request with v, marshalled with c.
func (m *mockTransport) replyWith(c codec.Serializer, v interface{}) {
	m.mu.Lock()
	m.replyFn = func(node *Node, _ *Packet) (*Packet, error) {
		payload, err := c.Marshal(v)
		if err != nil {
			return nil, err
		}
		reply := NewPacket()
		reply.MessageType = replyMsg
		reply.SenderID = node.ID
		reply.MessageID = MessageID(hlc.Now())
		reply.SetCodec(c)
		reply.SetPayload(payload)
		return reply, nil
	}
	m.mu.Unlock()
}

// mockCodec marshals everything to "mock" and unmarshals nothing.
type mockCodec struct{}

func (m *mockCodec) Marshal(v interface{}) ([]byte, error)      { return []byte("mock"), nil }
func (m *mockCodec) Unmarshal(data []byte, v interface{}) error { return nil }
func (m *mockCodec) Name() string                               { return "mock" }

// failCodec fails every call.
type failCodec struct{}

func (f *failCodec) Marshal(v interface{}) ([]byte, error)      { return nil, fmt.Errorf("marshal error") }
func (f *failCodec) Unmarshal(data []byte, v interface{}) error { return fmt.Errorf("unmarshal error") }
func (f *failCodec) Name() string                               { return "fail" }

// mockCipher leaves data as it is.
type mockCipher struct{}

func (m *mockCipher) Encrypt(key, data []byte) ([]byte, error) { return data, nil }
func (m *mockCipher) Decrypt(key, data []byte) ([]byte, error) { return data, nil }
func (m *mockCipher) Name() string                             { return "mock" }

// mockResolver answers name lookups with canned results.
type mockResolver struct {
	ips        []string
	ipErr      error
	srvAddrs   []*net.TCPAddr
	srvErr     error
	srvQueries []string // the services looked up
}

func (r *mockResolver) LookupIP(host string) ([]string, error) { return r.ips, r.ipErr }
func (r *mockResolver) LookupSRV(service string) ([]*net.TCPAddr, error) {
	r.srvQueries = append(r.srvQueries, service)
	return r.srvAddrs, r.srvErr
}

// mockConn is a net.Conn that records what is written to it and whether it was closed.
type mockConn struct {
	writeData []byte
	closed    bool
}

func (m *mockConn) Read(b []byte) (n int, err error) { return 0, nil }
func (m *mockConn) Write(b []byte) (n int, err error) {
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}
func (m *mockConn) Close() error                       { m.closed = true; return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// ---------------------------------------------------------------------------
// Clusters
// ---------------------------------------------------------------------------

// newUnitCluster builds a cluster that is not started, on a mockTransport and the JSON codec; opts adjust the
// config. The mock transport is c.config.Transport. The cluster is stopped with the test.
func newUnitCluster(t testing.TB, opts ...func(*Config)) *Cluster {
	t.Helper()
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	for _, opt := range opts {
		opt(config)
	}
	c, err := NewCluster(config)
	require.NoError(t, err)
	t.Cleanup(func() { stopQuietly(c) })
	return c
}

// unitTransport is the mock transport of a cluster made by newUnitCluster.
func unitTransport(c *Cluster) *mockTransport { return c.config.Transport.(*mockTransport) }

// stopQuietly stops c without the graceful leave: Stop skips Leave, and its 100 ms wait for the leave to go out,
// for a node that is already leaving. Use it to tear down clusters whose leave nobody observes.
func stopQuietly(c *Cluster) {
	c.nodes.updateState(c.localNode.ID, NodeLeaving)
	c.Stop()
}

// startLiveCluster starts a cluster on a free loopback port, with a socket transport and the JSON codec; opts adjust
// the config before the transport is made. The cluster is stopped with the test.
func startLiveCluster(t testing.TB, opts ...func(*Config)) *Cluster {
	t.Helper()
	addr := getFreeTCPAddress(t)
	cfg := DefaultConfig()
	cfg.BindAddr, cfg.AdvertiseAddr = addr, addr
	cfg.MsgCodec = codec.NewJSONCodec()
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.Transport == nil {
		cfg.Transport = NewSocketTransport(cfg)
	}
	c, err := NewCluster(cfg)
	require.NoError(t, err)
	c.Start()
	t.Cleanup(func() { stopQuietly(c) })
	return c
}

// joinAndWait joins b to a and waits until each knows the other.
func joinAndWait(t testing.TB, a, b *Cluster) {
	t.Helper()
	require.NoError(t, b.Join([]string{a.localNode.AdvertisedAddr()}))
	require.Eventually(t, func() bool {
		return a.GetNode(b.LocalNode().ID) != nil && b.GetNode(a.LocalNode().ID) != nil
	}, 5*time.Second, 20*time.Millisecond, "the nodes did not discover each other")
}

// getFreeTCPAddress returns a loopback address whose port is free for both TCP and UDP: a node binds both on the
// same port, and with test packages running in parallel a port free for TCP may be held for UDP by another process.
func getFreeTCPAddress(t testing.TB) string {
	t.Helper()

	for range 100 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to allocate free port: %v", err)
		}
		addr := ln.Addr().String()
		udp, err := net.ListenPacket("udp", addr)
		ln.Close()
		if err != nil {
			continue // the UDP side is taken: try another port
		}
		udp.Close()
		return addr
	}
	t.Fatal("no port free for both TCP and UDP")
	return ""
}

// addPeers adds n alive nodes (127.0.0.1:9001...) with the given tags to c's node list and returns them.
func addPeers(c *Cluster, n int, tags ...string) []*Node {
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 9001+i), tags...)
		c.nodes.addOrUpdate(nodes[i])
	}
	return nodes
}

// queuedBroadcasts takes what is waiting in c's broadcast queue, releasing nothing: the caller owns the packets.
func queuedBroadcasts(c *Cluster) []*broadcastQItem {
	var items []*broadcastQItem
	for {
		select {
		case item := <-c.broadcastQueue:
			items = append(items, item)
		default:
			return items
		}
	}
}

// queuedJoins takes the addresses waiting in c's join queue.
func queuedJoins(c *Cluster) []string {
	var addrs []string
	for {
		select {
		case req := <-c.joinQueue:
			addrs = append(addrs, req.nodeAddr)
		default:
			return addrs
		}
	}
}

// requestPacket builds a packet of the given type from sender carrying payload, marshalled with c's codec. The
// caller releases it if the code under test does not.
func requestPacket(t testing.TB, c *Cluster, msgType MessageType, sender NodeID, payload interface{}) *Packet {
	t.Helper()
	packet := NewPacket()
	packet.MessageType = msgType
	packet.SenderID = sender
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetCodec(c.config.MsgCodec)
	if payload != nil {
		data, err := c.config.MsgCodec.Marshal(payload)
		require.NoError(t, err)
		packet.SetPayload(data)
	}
	return packet
}
