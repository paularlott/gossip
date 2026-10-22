package gossip

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
)

// recordingDialer is a DialFunc that records every call and either dials for
// real or refuses with err.
type recordingDialer struct {
	mu        sync.Mutex
	addrs     []string
	deadlines []time.Duration // time left on the context when called; -1 = none
	err       error
}

func (d *recordingDialer) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	left := time.Duration(-1)
	if dl, ok := ctx.Deadline(); ok {
		left = time.Until(dl)
	}
	d.mu.Lock()
	d.addrs = append(d.addrs, network+" "+addr)
	d.deadlines = append(d.deadlines, left)
	d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
	return (&net.Dialer{}).DialContext(ctx, network, addr)
}

func (d *recordingDialer) calls() ([]string, []time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.addrs...), append([]time.Duration(nil), d.deadlines...)
}

func testPacket(c codec.Serializer, payload string) *Packet {
	p := NewPacket()
	p.MessageType = UserMsg
	p.SenderID = NodeID(uuid.New())
	p.MessageID = MessageID(hlc.Now())
	p.TTL = 5
	p.SetPayload([]byte(payload))
	p.SetCodec(c)
	return p
}

// The HTTP transport must route every connection through Config.DialFunc,
// for plain sends, replies and streams, with the dial timeout applied.
func TestHTTPTransport_UsesDialFunc(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	d := &recordingDialer{}
	config := &Config{
		MsgCodec:         codec.NewJSONCodec(),
		Logger:           logger.NewNullLogger(),
		TCPMaxPacketSize: 65535,
		TCPDialTimeout:   3 * time.Second,
		DialFunc:         d.dial,
	}
	transport := NewHTTPTransport(config)
	node := &Node{ID: NodeID(uuid.New()), advertiseAddr: server.URL, address: Address{URL: server.URL}}

	packet := testPacket(config.MsgCodec, "via dialfunc")
	defer packet.Release()
	if err := transport.Send(TransportBestEffort, node, packet.AddRef()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("server hits = %d, want 1", hits.Load())
	}

	addrs, deadlines := d.calls()
	want := "tcp " + strings.TrimPrefix(server.URL, "http://")
	if len(addrs) != 1 || addrs[0] != want {
		t.Fatalf("DialFunc calls = %q, want [%q]", addrs, want)
	}
	if deadlines[0] <= 0 || deadlines[0] > config.TCPDialTimeout {
		t.Errorf("DialFunc context deadline = %v, want within TCPDialTimeout (%v)", deadlines[0], config.TCPDialTimeout)
	}

	// The pooled connection is reused: no second dial.
	if err := transport.Send(TransportBestEffort, node, packet.AddRef()); err != nil {
		t.Fatalf("second Send: %v", err)
	}
	if addrs, _ := d.calls(); len(addrs) != 1 {
		t.Errorf("idle connection should be reused, DialFunc called %d times", len(addrs))
	}
}

// A DialFunc that refuses must stop the request before anything is sent,
// for Send, SendWithReply and OpenStream alike.
func TestHTTPTransport_DialFuncRefusalBlocksRequests(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer server.Close()

	refused := errors.New("refused by policy")
	d := &recordingDialer{err: refused}
	config := &Config{
		MsgCodec:         codec.NewJSONCodec(),
		Logger:           logger.NewNullLogger(),
		TCPMaxPacketSize: 65535,
		DialFunc:         d.dial,
	}
	transport := NewHTTPTransport(config)
	node := &Node{ID: NodeID(uuid.New()), advertiseAddr: server.URL, address: Address{URL: server.URL}}

	packet := testPacket(config.MsgCodec, "blocked")
	defer packet.Release()

	if err := transport.Send(TransportBestEffort, node, packet.AddRef()); err == nil || !errors.Is(err, refused) {
		t.Errorf("Send error = %v, want %v", err, refused)
	}
	if _, err := transport.SendWithReply(node, packet.AddRef()); err == nil || !errors.Is(err, refused) {
		t.Errorf("SendWithReply error = %v, want %v", err, refused)
	}
	if rc, err := transport.OpenStream(context.Background(), node, packet.AddRef()); err == nil {
		rc.Close()
		t.Error("OpenStream should fail when DialFunc refuses")
	}
	if hits.Load() != 0 {
		t.Errorf("server received %d requests despite DialFunc refusing", hits.Load())
	}
	if addrs, _ := d.calls(); len(addrs) == 0 {
		t.Error("DialFunc was never consulted")
	}
}

// Without a DialFunc the HTTP transport dials directly, as before.
func TestHTTPTransport_NoDialFuncDialsDirectly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	config := &Config{MsgCodec: codec.NewJSONCodec(), Logger: logger.NewNullLogger(), TCPMaxPacketSize: 65535}
	transport := NewHTTPTransport(config)
	node := &Node{ID: NodeID(uuid.New()), advertiseAddr: server.URL, address: Address{URL: server.URL}}
	packet := testPacket(config.MsgCodec, "direct")
	defer packet.Release()
	if err := transport.Send(TransportBestEffort, node, packet.AddRef()); err != nil {
		t.Fatalf("Send without DialFunc: %v", err)
	}
}

// The socket transport's reliable path routes through Config.DialFunc too,
// and a refusal stops the send.
func TestSocketTransport_UsesDialFunc(t *testing.T) {
	newConfig := func(dial func(context.Context, string, string) (net.Conn, error)) *Config {
		return &Config{
			BindAddr:                 "127.0.0.1:0",
			IncomingPacketQueueDepth: 10,
			Logger:                   logger.NewNullLogger(),
			MsgCodec:                 codec.NewJSONCodec(),
			TCPDeadline:              5 * time.Second,
			UDPDeadline:              5 * time.Second,
			TCPDialTimeout:           3 * time.Second,
			UDPMaxPacketSize:         1024,
			TCPMaxPacketSize:         65536,
			DialFunc:                 dial,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()

	receiver := NewSocketTransport(newConfig(nil))
	if err := receiver.Start(ctx, &wg); err != nil {
		t.Fatalf("start receiver: %v", err)
	}
	addr := receiver.tcpListener.Addr().(*net.TCPAddr)
	node := newNode(NodeID(uuid.New()), addr.String())
	node.address = Address{IP: addr.IP, Port: addr.Port}

	d := &recordingDialer{}
	sender := NewSocketTransport(newConfig(d.dial))
	if err := sender.Start(ctx, &wg); err != nil {
		t.Fatalf("start sender: %v", err)
	}
	packet := testPacket(sender.config.MsgCodec, "socket via dialfunc")
	defer packet.Release()
	if err := sender.Send(TransportReliable, node, packet.AddRef()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case got := <-receiver.PacketChannel():
		if string(got.Payload()) != "socket via dialfunc" {
			t.Errorf("payload = %q", got.Payload())
		}
		got.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("receiver got nothing")
	}
	addrs, deadlines := d.calls()
	if len(addrs) == 0 || addrs[0] != "tcp "+addr.String() {
		t.Fatalf("DialFunc calls = %q, want tcp %s", addrs, addr)
	}
	if deadlines[0] <= 0 || deadlines[0] > 3*time.Second {
		t.Errorf("DialFunc context deadline = %v, want within TCPDialTimeout", deadlines[0])
	}

	refused := errors.New("refused by policy")
	blocked := NewSocketTransport(newConfig((&recordingDialer{err: refused}).dial))
	if err := blocked.Start(ctx, &wg); err != nil {
		t.Fatalf("start blocked: %v", err)
	}
	if err := blocked.Send(TransportReliable, node, packet.AddRef()); err == nil || !errors.Is(err, refused) {
		t.Errorf("Send error = %v, want %v", err, refused)
	}
	select {
	case got := <-receiver.PacketChannel():
		got.Release()
		t.Error("receiver got a packet the DialFunc refused")
	case <-time.After(200 * time.Millisecond):
	}
}
