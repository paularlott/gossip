package gossip

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"

	ws "github.com/paularlott/gossip/websocket"
)

// dummyWS implements websocket.WebSocket
type dummyWS struct{}

func (d dummyWS) WriteMessage(messageType int, data []byte) error { return nil }
func (d dummyWS) ReadMessage() (int, []byte, error)               { return 0, nil, errors.New("not implemented") }
func (d dummyWS) NextReader() (int, io.Reader, error)             { return 0, nil, errors.New("not implemented") }
func (d dummyWS) Close() error                                    { return nil }

// fakeWSProvider minimally satisfies websocket.Provider
type fakeWSProvider struct{}

func (f fakeWSProvider) Dial(url string) (ws.WebSocket, error) { return nil, errors.New("not used") }
func (f fakeWSProvider) Upgrade(w http.ResponseWriter, r *http.Request) (ws.WebSocket, error) {
	return nil, errors.New("not used")
}
func (f fakeWSProvider) ToNetConn(_ ws.WebSocket) net.Conn { return nil }
func (f fakeWSProvider) CompressionEnabled() bool          { return false }

// fakeResolver allows deterministic SRV/IP responses
type fakeResolver struct {
	ips    []string
	srv    []*net.TCPAddr
	ipErr  error
	srvErr error
}

func (r *fakeResolver) LookupIP(host string) ([]string, error) {
	if r.ipErr != nil {
		return nil, r.ipErr
	}
	return r.ips, nil
}
func (r *fakeResolver) LookupSRV(service string) ([]*net.TCPAddr, error) {
	if r.srvErr != nil {
		return nil, r.srvErr
	}
	return r.srv, nil
}

func newTestTransport(t *testing.T, cfg *Config) *transport {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = NewNullLogger()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	// Use Port 0 to avoid opening listeners; transport methods under test don't require sockets
	tr, err := NewTransport(ctx, &wg, cfg, Address{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}
	// Immediately stop to close sockets; we only need methods
	cancel()
	wg.Wait()
	return tr
}

func TestResolveAddress_WS_InsecureRejected(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WebsocketProvider = fakeWSProvider{}
	cfg.AllowInsecureWebsockets = false
	tr := newTestTransport(t, cfg)

	if _, err := tr.ResolveAddress("http://example.test/ws"); err == nil {
		t.Fatalf("expected insecure http to be rejected when AllowInsecureWebsockets=false")
	}
	if _, err := tr.ResolveAddress("ws://example.test/ws"); err == nil {
		t.Fatalf("expected insecure ws to be rejected when AllowInsecureWebsockets=false")
	}
}

func TestResolveAddress_HTTP_ToWS(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WebsocketProvider = fakeWSProvider{}
	cfg.AllowInsecureWebsockets = true
	tr := newTestTransport(t, cfg)

	addrs, err := tr.ResolveAddress("http://example.test/ws")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(addrs) != 1 || addrs[0].URL != "ws://example.test/ws" {
		t.Fatalf("got %v, want ws://example.test/ws", addrs)
	}

	// https should become wss
	addrs, err = tr.ResolveAddress("https://example.test/secure")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(addrs) != 1 || addrs[0].URL != "wss://example.test/secure" {
		t.Fatalf("got %v, want wss://example.test/secure", addrs)
	}
}

func TestResolveAddress_SRV_HTTP_URL(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WebsocketProvider = fakeWSProvider{}
	cfg.AllowInsecureWebsockets = false
	cfg.Resolver = &fakeResolver{srv: []*net.TCPAddr{{Port: 8443}}}
	tr := newTestTransport(t, cfg)

	addrs, err := tr.ResolveAddress("srv+https://svc.example/ws")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(addrs) != 1 || addrs[0].URL != "wss://svc.example:8443/ws" {
		t.Fatalf("got %v, want wss://svc.example:8443/ws", addrs)
	}
}

func TestResolveAddress_SRV_SocketDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SocketTransportEnabled = false
	cfg.Resolver = &fakeResolver{}
	tr := newTestTransport(t, cfg)

	if _, err := tr.ResolveAddress("srv+_gossip._tcp.example"); err == nil {
		t.Fatalf("expected error when socket transport disabled for SRV non-WS")
	}
}

func TestLookupSRV_ResolveToIPs_Order(t *testing.T) {
	v4 := net.ParseIP("192.0.2.10")
	v6 := net.ParseIP("2001:db8::1")
	res := &fakeResolver{srv: []*net.TCPAddr{{IP: v4, Port: 1111}, {IP: v6, Port: 2222}}}

	cfg := DefaultConfig()
	cfg.Resolver = res
	tr := newTestTransport(t, cfg)

	// Prefer IPv4 (default)
	addrs, err := tr.lookupSRV("_svc._tcp.example", true)
	if err != nil {
		t.Fatalf("lookupSRV: %v", err)
	}
	if len(addrs) != 2 || !addrs[0].IP.Equal(v4) || addrs[0].Port != 1111 || !addrs[1].IP.Equal(v6) || addrs[1].Port != 2222 {
		t.Fatalf("unexpected order/values: %v", addrs)
	}

	// Prefer IPv6
	cfg2 := DefaultConfig()
	cfg2.Resolver = res
	cfg2.PreferIPv6 = true
	tr2 := newTestTransport(t, cfg2)
	addrs, err = tr2.lookupSRV("_svc._tcp.example", true)
	if err != nil {
		t.Fatalf("lookupSRV v6: %v", err)
	}
	if len(addrs) != 2 || !addrs[0].IP.Equal(v6) || addrs[0].Port != 2222 || !addrs[1].IP.Equal(v4) || addrs[1].Port != 1111 {
		t.Fatalf("unexpected v6-first order: %v", addrs)
	}
}

func TestEnsureNodeAddressResolved_Errors(t *testing.T) {
	cfg := DefaultConfig()
	tr := newTestTransport(t, cfg)

	// No advertise address
	n := &Node{}
	if err := tr.ensureNodeAddressResolved(n); err == nil {
		t.Fatalf("expected error for missing advertise address")
	}

	// Resolver returns error
	cfg2 := DefaultConfig()
	cfg2.Resolver = &fakeResolver{ipErr: errors.New("dns fail")}
	tr2 := newTestTransport(t, cfg2)
	n2 := &Node{advertiseAddr: "host.invalid:1234"}
	if err := tr2.ensureNodeAddressResolved(n2); err == nil {
		t.Fatalf("expected resolve error")
	}

	// Resolver returns zero addresses
	cfg3 := DefaultConfig()
	cfg3.Resolver = &fakeResolver{ips: []string{}}
	tr3 := newTestTransport(t, cfg3)
	n3 := &Node{advertiseAddr: "host.invalid:1234"}
	if err := tr3.ensureNodeAddressResolved(n3); err == nil {
		t.Fatalf("expected no addresses error")
	}
}

func TestDefaultResolver_LookupSRV_Empty(t *testing.T) {
	r := NewDefaultResolver()
	if _, err := r.LookupSRV(""); err == nil {
		t.Fatalf("expected error for empty service name")
	}
}
