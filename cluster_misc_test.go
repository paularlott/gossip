package gossip

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip/codec"
)

// fakeTransportWS extends Transport for WebsocketHandler visibility in tests.
type fakeTransportWS struct {
	fakeTransport
	wsCalled bool
}

func (f *fakeTransportWS) WebsocketHandler(_ context.Context, _ http.ResponseWriter, _ *http.Request) {
	f.wsCalled = true
}

// helper to build a minimal cluster with injectable transport
func newTestClusterWithTransport(t *testing.T, tr Transport) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SocketTransportEnabled = false
	cfg.WebsocketProvider = nil
	cfg.Transport = tr
	cfg.MsgCodec = codec.NewJsonCodec()
	// fast timings
	cfg.HealthCheckInterval = 50 * time.Millisecond
	cfg.SuspectAttemptInterval = 50 * time.Millisecond
	cfg.RecoveryAttemptInterval = 50 * time.Millisecond
	cfg.PingTimeout = 30 * time.Millisecond
	// Disable background loops that can race with test logic
	cfg.StateSyncInterval = time.Hour
	cfg.MsgHistoryGCInterval = 50 * time.Millisecond
	cfg.MsgHistoryMaxAge = 50 * time.Millisecond

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	c.Start()
	return c
}

// newUnstartedCluster creates a cluster suitable for unit tests without starting any background goroutines.
func newUnstartedCluster(t *testing.T) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SocketTransportEnabled = false
	cfg.WebsocketProvider = nil
	cfg.Transport = &fakeTransport{}
	cfg.MsgCodec = codec.NewJsonCodec()
	// Disable periodic loops
	cfg.HealthCheckInterval = time.Hour
	cfg.SuspectAttemptInterval = time.Hour
	cfg.RecoveryAttemptInterval = time.Hour
	cfg.StateSyncInterval = time.Hour
	cfg.GossipInterval = time.Hour

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	return c
}

func TestAddress_StringFormats(t *testing.T) {
	a := Address{}
	if got := a.String(); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}

	a = Address{IP: net.ParseIP("127.0.0.1"), Port: 9000}
	if got := a.String(); got != "127.0.0.1:9000" {
		t.Fatalf("unexpected: %q", got)
	}

	a = Address{URL: "wss://example/ws"}
	if got := a.String(); got != "wss://example/ws" {
		t.Fatalf("unexpected: %q", got)
	}

	a = Address{IP: net.ParseIP("::1"), Port: 8080, URL: "wss://example/ws"}
	// URL should append after host:port
	if got := a.String(); got != "::1:8080, wss://example/ws" {
		t.Fatalf("unexpected: %q", got)
	}
}

func TestAddress_IsEmptyAndClear(t *testing.T) {
	var a Address
	if !a.IsEmpty() {
		t.Fatalf("new Address should be empty")
	}
	a.IP = net.ParseIP("127.0.0.1")
	a.Port = 1234
	a.URL = "ws://x"
	if a.IsEmpty() {
		t.Fatalf("populated Address should not be empty")
	}
	a.Clear()
	if !a.IsEmpty() {
		t.Fatalf("Clear should reset to empty")
	}
}

func TestCluster_GettersAndCounts(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	if c.LocalNode() == nil || c.LocalMetadata() == nil {
		t.Fatalf("local getters should not be nil")
	}

	// Add nodes in different states
	nAlive := newNode(NodeID{1}, "")
	nSus := newNode(NodeID{2}, "")
	nDead := newNode(NodeID{3}, "")
	c.nodes.addOrUpdate(nAlive)
	c.nodes.addOrUpdate(nSus)
	c.nodes.addOrUpdate(nDead)
	c.nodes.updateState(nSus.ID, NodeSuspect)
	c.nodes.updateState(nDead.ID, NodeDead)

	if got := c.NumNodes(); got < 3 { // includes local as well
		t.Fatalf("expected at least 3 nodes including local, got %d", got)
	}
	if got := c.NumAliveNodes(); got < 1 {
		t.Fatalf("expected >=1 alive, got %d", got)
	}
	if got := c.NumSuspectNodes(); got != 1 {
		t.Fatalf("expected 1 suspect, got %d", got)
	}
	if got := c.NumDeadNodes(); got != 1 {
		t.Fatalf("expected 1 dead, got %d", got)
	}

	if node := c.GetNode(nAlive.ID); node == nil || !c.NodeIsLocal(c.LocalNode()) || c.NodeIsLocal(nAlive) {
		t.Fatalf("node/local checks failed")
	}

	idStr := nAlive.ID.String()
	if got := c.GetNodeByIDString(idStr); got == nil || got.ID != nAlive.ID {
		t.Fatalf("GetNodeByIDString returned wrong node")
	}
	if got := c.GetNodeByIDString("not-a-uuid"); got != nil {
		t.Fatalf("expected nil for invalid id string")
	}

	ids := c.NodesToIDs([]*Node{nAlive, nSus})
	if len(ids) != 2 || ids[0] != nAlive.ID || ids[1] != nSus.ID {
		t.Fatalf("NodesToIDs mismatch")
	}
}

func TestEventHandlers_GetHandlers(t *testing.T) {
	h := NewEventHandlers[int]()
	a := h.Add(1)
	b := h.Add(2)
	got := h.GetHandlers()
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("unexpected handlers slice: %v", got)
	}
	// remove one and ensure order & length update
	if !h.Remove(a) || !h.Remove(b) {
		t.Fatalf("Remove failed")
	}
	if len(h.GetHandlers()) != 0 {
		t.Fatalf("expected empty after removes")
	}
}

func TestWebsocketHandler_Auth(t *testing.T) {
	ft := &fakeTransportWS{}
	c := newTestClusterWithTransport(t, ft)
	defer c.Stop()

	c.config.BearerToken = "secret"

	// Missing header -> 401
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	c.WebsocketHandler(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing header, got %d", rr.Code)
	}

	// Wrong token -> 401
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.Header.Set("Authorization", "Bearer nope")
	c.WebsocketHandler(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong token, got %d", rr.Code)
	}

	// Correct token -> delegate to transport
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.Header.Set("Authorization", "Bearer secret")
	c.WebsocketHandler(rr, req)
	if !ft.wsCalled {
		t.Fatalf("expected transport.WebsocketHandler to be called on success")
	}
}

func TestHandleRegistrationAndUnregister(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// invalid types
	if err := c.HandleFunc(ReservedMsgsStart-1, func(*Node, *Packet) error { return nil }); err == nil {
		t.Fatalf("expected error for invalid message type")
	}
	if err := c.HandleFuncNoForward(ReservedMsgsStart-1, func(*Node, *Packet) error { return nil }); err == nil {
		t.Fatalf("expected error for invalid message type")
	}
	if err := c.HandleFuncWithReply(ReservedMsgsStart-1, func(*Node, *Packet) (interface{}, error) { return nil, nil }); err == nil {
		t.Fatalf("expected error for invalid message type")
	}

	// register & unregister a valid handler
	msgType := ReservedMsgsStart + 100
	if err := c.HandleFunc(msgType, func(*Node, *Packet) error { return nil }); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if !c.UnregisterMessageType(msgType) {
		t.Fatalf("unregister failed")
	}
}

func TestHealthMonitor_BroadcastHelpers(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	var sentAlive int64
	var sentSusp int64
	ft := c.transport.(*fakeTransport)
	ft.SetOnSend(func(pkt *Packet) error {
		switch pkt.MessageType {
		case aliveMsg:
			atomic.AddInt64(&sentAlive, 1)
		case suspicionMsg:
			atomic.AddInt64(&sentSusp, 1)
		}
		return nil
	})

	// Trigger broadcasts directly
	c.healthMonitor.broadcastAlive(c.localNode)
	c.healthMonitor.broadcastSuspicion(c.localNode)

	// Allow async queue
	time.Sleep(30 * time.Millisecond)
	if atomic.LoadInt64(&sentAlive) == 0 || atomic.LoadInt64(&sentSusp) == 0 {
		t.Fatalf("expected alive & suspicion messages to be sent; got alive=%d susp=%d", sentAlive, sentSusp)
	}
}

func TestAdjustGossipInterval_IncreaseAndDecrease(t *testing.T) {
	c := newUnstartedCluster(t)
	defer c.Stop()

	// Prepare ticker so Reset calls are safe
	c.gossipTicker = time.NewTicker(10 * time.Second)
	defer c.gossipTicker.Stop()
	c.gossipInterval = 100 * time.Millisecond
	// Gossip manager is not running, so no concurrent updates occur

	// Force increase (duration > interval)
	c.adjustGossipInterval(200 * time.Millisecond)
	if c.gossipInterval < 200*time.Millisecond {
		t.Fatalf("expected interval to increase to at least duration; got %s", c.gossipInterval)
	}

	// Force decrease path: set interval above base, and duration fast
	base := c.config.GossipInterval
	c.gossipInterval = base * 2
	c.adjustGossipInterval(10 * time.Millisecond)
	if c.gossipInterval < base {
		t.Fatalf("interval should not drop below base; base=%s got=%s", base, c.gossipInterval)
	}
}
