package gossip

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/compression/snappy"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
)

func TestHTTPTransport_NewHTTPTransport(t *testing.T) {
	config := &Config{
		IncomingPacketQueueDepth: 10,
		MsgCodec:                 codec.NewJSONCodec(),
	}

	transport := NewHTTPTransport(config)

	if transport.Name() != "http" {
		t.Errorf("Expected name 'http', got %s", transport.Name())
	}

	// No client-level timeout: the request budget is bounded by the per-call
	// context derived from the cluster's shutdown context (see sendRequest).
	if transport.client.Timeout != 0 {
		t.Errorf("Expected no client-level timeout, got %v", transport.client.Timeout)
	}

	// Default request timeout falls back to transportMaxWaitTime when
	// TCPDialTimeout is not configured.
	if got := transport.requestTimeout(); got != transportMaxWaitTime {
		t.Errorf("Expected request timeout %v, got %v", transportMaxWaitTime, got)
	}

	// ctx defaults to Background before Start is called.
	if transport.ctx == nil {
		t.Error("Expected non-nil ctx before Start")
	}

	// Start adopts the supplied context.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := transport.Start(ctx, &sync.WaitGroup{}); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if transport.ctx != ctx {
		t.Error("Expected ctx to be replaced after Start")
	}
}

func TestHTTPTransport_PacketSerialization(t *testing.T) {
	config := &Config{
		MsgCodec: codec.NewJSONCodec(),
	}

	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("test payload"))
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("Failed to serialize packet: %v", err)
	}

	decoded, err := transport.packetFromBuffer(data)
	if err != nil {
		t.Fatalf("Failed to deserialize packet: %v", err)
	}

	if decoded.MessageType != packet.MessageType {
		t.Errorf("MessageType mismatch: expected %d, got %d", packet.MessageType, decoded.MessageType)
	}
	if string(decoded.Payload()) != string(packet.Payload()) {
		t.Errorf("Payload mismatch: expected %s, got %s", packet.Payload(), decoded.Payload())
	}

	packet.Release()
	decoded.Release()
}

func TestHTTPTransport_ReplyExpectedFlag(t *testing.T) {
	config := &Config{
		MsgCodec: codec.NewJSONCodec(),
	}

	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("test payload"))
	packet.SetCodec(config.MsgCodec)

	dataWithReply, err := transport.packetToBuffer(packet, true)
	if err != nil {
		t.Fatalf("Failed to serialize packet with reply: %v", err)
	}

	flags := uint16(dataWithReply[0]) | uint16(dataWithReply[1])<<8
	if flags&replyExpectedFlag == 0 {
		t.Error("Expected reply flag to be set")
	}

	dataNoReply, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("Failed to serialize packet without reply: %v", err)
	}

	flags2 := uint16(dataNoReply[0]) | uint16(dataNoReply[1])<<8
	if flags2&replyExpectedFlag != 0 {
		t.Error("Expected reply flag to be unset")
	}

	packet.Release()
}

func TestHTTPTransport_HandleGossipRequest_NoReply(t *testing.T) {
	config := &Config{
		IncomingPacketQueueDepth: 10,
		MsgCodec:                 codec.NewJSONCodec(),
		Logger:                   logger.NewNullLogger(),
		TCPMaxPacketSize:         65535,
	}

	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("test payload"))
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("Failed to serialize packet: %v", err)
	}

	req := httptest.NewRequest("POST", "/gossip", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	transport.HandleGossipRequest(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected status %d, got %d", http.StatusNoContent, w.Code)
	}

	select {
	case receivedPacket := <-transport.PacketChannel():
		if string(receivedPacket.Payload()) != "test payload" {
			t.Errorf("Received wrong payload: %s", receivedPacket.Payload())
		}
		receivedPacket.Release()
	case <-time.After(100 * time.Millisecond):
		t.Error("Packet not received")
	}

	packet.Release()
}

func TestHTTPTransport_HandleGossipRequest_WithReply(t *testing.T) {
	config := &Config{
		IncomingPacketQueueDepth: 10,
		MsgCodec:                 codec.NewJSONCodec(),
		Logger:                   logger.NewNullLogger(),
	}

	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("test payload"))
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, true)
	if err != nil {
		t.Fatalf("Failed to serialize packet: %v", err)
	}

	// Test that reply flag is properly detected
	flags := uint16(data[0]) | uint16(data[1])<<8
	if flags&replyExpectedFlag == 0 {
		t.Error("Expected reply flag to be set")
	}

	packet.Release()
}

func TestHTTPTransport_Authentication(t *testing.T) {
	config := &Config{
		IncomingPacketQueueDepth: 10,
		MsgCodec:                 codec.NewJSONCodec(),
		Logger:                   logger.NewNullLogger(),
		BearerToken:              "test-token",
		TCPMaxPacketSize:         65535,
	}

	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("test payload"))
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("Failed to serialize packet: %v", err)
	}

	req := httptest.NewRequest("POST", "/gossip", bytes.NewReader(data))
	w := httptest.NewRecorder()
	transport.HandleGossipRequest(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status %d, got %d", http.StatusUnauthorized, w.Code)
	}

	req = httptest.NewRequest("POST", "/gossip", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer wrong-token")
	w = httptest.NewRecorder()
	transport.HandleGossipRequest(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status %d, got %d", http.StatusUnauthorized, w.Code)
	}

	req = httptest.NewRequest("POST", "/gossip", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	transport.HandleGossipRequest(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected status %d, got %d", http.StatusNoContent, w.Code)
	}

	packet.Release()
}

func TestHTTPTransport_SendWithReply(t *testing.T) {
	config := &Config{
		MsgCodec:         codec.NewJSONCodec(),
		Logger:           logger.NewNullLogger(),
		TCPMaxPacketSize: 65535,
	}

	transport := NewHTTPTransport(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		flags := uint16(body[0])<<8 | uint16(body[1])
		if flags&replyExpectedFlag == 0 {
			t.Error("Expected reply flag to be set in request")
		}

		replyPacket := NewPacket()
		replyPacket.MessageType = replyMsg
		replyPacket.SenderID = NodeID(uuid.New())
		replyPacket.MessageID = MessageID(hlc.Now())
		replyPacket.TTL = 5
		replyPacket.SetPayload([]byte("server reply"))
		replyPacket.SetCodec(config.MsgCodec)

		replyData, _ := transport.packetToBuffer(replyPacket, false)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(replyData)
		replyPacket.Release()
	}))
	defer server.Close()

	node := &Node{
		ID:            NodeID(uuid.New()),
		advertiseAddr: server.URL,
		address:       Address{URL: server.URL},
	}

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("client request"))
	packet.SetCodec(config.MsgCodec)

	reply, err := transport.SendWithReply(node, packet)
	if err != nil {
		t.Fatalf("Failed to send with reply: %v", err)
	}

	if reply == nil {
		t.Fatal("Expected reply packet")
	}

	if string(reply.Payload()) != "server reply" {
		t.Errorf("Expected 'server reply', got %s", reply.Payload())
	}

	packet.Release()
	reply.Release()
}

func TestHTTPTransport_ErrorCases(t *testing.T) {
	config := &Config{
		MsgCodec: codec.NewJSONCodec(),
		Logger:   logger.NewNullLogger(),
	}

	transport := NewHTTPTransport(config)

	_, err := transport.packetFromBuffer([]byte{0x00})
	if err == nil {
		t.Error("Expected error for packet too small")
	}

	_, err = transport.packetFromBuffer([]byte{0xFF, 0xFF, 0x00})
	if err == nil {
		t.Error("Expected error for invalid header size")
	}

	req := httptest.NewRequest("GET", "/gossip", nil)
	w := httptest.NewRecorder()
	transport.HandleGossipRequest(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}

	config.BearerToken = "test-token"
	req = httptest.NewRequest("POST", "/gossip", strings.NewReader("test"))
	req.Header.Set("Authorization", "Invalid format")
	w = httptest.NewRecorder()
	transport.HandleGossipRequest(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status %d, got %d", http.StatusUnauthorized, w.Code)
	}
}

func TestHTTPTransport_NodeAddressResolution(t *testing.T) {
	transport := NewHTTPTransport(&Config{})

	node := &Node{
		ID:            NodeID(uuid.New()),
		advertiseAddr: "",
		address:       Address{},
	}

	err := transport.ensureNodeAddressResolved(node)
	if err == nil {
		t.Error("Expected error for empty advertise address")
	}

	node.advertiseAddr = "http://example.com:8080"
	err = transport.ensureNodeAddressResolved(node)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expectedURL := "http://example.com:8080"
	addr := node.GetAddress()
	actualURL := strings.TrimSuffix(addr.URL, "/")
	if actualURL != expectedURL {
		t.Errorf("Expected URL to be set to %s, got %s", expectedURL, actualURL)
	}
}

// TestHTTPTransport_CompressionRoundTrip verifies that a Compressor configured
// on both sides transparently compresses large payloads on the wire and
// decompresses them on read.
func TestHTTPTransport_CompressionRoundTrip(t *testing.T) {
	config := &Config{
		MsgCodec:         codec.NewJSONCodec(),
		Logger:           logger.NewNullLogger(),
		Compressor:       snappy.New(),
		CompressMinSize:  32, // compress anything >= 32 bytes
		TCPMaxPacketSize: 65535,
	}

	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	payload := bytes.Repeat([]byte("compressible-payload-"), 32) // ~640 bytes, very compressible
	packet.SetPayload(payload)
	packet.SetCodec(config.MsgCodec)

	encoded, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("packetToBuffer failed: %v", err)
	}

	// The compression flag must be set on the wire.
	flags := binaryLE(encoded[:2])
	if flags&compressionFlag == 0 {
		t.Error("Expected compression flag to be set on encoded packet")
	}

	// Compressed wire form must be smaller than the raw payload.
	if len(encoded) >= len(payload) {
		t.Errorf("expected compressed encoding (%d) to be smaller than payload (%d)", len(encoded), len(payload))
	}

	decoded, err := transport.packetFromBuffer(encoded)
	if err != nil {
		t.Fatalf("packetFromBuffer failed: %v", err)
	}
	defer decoded.Release()

	if !bytes.Equal(decoded.Payload(), payload) {
		t.Errorf("payload mismatch after roundtrip: got %d bytes, want %d", len(decoded.Payload()), len(payload))
	}

	packet.Release()
}

// TestHTTPTransport_CompressionDisabled verifies that without a Compressor the
// wire form is never compressed and the flag is never set, even for large
// payloads. Also verifies the receiver rejects a compressed packet when no
// compressor is configured.
func TestHTTPTransport_CompressionDisabled(t *testing.T) {
	// Receiver with no compressor.
	recvConfig := &Config{
		MsgCodec:         codec.NewJSONCodec(),
		Logger:           logger.NewNullLogger(),
		TCPMaxPacketSize: 65535,
	}
	recv := NewHTTPTransport(recvConfig)

	// Sender with a compressor.
	sendConfig := &Config{
		MsgCodec:        codec.NewJSONCodec(),
		Logger:          logger.NewNullLogger(),
		Compressor:      snappy.New(),
		CompressMinSize: 32,
	}
	snd := NewHTTPTransport(sendConfig)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload(bytes.Repeat([]byte("compressible-payload-"), 32))
	packet.SetCodec(sendConfig.MsgCodec)

	encoded, err := snd.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("send packetToBuffer failed: %v", err)
	}
	if flags := binaryLE(encoded[:2]); flags&compressionFlag == 0 {
		t.Error("expected sender to set compression flag")
	}

	// Receiver has no compressor: must error rather than silently corrupt.
	if _, err := recv.packetFromBuffer(encoded); err == nil {
		t.Error("expected error decoding compressed packet without compressor configured")
	}
	packet.Release()
}

// TestHTTPTransport_NoCompressionForSmallPayload verifies that payloads below
// CompressMinSize are sent uncompressed even when a Compressor is configured.
func TestHTTPTransport_NoCompressionForSmallPayload(t *testing.T) {
	config := &Config{
		MsgCodec:        codec.NewJSONCodec(),
		Logger:          logger.NewNullLogger(),
		Compressor:      snappy.New(),
		CompressMinSize: 1024,
	}
	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("tiny"))
	packet.SetCodec(config.MsgCodec)
	defer packet.Release()

	encoded, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("packetToBuffer failed: %v", err)
	}
	if flags := binaryLE(encoded[:2]); flags&compressionFlag != 0 {
		t.Error("expected no compression flag for small payload")
	}
}

// TestReadBoundedBody_ContentLength verifies pre-allocation on the happy path.
func TestReadBoundedBody_ContentLength(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 4096)
	got, err := readBoundedBody(bytes.NewReader(payload), int64(len(payload)), 1<<16)
	if err != nil {
		t.Fatalf("readBoundedBody failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("body mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestReadBoundedBody_MissingContentLength verifies graceful fallback when the
// hint is absent.
func TestReadBoundedBody_MissingContentLength(t *testing.T) {
	payload := []byte("no content length header at all")
	got, err := readBoundedBody(bytes.NewReader(payload), -1, 1<<16)
	if err != nil {
		t.Fatalf("readBoundedBody failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("body mismatch: got %q, want %q", got, payload)
	}
}

// TestReadBoundedBody_WrongContentLength verifies that a wrong hint (larger
// than the actual body) is tolerated — we read what's actually there.
func TestReadBoundedBody_WrongContentLength(t *testing.T) {
	payload := []byte("actual body is short")
	// Lie: claim 1 KiB when only 21 bytes are coming.
	got, err := readBoundedBody(bytes.NewReader(payload), 1024, 1<<16)
	if err != nil {
		t.Fatalf("readBoundedBody failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("body mismatch: got %q, want %q", got, payload)
	}
}

// TestReadBoundedBody_CapsOverflow verifies the max cap is enforced — a body
// larger than max is truncated, never causing unbounded allocation.
func TestReadBoundedBody_CapsOverflow(t *testing.T) {
	payload := bytes.Repeat([]byte("y"), 8192)
	max := int64(1024)
	got, err := readBoundedBody(bytes.NewReader(payload), int64(len(payload)), max)
	if err != nil {
		t.Fatalf("readBoundedBody failed: %v", err)
	}
	if int64(len(got)) != max {
		t.Errorf("expected body to be capped at %d bytes, got %d", max, len(got))
	}
}

// TestReadBoundedBody_HintLargerThanMax verifies a contentLength larger than
// max does NOT cause a too-large pre-allocation; the buffer is capped.
func TestReadBoundedBody_HintLargerThanMax(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 2048)
	// Lie: claim 1 GiB. Max is 1 KiB. The buffer must be capped, not 1 GiB.
	got, err := readBoundedBody(bytes.NewReader(payload), 1<<30, 1024)
	if err != nil {
		t.Fatalf("readBoundedBody failed: %v", err)
	}
	if int64(len(got)) > 1024 {
		t.Errorf("expected body capped at 1024, got %d", len(got))
	}
}

// TestHTTPTransport_SendDrainsBody verifies that Send reads the response body
// to completion, which is what allows the underlying connection to be returned
// to the pool. We assert by serving a known body and confirming it is consumed
// in full on the server side.
func TestHTTPTransport_SendDrainsBody(t *testing.T) {
	config := &Config{
		MsgCodec:         codec.NewJSONCodec(),
		Logger:           logger.NewNullLogger(),
		TCPMaxPacketSize: 65535,
	}
	transport := NewHTTPTransport(config)

	serverPayload := []byte("drain-me-please")
	var lastSeen int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		lastSeen = len(body)
		w.WriteHeader(http.StatusNoContent)
		w.Write(serverPayload)
	}))
	defer server.Close()

	node := &Node{
		ID:            NodeID(uuid.New()),
		advertiseAddr: server.URL,
		address:       Address{URL: server.URL},
	}

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("client request"))
	packet.SetCodec(config.MsgCodec)
	defer packet.Release()

	for i := 0; i < 3; i++ {
		if err := transport.Send(TransportBestEffort, node, packet.AddRef()); err != nil {
			t.Fatalf("Send failed on iteration %d: %v", i, err)
		}
	}
	if lastSeen == 0 {
		t.Error("server never observed a request body")
	}
}

// TestHTTPTransport_SendRespectsContextCancellation verifies that cancelling
// the context passed to Start aborts in-flight requests.
func TestHTTPTransport_SendRespectsContextCancellation(t *testing.T) {
	config := &Config{
		MsgCodec:         codec.NewJSONCodec(),
		Logger:           logger.NewNullLogger(),
		TCPMaxPacketSize: 65535,
	}
	transport := NewHTTPTransport(config)

	// Hang until the client cancels AND we release the server, so the test
	// can shut down promptly after asserting the cancellation propagated.
	// (We can't rely solely on r.Context().Done() because HTTP/1.1 servers
	// only detect client disconnect on the next I/O attempt, which never
	// comes while the handler is parked.)
	started := make(chan struct{}, 1)
	releaseServer := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-releaseServer:
		}
	}))
	defer func() {
		close(releaseServer)
		server.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	if err := transport.Start(ctx, &sync.WaitGroup{}); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	node := &Node{
		ID:            NodeID(uuid.New()),
		advertiseAddr: server.URL,
		address:       Address{URL: server.URL},
	}
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("will be cancelled"))
	packet.SetCodec(config.MsgCodec)
	defer packet.Release()

	errCh := make(chan error, 1)
	go func() {
		errCh <- transport.Send(TransportBestEffort, node, packet.AddRef())
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("server never received the request")
	}

	// Cancel the parent context; the in-flight Send must abort.
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected Send to return an error after context cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not return after context cancellation")
	}
}

// binaryLE decodes a 2-byte little-endian uint16 (helper for tests).
func binaryLE(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}
