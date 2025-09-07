package gossip

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

func TestHTTPTransport_NewHTTPTransport(t *testing.T) {
	config := &Config{
		IncomingPacketQueueDepth: 10,
		MsgCodec:                 codec.NewJsonCodec(),
	}

	transport := NewHTTPTransport(config)

	if transport.Name() != "http" {
		t.Errorf("Expected name 'http', got %s", transport.Name())
	}

	if transport.client.Timeout != 5*time.Second {
		t.Errorf("Expected timeout 5s, got %v", transport.client.Timeout)
	}
}

func TestHTTPTransport_PacketSerialization(t *testing.T) {
	config := &Config{
		MsgCodec: codec.NewJsonCodec(),
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
		MsgCodec: codec.NewJsonCodec(),
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

	flags := uint16(dataWithReply[0])<<8 | uint16(dataWithReply[1])
	if flags&replyExpectedFlag == 0 {
		t.Error("Expected reply flag to be set")
	}

	dataNoReply, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("Failed to serialize packet without reply: %v", err)
	}

	flags2 := uint16(dataNoReply[0])<<8 | uint16(dataNoReply[1])
	if flags2&replyExpectedFlag != 0 {
		t.Error("Expected reply flag to be unset")
	}

	packet.Release()
}

func TestHTTPTransport_HandleGossipRequest_NoReply(t *testing.T) {
	config := &Config{
		IncomingPacketQueueDepth: 10,
		MsgCodec:                 codec.NewJsonCodec(),
		Logger:                   NewNullLogger(),
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
		MsgCodec:                 codec.NewJsonCodec(),
		Logger:                   NewNullLogger(),
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
	flags := uint16(data[0])<<8 | uint16(data[1])
	if flags&replyExpectedFlag == 0 {
		t.Error("Expected reply flag to be set")
	}

	packet.Release()
}

func TestHTTPTransport_Authentication(t *testing.T) {
	config := &Config{
		IncomingPacketQueueDepth: 10,
		MsgCodec:                 codec.NewJsonCodec(),
		Logger:                   NewNullLogger(),
		BearerToken:              "test-token",
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
		MsgCodec: codec.NewJsonCodec(),
		Logger:   NewNullLogger(),
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
		MsgCodec: codec.NewJsonCodec(),
		Logger:   NewNullLogger(),
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

	if node.Address().URL != "http://example.com:8080" {
		t.Errorf("Expected URL to be set to %s, got %s", "http://example.com:8080", node.Address().URL)
	}
}
