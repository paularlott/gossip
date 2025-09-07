package gossip

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/compression"
	"github.com/paularlott/gossip/encryption"
	"github.com/paularlott/gossip/hlc"
)

func TestSocketTransport_NewSocketTransport(t *testing.T) {
	config := &Config{
		BindAddr:                 "127.0.0.1:0",
		IncomingPacketQueueDepth: 10,
		Logger:                   NewNullLogger(),
		MsgCodec:                 codec.NewJsonCodec(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	transport, err := NewSocketTransport(ctx, &wg, config)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	if transport.Name() != "socket" {
		t.Errorf("Expected name 'socket', got %s", transport.Name())
	}

	cancel()
	wg.Wait()
}

func TestSocketTransport_PacketSerialization(t *testing.T) {
	config := &Config{
		MsgCodec:        codec.NewJsonCodec(),
		CompressMinSize: 100,
	}

	transport := &SocketTransport{config: config}

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

	decoded, _, err := transport.packetFromBuffer(data)
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

func TestSocketTransport_PacketWithCompression(t *testing.T) {
	config := &Config{
		MsgCodec:        codec.NewJsonCodec(),
		Compressor:      compression.NewSnappyCompressor(),
		CompressMinSize: 10,
	}

	transport := &SocketTransport{config: config}

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	packet.SetPayload(payload)
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("Failed to serialize packet: %v", err)
	}

	decoded, _, err := transport.packetFromBuffer(data)
	if err != nil {
		t.Fatalf("Failed to deserialize packet: %v", err)
	}

	if len(decoded.Payload()) != len(packet.Payload()) {
		t.Errorf("Payload length mismatch: expected %d, got %d", len(packet.Payload()), len(decoded.Payload()))
	}

	packet.Release()
	decoded.Release()
}

func TestSocketTransport_PacketWithEncryption(t *testing.T) {
	config := &Config{
		MsgCodec:      codec.NewJsonCodec(),
		Cipher:        encryption.NewAESEncryptor(),
		EncryptionKey: []byte("12345678901234567890123456789012"),
	}

	transport := &SocketTransport{config: config}

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("encrypted test payload"))
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("Failed to serialize packet: %v", err)
	}

	decoded, _, err := transport.packetFromBuffer(data)
	if err != nil {
		t.Fatalf("Failed to deserialize packet: %v", err)
	}

	if string(decoded.Payload()) != string(packet.Payload()) {
		t.Errorf("Payload mismatch after encryption: expected %s, got %s", packet.Payload(), decoded.Payload())
	}

	packet.Release()
	decoded.Release()
}

func TestSocketTransport_ReplyExpectedFlag(t *testing.T) {
	config := &Config{
		MsgCodec: codec.NewJsonCodec(),
	}

	transport := &SocketTransport{config: config}

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

	decoded, replyExpected, err := transport.packetFromBuffer(dataWithReply)
	if err != nil {
		t.Fatalf("Failed to deserialize packet with flags: %v", err)
	}

	if !replyExpected {
		t.Error("Expected reply flag to be set")
	}

	dataNoReply, err := transport.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("Failed to serialize packet without reply: %v", err)
	}

	decoded2, replyExpected2, err := transport.packetFromBuffer(dataNoReply)
	if err != nil {
		t.Fatalf("Failed to deserialize packet without reply: %v", err)
	}

	if replyExpected2 {
		t.Error("Expected reply flag to be unset")
	}

	packet.Release()
	decoded.Release()
	decoded2.Release()
}

func TestSocketTransport_SendReceive(t *testing.T) {
	config1 := &Config{
		BindAddr:                 "127.0.0.1:0",
		IncomingPacketQueueDepth: 10,
		Logger:                   NewNullLogger(),
		MsgCodec:                 codec.NewJsonCodec(),
		TCPDeadline:              5 * time.Second,
		UDPDeadline:              5 * time.Second,
		TCPDialTimeout:           5 * time.Second,
		UDPMaxPacketSize:         1024,
		TCPMaxPacketSize:         65536,
	}

	config2 := &Config{
		BindAddr:                 "127.0.0.1:0",
		IncomingPacketQueueDepth: 10,
		Logger:                   NewNullLogger(),
		MsgCodec:                 codec.NewJsonCodec(),
		TCPDeadline:              5 * time.Second,
		UDPDeadline:              5 * time.Second,
		TCPDialTimeout:           5 * time.Second,
		UDPMaxPacketSize:         1024,
		TCPMaxPacketSize:         65536,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	transport1, err := NewSocketTransport(ctx, &wg, config1)
	if err != nil {
		t.Fatalf("Failed to create transport1: %v", err)
	}

	transport2, err := NewSocketTransport(ctx, &wg, config2)
	if err != nil {
		t.Fatalf("Failed to create transport2: %v", err)
	}

	addr1 := transport1.tcpListener.Addr().(*net.TCPAddr)
	addr2 := transport2.tcpListener.Addr().(*net.TCPAddr)

	node1 := newNode(NodeID(uuid.New()), addr1.String())
	node1.address = Address{IP: addr1.IP, Port: addr1.Port}

	node2 := newNode(NodeID(uuid.New()), addr2.String())
	node2.address = Address{IP: addr2.IP, Port: addr2.Port}

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("TCP test message"))
	packet.SetCodec(config2.MsgCodec)

	err = transport2.Send(TransportReliable, node1, packet)
	if err != nil {
		t.Fatalf("Failed to send TCP packet: %v", err)
	}

	select {
	case receivedPacket := <-transport1.PacketChannel():
		if string(receivedPacket.Payload()) != "TCP test message" {
			t.Errorf("Received wrong payload: %s", receivedPacket.Payload())
		}
		receivedPacket.Release()
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for TCP packet")
	}

	packet.Release()

	cancel()
	wg.Wait()
}

func TestSocketTransport_ErrorCases(t *testing.T) {
	config := &Config{
		MsgCodec: codec.NewJsonCodec(),
	}

	transport := &SocketTransport{config: config}

	_, _, err := transport.packetFromBuffer([]byte{0x00})
	if err == nil {
		t.Error("Expected error for packet too small")
	}

	_, _, err = transport.packetFromBuffer([]byte{0xFF, 0xFF, 0x00})
	if err == nil {
		t.Error("Expected error for invalid header size")
	}
}
