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
	"github.com/paularlott/gossip/compression/snappy"
	"github.com/paularlott/gossip/encryption/aes"
)

// ============================================================================
// mockResolver - provides canned DNS/SRV responses
// ============================================================================

type mockResolver struct {
	ips        []string
	ipErr      error
	srvAddrs   []*net.TCPAddr
	srvErr     error
	lookupCall int
}

func (r *mockResolver) LookupIP(host string) ([]string, error) {
	r.lookupCall++
	return r.ips, r.ipErr
}

func (r *mockResolver) LookupSRV(service string) ([]*net.TCPAddr, error) {
	r.lookupCall++
	return r.srvAddrs, r.srvErr
}

// ============================================================================
// SocketTransport: resolveAddress, lookupIP, lookupSRV
// ============================================================================

func TestSocketTransport_ResolveAddressSRV(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Resolver = &mockResolver{
		srvAddrs: []*net.TCPAddr{
			{IP: net.ParseIP("10.0.0.1"), Port: 7946},
			{IP: net.ParseIP("10.0.0.2"), Port: 7946},
		},
	}

	st := NewSocketTransport(config)
	addrs, err := st.resolveAddress("srv+myservice.consul")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(addrs) != 2 {
		t.Fatalf("Expected 2 addrs, got %d", len(addrs))
	}
	if addrs[0].Port != 7946 {
		t.Errorf("Expected port 7946, got %d", addrs[0].Port)
	}
}

func TestSocketTransport_ResolveAddressSRVError(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Resolver = &mockResolver{
		srvErr: fmt.Errorf("SRV lookup failed"),
	}

	st := NewSocketTransport(config)
	_, err := st.resolveAddress("srv+myservice.consul")
	if err == nil {
		t.Fatal("Expected error from SRV lookup")
	}
}

func TestSocketTransport_ResolveAddressSRVPreferIPv6(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.PreferIPv6 = true
	config.Resolver = &mockResolver{
		srvAddrs: []*net.TCPAddr{
			{IP: net.ParseIP("10.0.0.1"), Port: 7946},
			{IP: net.ParseIP("::1"), Port: 7947},
		},
	}

	st := NewSocketTransport(config)
	addrs, err := st.resolveAddress("srv+myservice.consul")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(addrs) < 2 {
		t.Fatalf("Expected at least 2 addrs, got %d", len(addrs))
	}
	// IPv6 should come first when PreferIPv6=true
	if addrs[0].IP.To4() != nil {
		t.Error("Expected IPv6 address first when PreferIPv6=true")
	}
}

func TestSocketTransport_LookupIPDirectIP(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	addrs, err := st.lookupIP("127.0.0.1:8080")
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}
	if len(addrs) != 1 {
		t.Fatalf("Expected 1 addr, got %d", len(addrs))
	}
	if addrs[0].Port != 8080 {
		t.Errorf("Expected port 8080, got %d", addrs[0].Port)
	}
}

func TestSocketTransport_LookupIPNoPort(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	_, err := st.lookupIP("127.0.0.1")
	if err == nil {
		t.Fatal("Expected error for address without port")
	}
}

func TestSocketTransport_LookupIPPortOnly(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	addrs, err := st.lookupIP("8080")
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}
	if len(addrs) != 1 {
		t.Fatalf("Expected 1 addr, got %d", len(addrs))
	}
	if addrs[0].Port != 8080 {
		t.Errorf("Expected port 8080, got %d", addrs[0].Port)
	}
	if !addrs[0].IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Errorf("Expected 127.0.0.1, got %s", addrs[0].IP)
	}
}

func TestSocketTransport_LookupIPEmptyHost(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	addrs, err := st.lookupIP(":9000")
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}
	if len(addrs) != 1 {
		t.Fatalf("Expected 1 addr, got %d", len(addrs))
	}
	if !addrs[0].IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Errorf("Expected 127.0.0.1, got %s", addrs[0].IP)
	}
}

func TestSocketTransport_LookupIPInvalidPort(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	_, err := st.lookupIP("127.0.0.1:notaport")
	if err == nil {
		t.Fatal("Expected error for invalid port")
	}
}

func TestSocketTransport_LookupIPResolvesHostname(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Resolver = &mockResolver{
		ips: []string{"192.168.1.1", "fd00::1"},
	}

	st := NewSocketTransport(config)
	addrs, err := st.lookupIP("myhost:7946")
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}
	if len(addrs) != 2 {
		t.Fatalf("Expected 2 addrs, got %d", len(addrs))
	}
	// Default is prefer IPv4
	if addrs[0].IP.To4() == nil {
		t.Error("IPv4 should come first by default")
	}
}

func TestSocketTransport_LookupIPResolvesHostnamePreferIPv6(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.PreferIPv6 = true
	config.Resolver = &mockResolver{
		ips: []string{"192.168.1.1", "fd00::1"},
	}

	st := NewSocketTransport(config)
	addrs, err := st.lookupIP("myhost:7946")
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}
	if len(addrs) != 2 {
		t.Fatalf("Expected 2 addrs, got %d", len(addrs))
	}
	if addrs[0].IP.To4() != nil {
		t.Error("IPv6 should come first when PreferIPv6=true")
	}
}

func TestSocketTransport_LookupIPResolveError(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Resolver = &mockResolver{
		ipErr: fmt.Errorf("DNS lookup failed"),
	}

	st := NewSocketTransport(config)
	_, err := st.lookupIP("unknownhost:7946")
	if err == nil {
		t.Fatal("Expected DNS lookup error")
	}
}

func TestSocketTransport_LookupSRVWithoutResolve(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Resolver = &mockResolver{
		srvAddrs: []*net.TCPAddr{
			{Port: 7946},
			{Port: 7947},
		},
	}

	st := NewSocketTransport(config)
	addrs, err := st.lookupSRV("myservice.consul", false)
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}
	if len(addrs) != 2 {
		t.Fatalf("Expected 2 addrs, got %d", len(addrs))
	}
	if addrs[0].IP != nil {
		t.Error("IP should be nil when resolveToIPs=false")
	}
	if addrs[0].Port != 7946 {
		t.Errorf("Expected port 7946, got %d", addrs[0].Port)
	}
}

func TestSocketTransport_LookupSRVEmpty(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Resolver = &mockResolver{
		srvAddrs: []*net.TCPAddr{},
	}

	st := NewSocketTransport(config)
	_, err := st.lookupSRV("myservice.consul", true)
	if err == nil {
		t.Fatal("Expected error for empty SRV results")
	}
}

// ============================================================================
// SocketTransport: parseBindAddress
// ============================================================================

func TestSocketTransport_ParseBindAddressLeadingColon(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	addr, err := st.parseBindAddress(":8080")
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}
	if addr.Port != 8080 {
		t.Errorf("Expected port 8080, got %d", addr.Port)
	}
}

func TestSocketTransport_ParseBindAddressFull(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	addr, err := st.parseBindAddress("192.168.1.1:9000")
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}
	if addr.Port != 9000 {
		t.Errorf("Expected port 9000, got %d", addr.Port)
	}
	if !addr.IP.Equal(net.ParseIP("192.168.1.1")) {
		t.Errorf("Expected IP 192.168.1.1, got %s", addr.IP)
	}
}

func TestSocketTransport_ParseBindAddressInvalid(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	_, err := st.parseBindAddress("not[valid:address")
	if err == nil {
		t.Fatal("Expected error for invalid address")
	}
}

// ============================================================================
// SocketTransport: ensureNodeAddressResolved
// ============================================================================

func TestSocketTransport_EnsureNodeAddressResolvedAlreadyResolved(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8080")
	node.SetAddress(Address{IP: net.ParseIP("127.0.0.1"), Port: 8080})

	err := st.ensureNodeAddressResolved(node)
	if err != nil {
		t.Fatalf("Should not error for already-resolved node: %v", err)
	}
}

func TestSocketTransport_EnsureNodeAddressResolvedEmpty(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	node := newNode(NodeID(uuid.New()), "")

	err := st.ensureNodeAddressResolved(node)
	if err == nil {
		t.Fatal("Expected error for empty advertise address")
	}
}

func TestSocketTransport_EnsureNodeAddressResolved(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8080")

	err := st.ensureNodeAddressResolved(node)
	if err != nil {
		t.Fatalf("Should resolve 127.0.0.1:8080: %v", err)
	}
	if node.IsAddressEmpty() {
		t.Error("Address should be resolved after ensure")
	}
}

// ============================================================================
// SocketTransport: packetToBuffer / packetFromBuffer round-trip
// ============================================================================

func TestSocketTransport_PacketRoundTripPlain(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)

	packet := NewPacket()
	packet.TTL = 5
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("hello")
	packet.SetPayload(payload)

	buf, err := st.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("packetToBuffer failed: %v", err)
	}

	decoded, replyExpected, err := st.packetFromBuffer(buf)
	if err != nil {
		t.Fatalf("packetFromBuffer failed: %v", err)
	}
	defer decoded.Release()

	if replyExpected {
		t.Error("Reply should not be expected")
	}
	if decoded.TTL != 5 {
		t.Errorf("Expected TTL 5, got %d", decoded.TTL)
	}
	if decoded.MessageType != UserMsg {
		t.Errorf("Expected MsgType %d, got %d", UserMsg, decoded.MessageType)
	}
}

func TestSocketTransport_PacketRoundTripWithReplyFlag(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)

	packet := NewPacket()
	packet.TTL = 1
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("ping")
	packet.SetPayload(payload)

	buf, err := st.packetToBuffer(packet, true)
	if err != nil {
		t.Fatalf("packetToBuffer failed: %v", err)
	}

	decoded, replyExpected, err := st.packetFromBuffer(buf)
	if err != nil {
		t.Fatalf("packetFromBuffer failed: %v", err)
	}
	defer decoded.Release()

	if !replyExpected {
		t.Error("Reply should be expected")
	}
}

func TestSocketTransport_PacketRoundTripWithCompression(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Compressor = snappy.New()
	config.CompressMinSize = 1

	st := NewSocketTransport(config)

	packet := NewPacket()
	packet.TTL = 2
	packet.SetCodec(config.MsgCodec)
	// Large payload to ensure compression kicks in
	largePayload := make([]byte, 1000)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}
	packet.SetPayload(largePayload)

	buf, err := st.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("packetToBuffer with compression failed: %v", err)
	}

	decoded, _, err := st.packetFromBuffer(buf)
	if err != nil {
		t.Fatalf("packetFromBuffer with compression failed: %v", err)
	}
	defer decoded.Release()

	if len(decoded.Payload()) != len(largePayload) {
		t.Errorf("Payload length mismatch: %d vs %d", len(decoded.Payload()), len(largePayload))
	}
}

func TestSocketTransport_PacketRoundTripWithEncryption(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Cipher = aes.New()
	config.EncryptionKey = []byte("0123456789abcdef0123456789abcdef")

	st := NewSocketTransport(config)

	packet := NewPacket()
	packet.TTL = 3
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("encrypted data")
	packet.SetPayload(payload)

	buf, err := st.packetToBuffer(packet, false)
	if err != nil {
		t.Fatalf("packetToBuffer with encryption failed: %v", err)
	}

	decoded, _, err := st.packetFromBuffer(buf)
	if err != nil {
		t.Fatalf("packetFromBuffer with encryption failed: %v", err)
	}
	defer decoded.Release()

	if decoded.TTL != 3 {
		t.Errorf("Expected TTL 3, got %d", decoded.TTL)
	}
}

func TestSocketTransport_PacketRoundTripWithCompressionAndEncryption(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Compressor = snappy.New()
	config.CompressMinSize = 1
	config.Cipher = aes.New()
	config.EncryptionKey = []byte("0123456789abcdef0123456789abcdef")

	st := NewSocketTransport(config)

	packet := NewPacket()
	packet.TTL = 4
	packet.SetCodec(config.MsgCodec)
	largePayload := make([]byte, 500)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}
	packet.SetPayload(largePayload)

	buf, err := st.packetToBuffer(packet, true)
	if err != nil {
		t.Fatalf("packetToBuffer failed: %v", err)
	}

	decoded, replyExpected, err := st.packetFromBuffer(buf)
	if err != nil {
		t.Fatalf("packetFromBuffer failed: %v", err)
	}
	defer decoded.Release()

	if !replyExpected {
		t.Error("Reply should be expected")
	}
	if decoded.TTL != 4 {
		t.Errorf("Expected TTL 4, got %d", decoded.TTL)
	}
	if len(decoded.Payload()) != len(largePayload) {
		t.Errorf("Payload length mismatch")
	}
}

func TestSocketTransport_PacketFromBufferTooSmall(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)

	_, _, err := st.packetFromBuffer([]byte{0x01})
	if err == nil {
		t.Fatal("Expected error for too-small packet")
	}
}

func TestSocketTransport_PacketFromBufferTruncatedHeader(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()

	st := NewSocketTransport(config)

	// Set headerSize large but data short
	data := []byte{0xFF, 0x3F, 0x00} // headerSize=0x3FFF (max) but only 1 byte of data
	_, _, err := st.packetFromBuffer(data)
	if err == nil {
		t.Fatal("Expected error for truncated header")
	}
}

func TestSocketTransport_PacketFromBufferBadDecryption(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Cipher = aes.New()
	config.EncryptionKey = []byte("0123456789abcdef0123456789abcdef")

	st := NewSocketTransport(config)

	// Garbage encrypted data
	data := make([]byte, 100)
	data[0] = 0x10 // headerSize=16
	data[1] = 0x00
	_, _, err := st.packetFromBuffer(data)
	if err == nil {
		t.Fatal("Expected decryption error for garbage data")
	}
}

// ============================================================================
// SocketTransport: TCP send/receive integration
// ============================================================================

func TestSocketTransport_TCPSendReceiveIntegration(t *testing.T) {
	config1 := DefaultConfig()
	config1.MsgCodec = codec.NewJSONCodec()
	config1.BindAddr = ":0" // random port
	config1.AdvertiseAddr = "127.0.0.1:0"

	st1 := NewSocketTransport(config1)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := st1.Start(ctx, &wg)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}

	// Get the actual port
	tcpAddr := st1.tcpListener.Addr().(*net.TCPAddr)
	actualPort := tcpAddr.Port

	// Create second transport to connect to first
	config2 := DefaultConfig()
	config2.MsgCodec = codec.NewJSONCodec()

	st2 := NewSocketTransport(config2)

	// Create a node pointing to st1
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, fmt.Sprintf("127.0.0.1:%d", actualPort))

	// Prepare packet
	packet := NewPacket()
	packet.TTL = 3
	packet.MessageType = UserMsg
	packet.SetCodec(config2.MsgCodec)
	payload, _ := config2.MsgCodec.Marshal("tcp-test-data")
	packet.SetPayload(payload)

	// Send via TCP
	err = st2.Send(TransportReliable, node, packet)
	if err != nil {
		t.Fatalf("TCP Send failed: %v", err)
	}

	// Receive on st1
	select {
	case receivedPacket := <-st1.packetChannel:
		if receivedPacket.MessageType != UserMsg {
			t.Errorf("Expected MsgType %d, got %d", UserMsg, receivedPacket.MessageType)
		}
		receivedPacket.Release()
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for packet")
	}

	cancel()
	wg.Wait()
}

func TestSocketTransport_UDPSendReceiveIntegration(t *testing.T) {
	config1 := DefaultConfig()
	config1.MsgCodec = codec.NewJSONCodec()
	config1.BindAddr = ":0"
	config1.AdvertiseAddr = "127.0.0.1:0"

	st1 := NewSocketTransport(config1)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := st1.Start(ctx, &wg)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}

	// Get actual port
	udpAddr := st1.udpListener.LocalAddr().(*net.UDPAddr)
	actualPort := udpAddr.Port

	// Second transport
	config2 := DefaultConfig()
	config2.MsgCodec = codec.NewJSONCodec()
	config2.BindAddr = ":0"
	config2.AdvertiseAddr = "127.0.0.1:0"
	config2.UDPMaxPacketSize = 65535

	st2 := NewSocketTransport(config2)
	err = st2.Start(ctx, &wg)
	if err != nil {
		t.Fatalf("Failed to start second transport: %v", err)
	}

	// Create node pointing to st1
	nodeID := NodeID(uuid.New())
	node := newNode(nodeID, fmt.Sprintf("127.0.0.1:%d", actualPort))

	// Small packet for UDP
	packet := NewPacket()
	packet.TTL = 2
	packet.MessageType = UserMsg
	packet.SetCodec(config2.MsgCodec)
	payload, _ := config2.MsgCodec.Marshal("udp-test")
	packet.SetPayload(payload)

	err = st2.Send(TransportBestEffort, node, packet)
	if err != nil {
		t.Fatalf("UDP Send failed: %v", err)
	}

	select {
	case receivedPacket := <-st1.packetChannel:
		if receivedPacket.MessageType != UserMsg {
			t.Errorf("Expected MsgType %d, got %d", UserMsg, receivedPacket.MessageType)
		}
		receivedPacket.Release()
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for UDP packet")
	}

	cancel()
	wg.Wait()
}

func TestSocketTransport_SendWithReplyIntegration(t *testing.T) {
	config1 := DefaultConfig()
	config1.MsgCodec = codec.NewJSONCodec()
	config1.BindAddr = ":0"
	config1.AdvertiseAddr = "127.0.0.1:0"

	st1 := NewSocketTransport(config1)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := st1.Start(ctx, &wg)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}

	tcpAddr := st1.tcpListener.Addr().(*net.TCPAddr)
	actualPort := tcpAddr.Port

	// Handler goroutine: read packet and send reply
	go func() {
		packet, ok := <-st1.packetChannel
		if !ok || packet == nil {
			return
		}
		if packet.CanReply() {
			// Modify the received packet to be the reply and send it back
			// SendReply() sends the packet itself into the reply channel
			packet.SetCodec(config1.MsgCodec)
			replyPayload, _ := config1.MsgCodec.Marshal("pong")
			packet.SetPayload(replyPayload)
			packet.AddRef()
			packet.SendReply()
		}
		packet.Release()
	}()

	// Second transport sends with reply
	config2 := DefaultConfig()
	config2.MsgCodec = codec.NewJSONCodec()

	st2 := NewSocketTransport(config2)

	node := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", actualPort))

	packet := NewPacket()
	packet.TTL = 1
	packet.MessageType = UserMsg
	packet.SetCodec(config2.MsgCodec)
	payload, _ := config2.MsgCodec.Marshal("ping")
	packet.SetPayload(payload)

	reply, err := st2.SendWithReply(node, packet)
	if err != nil {
		t.Fatalf("SendWithReply failed: %v", err)
	}
	if reply == nil {
		t.Fatal("Expected reply, got nil")
	}
	defer reply.Release()

	cancel()
	wg.Wait()
}

func TestSocketTransport_ForceReliableTransport(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.ForceReliableTransport = true
	config.BindAddr = ":0"
	config.AdvertiseAddr = "127.0.0.1:0"

	st := NewSocketTransport(config)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := st.Start(ctx, &wg)
	if err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	// Should have TCP listener but no UDP
	if st.tcpListener == nil {
		t.Error("TCP listener should exist")
	}
	if st.udpListener != nil {
		t.Error("UDP listener should be nil with ForceReliableTransport")
	}

	cancel()
	wg.Wait()
}

func TestSocketTransport_SendForcesReliableForLargePacket(t *testing.T) {
	config1 := DefaultConfig()
	config1.MsgCodec = codec.NewJSONCodec()
	config1.BindAddr = ":0"
	config1.AdvertiseAddr = "127.0.0.1:0"
	config1.UDPMaxPacketSize = 10 // very small

	st1 := NewSocketTransport(config1)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := st1.Start(ctx, &wg)
	if err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	tcpAddr := st1.tcpListener.Addr().(*net.TCPAddr)
	actualPort := tcpAddr.Port

	config2 := DefaultConfig()
	config2.MsgCodec = codec.NewJSONCodec()
	config2.UDPMaxPacketSize = 10 // force large packets to TCP

	st2 := NewSocketTransport(config2)

	node := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", actualPort))

	packet := NewPacket()
	packet.TTL = 1
	packet.MessageType = UserMsg
	packet.SetCodec(config2.MsgCodec)
	payload, _ := config2.MsgCodec.Marshal("this is larger than 10 bytes")
	packet.SetPayload(payload)

	// BestEffort should fall back to TCP for large packet
	err = st2.Send(TransportBestEffort, node, packet)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	select {
	case receivedPacket := <-st1.packetChannel:
		receivedPacket.Release()
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for packet")
	}

	cancel()
	wg.Wait()
}

// ============================================================================
// SocketTransport: dial failures
// ============================================================================

func TestSocketTransport_DialPeerAllFail(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.TCPDialTimeout = 200 * time.Millisecond

	st := NewSocketTransport(config)

	// Use an unreachable address
	node := newNode(NodeID(uuid.New()), "127.0.0.1:1")

	_, err := st.dialPeer(node)
	if err == nil {
		t.Fatal("Expected dial error for unreachable address")
	}
}

func TestSocketTransport_SendUDPResolveFailed(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.BindAddr = ":0"
	config.AdvertiseAddr = "127.0.0.1:0"
	config.Resolver = &mockResolver{
		ipErr: fmt.Errorf("DNS failure"),
	}

	st := NewSocketTransport(config)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := st.Start(ctx, &wg); err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	node := newNode(NodeID(uuid.New()), "unreachable.host:7946")

	packet := NewPacket()
	packet.TTL = 1
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("test")
	packet.SetPayload(payload)

	buf, _ := st.packetToBuffer(packet, false)
	err := st.sendUDP(node, buf)
	if err == nil {
		t.Fatal("Expected error for unresolvable host UDP send")
	}

	cancel()
	wg.Wait()
}

// ============================================================================
// HTTPTransport
// ============================================================================

func TestHTTPTransport_EnsureNodeAddressResolvedEmptyAddr(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	ht := NewHTTPTransport(config)

	node := newNode(NodeID(uuid.New()), "")
	err := ht.ensureNodeAddressResolved(node)
	if err == nil {
		t.Fatal("Expected error for empty advertise address")
	}
}

func TestHTTPTransport_EnsureNodeAddressResolvedHTTPS(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.BindAddr = "/gossip"
	ht := NewHTTPTransport(config)

	node := newNode(NodeID(uuid.New()), "https://myhost:8443")
	err := ht.ensureNodeAddressResolved(node)
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}

	addr := node.GetAddress()
	if addr.URL == "" {
		t.Error("URL should be set")
	}
}

func TestHTTPTransport_EnsureNodeAddressResolvedSRV(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.BindAddr = "/gossip"
	config.Resolver = &mockResolver{
		srvAddrs: []*net.TCPAddr{
			{IP: net.ParseIP("10.0.0.1"), Port: 443},
		},
	}
	ht := NewHTTPTransport(config)

	node := newNode(NodeID(uuid.New()), "srv+https://myservice.consul")
	err := ht.ensureNodeAddressResolved(node)
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}

	addr := node.GetAddress()
	if addr.URL == "" {
		t.Error("URL should be resolved")
	}
}

func TestHTTPTransport_EnsureNodeAddressResolvedSRVError(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Resolver = &mockResolver{
		srvErr: fmt.Errorf("SRV failed"),
	}
	ht := NewHTTPTransport(config)

	node := newNode(NodeID(uuid.New()), "srv+https://myservice.consul")
	err := ht.ensureNodeAddressResolved(node)
	if err == nil {
		t.Fatal("Expected SRV lookup error")
	}
}

func TestHTTPTransport_EnsureNodeAddressResolvedSRVNoRecords(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Resolver = &mockResolver{
		srvAddrs: []*net.TCPAddr{},
	}
	ht := NewHTTPTransport(config)

	node := newNode(NodeID(uuid.New()), "srv+https://myservice.consul")
	err := ht.ensureNodeAddressResolved(node)
	if err == nil {
		t.Fatal("Expected error for empty SRV results")
	}
}

func TestHTTPTransport_EnsureNodeAddressResolvedAlreadySet(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	ht := NewHTTPTransport(config)

	node := newNode(NodeID(uuid.New()), "https://myhost:8443")
	node.SetAddress(Address{URL: "https://myhost:8443/gossip"})

	err := ht.ensureNodeAddressResolved(node)
	if err != nil {
		t.Fatalf("Should not error for already-resolved node: %v", err)
	}
}

func TestHTTPTransport_EnsureNodeAddressResolvedNoScheme(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.BindAddr = ""
	ht := NewHTTPTransport(config)

	node := newNode(NodeID(uuid.New()), "myhost:8443")
	err := ht.ensureNodeAddressResolved(node)
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}

	addr := node.GetAddress()
	if addr.URL == "" {
		t.Error("URL should be set")
	}
}

func TestHTTPTransport_EnsureNodeAddressResolvedBadURL(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	ht := NewHTTPTransport(config)

	node := newNode(NodeID(uuid.New()), "srv+://[bad url\\")
	err := ht.ensureNodeAddressResolved(node)
	if err == nil {
		t.Fatal("Expected error for bad URL")
	}
}

func TestHTTPTransport_EnsureNodeAddressResolvedBadNonSRVURL(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	ht := NewHTTPTransport(config)

	node := newNode(NodeID(uuid.New()), "://[bad url\\")
	err := ht.ensureNodeAddressResolved(node)
	if err == nil {
		t.Fatal("Expected error for bad non-SRV URL")
	}
}

func TestHTTPTransport_PacketFromBufferTooSmall(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	ht := NewHTTPTransport(config)

	_, err := ht.packetFromBuffer([]byte{0x01})
	if err == nil {
		t.Fatal("Expected error for too-small packet")
	}
}

func TestHTTPTransport_PacketFromBufferTruncatedHeader(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	ht := NewHTTPTransport(config)

	data := []byte{0xFF, 0x3F, 0x00}
	_, err := ht.packetFromBuffer(data)
	if err == nil {
		t.Fatal("Expected error for truncated header")
	}
}

func TestHTTPTransport_PacketRoundTrip(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	ht := NewHTTPTransport(config)

	packet := NewPacket()
	packet.TTL = 5
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)
	payload, _ := config.MsgCodec.Marshal("http-test")
	packet.SetPayload(payload)

	buf, err := ht.packetToBuffer(packet, true)
	if err != nil {
		t.Fatalf("packetToBuffer failed: %v", err)
	}

	decoded, err := ht.packetFromBuffer(buf)
	if err != nil {
		t.Fatalf("packetFromBuffer failed: %v", err)
	}
	defer decoded.Release()

	if decoded.TTL != 5 {
		t.Errorf("Expected TTL 5, got %d", decoded.TTL)
	}
}

// ============================================================================
// DefaultResolver
// ============================================================================

func TestDefaultResolver_LookupIPLocalhost(t *testing.T) {
	r := NewDefaultResolver()
	ips, err := r.LookupIP("localhost")
	if err != nil {
		t.Fatalf("LookupIP localhost failed: %v", err)
	}
	if len(ips) == 0 {
		t.Fatal("Expected at least 1 IP for localhost")
	}
}

func TestDefaultResolver_LookupIPUnknown(t *testing.T) {
	r := NewDefaultResolver()
	_, err := r.LookupIP("thishostdoesnotexist.invalid")
	if err == nil {
		t.Fatal("Expected error for unknown host")
	}
}

func TestDefaultResolver_LookupSRVEmpty(t *testing.T) {
	r := NewDefaultResolver()
	_, err := r.LookupSRV("")
	if err == nil {
		t.Fatal("Expected error for empty service")
	}
}

func TestDefaultResolver_LookupSRVUnknown(t *testing.T) {
	r := NewDefaultResolver()
	_, err := r.LookupSRV("doesnotexist.invalid.")
	if err == nil {
		t.Fatal("Expected error for unknown SRV")
	}
}

// ============================================================================
// Address
// ============================================================================

func TestAddressStringFormats(t *testing.T) {
	tests := []struct {
		name     string
		addr     Address
		expected string
	}{
		{
			name:     "IP and port",
			addr:     Address{IP: net.ParseIP("10.0.0.1"), Port: 8080},
			expected: "10.0.0.1:8080",
		},
		{
			name:     "URL only",
			addr:     Address{URL: "https://example.com/gossip"},
			expected: "https://example.com/gossip",
		},
		{
			name:     "IP, port, and URL",
			addr:     Address{IP: net.ParseIP("10.0.0.1"), Port: 8080, URL: "https://example.com"},
			expected: "10.0.0.1:8080, https://example.com",
		},
		{
			name:     "empty",
			addr:     Address{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.addr.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestAddressIsEmpty(t *testing.T) {
	a := Address{}
	if !a.IsEmpty() {
		t.Error("Empty address should report IsEmpty")
	}

	a.IP = net.ParseIP("127.0.0.1")
	if a.IsEmpty() {
		t.Error("Address with IP should not be empty")
	}

	a2 := Address{Port: 8080}
	if a2.IsEmpty() {
		t.Error("Address with Port should not be empty")
	}

	a3 := Address{URL: "http://test"}
	if a3.IsEmpty() {
		t.Error("Address with URL should not be empty")
	}
}

func TestAddressClear(t *testing.T) {
	a := Address{
		IP:   net.ParseIP("10.0.0.1"),
		Port: 8080,
		URL:  "https://test",
	}
	a.Clear()

	if !a.IsEmpty() {
		t.Error("Address should be empty after Clear()")
	}
}

// ============================================================================
// Transport type name
// ============================================================================

func TestSocketTransportName(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	st := NewSocketTransport(config)
	if st.Name() != "socket" {
		t.Errorf("Expected 'socket', got %q", st.Name())
	}
}

func TestHTTPTransportName(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	ht := NewHTTPTransport(config)
	if ht.Name() != "http" {
		t.Errorf("Expected 'http', got %q", ht.Name())
	}
}
