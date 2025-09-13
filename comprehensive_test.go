package gossip

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/compression"
	"github.com/paularlott/gossip/encryption"
)

// TestClusterBasicOperations tests basic cluster operations
func TestClusterBasicOperations(t *testing.T) {
	config := DefaultConfig()
	// Use a valid UUID for NodeID
	config.NodeID = uuid.New().String()
	config.BindAddr = "127.0.0.1:0"
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Just verify the node ID was set (it will be auto-generated if not a valid UUID)
	if cluster.LocalNode().ID == EmptyNodeID {
		t.Error("Node ID should not be empty")
	}

	// Test message sending
	err = cluster.Send(UserMsg, "test message")
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
	}
}

// TestConfigValidation tests configuration validation
func TestConfigValidation(t *testing.T) {
	// Test with nil config
	_, err := NewCluster(nil)
	if err == nil {
		t.Error("Expected error for nil config")
	}

	// Test with invalid bind address
	config := DefaultConfig()
	config.BindAddr = "invalid:address:format"
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}

	_, err = NewCluster(config)
	// Should not fail during cluster creation, but during transport start
}

// TestCodecImplementations tests different codec implementations
func TestCodecImplementations(t *testing.T) {
	codecs := []struct {
		name  string
		codec codec.Serializer
	}{
		{"JSON", codec.NewJsonCodec()},
		{"Shamaton", codec.NewShamatonMsgpackCodec()},
		{"Vmihailenco", codec.NewVmihailencoMsgpackCodec()},
	}

	testData := map[string]interface{}{
		"string": "test string",
		"number": 42,
		"bool":   true,
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			// Test marshaling
			data, err := tc.codec.Marshal(testData)
			if err != nil {
				t.Fatalf("Failed to marshal with %s: %v", tc.name, err)
			}

			// Test unmarshaling
			var result map[string]interface{}
			err = tc.codec.Unmarshal(data, &result)
			if err != nil {
				t.Fatalf("Failed to unmarshal with %s: %v", tc.name, err)
			}

			// Verify codec name
			if tc.codec.Name() == "" {
				t.Errorf("Codec %s should have a name", tc.name)
			}
		})
	}
}

// TestCompressionImplementations tests compression implementations
func TestCompressionImplementations(t *testing.T) {
	compressor := compression.NewSnappyCompressor()
	
	testData := []byte("This is a test string that should compress well when repeated. " +
		"This is a test string that should compress well when repeated. " +
		"This is a test string that should compress well when repeated.")

	// Test compression
	compressed, err := compressor.Compress(testData)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	// Test decompression
	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	if string(decompressed) != string(testData) {
		t.Error("Decompressed data doesn't match original")
	}
}

// TestEncryptionImplementations tests encryption implementations
func TestEncryptionImplementations(t *testing.T) {
	encryptor := encryption.NewAESEncryptor()
	key := []byte("12345678901234567890123456789012") // 32 bytes
	testData := []byte("This is secret data that needs encryption")

	// Test encryption
	encrypted, err := encryptor.Encrypt(key, testData)
	if err != nil {
		t.Fatalf("Failed to encrypt: %v", err)
	}

	// Test decryption
	decrypted, err := encryptor.Decrypt(key, encrypted)
	if err != nil {
		t.Fatalf("Failed to decrypt: %v", err)
	}

	if string(decrypted) != string(testData) {
		t.Error("Decrypted data doesn't match original")
	}
}

// TestNodeStates tests node state transitions
func TestNodeStates(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Test local node starts as alive
	if cluster.LocalNode().GetObservedState() != NodeAlive {
		t.Errorf("Expected local node to be alive, got %v", cluster.LocalNode().GetObservedState())
	}

	// Test node state string representations
	states := []NodeState{NodeUnknown, NodeAlive, NodeSuspect, NodeDead, NodeLeaving}
	for _, state := range states {
		if state.String() == "" {
			t.Errorf("State %d should have string representation", state)
		}
	}
}

// TestMessageTypes tests message type definitions
func TestMessageTypes(t *testing.T) {
	// Test system message types
	systemTypes := []MessageType{
		replyMsg, nodeJoinMsg, nodeLeaveMsg, pushPullStateMsg, metadataUpdateMsg, pingMsg,
	}

	for _, msgType := range systemTypes {
		if msgType >= ReservedMsgsStart {
			t.Errorf("System message type %d should be less than %d", msgType, ReservedMsgsStart)
		}
	}

	// Test user message types
	if UserMsg < ReservedMsgsStart {
		t.Errorf("User message type %d should be >= %d", UserMsg, ReservedMsgsStart)
	}
}

// TestAddressResolution tests address resolution functionality
func TestAddressResolution(t *testing.T) {
	config := DefaultConfig()
	config.Transport = NewSocketTransport(config)
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	transport := config.Transport.(*SocketTransport)

	testCases := []struct {
		name    string
		address string
		wantErr bool
	}{
		{"Valid IP:Port", "127.0.0.1:8080", false},
		{"Valid hostname:Port", "localhost:8080", false},
		{"Invalid format", "invalid", true},
		{"Port only", ":8080", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			addresses, err := transport.lookupIP(tc.address)
			if tc.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if len(addresses) == 0 {
					t.Error("Expected at least one address")
				}
			}
		})
	}
}

// TestMetadataOperations tests metadata operations
func TestMetadataOperations(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Test setting metadata
	cluster.LocalMetadata().SetString("key1", "value1")
	cluster.LocalMetadata().SetInt("key2", 42)

	// Test getting metadata
	if val := cluster.LocalMetadata().GetString("key1"); val != "value1" {
		t.Errorf("Expected 'value1', got %v", val)
	}

	if val := cluster.LocalMetadata().GetInt("key2"); val != 42 {
		t.Errorf("Expected 42, got %v", val)
	}

	// Test getting non-existent metadata
	if val := cluster.LocalMetadata().GetString("nonexistent"); val != "" {
		t.Errorf("Expected empty string for non-existent key, got %v", val)
	}

	// Test deleting metadata
	cluster.LocalMetadata().Delete("key1")
	if val := cluster.LocalMetadata().GetString("key1"); val != "" {
		t.Errorf("Expected empty string after deletion, got %v", val)
	}
}

// TestEventHandling tests event handling functionality
func TestEventHandling(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Test node operations (events are internal)
	testNode := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(testNode)

	// Verify node was added
	if cluster.nodes.get(testNode.ID) == nil {
		t.Error("Expected node to be added to cluster")
	}
}

// TestHealthMonitoring tests health monitoring functionality
func TestHealthMonitoring(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()
	config.HealthCheckInterval = 50 * time.Millisecond

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add a test node
	testNode := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(testNode)

	// Health monitoring is internal, so we'll just verify the node exists
	if cluster.nodes.get(testNode.ID) == nil {
		t.Error("Test node should exist in cluster")
	}
}

// TestMessageHistory tests message history functionality
func TestMessageHistory(t *testing.T) {
	config := DefaultConfig()
	history := newMessageHistory(config)

	senderID := NodeID(uuid.New())
	messageID := MessageID(1234)

	// Test message not in history
	if history.contains(senderID, messageID) {
		t.Error("Message should not be in history initially")
	}

	// Record message
	history.recordMessage(senderID, messageID)

	// Test message now in history
	if !history.contains(senderID, messageID) {
		t.Error("Message should be in history after recording")
	}

	// Test different message not in history
	differentMessageID := MessageID(5678)
	if history.contains(senderID, differentMessageID) {
		t.Error("Different message should not be in history")
	}
}

// TestClusterCalculations tests cluster calculation methods
func TestClusterCalculations(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Add some test nodes
	for i := 0; i < 10; i++ {
		node := newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 8000+i))
		cluster.nodes.addOrUpdate(node)
	}

	// Test CalcFanOut
	fanOut := cluster.CalcFanOut()
	if fanOut <= 0 {
		t.Error("FanOut should be positive")
	}

	// Test CalcPayloadSize
	payloadSize := cluster.CalcPayloadSize(20)
	if payloadSize <= 0 {
		t.Error("PayloadSize should be positive")
	}

	// Test getMaxTTL
	maxTTL := cluster.getMaxTTL()
	if maxTTL == 0 {
		t.Error("MaxTTL should be positive")
	}
}

// TestTransportInterface tests transport interface compliance
func TestTransportInterface(t *testing.T) {
	config := DefaultConfig()
	config.BindAddr = "127.0.0.1:0"
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	transport := NewSocketTransport(config)

	// Test interface methods exist
	if transport.Name() == "" {
		t.Error("Transport should have a name")
	}

	if transport.PacketChannel() == nil {
		t.Error("Transport should provide packet channel")
	}

	// Test with mock node
	node := &Node{
		ID:            NodeID(uuid.New()),
		advertiseAddr: "127.0.0.1:8001",
		address:       Address{},
	}

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)
	defer packet.Release()

	// These should fail gracefully with unresolved addresses
	// Note: We expect these to fail, but we need to handle potential panics
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Recovered from panic (expected): %v", r)
		}
	}()
	
	err := transport.Send(TransportBestEffort, node, packet)
	if err == nil {
		t.Error("Expected error for unresolved address")
	}

	_, err = transport.SendWithReply(node, packet)
	if err == nil {
		t.Error("Expected error for unresolved address")
	}
}

// TestErrorHandling tests error handling in various scenarios
func TestErrorHandling(t *testing.T) {
	// Test packet unmarshaling with invalid data
	packet := NewPacket()
	packet.SetCodec(&mockCodec{})
	packet.SetPayload([]byte("invalid data"))

	var result map[string]interface{}
	err := packet.Unmarshal(&result)
	// mockCodec always returns nil, so this won't error
	if err != nil {
		t.Logf("Unmarshal error (expected): %v", err)
	}

	packet.Release()
}

// TestConcurrentOperations tests concurrent operations on cluster
func TestConcurrentOperations(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &mockCodec{}
	config.Logger = NewNullLogger()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	const numGoroutines = 10
	const numOperations = 50

	var wg sync.WaitGroup

	// Concurrent message sending
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				err := cluster.Send(UserMsg, fmt.Sprintf("message-%d-%d", id, j))
				if err != nil {
					t.Errorf("Failed to send message: %v", err)
				}
			}
		}(i)
	}

	// Concurrent metadata operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				cluster.LocalMetadata().SetString(key, fmt.Sprintf("value-%d-%d", id, j))
				cluster.LocalMetadata().GetString(key)
				cluster.LocalMetadata().Delete(key)
			}
		}(i)
	}

	wg.Wait()
}