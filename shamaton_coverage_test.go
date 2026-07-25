package gossip

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/compression/snappy"
	"github.com/paularlott/gossip/encryption/aes"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// HTTP Transport with Shamaton
// ============================================================================

func TestShamatonHTTP_PacketSerialization(t *testing.T) {
	config := &Config{
		MsgCodec:         shamaton.New(),
		TCPMaxPacketSize: 65535,
	}
	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("shamaton-http-payload"))
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, false)
	require.NoError(t, err)

	decoded, err := transport.packetFromBuffer(data)
	require.NoError(t, err)

	assert.Equal(t, packet.MessageType, decoded.MessageType)
	assert.Equal(t, packet.SenderID, decoded.SenderID)
	assert.Equal(t, packet.MessageID, decoded.MessageID)
	assert.Equal(t, packet.TTL, decoded.TTL)
	assert.Equal(t, "shamaton-http-payload", string(decoded.Payload()))

	packet.Release()
	decoded.Release()
}

func TestShamatonHTTP_PacketWithOptionalFields(t *testing.T) {
	config := &Config{
		MsgCodec:         shamaton.New(),
		TCPMaxPacketSize: 65535,
	}
	transport := NewHTTPTransport(config)

	tag := "route-tag"
	target := NodeID(uuid.New())

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.TargetNodeID = &target
	packet.Tag = &tag
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 3
	packet.SetPayload([]byte("with-optional"))
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, false)
	require.NoError(t, err)

	decoded, err := transport.packetFromBuffer(data)
	require.NoError(t, err)

	require.NotNil(t, decoded.TargetNodeID)
	assert.Equal(t, target, *decoded.TargetNodeID)
	require.NotNil(t, decoded.Tag)
	assert.Equal(t, tag, *decoded.Tag)

	packet.Release()
	decoded.Release()
}

func TestShamatonHTTP_PacketWithNilOptionals(t *testing.T) {
	config := &Config{
		MsgCodec:         shamaton.New(),
		TCPMaxPacketSize: 65535,
	}
	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.TargetNodeID = nil
	packet.Tag = nil
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("nil-optionals"))
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, false)
	require.NoError(t, err)

	decoded, err := transport.packetFromBuffer(data)
	require.NoError(t, err)

	assert.Nil(t, decoded.TargetNodeID)
	assert.Nil(t, decoded.Tag)

	packet.Release()
	decoded.Release()
}

func TestShamatonHTTP_ReplyExpectedFlag(t *testing.T) {
	config := &Config{
		MsgCodec:         shamaton.New(),
		TCPMaxPacketSize: 65535,
	}
	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("flag-test"))
	packet.SetCodec(config.MsgCodec)

	dataReply, err := transport.packetToBuffer(packet, true)
	require.NoError(t, err)
	flags := uint16(dataReply[0]) | uint16(dataReply[1])<<8
	assert.NotZero(t, flags&replyExpectedFlag, "reply flag should be set")

	dataNoReply, err := transport.packetToBuffer(packet, false)
	require.NoError(t, err)
	flags2 := uint16(dataNoReply[0]) | uint16(dataNoReply[1])<<8
	assert.Zero(t, flags2&replyExpectedFlag, "reply flag should be unset")

	packet.Release()
}

func TestShamatonHTTP_HandleGossipRequest(t *testing.T) {
	config := &Config{
		IncomingPacketQueueDepth: 10,
		MsgCodec:                 shamaton.New(),
		Logger:                   logger.NewNullLogger(),
		TCPMaxPacketSize:         65535,
	}
	transport := NewHTTPTransport(config)

	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("http-request-body"))
	packet.SetCodec(config.MsgCodec)

	data, err := transport.packetToBuffer(packet, false)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/gossip", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	transport.HandleGossipRequest(w, req)
	assert.Equal(t, http.StatusNoContent, w.Code)

	select {
	case received := <-transport.PacketChannel():
		assert.Equal(t, "http-request-body", string(received.Payload()))
		received.Release()
	case <-time.After(time.Second):
		t.Fatal("packet not received on channel")
	}

	packet.Release()
}

func TestShamatonHTTP_SendWithReply(t *testing.T) {
	config := &Config{
		MsgCodec:         shamaton.New(),
		Logger:           logger.NewNullLogger(),
		TCPMaxPacketSize: 65535,
	}
	transport := NewHTTPTransport(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		flags := uint16(body[0]) | uint16(body[1])<<8
		require.NotZero(t, flags&replyExpectedFlag, "reply flag should be set in request")

		replyPacket := NewPacket()
		replyPacket.MessageType = replyMsg
		replyPacket.SenderID = NodeID(uuid.New())
		replyPacket.MessageID = MessageID(hlc.Now())
		replyPacket.TTL = 5
		replyPacket.SetPayload([]byte("shamaton-http-reply"))
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
	packet.SetPayload([]byte("shamaton-http-request"))
	packet.SetCodec(config.MsgCodec)

	reply, err := transport.SendWithReply(node, packet)
	require.NoError(t, err)
	require.NotNil(t, reply)
	assert.Equal(t, "shamaton-http-reply", string(reply.Payload()))

	packet.Release()
	reply.Release()
}

func TestShamatonHTTP_TruncatedPacket(t *testing.T) {
	config := &Config{
		MsgCodec: shamaton.New(),
		Logger:   logger.NewNullLogger(),
	}
	transport := NewHTTPTransport(config)

	_, err := transport.packetFromBuffer([]byte{0x00})
	require.Error(t, err)

	_, err = transport.packetFromBuffer([]byte{0xFF, 0xFF, 0x00})
	require.Error(t, err)

	// Valid size header but truncated msgpack body
	_, err = transport.packetFromBuffer([]byte{0x10, 0x00, 0x81})
	require.Error(t, err)
}

// ============================================================================
// Socket Transport: Shamaton with Encryption + Compression (edge cases)
// ============================================================================

func TestShamatonSocket_PacketFromBufferTooSmall(t *testing.T) {
	config := DefaultConfig()
	config.MsgCodec = shamaton.New()
	st := NewSocketTransport(config)

	_, _, err := st.packetFromBuffer([]byte{0x00})
	require.Error(t, err)

	_, _, err = st.packetFromBuffer([]byte{0x04, 0x00, 0x81})
	require.Error(t, err)
}

func TestShamatonSocket_EncryptionRoundTrip(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	config := DefaultConfig()
	config.MsgCodec = shamaton.New()
	config.Cipher = aes.New()
	config.EncryptionKey = key
	config.Compressor = snappy.New()
	config.CompressMinSize = 0

	st := NewSocketTransport(config)

	tag := "encrypted-tag"
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.Tag = &tag
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 7
	packet.SetPayload([]byte("payload-that-should-be-compressed-and-encrypted"))
	packet.SetCodec(config.MsgCodec)

	data, err := st.packetToBuffer(packet, true)
	require.NoError(t, err)

	decoded, _, err := st.packetFromBuffer(data)
	require.NoError(t, err)

	assert.Equal(t, packet.MessageType, decoded.MessageType)
	assert.Equal(t, packet.SenderID, decoded.SenderID)
	require.NotNil(t, decoded.Tag)
	assert.Equal(t, tag, *decoded.Tag)
	assert.Equal(t, "payload-that-should-be-compressed-and-encrypted", string(decoded.Payload()))

	packet.Release()
	decoded.Release()
}

// ============================================================================
// Cluster Handlers with Shamaton (handler-level unit tests)
// ============================================================================

func mkShamatonTestCluster(t *testing.T) *Cluster {
	t.Helper()
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = shamaton.New()
	config.Logger = logger.NewNullLogger()
	c, err := NewCluster(config)
	require.NoError(t, err)
	return c
}

func mkPacket(t *testing.T, c *Cluster, msgType MessageType, sender NodeID, payload interface{}) *Packet {
	t.Helper()
	packet := NewPacket()
	packet.MessageType = msgType
	packet.SenderID = sender
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetCodec(c.config.MsgCodec)
	data, err := c.config.MsgCodec.Marshal(payload)
	require.NoError(t, err)
	packet.SetPayload(data)
	return packet
}

func TestShamatonHandler_Join(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	joinerID := NodeID(uuid.New())
	joinMsg := &joinMessage{
		ID:                 joinerID,
		AdvertiseAddr:      "127.0.0.1:8001",
		State:              NodeAlive,
		MetadataTimestamp:  hlc.Now(),
		Metadata:           map[string]interface{}{"region": "us-east", "priority": int64(5)},
		ProtocolVersion:    PROTOCOL_VERSION,
		ApplicationVersion: cluster.config.ApplicationVersion,
		Tags:               []string{"tag1", "tag2"},
	}

	packet := mkPacket(t, cluster, nodeJoinMsg, joinerID, joinMsg)
	defer packet.Release()

	replyData, err := cluster.handleJoin(nil, packet)
	require.NoError(t, err)

	reply, ok := replyData.(*joinReplyMessage)
	require.True(t, ok)
	assert.True(t, reply.Accepted, "join should be accepted")

	// The joining node should now be in the cluster's node list
	joined := cluster.GetNode(joinerID)
	require.NotNil(t, joined)
	assert.Equal(t, "127.0.0.1:8001", joined.AdvertisedAddr())
	assert.Contains(t, joined.GetTags(), "tag1")
	assert.Contains(t, joined.GetTags(), "tag2")
}

func TestShamatonHandler_JoinVersionMismatch(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	joinMsg := &joinMessage{
		ID:                 NodeID(uuid.New()),
		AdvertiseAddr:      "127.0.0.1:8001",
		State:              NodeAlive,
		MetadataTimestamp:  hlc.Now(),
		Metadata:           map[string]interface{}{},
		ProtocolVersion:    PROTOCOL_VERSION + 1,
		ApplicationVersion: "99.0.0",
	}

	packet := mkPacket(t, cluster, nodeJoinMsg, joinMsg.ID, joinMsg)
	defer packet.Release()

	replyData, err := cluster.handleJoin(nil, packet)
	require.NoError(t, err)

	reply, ok := replyData.(*joinReplyMessage)
	require.True(t, ok)
	assert.False(t, reply.Accepted, "join should be rejected for version mismatch")
}

func TestShamatonHandler_JoinSelf(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	joinMsg := &joinMessage{
		ID:                 cluster.localNode.ID,
		AdvertiseAddr:      "127.0.0.1:8001",
		State:              NodeAlive,
		MetadataTimestamp:  hlc.Now(),
		Metadata:           map[string]interface{}{},
		ProtocolVersion:    PROTOCOL_VERSION,
		ApplicationVersion: cluster.config.ApplicationVersion,
	}

	packet := mkPacket(t, cluster, nodeJoinMsg, cluster.localNode.ID, joinMsg)
	defer packet.Release()

	replyData, err := cluster.handleJoin(nil, packet)
	require.NoError(t, err)

	reply, ok := replyData.(*joinReplyMessage)
	require.True(t, ok)
	assert.False(t, reply.Accepted, "join should be rejected for self-join")
	assert.Contains(t, reply.RejectReason, "self")
}

func TestShamatonHandler_PushPullState(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	cluster.nodes.addOrUpdate(node1)
	cluster.nodes.addOrUpdate(node2)

	peerStates := []exchangeNodeState{
		{ID: node1.ID, AdvertiseAddr: "127.0.0.1:8001", State: NodeAlive, StateTimestamp: hlc.Now()},
	}

	packet := mkPacket(t, cluster, pushPullStateMsg, node1.ID, &peerStates)
	defer packet.Release()

	replyData, err := cluster.handlePushPullState(node1, packet)
	require.NoError(t, err)

	replyStates, ok := replyData.(*[]exchangeNodeState)
	require.True(t, ok)
	assert.NotEmpty(t, *replyStates, "should return local state")
}

func TestShamatonHandler_MetadataUpdate(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	time.Sleep(2 * time.Millisecond)
	newTS := hlc.Now()

	metaMsg := metadataUpdateMessage{
		MetadataTimestamp: newTS,
		Metadata: map[string]interface{}{
			"zone":     "us-west-2",
			"priority": int64(10),
			"enabled":  true,
		},
		NodeState: NodeAlive,
	}

	packet := mkPacket(t, cluster, metadataUpdateMsg, node.ID, &metaMsg)
	defer packet.Release()

	err := cluster.handleMetadataUpdate(node, packet)
	require.NoError(t, err)

	assert.Equal(t, "us-west-2", node.Metadata.GetString("zone"))
	assert.True(t, node.Metadata.GetBool("enabled"))
}

func TestShamatonHandler_MetadataStaleTimestamp(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)
	currentTS := node.metadata.GetTimestamp()

	metaMsg := metadataUpdateMessage{
		MetadataTimestamp: currentTS, // same or older — should be rejected
		Metadata:          map[string]interface{}{"key": "rejected"},
		NodeState:         NodeAlive,
	}

	packet := mkPacket(t, cluster, metadataUpdateMsg, node.ID, &metaMsg)
	defer packet.Release()

	err := cluster.handleMetadataUpdate(node, packet)
	require.NoError(t, err)

	// The stale update should NOT have overwritten the metadata
	_, exists := node.metadata.get("key")
	assert.False(t, exists, "stale metadata should be rejected")
}

func TestShamatonHandler_Ping(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	pingData := &pingMessage{
		SenderID:      node.ID,
		AdvertiseAddr: "127.0.0.1:8001",
	}

	packet := mkPacket(t, cluster, pingMsg, node.ID, pingData)
	defer packet.Release()

	replyData, err := cluster.handlePing(node, packet)
	require.NoError(t, err)

	pong, ok := replyData.(*pongMessage)
	require.True(t, ok)
	assert.Equal(t, cluster.localNode.ID, pong.NodeID)
	assert.Equal(t, cluster.localNode.AdvertisedAddr(), pong.AdvertiseAddr)
}

func TestShamatonHandler_NodeLeave(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	packet := NewPacket()
	packet.MessageType = nodeLeaveMsg
	packet.SenderID = node.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	defer packet.Release()

	err := cluster.handleNodeLeave(node, packet)
	require.NoError(t, err)

	assert.Equal(t, NodeLeaving, node.GetObservedState())
}

// ============================================================================
// Handler Unhappy Paths with Shamaton
// ============================================================================

func TestShamatonHandler_JoinMalformedPayload(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	packet := NewPacket()
	packet.MessageType = nodeJoinMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetCodec(cluster.config.MsgCodec)
	packet.SetPayload([]byte{0x91, 0x00, 0xFF, 0xFE}) // truncated/invalid msgpack
	defer packet.Release()

	_, err := cluster.handleJoin(nil, packet)
	require.Error(t, err)
}

func TestShamatonHandler_PingMalformedPayload(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	packet := NewPacket()
	packet.MessageType = pingMsg
	packet.SenderID = node.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetCodec(cluster.config.MsgCodec)
	packet.SetPayload([]byte("not-msgpack-at-all"))
	defer packet.Release()

	_, err := cluster.handlePing(node, packet)
	require.Error(t, err)
}

func TestShamatonHandler_PushPullMalformedPayload(t *testing.T) {
	cluster := mkShamatonTestCluster(t)

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	packet := NewPacket()
	packet.MessageType = pushPullStateMsg
	packet.SenderID = node.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetCodec(cluster.config.MsgCodec)
	packet.SetPayload([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	defer packet.Release()

	_, err := cluster.handlePushPullState(node, packet)
	require.Error(t, err)
}

// ============================================================================
// Serialization Edge Cases with Shamaton
// ============================================================================

func TestShamatonSerialization_NilVsEmptyTags(t *testing.T) {
	sr := shamaton.New()

	// nil tags
	j1 := joinMessage{
		ID: NodeID(uuid.New()), AdvertiseAddr: "addr1",
		MetadataTimestamp: hlc.Now(),
		ProtocolVersion:   1, ApplicationVersion: "1.0",
	}
	data1, err := sr.Marshal(&j1)
	require.NoError(t, err)

	// empty (non-nil) tags
	j2 := joinMessage{
		ID: NodeID(uuid.New()), AdvertiseAddr: "addr2",
		Tags:              []string{},
		MetadataTimestamp: hlc.Now(),
		ProtocolVersion:   1, ApplicationVersion: "1.0",
	}
	data2, err := sr.Marshal(&j2)
	require.NoError(t, err)

	// Both should decode fine — with omitempty, nil and empty []string are suppressed
	// on the wire. On decode, shamaton produces nil for absent fields.
	var d1, d2 joinMessage
	require.NoError(t, sr.Unmarshal(data1, &d1))
	require.NoError(t, sr.Unmarshal(data2, &d2))
	assert.True(t, len(d1.Tags) == 0)
	assert.True(t, len(d2.Tags) == 0)
}

func TestShamatonSerialization_EmptyVsNilMetadata(t *testing.T) {
	sr := shamaton.New()

	// nil metadata
	j1 := joinMessage{
		ID: NodeID(uuid.New()), AdvertiseAddr: "addr1",
		MetadataTimestamp: hlc.Now(),
		ProtocolVersion:   1, ApplicationVersion: "1.0",
	}
	data1, _ := sr.Marshal(&j1)

	// empty metadata map
	j2 := joinMessage{
		ID: NodeID(uuid.New()), AdvertiseAddr: "addr2",
		Metadata:          map[string]interface{}{},
		MetadataTimestamp: hlc.Now(),
		ProtocolVersion:   1, ApplicationVersion: "1.0",
	}
	data2, _ := sr.Marshal(&j2)

	var d1, d2 joinMessage
	require.NoError(t, sr.Unmarshal(data1, &d1))
	require.NoError(t, sr.Unmarshal(data2, &d2))
}

func TestShamatonSerialization_ZeroNodeID(t *testing.T) {
	sr := shamaton.New()

	p := Packet{
		MessageType: UserMsg,
		SenderID:    EmptyNodeID,
		MessageID:   MessageID(hlc.Now()),
		TTL:         5,
	}
	data, err := sr.Marshal(&p)
	require.NoError(t, err)

	var decoded Packet
	require.NoError(t, sr.Unmarshal(data, &decoded))
	assert.Equal(t, EmptyNodeID, decoded.SenderID)
}

func TestShamatonSerialization_MaxTimestamp(t *testing.T) {
	sr := shamaton.New()

	maxTS := hlc.Timestamp(^uint64(0))
	p := Packet{
		MessageType: UserMsg,
		SenderID:    NodeID(uuid.New()),
		MessageID:   MessageID(maxTS),
		TTL:         5,
	}
	data, err := sr.Marshal(&p)
	require.NoError(t, err)

	var decoded Packet
	require.NoError(t, sr.Unmarshal(data, &decoded))
	assert.Equal(t, maxTS, hlc.Timestamp(decoded.MessageID))
}

func TestShamatonSerialization_TypeMismatch(t *testing.T) {
	sr := shamaton.New()

	// Encode a plain string
	data, err := sr.Marshal("hello-world")
	require.NoError(t, err)

	// Try to decode as a struct — should error
	var msg joinMessage
	err = sr.Unmarshal(data, &msg)
	require.Error(t, err)
}

func TestShamatonSerialization_TimeInMetadata(t *testing.T) {
	md := NewMetadata()
	testTime := time.Date(2025, 6, 15, 12, 30, 45, 0, time.UTC)
	md.SetTime("created", testTime)
	md.SetString("name", "test-node")

	sr := shamaton.New()

	// Serialize the metadata map
	allData := md.GetAll()
	data, err := sr.Marshal(allData)
	require.NoError(t, err)

	// Deserialize into a new map
	var decoded map[string]interface{}
	require.NoError(t, sr.Unmarshal(data, &decoded))

	// shamaton decodes time.Time as UTC
	decodedTime, ok := decoded["created"].(time.Time)
	require.True(t, ok, "created should be time.Time, got %T", decoded["created"])
	assert.True(t, testTime.Equal(decodedTime), "time instant should match")
	assert.Equal(t, time.UTC, decodedTime.Location(), "shamaton should decode time as UTC")

	assert.Equal(t, "test-node", decoded["name"])
}

func TestShamatonSerialization_MixedMetadataTypes(t *testing.T) {
	md := NewMetadata()
	md.SetString("s", "string-val")
	md.SetBool("b", true)
	md.SetInt("i", 42)
	md.SetInt64("i64", 9223372036854775807)
	md.SetUint64("u64", 18446744073709551615)
	md.SetFloat64("f", 3.14159)
	md.SetTime("t", time.Now().UTC())

	sr := shamaton.New()

	data, err := sr.Marshal(md.GetAll())
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, sr.Unmarshal(data, &decoded))

	// All keys should be present
	for _, key := range []string{"s", "b", "i", "i64", "u64", "f", "t"} {
		_, exists := decoded[key]
		assert.True(t, exists, "key %s should exist in decoded metadata", key)
	}

	// String should round-trip as string (not []byte)
	assert.Equal(t, "string-val", decoded["s"])

	// Bool should round-trip as bool
	assert.Equal(t, true, decoded["b"])
}

func TestShamatonSerialization_LargeNodeList(t *testing.T) {
	sr := shamaton.New()

	// Simulate a large pushPullState response
	states := make([]exchangeNodeState, 200)
	for i := range states {
		states[i] = exchangeNodeState{
			ID:             NodeID(uuid.New()),
			AdvertiseAddr:  "127.0.0.1:" + itoa(8000+i),
			State:          NodeAlive,
			StateTimestamp: hlc.Now(),
		}
	}

	data, err := sr.Marshal(&states)
	require.NoError(t, err)

	var decoded []exchangeNodeState
	require.NoError(t, sr.Unmarshal(data, &decoded))
	assert.Len(t, decoded, 200)
	assert.Equal(t, states[0].ID, decoded[0].ID)
	assert.Equal(t, states[199].AdvertiseAddr, decoded[199].AdvertiseAddr)
}

// ============================================================================
// Health Monitor with Shamaton (live cluster)
// ============================================================================

func TestShamaton_HealthMonitor(t *testing.T) {
	addr1 := getFreeTCPAddress(t)
	addr2 := getFreeTCPAddress(t)

	mkLiveShamatonCluster := func(addr string) *Cluster {
		t.Helper()
		cfg := DefaultConfig()
		cfg.BindAddr = addr
		cfg.AdvertiseAddr = addr
		cfg.Transport = NewSocketTransport(cfg)
		cfg.MsgCodec = shamaton.New()
		cfg.Logger = logger.NewNullLogger()
		c, err := NewCluster(cfg)
		require.NoError(t, err)
		c.Start()
		return c
	}

	c1 := mkLiveShamatonCluster(addr1)
	defer c1.Stop()
	c2 := mkLiveShamatonCluster(addr2)
	defer c2.Stop()

	require.NoError(t, c2.Join([]string{addr1}))

	// Wait for mutual discovery
	require.Eventually(t, func() bool {
		return c1.GetNode(c2.LocalNode().ID) != nil &&
			c2.GetNode(c1.LocalNode().ID) != nil
	}, 5*time.Second, 100*time.Millisecond)

	// Both nodes should be alive (health monitor pings working with shamaton)
	require.Eventually(t, func() bool {
		n1 := c2.GetNode(c1.LocalNode().ID)
		n2 := c1.GetNode(c2.LocalNode().ID)
		return n1 != nil && n1.Alive() && n2 != nil && n2.Alive()
	}, 5*time.Second, 200*time.Millisecond, "both nodes should be alive via ping/pong")

	// Verify metadata propagates through pong (health monitor includes metadata in pong)
	c2.LocalMetadata().SetString("health-test", "ok")
	require.Eventually(t, func() bool {
		n := c1.GetNode(c2.LocalNode().ID)
		if n == nil {
			return false
		}
		return n.Metadata.GetString("health-test") == "ok"
	}, 5*time.Second, 200*time.Millisecond, "metadata should propagate through health-monitor pong messages")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
