package gossip

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

// ============================================================================
// Packet: lifecycle and ref counting
// ============================================================================

func TestPacketRefCounting(t *testing.T) {
	p := NewPacket()
	// refCount should be 1

	p.AddRef()
	// refCount should be 2

	p.Release() // -> 1
	p.Release() // -> 0, returned to pool
}

func TestPacketPayloadAndCodec(t *testing.T) {
	p := NewPacket()
	defer p.Release()

	c := codec.NewJSONCodec()
	p.SetCodec(c)

	if p.Codec() != c {
		t.Error("Codec() should return the set codec")
	}

	payload := []byte(`{"key":"value"}`)
	p.SetPayload(payload)

	if string(p.Payload()) != string(payload) {
		t.Error("Payload() should return the set payload")
	}

	// Unmarshal
	var result map[string]interface{}
	err := p.Unmarshal(&result)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("Expected key=value, got %v", result["key"])
	}
}

func TestPacketCodecAccessor(t *testing.T) {
	p := NewPacket()
	defer p.Release()

	c := codec.NewJSONCodec()
	p.SetCodec(c)
	if p.Codec() != c {
		t.Error("Codec() should return what was set")
	}
}

// ============================================================================
// Packet: CanReply and SendReply
// ============================================================================

func TestPacketCanReplyWithReplyChan(t *testing.T) {
	p := NewPacket()
	defer p.Release()

	if p.CanReply() {
		t.Error("Should not be able to reply without channel or conn")
	}

	ch := make(chan *Packet, 1)
	p.SetReplyChan(ch)

	if !p.CanReply() {
		t.Error("Should be able to reply with channel set")
	}

	// Send a reply
	p.AddRef()
	err := p.SendReply()
	if err != nil {
		t.Fatalf("SendReply failed: %v", err)
	}

	// Verify reply received
	select {
	case reply := <-ch:
		reply.Release()
	default:
		t.Error("Expected reply on channel")
	}
}

func TestPacketSendReplyNoMechanism(t *testing.T) {
	p := NewPacket()
	defer p.Release()

	err := p.SendReply()
	if err == nil {
		t.Error("Expected error when no reply mechanism is set")
	}
}

func TestPacketSendReplyFullChannel(t *testing.T) {
	p := NewPacket()
	defer p.Release()

	ch := make(chan *Packet) // unbuffered
	p.SetReplyChan(ch)

	// Send without anyone reading should fail
	err := p.SendReply()
	if err == nil {
		t.Error("Expected error when channel is full")
	}
}

// ============================================================================
// handlerRegistry: register, dispatch, unregister
// ============================================================================

func TestHandlerRegistryBasic(t *testing.T) {
	hr := newHandlerRegistry()

	hr.registerHandler(UserMsg, func(node *Node, packet *Packet) error {
		return nil
	})

	h := hr.getHandler(UserMsg)
	if h == nil {
		t.Fatal("Handler should be registered")
	}

	// Unregistered type returns nil
	if hr.getHandler(UserMsg+1) != nil {
		t.Error("Unregistered handler should return nil")
	}

	// Unregister
	if !hr.unregister(UserMsg) {
		t.Error("Should succeed unregistering")
	}
	if hr.unregister(UserMsg) {
		t.Error("Should fail re-unregistering")
	}
}

func TestHandlerRegistryDuplicatePanics(t *testing.T) {
	hr := newHandlerRegistry()

	hr.registerHandler(UserMsg, func(node *Node, packet *Packet) error { return nil })

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic on duplicate registration")
		}
	}()

	hr.registerHandler(UserMsg, func(node *Node, packet *Packet) error { return nil })
}

func TestHandlerRegistryWithReply(t *testing.T) {
	hr := newHandlerRegistry()

	hr.registerHandlerWithReply(UserMsg, func(node *Node, packet *Packet) (interface{}, error) {
		return "response", nil
	})

	h := hr.getHandler(UserMsg)
	if h == nil {
		t.Fatal("Handler should be registered")
	}
	if h.replyHandler == nil {
		t.Error("Reply handler should be set")
	}
}

// ============================================================================
// msgHandler dispatch
// ============================================================================

func TestMsgHandlerDispatchNilPacketDeep(t *testing.T) {
	mh := &msgHandler{
		handler: func(node *Node, packet *Packet) error { return nil },
	}

	err := mh.dispatch(nil, nil, nil)
	if err == nil {
		t.Error("Expected error for nil packet")
	}
}

func TestMsgHandlerDispatchFireAndForget(t *testing.T) {
	called := false
	mh := &msgHandler{
		handler: func(node *Node, packet *Packet) error {
			called = true
			return nil
		},
	}

	packet := NewPacket()
	packet.SetCodec(codec.NewJSONCodec())

	err := mh.dispatch(nil, nil, packet)
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	if !called {
		t.Error("Handler should have been called")
	}
}

func TestMsgHandlerDispatchHandlerError(t *testing.T) {
	mh := &msgHandler{
		handler: func(node *Node, packet *Packet) error {
			return fmt.Errorf("handler error")
		},
	}

	packet := NewPacket()
	packet.SetCodec(codec.NewJSONCodec())

	err := mh.dispatch(nil, nil, packet)
	if err == nil || err.Error() != "handler error" {
		t.Errorf("Expected 'handler error', got %v", err)
	}
}

func TestMsgHandlerDispatchReplyWithChannel(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	mh := &msgHandler{
		replyHandler: func(node *Node, packet *Packet) (interface{}, error) {
			return map[string]string{"reply": "ok"}, nil
		},
	}

	replyCh := make(chan *Packet, 1)
	packet := NewPacket()
	packet.SetCodec(config.MsgCodec)
	packet.SetReplyChan(replyCh)

	err = mh.dispatch(cluster, nil, packet)
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	select {
	case reply := <-replyCh:
		if reply.MessageType != replyMsg {
			t.Error("Reply should have replyMsg type")
		}
		reply.Release()
	default:
		t.Error("Expected reply on channel")
	}
}

func TestMsgHandlerDispatchReplyHandlerErrorDeep(t *testing.T) {
	mh := &msgHandler{
		replyHandler: func(node *Node, packet *Packet) (interface{}, error) {
			return nil, fmt.Errorf("reply handler error")
		},
	}

	packet := NewPacket()
	packet.SetCodec(codec.NewJSONCodec())

	err := mh.dispatch(nil, nil, packet)
	if err == nil || err.Error() != "reply handler error" {
		t.Errorf("Expected 'reply handler error', got %v", err)
	}
}

func TestMsgHandlerDispatchReplyNilReplyData(t *testing.T) {
	mh := &msgHandler{
		replyHandler: func(node *Node, packet *Packet) (interface{}, error) {
			return nil, nil // no reply data
		},
	}

	replyCh := make(chan *Packet, 1)
	packet := NewPacket()
	packet.SetCodec(codec.NewJSONCodec())
	packet.SetReplyChan(replyCh)

	err := mh.dispatch(nil, nil, packet)
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	// No reply should be sent for nil data
	select {
	case <-replyCh:
		t.Error("Should not send reply for nil data")
	default:
		// expected
	}
}

func TestMsgHandlerDispatchNoHandlerDeep(t *testing.T) {
	mh := &msgHandler{} // no handler or replyHandler

	packet := NewPacket()
	packet.SetCodec(codec.NewJSONCodec())

	err := mh.dispatch(nil, nil, packet)
	if err == nil {
		t.Error("Expected error when no handler is registered")
	}
}

func TestMsgHandlerDispatchReplyNoCanReply(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	mh := &msgHandler{
		replyHandler: func(node *Node, packet *Packet) (interface{}, error) {
			return map[string]string{"reply": "ok"}, nil
		},
	}

	// Packet without reply channel - should handle gracefully
	packet := NewPacket()
	packet.SetCodec(config.MsgCodec)

	err = mh.dispatch(cluster, nil, packet)
	if err != nil {
		t.Errorf("Should not fail when can't reply, got: %v", err)
	}
}

// ============================================================================
// Packet: message types
// ============================================================================

func TestMessageTypeConstants(t *testing.T) {
	if replyMsg != 0 {
		t.Error("replyMsg should be 0")
	}
	if UserMsg != 128 {
		t.Errorf("UserMsg should be 128, got %d", UserMsg)
	}
	if ReservedMsgsStart != 64 {
		t.Errorf("ReservedMsgsStart should be 64, got %d", ReservedMsgsStart)
	}
}

// ============================================================================
// Cluster: handler dispatch for cluster handlers
// ============================================================================

func TestHandleJoinWithTags(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.Tags = []string{"web"}

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	joinMsg := &joinMessage{
		ID:                 NodeID(uuid.New()),
		AdvertiseAddr:      "127.0.0.1:8001",
		State:              NodeAlive,
		Tags:               []string{"api"},
		MetadataTimestamp:  hlc.Now(),
		Metadata:           map[string]interface{}{"foo": "bar"},
		ProtocolVersion:    PROTOCOL_VERSION,
		ApplicationVersion: cluster.config.ApplicationVersion,
	}

	packet := NewPacket()
	packet.SenderID = joinMsg.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = nodeJoinMsg
	packet.TTL = 5
	packet.SetCodec(config.MsgCodec)

	payload, _ := config.MsgCodec.Marshal(joinMsg)
	packet.SetPayload(payload)

	replyData, err := cluster.handleJoin(nil, packet)
	if err != nil {
		t.Fatalf("handleJoin failed: %v", err)
	}

	reply, ok := replyData.(*joinReplyMessage)
	if !ok {
		t.Fatal("Expected joinReplyMessage")
	}
	if !reply.Accepted {
		t.Error("Join should be accepted")
	}

	// Verify the joining node was added
	joinedNode := cluster.nodes.get(joinMsg.ID)
	if joinedNode == nil {
		t.Error("Joined node should exist in cluster")
	}
	if joinedNode.HasTag("api") != true {
		t.Error("Joined node should have 'api' tag")
	}

	packet.Release()
}

func TestHandleJoinAppVersionCheckReject(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()
	config.ApplicationVersion = "2.0.0"
	config.ApplicationVersionCheck = func(version string) bool {
		return version == "2.0.0" // only accept exact match
	}

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	joinMsg := &joinMessage{
		ID:                 NodeID(uuid.New()),
		AdvertiseAddr:      "127.0.0.1:8001",
		State:              NodeAlive,
		MetadataTimestamp:  hlc.Now(),
		Metadata:           map[string]interface{}{},
		ProtocolVersion:    PROTOCOL_VERSION,
		ApplicationVersion: "1.0.0", // rejected by check
	}

	packet := NewPacket()
	packet.SenderID = joinMsg.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = nodeJoinMsg
	packet.TTL = 5
	packet.SetCodec(config.MsgCodec)

	payload, _ := config.MsgCodec.Marshal(joinMsg)
	packet.SetPayload(payload)

	replyData, err := cluster.handleJoin(nil, packet)
	if err != nil {
		t.Fatalf("handleJoin failed: %v", err)
	}

	reply := replyData.(*joinReplyMessage)
	if reply.Accepted {
		t.Error("Join should be rejected due to version check")
	}

	packet.Release()
}

// ============================================================================
// Cluster: createPacketWithTargetAndTag
// ============================================================================

func TestCreatePacketWithTargetAndTag(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJSONCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	targetID := NodeID(uuid.New())
	tag := "web"

	p, err := cluster.createPacketWithTargetAndTag(
		cluster.localNode.ID,
		&targetID,
		&tag,
		UserMsg,
		5,
		map[string]string{"data": "test"},
	)
	if err != nil {
		t.Fatalf("Failed to create packet: %v", err)
	}
	defer p.Release()

	if p.MessageType != UserMsg {
		t.Error("Wrong message type")
	}
	if p.SenderID != cluster.localNode.ID {
		t.Error("Wrong sender")
	}
	if *p.TargetNodeID != targetID {
		t.Error("Wrong target")
	}
	if *p.Tag != tag {
		t.Error("Wrong tag")
	}
	if p.TTL != 5 {
		t.Error("Wrong TTL")
	}
}

func TestCreatePacketMarshalError(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = &failCodec{}

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	_, err = cluster.createPacketWithTarget(
		cluster.localNode.ID, nil, UserMsg, 5, make(chan int), // unmarshalable
	)
	if err == nil {
		t.Error("Expected marshal error")
	}
}

// failCodec always fails on Marshal
type failCodec struct{}

func (f *failCodec) Marshal(v interface{}) ([]byte, error) { return nil, fmt.Errorf("marshal error") }
func (f *failCodec) Unmarshal(data []byte, v interface{}) error {
	return fmt.Errorf("unmarshal error")
}
func (f *failCodec) Name() string { return "fail" }
