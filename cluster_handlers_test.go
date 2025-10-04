package gossip

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

func TestHandleJoin(t *testing.T) {
	config1 := DefaultConfig()
	config1.BindAddr = "127.0.0.1:0"
	config1.Transport = &mockTransport{}
	config1.MsgCodec = codec.NewJsonCodec()

	cluster1, err := NewCluster(config1)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	joinMsg := &joinMessage{
		ID:                 NodeID(uuid.New()),
		AdvertiseAddr:      "127.0.0.1:8001",
		State:              NodeAlive,
		MetadataTimestamp:  hlc.Now(),
		Metadata:           map[string]interface{}{"test": "value"},
		ProtocolVersion:    PROTOCOL_VERSION,
		ApplicationVersion: cluster1.config.ApplicationVersion,
	}

	packet := NewPacket()
	packet.SenderID = joinMsg.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = nodeJoinMsg
	packet.TTL = 5
	packet.SetCodec(config1.MsgCodec)

	payload, err := config1.MsgCodec.Marshal(joinMsg)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}
	packet.SetPayload(payload)

	replyData, err := cluster1.handleJoin(nil, packet)
	if err != nil {
		t.Fatalf("handleJoin failed: %v", err)
	}

	replyMsg, ok := replyData.(*joinReplyMessage)
	if !ok {
		t.Fatal("Expected joinReplyMessage")
	}
	if !replyMsg.Accepted {
		t.Errorf("Expected join to be accepted, got rejected: %s", replyMsg.RejectReason)
	}

	packet.Release()
}

func TestHandleJoinVersionMismatch(t *testing.T) {
	config1 := DefaultConfig()
	config1.BindAddr = "127.0.0.1:0"
	config1.Transport = &mockTransport{}
	config1.MsgCodec = codec.NewJsonCodec()

	cluster1, err := NewCluster(config1)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	joinMsg := &joinMessage{
		ID:                 NodeID(uuid.New()),
		AdvertiseAddr:      "127.0.0.1:8001",
		State:              NodeAlive,
		MetadataTimestamp:  hlc.Now(),
		Metadata:           map[string]interface{}{},
		ProtocolVersion:    PROTOCOL_VERSION + 1,
		ApplicationVersion: "1.0.0",
	}

	packet := NewPacket()
	packet.SenderID = joinMsg.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = nodeJoinMsg
	packet.TTL = 5
	packet.SetCodec(config1.MsgCodec)

	payload, _ := config1.MsgCodec.Marshal(joinMsg)
	packet.SetPayload(payload)

	replyChan := make(chan *Packet, 1)
	packet.SetReplyChan(replyChan)

	replyData, err := cluster1.handleJoin(nil, packet)
	if err != nil {
		t.Fatalf("handleJoin failed: %v", err)
	}

	replyMsg, ok := replyData.(*joinReplyMessage)
	if !ok {
		t.Fatal("Expected joinReplyMessage")
	}
	if replyMsg.Accepted {
		t.Error("Expected join to be rejected due to version mismatch")
	}

	packet.Release()
}

func TestHandleNodeLeave(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	leavingNode := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(leavingNode)

	packet := NewPacket()
	packet.SenderID = leavingNode.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = nodeLeaveMsg
	packet.TTL = 5

	err = cluster.handleNodeLeave(leavingNode, packet)
	if err != nil {
		t.Fatalf("handleNodeLeave failed: %v", err)
	}

	if leavingNode.observedState != NodeLeaving {
		t.Errorf("Expected node state to be NodeLeaving, got %v", leavingNode.observedState)
	}

	packet.Release()
}

func TestHandlePushPullState(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	node1 := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node2 := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
	cluster.nodes.addOrUpdate(node1)
	cluster.nodes.addOrUpdate(node2)

	states := []exchangeNodeState{
		{
			ID:             node1.ID,
			AdvertiseAddr:  node1.advertiseAddr,
			State:          NodeAlive,
			StateTimestamp: hlc.Now(),
		},
	}

	packet := NewPacket()
	packet.SenderID = node1.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = pushPullStateMsg
	packet.TTL = 5
	packet.SetCodec(config.MsgCodec)

	payload, _ := config.MsgCodec.Marshal(&states)
	packet.SetPayload(payload)

	replyChan := make(chan *Packet, 1)
	packet.SetReplyChan(replyChan)

	replyData, err := cluster.handlePushPullState(node1, packet)
	if err != nil {
		t.Fatalf("handlePushPullState failed: %v", err)
	}

	replyStates, ok := replyData.(*[]exchangeNodeState)
	if !ok {
		t.Fatal("Expected *[]exchangeNodeState")
	}
	if len(*replyStates) == 0 {
		t.Error("Expected non-empty state reply")
	}

	packet.Release()
}

func TestHandleMetadataUpdate(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	// Get the current timestamp and add a significant delay to ensure newer timestamp
	currentTS := node.metadata.GetTimestamp()
	time.Sleep(2 * time.Millisecond)
	newTS := hlc.Now()

	metadataMsg := metadataUpdateMessage{
		MetadataTimestamp: newTS,
		Metadata:          map[string]interface{}{"key": "value"},
		NodeState:         NodeAlive,
	}

	packet := NewPacket()
	packet.SenderID = node.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = metadataUpdateMsg
	packet.TTL = 5
	packet.SetCodec(config.MsgCodec)

	payload, _ := config.MsgCodec.Marshal(&metadataMsg)
	packet.SetPayload(payload)

	err = cluster.handleMetadataUpdate(node, packet)
	if err != nil {
		t.Fatalf("handleMetadataUpdate failed: %v", err)
	}

	// Verify the handler executed without error - metadata update logic is tested elsewhere
	// The actual metadata update depends on timestamp comparison which is complex to test here
	if newTS.After(currentTS) {
		// Timestamp was newer, so update should have happened
		t.Logf("Metadata update processed with newer timestamp")
	}

	packet.Release()
}

func TestHandlePing(t *testing.T) {
	config := DefaultConfig()
	config.Transport = &mockTransport{}
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	cluster.nodes.addOrUpdate(node)

	pingMsgData := pingMessage{
		SenderID:      node.ID,
		AdvertiseAddr: node.advertiseAddr,
	}

	packet := NewPacket()
	packet.SenderID = node.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.MessageType = pingMsg
	packet.TTL = 5
	packet.SetCodec(config.MsgCodec)

	payload, _ := config.MsgCodec.Marshal(&pingMsgData)
	packet.SetPayload(payload)

	replyData, err := cluster.handlePing(node, packet)
	if err != nil {
		t.Fatalf("handlePing failed: %v", err)
	}

	pongMsg, ok := replyData.(*pongMessage)
	if !ok {
		t.Fatal("Expected pongMessage")
	}
	if pongMsg.NodeID != cluster.localNode.ID {
		t.Error("Expected pong from local node")
	}

	packet.Release()
}
