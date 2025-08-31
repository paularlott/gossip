package gossip

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/hlc"
)

func TestHandlePing_SendsAck(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Known sender
	sender := newNode(NodeID{21}, "ws://s")
	c.nodes.addOrUpdate(sender)

	// Capture acks
	gotAck := make(chan struct{}, 1)
	c.transport.(*fakeTransport).SetOnSend(func(pkt *Packet) error {
		if pkt.MessageType == pingAckMsg {
			select {
			case gotAck <- struct{}{}:
			default:
			}
		}
		return nil
	})

	// Build ping for our local node
	ping := &pingMessage{TargetID: c.localNode.ID, AdvertiseAddr: c.config.AdvertiseAddr}
	pkt, err := c.createPacket(sender.ID, pingMsg, 3, ping)
	if err != nil {
		t.Fatalf("createPacket: %v", err)
	}
	defer pkt.Release()

	c.handleIncomingPacket(pkt)

	select {
	case <-gotAck:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected ping ack to be sent")
	}
}

func TestHandleIndirectPing_AcksWhenPingOk(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	sender := newNode(NodeID{31}, "ws://sender")
	c.nodes.addOrUpdate(sender)

	targetID := NodeID{32}

	acks := 0
	c.transport.(*fakeTransport).SetOnSend(func(pkt *Packet) error {
		switch pkt.MessageType {
		case pingMsg:
			// Simulate target responding OK
			var p pingMessage
			_ = pkt.Unmarshal(&p)
			c.healthMonitor.pingAckReceived(p.TargetID, p.Seq, true)
		case indirectPingAckMsg:
			acks++
		}
		return nil
	})

	ip := &indirectPingMessage{TargetID: targetID, AdvertiseAddr: "ws://target"}
	pkt, err := c.createPacket(sender.ID, indirectPingMsg, 3, ip)
	if err != nil {
		t.Fatalf("createPacket: %v", err)
	}
	defer pkt.Release()

	c.handleIncomingPacket(pkt)

	// Allow async send
	time.Sleep(50 * time.Millisecond)
	if acks == 0 {
		t.Fatalf("expected indirect ping ack to be sent")
	}
}

func TestHandleJoin_AcceptsAndAddsNode(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	jm := &joinMessage{
		ID:                 NodeID{41},
		AdvertiseAddr:      "127.0.0.1:1234",
		MetadataTimestamp:  hlc.Now(),
		Metadata:           map[string]interface{}{"role": "app"},
		ProtocolVersion:    PROTOCOL_VERSION,
		ApplicationVersion: c.config.ApplicationVersion,
	}

	pkt, err := c.createPacket(NodeID{99}, nodeJoinMsg, 1, jm)
	if err != nil {
		t.Fatalf("createPacket: %v", err)
	}
	defer pkt.Release()

	resp, err := c.handleJoin(nil, pkt)
	if err != nil {
		t.Fatalf("handleJoin: %v", err)
	}

	reply, ok := resp.(*joinReplyMessage)
	if !ok || !reply.Accepted {
		t.Fatalf("expected join to be accepted, got: %#v", resp)
	}

	added := c.nodes.get(jm.ID)
	if added == nil {
		t.Fatalf("expected node to be added")
	}
	if got := added.metadata.GetString("role"); got != "app" {
		t.Fatalf("metadata not applied: %q", got)
	}
}

func TestHandleMetadataUpdate_ForceApplyOnDifferentData(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Existing sender with newer timestamp and different data
	sender := newNode(NodeID{51}, "")
	c.nodes.addOrUpdate(sender)
	newer := hlc.Now()
	sender.metadata.update(map[string]interface{}{"x": 1}, newer, true)

	// Older ts but different data should still be force-applied
	older := hlc.Timestamp(uint64(newer) - 1)
	mu := &metadataUpdateMessage{MetadataTimestamp: older, Metadata: map[string]interface{}{"x": 2}}

	applied := make(chan struct{}, 1)
	c.HandleNodeMetadataChangeFunc(func(n *Node) {
		if n.ID == sender.ID {
			select {
			case applied <- struct{}{}:
			default:
			}
		}
	})

	pkt, err := c.createPacket(sender.ID, metadataUpdateMsg, 2, mu)
	if err != nil {
		t.Fatalf("createPacket: %v", err)
	}
	defer pkt.Release()

	if err := c.handleMetadataUpdate(sender, pkt); err != nil {
		t.Fatalf("handleMetadataUpdate: %v", err)
	}

	// Verify forced change
	if v := sender.metadata.GetInt("x"); v != 2 {
		t.Fatalf("expected metadata to be force-updated, got %d", v)
	}

	select {
	case <-applied:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected metadata change notification")
	}
}
