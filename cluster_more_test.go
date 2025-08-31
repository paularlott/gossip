package gossip

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestCluster_LoggerAndNodesHelpers(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Touch Nodes() and AliveNodes()
	_ = c.Nodes()
	_ = c.AliveNodes()

	// Logger accessor
	if c.Logger() == nil {
		t.Fatalf("expected non-nil logger")
	}
}

func TestCluster_GossipHandlers(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	called := 0
	id := c.HandleGossipFunc(func() { called++ })
	c.notifyDoGossip()
	if called != 1 {
		t.Fatalf("expected gossip handler to be called once; got %d", called)
	}
	if !c.RemoveGossipHandler(id) {
		t.Fatalf("RemoveGossipHandler failed")
	}
}

func TestCluster_StreamCompressionHelpers(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// On non-stream it should be a no-op and return same cluster
	s1, s2 := net.Pipe()
	defer s1.Close()
	defer s2.Close()
	if c.EnableStreamCompression(s1) != c {
		t.Fatalf("EnableStreamCompression should return cluster")
	}
	if c.DisableStreamCompression(s1) != c {
		t.Fatalf("DisableStreamCompression should return cluster")
	}
}

func TestClusterHandlers_PingAckAndIndirectAck(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Prepare channels in health monitor for ack tracking
	c.healthMonitor.peerPingMutex.Lock()
	seq1 := c.healthMonitor.pingSeq
	c.healthMonitor.pingSeq++
	key1 := healthPingID{TargetID: NodeID{7}, Seq: seq1}
	ch1 := make(chan bool, 1)
	c.healthMonitor.peerPingAck[key1] = ch1

	seq2 := c.healthMonitor.pingSeq
	c.healthMonitor.pingSeq++
	key2 := healthPingID{TargetID: NodeID{7}, Seq: seq2}
	ch2 := make(chan bool, 1)
	c.healthMonitor.peerPingAck[key2] = ch2
	c.healthMonitor.peerPingMutex.Unlock()

	sender := newNode(NodeID{7}, "")

	// Build ping ack packet
	pkt1, _ := c.createPacket(sender.ID, pingAckMsg, 1, &pingMessage{TargetID: NodeID{9}, Seq: seq1})
	defer pkt1.Release()
	if err := c.handlePingAck(sender, pkt1); err != nil {
		t.Fatalf("handlePingAck: %v", err)
	}
	select {
	case v := <-ch1:
		if !v {
			t.Fatalf("expected true ack")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout waiting for ack")
	}

	// Build indirect ping ack packet
	pkt2, _ := c.createPacket(sender.ID, indirectPingAckMsg, 1, &indirectPingMessage{TargetID: NodeID{10}, Seq: seq2, Ok: true})
	defer pkt2.Release()
	if err := c.handleIndirectPingAck(sender, pkt2); err != nil {
		t.Fatalf("handleIndirectPingAck: %v", err)
	}
	select {
	case v := <-ch2:
		if !v {
			t.Fatalf("expected true ack")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout waiting for ack")
	}
}

func TestHandleAlive_UnknownNodeAdds(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	from := newNode(NodeID{20}, "")
	msg := &aliveMessage{NodeID: NodeID{21}, AdvertiseAddr: "ws://n21"}
	pkt, _ := c.createPacket(from.ID, aliveMsg, 1, msg)
	defer pkt.Release()

	if err := c.healthMonitor.handleAlive(from, pkt); err != nil {
		t.Fatalf("handleAlive: %v", err)
	}
	if got := c.nodes.get(msg.NodeID); got == nil {
		t.Fatalf("expected unknown node to be added")
	}
}

func TestStreamMsg_ErrorBranches(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Oversize payload error in WriteStreamMsg
	c.config.TCPMaxPacketSize = 32
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()
	big := bytes.Repeat([]byte{'a'}, int(c.config.TCPMaxPacketSize)) // payload+6 will exceed
	if err := c.WriteStreamMsg(conn1, UserMsg, big); err == nil {
		t.Fatalf("expected oversize error")
	}

	// ReadStreamMsg wrong type
	// Write header [msgType=UserMsg+1][len=0]
	buf := make([]byte, 6)
	binary.BigEndian.PutUint16(buf[0:2], uint16(UserMsg+1))
	binary.BigEndian.PutUint32(buf[2:6], 0)
	go func() { conn2.Write(buf) }()
	var out struct{}
	if err := c.ReadStreamMsg(conn1, UserMsg, &out); err == nil {
		t.Fatalf("expected type mismatch error")
	}

	// ReadStreamMsg too large length
	conn1, conn2 = net.Pipe()
	defer conn1.Close()
	defer conn2.Close()
	c.config.TCPMaxPacketSize = 16
	buf = make([]byte, 6)
	binary.BigEndian.PutUint16(buf[0:2], uint16(UserMsg))
	binary.BigEndian.PutUint32(buf[2:6], 1024)
	go func() { conn2.Write(buf) }()
	if err := c.ReadStreamMsg(conn1, UserMsg, &out); err == nil {
		t.Fatalf("expected payload too large error")
	}
}
