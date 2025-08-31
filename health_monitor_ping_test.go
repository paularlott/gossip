package gossip

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip/codec"
)

// newTestCluster returns a minimal cluster with fake transport and immediate timers suitable for unit tests.
func newTestCluster(t *testing.T) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SocketTransportEnabled = false
	cfg.WebsocketProvider = nil
	cfg.Transport = &fakeTransport{}
	cfg.MsgCodec = codec.NewJsonCodec()
	// Shorten everything for tests
	cfg.HealthCheckInterval = 50 * time.Millisecond
	cfg.SuspectAttemptInterval = 50 * time.Millisecond
	cfg.RecoveryAttemptInterval = 50 * time.Millisecond
	cfg.PingTimeout = 30 * time.Millisecond
	// Disable periodic state sync to avoid background races in tests
	cfg.StateSyncInterval = time.Hour
	cfg.MsgHistoryGCInterval = 50 * time.Millisecond
	cfg.MsgHistoryMaxAge = 50 * time.Millisecond

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	// Inject fake transport explicitly (NewCluster already set it), then start workers
	c.transport = cfg.Transport
	c.Start()
	return c
}

func TestPingNode_AckFlow(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Create a remote node in list
	remote := newNode(NodeID{1}, "ws://n1")
	c.nodes.addOrUpdate(remote)

	// Wire fake transport to simulate immediate ack delivery
	ft := c.transport.(*fakeTransport)
	ft.SetOnSend(func(pkt *Packet) error {
		switch pkt.MessageType {
		case pingMsg:
			// Deliver ack back
			var ping pingMessage
			_ = pkt.Unmarshal(&ping)
			c.healthMonitor.pingAckReceived(ping.TargetID, ping.Seq, true)
		}
		return nil
	})

	ok, err := c.healthMonitor.pingNode(remote)
	if err != nil {
		t.Fatalf("pingNode error: %v", err)
	}
	if !ok {
		t.Fatalf("expected ping success")
	}
}

func TestIndirectPing_AckFlow(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// target and two helpers
	target := newNode(NodeID{2}, "ws://target")
	helper1 := newNode(NodeID{3}, "ws://h1")
	helper2 := newNode(NodeID{4}, "ws://h2")
	c.nodes.addOrUpdate(target)
	c.nodes.addOrUpdate(helper1)
	c.nodes.addOrUpdate(helper2)

	ft := c.transport.(*fakeTransport)
	ft.SetOnSend(func(pkt *Packet) error {
		switch pkt.MessageType {
		case indirectPingMsg:
			var ip indirectPingMessage
			_ = pkt.Unmarshal(&ip)
			// Simulate one helper getting an ack from target and replying
			c.healthMonitor.pingAckReceived(ip.TargetID, ip.Seq, true)
		}
		return nil
	})

	ok, err := c.healthMonitor.indirectPingNode(target)
	if err != nil {
		t.Fatalf("indirectPingNode error: %v", err)
	}
	if !ok {
		t.Fatalf("expected indirect ping success")
	}
}

func TestMetadataAutoBroadcast_Smoke(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	var sent int64
	ft := c.transport.(*fakeTransport)
	ft.SetOnSend(func(pkt *Packet) error {
		if pkt.MessageType == metadataUpdateMsg {
			atomic.AddInt64(&sent, 1)
		}
		return nil
	})

	// Trigger metadata change
	c.localNode.metadata.SetString("foo", "bar")

	// Allow async enqueue/broadcast
	time.Sleep(60 * time.Millisecond)
	if atomic.LoadInt64(&sent) == 0 {
		t.Fatalf("expected metadata update to be broadcast")
	}
}

func TestSuspectQuorumMarksDead(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Make 4 alive nodes total so quorum = max(2, ceil(4*0.25)=1) => 2
	n1 := newNode(NodeID{10}, "")
	n2 := newNode(NodeID{11}, "")
	n3 := newNode(NodeID{12}, "")
	c.nodes.addOrUpdate(n1)
	c.nodes.addOrUpdate(n2)
	c.nodes.addOrUpdate(n3)

	// Target node starts Suspect
	target := newNode(NodeID{13}, "")
	c.nodes.addOrUpdate(target)
	c.nodes.updateState(target.ID, NodeSuspect)

	// Seed suspicion evidence with two confirmations
	ev := &suspicionEvidence{
		suspectTime:   time.Now().Add(-time.Second),
		confirmations: map[NodeID]bool{n1.ID: true, n2.ID: true},
		refutations:   map[NodeID]bool{},
	}
	c.healthMonitor.suspicionMap.Store(target.ID, ev)

	// Evaluate
	c.healthMonitor.evaluateSuspectNode(target)

	// Expect marked dead
	got := c.nodes.get(target.ID)
	if got == nil || got.state != NodeDead {
		t.Fatalf("expected target to be marked dead by quorum; got state %v", got.state)
	}
}
