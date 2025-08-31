package gossip

import (
	"testing"
	"time"
)

// TestTinyCluster_SuspectToDead ensures in a 2-node cluster the survivor can
// mark the peer dead without waiting for quorum when pings keep failing.
func TestTinyCluster_SuspectToDead(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Configure short intervals to make the test fast
	c.config.HealthCheckInterval = 30 * time.Millisecond
	c.config.SuspectAttemptInterval = 30 * time.Millisecond
	c.config.PingTimeout = 10 * time.Millisecond
	c.config.SuspectThreshold = 1

	// Add a single peer under test
	peer := newNode(NodeID{9}, "ws://peer")
	c.nodes.addOrUpdate(peer)

	// Add a second dummy peer to avoid the health monitor's rejoin short-circuit
	// (the test helper excludes the local node from NodeList, so we need >=2 peers)
	dummy := newNode(NodeID{10}, "ws://dummy")
	c.nodes.addOrUpdate(dummy)

	// Fake transport never acks pings to simulate a dead peer
	ft := c.transport.(*fakeTransport)
	ft.SetOnSend(func(pkt *Packet) error { return nil })

	// Wait up to ~1 second for transitions: suspect then dead
	deadline := time.Now().Add(1 * time.Second)
	becameSuspect := false
	for time.Now().Before(deadline) {
		st := c.GetNode(peer.ID).GetState()
		if st == NodeSuspect {
			becameSuspect = true
		}
		if st == NodeDead {
			if !becameSuspect {
				t.Fatalf("peer became dead without passing through suspect")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("peer did not become dead in time; final state=%v", c.GetNode(peer.ID).GetState())
}
