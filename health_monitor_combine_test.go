package gossip

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/hlc"
)

// Drives combineRemoteNodeState for key transitions using real ping flows via fake transport.
func TestCombineRemote_StateTransitions_WithPingPaths(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Nodes
	target := newNode(NodeID{1}, "")
	c.nodes.addOrUpdate(target)
	c.nodes.updateState(target.ID, NodeAlive)

	sender := newNode(NodeID{7}, "")
	c.nodes.addOrUpdate(sender)

	ft := c.transport.(*fakeTransport)

	// Case 1: Remote says Dead, our ping fails -> mark Suspect
	ft.SetOnSend(func(pkt *Packet) error { return nil }) // no ack => timeout
	rs := []exchangeNodeState{{ID: target.ID, State: NodeDead, StateChangeTime: hlc.Now()}}
	start := time.Now()
	c.healthMonitor.combineRemoteNodeState(sender, rs)
	if time.Since(start) < c.config.PingTimeout {
		t.Fatalf("expected combine to wait for ping timeout")
	}
	if got := c.nodes.get(target.ID).state; got != NodeSuspect {
		t.Fatalf("expected suspect after dead report and failed ping; got %v", got)
	}

	// Case 2: Remote says Dead, our ping succeeds -> refute (stay Alive)
	c.nodes.updateState(target.ID, NodeAlive)
	ft.SetOnSend(func(pkt *Packet) error {
		if pkt.MessageType == pingMsg {
			var pm pingMessage
			_ = pkt.Unmarshal(&pm)
			c.healthMonitor.pingAckReceived(pm.TargetID, pm.Seq, true)
		}
		return nil
	})
	c.healthMonitor.combineRemoteNodeState(sender, rs)
	if got := c.nodes.get(target.ID).state; got != NodeAlive {
		t.Fatalf("expected remain alive after successful ping; got %v", got)
	}

	// Case 3: Remote says Alive, we restore from Suspect/Dead when ping succeeds
	c.nodes.updateState(target.ID, NodeSuspect)
	rsAlive := []exchangeNodeState{{ID: target.ID, State: NodeAlive, StateChangeTime: hlc.Now()}}
	c.healthMonitor.combineRemoteNodeState(sender, rsAlive)
	if got := c.nodes.get(target.ID).state; got != NodeAlive {
		t.Fatalf("expected restore to alive from suspect; got %v", got)
	}
	c.nodes.updateState(target.ID, NodeDead)
	c.healthMonitor.combineRemoteNodeState(sender, rsAlive)
	if got := c.nodes.get(target.ID).state; got != NodeAlive {
		t.Fatalf("expected restore to alive from dead; got %v", got)
	}

	// Case 4: Remote says Leaving -> apply and cleanup tracking
	c.healthMonitor.suspicionMap.Store(target.ID, &suspicionEvidence{})
	rsLeave := []exchangeNodeState{{ID: target.ID, State: NodeLeaving, StateChangeTime: hlc.Now()}}
	c.healthMonitor.combineRemoteNodeState(sender, rsLeave)
	if got := c.nodes.get(target.ID).state; got != NodeLeaving {
		t.Fatalf("expected leaving applied; got %v", got)
	}
	if _, ok := c.healthMonitor.suspicionMap.Load(target.ID); ok {
		t.Fatalf("expected suspicion cleanup on leaving")
	}

	// Case 5: Remote says Suspect; ping outcome drives decision
	c.nodes.updateState(target.ID, NodeAlive)
	// success ping => refute (stay alive)
	ft.SetOnSend(func(pkt *Packet) error {
		if pkt.MessageType == pingMsg {
			var pm pingMessage
			_ = pkt.Unmarshal(&pm)
			c.healthMonitor.pingAckReceived(pm.TargetID, pm.Seq, true)
		}
		return nil
	})
	rsSus := []exchangeNodeState{{ID: target.ID, State: NodeSuspect, StateChangeTime: hlc.Now()}}
	c.healthMonitor.combineRemoteNodeState(sender, rsSus)
	if got := c.nodes.get(target.ID).state; got != NodeAlive {
		t.Fatalf("expected remain alive after suspect + successful ping; got %v", got)
	}
	// fail ping => set suspect
	ft.SetOnSend(func(pkt *Packet) error { return nil })
	start = time.Now()
	c.healthMonitor.combineRemoteNodeState(sender, rsSus)
	if time.Since(start) < c.config.PingTimeout {
		t.Fatalf("expected combine to wait for ping timeout on suspect case")
	}
	if got := c.nodes.get(target.ID).state; got != NodeSuspect {
		t.Fatalf("expected suspect after suspect report + failed ping; got %v", got)
	}
}
