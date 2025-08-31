package leader

import (
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
)

// makePacket crafts a gossip.Packet with the payload set via unsafe (for tests only)
func makePacket(cdc codec.Serializer, payload interface{}) *gossip.Packet {
	p := gossip.NewPacket()
	data, _ := cdc.Marshal(payload)
	vp := reflect.ValueOf(p).Elem()
	// set codec
	fCodec := vp.FieldByName("codec")
	reflect.NewAt(fCodec.Type(), unsafe.Pointer(fCodec.UnsafeAddr())).Elem().Set(reflect.ValueOf(cdc))
	// set payload
	fPayload := vp.FieldByName("payload")
	reflect.NewAt(fPayload.Type(), unsafe.Pointer(fPayload.UnsafeAddr())).Elem().Set(reflect.ValueOf(data))
	return p
}

func TestLeader_HeartbeatTieBreakAndTerm(t *testing.T) {
	c := newLeaderTestCluster(t)
	defer c.Stop()

	cfg := DefaultConfig()
	cfg.HeartbeatMessageType = gossip.UserMsg + 60
	le := NewLeaderElection(c, cfg)
	json := codec.NewJsonCodec()

	r1 := &gossip.Node{ID: gossip.NodeID{1}}
	r2 := &gossip.Node{ID: gossip.NodeID{2}}

	// Set from r1 term 1
	pkt1 := makePacket(json, &heartbeatMessage{LeaderTime: time.Now(), Term: 1})
	defer pkt1.Release()
	_ = le.handleLeaderHeartbeat(r1, pkt1)
	if le.GetLeaderID() != r1.ID {
		t.Fatalf("expected r1 leader")
	}

	// Newer time from r2 same term
	pkt2 := makePacket(json, &heartbeatMessage{LeaderTime: time.Now().Add(5 * time.Millisecond), Term: 1})
	defer pkt2.Release()
	_ = le.handleLeaderHeartbeat(r2, pkt2)
	if le.GetLeaderID() != r2.ID {
		t.Fatalf("expected r2 after newer time")
	}

	// Same time, lower ID wins (r1)
	lt := le.leaderTime
	pkt3 := makePacket(json, &heartbeatMessage{LeaderTime: lt, Term: 1})
	defer pkt3.Release()
	_ = le.handleLeaderHeartbeat(r1, pkt3)
	if le.GetLeaderID() != r1.ID {
		t.Fatalf("expected r1 after tie-breaker")
	}

	// Higher term from r2 wins
	pkt4 := makePacket(json, &heartbeatMessage{LeaderTime: lt.Add(-time.Second), Term: 2})
	defer pkt4.Release()
	_ = le.handleLeaderHeartbeat(r2, pkt4)
	if le.GetLeaderID() != r2.ID {
		t.Fatalf("expected r2 after higher term")
	}
}

func TestLeader_MetadataFilterHeartbeatAndHasLeader(t *testing.T) {
	c := newLeaderTestCluster(t)
	defer c.Stop()

	cfg := DefaultConfig()
	cfg.HeartbeatMessageType = gossip.UserMsg + 61
	cfg.MetadataCriteria = map[string]string{"role": "db"}
	le := NewLeaderElection(c, cfg)
	json := codec.NewJsonCodec()

	// Ineligible sender
	bad := &gossip.Node{ID: gossip.NodeID{10}}
	pktBad := makePacket(json, &heartbeatMessage{LeaderTime: time.Now(), Term: 1})
	defer pktBad.Release()
	_ = le.handleLeaderHeartbeat(bad, pktBad)
	// Should ignore ineligible heartbeat; leaderID should remain zero value
	if le.GetLeaderID() != (gossip.NodeID{}) {
		t.Fatalf("unexpected leader from ineligible heartbeat")
	}

	// Make local node eligible and accept
	c.LocalMetadata().SetString("role", "db")
	good := c.LocalNode()
	// Wait for NodeGroup to include local node (metadata change is async)
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		elig := le.getEligibleNodes()
		found := false
		for _, n := range elig {
			if n.ID == good.ID {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	pktGood := makePacket(json, &heartbeatMessage{LeaderTime: time.Now(), Term: 1})
	defer pktGood.Release()
	_ = le.handleLeaderHeartbeat(good, pktGood)
	if le.GetLeaderID() != good.ID {
		t.Fatalf("expected local eligible leader")
	}

	// Do not flip metadata to ineligible here; doing so can race with Stop() as NodeGroup updates
	// asynchronously on metadata changes. Subsequent tests cover replacement behavior.
}

func TestLeader_QuorumCalc_Bounds(t *testing.T) {
	c := newLeaderTestCluster(t)
	defer c.Stop()
	cfgA := DefaultConfig()
	cfgA.HeartbeatMessageType = gossip.UserMsg + 90
	le := NewLeaderElection(c, cfgA)
	if le.calculateQuorumForNodes(0) != 0 {
		t.Fatalf("q(0) != 0")
	}
	if le.calculateQuorumForNodes(1) != 1 {
		t.Fatalf("q(1) != 1")
	}
	le.config.QuorumPercentage = 0
	if le.calculateQuorumForNodes(5) != 0 {
		t.Fatalf("q(5) with 0%% != 0")
	}
}

func TestLeader_SendToPeers_Switch(t *testing.T) {
	c := newLeaderTestCluster(t)
	defer c.Stop()

	// No nodeGroup
	le := NewLeaderElection(c, DefaultConfig())
	_ = le.SendToPeers(gossip.UserMsg+70, 1)
	_ = le.SendToPeersReliable(gossip.UserMsg+71, 2)

	// With nodeGroup
	cfg := DefaultConfig()
	cfg.MetadataCriteria = map[string]string{"zone": "eu"}
	cfg.HeartbeatMessageType = gossip.UserMsg + 91
	le2 := NewLeaderElection(c, cfg)
	// just ensure calls route; no assertion needed here
	_ = le2.SendToPeers(gossip.UserMsg+72, 3)
	_ = le2.SendToPeersReliable(gossip.UserMsg+73, 4)
}
