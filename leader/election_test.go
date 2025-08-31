package leader

import (
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
)

func newLeaderTestCluster(t *testing.T) *gossip.Cluster {
	t.Helper()
	cfg := gossip.DefaultConfig()
	cfg.SocketTransportEnabled = false
	cfg.WebsocketProvider = nil
	cfg.MsgCodec = codec.NewJsonCodec()
	cfg.GossipInterval = 100 * time.Millisecond
	cfg.HealthCheckInterval = 200 * time.Millisecond
	cfg.StateSyncInterval = 200 * time.Millisecond

	c, err := gossip.NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	c.Start()
	return c
}

func TestLeader_SingleNodeElection(t *testing.T) {
	c := newLeaderTestCluster(t)
	defer c.Stop()

	cfg := DefaultConfig()
	cfg.LeaderCheckInterval = 20 * time.Millisecond
	cfg.LeaderTimeout = 150 * time.Millisecond
	cfg.HeartbeatMessageType = gossip.UserMsg + 10

	le := NewLeaderElection(c, cfg)
	le.Start()
	defer le.Stop()

	time.Sleep(80 * time.Millisecond)

	if !le.HasLeader() {
		t.Fatalf("expected a leader to be elected")
	}
	if !le.IsLeader() {
		t.Fatalf("local node should be leader in single-node cluster")
	}
}

func TestLeader_TimeoutAfterStop(t *testing.T) {
	c := newLeaderTestCluster(t)
	defer c.Stop()

	cfg := DefaultConfig()
	cfg.LeaderCheckInterval = 20 * time.Millisecond
	cfg.LeaderTimeout = 100 * time.Millisecond
	cfg.HeartbeatMessageType = gossip.UserMsg + 11

	le := NewLeaderElection(c, cfg)
	le.Start()

	time.Sleep(80 * time.Millisecond)
	if !le.HasLeader() {
		t.Fatalf("expected leader before stop")
	}

	le.Stop()
	// Wait past timeout so heartbeats lapse
	time.Sleep(120 * time.Millisecond)

	if le.HasLeader() {
		t.Fatalf("expected no leader after timeout once stopped")
	}
}

func TestLeader_MetadataFilteringParticipation(t *testing.T) {
	c := newLeaderTestCluster(t)
	defer c.Stop()

	cfg := DefaultConfig()
	cfg.LeaderCheckInterval = 20 * time.Millisecond
	cfg.LeaderTimeout = 120 * time.Millisecond
	cfg.HeartbeatMessageType = gossip.UserMsg + 12
	cfg.MetadataCriteria = map[string]string{"role": "db"}

	// Match initially
	c.LocalMetadata().SetString("role", "db")

	le := NewLeaderElection(c, cfg)
	le.Start()
	defer le.Stop()

	time.Sleep(80 * time.Millisecond)
	if !le.IsLeader() {
		t.Fatalf("expected local node to be leader when matching criteria")
	}

	// Now change metadata so local node is no longer eligible
	c.LocalMetadata().SetString("role", "app")

	// Wait enough for check + timeout to clear leader
	time.Sleep(160 * time.Millisecond)

	if le.IsLeader() {
		t.Fatalf("expected local node to step down when not matching criteria")
	}
	if le.HasLeader() {
		// With no eligible nodes, we should eventually report no leader
		// (quorum not met and heartbeat timeout)
		t.Fatalf("expected no leader after losing eligibility and timeout")
	}
}
