package leader

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/logger"
)

type countingTransport struct {
	sendCount atomic.Int64
	ch        chan *gossip.Packet
}

func (t *countingTransport) Name() string { return "counting" }

func (t *countingTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	t.ch = make(chan *gossip.Packet, 16)
	return nil
}

func (t *countingTransport) PacketChannel() chan *gossip.Packet {
	if t.ch == nil {
		t.ch = make(chan *gossip.Packet, 16)
	}
	return t.ch
}

func (t *countingTransport) Send(transportType gossip.TransportType, node *gossip.Node, packet *gossip.Packet) error {
	t.sendCount.Add(1)
	return nil
}

func (t *countingTransport) SendWithReply(node *gossip.Node, packet *gossip.Packet) (*gossip.Packet, error) {
	t.sendCount.Add(1)
	return nil, nil
}

func newLeaderTestCluster(t *testing.T, transport gossip.Transport) *gossip.Cluster {
	t.Helper()

	config := gossip.DefaultConfig()
	config.NodeID = uuid.New().String()
	config.Transport = transport
	config.MsgCodec = codec.NewJSONCodec()
	config.Logger = logger.NewNullLogger()

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	return cluster
}

func TestNormalizeConfig(t *testing.T) {
	cfg := &Config{
		LeaderCheckInterval:  0,
		LeaderTimeout:        -time.Second,
		HeartbeatMessageType: gossip.MessageType(1),
		QuorumPercentage:     101,
	}

	normalized := normalizeConfig(cfg)
	defaults := DefaultConfig()

	if normalized.LeaderCheckInterval != defaults.LeaderCheckInterval {
		t.Fatalf("expected LeaderCheckInterval %v, got %v", defaults.LeaderCheckInterval, normalized.LeaderCheckInterval)
	}
	if normalized.LeaderTimeout != defaults.LeaderTimeout {
		t.Fatalf("expected LeaderTimeout %v, got %v", defaults.LeaderTimeout, normalized.LeaderTimeout)
	}
	if normalized.HeartbeatMessageType != defaults.HeartbeatMessageType {
		t.Fatalf("expected HeartbeatMessageType %d, got %d", defaults.HeartbeatMessageType, normalized.HeartbeatMessageType)
	}
	if normalized.QuorumPercentage != defaults.QuorumPercentage {
		t.Fatalf("expected QuorumPercentage %d, got %d", defaults.QuorumPercentage, normalized.QuorumPercentage)
	}
}

func TestNormalizeConfigTimeoutAtLeastCheckInterval(t *testing.T) {
	cfg := &Config{
		LeaderCheckInterval:  2 * time.Second,
		LeaderTimeout:        time.Second,
		HeartbeatMessageType: gossip.ReservedMsgsStart + 5,
		QuorumPercentage:     75,
	}

	normalized := normalizeConfig(cfg)
	if normalized.LeaderTimeout != normalized.LeaderCheckInterval {
		t.Fatalf("expected timeout %v, got %v", normalized.LeaderCheckInterval, normalized.LeaderTimeout)
	}
}

func TestLeaderEventHandlersConcurrentAddNoLoss(t *testing.T) {
	handlers := newLeaderEventHandlers(logger.NewNullLogger())

	var count atomic.Int64
	var wg sync.WaitGroup
	const numHandlers = 64

	for i := 0; i < numHandlers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handlers.add(BecameLeaderEvent, func(EventType, gossip.NodeID) {
				count.Add(1)
			})
		}()
	}
	wg.Wait()

	handlers.dispatch(BecameLeaderEvent, testNodeID)

	if got := count.Load(); got != numHandlers {
		t.Fatalf("expected %d handlers to run, got %d", numHandlers, got)
	}
}

func TestLeaderElectionStartIdempotent(t *testing.T) {
	transport := &countingTransport{}
	cluster := newLeaderTestCluster(t, transport)
	cluster.Start()
	defer cluster.Stop()

	config := DefaultConfig()
	config.LeaderCheckInterval = 20 * time.Millisecond
	config.LeaderTimeout = 100 * time.Millisecond
	config.HeartbeatMessageType = gossip.ReservedMsgsStart + 50

	election := NewLeaderElection(cluster, config)
	defer election.Stop()

	election.Start()
	election.Start()

	time.Sleep(75 * time.Millisecond)

	sends := transport.sendCount.Load()
	if sends > 6 {
		t.Fatalf("expected a single election loop worth of heartbeats, got %d sends", sends)
	}
}

func TestLeaderElectionStopUnregistersHandlers(t *testing.T) {
	cluster := newLeaderTestCluster(t, &countingTransport{})
	config := DefaultConfig()
	config.HeartbeatMessageType = gossip.ReservedMsgsStart + 51

	election := NewLeaderElection(cluster, config)
	election.Stop()
	election.Stop()

	if cluster.UnregisterMessageType(config.HeartbeatMessageType) {
		t.Fatal("heartbeat message type should already be unregistered after Stop")
	}
}

func TestLeaderElectionCanBeRecreatedAfterStop(t *testing.T) {
	cluster := newLeaderTestCluster(t, &countingTransport{})
	config := DefaultConfig()
	config.HeartbeatMessageType = gossip.ReservedMsgsStart + 53

	election1 := NewLeaderElection(cluster, config)
	election1.Start()
	election1.Stop()

	election2 := NewLeaderElection(cluster, config)
	election2.Start()
	election2.Stop()
}

func TestLeaderElectionHandlerIgnoresCallbacksAfterStop(t *testing.T) {
	cluster := newLeaderTestCluster(t, &countingTransport{})

	config := DefaultConfig()
	config.HeartbeatMessageType = gossip.ReservedMsgsStart + 52

	election := NewLeaderElection(cluster, config)
	election.Start()
	election.lock.Lock()
	election.hasLeader = false
	election.leaderID = gossip.EmptyNodeID
	election.currentTerm = 0
	election.lock.Unlock()
	election.Stop()

	packet := gossip.NewPacket()
	defer packet.Release()
	packet.SetCodec(codec.NewJSONCodec())
	payload, err := codec.NewJSONCodec().Marshal(heartbeatMessage{
		LeaderTime: time.Now(),
		Term:       99,
	})
	if err != nil {
		t.Fatalf("failed to marshal heartbeat: %v", err)
	}
	packet.SetPayload(payload)

	if err := election.handleLeaderHeartbeat(cluster.LocalNode(), packet); err != nil {
		t.Fatalf("unexpected heartbeat error: %v", err)
	}

	if election.HasLeader() {
		t.Fatal("stopped election should ignore heartbeats")
	}
}
