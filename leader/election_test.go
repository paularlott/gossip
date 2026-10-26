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

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.LeaderCheckInterval != time.Second || config.LeaderTimeout != 3*time.Second || config.MetadataCriteria != nil {
		t.Fatalf("unexpected defaults: %+v", config)
	}
}

func TestNormalizeConfig(t *testing.T) {
	defaults := DefaultConfig()

	t.Run("replaces invalid values with the defaults", func(t *testing.T) {
		normalized := normalizeConfig(&Config{
			LeaderCheckInterval:  0,
			LeaderTimeout:        -time.Second,
			HeartbeatMessageType: gossip.MessageType(1),
		})

		if normalized.LeaderCheckInterval != defaults.LeaderCheckInterval {
			t.Fatalf("expected LeaderCheckInterval %v, got %v", defaults.LeaderCheckInterval, normalized.LeaderCheckInterval)
		}
		if normalized.LeaderTimeout != defaults.LeaderTimeout {
			t.Fatalf("expected LeaderTimeout %v, got %v", defaults.LeaderTimeout, normalized.LeaderTimeout)
		}
		if normalized.HeartbeatMessageType != defaults.HeartbeatMessageType {
			t.Fatalf("expected HeartbeatMessageType %d, got %d", defaults.HeartbeatMessageType, normalized.HeartbeatMessageType)
		}
		if normalized.ForgetMessageType != defaults.ForgetMessageType {
			t.Fatalf("expected ForgetMessageType %d, got %d", defaults.ForgetMessageType, normalized.ForgetMessageType)
		}
	})

	t.Run("raises the timeout to the check interval", func(t *testing.T) {
		normalized := normalizeConfig(&Config{
			LeaderCheckInterval:  2 * time.Second,
			LeaderTimeout:        time.Second,
			HeartbeatMessageType: gossip.ReservedMsgsStart + 5,
		})
		if normalized.LeaderTimeout != normalized.LeaderCheckInterval {
			t.Fatalf("expected timeout %v, got %v", normalized.LeaderCheckInterval, normalized.LeaderTimeout)
		}
	})

	t.Run("separates the forget type from the heartbeat type", func(t *testing.T) {
		normalized := normalizeConfig(&Config{
			HeartbeatMessageType: gossip.ReservedMsgsStart + 5,
			ForgetMessageType:    gossip.ReservedMsgsStart + 5,
		})
		if normalized.ForgetMessageType != gossip.ReservedMsgsStart+6 {
			t.Fatalf("forget type %d, want %d", normalized.ForgetMessageType, gossip.ReservedMsgsStart+6)
		}
	})

	t.Run("keeps MinClusterSize", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MinClusterSize = 7
		if got := normalizeConfig(cfg).MinClusterSize; got != 7 {
			t.Errorf("MinClusterSize should survive normalisation, got %d", got)
		}
	})
}

func TestLeaderElectionStartIdempotent(t *testing.T) {
	transport := &countingTransport{}
	cluster := newLeaderTestCluster(t, transport)
	cluster.Start()
	defer cluster.Stop()

	config := DefaultConfig()
	config.LeaderCheckInterval = 10 * time.Millisecond
	config.LeaderTimeout = 100 * time.Millisecond
	config.HeartbeatMessageType = gossip.ReservedMsgsStart + 50

	election := NewLeaderElection(cluster, config)
	defer election.Stop()

	start := time.Now()
	election.Start()
	election.Start()
	time.Sleep(100 * time.Millisecond)

	// One loop sends at most one heartbeat per tick, plus the one from Start;
	// a second loop would double that. Measured against the elapsed time, so
	// a stalled scheduler can only lower the count.
	limit := int64(time.Since(start)/config.LeaderCheckInterval) + 2
	if sends := transport.sendCount.Load(); sends > limit {
		t.Fatalf("expected a single election loop worth of heartbeats (at most %d), got %d sends", limit, sends)
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

// TestHeartbeatConcurrentWithLeaderLoss is a regression test for a data race: handleLeaderHeartbeat read
// isLeader/currentTerm after applyState released the lock, while handleNodeStateChange cleared the leader state
// under the lock. Run with -race.
func TestHeartbeatConcurrentWithLeaderLoss(t *testing.T) {
	cluster := newLeaderTestCluster(t, &countingTransport{})
	config := DefaultConfig()
	config.HeartbeatMessageType = gossip.ReservedMsgsStart + 53
	config.ForgetMessageType = gossip.ReservedMsgsStart + 54
	election := NewLeaderElection(cluster, config)
	election.Start()
	defer election.Stop()

	// A remote leader that is not alive: a zero-value node's observed state is NodeUnknown.
	remote := &gossip.Node{ID: gossip.NodeID(uuid.New())}
	jc := codec.NewJSONCodec()

	const rounds = 500
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range rounds {
			payload, err := jc.Marshal(heartbeatMessage{LeaderTime: time.Now(), Term: uint64(i + 1)})
			if err != nil {
				t.Error(err)
				return
			}
			p := gossip.NewPacket()
			p.SetCodec(jc)
			p.SetPayload(payload)
			if err := election.handleLeaderHeartbeat(remote, p); err != nil {
				t.Error(err)
			}
			p.Release()
		}
	}()
	go func() {
		defer wg.Done()
		for range rounds {
			election.handleNodeStateChange(remote, gossip.NodeAlive)
		}
	}()
	wg.Wait()

	if election.IsLeader() {
		t.Fatal("the local node must never become leader from a remote node's heartbeat")
	}
}

// Eligibility of a departure is decided by the election, not the tracker: the
// tracker discounts whatever it is told, exactly once. This pins the decision
// the election makes.
func TestWasEligibleForBaseline(t *testing.T) {
	worker := gossip.NewTestNode(gossip.NodeID(uuid.New()), "127.0.0.1:1", map[string]string{"role": "worker"})
	bystander := gossip.NewTestNode(gossip.NodeID(uuid.New()), "127.0.0.1:2", map[string]string{"role": "bystander"})

	scoped := &LeaderElection{config: &Config{MetadataCriteria: map[string]string{"role": "worker"}}}
	clusterWide := &LeaderElection{config: &Config{}}

	cases := []struct {
		name string
		le   *LeaderElection
		node *gossip.Node
		prev gossip.NodeState
		want bool
	}{
		{"scoped worker from alive", scoped, worker, gossip.NodeAlive, true},
		{"scoped worker from suspect", scoped, worker, gossip.NodeSuspect, true},
		{"scoped bystander is not ours", scoped, bystander, gossip.NodeAlive, false},
		{"scoped worker already dead", scoped, worker, gossip.NodeDead, false},
		{"cluster-wide alive node", clusterWide, bystander, gossip.NodeAlive, true},
		{"cluster-wide suspect node", clusterWide, bystander, gossip.NodeSuspect, true},
		{"cluster-wide dead then leaving", clusterWide, worker, gossip.NodeDead, false},
		{"cluster-wide unknown then leaving", clusterWide, worker, gossip.NodeUnknown, false},
	}

	for _, tc := range cases {
		if got := tc.le.wasEligibleForBaseline(tc.node, tc.prev); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// WatchLeadership must fire on every leadership mutation a consumer like the
// lock pool cares about, including ones the public events miss.
func TestWatchLeadershipNotifications(t *testing.T) {
	le := &LeaderElection{}

	var mu sync.Mutex
	var got []struct {
		isLeader bool
		term     uint64
	}
	le.WatchLeadership(func(isLeader bool, term uint64) {
		mu.Lock()
		got = append(got, struct {
			isLeader bool
			term     uint64
		}{isLeader, term})
		mu.Unlock()
	})

	// Registration reports current state immediately.
	if len(got) != 1 || got[0].isLeader || got[0].term != 0 {
		t.Fatalf("registration must report the current state, got %+v", got)
	}

	// A term advance with the same leader fires no public event but must
	// notify: another node may have led in between.
	le.lock.Lock()
	le.currentTerm = 7
	le.lock.Unlock()
	le.notifyLeadershipWatchers()

	mu.Lock()
	last := got[len(got)-1]
	mu.Unlock()
	if last.term != 7 {
		t.Errorf("term change must notify watchers, last notification %+v", last)
	}

	// The callback may call back into the election: notify must not hold the
	// lock.
	le.WatchLeadership(func(isLeader bool, term uint64) {
		if le.IsLeader() || le.Term() != term {
			t.Errorf("watcher callback could not read election state: isLeader=%v term=%d (want %d)", isLeader, le.Term(), term)
		}
	})
	le.notifyLeadershipWatchers()
}
