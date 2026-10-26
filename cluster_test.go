package gossip

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClusterValidation(t *testing.T) {
	_, err := NewCluster(nil)
	require.Error(t, err, "a nil config has no transport")

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(c *Config) {}, false},
		{"nil codec", func(c *Config) { c.MsgCodec = nil }, true},
		{"nil transport", func(c *Config) { c.Transport = nil }, true},
		{"short key", func(c *Config) { c.EncryptionKey = []byte("short") }, true},
		{"16 byte key", func(c *Config) { c.EncryptionKey = make([]byte, 16) }, false},
		{"24 byte key", func(c *Config) { c.EncryptionKey = make([]byte, 24) }, false},
		{"32 byte key", func(c *Config) { c.EncryptionKey = make([]byte, 32) }, false},
		{"history shards not a power of two", func(c *Config) { c.MsgHistoryShardCount = 3 }, true},
		{"node shards not a power of two", func(c *Config) { c.NodeShardCount = 3 }, true},
		{"cipher without key", func(c *Config) { c.Cipher = &mockCipher{} }, true},
		{"invalid node id", func(c *Config) { c.NodeID = "not-a-uuid" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			config.Transport = &mockTransport{}
			config.MsgCodec = codec.NewJSONCodec()
			tt.mutate(config)
			c, err := NewCluster(config)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			stopQuietly(c)
		})
	}

	id := uuid.New().String()
	c := newUnitCluster(t, func(c *Config) { c.NodeID = id })
	assert.Equal(t, id, c.localNode.ID.String(), "the local node takes the configured id")
}

func TestClusterAccessors(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) { c.Tags = []string{"web", "api"} })

	assert.NotNil(t, c.LocalNode())
	assert.NotNil(t, c.LocalMetadata())
	assert.NotNil(t, c.Logger())
	assert.Equal(t, 1, c.NumNodes(), "the local node")
	assert.True(t, c.NodeIsLocal(c.localNode))

	alive := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	tagged := newNode(NodeID(uuid.New()), "127.0.0.1:8002", "web")
	suspect := newNode(NodeID(uuid.New()), "127.0.0.1:8003", "web")
	suspect.observedState = NodeSuspect
	dead := newNode(NodeID(uuid.New()), "127.0.0.1:8004", "web")
	dead.observedState = NodeDead
	for _, n := range []*Node{alive, tagged, suspect, dead} {
		c.nodes.addOrUpdate(n)
	}

	assert.Len(t, c.Nodes(), 5)
	assert.Len(t, c.AliveNodes(), 3, "local, alive and tagged")
	assert.Len(t, c.RoutableNodes(), 4, "alive and suspect")
	assert.Equal(t, 5, c.NumNodes())
	assert.Equal(t, 3, c.NumAliveNodes())
	assert.Equal(t, 1, c.NumSuspectNodes())
	assert.Equal(t, 1, c.NumDeadNodes())
	assert.False(t, c.NodeIsLocal(alive))

	assert.Same(t, alive, c.GetNode(alive.ID))
	assert.Same(t, alive, c.GetNodeByIDString(alive.ID.String()))
	assert.Nil(t, c.GetNodeByIDString("invalid-uuid"))
	assert.Nil(t, c.GetNode(NodeID(uuid.New())))

	assert.ElementsMatch(t, []*Node{c.localNode, tagged, suspect}, c.GetNodesByTag("web"), "alive and suspect nodes with the tag")

	candidates := c.GetCandidates()
	assert.NotEmpty(t, candidates)
	for _, n := range candidates {
		assert.NotEqual(t, c.localNode.ID, n.ID, "the local node is never a candidate")
		assert.True(t, n.Routable())
	}

	assert.Equal(t, []NodeID{alive.ID, tagged.ID}, c.NodesToIDs([]*Node{alive, tagged}))
}

func TestClusterCalc(t *testing.T) {
	// Expected values follow cluster.go: fan out ceil(log2 n * FanOutMultiplier) within 3..10 (at most n),
	// payload ceil(log2 n * StateExchangeMultiplier)+2 within 3..16 (at most n), TTL ceil(log2 n * TTLMultiplier)+2 up to 8.
	tests := []struct {
		nodes, fanOut, payload int
		ttl                    uint8
	}{
		{1, 1, 1, 2},
		{2, 2, 2, 3},
		{10, 4, 5, 6},
		{100, 7, 8, 8},
		{2000, 10, 11, 8},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d nodes", tt.nodes), func(t *testing.T) {
			c := newUnitCluster(t)
			addPeers(c, tt.nodes-1)
			assert.Equal(t, tt.fanOut, c.CalcFanOut())
			assert.Equal(t, tt.payload, c.CalcPayloadSize(tt.nodes))
			assert.Equal(t, tt.ttl, c.getMaxTTL())
		})
	}

	c := newUnitCluster(t)
	assert.Zero(t, c.CalcPayloadSize(0))
	assert.Zero(t, c.CalcPayloadSize(-1))

	// Suspect nodes still count for the fan out, dead ones do not.
	peers := addPeers(c, 20)
	c.nodes.updateState(peers[0].ID, NodeSuspect)
	c.nodes.updateState(peers[1].ID, NodeDead)
	assert.Equal(t, 5, c.CalcFanOut(), "20 nodes")
}

// Every send function queues one packet that carries what the function promises, and refuses the reserved types.
func TestSendValidation(t *testing.T) {
	dst := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	tests := []struct {
		name      string
		send      func(c *Cluster, msgType MessageType) error
		transport TransportType
		tag       string
		targeted  bool
	}{
		{"Send", func(c *Cluster, m MessageType) error { return c.Send(m, "x") }, TransportBestEffort, "", false},
		{"SendReliable", func(c *Cluster, m MessageType) error { return c.SendReliable(m, "x") }, TransportReliable, "", false},
		{"SendTagged", func(c *Cluster, m MessageType) error { return c.SendTagged("web", m, "x") }, TransportBestEffort, "web", false},
		{"SendTaggedReliable", func(c *Cluster, m MessageType) error { return c.SendTaggedReliable("web", m, "x") }, TransportReliable, "web", false},
		{"SendTo", func(c *Cluster, m MessageType) error { return c.SendTo(dst, m, "x") }, TransportBestEffort, "", true},
		{"SendToReliable", func(c *Cluster, m MessageType) error { return c.SendToReliable(dst, m, "x") }, TransportReliable, "", true},
		{"SendToPeers", func(c *Cluster, m MessageType) error { return c.SendToPeers([]*Node{dst}, m, "x") }, TransportBestEffort, "", false},
		{"SendToPeersReliable", func(c *Cluster, m MessageType) error { return c.SendToPeersReliable([]*Node{dst}, m, "x") }, TransportReliable, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newUnitCluster(t)

			require.Error(t, tt.send(c, 0), "a reserved message type")
			require.Error(t, tt.send(c, ReservedMsgsStart-1))
			assert.Empty(t, queuedBroadcasts(c), "nothing is queued for a reserved type")

			require.NoError(t, tt.send(c, UserMsg))
			items := queuedBroadcasts(c)
			require.Len(t, items, 1)
			p := items[0].packet
			defer p.Release()
			assert.Equal(t, UserMsg, p.MessageType)
			assert.Equal(t, c.localNode.ID, p.SenderID)
			assert.Equal(t, tt.transport, items[0].transportType)
			if tt.tag == "" {
				assert.Nil(t, p.Tag)
			} else {
				require.NotNil(t, p.Tag)
				assert.Equal(t, tt.tag, *p.Tag)
			}
			if tt.targeted {
				require.NotNil(t, p.TargetNodeID)
				assert.Equal(t, dst.ID, *p.TargetNodeID)
			} else {
				assert.Nil(t, p.TargetNodeID)
			}
		})
	}

	c := newUnitCluster(t)
	var reply string
	require.Error(t, c.SendToWithResponse(dst, 0, "x", &reply), "a reserved message type")
}

// A transport that answers with nothing is an error for the caller, not a panic.
func TestSendToWithResponseNilResponse(t *testing.T) {
	c := newUnitCluster(t)
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	c.nodes.addOrUpdate(node)

	var response struct{}
	require.Error(t, c.sendToWithResponse(node, pushPullStateMsg, "test", &response))
}

func TestJoinValidation(t *testing.T) {
	tests := []struct {
		name      string
		advertise string
		peers     []string
		wantSends int // join requests sent to the peers that are not this node
	}{
		{"no peers", "127.0.0.1:8000", []string{}, 0},
		{"only itself", "127.0.0.1:8000", []string{"127.0.0.1:8000"}, 0},
		{"itself and another", "127.0.0.1:8000", []string{"127.0.0.1:8000", "127.0.0.1:8001"}, 1},
		{"http itself", "http://localhost:8080/gossip", []string{"http://localhost:8080/gossip"}, 0},
		{"http another host", "http://localhost:8080/gossip", []string{"http://otherhost:8080/gossip"}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newUnitCluster(t, func(c *Config) {
				c.BindAddr, c.AdvertiseAddr = tt.advertise, tt.advertise
			})
			err := c.Join(tt.peers)
			if tt.wantSends == 0 {
				require.Error(t, err, "no peers to join")
			} else {
				require.NoError(t, err)
			}
			sends := unitTransport(c).sends()
			require.Len(t, sends, tt.wantSends)
			for _, s := range sends {
				assert.True(t, s.reply, "a join waits for the peer's reply")
				assert.Equal(t, nodeJoinMsg, s.msgType)
				assert.NotEqual(t, tt.advertise, s.node.AdvertisedAddr())
			}
			assert.Len(t, c.seedPeers, tt.wantSends, "the peers are remembered for recovery")
		})
	}
}

func TestLeave(t *testing.T) {
	t.Run("announced to the cluster", func(t *testing.T) {
		c := newUnitCluster(t)
		addPeers(c, 1)
		c.Leave()

		assert.Equal(t, NodeLeaving, c.localNode.GetObservedState())
		items := queuedBroadcasts(c)
		require.Len(t, items, 1)
		assert.Equal(t, nodeLeaveMsg, items[0].packet.MessageType)
		assert.Equal(t, c.localNode.ID, items[0].packet.SenderID)
		items[0].packet.Release()
	})

	t.Run("alone there is nobody to tell", func(t *testing.T) {
		c := newUnitCluster(t)
		c.Leave()
		assert.Equal(t, NodeLeaving, c.localNode.GetObservedState())
		assert.Empty(t, queuedBroadcasts(c))
	})
}

// Stop must not wait for the background managers' start-up delays (peer recovery waits 3-5 s before its first retry).
func TestClusterStopDuringJitter(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) {
		c.GossipInterval = time.Second
		c.MetadataGossipInterval = time.Second
		c.StateGossipInterval = 2 * time.Second
		c.NodeCleanupInterval = time.Second
		c.PeerRecoveryInterval = 2 * time.Second
		c.HealthCheckInterval = time.Second
		c.SuspectRetryInterval = time.Second
		c.DeadNodeRetryInterval = time.Second
	})
	c.Start()

	done := make(chan struct{})
	go func() {
		stopQuietly(c)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("cluster stop waited for the manager jitter sleeps")
	}
}

func TestCreatePacketWithTargetAndTag(t *testing.T) {
	c := newUnitCluster(t)
	targetID := NodeID(uuid.New())
	tag := "web"

	p, err := c.createPacketWithTargetAndTag(c.localNode.ID, &targetID, &tag, UserMsg, 5, map[string]string{"data": "test"})
	require.NoError(t, err)
	defer p.Release()

	assert.Equal(t, UserMsg, p.MessageType)
	assert.Equal(t, c.localNode.ID, p.SenderID)
	require.NotNil(t, p.TargetNodeID)
	assert.Equal(t, targetID, *p.TargetNodeID)
	require.NotNil(t, p.Tag)
	assert.Equal(t, tag, *p.Tag)
	assert.Equal(t, uint8(5), p.TTL)
	var got map[string]string
	require.NoError(t, p.Unmarshal(&got))
	assert.Equal(t, map[string]string{"data": "test"}, got)
}

func TestCreatePacketMarshalError(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) { c.MsgCodec = &failCodec{} })
	_, err := c.createPacketWithTarget(c.localNode.ID, nil, UserMsg, 5, make(chan int))
	require.Error(t, err)
}

func TestClusterHandlerRegistration(t *testing.T) {
	noop := func(*Node, *Packet) error { return nil }
	reply := func(*Node, *Packet) (interface{}, error) { return nil, nil }
	deferred := func(*Node, *Packet, Replier) {}
	tests := []struct {
		name     string
		register func(c *Cluster, m MessageType) error
		check    func(t *testing.T, h *msgHandler)
	}{
		{"HandleFunc", func(c *Cluster, m MessageType) error { return c.HandleFunc(m, noop) },
			func(t *testing.T, h *msgHandler) { assert.NotNil(t, h.handler) }},
		{"HandleFuncWithReply", func(c *Cluster, m MessageType) error { return c.HandleFuncWithReply(m, reply) },
			func(t *testing.T, h *msgHandler) { assert.NotNil(t, h.replyHandler) }},
		{"HandleFuncWithResponse", func(c *Cluster, m MessageType) error { return c.HandleFuncWithResponse(m, reply) },
			func(t *testing.T, h *msgHandler) { assert.NotNil(t, h.replyHandler) }},
		{"HandleFuncWithDeferredReply", func(c *Cluster, m MessageType) error { return c.HandleFuncWithDeferredReply(m, deferred) },
			func(t *testing.T, h *msgHandler) { assert.NotNil(t, h.deferredHandler) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newUnitCluster(t)
			for _, reserved := range []MessageType{0, nodeJoinMsg, ReservedMsgsStart - 1} {
				require.Error(t, tt.register(c, reserved), "message type %d is reserved", reserved)
				assert.Nil(t, c.handlers.getHandler(reserved))
			}

			require.NoError(t, tt.register(c, UserMsg))
			h := c.handlers.getHandler(UserMsg)
			require.NotNil(t, h)
			tt.check(t, h)

			assert.True(t, c.UnregisterMessageType(UserMsg))
			assert.False(t, c.UnregisterMessageType(UserMsg), "already unregistered")
			assert.Nil(t, c.handlers.getHandler(UserMsg))
		})
	}

	c := newUnitCluster(t)
	assert.False(t, c.UnregisterMessageType(0), "a reserved type cannot be unregistered")
	assert.False(t, c.UnregisterMessageType(nodeJoinMsg))
}

// The event handlers run when the node list changes, on the goroutine making the change.
func TestClusterEventHandlers(t *testing.T) {
	c := newUnitCluster(t)

	var mu sync.Mutex
	var states []NodeState
	stateID := c.HandleNodeStateChangeFunc(func(n *Node, prev NodeState) {
		mu.Lock()
		states = append(states, prev)
		mu.Unlock()
	})
	var changed []NodeID
	metaID := c.HandleNodeMetadataChangeFunc(func(n *Node) {
		mu.Lock()
		changed = append(changed, n.ID)
		mu.Unlock()
	})
	gossips := 0
	gossipID := c.HandleGossipFunc(func() { gossips++ })

	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	c.nodes.addOrUpdate(node)
	c.nodes.updateState(node.ID, NodeSuspect)
	node.metadata.set("key", "value")
	c.nodes.notifyMetadataChanged(node)
	c.gossipEventHandlers.ForEach(func(h GossipHandler) { h() })

	mu.Lock()
	assert.Equal(t, []NodeState{NodeUnknown, NodeAlive}, states, "added, then suspected")
	assert.Equal(t, []NodeID{node.ID}, changed)
	mu.Unlock()
	assert.Equal(t, 1, gossips)

	assert.True(t, c.RemoveNodeStateChangeHandler(stateID))
	assert.True(t, c.RemoveNodeMetadataChangeHandler(metaID))
	assert.True(t, c.RemoveGossipHandler(gossipID))
	assert.False(t, c.RemoveGossipHandler(gossipID), "already removed")

	c.nodes.updateState(node.ID, NodeAlive)
	mu.Lock()
	assert.Len(t, states, 2, "a removed handler is not called")
	mu.Unlock()
}

// A race smoke test: sending from many goroutines while the node list and the local metadata change.
func TestClusterConcurrentUse(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) { c.SendQueueSize = 4096 })
	addPeers(c, 3)

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 50 {
				assert.NoError(t, c.Send(UserMsg, fmt.Sprintf("message-%d-%d", i, j)))
				c.GetCandidates()
				c.NumNodes()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := range 50 {
			key := fmt.Sprintf("key-%d", j)
			c.LocalMetadata().SetString(key, "value")
			c.LocalMetadata().Delete(key)
		}
	}()
	wg.Wait()
	assert.Empty(t, c.LocalMetadata().GetAllKeys())
}
