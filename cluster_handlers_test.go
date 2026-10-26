package gossip

import (
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The system message handlers, with each wire codec.
func TestClusterHandlers(t *testing.T) {
	garbage := []byte{0x91, 0x00, 0xFF, 0xFE, 'n', 'o', 't', '-', 'v', 'a', 'l', 'i', 'd'}
	joinFrom := func(id NodeID, mod func(*joinMessage)) *joinMessage {
		m := &joinMessage{
			ID: id, AdvertiseAddr: "127.0.0.1:8001", State: NodeAlive, MetadataTimestamp: hlc.Now(),
			Metadata: map[string]interface{}{"region": "us-east", "priority": int64(5)}, ProtocolVersion: PROTOCOL_VERSION,
		}
		if mod != nil {
			mod(m)
		}
		return m
	}

	tests := []struct {
		name string
		opts []func(*Config)
		run  func(t *testing.T, c *Cluster)
	}{
		{name: "join adds the joiner and tells it about the others", run: func(t *testing.T, c *Cluster) {
			peers := addPeers(c, 2)
			id := NodeID(uuid.New())
			p := requestPacket(t, c, nodeJoinMsg, id, joinFrom(id, func(m *joinMessage) { m.Tags = []string{"api", "eu"} }))

			reply, err := c.handleJoin(nil, p)
			require.NoError(t, err)

			r := reply.(*joinReplyMessage)
			assert.True(t, r.Accepted, r.RejectReason)
			assert.Equal(t, c.localNode.ID, r.NodeID)
			assert.Equal(t, c.localNode.AdvertisedAddr(), r.AdvertiseAddr)
			var told []NodeID
			for _, n := range r.Nodes {
				told = append(told, n.ID)
			}
			assert.ElementsMatch(t, []NodeID{peers[0].ID, peers[1].ID}, told, "every other alive node, not the joiner or us")

			joined := c.GetNode(id)
			require.NotNil(t, joined)
			assert.Equal(t, "127.0.0.1:8001", joined.AdvertisedAddr())
			assert.ElementsMatch(t, []string{"api", "eu"}, joined.GetTags())
			assert.Equal(t, "us-east", joined.metadata.GetString("region"))
			assert.EqualValues(t, 5, joined.metadata.GetInt64("priority"))
		}},
		{name: "join with a protocol version we do not speak is refused", run: func(t *testing.T, c *Cluster) {
			id := NodeID(uuid.New())
			p := requestPacket(t, c, nodeJoinMsg, id, joinFrom(id, func(m *joinMessage) { m.ProtocolVersion++ }))

			reply, err := c.handleJoin(nil, p)
			require.NoError(t, err)

			r := reply.(*joinReplyMessage)
			assert.False(t, r.Accepted)
			assert.Contains(t, r.RejectReason, "protocol")
			assert.Nil(t, c.GetNode(id))
			assert.Empty(t, r.Nodes, "a refused joiner learns nothing about the cluster")
		}},
		{name: "join refused by the application version check", opts: []func(*Config){func(c *Config) {
			c.ApplicationVersion = "2.0.0"
			c.ApplicationVersionCheck = func(version string) bool { return version == "2.0.0" }
		}}, run: func(t *testing.T, c *Cluster) {
			id := NodeID(uuid.New())
			p := requestPacket(t, c, nodeJoinMsg, id, joinFrom(id, func(m *joinMessage) { m.ApplicationVersion = "1.0.0" }))
			reply, err := c.handleJoin(nil, p)
			require.NoError(t, err)
			assert.False(t, reply.(*joinReplyMessage).Accepted)
			assert.Nil(t, c.GetNode(id))

			id2 := NodeID(uuid.New())
			p = requestPacket(t, c, nodeJoinMsg, id2, joinFrom(id2, func(m *joinMessage) { m.ApplicationVersion = "2.0.0" }))
			reply, err = c.handleJoin(nil, p)
			require.NoError(t, err)
			assert.True(t, reply.(*joinReplyMessage).Accepted, "the version the check accepts")
			assert.NotNil(t, c.GetNode(id2))
		}},
		{name: "join from ourselves is refused", run: func(t *testing.T, c *Cluster) {
			p := requestPacket(t, c, nodeJoinMsg, c.localNode.ID, joinFrom(c.localNode.ID, nil))
			reply, err := c.handleJoin(nil, p)
			require.NoError(t, err)
			r := reply.(*joinReplyMessage)
			assert.False(t, r.Accepted)
			assert.Contains(t, r.RejectReason, "self")
			assert.Equal(t, 1, c.NumNodes())
		}},
		{name: "join from a node held dead makes it alive again", run: func(t *testing.T, c *Cluster) {
			dead := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
			dead.observedState = NodeDead
			c.nodes.addOrUpdate(dead)
			p := requestPacket(t, c, nodeJoinMsg, dead.ID, joinFrom(dead.ID, nil))

			reply, err := c.handleJoin(dead, p)
			require.NoError(t, err)

			assert.True(t, reply.(*joinReplyMessage).Accepted)
			assert.Equal(t, NodeAlive, dead.GetObservedState())
		}},
		{name: "join with a malformed payload", run: func(t *testing.T, c *Cluster) {
			p := requestPacket(t, c, nodeJoinMsg, NodeID(uuid.New()), nil)
			p.SetPayload(garbage)
			_, err := c.handleJoin(nil, p)
			require.Error(t, err)
		}},

		{name: "leave marks the sender leaving", run: func(t *testing.T, c *Cluster) {
			n := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
			c.nodes.addOrUpdate(n)
			p := requestPacket(t, c, nodeLeaveMsg, n.ID, nil)
			require.NoError(t, c.handleNodeLeave(n, p))
			assert.Equal(t, NodeLeaving, n.GetObservedState())
			assert.Equal(t, 1, c.nodes.getLeavingCount())
		}},

		{name: "push-pull merges the sender's states and answers with ours", run: func(t *testing.T, c *Cluster) {
			peers := addPeers(c, 2)
			sender := peers[0]
			unknownAlive, unknownDead := NodeID(uuid.New()), NodeID(uuid.New())
			p := requestPacket(t, c, pushPullStateMsg, sender.ID, &[]exchangeNodeState{
				{ID: unknownAlive, AdvertiseAddr: "127.0.0.1:7001", State: NodeAlive, StateTimestamp: hlc.Now()},
				{ID: unknownDead, AdvertiseAddr: "127.0.0.1:7002", State: NodeDead, StateTimestamp: hlc.Now()},
			})

			reply, err := c.handlePushPullState(sender, p)
			require.NoError(t, err)

			assert.Equal(t, []string{"127.0.0.1:7001"}, queuedJoins(c), "the unknown alive node is joined")
			require.NotNil(t, c.GetNode(unknownDead), "the unknown dead one is kept as a tombstone")
			assert.Equal(t, NodeDead, c.GetNode(unknownDead).GetObservedState())

			states := *reply.(*[]exchangeNodeState)
			var ids []NodeID
			for _, s := range states {
				ids = append(ids, s.ID)
			}
			assert.NotContains(t, ids, sender.ID, "the sender is not told about itself")
			assert.Contains(t, ids, peers[1].ID)
		}},
		{name: "push-pull with a malformed payload", run: func(t *testing.T, c *Cluster) {
			sender := addPeers(c, 1)[0]
			p := requestPacket(t, c, pushPullStateMsg, sender.ID, nil)
			p.SetPayload(garbage)
			_, err := c.handlePushPullState(sender, p)
			require.Error(t, err)
		}},

		{name: "metadata update applies newer metadata and notifies", run: func(t *testing.T, c *Cluster) {
			n := addPeers(c, 1)[0]
			changed := 0
			c.HandleNodeMetadataChangeFunc(func(*Node) { changed++ })
			p := requestPacket(t, c, metadataUpdateMsg, n.ID, &metadataUpdateMessage{
				MetadataTimestamp: hlc.Now(), NodeState: NodeAlive,
				Metadata: map[string]interface{}{"zone": "us-west-2", "priority": int64(10), "enabled": true},
			})

			require.NoError(t, c.handleMetadataUpdate(n, p))

			assert.Equal(t, "us-west-2", n.metadata.GetString("zone"))
			assert.EqualValues(t, 10, n.metadata.GetInt64("priority"))
			assert.True(t, n.metadata.GetBool("enabled"))
			assert.Equal(t, 1, changed)
		}},
		{name: "metadata update older than what we hold is ignored", run: func(t *testing.T, c *Cluster) {
			n := addPeers(c, 1)[0]
			older := hlc.Now()
			n.metadata.update(map[string]interface{}{"role": "gateway"}, hlc.Now(), false)
			changed := 0
			c.HandleNodeMetadataChangeFunc(func(*Node) { changed++ })
			p := requestPacket(t, c, metadataUpdateMsg, n.ID, &metadataUpdateMessage{
				MetadataTimestamp: older, NodeState: NodeAlive, Metadata: map[string]interface{}{"role": "ai"},
			})

			require.NoError(t, c.handleMetadataUpdate(n, p))

			assert.Equal(t, "gateway", n.metadata.GetString("role"))
			assert.Zero(t, changed)
		}},
		{name: "metadata update carries the sender's state", run: func(t *testing.T, c *Cluster) {
			n := addPeers(c, 1)[0]
			p := requestPacket(t, c, metadataUpdateMsg, n.ID, &metadataUpdateMessage{
				MetadataTimestamp: hlc.Now(), NodeState: NodeSuspect, Metadata: map[string]interface{}{"role": "test"},
			})
			require.NoError(t, c.handleMetadataUpdate(n, p))
			assert.Equal(t, NodeSuspect, n.GetObservedState())
		}},
		{name: "metadata update with a malformed payload", run: func(t *testing.T, c *Cluster) {
			n := addPeers(c, 1)[0]
			p := requestPacket(t, c, metadataUpdateMsg, n.ID, nil)
			p.SetPayload(garbage)
			require.Error(t, c.handleMetadataUpdate(n, p))
		}},

		{name: "ping from a known node is answered with a pong", run: func(t *testing.T, c *Cluster) {
			n := addPeers(c, 1)[0]
			c.LocalMetadata().SetString("role", "gateway")
			p := requestPacket(t, c, pingMsg, n.ID, &pingMessage{SenderID: n.ID, AdvertiseAddr: n.AdvertisedAddr()})

			reply, err := c.handlePing(n, p)
			require.NoError(t, err)

			pong := reply.(*pongMessage)
			assert.Equal(t, c.localNode.ID, pong.NodeID)
			assert.Equal(t, c.localNode.AdvertisedAddr(), pong.AdvertiseAddr)
			assert.Equal(t, NodeAlive, pong.NodeState)
			assert.Equal(t, "gateway", pong.Metadata["role"], "the pong carries our metadata")
			assert.Empty(t, queuedJoins(c))
		}},
		{name: "ping from an unknown node is answered and the node joined", run: func(t *testing.T, c *Cluster) {
			id := NodeID(uuid.New())
			p := requestPacket(t, c, pingMsg, id, &pingMessage{SenderID: id, AdvertiseAddr: "127.0.0.1:9999"})

			reply, err := c.handlePing(nil, p)
			require.NoError(t, err)

			assert.Equal(t, c.localNode.ID, reply.(*pongMessage).NodeID)
			assert.Equal(t, []string{"127.0.0.1:9999"}, queuedJoins(c))
		}},
		{name: "ping with a malformed payload", run: func(t *testing.T, c *Cluster) {
			p := requestPacket(t, c, pingMsg, NodeID(uuid.New()), nil)
			p.SetPayload([]byte("not-valid"))
			_, err := c.handlePing(nil, p)
			require.Error(t, err)
		}},
	}

	for _, cd := range []codec.Serializer{codec.NewJSONCodec(), shamaton.New()} {
		t.Run(cd.Name(), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					opts := append([]func(*Config){func(c *Config) { c.MsgCodec = cd }}, tt.opts...)
					tt.run(t, newUnitCluster(t, opts...))
				})
			}
		})
	}
}

// The reply to a join carries a share of the alive nodes: all of a tiny cluster, 80% of a small one, then capped.
func TestCalculateJoinResponseSize(t *testing.T) {
	c := newUnitCluster(t)
	for alive, want := range map[int]int{0: 0, 3: 3, 5: 5, 6: 5, 10: 8, 20: 16, 21: 20, 100: 20, 101: 25, 500: 25} {
		assert.Equal(t, want, c.calculateJoinResponseSize(alive), "%d alive", alive)
	}
}
