package gossip

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/codec/hashicorp"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/codec/vmihailenco"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every message a node sends about itself carries its address with authority: a join, a join reply, a ping and a
// metadata update. A known node's address follows them, and the address resolved from the old one is dropped.
func TestAddressFollowsANodesOwnMessages(t *testing.T) {
	const oldAddr, newAddr = "127.0.0.1:9101", "127.0.0.2:9101"
	known := func(c *Cluster) *Node {
		n := newNode(NodeID(uuid.New()), oldAddr)
		n.SetAddress(Address{IP: []byte{127, 0, 0, 1}, Port: 9101})
		n.metadata.update(map[string]interface{}{"k": "v"}, hlc.Now(), true)
		c.nodes.addOrUpdate(n)
		return n
	}
	moved := func(t *testing.T, n *Node) {
		t.Helper()
		assert.Equal(t, newAddr, n.AdvertisedAddr())
		assert.True(t, n.IsAddressEmpty(), "the address resolved from the old one is dropped")
	}
	join := func(id NodeID, mod func(*joinMessage)) *joinMessage {
		m := &joinMessage{ID: id, AdvertiseAddr: newAddr, State: NodeAlive, MetadataTimestamp: hlc.Now(), ProtocolVersion: PROTOCOL_VERSION}
		if mod != nil {
			mod(m)
		}
		return m
	}

	t.Run("join from a known node", func(t *testing.T) {
		c := newUnitCluster(t)
		n := known(c)
		reply, err := c.handleJoin(n, requestPacket(t, c, nodeJoinMsg, n.ID, join(n.ID, nil)))
		require.NoError(t, err)
		require.True(t, reply.(*joinReplyMessage).Accepted)
		moved(t, n)
	})

	t.Run("join from a node added while its join was on its way", func(t *testing.T) {
		// handleIncomingPacket looks the sender up before the handler runs; a node added in between (from a join
		// reply's list, say) is the one addIfNotExists finds.
		c := newUnitCluster(t)
		n := known(c)
		_, err := c.handleJoin(nil, requestPacket(t, c, nodeJoinMsg, n.ID, join(n.ID, nil)))
		require.NoError(t, err)
		moved(t, n)
	})

	t.Run("a refused join changes nothing", func(t *testing.T) {
		c := newUnitCluster(t)
		n := known(c)
		reply, err := c.handleJoin(n, requestPacket(t, c, nodeJoinMsg, n.ID, join(n.ID, func(m *joinMessage) { m.ProtocolVersion++ })))
		require.NoError(t, err)
		require.False(t, reply.(*joinReplyMessage).Accepted)
		assert.Equal(t, oldAddr, n.AdvertisedAddr())
		assert.False(t, n.IsAddressEmpty())
	})

	t.Run("join reply from a known node", func(t *testing.T) {
		// We know the node at its old address (another node's join reply listed it there) and join it at its new
		// one, a seed: its own reply is right.
		id := NodeID(uuid.New())
		tr := &joinReplyTransport{replies: map[string]*joinReplyMessage{
			newAddr: {Accepted: true, NodeID: id, AdvertiseAddr: newAddr},
		}}
		c := newUnitCluster(t, func(c *Config) { c.Transport = tr })
		n := newNode(id, oldAddr)
		n.SetAddress(Address{IP: []byte{127, 0, 0, 1}, Port: 9101})
		c.nodes.addOrUpdate(n)
		c.joinPeer(newAddr)
		moved(t, n)
	})

	t.Run("ping from a known node", func(t *testing.T) {
		c := newUnitCluster(t)
		n := known(c)
		_, err := c.handlePing(n, requestPacket(t, c, pingMsg, n.ID, &pingMessage{SenderID: n.ID, AdvertiseAddr: newAddr}))
		require.NoError(t, err)
		moved(t, n)
	})

	t.Run("metadata update from a known node", func(t *testing.T) {
		c := newUnitCluster(t)
		n := known(c)
		p := requestPacket(t, c, metadataUpdateMsg, n.ID, &metadataUpdateMessage{MetadataTimestamp: hlc.Now(),
			Metadata: map[string]interface{}{"k": "v2"}, NodeState: NodeAlive, AdvertiseAddr: newAddr})
		require.NoError(t, c.handleMetadataUpdate(n, p))
		moved(t, n)
		assert.Equal(t, "v2", n.metadata.GetString("k"))
	})

	t.Run("a repeated metadata update (the periodic one) carries it too", func(t *testing.T) {
		c := newUnitCluster(t)
		n := known(c)
		p := requestPacket(t, c, metadataUpdateMsg, n.ID, &metadataUpdateMessage{MetadataTimestamp: n.metadata.GetTimestamp(),
			Metadata: map[string]interface{}{"k": "v"}, NodeState: NodeAlive, AdvertiseAddr: newAddr})
		require.NoError(t, c.handleMetadataUpdate(n, p))
		moved(t, n)
	})

	t.Run("an older metadata update does not bring the old address back", func(t *testing.T) {
		// Metadata updates are forwarded: one the node sent before it moved can arrive after its join from the new
		// address.
		c := newUnitCluster(t)
		before := hlc.Now()
		n := known(c) // its metadata is stamped after before
		_, err := c.handleJoin(n, requestPacket(t, c, nodeJoinMsg, n.ID, join(n.ID, nil)))
		require.NoError(t, err)
		p := requestPacket(t, c, metadataUpdateMsg, n.ID, &metadataUpdateMessage{MetadataTimestamp: before,
			Metadata: map[string]interface{}{"k": "old"}, NodeState: NodeAlive, AdvertiseAddr: oldAddr})
		require.NoError(t, c.handleMetadataUpdate(n, p))
		assert.Equal(t, newAddr, n.AdvertisedAddr())
	})

	t.Run("a metadata update without an address (an older node's) changes nothing", func(t *testing.T) {
		c := newUnitCluster(t)
		n := known(c)
		p := requestPacket(t, c, metadataUpdateMsg, n.ID, &metadataUpdateMessage{MetadataTimestamp: hlc.Now(),
			Metadata: map[string]interface{}{"k": "v2"}, NodeState: NodeAlive})
		require.NoError(t, c.handleMetadataUpdate(n, p))
		assert.Equal(t, oldAddr, n.AdvertisedAddr())
		assert.False(t, n.IsAddressEmpty())
	})

	t.Run("second-hand news of an address is still not taken", func(t *testing.T) {
		c := newUnitCluster(t)
		n := known(c)
		c.combineStates([]exchangeNodeState{{ID: n.ID, AdvertiseAddr: newAddr, State: NodeAlive, StateTimestamp: hlc.Now()}})
		assert.Equal(t, oldAddr, n.AdvertisedAddr())
	})

	t.Run("the same address keeps the resolved one", func(t *testing.T) {
		c := newUnitCluster(t)
		n := known(c)
		_, err := c.handlePing(n, requestPacket(t, c, pingMsg, n.ID, &pingMessage{SenderID: n.ID, AdvertiseAddr: oldAddr}))
		require.NoError(t, err)
		assert.False(t, n.IsAddressEmpty(), "no new resolution (a DNS lookup) for every ping")
	})
}

// The local node sends its address in its metadata updates.
func TestMetadataUpdatesCarryTheAddress(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) { c.AdvertiseAddr = "127.0.0.1:9100" })
	addPeers(c, 2)
	c.LocalMetadata().SetString("k", "v") // sends one
	c.localNode.metadata.lastModified.Store(uint64(hlc.FromTime(hlc.Now().Time().Add(-time.Minute))))
	c.gossipMetadata() // and the periodic one
	n := 0
	for _, item := range queuedBroadcasts(c) {
		if item.packet.MessageType != metadataUpdateMsg {
			continue
		}
		var m metadataUpdateMessage
		require.NoError(t, item.packet.Unmarshal(&m))
		assert.Equal(t, "127.0.0.1:9100", m.AdvertiseAddr)
		n++
	}
	assert.Equal(t, 2, n)
}

// Every codec reads the new optional address of a metadata update as absent in an older node's struct (it ignores
// it), and reads an older node's update (without it) as empty.
func TestMetadataAddressFieldCompatibility(t *testing.T) {
	type oldMetadataUpdate struct {
		MetadataTimestamp hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
		Metadata          map[string]interface{} `msgpack:"md" json:"md"`
		NodeState         NodeState              `msgpack:"state" json:"state"`
		KeyWindow         uint32                 `msgpack:"kw,omitempty" json:"kw,omitempty"`
		Caps              uint32                 `msgpack:"cp,omitempty" json:"cp,omitempty"`
	}
	for name, c := range map[string]codec.Serializer{"shamaton": shamaton.New(), "vmihailenco": vmihailenco.New(),
		"hashicorp": hashicorp.New(), "json": codec.NewJSONCodec()} {
		b, err := c.Marshal(&metadataUpdateMessage{NodeState: NodeAlive, Metadata: map[string]interface{}{"k": "v"}, Caps: 3, AdvertiseAddr: "a:1"})
		require.NoError(t, err, name)
		var o oldMetadataUpdate
		require.NoError(t, c.Unmarshal(b, &o), name)
		require.Equal(t, "v", o.Metadata["k"], name)
		require.EqualValues(t, 3, o.Caps, name)
		require.Equal(t, NodeAlive, o.NodeState, name)

		b, err = c.Marshal(&oldMetadataUpdate{NodeState: NodeAlive, Metadata: map[string]interface{}{"k": "v"}})
		require.NoError(t, err, name)
		var n metadataUpdateMessage
		require.NoError(t, c.Unmarshal(b, &n), name)
		require.Empty(t, n.AdvertiseAddr, name)
		require.Equal(t, "v", n.Metadata["k"], name)
	}
}
