package gossip

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every way handleIncomingPacket sees a packet: whether the message is recorded, forwarded and handled, and that the
// packet's references end up with the broadcast queue alone.
func TestHandleIncomingPacket(t *testing.T) {
	tests := []struct {
		name     string
		packet   func(c *Cluster, sender NodeID, p *Packet) // adjusts a packet from the known sender
		known    bool                                       // the sender is in the node list
		failWith error                                      // returned by the handler
		handled  bool
		forward  bool // queued for re-broadcast
		recorded bool // in the message history
	}{
		{name: "from self", known: true, packet: func(c *Cluster, _ NodeID, p *Packet) { p.SenderID = c.localNode.ID }},
		{name: "already seen", known: true, packet: func(c *Cluster, s NodeID, p *Packet) { c.msgHistory.recordMessage(s, p.MessageID) },
			recorded: true},
		{name: "targeted at another node", known: true, packet: func(c *Cluster, _ NodeID, p *Packet) {
			other := NodeID(uuid.New())
			p.TargetNodeID = &other
		}},
		{name: "targeted at us", known: true, packet: func(c *Cluster, _ NodeID, p *Packet) { p.TargetNodeID = &c.localNode.ID },
			handled: true, forward: true, recorded: true},
		{name: "ordinary", known: true, handled: true, forward: true, recorded: true},
		{name: "last hop is handled but not forwarded", known: true, packet: func(_ *Cluster, _ NodeID, p *Packet) { p.TTL = 0 },
			handled: true, recorded: true},
		{name: "tag we do not have is forwarded, not handled", known: true, packet: func(_ *Cluster, _ NodeID, p *Packet) {
			tag := "api"
			p.Tag = &tag
		}, forward: true, recorded: true},
		{name: "tag we have", known: true, packet: func(_ *Cluster, _ NodeID, p *Packet) {
			tag := "web"
			p.Tag = &tag
		}, handled: true, forward: true, recorded: true},
		{name: "unknown sender is forwarded, not handled", known: false, forward: true, recorded: true},
		{name: "handler error", known: true, failWith: errors.New("boom"), handled: true, forward: true, recorded: true},
		{name: "awaiting a reply is handled, not forwarded", known: true, packet: func(_ *Cluster, _ NodeID, p *Packet) {
			p.SetReplyChan(make(chan *Packet, 1))
		}, handled: true, recorded: true},
		{name: "no handler for the type", known: true, packet: func(_ *Cluster, _ NodeID, p *Packet) { p.MessageType = UserMsg + 1 },
			forward: true, recorded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newUnitCluster(t, func(c *Config) { c.Tags = []string{"web"} })
			sender := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
			if tt.known {
				c.nodes.addOrUpdate(sender)
			}
			var calls atomic.Int32
			require.NoError(t, c.HandleFunc(UserMsg, func(*Node, *Packet) error {
				calls.Add(1)
				return tt.failWith
			}))

			p := requestPacket(t, c, UserMsg, sender.ID, nil)
			p.TTL = 3
			if tt.packet != nil {
				tt.packet(c, sender.ID, p)
			}
			sentBy, id := p.SenderID, p.MessageID

			c.handleIncomingPacket(p)

			assert.Equal(t, tt.handled, calls.Load() == 1, "handled")
			assert.Equal(t, tt.recorded, c.msgHistory.contains(sentBy, id), "recorded in the history")
			queued := queuedBroadcasts(c)
			if !tt.forward {
				assert.Empty(t, queued, "forwarded")
				assert.Zero(t, refs(p), "the packet was released")
				return
			}
			require.Len(t, queued, 1, "forwarded")
			assert.Same(t, p, queued[0].packet)
			assert.EqualValues(t, 1, refs(p), "only the broadcast queue still holds the packet")
			assert.Equal(t, uint8(2), p.TTL, "one hop used")
			assert.Equal(t, TransportType(TransportBestEffort), queued[0].transportType)
			assert.ElementsMatch(t, []NodeID{c.localNode.ID, sentBy}, queued[0].excludePeers, "not back to us or the sender")
			p.Release()
		})
	}
}

// What arrives over TCP goes on over TCP.
func TestHandleIncomingPacketForwardsReliably(t *testing.T) {
	c := newUnitCluster(t)
	sender := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	c.nodes.addOrUpdate(sender)
	require.NoError(t, c.HandleFunc(UserMsg, func(*Node, *Packet) error { return nil }))

	p := requestPacket(t, c, UserMsg, sender.ID, nil)
	p.TTL = 3
	p.reliable = true
	c.handleIncomingPacket(p)

	queued := queuedBroadcasts(c)
	require.Len(t, queued, 1)
	assert.Equal(t, TransportType(TransportReliable), queued[0].transportType)
	p.Release()
}

func TestEnqueueForBroadcast(t *testing.T) {
	peers := []*Node{newNode(NodeID(uuid.New()), "127.0.0.1:8001")}
	exclude := []NodeID{NodeID(uuid.New())}

	t.Run("queued with a hop used", func(t *testing.T) {
		c := newUnitCluster(t)
		p := NewPacket()
		p.TTL = 3
		c.enqueuePacketForBroadcast(p, TransportReliable, exclude, peers)

		queued := queuedBroadcasts(c)
		require.Len(t, queued, 1)
		assert.Same(t, p, queued[0].packet)
		assert.Equal(t, uint8(2), p.TTL)
		assert.Equal(t, TransportType(TransportReliable), queued[0].transportType)
		assert.Equal(t, exclude, queued[0].excludePeers)
		assert.Equal(t, peers, queued[0].peers)
		p.Release()
	})

	t.Run("out of hops is dropped and released", func(t *testing.T) {
		c := newUnitCluster(t)
		p := NewPacket()
		p.TTL = 0 // a pooled packet keeps the TTL it last had
		c.enqueuePacketForBroadcast(p, TransportBestEffort, nil, nil)

		assert.Empty(t, queuedBroadcasts(c))
		assert.Zero(t, refs(p))
	})

	t.Run("full queue drops and releases without blocking", func(t *testing.T) {
		c := newUnitCluster(t, func(c *Config) { c.SendQueueSize = 1 })
		first, second := NewPacket(), NewPacket()
		first.TTL, second.TTL = 3, 3
		c.enqueuePacketForBroadcast(first, TransportBestEffort, nil, nil)
		c.enqueuePacketForBroadcast(second, TransportBestEffort, nil, nil)

		queued := queuedBroadcasts(c)
		require.Len(t, queued, 1)
		assert.Same(t, first, queued[0].packet)
		assert.EqualValues(t, 1, refs(first))
		assert.Zero(t, refs(second), "the packet that did not fit was released")
		first.Release()
	})
}

// The broadcast worker sends a queued packet to a fan out of random peers (never to the local node), and only to the
// peers with the tag for a tagged message; a failing transport does not stop it.
func TestBroadcastWorker(t *testing.T) {
	start := func(t *testing.T, c *Cluster) {
		c.shutdownWg.Add(1)
		go c.broadcastWorker()
	}
	broadcast := func(c *Cluster, tag *string) {
		p, err := c.createPacketWithTargetAndTag(c.localNode.ID, nil, tag, UserMsg, 3, "payload")
		require.NoError(t, err)
		c.enqueuePacketForBroadcast(p, TransportBestEffort, []NodeID{c.localNode.ID}, nil)
	}
	recipients := func(c *Cluster) map[NodeID]int {
		got := map[NodeID]int{}
		for _, s := range unitTransport(c).sends() {
			got[s.node.ID]++
		}
		return got
	}

	t.Run("untagged", func(t *testing.T) {
		c := newUnitCluster(t)
		peers := addPeers(c, 3)
		start(t, c)
		broadcast(c, nil)

		require.Eventually(t, func() bool { return len(unitTransport(c).sends()) == 3 }, 5*time.Second, time.Millisecond)
		for _, p := range peers {
			assert.Equal(t, 1, recipients(c)[p.ID], "each peer gets the packet once")
		}
		assert.NotContains(t, recipients(c), c.localNode.ID)
	})

	t.Run("tagged", func(t *testing.T) {
		c := newUnitCluster(t)
		tagged := addPeers(c, 2, "group-a")
		addPeers(c, 2, "group-b")
		start(t, c)
		tag := "group-a"
		broadcast(c, &tag)

		require.Eventually(t, func() bool { return len(unitTransport(c).sends()) == 2 }, 5*time.Second, time.Millisecond)
		for _, p := range tagged {
			assert.Equal(t, 1, recipients(c)[p.ID])
		}
		for _, s := range unitTransport(c).sends() {
			require.NotNil(t, s.tag)
			assert.Equal(t, tag, *s.tag)
		}
	})

	t.Run("survives a failing transport", func(t *testing.T) {
		c := newUnitCluster(t)
		addPeers(c, 3)
		start(t, c)

		unitTransport(c).setSendError(errors.New("connection refused"))
		for range 5 {
			broadcast(c, nil)
		}
		require.Eventually(t, func() bool { return len(unitTransport(c).sends()) == 15 }, 5*time.Second, time.Millisecond,
			"every packet is tried, whatever the failures")

		unitTransport(c).setSendError(nil)
		for range 5 {
			broadcast(c, nil)
		}
		require.Eventually(t, func() bool { return len(unitTransport(c).sends()) == 30 }, 5*time.Second, time.Millisecond,
			"the worker carries on once the transport works again")
	})
}
