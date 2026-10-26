package gossip

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// agedNode is a node in the given state that has been in it for age.
func agedNode(state NodeState, age time.Duration) *Node {
	n := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	n.observedState = state
	n.observedStateTime = hlc.FromTime(time.Now().Add(-age))
	return n
}

// A leaving node that has outstayed LeavingNodeTimeout is dead, and a dead one that has outstayed NodeRetentionTime is
// forgotten.
func TestCleanupNodes(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) {
		c.LeavingNodeTimeout = time.Minute
		c.NodeRetentionTime = time.Hour
	})
	leavingOld := agedNode(NodeLeaving, 2*time.Minute)
	leavingNew := agedNode(NodeLeaving, 0)
	deadOld := agedNode(NodeDead, 2*time.Hour)
	deadNew := agedNode(NodeDead, 10*time.Minute)
	for _, n := range []*Node{leavingOld, leavingNew, deadOld, deadNew} {
		c.nodes.addOrUpdate(n)
	}

	c.cleanupNodes()

	assert.Equal(t, NodeDead, leavingOld.GetObservedState(), "left too long ago")
	assert.Equal(t, NodeLeaving, leavingNew.GetObservedState())
	assert.Nil(t, c.nodes.get(deadOld.ID), "dead beyond the retention time")
	assert.Same(t, deadNew, c.nodes.get(deadNew.ID))
	assert.Equal(t, 2, c.nodes.getDeadCount())
	assert.Equal(t, 1, c.nodes.getLeavingCount())

	// Run again: a node that has only just died is kept.
	c.cleanupNodes()
	assert.Same(t, leavingOld, c.nodes.get(leavingOld.ID))
}

// The gossip interval follows how long the handlers take: it rises to 25% over a slow round (up to the maximum) and
// eases back to the base interval when they are fast again.
func TestAdjustGossipInterval(t *testing.T) {
	const base, max = 5 * time.Second, 10 * time.Second
	tests := []struct {
		name     string
		interval time.Duration // the current interval
		took     time.Duration
		want     time.Duration
	}{
		{"slow round increases it", base, 6 * time.Second, 7500 * time.Millisecond},
		{"capped at the maximum", base, 15 * time.Second, max},
		{"fast round eases it back", max, 2 * time.Second, 6 * time.Second},
		{"never below the base", 6 * time.Second, time.Millisecond, base},
		{"unchanged at the base", base, time.Second, base},
		{"unchanged when close to the interval", max, 9 * time.Second, max},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newUnitCluster(t, func(c *Config) {
				c.GossipInterval, c.GossipMaxInterval = base, max
			})
			c.gossipInterval = tt.interval
			c.gossipTicker = time.NewTicker(tt.interval)
			defer c.gossipTicker.Stop()

			c.adjustGossipInterval(tt.took)

			assert.Equal(t, tt.want, c.gossipInterval)
		})
	}
}

// With half of the seed peers or fewer alive (the local node counts) the seeds are queued to be joined again, at most
// once per half PeerRecoveryInterval.
func TestCheckPeerConnectivity(t *testing.T) {
	tests := []struct {
		name          string
		seeds         int
		alive         int  // alive nodes including the local node
		justRecovered bool // a recovery ran a moment ago
		wantJoins     int
	}{
		{"no seeds", 0, 1, false, 0},
		{"alone with four seeds", 4, 1, false, 4},
		{"half alive of four seeds", 4, 2, false, 4},
		{"more than half alive of four seeds", 4, 3, false, 0},
		{"odd seed count rounds the threshold up", 3, 2, false, 3},
		{"above that for three seeds", 3, 3, false, 0},
		{"recovered a moment ago", 4, 1, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newUnitCluster(t, func(c *Config) { c.PeerRecoveryInterval = time.Hour })
			var seeds []string
			for i := range tt.seeds {
				seeds = append(seeds, fmt.Sprintf("127.0.0.1:%d", 9001+i))
			}
			c.seedPeers = seeds
			addPeers(c, tt.alive-1)
			if tt.justRecovered {
				c.lastPeerRecovery = time.Now()
			}

			c.checkPeerConnectivity()

			joins := queuedJoins(c)
			assert.Len(t, joins, tt.wantJoins)
			if tt.wantJoins > 0 {
				assert.ElementsMatch(t, seeds, joins)
				assert.WithinDuration(t, time.Now(), c.lastPeerRecovery, time.Minute)
			}
		})
	}
}

func TestRetrySeedPeers(t *testing.T) {
	seeds := []string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"}

	c := newUnitCluster(t)
	c.retrySeedPeers()
	assert.Empty(t, queuedJoins(c), "no seeds")

	c.seedPeers = seeds
	c.retrySeedPeers()
	assert.ElementsMatch(t, seeds, queuedJoins(c))

	full := newUnitCluster(t, func(c *Config) { c.JoinQueueSize = 1 })
	full.seedPeers = seeds
	full.retrySeedPeers()
	assert.Len(t, queuedJoins(full), 1, "a full join queue drops the rest without blocking")
}

// Metadata is gossiped to a few peers only when it has not changed for half the interval: while it is changing,
// every change is already broadcast.
func TestGossipMetadata(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) { c.MetadataGossipInterval = time.Hour })
	addPeers(c, 5)

	c.LocalMetadata().SetString("role", "gateway")
	for _, item := range queuedBroadcasts(c) {
		item.packet.Release()
	}
	c.gossipMetadata()
	assert.Empty(t, queuedBroadcasts(c), "just changed")

	c.localNode.metadata.update(map[string]interface{}{"role": "gateway"}, hlc.FromTime(time.Now().Add(-time.Hour)), true)
	for _, item := range queuedBroadcasts(c) {
		item.packet.Release()
	}
	c.gossipMetadata()
	items := queuedBroadcasts(c)
	require.Len(t, items, 1, "unchanged for a while")
	assert.Equal(t, metadataUpdateMsg, items[0].packet.MessageType)
	assert.Len(t, items[0].peers, 3, "to three peers")
	for _, p := range items[0].peers {
		assert.NotEqual(t, c.localNode.ID, p.ID)
	}
	items[0].packet.Release()
}

// The managers a started cluster runs each do their work on their own tick: gossip handlers are called, state is
// exchanged with a peer, old dead nodes are dropped, dead nodes are pinged again, and metadata is gossiped.
func TestBackgroundManagers(t *testing.T) {
	const tick = 20 * time.Millisecond
	c := newUnitCluster(t, func(c *Config) {
		c.GossipInterval, c.GossipMaxInterval = tick, 5*tick
		c.StateGossipInterval = tick
		c.MetadataGossipInterval = tick
		c.NodeCleanupInterval = tick
		c.DeadNodeRetryInterval = tick
		c.HealthCheckInterval = time.Hour
		c.NodeRetentionTime = time.Hour
	})
	peer := newNode(NodeID(uuid.New()), "127.0.0.1:9001")
	expired := agedNode(NodeDead, 2*time.Hour)
	recentlyDead := agedNode(NodeDead, time.Minute)
	for _, n := range []*Node{peer, expired, recentlyDead} {
		c.nodes.addOrUpdate(n)
	}
	var gossips atomic.Int32
	c.HandleGossipFunc(func() { gossips.Add(1) })
	c.localNode.metadata.update(map[string]interface{}{"role": "gateway"}, hlc.FromTime(time.Now().Add(-time.Hour)), true)

	c.Start()

	sentTo := func(msg MessageType, node *Node) bool {
		for _, s := range unitTransport(c).sends() {
			if s.msgType == msg && s.node.ID == node.ID {
				return true
			}
		}
		return false
	}
	eventually := func(what string, cond func() bool) {
		require.Eventually(t, cond, 5*time.Second, 5*time.Millisecond, what)
	}
	eventually("gossip handlers are called every round", func() bool { return gossips.Load() >= 3 })
	eventually("a dead node past the retention time is dropped", func() bool { return c.nodes.get(expired.ID) == nil })
	assert.NotNil(t, c.nodes.get(recentlyDead.ID), "one that died recently is kept")
	eventually("state is exchanged with a peer", func() bool { return sentTo(pushPullStateMsg, peer) })
	eventually("a dead node is pinged to see whether it is back", func() bool { return sentTo(pingMsg, recentlyDead) })
	eventually("metadata is gossiped", func() bool {
		for _, s := range unitTransport(c).sends() {
			if s.msgType == metadataUpdateMsg {
				return true
			}
		}
		return false
	})
}
