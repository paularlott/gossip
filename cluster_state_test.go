package gossip

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// How a state report's timestamp compares with the timestamp of the state we hold.
type reportAge int

const (
	reportNewer reportAge = iota
	reportSame
	reportOlder
)

// combineStates takes a report only when it is newer than what we hold, and revives a dead or leaving node only to
// alive or suspect, which also queues a join to re-establish the connection; an alive or suspect node is moved only to
// dead or leaving by second-hand reports (it has to prove itself alive to us with a ping).
func TestCombineStates(t *testing.T) {
	const addr = "127.0.0.1:9001"
	tests := []struct {
		name   string
		local  NodeState // the state we hold; NodeUnknown for a node we have not heard of
		remote NodeState
		age    reportAge
		want   NodeState // NodeUnknown: still not in the node list
		joins  int       // join requests queued for the node
	}{
		{"unknown alive is joined, not added", NodeUnknown, NodeAlive, reportNewer, NodeUnknown, 1},
		{"unknown suspect is joined, not added", NodeUnknown, NodeSuspect, reportNewer, NodeUnknown, 1},
		{"unknown dead is kept as a tombstone", NodeUnknown, NodeDead, reportNewer, NodeDead, 0},
		{"unknown leaving is kept as a tombstone", NodeUnknown, NodeLeaving, reportNewer, NodeLeaving, 0},
		{"alive dies", NodeAlive, NodeDead, reportNewer, NodeDead, 0},
		{"alive leaves", NodeAlive, NodeLeaving, reportNewer, NodeLeaving, 0},
		{"older death report ignored", NodeAlive, NodeDead, reportOlder, NodeAlive, 0},
		{"same-time death report ignored", NodeAlive, NodeDead, reportSame, NodeAlive, 0},
		{"alive stays alive", NodeAlive, NodeAlive, reportNewer, NodeAlive, 0},
		{"suspect report does not demote alive", NodeAlive, NodeSuspect, reportNewer, NodeAlive, 0},
		{"alive report does not clear suspect", NodeSuspect, NodeAlive, reportNewer, NodeSuspect, 0},
		{"suspect dies", NodeSuspect, NodeDead, reportNewer, NodeDead, 0},
		{"dead recovers", NodeDead, NodeAlive, reportNewer, NodeAlive, 1},
		{"dead partly recovers", NodeDead, NodeSuspect, reportNewer, NodeSuspect, 1},
		{"leaving recovers", NodeLeaving, NodeAlive, reportNewer, NodeAlive, 1},
		{"older alive report does not revive dead", NodeDead, NodeAlive, reportOlder, NodeDead, 0},
		{"dead becomes leaving", NodeDead, NodeLeaving, reportNewer, NodeLeaving, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newUnitCluster(t)
			id := NodeID(uuid.New())
			held := hlc.Now()
			if tt.local != NodeUnknown {
				n := newNode(id, addr)
				n.observedState, n.observedStateTime = tt.local, held
				c.nodes.addOrUpdate(n)
			}
			reported := held
			switch tt.age {
			case reportNewer:
				reported = hlc.Now()
			case reportOlder:
				reported = hlc.FromTime(time.Now().Add(-time.Hour))
			}

			c.combineStates([]exchangeNodeState{{ID: id, AdvertiseAddr: addr, State: tt.remote, StateTimestamp: reported}})

			n := c.nodes.get(id)
			if tt.want == NodeUnknown {
				assert.Nil(t, n)
			} else {
				require.NotNil(t, n)
				state, ts := n.GetStateSnapshot()
				assert.Equal(t, tt.want, state)
				if tt.want != tt.local {
					assert.Equal(t, reported, ts, "the report's time is kept")
				}
			}
			if tt.joins == 0 {
				assert.Empty(t, queuedJoins(c))
			} else {
				assert.Equal(t, []string{addr}, queuedJoins(c))
			}
		})
	}

	t.Run("a report cannot change a node's address", func(t *testing.T) {
		c := newUnitCluster(t)
		n := newNode(NodeID(uuid.New()), addr)
		c.nodes.addOrUpdate(n)
		c.combineStates([]exchangeNodeState{{ID: n.ID, AdvertiseAddr: "127.0.0.1:9999", State: NodeDead, StateTimestamp: hlc.Now()}})
		assert.Equal(t, NodeDead, n.GetObservedState(), "the state is taken")
		assert.Equal(t, addr, n.AdvertisedAddr(), "second-hand news is not authoritative for the address")
	})

	t.Run("the local node is not told what it is", func(t *testing.T) {
		c := newUnitCluster(t)
		c.combineStates([]exchangeNodeState{{ID: c.localNode.ID, AdvertiseAddr: c.localNode.AdvertisedAddr(), State: NodeDead, StateTimestamp: hlc.Now()}})
		assert.Equal(t, NodeAlive, c.localNode.GetObservedState())
	})

	// Two partitions disagree about two nodes: each of our views is overruled by the newer report.
	t.Run("split brain", func(t *testing.T) {
		c := newUnitCluster(t)
		a, b := newNode(NodeID(uuid.New()), "127.0.0.1:9001"), newNode(NodeID(uuid.New()), "127.0.0.1:9002")
		b.observedState = NodeDead
		c.nodes.addOrUpdate(a)
		c.nodes.addOrUpdate(b)
		ts := hlc.Now()
		c.combineStates([]exchangeNodeState{
			{ID: a.ID, AdvertiseAddr: a.advertiseAddr, State: NodeDead, StateTimestamp: ts},
			{ID: b.ID, AdvertiseAddr: b.advertiseAddr, State: NodeAlive, StateTimestamp: ts},
		})
		assert.Equal(t, NodeDead, a.GetObservedState())
		assert.Equal(t, NodeAlive, b.GetObservedState())
	})
}

// B joins A (alive at T1), C says B died at T2, A pings B and marks it alive at T3: C repeating its stale T2 report must
// not kill B again, but a newer death report must.
func TestCombineStatesFlappingResistance(t *testing.T) {
	c := newUnitCluster(t)
	b := newNode(NodeID(uuid.New()), "127.0.0.1:9001")
	c.nodes.addOrUpdate(b)

	t2 := hlc.Now()
	report := func(ts hlc.Timestamp) {
		c.combineStates([]exchangeNodeState{{ID: b.ID, AdvertiseAddr: b.advertiseAddr, State: NodeDead, StateTimestamp: ts}})
	}

	report(t2)
	require.Equal(t, NodeDead, b.GetObservedState(), "newer than the join")

	c.nodes.updateState(b.ID, NodeAlive) // the ping succeeded
	require.True(t, b.observedStateTime.After(t2))

	report(t2)
	assert.Equal(t, NodeAlive, b.GetObservedState(), "a stale death report is rejected")

	report(hlc.Now())
	assert.Equal(t, NodeDead, b.GetObservedState(), "a newer death report is accepted")
}

// All the nodes we knew are dead after a restart; peers' state exchanges bring them back one by one.
func TestSimultaneousRestartConvergence(t *testing.T) {
	c := newUnitCluster(t)
	nodes := make([]*Node, 3)
	for i := range nodes {
		nodes[i] = newNode(NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:900%d", i+1))
		nodes[i].observedState, nodes[i].observedStateTime = NodeDead, hlc.Now()
		c.nodes.addOrUpdate(nodes[i])
	}
	require.Equal(t, 1, c.nodes.getAliveCount(), "only the local node")
	require.Equal(t, 3, c.nodes.getDeadCount())

	for i, n := range nodes {
		c.combineStates([]exchangeNodeState{{ID: n.ID, AdvertiseAddr: n.advertiseAddr, State: NodeAlive, StateTimestamp: hlc.Now()}})
		assert.Equal(t, NodeAlive, n.GetObservedState())
		assert.Equal(t, 2+i, c.nodes.getAliveCount())
		assert.Equal(t, 2-i, c.nodes.getDeadCount())
	}
}

// The state counters track every transition, and addIfNotExists on a node we know changes nothing.
func TestCounterConsistencyAfterStateTransitions(t *testing.T) {
	c := newUnitCluster(t)
	ids := make([]NodeID, 5)
	for i := range ids {
		ids[i] = NodeID(uuid.New())
		c.nodes.addOrUpdate(newNode(ids[i], fmt.Sprintf("127.0.0.1:%d", 9001+i)))
	}
	counts := func() [4]int {
		return [4]int{c.nodes.getAliveCount(), c.nodes.getSuspectCount(), c.nodes.getDeadCount(), c.nodes.getLeavingCount()}
	}
	require.Equal(t, [4]int{6, 0, 0, 0}, counts(), "the local node and five")

	c.nodes.updateState(ids[0], NodeSuspect)
	c.nodes.updateState(ids[1], NodeSuspect)
	assert.Equal(t, [4]int{4, 2, 0, 0}, counts())
	c.nodes.updateState(ids[0], NodeDead)
	assert.Equal(t, [4]int{4, 1, 1, 0}, counts())
	c.nodes.updateState(ids[0], NodeAlive)
	assert.Equal(t, [4]int{5, 1, 0, 0}, counts())
	c.nodes.updateState(ids[2], NodeLeaving)
	assert.Equal(t, [4]int{4, 1, 0, 1}, counts())

	// addIfNotExists returns the node we hold, whatever state the new object claims.
	c.nodes.updateState(ids[3], NodeDead)
	before := counts()
	held := c.nodes.addIfNotExists(newNode(ids[3], "127.0.0.1:1"))
	assert.Equal(t, NodeDead, held.GetObservedState())
	assert.Equal(t, before, counts())

	// A node that flaps a lot still counts once.
	for i := range 20 {
		c.nodes.updateState(ids[4], []NodeState{NodeDead, NodeAlive}[i%2])
	}
	assert.Equal(t, 6, c.NumNodes())
}

// Concurrent state changes keep every node counted exactly once.
func TestConcurrentStateChangesCounterConsistency(t *testing.T) {
	c := newUnitCluster(t)
	ids := make([]NodeID, 20)
	for i := range ids {
		ids[i] = NodeID(uuid.New())
		c.nodes.addOrUpdate(newNode(ids[i], fmt.Sprintf("127.0.0.1:%d", 8001+i)))
	}

	states := []NodeState{NodeAlive, NodeSuspect, NodeDead}
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.nodes.updateState(ids[i%len(ids)], states[i%len(states)])
		}()
	}
	wg.Wait()

	assert.Equal(t, 21, c.NumNodes(), "the local node and 20")
	assert.Equal(t, 21, len(c.nodes.getAll()))
}

// A failed exchange makes the peer suspect; one that answers recovers it, and what it tells us is merged.
func TestExchangeState(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		c := newUnitCluster(t)
		peer := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
		c.nodes.addOrUpdate(peer)

		c.exchangeState([]*Node{peer}, []NodeID{c.localNode.ID})

		assert.Equal(t, NodeSuspect, peer.GetObservedState())
		sends := unitTransport(c).sends()
		require.Len(t, sends, 1)
		assert.Equal(t, pushPullStateMsg, sends[0].msgType)
		assert.Same(t, peer, sends[0].node)
	})

	t.Run("answered", func(t *testing.T) {
		c := newUnitCluster(t)
		peer := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
		c.nodes.addOrUpdate(peer)
		c.nodes.updateState(peer.ID, NodeSuspect)
		gone := NodeID(uuid.New())
		unitTransport(c).replyWith(c.config.MsgCodec, []exchangeNodeState{
			{ID: gone, AdvertiseAddr: "127.0.0.1:8002", State: NodeDead, StateTimestamp: hlc.Now()},
		})

		c.exchangeState([]*Node{peer}, []NodeID{c.localNode.ID})

		assert.Equal(t, NodeAlive, peer.GetObservedState(), "an answer recovers a suspect peer")
		require.NotNil(t, c.nodes.get(gone), "the peer's tombstone is merged")
		assert.Equal(t, NodeDead, c.nodes.get(gone).GetObservedState())
	})

	t.Run("nobody to exchange with", func(t *testing.T) {
		c := newUnitCluster(t)
		c.exchangeState(nil, []NodeID{c.localNode.ID})
		assert.Empty(t, unitTransport(c).sends())
	})
}

// A join reply lists peers: the unknown ones are added and joined, the ones we hold as dead are joined again (they
// may have restarted), the live ones are left alone.
func TestJoinReplyProcessing(t *testing.T) {
	const addrA, addrDead, addrNew, addrAlive = "127.0.0.1:9101", "127.0.0.1:9102", "127.0.0.1:9103", "127.0.0.1:9104"
	a, dead, fresh, alive := NodeID(uuid.New()), NodeID(uuid.New()), NodeID(uuid.New()), NodeID(uuid.New())
	tr := &joinReplyTransport{replies: map[string]*joinReplyMessage{
		addrA: {Accepted: true, NodeID: a, AdvertiseAddr: addrA, Nodes: []joinNode{
			{ID: dead, AdvertiseAddr: addrDead}, {ID: fresh, AdvertiseAddr: addrNew}, {ID: alive, AdvertiseAddr: addrAlive},
		}},
	}}
	c := newUnitCluster(t, func(c *Config) { c.Transport = tr })
	deadNode := newNode(dead, addrDead)
	deadNode.observedState = NodeDead
	c.nodes.addOrUpdate(deadNode)
	c.nodes.addOrUpdate(newNode(alive, addrAlive))

	c.joinPeer(addrA)

	require.NotNil(t, c.nodes.get(a), "the peer that answered is added")
	assert.Equal(t, NodeAlive, c.nodes.get(a).GetObservedState())
	require.NotNil(t, c.nodes.get(fresh), "an unknown peer is added until its own join confirms it")
	assert.Equal(t, NodeDead, deadNode.GetObservedState(), "a dead peer stays dead until it answers us itself")
	assert.ElementsMatch(t, []string{addrDead, addrNew}, queuedJoins(c))
}

// joinReplyTransport answers every join request as the peer at the request's address, with the reply given for it.
type joinReplyTransport struct {
	mockTransport
	replies map[string]*joinReplyMessage
}

func (m *joinReplyTransport) SendWithReply(node *Node, packet *Packet) (*Packet, error) {
	payload, err := codec.NewJSONCodec().Marshal(m.replies[node.AdvertisedAddr()])
	if err != nil {
		return nil, err
	}
	reply := NewPacket()
	reply.SetCodec(codec.NewJSONCodec())
	reply.SetPayload(payload)
	return reply, nil
}

// TestJoinReplyFromDeadPeerRevivesIt verifies that a peer held dead that answers a join is marked alive.
// BUG FIX: Previously it stayed dead, so two peers held dead that list each other in their join replies were joined
// in turn without end: thousands of connections a second until the next dead-node retry, which exhausted the
// ephemeral ports of a host.
func TestJoinReplyFromDeadPeerRevivesIt(t *testing.T) {
	a, b := NodeID(uuid.New()), NodeID(uuid.New())
	const addrA, addrB = "127.0.0.1:9101", "127.0.0.1:9102"
	tr := &joinReplyTransport{replies: map[string]*joinReplyMessage{
		addrA: {Accepted: true, NodeID: a, AdvertiseAddr: addrA, Nodes: []joinNode{{ID: b, AdvertiseAddr: addrB}}},
		addrB: {Accepted: true, NodeID: b, AdvertiseAddr: addrB, Nodes: []joinNode{{ID: a, AdvertiseAddr: addrA}}},
	}}
	config := DefaultConfig()
	config.Transport = tr
	config.MsgCodec = codec.NewJSONCodec()
	config.JoinQueueSize = 100

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	for _, p := range []struct {
		id   NodeID
		addr string
	}{{a, addrA}, {b, addrB}} {
		n := newNode(p.id, p.addr)
		n.observedState = NodeDead
		cluster.nodes.addOrUpdate(n)
	}

	// Run the join worker's loop by hand: the queue must drain.
	cluster.joinPeer(addrA)
	joins := 1
	for ; joins < 20; joins++ {
		select {
		case req := <-cluster.joinQueue:
			cluster.joinPeer(req.nodeAddr)
			continue
		default:
		}
		break
	}
	if joins >= 20 {
		t.Fatalf("join queue still not empty after %d joins: the dead peers keep listing each other", joins)
	}
	for _, id := range []NodeID{a, b} {
		if s := cluster.nodes.get(id).GetObservedState(); s != NodeAlive {
			t.Errorf("peer %s is %v after answering a join, want alive", id, s)
		}
	}
}
