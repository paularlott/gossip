package gossip

import (
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
	"github.com/stretchr/testify/require"
)

// A forgotten node is not brought back by second-hand reports from nodes that have not forgotten it yet (a
// tombstone or an alive entry in a state exchange, or a join reply's node list); the node itself joining again does
// bring it back.
func TestForgottenNodeStaysForgotten(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Transport = &mockTransport{}
	cfg.MsgCodec = codec.NewJSONCodec()
	cfg.Logger = logger.NewNullLogger()
	c, err := NewCluster(cfg)
	require.NoError(t, err)
	t.Cleanup(c.msgHistory.stop)

	id := NodeID(uuid.New())
	n := newNode(id, "127.0.0.1:1")
	n.observedState = NodeDead
	c.nodes.addOrUpdate(n)
	require.True(t, c.ForgetNode(id))
	require.Nil(t, c.GetNode(id))

	for _, st := range []NodeState{NodeDead, NodeLeaving} {
		c.combineStates([]exchangeNodeState{{ID: id, AdvertiseAddr: "127.0.0.1:1", State: st, StateTimestamp: hlc.Now()}})
		require.Nil(t, c.GetNode(id), "a %s report brought a forgotten node back", st)
	}
	c.combineStates([]exchangeNodeState{{ID: id, AdvertiseAddr: "127.0.0.1:1", State: NodeAlive, StateTimestamp: hlc.Now()}})
	require.Empty(t, c.joinQueue, "a forgotten node was queued for joining from a second-hand report")

	// the node joins us itself: it is back
	p, err := c.createPacketWithTarget(id, nil, nodeJoinMsg, 1, &joinMessage{ID: id, AdvertiseAddr: "127.0.0.1:1",
		ProtocolVersion: PROTOCOL_VERSION, State: NodeAlive})
	require.NoError(t, err)
	_, err = c.handleJoin(nil, p)
	p.Release()
	require.NoError(t, err)
	require.NotNil(t, c.GetNode(id))
	require.False(t, c.isForgotten(id))
}
