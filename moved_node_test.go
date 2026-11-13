package gossip

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// A node that restarts with the same ID at another address (a rescheduled container that kept its data) must be
// reachable again by the nodes that knew it at the old one.
// BUG FIX: Previously a join from a known node ID left its address as it was, so those nodes kept dialling the old
// address for as long as they ran, while still counting the node alive (they heard from it).
func TestMovedNodeIsReachedAtItsNewAddress(t *testing.T) {
	const echoMsg = UserMsg + 1
	echo := func(c *Cluster) {
		require.NoError(t, c.HandleFuncWithReply(echoMsg, func(_ *Node, p *Packet) (interface{}, error) {
			var s string
			if err := p.Unmarshal(&s); err != nil {
				return nil, err
			}
			return c.LocalNode().ID.String() + ":" + s, nil
		}))
	}
	a, b := startLiveCluster(t), startLiveCluster(t)
	id := NodeID(uuid.New())
	moving := startLiveCluster(t, func(c *Config) { c.NodeID = id.String() })
	echo(a)
	echo(b)
	echo(moving)
	joinAndWait(t, a, b)
	joinAndWait(t, a, moving)
	joinAndWait(t, b, moving)
	oldAddr := moving.LocalNode().AdvertisedAddr()

	ask := func(from *Cluster) (string, error) {
		var got string
		err := from.SendToWithResponse(from.GetNode(id), echoMsg, "hi", &got)
		return got, err
	}
	for _, c := range []*Cluster{a, b} {
		got, err := ask(c)
		require.NoError(t, err)
		require.Equal(t, id.String()+":hi", got)
	}

	// The node stops (leaving, as a service does) and comes back with its ID at a new address, knowing one seed only:
	// the other node hears of the move from the moved node's join once the seed's reply lists it.
	moving.Stop()
	moved := startLiveCluster(t, func(c *Config) { c.NodeID = id.String() })
	echo(moved)
	newAddr := moved.LocalNode().AdvertisedAddr()
	require.NotEqual(t, oldAddr, newAddr)
	require.NoError(t, moved.Join([]string{a.LocalNode().AdvertisedAddr()}))

	for name, c := range map[string]*Cluster{"the seed": a, "the other node": b} {
		require.Eventually(t, func() bool {
			return c.GetNode(id).AdvertisedAddr() == newAddr && c.GetNode(id).GetObservedState() == NodeAlive
		}, 10*time.Second, 20*time.Millisecond, "%s still has the moved node at %s (%s), new address %s",
			name, c.GetNode(id).AdvertisedAddr(), c.GetNode(id).GetObservedState(), newAddr)
		got, err := ask(c)
		require.NoError(t, err, "%s cannot reach the moved node", name)
		require.Equal(t, id.String()+":hi", got)
	}
}
