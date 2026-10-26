package gossip

import (
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeMethods(t *testing.T) {
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")

	assert.True(t, node.Alive())
	assert.False(t, node.Suspect())
	assert.False(t, node.DeadOrLeft())
	assert.False(t, node.Removed())
	assert.Equal(t, NodeAlive, node.GetObservedState())
	assert.Equal(t, "127.0.0.1:8001", node.AdvertisedAddr())

	for _, tt := range []struct {
		state                          NodeState
		alive, suspect, deadOrLeft, rm bool
		routable                       bool
	}{
		{NodeAlive, true, false, false, false, true},
		{NodeSuspect, false, true, false, false, true},
		{NodeLeaving, false, false, true, false, false},
		{NodeDead, false, false, true, false, false},
		{NodeRemoved, false, false, false, true, false},
	} {
		node.observedState = tt.state
		assert.Equal(t, tt.alive, node.Alive(), "%s Alive", tt.state)
		assert.Equal(t, tt.suspect, node.Suspect(), "%s Suspect", tt.state)
		assert.Equal(t, tt.deadOrLeft, node.DeadOrLeft(), "%s DeadOrLeft", tt.state)
		assert.Equal(t, tt.rm, node.Removed(), "%s Removed", tt.state)
		assert.Equal(t, tt.routable, node.Routable(), "%s Routable", tt.state)
	}

	before := node.getLastActivity()
	node.updateLastActivity()
	assert.True(t, node.getLastActivity().After(before), "activity is recorded")

	// Its resolved address is kept until cleared; the advertised address is not touched.
	require.True(t, node.IsAddressEmpty())
	node.SetAddress(Address{IP: net.ParseIP("127.0.0.1"), Port: 8001})
	assert.False(t, node.IsAddressEmpty())
	got := node.GetAddress()
	assert.True(t, got.IP.Equal(net.ParseIP("127.0.0.1")))
	assert.Equal(t, 8001, got.Port)
	node.ClearAddress()
	assert.True(t, node.IsAddressEmpty())
	assert.Equal(t, "127.0.0.1:8001", node.AdvertisedAddr())
}

func TestNodeTags(t *testing.T) {
	none := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	assert.Empty(t, none.GetTags())
	assert.False(t, none.HasTag("web"))

	node := newNodeWithTags(NodeID(uuid.New()), "127.0.0.1:8001", []string{"web", "api"})
	assert.ElementsMatch(t, []string{"web", "api"}, node.GetTags())
	assert.True(t, node.HasTag("web"))
	assert.True(t, node.HasTag("api"))
	assert.False(t, node.HasTag("db"))
	assert.False(t, node.HasTag(""))

	tags := node.GetTags()
	tags[0] = "modified"
	assert.False(t, node.HasTag("modified"), "GetTags returns a copy")
}

func TestNodeStateString(t *testing.T) {
	for state, want := range map[NodeState]string{
		NodeUnknown: "Unknown", NodeAlive: "Alive", NodeSuspect: "Suspect", NodeDead: "Dead",
		NodeLeaving: "Leaving", NodeRemoved: "Removed", NodeState(99): "Unknown",
	} {
		assert.Equal(t, want, state.String())
	}
}

func TestAddress(t *testing.T) {
	tests := []struct {
		name    string
		addr    Address
		str     string
		isEmpty bool
	}{
		{"ip and port", Address{IP: net.ParseIP("10.0.0.1"), Port: 8080}, "10.0.0.1:8080", false},
		{"ipv4 bytes", Address{IP: []byte{127, 0, 0, 1}, Port: 8080}, "127.0.0.1:8080", false},
		{"url only", Address{URL: "https://example.com/gossip"}, "https://example.com/gossip", false},
		{"ip, port and url", Address{IP: net.ParseIP("10.0.0.1"), Port: 8080, URL: "https://example.com"}, "10.0.0.1:8080, https://example.com", false},
		{"empty", Address{}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.str, tt.addr.String())
			assert.Equal(t, tt.isEmpty, tt.addr.IsEmpty())
			tt.addr.Clear()
			assert.True(t, tt.addr.IsEmpty(), "cleared")
		})
	}
}

func TestAddressIsEmpty(t *testing.T) {
	for _, tt := range []struct {
		addr  Address
		empty bool
	}{
		{Address{}, true},
		{Address{IP: net.ParseIP("127.0.0.1")}, false},
		{Address{Port: 8080}, false},
		{Address{URL: "http://test"}, false},
	} {
		assert.Equal(t, tt.empty, tt.addr.IsEmpty(), "%+v", tt.addr)
	}
}

func TestClusterRoutableNodes(t *testing.T) {
	c := newUnitCluster(t)
	want := []*Node{c.localNode}
	for _, state := range []NodeState{NodeAlive, NodeAlive, NodeSuspect, NodeLeaving, NodeDead} {
		n := clusterNode(c, state)
		if state == NodeAlive || state == NodeSuspect {
			want = append(want, n)
		}
	}
	assert.ElementsMatch(t, want, c.RoutableNodes())
}
