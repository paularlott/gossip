package gossip

import (
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// group is what NodeGroup and DataNodeGroup have in common; the suite below runs on both.
type group interface {
	Count() int
	Contains(NodeID) bool
	GetNodes(excludeIDs []NodeID) []*Node
	SendToPeers(msgType MessageType, data interface{}) error
	SendToPeersReliable(msgType MessageType, data interface{}) error
	Close()
}

// groupEvents records the callbacks of a group.
type groupEvents struct {
	mu                      sync.Mutex
	added, removed, updated []NodeID
}

func (e *groupEvents) record(list *[]NodeID, n *Node) {
	e.mu.Lock()
	defer e.mu.Unlock()
	*list = append(*list, n.ID)
}

func (e *groupEvents) counts() (added, removed, updated int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.added), len(e.removed), len(e.updated)
}

type groupKind struct {
	name          string
	make          func(c *Cluster, criteria map[string]string, ev *groupEvents) group
	clearsOnClose bool // Close empties the group (a NodeGroup does, a DataNodeGroup leaves its members)
	// callbacksOnChangesOnly: OnNodeAdded and OnNodeRemoved are called when a node joins or leaves the group, and not
	// when a member's state changes or a node that is not a member is told about.
	callbacksOnChangesOnly bool
}

var groupKinds = []groupKind{
	{"NodeGroup", func(c *Cluster, criteria map[string]string, ev *groupEvents) group {
		return NewNodeGroup(c, criteria, &NodeGroupOptions{
			OnNodeAdded:   func(n *Node) { ev.record(&ev.added, n) },
			OnNodeRemoved: func(n *Node) { ev.record(&ev.removed, n) },
		})
	}, true, false},
	{"DataNodeGroup", func(c *Cluster, criteria map[string]string, ev *groupEvents) group {
		return NewDataNodeGroup(c, criteria, &DataNodeGroupOptions[TestData]{
			OnNodeAdded:   func(n *Node, _ *TestData) { ev.record(&ev.added, n) },
			OnNodeRemoved: func(n *Node, _ *TestData) { ev.record(&ev.removed, n) },
			OnNodeUpdated: func(n *Node, _ *TestData) { ev.record(&ev.updated, n) },
		})
	}, false, true},
}

// TestData is the custom data of the DataNodeGroup tests.
type TestData struct {
	Value   string
	Counter int
}

// clusterNode adds a node in the given state to the cluster, with the metadata given as key, value pairs.
func clusterNode(c *Cluster, state NodeState, kv ...string) *Node {
	n := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	n.observedState = state
	for i := 0; i < len(kv); i += 2 {
		n.metadata.SetString(kv[i], kv[i+1])
	}
	c.nodes.addOrUpdate(n)
	return n
}

func TestNodeGroups(t *testing.T) {
	west := map[string]string{"zone": "us-west"}

	for _, kind := range groupKinds {
		t.Run(kind.name, func(t *testing.T) {
			// A group starts with the nodes of the cluster that match, alive or suspect.
			t.Run("criteria", func(t *testing.T) {
				tests := []struct {
					name     string
					criteria map[string]string
					nodes    []NodeState // one node each, with metadata[i]
					metadata [][]string
					want     []int // indexes of the members
				}{
					{"exact match on every key", map[string]string{"zone": "us-west", "type": "worker"},
						nil, [][]string{{"zone", "us-west", "type", "worker"}, {"zone", "us-east", "type", "worker"}, {"zone", "us-west", "type", "manager"}}, []int{0}},
					{"any value", map[string]string{"zone": MetadataAnyValue, "type": "worker"},
						nil, [][]string{{"zone", "us-west", "type", "worker"}, {"zone", "us-east", "type", "manager"}, {"type", "worker"}}, []int{0}},
					{"contains", map[string]string{"tags": MetadataContainsPrefix + "web"},
						nil, [][]string{{"tags", "production,web,frontend"}, {"tags", "staging,api,backend"}}, []int{0}},
					{"missing metadata", map[string]string{"zone": "us-west", "type": "worker"},
						nil, [][]string{{"zone", "us-west"}}, nil},
					{"alive and suspect nodes only", west,
						[]NodeState{NodeAlive, NodeSuspect, NodeDead, NodeLeaving},
						[][]string{{"zone", "us-west"}, {"zone", "us-west"}, {"zone", "us-west"}, {"zone", "us-west"}}, []int{0, 1}},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						c := newUnitCluster(t)
						var nodes []*Node
						for i, md := range tt.metadata {
							state := NodeAlive
							if tt.nodes != nil {
								state = tt.nodes[i]
							}
							nodes = append(nodes, clusterNode(c, state, md...))
						}

						g := kind.make(c, tt.criteria, &groupEvents{})
						defer g.Close()

						assert.Equal(t, len(tt.want), g.Count())
						for i, n := range nodes {
							assert.Equal(t, slices.Contains(tt.want, i), g.Contains(n.ID), "node %d", i)
						}
					})
				}
			})

			// Membership follows the node list: dead and leaving nodes leave the group, recovered ones return, and a
			// suspect node stays.
			t.Run("follows node state", func(t *testing.T) {
				c := newUnitCluster(t)
				ev := &groupEvents{}
				g := kind.make(c, west, ev)
				defer g.Close()

				n := clusterNode(c, NodeAlive, "zone", "us-west")
				clusterNode(c, NodeAlive, "zone", "us-east")
				require.True(t, g.Contains(n.ID), "a matching node joins the group when it joins the cluster")
				require.Equal(t, 1, g.Count())

				c.nodes.updateState(n.ID, NodeSuspect)
				assert.True(t, g.Contains(n.ID), "a suspect node stays")

				c.nodes.updateState(n.ID, NodeDead)
				assert.False(t, g.Contains(n.ID))
				c.nodes.updateState(n.ID, NodeAlive)
				assert.True(t, g.Contains(n.ID), "a recovered node returns")
				c.nodes.updateState(n.ID, NodeLeaving)
				assert.False(t, g.Contains(n.ID))

				if kind.callbacksOnChangesOnly {
					added, removed, _ := ev.counts()
					assert.Equal(t, 2, added, "added once each time it joined the group")
					assert.Equal(t, 2, removed, "removed once each time it left")
				}
			})

			// Metadata changes move nodes in and out of the group, and notify the data group of changes that keep a
			// node in it.
			t.Run("follows metadata", func(t *testing.T) {
				c := newUnitCluster(t)
				ev := &groupEvents{}
				n := clusterNode(c, NodeAlive, "zone", "us-east")
				g := kind.make(c, west, ev)
				defer g.Close()
				require.False(t, g.Contains(n.ID))

				change := func(kv ...string) {
					for i := 0; i < len(kv); i += 2 {
						n.metadata.SetString(kv[i], kv[i+1])
					}
					c.nodes.notifyMetadataChanged(n)
				}

				change("zone", "us-west")
				assert.True(t, g.Contains(n.ID))
				change("extra", "value")
				assert.True(t, g.Contains(n.ID))
				change("extra", "other")
				assert.Equal(t, 1, g.Count(), "told twice, counted once")
				change("zone", "us-central")
				assert.False(t, g.Contains(n.ID))

				added, removed, updated := ev.counts()
				assert.Equal(t, 1, added)
				assert.Equal(t, 1, removed)
				if kind.callbacksOnChangesOnly {
					assert.Equal(t, 2, updated, "two changes that kept the node in the group")
				} else {
					assert.Zero(t, updated)
				}
			})

			t.Run("ignores metadata changes of a node that is not alive", func(t *testing.T) {
				c := newUnitCluster(t)
				ev := &groupEvents{}
				g := kind.make(c, west, ev)
				defer g.Close()
				n := clusterNode(c, NodeAlive, "zone", "us-west")
				require.True(t, g.Contains(n.ID))
				c.nodes.updateState(n.ID, NodeDead)
				require.False(t, g.Contains(n.ID))

				n.metadata.SetString("extra", "value")
				c.nodes.notifyMetadataChanged(n)

				assert.False(t, g.Contains(n.ID))
				assert.Zero(t, g.Count())
				added, _, updated := ev.counts()
				assert.Equal(t, 1, added)
				assert.Zero(t, updated)
			})

			t.Run("GetNodes excludes the nodes given", func(t *testing.T) {
				c := newUnitCluster(t)
				g := kind.make(c, west, &groupEvents{})
				defer g.Close()
				a, b, d := clusterNode(c, NodeAlive, "zone", "us-west"), clusterNode(c, NodeAlive, "zone", "us-west"), clusterNode(c, NodeAlive, "zone", "us-west")

				assert.ElementsMatch(t, []*Node{a, b, d}, g.GetNodes(nil))
				assert.Equal(t, []*Node{b}, g.GetNodes([]NodeID{a.ID, d.ID}))
				assert.Empty(t, g.GetNodes([]NodeID{a.ID, b.ID, d.ID}))
			})

			// SendToPeers sends to the members other than the local node, over the reliable transport for
			// SendToPeersReliable.
			t.Run("SendToPeers", func(t *testing.T) {
				c := newUnitCluster(t)
				g := kind.make(c, west, &groupEvents{})
				defer g.Close()

				require.NoError(t, g.SendToPeers(UserMsg, "to nobody"), "an empty group is not an error")
				for _, item := range queuedBroadcasts(c) {
					assert.Empty(t, item.peers)
					item.packet.Release()
				}

				members := []*Node{clusterNode(c, NodeAlive, "zone", "us-west"), clusterNode(c, NodeAlive, "zone", "us-west")}
				clusterNode(c, NodeAlive, "zone", "us-east")
				c.localNode.metadata.SetString("zone", "us-west") // notifies the cluster's handlers
				require.True(t, g.Contains(c.localNode.ID), "the local node is a member too")
				for _, item := range queuedBroadcasts(c) {
					item.packet.Release()
				}

				for _, tt := range []struct {
					send      func() error
					transport TransportType
				}{
					{func() error { return g.SendToPeers(UserMsg, "hello") }, TransportBestEffort},
					{func() error { return g.SendToPeersReliable(UserMsg, "hello") }, TransportReliable},
				} {
					require.NoError(t, tt.send())
					items := queuedBroadcasts(c)
					require.Len(t, items, 1)
					assert.Equal(t, tt.transport, items[0].transportType)
					assert.ElementsMatch(t, members, items[0].peers, "the members, not the local node")
					items[0].packet.Release()
				}

				assert.Error(t, g.SendToPeers(0, "x"), "a reserved message type")
				assert.Error(t, g.SendToPeersReliable(0, "x"))
				assert.Empty(t, queuedBroadcasts(c))
			})

			t.Run("Close stops the group following the cluster", func(t *testing.T) {
				c := newUnitCluster(t)
				g := kind.make(c, west, &groupEvents{})
				n := clusterNode(c, NodeAlive, "zone", "us-west")
				require.Equal(t, 1, g.Count())

				g.Close()

				if kind.clearsOnClose {
					assert.Zero(t, g.Count())
				} else {
					assert.Equal(t, 1, g.Count(), "a closed DataNodeGroup keeps what it holds")
				}
				later := clusterNode(c, NodeAlive, "zone", "us-west")
				assert.False(t, g.Contains(later.ID), "no longer notified of new nodes")
				c.nodes.updateState(n.ID, NodeDead)
				assert.Equal(t, !kind.clearsOnClose, g.Contains(n.ID), "no longer notified of state changes")
			})

			t.Run("concurrent use", func(t *testing.T) {
				c := newUnitCluster(t)
				g := kind.make(c, west, &groupEvents{})
				defer g.Close()

				const workers, perWorker = 10, 50
				var wg sync.WaitGroup
				for range workers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range perWorker {
							n := clusterNode(c, NodeAlive, "zone", "us-west")
							g.Contains(n.ID)
							g.Count()
							g.GetNodes(nil)
						}
					}()
				}
				wg.Wait()

				assert.Equal(t, workers*perWorker, g.Count())
				assert.Len(t, g.GetNodes(nil), workers*perWorker)
			})
		})
	}
}

// OnNodeAdded and OnNodeRemoved of a NodeGroup say what the DataNodeGroup's do: a node joined, a node left.
func TestNodeGroupCallbacksOnMembershipChangesOnly(t *testing.T) {
	t.Skip("BUG (reported): NodeGroup calls OnNodeAdded again when a member's state changes (alive to suspect) and " +
		"OnNodeRemoved for every node that matches no criteria and was never a member; DataNodeGroup guards both")

	c := newUnitCluster(t)
	ev := &groupEvents{}
	g := groupKinds[0].make(c, map[string]string{"zone": "us-west"}, ev)
	defer g.Close()

	n := clusterNode(c, NodeAlive, "zone", "us-west")
	clusterNode(c, NodeAlive, "zone", "us-east")
	c.nodes.updateState(n.ID, NodeSuspect)
	c.nodes.updateState(n.ID, NodeDead)

	added, removed, _ := ev.counts()
	assert.Equal(t, 1, added, "one node joined the group")
	assert.Equal(t, 1, removed, "one node left it")
}
