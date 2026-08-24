package kv

import "github.com/paularlott/gossip"

// Membership resolves the set of nodes a store replicates within — its write
// quorum is drawn from this set, gossip fan-out is scoped to it, and a store
// bound to a group never leaks entries to nodes outside it. The store filters
// what it receives to alive nodes and excludes itself, so implementations may
// return dead nodes or the local node alongside the live members.
//
// Keeping this an interface mirrors the lock package's Leadership: a store can
// be scoped to the whole cluster or to a metadata-defined NodeGroup (a zone)
// without the store caring which.
type Membership interface {
	// Nodes returns the current member nodes of the store's scope.
	Nodes() []*gossip.Node
}

// ClusterMembership scopes a store to every node of the cluster.
type ClusterMembership struct {
	Cluster *gossip.Cluster
}

// Nodes returns the cluster's currently alive nodes.
func (m ClusterMembership) Nodes() []*gossip.Node {
	if m.Cluster == nil {
		return nil
	}
	return m.Cluster.AliveNodes()
}

// GroupMembership scopes a store to the members of a NodeGroup — the nodes
// whose metadata matches the group's criteria, e.g. zone=eu-west. Stores on
// member nodes of a zone replicate within that zone only; their writes never
// cross to other zones.
type GroupMembership struct {
	Group *gossip.NodeGroup
}

// Nodes returns the group's current members.
func (m GroupMembership) Nodes() []*gossip.Node {
	if m.Group == nil {
		return nil
	}
	return m.Group.GetNodes(nil)
}
