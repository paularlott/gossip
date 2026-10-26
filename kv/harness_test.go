package kv_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/internal/clustertest"
	"github.com/paularlott/gossip/kv"
	"github.com/stretchr/testify/require"
)

// node bundles a cluster node and its store.
type node struct {
	*clustertest.Node
	idx   int
	store *kv.Store
	group *gossip.NodeGroup
}

type harnessOpts struct {
	count int

	// metadata returns per-node metadata (e.g. zone tags).
	metadata func(i int) map[string]string

	// groupCriteria scopes every store to a NodeGroup with these criteria
	// instead of the whole cluster.
	groupCriteria map[string]string

	// storeCfg builds each store's config.
	storeCfg func() *kv.Config

	// skipStoreFor marks nodes that get no store.
	skipStoreFor func(i int) bool

	// persister supplies each node's Persister; the same function is used
	// on restart so a restarted node restores from "disk".
	persister func(i int) kv.Persister
}

// attach gives the node its store, scoped by the options. The store (and its
// group) close when the node stops.
func (o harnessOpts) attach(n *node) {
	var membership kv.Membership = kv.ClusterMembership{Cluster: n.Cluster}
	if o.groupCriteria != nil {
		n.group = gossip.NewNodeGroup(n.Cluster, o.groupCriteria, nil)
		n.OnStop(n.group.Close)
		membership = kv.GroupMembership{Group: n.group}
	}
	if o.skipStoreFor != nil && o.skipStoreFor(n.idx) {
		return
	}
	storeCfg := o.storeCfg
	if storeCfg == nil {
		storeCfg = kv.DefaultConfig
	}
	cfg := storeCfg()
	if o.persister != nil {
		cfg.Persister = o.persister(n.idx)
	}
	n.store = kv.NewStore(n.Cluster, membership, cfg)
	n.OnStop(n.store.Close)
}

// build starts count nodes, joins them, and creates each node's store.
func build(t *testing.T, o harnessOpts) []*node {
	t.Helper()

	var opts []clustertest.Option
	if o.metadata != nil {
		opts = append(opts, clustertest.WithMetaFunc(o.metadata))
	}
	nodes := make([]*node, 0, o.count)
	for i, n := range clustertest.Build(t, o.count, opts...) {
		nd := &node{Node: n, idx: i}
		o.attach(nd)
		nodes = append(nodes, nd)
	}
	return nodes
}

// restart replaces a stopped node with a fresh cluster on the same address,
// joined back to seed, with a fresh store.
func restart(t *testing.T, o harnessOpts, old, seed *node) *node {
	t.Helper()

	opts := []clustertest.Option{clustertest.WithAddr(old.Addr)}
	if o.metadata != nil {
		meta := o.metadata(old.idx)
		opts = append(opts, clustertest.WithMeta(meta))
	}
	n := &node{Node: clustertest.NewNode(t, old.Label, opts...), idx: old.idx}
	n.Join(t, seed.Node)
	o.attach(n)
	return n
}

// bases returns the nodes' cluster nodes, for the clustertest helpers.
func bases(nodes []*node) []*clustertest.Node {
	out := make([]*clustertest.Node, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.Node)
	}
	return out
}

// memPersister is an in-memory fake of the Persister interface: records
// saves, can be made to fail, and hands back whatever was last saved.
type memPersister struct {
	mu       sync.Mutex
	snap     *kv.StoreSnapshot
	saves    int
	attempts int
	fail     error
	corrupt  bool // Load fails instead of returning the snapshot
}

func (p *memPersister) Save(snap *kv.StoreSnapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts++
	if p.fail != nil {
		return p.fail
	}
	p.snap = snap
	p.saves++
	return nil
}

func (p *memPersister) Load() (*kv.StoreSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.corrupt {
		return nil, errors.New("unreadable")
	}
	return p.snap, nil
}

func (p *memPersister) saveCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saves
}

func (p *memPersister) attemptCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts
}

func (p *memPersister) setFail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fail = err
}

// waitValue polls until the store returns exactly the wanted value.
func waitValue(t *testing.T, s *kv.Store, key, want string, limit time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		v, ok := s.Get(key)
		return ok && string(v) == want
	}, limit, 50*time.Millisecond, "store %q never converged to %q for %q", s.Name(), want, key)
}

// waitMiss polls until the store reports the key missing.
func waitMiss(t *testing.T, s *kv.Store, key string, limit time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, ok := s.Get(key)
		return !ok
	}, limit, 50*time.Millisecond, "store %q still holds %q", s.Name(), key)
}

// requireErr fails the test unless err is (or wraps) target.
func requireErr(t *testing.T, err, target error) {
	t.Helper()
	require.True(t, errors.Is(err, target), "error = %v, want %v", err, target)
}

// mustSet writes a value without a TTL.
func mustSet(t *testing.T, s *kv.Store, key, value string) {
	t.Helper()
	require.NoError(t, s.Set(key, []byte(value), 0), "set %s", key)
}

// waitConverged waits until every node holds the same value for key, stable
// across two reads, and returns it.
func waitConverged(t *testing.T, nodes []*node, key string, limit time.Duration) string {
	t.Helper()
	var last string
	var seen bool
	require.Eventually(t, func() bool {
		var winner string
		for i, n := range nodes {
			v, ok := n.store.Get(key)
			if !ok || (i > 0 && string(v) != winner) {
				seen = false
				return false
			}
			winner = string(v)
		}
		if seen && winner == last {
			return true
		}
		last, seen = winner, true
		return false
	}, limit, 100*time.Millisecond, "no stable winner for %q", key)
	return last
}
