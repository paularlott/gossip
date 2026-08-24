package kv_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/internal/cuttable"
	"github.com/paularlott/gossip/kv"
	"github.com/paularlott/logger"
)

// --- harness ---

// node bundles a cluster and its store.
type node struct {
	cluster   *gossip.Cluster
	store     *kv.Store
	group     *gossip.NodeGroup
	transport *cuttable.Transport
	label     string
	addr      string
}

// crash severs the node then stops it. The transport is cut first so the
// Leave() that Stop() performs never reaches anyone — peers see only
// silence, which is what a real crash looks like.
func (n *node) crash() {
	if n.transport != nil {
		n.transport.Cut()
	}
	if n.store != nil {
		n.store.Close()
	}
	if n.group != nil {
		n.group.Close()
	}
	if n.cluster != nil {
		n.cluster.Stop()
	}
	n.store, n.group, n.cluster = nil, nil, nil
}

// retire shuts the node down cleanly.
func (n *node) retire() {
	if n.store != nil {
		n.store.Close()
	}
	if n.group != nil {
		n.group.Close()
	}
	if n.cluster != nil {
		n.cluster.Leave()
		n.cluster.Stop()
	}
	n.store, n.group, n.cluster = nil, nil, nil
}

func newCluster(t *testing.T, addr string) (*gossip.Cluster, *cuttable.Transport) {
	t.Helper()
	cfg := gossip.DefaultConfig()
	cfg.BindAddr = addr
	cfg.AdvertiseAddr = addr
	cfg.MsgCodec = codec.NewJSONCodec()
	cfg.Logger = logger.NewNullLogger()
	// Brisk failure detection keeps crash tests to a sane duration.
	cfg.SuspectTimeout = 500 * time.Millisecond
	cfg.DeadNodeTimeout = 2 * time.Second
	// Fast gossip ticks drive the store's anti-entropy catch-up quickly.
	cfg.GossipInterval = 250 * time.Millisecond

	ct := cuttable.New(gossip.NewSocketTransport(cfg))
	cfg.Transport = ct

	c, err := gossip.NewCluster(cfg)
	if err != nil {
		t.Fatalf("cluster on %s: %v", addr, err)
	}
	return c, ct
}

type harnessOpts struct {
	basePort int
	count    int

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
	// on rebuild so a restarted node restores from "disk".
	persister func(i int) kv.Persister
}

// memPersister is an in-memory fake of the Persister interface: records
// saves, can be made to fail, and hands back whatever was last saved.
type memPersister struct {
	mu      sync.Mutex
	snap    *kv.StoreSnapshot
	saves   int
	fail    error
	corrupt bool // Load fails instead of returning the snapshot
}

func (p *memPersister) Save(snap *kv.StoreSnapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
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

func (p *memPersister) setFail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fail = err
}

// build starts count nodes, joins them, and creates each node's store.
func build(t *testing.T, o harnessOpts) []*node {
	t.Helper()

	if o.storeCfg == nil {
		o.storeCfg = kv.DefaultConfig
	}

	seed := fmt.Sprintf("127.0.0.1:%d", o.basePort)
	nodes := make([]*node, 0, o.count)

	for i := 0; i < o.count; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", o.basePort+i)
		c, ct := newCluster(t, addr)

		if o.metadata != nil {
			for k, v := range o.metadata(i) {
				c.LocalMetadata().SetString(k, v)
			}
		}

		c.Start()
		if i > 0 {
			if err := c.Join([]string{seed}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}

		nodes = append(nodes, &node{
			cluster:   c,
			transport: ct,
			label:     fmt.Sprintf("n%d", i),
			addr:      addr,
		})
	}

	for _, n := range nodes {
		waitClusterSize(t, n.cluster, o.count, 15*time.Second)
	}
	// Let metadata propagate before node groups start filtering on it.
	if o.metadata != nil {
		time.Sleep(1200 * time.Millisecond)
	}

	for i, n := range nodes {
		var membership kv.Membership = kv.ClusterMembership{Cluster: n.cluster}
		if o.groupCriteria != nil {
			n.group = gossip.NewNodeGroup(n.cluster, o.groupCriteria, nil)
			membership = kv.GroupMembership{Group: n.group}
		}
		if o.skipStoreFor == nil || !o.skipStoreFor(i) {
			cfg := o.storeCfg()
			if o.persister != nil {
				cfg.Persister = o.persister(i)
			}
			n.store = kv.NewStore(n.cluster, membership, cfg)
		}
	}

	return nodes
}

// rebuild restarts a crashed node in place: a fresh cluster on the same
// address, joined back to the seed, with a fresh store.
func rebuild(t *testing.T, o harnessOpts, i int, seedNode *node) *node {
	t.Helper()

	if o.storeCfg == nil {
		o.storeCfg = kv.DefaultConfig
	}

	c, ct := newCluster(t, fmt.Sprintf("127.0.0.1:%d", o.basePort+i))
	if o.metadata != nil {
		for k, v := range o.metadata(i) {
			c.LocalMetadata().SetString(k, v)
		}
	}
	c.Start()
	if err := c.Join([]string{seedNode.addr}); err != nil {
		t.Fatalf("node %d rejoin: %v", i, err)
	}

	n := &node{cluster: c, transport: ct, label: fmt.Sprintf("n%d", i), addr: fmt.Sprintf("127.0.0.1:%d", o.basePort+i)}
	var membership kv.Membership = kv.ClusterMembership{Cluster: c}
	if o.groupCriteria != nil {
		n.group = gossip.NewNodeGroup(c, o.groupCriteria, nil)
		membership = kv.GroupMembership{Group: n.group}
	}
	cfg := o.storeCfg()
	if o.persister != nil {
		cfg.Persister = o.persister(i)
	}
	n.store = kv.NewStore(c, membership, cfg)
	return n
}

func teardown(nodes []*node) {
	for _, n := range nodes {
		n.retire()
	}
}

func waitClusterSize(t *testing.T, c *gossip.Cluster, want int, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if c.NumAliveNodes() >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cluster stalled at %d nodes, wanted %d", c.NumAliveNodes(), want)
}

// waitValue polls until the store returns exactly the wanted value.
func waitValue(t *testing.T, s *kv.Store, key, want string, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if v, ok := s.Get(key); ok && string(v) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	v, _ := s.Get(key)
	t.Fatalf("store %q never converged to %q for %q (last: %q)", s.Name(), want, key, v)
}

// waitMiss polls until the store reports the key missing.
func waitMiss(t *testing.T, s *kv.Store, key string, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if _, ok := s.Get(key); !ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("store %q still holds %q", s.Name(), key)
}

// --- happy paths ---

func TestConvergenceSetUpdateDelete(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21100, count: 3})
	defer teardown(nodes)
	n0, n1, n2 := nodes[0], nodes[1], nodes[2]

	if err := n0.store.Set("k", []byte("v1"), 0); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	waitValue(t, n1.store, "k", "v1", 5*time.Second)
	waitValue(t, n2.store, "k", "v1", 5*time.Second)

	// An update from a different node converges too.
	if err := n1.store.Set("k", []byte("v2"), 0); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	waitValue(t, n0.store, "k", "v2", 5*time.Second)
	waitValue(t, n2.store, "k", "v2", 5*time.Second)

	// And a delete from a third node.
	if err := n2.store.Delete("k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitMiss(t, n0.store, "k", 5*time.Second)
	waitMiss(t, n1.store, "k", 5*time.Second)
}

func TestConcurrentWritesConvergeToSingleWinner(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21110, count: 3})
	defer teardown(nodes)

	// Three nodes write the same key at the same instant; there is no
	// coordinator, so the outcome is decided by the LWW rules. The contract
	// under test: every replica agrees on the same winner.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n *node) {
			defer wg.Done()
			<-start
			if err := n.store.Set("race", []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
				t.Errorf("node %d set: %v", i, err)
			}
		}(i, n)
	}
	close(start)
	wg.Wait()

	// Wait until every node holds the same value, twice in a row.
	var winner string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		vals := make([]string, len(nodes))
		ok := true
		for i, n := range nodes {
			v, found := n.store.Get("race")
			if !found {
				ok = false
				break
			}
			vals[i] = string(v)
		}
		if ok && vals[0] == vals[1] && vals[1] == vals[2] {
			if winner != "" && winner == vals[0] {
				return // stable across two reads
			}
			winner = vals[0]
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no stable winner: %q", winner)
}

func TestTTLExpiryConverges(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21120, count: 3})
	defer teardown(nodes)

	if err := nodes[0].store.Set("fleeting", []byte("v"), 400*time.Millisecond); err != nil {
		t.Fatalf("set: %v", err)
	}
	waitValue(t, nodes[1].store, "fleeting", "v", 5*time.Second)
	waitValue(t, nodes[2].store, "fleeting", "v", 5*time.Second)

	// Expiry is derived from the entry itself, so every node stops serving
	// the key at the same instant without any further messaging.
	time.Sleep(700 * time.Millisecond)
	for i, n := range nodes {
		if _, ok := n.store.Get("fleeting"); ok {
			t.Fatalf("node %d serves expired key", i)
		}
	}
}

func TestDeletePrefixConverges(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21130, count: 3})
	defer teardown(nodes)

	for _, k := range []string{"a/1", "a/2", "b/1"} {
		if err := nodes[0].store.Set(k, []byte("v"), 0); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	for _, k := range []string{"a/1", "a/2", "b/1"} {
		waitValue(t, nodes[2].store, k, "v", 5*time.Second)
	}

	n, err := nodes[1].store.DeletePrefix("a/")
	if err != nil {
		t.Fatalf("delete prefix: %v", err)
	}
	if n != 2 {
		t.Fatalf("DeletePrefix deleted %d keys, want 2", n)
	}

	waitMiss(t, nodes[0].store, "a/1", 5*time.Second)
	waitMiss(t, nodes[0].store, "a/2", 5*time.Second)
	waitMiss(t, nodes[2].store, "a/1", 5*time.Second)
	waitValue(t, nodes[2].store, "b/1", "v", 5*time.Second)
}

func TestSyncExplicit(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21140, count: 3})
	defer teardown(nodes)

	if err := nodes[0].store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}

	if !nodes[2].store.Sync() {
		t.Fatal("Sync reported no peer answered")
	}
	waitValue(t, nodes[2].store, "k", "v", time.Second)
}

func TestNodeRejoinCatchesUp(t *testing.T) {
	opts := harnessOpts{basePort: 21150, count: 3}
	nodes := build(t, opts)
	defer teardown(nodes)

	if err := nodes[0].store.Set("before", []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	waitValue(t, nodes[2].store, "before", "v", 5*time.Second)

	// Crash n2, write more while it is gone, then bring it back.
	nodes[2].crash()
	if err := nodes[0].store.Set("during", []byte("v"), 0); err != nil {
		t.Fatalf("set during downtime: %v", err)
	}
	waitValue(t, nodes[1].store, "during", "v", 5*time.Second)

	nodes[2] = rebuild(t, opts, 2, nodes[0])
	waitClusterSize(t, nodes[2].cluster, 3, 15*time.Second)
	waitValue(t, nodes[2].store, "before", "v", 10*time.Second)
	waitValue(t, nodes[2].store, "during", "v", 10*time.Second)
}

func TestFullSyncBidirectional(t *testing.T) {
	// n0 and n1 form a cluster; n2 starts as a lone node with its own data
	// and joins — the exchange must be a union: n2 learns the cluster's
	// keys, and the cluster learns n2's.
	base := 21160
	n0 := build(t, harnessOpts{basePort: base, count: 1})[0]
	n1 := build(t, harnessOpts{basePort: base + 1, count: 1,
		skipStoreFor: func(i int) bool { return true }})[0]
	defer func() { n0.retire(); n1.retire() }()

	if err := n1.cluster.Join([]string{n0.addr}); err != nil {
		t.Fatalf("n1 join: %v", err)
	}
	waitClusterSize(t, n0.cluster, 2, 15*time.Second)
	n1.store = kv.NewStore(n1.cluster, kv.ClusterMembership{Cluster: n1.cluster}, kv.DefaultConfig())

	if err := n0.store.Set("on-cluster", []byte("v"), 0); err != nil {
		t.Fatalf("set on-cluster: %v", err)
	}
	waitValue(t, n1.store, "on-cluster", "v", 5*time.Second)

	// A lone node that writes before joining.
	n2 := build(t, harnessOpts{basePort: base + 2, count: 1})[0]
	defer n2.retire()
	if err := n2.store.Set("lone-origin", []byte("v"), 0); err != nil {
		t.Fatalf("lone set: %v", err)
	}
	if err := n2.cluster.Join([]string{n0.addr}); err != nil {
		t.Fatalf("n2 join: %v", err)
	}
	waitClusterSize(t, n2.cluster, 3, 15*time.Second)

	waitValue(t, n2.store, "on-cluster", "v", 10*time.Second)
	waitValue(t, n0.store, "lone-origin", "v", 10*time.Second)
	waitValue(t, n1.store, "lone-origin", "v", 10*time.Second)
}

func TestMultipleStoresIsolated(t *testing.T) {
	opts := harnessOpts{basePort: 21170, count: 3,
		storeCfg: func() *kv.Config { c := kv.DefaultConfig(); c.Name = "a"; return c }}
	nodes := build(t, opts)
	defer teardown(nodes)

	// A second, differently named store on the same clusters.
	bStores := make([]*kv.Store, len(nodes))
	for i, n := range nodes {
		c := kv.DefaultConfig()
		c.Name = "b"
		bStores[i] = kv.NewStore(n.cluster, kv.ClusterMembership{Cluster: n.cluster}, c)
	}
	defer func() {
		for _, s := range bStores {
			s.Close()
		}
	}()

	if err := nodes[0].store.Set("k", []byte("in-a"), 0); err != nil {
		t.Fatalf("set in a: %v", err)
	}
	waitValue(t, nodes[1].store, "k", "in-a", 5*time.Second)

	// The entry must not cross into store "b" even after gossip rounds.
	if err := bStores[0].Set("k", []byte("in-b"), 0); err != nil {
		t.Fatalf("set in b: %v", err)
	}
	waitValue(t, bStores[1], "k", "in-b", 5*time.Second)
	waitValue(t, nodes[2].store, "k", "in-a", time.Second)

	if v, ok := bStores[2].Get("k"); !ok || string(v) != "in-b" {
		t.Fatalf("store b lost its own value: %q,%v", v, ok)
	}
	if v, ok := nodes[2].store.Get("k"); !ok || string(v) != "in-a" {
		t.Fatalf("stores cross-contaminated: a sees %q", v)
	}
}

func TestGroupScopedStore(t *testing.T) {
	// Five nodes in one cluster: three tagged zone=alpha, two zone=beta.
	// Stores scoped to a group must converge within the group and never
	// leak across it.
	opts := harnessOpts{
		basePort: 21180, count: 5,
		metadata: func(i int) map[string]string {
			if i < 3 {
				return map[string]string{"zone": "alpha"}
			}
			return map[string]string{"zone": "beta"}
		},
		groupCriteria: map[string]string{"zone": "alpha"},
		storeCfg:      func() *kv.Config { c := kv.DefaultConfig(); c.Name = "scoped"; return c },
	}
	nodes := build(t, opts)
	defer teardown(nodes)

	alpha := nodes[:3]
	beta := nodes[3:]

	// The harness gives every node a store scoped to its own zone via the
	// shared criteria; rebuild beta's stores against a beta criteria instead
	// (same name, different scope — disjoint groups never exchange).
	for _, n := range beta {
		n.store.Close()
		n.group.Close()
		n.group = gossip.NewNodeGroup(n.cluster, map[string]string{"zone": "beta"}, nil)
		n.store = kv.NewStore(n.cluster, kv.GroupMembership{Group: n.group}, opts.storeCfg())
	}

	// Wait for both groups to observe their members.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) &&
		(alpha[0].group.Count() < 3 || beta[0].group.Count() < 2) {
		time.Sleep(50 * time.Millisecond)
	}
	if alpha[0].group.Count() < 3 || beta[0].group.Count() < 2 {
		t.Fatalf("groups not formed: alpha=%d beta=%d", alpha[0].group.Count(), beta[0].group.Count())
	}

	if err := alpha[0].store.Set("alpha-only", []byte("v"), 0); err != nil {
		t.Fatalf("alpha set: %v", err)
	}
	if err := beta[0].store.Set("beta-only", []byte("v"), 0); err != nil {
		t.Fatalf("beta set: %v", err)
	}

	waitValue(t, alpha[1].store, "alpha-only", "v", 5*time.Second)
	waitValue(t, alpha[2].store, "alpha-only", "v", 5*time.Second)
	waitValue(t, beta[1].store, "beta-only", "v", 5*time.Second)

	// Give the groups several gossip rounds to leak, then assert isolation.
	time.Sleep(1500 * time.Millisecond)
	for i, n := range alpha {
		if _, ok := n.store.Get("beta-only"); ok {
			t.Fatalf("alpha node %d sees beta's key", i)
		}
	}
	for i, n := range beta {
		if _, ok := n.store.Get("alpha-only"); ok {
			t.Fatalf("beta node %d sees alpha's key", i)
		}
	}
}

// --- unhappy paths ---

func TestQuorumToleratesOneDown(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21200, count: 3})
	defer teardown(nodes)

	// W=2: the writer plus one peer. Losing one node leaves two ack candidates.
	nodes[1].crash()

	if err := nodes[0].store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("set with one node down: %v", err)
	}
	waitValue(t, nodes[2].store, "k", "v", 5*time.Second)
}

func TestQuorumFailsClosedAndCompensates(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21210, count: 3})
	defer teardown(nodes)

	// Form the group first so the high-water mark is 3.
	if err := nodes[0].store.Set("warm", []byte("v"), 0); err != nil {
		t.Fatalf("warm set: %v", err)
	}
	waitValue(t, nodes[2].store, "warm", "v", 5*time.Second)

	nodes[1].crash()
	nodes[2].crash()

	// W=2 against an observed group of 3: one peer ack is required, and no
	// peer is reachable — the write must be refused, not degraded to local
	// only (the sticky high-water bar).
	err := nodes[0].store.Set("k", []byte("v"), 0)
	if !errors.Is(err, kv.ErrWriteQuorum) {
		t.Fatalf("set error = %v, want ErrWriteQuorum", err)
	}

	// The failed write was compensated: nothing is visible locally.
	if _, ok := nodes[0].store.Get("k"); ok {
		t.Fatal("failed write is visible locally")
	}

	// The bar stays up: still refused later, not just in the instant after
	// the crashes.
	time.Sleep(time.Second)
	if err := nodes[0].store.Set("k2", []byte("v"), 0); !errors.Is(err, kv.ErrWriteQuorum) {
		t.Fatalf("second set error = %v, want ErrWriteQuorum", err)
	}

	// Healing — a rebuilt peer — lets writes flow again, and the compensated
	// key must not resurrect.
	nodes[2] = rebuild(t, harnessOpts{basePort: 21210}, 2, nodes[0])
	waitClusterSize(t, nodes[0].cluster, 2, 15*time.Second)
	if err := nodes[0].store.Set("k3", []byte("v"), 0); err != nil {
		t.Fatalf("set after heal: %v", err)
	}
	waitValue(t, nodes[2].store, "k3", "v", 10*time.Second)
	waitMiss(t, nodes[2].store, "k", 10*time.Second)
}

func TestLoneNodeAcceptsWrites(t *testing.T) {
	// A store that starts alone is in bootstrap mode: its own copy is the
	// whole replica set and writes succeed.
	nodes := build(t, harnessOpts{basePort: 21220, count: 1})
	defer teardown(nodes)

	if err := nodes[0].store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("lone set: %v", err)
	}
	if v, ok := nodes[0].store.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("lone get = %q,%v", v, ok)
	}
	if err := nodes[0].store.Delete("k"); err != nil {
		t.Fatalf("lone delete: %v", err)
	}
	if _, ok := nodes[0].store.Get("k"); ok {
		t.Fatal("lone delete did not take")
	}
	if !nodes[0].store.Synced() {
		t.Fatal("lone store should be vacuously synced")
	}
}

func TestDeleteWithoutQuorumStandsAndConverges(t *testing.T) {
	opts := harnessOpts{basePort: 21230, count: 3}
	nodes := build(t, opts)
	defer teardown(nodes)

	if err := nodes[0].store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	waitValue(t, nodes[1].store, "k", "v", 5*time.Second)

	// Lose the peers; the delete falls short of quorum but stands — the
	// safe direction under uncertainty.
	nodes[1].crash()
	nodes[2].crash()
	if err := nodes[0].store.Delete("k"); !errors.Is(err, kv.ErrWriteQuorum) {
		t.Fatalf("delete error = %v, want ErrWriteQuorum", err)
	}
	if _, ok := nodes[0].store.Get("k"); ok {
		t.Fatal("delete that fell short of quorum did not stand")
	}

	// The tombstone must win once the peer returns and syncs.
	nodes[1] = rebuild(t, opts, 1, nodes[0])
	waitClusterSize(t, nodes[1].cluster, 2, 15*time.Second)
	waitMiss(t, nodes[1].store, "k", 10*time.Second)
}

func TestPartitionHealsByLWW(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21240, count: 3})
	defer teardown(nodes)

	if err := nodes[0].store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	waitValue(t, nodes[2].store, "k", "v", 5*time.Second)

	// Partition n2, let the others move the key on, then heal. LWW picks
	// the later write; the stale copy on n2 loses.
	nodes[2].transport.Cut()
	time.Sleep(600 * time.Millisecond) // let n0/n1 mark n2 suspect or worse

	if err := nodes[0].store.Set("k", []byte("v2"), 0); err != nil {
		t.Fatalf("set during partition: %v", err)
	}
	waitValue(t, nodes[1].store, "k", "v2", 5*time.Second)

	nodes[2].transport.Uncut()
	waitValue(t, nodes[2].store, "k", "v2", 15*time.Second)
}

// --- validation and lifecycle ---

func TestValidationErrors(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21250, count: 1,
		storeCfg: func() *kv.Config {
			c := kv.DefaultConfig()
			c.MaxValueSize = 10
			c.MaxKeys = 2
			return c
		}})
	defer teardown(nodes)
	s := nodes[0].store

	if err := s.Set("", []byte("v"), 0); !errors.Is(err, kv.ErrKeyEmpty) {
		t.Fatalf("empty key error = %v", err)
	}
	if err := s.Set("k", []byte("0123456789A"), 0); !errors.Is(err, kv.ErrValueTooLarge) {
		t.Fatalf("oversize error = %v", err)
	}
	if err := s.Set("k", []byte("v"), -time.Second); !errors.Is(err, kv.ErrTTLOutOfRange) {
		t.Fatalf("negative ttl error = %v", err)
	}
	if err := s.Set("k", []byte("v"), 25*time.Hour); !errors.Is(err, kv.ErrTTLOutOfRange) {
		t.Fatalf("over-max ttl error = %v", err)
	}

	if err := s.Set("k1", []byte("v"), 0); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := s.Set("k2", []byte("v"), 0); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := s.Set("k3", []byte("v"), 0); !errors.Is(err, kv.ErrTooManyKeys) {
		t.Fatalf("key cap error = %v", err)
	}
	// Overwriting an existing key stays within the cap.
	if err := s.Set("k1", []byte("v2"), 0); err != nil {
		t.Fatalf("overwrite at cap: %v", err)
	}
	// Freeing a slot lets a new key in.
	if err := s.Delete("k2"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.Set("k3", []byte("v"), 0); err != nil {
		t.Fatalf("set after freeing slot: %v", err)
	}
}

func TestClosedStore(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21260, count: 1})
	n := nodes[0]
	defer n.retire()

	s := n.store
	s.Close()

	if err := s.Set("k", []byte("v"), 0); !errors.Is(err, kv.ErrStoreClosed) {
		t.Fatalf("set on closed store = %v", err)
	}
	if err := s.Delete("k"); !errors.Is(err, kv.ErrStoreClosed) {
		t.Fatalf("delete on closed store = %v", err)
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("closed store served a value")
	}

	// A fresh store on the same cluster must work (handlers survive).
	s2 := kv.NewStore(n.cluster, kv.ClusterMembership{Cluster: n.cluster}, kv.DefaultConfig())
	defer s2.Close()
	if err := s2.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("set on replacement store: %v", err)
	}
	if err := s2.Delete("k"); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("delete on replacement store: %v", err)
	}
}

func TestDuplicateStoreNamePanics(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21270, count: 1})
	defer teardown(nodes)

	defer func() {
		if recover() == nil {
			t.Fatal("duplicate store name did not panic")
		}
	}()
	kv.NewStore(nodes[0].cluster, kv.ClusterMembership{Cluster: nodes[0].cluster}, kv.DefaultConfig())
}

// fastWaterCfg returns a store config whose water-mark timings are fast
// enough for tests: growth settles quickly, shrinkage dwells briefly.
func fastWaterCfg() *kv.Config {
	c := kv.DefaultConfig()
	c.StabilityPeriod = 300 * time.Millisecond
	c.ShrinkDwell = 600 * time.Millisecond
	return c
}

func TestScaleDownOneAtATimeReopensWrites(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21280, count: 3, storeCfg: fastWaterCfg})
	defer teardown(nodes)

	if err := nodes[0].store.Set("warm", []byte("v"), 0); err != nil {
		t.Fatalf("warm set: %v", err)
	}

	// Drop the first node silently (a crash produces no departure signal).
	// With one peer still alive, W=2 writes continue immediately...
	nodes[2].crash()
	if err := nodes[0].store.Set("after-first-loss", []byte("v"), 0); err != nil {
		t.Fatalf("set with one peer alive: %v", err)
	}

	// ...and after the shrink dwell the water mark itself follows down, so
	// the loss is now absorbed. Drop the second node: with the mark still at
	// 2 the lone node must refuse, then after another dwell it re-opens.
	time.Sleep(2500 * time.Millisecond) // suspect detection + dwell + margin
	nodes[1].crash()

	err := nodes[0].store.Set("lone", []byte("v"), 0)
	if !errors.Is(err, kv.ErrWriteQuorum) {
		t.Fatalf("lone set before dwell = %v, want ErrWriteQuorum", err)
	}

	// One member short of the mark, steady for the dwell: the mark follows,
	// the bar drops, and the surviving node writes again on its own.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := nodes[0].store.Set("lone", []byte("v"), 0); err == nil {
			if v, ok := nodes[0].store.Get("lone"); !ok || string(v) != "v" {
				t.Fatalf("reopened write not readable: %q,%v", v, ok)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("writes never re-opened after one-at-a-time scale-down")
}

func TestGracefulLeaveDropsBarImmediately(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21290, count: 3, storeCfg: fastWaterCfg})
	defer teardown(nodes)

	if err := nodes[0].store.Set("warm", []byte("v"), 0); err != nil {
		t.Fatalf("warm set: %v", err)
	}

	// A graceful Leave() is a positive departure signal: the water mark
	// follows down immediately, with no dwell.
	nodes[2].retire()
	time.Sleep(500 * time.Millisecond) // let the leave broadcast propagate

	// Now the second loss (silent) initially refuses the lone node...
	nodes[1].crash()
	err := nodes[0].store.Set("k", []byte("v"), 0)
	if !errors.Is(err, kv.ErrWriteQuorum) {
		t.Fatalf("lone set = %v, want ErrWriteQuorum", err)
	}

	// ...and after the dwell the group of one is legitimate again.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := nodes[0].store.Set("k", []byte("v"), 0); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("writes never re-opened after graceful drain plus loss")
}

func TestGracefulDrainToOneNode(t *testing.T) {
	// Five nodes drained with clean Leave() events, one at a time, down to
	// a single survivor. The water mark follows each graceful departure
	// immediately, so writes must keep working at every step of the drain —
	// including the last node writing entirely alone.
	nodes := build(t, harnessOpts{basePort: 21300, count: 5})
	defer teardown(nodes)

	if err := nodes[0].store.Set("keeper", []byte("v"), 0); err != nil {
		t.Fatalf("warm set: %v", err)
	}
	for i := 1; i <= 4; i++ {
		if err := nodes[0].store.Set(fmt.Sprintf("step-%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("pre-drain set step %d: %v", i, err)
		}
	}
	waitValue(t, nodes[4].store, "step-4", "v", 5*time.Second)

	// Drain from the top, writing on the intended survivor after each leave.
	for i := 4; i >= 1; i-- {
		nodes[i].retire()
		time.Sleep(400 * time.Millisecond) // leave broadcast propagation

		survivor := nodes[0]
		if err := survivor.store.Set(fmt.Sprintf("after-drain-%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("write after draining node %d: %v", i, err)
		}
	}

	// The last node stands alone: water 1, need 0, and everything written
	// before and during the drain is still there.
	lone := nodes[0].store
	if _, ok := lone.Get("keeper"); !ok {
		t.Fatal("data lost across the drain")
	}
	for i := 1; i <= 4; i++ {
		if _, ok := lone.Get(fmt.Sprintf("after-drain-%d", i)); !ok {
			t.Fatalf("drain-step write %d missing on the survivor", i)
		}
	}
	if err := lone.Set("final-lone-write", []byte("v"), 0); err != nil {
		t.Fatalf("lone write at end of drain: %v", err)
	}
}

func TestIsolatedNodeFailsClosedGroupWrites(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21310, count: 5})
	defer teardown(nodes)
	if err := nodes[0].store.Set("warm", []byte("v"), 0); err != nil {
		t.Fatalf("warm set: %v", err)
	}
	waitValue(t, nodes[4].store, "warm", "v", 5*time.Second)

	// Isolate one node completely — silent loss, indistinguishable from a
	// crash or a partition.
	nodes[4].transport.Cut()

	// The group of four keeps writing: W=2 needs one ack among three peers.
	if err := nodes[0].store.Set("group-key", []byte("v"), 0); err != nil {
		t.Fatalf("group write during isolation: %v", err)
	}
	if err := nodes[2].store.Set("group-key-2", []byte("v"), 0); err != nil {
		t.Fatalf("second group write during isolation: %v", err)
	}

	// The isolated node must fail closed, both while its peers still look
	// alive and after failure detection has marked them.
	if err := nodes[4].store.Set("iso-key", []byte("v"), 0); !errors.Is(err, kv.ErrWriteQuorum) {
		t.Fatalf("isolated set = %v, want ErrWriteQuorum", err)
	}
	time.Sleep(800 * time.Millisecond)
	if err := nodes[4].store.Set("iso-key-2", []byte("v"), 0); !errors.Is(err, kv.ErrWriteQuorum) {
		t.Fatalf("isolated set after detection = %v, want ErrWriteQuorum", err)
	}

	// Heal. The isolated node converges to the group's writes, and its own
	// failed writes must not have taken effect anywhere (compensation).
	nodes[4].transport.Uncut()
	waitValue(t, nodes[4].store, "group-key", "v", 15*time.Second)
	waitValue(t, nodes[4].store, "group-key-2", "v", 15*time.Second)
	waitMiss(t, nodes[4].store, "iso-key", 15*time.Second)
	waitMiss(t, nodes[4].store, "iso-key-2", 15*time.Second)
	waitMiss(t, nodes[0].store, "iso-key", 5*time.Second)
}

func TestSplitPartitionBothSidesWriteHealByLWW(t *testing.T) {
	// A true 2|3 split: both sides satisfy W=2 internally, so both accept
	// writes during the partition — the documented semantics. On heal, LWW
	// must drive every node to the same winner for a contended key, and
	// each side's uncontended keys must flow to the other side.
	nodes := build(t, harnessOpts{basePort: 21320, count: 5})
	defer teardown(nodes)
	if err := nodes[0].store.Set("warm", []byte("v"), 0); err != nil {
		t.Fatalf("warm set: %v", err)
	}
	waitValue(t, nodes[4].store, "warm", "v", 5*time.Second)

	left := nodes[:2]  // n0, n1
	right := nodes[2:] // n2, n3, n4
	var leftIDs, rightIDs []gossip.NodeID
	for _, n := range left {
		leftIDs = append(leftIDs, n.cluster.LocalNode().ID)
	}
	for _, n := range right {
		rightIDs = append(rightIDs, n.cluster.LocalNode().ID)
	}
	for _, n := range left {
		n.transport.CutPeers(rightIDs...)
	}
	for _, n := range right {
		n.transport.CutPeers(leftIDs...)
	}

	// Both sides write through the partition.
	if err := left[0].store.Set("contended", []byte("left-value"), 0); err != nil {
		t.Fatalf("left write during split: %v", err)
	}
	if err := left[1].store.Set("left-only", []byte("v"), 0); err != nil {
		t.Fatalf("left-only write during split: %v", err)
	}
	if err := right[0].store.Set("contended", []byte("right-value"), 0); err != nil {
		t.Fatalf("right write during split: %v", err)
	}
	if err := right[1].store.Set("right-only", []byte("v"), 0); err != nil {
		t.Fatalf("right-only write during split: %v", err)
	}

	// Neither side may see the other's writes while partitioned.
	time.Sleep(500 * time.Millisecond)
	if _, ok := left[0].store.Get("right-only"); ok {
		t.Fatal("partition leaks: left sees right's write")
	}
	if _, ok := right[0].store.Get("left-only"); ok {
		t.Fatal("partition leaks: right sees left's write")
	}

	// Heal.
	for _, n := range left {
		n.transport.UncutPeers(rightIDs...)
	}
	for _, n := range right {
		n.transport.UncutPeers(leftIDs...)
	}

	// The contended key converges to one identical winner on every node,
	// stable across two reads.
	var winner string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		vals := make([]string, len(nodes))
		ok := true
		for i, n := range nodes {
			v, found := n.store.Get("contended")
			if !found {
				ok = false
				break
			}
			vals[i] = string(v)
		}
		if ok && vals[0] == vals[1] && vals[1] == vals[2] && vals[2] == vals[3] && vals[3] == vals[4] {
			if winner != "" && winner == vals[0] {
				break
			}
			winner = vals[0]
		}
		time.Sleep(100 * time.Millisecond)
	}
	if winner == "" {
		t.Fatal("contended key never converged after heal")
	}

	// Uncontended keys from each side flow everywhere.
	for _, n := range nodes {
		waitValue(t, n.store, "left-only", "v", 15*time.Second)
		waitValue(t, n.store, "right-only", "v", 15*time.Second)
	}
}

func TestFlappingPartition(t *testing.T) {
	// Repeated cut/heal cycles on one node: writes fail closed while it is
	// isolated, succeed after each heal, converge to the group, and nothing
	// corrupts along the way.
	nodes := build(t, harnessOpts{basePort: 21330, count: 3})
	defer teardown(nodes)
	if err := nodes[0].store.Set("warm", []byte("v"), 0); err != nil {
		t.Fatalf("warm set: %v", err)
	}
	waitValue(t, nodes[2].store, "warm", "v", 5*time.Second)

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("flap-%d", i)
		nodes[2].transport.Cut()

		if err := nodes[2].store.Set(key, []byte("v"), 0); !errors.Is(err, kv.ErrWriteQuorum) {
			t.Fatalf("flap %d: isolated set = %v, want ErrWriteQuorum", i, err)
		}

		nodes[2].transport.Uncut()
		if err := nodes[2].store.Set(key, []byte("v"), 0); err != nil {
			t.Fatalf("flap %d: healed set: %v", i, err)
		}
		waitValue(t, nodes[0].store, key, "v", 15*time.Second)
		waitValue(t, nodes[1].store, key, "v", 15*time.Second)
	}

	// Everything the flapping node wrote while healed is consistent cluster-wide.
	for _, k := range []string{"warm", "flap-0", "flap-1", "flap-2"} {
		for _, n := range nodes {
			waitValue(t, n.store, k, "v", 5*time.Second)
		}
	}
}

// --- optional persistence ---

// fastSnapCfg makes the snapshot triggers quick for tests.
func fastSnapCfg(writes int, interval time.Duration) *kv.Config {
	c := kv.DefaultConfig()
	c.SnapshotWrites = writes
	c.SnapshotInterval = interval
	return c
}

func TestSnapshotRestoreAfterRestart(t *testing.T) {
	persisters := []*memPersister{{}, {}, {}}
	opts := harnessOpts{basePort: 21400, count: 3,
		storeCfg:  func() *kv.Config { return kv.DefaultConfig() },
		persister: func(i int) kv.Persister { return persisters[i] },
	}
	nodes := build(t, opts)
	defer teardown(nodes)

	// A surviving key, and a deleted one whose tombstone must travel
	// through the snapshot or the delete resurrects from disk.
	if err := nodes[0].store.Set("keep", []byte("v"), 0); err != nil {
		t.Fatalf("set keep: %v", err)
	}
	if err := nodes[0].store.Set("gone", []byte("v"), 0); err != nil {
		t.Fatalf("set gone: %v", err)
	}
	if err := nodes[0].store.Delete("gone"); err != nil {
		t.Fatalf("delete gone: %v", err)
	}
	waitMiss(t, nodes[2].store, "gone", 5*time.Second)

	// Save, then lose the node and its memory; write more while it is down.
	if err := nodes[2].store.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	nodes[2].crash()
	if err := nodes[0].store.Set("during-downtime", []byte("v"), 0); err != nil {
		t.Fatalf("set during downtime: %v", err)
	}
	waitValue(t, nodes[1].store, "during-downtime", "v", 5*time.Second)

	// Restart: the node loads its snapshot from "disk", then catches up.
	nodes[2] = rebuild(t, opts, 2, nodes[0])
	waitClusterSize(t, nodes[2].cluster, 3, 15*time.Second)
	waitValue(t, nodes[2].store, "keep", "v", 10*time.Second)
	waitValue(t, nodes[2].store, "during-downtime", "v", 10*time.Second)
	waitMiss(t, nodes[2].store, "gone", 10*time.Second) // tombstone survived the round trip
}

func TestRestoreOlderThanClusterState(t *testing.T) {
	// A snapshot older than the live cluster must lose to the cluster's
	// fresher writes on restore.
	persisters := []*memPersister{{}, {}, {}}
	opts := harnessOpts{basePort: 21410, count: 3,
		storeCfg:  func() *kv.Config { return kv.DefaultConfig() },
		persister: func(i int) kv.Persister { return persisters[i] },
	}
	nodes := build(t, opts)
	defer teardown(nodes)

	if err := nodes[0].store.Set("shared", []byte("old"), 0); err != nil {
		t.Fatalf("set old: %v", err)
	}
	waitValue(t, nodes[2].store, "shared", "old", 5*time.Second)

	if err := nodes[2].store.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	nodes[2].crash()

	if err := nodes[0].store.Set("shared", []byte("new"), 0); err != nil {
		t.Fatalf("set new: %v", err)
	}
	waitValue(t, nodes[1].store, "shared", "new", 5*time.Second)

	nodes[2] = rebuild(t, opts, 2, nodes[0])
	waitClusterSize(t, nodes[2].cluster, 3, 15*time.Second)
	waitValue(t, nodes[2].store, "shared", "new", 10*time.Second)
}

func TestFullOutageRestoreUnion(t *testing.T) {
	// Every node crashes with its own snapshot; on restart each loads its
	// disk and the group converges to the union.
	persisters := []*memPersister{{}, {}}
	opts := harnessOpts{basePort: 21420, count: 2,
		storeCfg:  func() *kv.Config { return kv.DefaultConfig() },
		persister: func(i int) kv.Persister { return persisters[i] },
	}
	nodes := build(t, opts)

	if err := nodes[0].store.Set("from-n0", []byte("v"), 0); err != nil {
		t.Fatalf("set from-n0: %v", err)
	}
	if err := nodes[1].store.Set("from-n1", []byte("v"), 0); err != nil {
		t.Fatalf("set from-n1: %v", err)
	}
	waitValue(t, nodes[0].store, "from-n1", "v", 5*time.Second)
	waitValue(t, nodes[1].store, "from-n0", "v", 5*time.Second)

	if err := nodes[0].store.Snapshot(); err != nil {
		t.Fatalf("snapshot n0: %v", err)
	}
	if err := nodes[1].store.Snapshot(); err != nil {
		t.Fatalf("snapshot n1: %v", err)
	}
	nodes[0].crash()
	nodes[1].crash()

	// Total outage: both come back with nothing but their disks.
	nodes[0] = rebuild(t, opts, 0, nodes[1]) // seed via n1's address book
	waitClusterSize(t, nodes[0].cluster, 1, 5*time.Second)
	waitValue(t, nodes[0].store, "from-n0", "v", 5*time.Second)

	nodes[1] = rebuild(t, opts, 1, nodes[0])
	waitClusterSize(t, nodes[0].cluster, 2, 15*time.Second)
	waitValue(t, nodes[0].store, "from-n1", "v", 10*time.Second)
	waitValue(t, nodes[1].store, "from-n0", "v", 10*time.Second)
}

func TestSnapshotTriggersCountIntervalAndIdle(t *testing.T) {
	p := &memPersister{}
	nodes := build(t, harnessOpts{basePort: 21430, count: 1,
		storeCfg:  func() *kv.Config { return fastSnapCfg(3, 400*time.Millisecond) },
		persister: func(i int) kv.Persister { return p },
	})
	defer teardown(nodes)
	s := nodes[0].store

	// Three writes reach the write-count trigger.
	for i := 0; i < 3; i++ {
		if err := s.Set(fmt.Sprintf("k%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("set k%d: %v", i, err)
		}
	}
	waitFor(t, func() bool { return p.saveCount() >= 1 }, 5*time.Second, "write-count trigger")

	// Clean store: the interval must not fire on its own.
	time.Sleep(900 * time.Millisecond)
	if got := p.saveCount(); got != 1 {
		t.Fatalf("idle store saved %d times, want 1", got)
	}

	// One write is below the count trigger but the interval catches it.
	if err := s.Set("k3", []byte("v"), 0); err != nil {
		t.Fatalf("set k3: %v", err)
	}
	waitFor(t, func() bool { return p.saveCount() >= 2 }, 5*time.Second, "interval trigger")

	// Clean again: still exactly two.
	time.Sleep(900 * time.Millisecond)
	if got := p.saveCount(); got != 2 {
		t.Fatalf("idle store saved %d times after interval, want 2", got)
	}
}

func TestCloseFlushesDirtyStore(t *testing.T) {
	// Triggers are far away (defaults); only the close-time flush saves.
	p := &memPersister{}
	n := build(t, harnessOpts{basePort: 21440, count: 1,
		storeCfg:  func() *kv.Config { return kv.DefaultConfig() },
		persister: func(i int) kv.Persister { return p },
	})[0]

	if err := n.store.Set("survives", []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	n.retire() // Close flushes the dirty store

	if p.saveCount() != 1 {
		t.Fatalf("close flush saved %d times, want 1", p.saveCount())
	}

	// A fresh store on a fresh cluster restores from the same "disk".
	n2 := build(t, harnessOpts{basePort: 21441, count: 1,
		storeCfg:  func() *kv.Config { return kv.DefaultConfig() },
		persister: func(i int) kv.Persister { return p },
	})[0]
	defer n2.retire()
	waitValue(t, n2.store, "survives", "v", time.Second)
}

func TestSnapshotSaveRetriesAfterFailure(t *testing.T) {
	p := &memPersister{}
	p.setFail(errors.New("disk full"))
	nodes := build(t, harnessOpts{basePort: 21450, count: 1,
		storeCfg:  func() *kv.Config { return fastSnapCfg(2, time.Hour) },
		persister: func(i int) kv.Persister { return p },
	})
	defer teardown(nodes)
	s := nodes[0].store

	// The write-count trigger fires but the save fails; the dirty count is
	// restored, so nothing is considered persisted.
	for i := 0; i < 2; i++ {
		if err := s.Set(fmt.Sprintf("k%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("set k%d: %v", i, err)
		}
	}
	time.Sleep(1 * time.Second)
	if p.saveCount() != 0 {
		t.Fatalf("failing persister recorded %d saves, want 0", p.saveCount())
	}

	// The Persister recovers; one more write re-triggers with the carried
	// dirty count and the save lands.
	p.setFail(nil)
	if err := s.Set("k2", []byte("v"), 0); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	waitFor(t, func() bool { return p.saveCount() >= 1 }, 5*time.Second, "retry after recovery")
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, cond func() bool, limit time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestLoadErrorStartsEmpty(t *testing.T) {
	p := &memPersister{}
	p.setFail(nil)
	p.corrupt = true // Load returns an error
	nodes := build(t, harnessOpts{basePort: 21460, count: 1,
		storeCfg:  func() *kv.Config { return kv.DefaultConfig() },
		persister: func(i int) kv.Persister { return p },
	})
	defer teardown(nodes)

	// A failed load must not prevent the store from working normally.
	if err := nodes[0].store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("set after failed load: %v", err)
	}
	waitValue(t, nodes[0].store, "k", "v", time.Second)
}

func TestLoadWrongStoreNameIgnored(t *testing.T) {
	p := &memPersister{snap: &kv.StoreSnapshot{Store: "other-store",
		Entries: []*kv.Entry{{Key: "k", Version: 1, Origin: [16]byte{1}, Value: []byte("v")}}}}
	nodes := build(t, harnessOpts{basePort: 21470, count: 1,
		storeCfg:  func() *kv.Config { return kv.DefaultConfig() },
		persister: func(i int) kv.Persister { return p },
	})
	defer teardown(nodes)

	// The snapshot belongs to another store: ignored, not merged.
	if _, ok := nodes[0].store.Get("k"); ok {
		t.Fatal("foreign store's snapshot leaked into this store")
	}
}
