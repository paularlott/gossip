package kv_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/internal/clustertest"
	"github.com/paularlott/gossip/kv"
	"github.com/stretchr/testify/require"
)

func TestConvergenceSetUpdateDelete(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3})
	n0, n1, n2 := nodes[0], nodes[1], nodes[2]

	mustSet(t, n0.store, "k", "v1")
	waitValue(t, n1.store, "k", "v1", 5*time.Second)
	waitValue(t, n2.store, "k", "v1", 5*time.Second)

	// An update from a different node converges too.
	mustSet(t, n1.store, "k", "v2")
	waitValue(t, n0.store, "k", "v2", 5*time.Second)
	waitValue(t, n2.store, "k", "v2", 5*time.Second)

	// And a delete from a third node.
	require.NoError(t, n2.store.Delete("k"))
	waitMiss(t, n0.store, "k", 5*time.Second)
	waitMiss(t, n1.store, "k", 5*time.Second)
}

func TestConcurrentWritesConvergeToSingleWinner(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3})

	// Three nodes write the same key at the same instant; there is no
	// coordinator, so the outcome is decided by the LWW rules. The contract
	// under test: every replica agrees on the same winner.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := n.store.Set("race", []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
				t.Errorf("node %d set: %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	waitConverged(t, nodes, "race", 10*time.Second)
}

func TestTTLExpiryConverges(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3})

	require.NoError(t, nodes[0].store.Set("fleeting", []byte("v"), 400*time.Millisecond))
	waitValue(t, nodes[1].store, "fleeting", "v", 5*time.Second)
	waitValue(t, nodes[2].store, "fleeting", "v", 5*time.Second)

	// Expiry is derived from the entry itself, so every node stops serving
	// the key without any further messaging.
	for _, n := range nodes {
		waitMiss(t, n.store, "fleeting", 3*time.Second)
	}
}

func TestDeletePrefixConverges(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3})

	for _, k := range []string{"a/1", "a/2", "b/1"} {
		mustSet(t, nodes[0].store, k, "v")
	}
	for _, k := range []string{"a/1", "a/2", "b/1"} {
		waitValue(t, nodes[2].store, k, "v", 5*time.Second)
	}

	n, err := nodes[1].store.DeletePrefix("a/")
	require.NoError(t, err)
	require.Equal(t, 2, n, "DeletePrefix deleted the wrong number of keys")

	waitMiss(t, nodes[0].store, "a/1", 5*time.Second)
	waitMiss(t, nodes[0].store, "a/2", 5*time.Second)
	waitMiss(t, nodes[2].store, "a/1", 5*time.Second)
	waitValue(t, nodes[2].store, "b/1", "v", 5*time.Second)
}

func TestSyncExplicit(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3})

	mustSet(t, nodes[0].store, "k", "v")

	require.True(t, nodes[2].store.Sync(), "Sync reported no peer answered")
	waitValue(t, nodes[2].store, "k", "v", time.Second)
}

func TestNodeRejoinCatchesUp(t *testing.T) {
	opts := harnessOpts{count: 3}
	nodes := build(t, opts)

	mustSet(t, nodes[0].store, "before", "v")
	waitValue(t, nodes[2].store, "before", "v", 5*time.Second)

	// Crash n2, write more while it is gone, then bring it back.
	nodes[2].Crash()
	mustSet(t, nodes[0].store, "during", "v")
	waitValue(t, nodes[1].store, "during", "v", 5*time.Second)

	nodes[2] = restart(t, opts, nodes[2], nodes[0])
	waitValue(t, nodes[2].store, "before", "v", 10*time.Second)
	waitValue(t, nodes[2].store, "during", "v", 10*time.Second)
}

func TestFullSyncBidirectional(t *testing.T) {
	// n0 and n1 form a cluster; n2 starts as a lone node with its own data
	// and joins: the exchange must be a union, n2 learns the cluster's keys,
	// and the cluster learns n2's.
	n0 := build(t, harnessOpts{count: 1})[0]
	n1 := &node{Node: clustertest.NewNode(t, "n1"), idx: 1}
	n1.Join(t, n0.Node)
	clustertest.WaitAlive(t, n0.Cluster, 2)
	harnessOpts{}.attach(n1)

	// n0's water mark has not grown to two yet, so this write is gossiped to
	// n1 rather than waiting for its ack.
	mustSet(t, n0.store, "on-cluster", "v")
	waitValue(t, n1.store, "on-cluster", "v", 5*time.Second)

	// A lone node that writes before joining.
	n2 := build(t, harnessOpts{count: 1})[0]
	mustSet(t, n2.store, "lone-origin", "v")
	n2.Join(t, n0.Node)

	waitValue(t, n2.store, "on-cluster", "v", 10*time.Second)
	waitValue(t, n0.store, "lone-origin", "v", 10*time.Second)
	waitValue(t, n1.store, "lone-origin", "v", 10*time.Second)
}

func TestMultipleStoresIsolated(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3,
		storeCfg: func() *kv.Config { c := kv.DefaultConfig(); c.Name = "a"; return c }})

	// A second, differently named store on the same clusters.
	bStores := make([]*kv.Store, len(nodes))
	for i, n := range nodes {
		c := kv.DefaultConfig()
		c.Name = "b"
		bStores[i] = kv.NewStore(n.Cluster, kv.ClusterMembership{Cluster: n.Cluster}, c)
		n.OnStop(bStores[i].Close)
	}

	mustSet(t, nodes[0].store, "k", "in-a")
	waitValue(t, nodes[1].store, "k", "in-a", 5*time.Second)

	// The entry must not cross into store "b" even after gossip rounds.
	mustSet(t, bStores[0], "k", "in-b")
	waitValue(t, bStores[1], "k", "in-b", 5*time.Second)
	waitValue(t, nodes[2].store, "k", "in-a", time.Second)

	waitValue(t, bStores[2], "k", "in-b", time.Second)
	v, ok := nodes[2].store.Get("k")
	require.True(t, ok)
	require.Equal(t, "in-a", string(v), "the stores cross-contaminated")
}

func TestGroupScopedStore(t *testing.T) {
	// Five nodes in one cluster: three tagged zone=alpha, two zone=beta.
	// Stores scoped to a group must converge within the group and never
	// leak across it.
	opts := harnessOpts{
		count: 5,
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

	alpha := nodes[:3]
	beta := nodes[3:]

	// The harness gives every node a store scoped to the alpha criteria;
	// rebuild beta's stores against a beta criteria instead (same name,
	// different scope: disjoint groups never exchange).
	for _, n := range beta {
		n.store.Close()
		n.group.Close()
		n.group = gossip.NewNodeGroup(n.Cluster, map[string]string{"zone": "beta"}, nil)
		n.OnStop(n.group.Close)
		n.store = kv.NewStore(n.Cluster, kv.GroupMembership{Group: n.group}, opts.storeCfg())
		n.OnStop(n.store.Close)
	}

	// Wait for both groups to observe their members.
	require.Eventually(t, func() bool {
		return alpha[0].group.Count() >= 3 && beta[0].group.Count() >= 2
	}, 10*time.Second, 50*time.Millisecond, "groups not formed")

	mustSet(t, alpha[0].store, "alpha-only", "v")
	mustSet(t, beta[0].store, "beta-only", "v")

	waitValue(t, alpha[1].store, "alpha-only", "v", 5*time.Second)
	waitValue(t, alpha[2].store, "alpha-only", "v", 5*time.Second)
	waitValue(t, beta[1].store, "beta-only", "v", 5*time.Second)

	// Give the groups several gossip rounds (250 ms each) to leak, then
	// assert isolation.
	time.Sleep(750 * time.Millisecond)
	for i, n := range alpha {
		_, ok := n.store.Get("beta-only")
		require.False(t, ok, "alpha node %d sees beta's key", i)
	}
	for i, n := range beta {
		_, ok := n.store.Get("alpha-only")
		require.False(t, ok, "beta node %d sees alpha's key", i)
	}
}
