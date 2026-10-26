package lock_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/internal/clustertest"
	"github.com/paularlott/gossip/leader"
	"github.com/paularlott/gossip/lock"
	"github.com/stretchr/testify/require"
)

// node bundles a cluster node, its election and its lock pool.
type node struct {
	*clustertest.Node
	election *leader.LeaderElection
	pool     *lock.Pool
}

type harnessOpts struct {
	count    int
	minSize  int                           // leader MinClusterSize
	metadata func(i int) map[string]string // per-node metadata
	criteria map[string]string             // election scoping
	lockCfg  func() *lock.Config
	election func(*leader.Config) // adjusts each node's election config
}

func defaultLockCfg() *lock.Config {
	return &lock.Config{
		Name: "t", MinTTL: 500 * time.Millisecond,
		MaxTTL: 3 * time.Second, RetryInterval: 10 * time.Millisecond,
	}
}

// longLockCfg is a pool config with a MaxTTL far beyond any plausible recovery
// time, for the failover tests.
func longLockCfg() *lock.Config {
	return &lock.Config{
		Name: "t", MinTTL: 1 * time.Second,
		MaxTTL: 60 * time.Second, RetryInterval: 10 * time.Millisecond,
	}
}

// attach starts an election and a lock pool on the node; both stop with it.
func (o harnessOpts) attach(n *node) {
	ec := leader.DefaultConfig()
	ec.LeaderCheckInterval = 100 * time.Millisecond
	ec.LeaderTimeout = 1 * time.Second
	ec.MinClusterSize = o.minSize
	ec.MetadataCriteria = o.criteria
	if o.election != nil {
		o.election(ec)
	}
	lockCfg := o.lockCfg
	if lockCfg == nil {
		lockCfg = defaultLockCfg
	}

	n.election = leader.NewLeaderElection(n.Cluster, ec)
	n.election.Start()
	n.OnStop(n.election.Stop)
	n.pool = lock.NewPool(n.Cluster, n.election, lockCfg())
	n.OnStop(n.pool.Close)
}

// build starts count nodes, joins them, and starts an election and a pool on
// each. Tests that need a leader wait for it with waitOneLeader.
func build(t *testing.T, o harnessOpts) []*node {
	t.Helper()

	var opts []clustertest.Option
	if o.metadata != nil {
		opts = append(opts, clustertest.WithMetaFunc(o.metadata))
	}
	nodes := make([]*node, 0, o.count)
	for _, n := range clustertest.Build(t, o.count, opts...) {
		nd := &node{Node: n}
		o.attach(nd)
		nodes = append(nodes, nd)
	}
	return nodes
}

// waitOneLeader waits until every live node agrees on a single leader.
func waitOneLeader(t *testing.T, nodes []*node) gossip.NodeID {
	t.Helper()
	return waitLeader(t, nodes, gossip.EmptyNodeID, 20*time.Second)
}

// waitNewLeader waits until every live node agrees on a leader that is not
// excluded. Needed after a crash, because survivors keep pointing at the dead
// node until failure detection fires.
func waitNewLeader(t *testing.T, nodes []*node, excluded gossip.NodeID) gossip.NodeID {
	t.Helper()
	return waitLeader(t, nodes, excluded, 40*time.Second)
}

func waitLeader(t *testing.T, nodes []*node, excluded gossip.NodeID, limit time.Duration) gossip.NodeID {
	t.Helper()
	var agreed gossip.NodeID
	ok := assertEventually(t, func() bool {
		seen := make(map[gossip.NodeID]int)
		for _, n := range nodes {
			if !n.Alive() {
				continue
			}
			if !n.election.HasLeader() {
				return false
			}
			id := n.election.GetLeaderID()
			if id == excluded {
				return false
			}
			seen[id]++
		}
		for id := range seen {
			agreed = id
		}
		return len(seen) == 1
	}, limit)
	if !ok {
		for _, n := range nodes {
			if n.Alive() {
				t.Logf("  %s: hasLeader=%v leader=%v", n.Label, n.election.HasLeader(), n.election.GetLeaderID())
			}
		}
		t.Fatalf("no single agreed leader within %v", limit)
	}
	return agreed
}

// assertEventually reports whether cond became true within limit.
func assertEventually(t *testing.T, cond func() bool, limit time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

// waitGrantable waits until the pool can actually issue locks (leader elected
// and past its warm-up gate). probeTTL must satisfy the pool's MinTTL.
func waitGrantable(t *testing.T, p *lock.Pool, limit time.Duration, probeTTL ...time.Duration) {
	t.Helper()

	ttl := 600 * time.Millisecond
	if len(probeTTL) > 0 {
		ttl = probeTTL[0]
	}

	var last error
	require.Eventually(t, func() bool {
		lk, err := p.TryAcquire("__probe__", ttl)
		if err != nil {
			last = err
			return false
		}
		lk.Release()
		return true
	}, limit, 50*time.Millisecond, "pool never became grantable within %v (last error: %v)", limit, last)
}

func nodeOf(nodes []*node, id gossip.NodeID) *node {
	for _, n := range nodes {
		if n.ID() == id {
			return n
		}
	}
	return nil
}

// others returns the nodes other than the one with the given ID.
func others(nodes []*node, id gossip.NodeID) []*node {
	var out []*node
	for _, n := range nodes {
		if n.ID() != id {
			out = append(out, n)
		}
	}
	return out
}

// exclusionMonitor flags any instant where two different nodes hold one key, and
// separately flags fencing-token inversions.
type exclusionMonitor struct {
	mu        sync.Mutex
	holders   map[string]holder
	overlap   atomic.Int64
	inversion atomic.Int64
	notes     []string
}

type holder struct {
	label string
	token lock.Token
}

func newMonitor() *exclusionMonitor {
	return &exclusionMonitor{holders: make(map[string]holder)}
}

func (m *exclusionMonitor) enter(key, label string, tok lock.Token) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cur, held := m.holders[key]; held {
		m.overlap.Add(1)
		if !tok.After(cur.token) {
			m.inversion.Add(1)
		}
		if len(m.notes) < 15 {
			m.notes = append(m.notes, fmt.Sprintf(
				"overlap on %q: %s token=%s vs %s token=%s (ordered=%v)",
				key, cur.label, cur.token, label, tok, tok.After(cur.token)))
		}
		return false
	}
	m.holders[key] = holder{label: label, token: tok}
	return true
}

func (m *exclusionMonitor) exit(key, label string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.holders[key]; ok && h.label == label {
		delete(m.holders, key)
	}
}

func (m *exclusionMonitor) overlaps() int64   { return m.overlap.Load() }
func (m *exclusionMonitor) inversions() int64 { return m.inversion.Load() }

// requireExclusive fails the test if the monitor saw two holders of one key.
func (m *exclusionMonitor) requireExclusive(t *testing.T, what string) {
	t.Helper()
	if v := m.overlap.Load(); v != 0 {
		m.mu.Lock()
		for _, s := range m.notes {
			t.Errorf("  %s", s)
		}
		m.mu.Unlock()
		t.Fatalf("%s: mutual exclusion broken %d times", what, v)
	}
}
