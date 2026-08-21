package lock_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/internal/cuttable"
	"github.com/paularlott/gossip/leader"
	"github.com/paularlott/gossip/lock"
	"github.com/paularlott/logger"
)

// --- harness ---

// node bundles a cluster, its election and its lock pool.
type node struct {
	cluster   *gossip.Cluster
	election  *leader.LeaderElection
	pool      *lock.Pool
	transport *cuttable.Transport
	label     string
	addr      string
}

// crash severs the node then stops it. The transport is cut first so the Leave()
// that Stop() performs never reaches anyone — peers see only silence, which is
// what a real crash looks like. Without the cut this would be a graceful exit.
func (n *node) crash() {
	if n.transport != nil {
		n.transport.Cut()
	}
	if n.pool != nil {
		n.pool.Close()
	}
	if n.election != nil {
		n.election.Stop()
	}
	if n.cluster != nil {
		n.cluster.Stop()
	}
	n.pool, n.election, n.cluster = nil, nil, nil
}

// retire shuts the node down cleanly, announcing its departure and handing over
// any lock table it owns.
func (n *node) retire() {
	if n.pool != nil {
		n.pool.Close()
	}
	if n.election != nil {
		n.election.Stop()
	}
	if n.cluster != nil {
		n.cluster.Leave()
		n.cluster.Stop()
	}
	n.pool, n.election, n.cluster = nil, nil, nil
}

func newCluster(t *testing.T, addr string) (*gossip.Cluster, *cuttable.Transport) {
	t.Helper()
	cfg := gossip.DefaultConfig()
	cfg.BindAddr = addr
	cfg.AdvertiseAddr = addr
	cfg.MsgCodec = codec.NewJSONCodec()
	cfg.Logger = logger.NewNullLogger()
	// Brisk failure detection keeps crash-recovery tests to a sane duration.
	cfg.SuspectTimeout = 500 * time.Millisecond
	cfg.DeadNodeTimeout = 2 * time.Second
	// Fast gossip ticks drive the lock pool's anti-entropy catch-up quickly.
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
	basePort    int
	count       int
	minSize     int               // leader MinClusterSize
	metadata    map[string]string // set on every node
	criteria    map[string]string // election scoping
	lockCfg     func() *lock.Config
	skipPoolFor func(i int) bool // nodes that get no pool
}

// build starts count nodes, joins them, and waits for a single agreed leader.
func build(t *testing.T, o harnessOpts) []*node {
	t.Helper()

	if o.lockCfg == nil {
		o.lockCfg = func() *lock.Config {
			return &lock.Config{
				Name: "t", MinTTL: 500 * time.Millisecond,
				MaxTTL: 3 * time.Second, RetryInterval: 10 * time.Millisecond,
			}
		}
	}

	seed := fmt.Sprintf("127.0.0.1:%d", o.basePort)
	nodes := make([]*node, 0, o.count)

	for i := 0; i < o.count; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", o.basePort+i)
		c, ct := newCluster(t, addr)

		for k, v := range o.metadata {
			c.LocalMetadata().SetString(k, v)
		}

		c.Start()
		if i > 0 {
			if err := c.Join([]string{seed}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}

		nodes = append(nodes, &node{cluster: c, transport: ct, label: fmt.Sprintf("n%d", i), addr: addr})
	}

	for _, n := range nodes {
		waitClusterSize(t, n.cluster, o.count, 15*time.Second)
	}
	// Let metadata propagate before elections start filtering on it.
	if len(o.metadata) > 0 {
		time.Sleep(1200 * time.Millisecond)
	}

	for i, n := range nodes {
		ec := leader.DefaultConfig()
		ec.LeaderCheckInterval = 100 * time.Millisecond
		ec.LeaderTimeout = 1 * time.Second
		ec.MinClusterSize = o.minSize
		ec.MetadataCriteria = o.criteria

		n.election = leader.NewLeaderElection(n.cluster, ec)
		n.election.Start()

		if o.skipPoolFor == nil || !o.skipPoolFor(i) {
			n.pool = lock.NewPool(n.cluster, n.election, o.lockCfg())
		}
	}

	return nodes
}

func teardown(nodes []*node) {
	for _, n := range nodes {
		if n.pool != nil {
			n.pool.Close()
		}
	}
	for _, n := range nodes {
		if n.election != nil {
			n.election.Stop()
		}
	}
	for _, n := range nodes {
		if n.cluster != nil {
			n.cluster.Stop()
		}
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

// waitOneLeader waits until every live node agrees on a single leader.
func waitOneLeader(t *testing.T, nodes []*node, limit time.Duration) gossip.NodeID {
	t.Helper()
	deadline := time.Now().Add(limit)

	for time.Now().Before(deadline) {
		seen := make(map[gossip.NodeID]int)
		all := true
		for _, n := range nodes {
			if n.election == nil {
				continue
			}
			if !n.election.HasLeader() {
				all = false
				break
			}
			seen[n.election.GetLeaderID()]++
		}
		if all && len(seen) == 1 {
			for id := range seen {
				return id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	for _, n := range nodes {
		if n.election != nil {
			t.Logf("  %s: hasLeader=%v leader=%v", n.label, n.election.HasLeader(), n.election.GetLeaderID())
		}
	}
	t.Fatalf("no single agreed leader within %v", limit)
	return gossip.EmptyNodeID
}

// waitNewLeader waits until every live node agrees on a leader that is not
// excluded. Needed after a crash, because survivors keep pointing at the dead
// node until failure detection fires.
func waitNewLeader(t *testing.T, nodes []*node, excluded gossip.NodeID, limit time.Duration) gossip.NodeID {
	t.Helper()
	deadline := time.Now().Add(limit)

	for time.Now().Before(deadline) {
		seen := make(map[gossip.NodeID]int)
		all := true
		for _, n := range nodes {
			if n.election == nil {
				continue
			}
			if !n.election.HasLeader() {
				all = false
				break
			}
			id := n.election.GetLeaderID()
			if id == excluded {
				all = false
				break
			}
			seen[id]++
		}
		if all && len(seen) == 1 {
			for id := range seen {
				return id
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	for _, n := range nodes {
		if n.election != nil {
			t.Logf("  %s: hasLeader=%v leader=%v", n.label, n.election.HasLeader(), n.election.GetLeaderID())
		}
	}
	t.Fatalf("no agreed replacement leader within %v", limit)
	return gossip.EmptyNodeID
}

// waitGrantable waits until the pool can actually issue locks (leader elected
// and past its warm-up gate). probeTTL must satisfy the pool's MinTTL.
func waitGrantable(t *testing.T, p *lock.Pool, limit time.Duration, probeTTL ...time.Duration) {
	t.Helper()

	ttl := 600 * time.Millisecond
	if len(probeTTL) > 0 {
		ttl = probeTTL[0]
	}

	deadline := time.Now().Add(limit)
	var last error
	for time.Now().Before(deadline) {
		lk, err := p.TryAcquire("__probe__", ttl)
		if err == nil {
			lk.Release()
			return
		}
		last = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("pool never became grantable within %v (last error: %v)", limit, last)
}

func poolOf(nodes []*node, id gossip.NodeID) *node {
	for _, n := range nodes {
		if n.cluster != nil && n.cluster.LocalNode().ID == id {
			return n
		}
	}
	return nil
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
func (m *exclusionMonitor) report() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.notes...)
}

// --- happy paths ---

func TestOneLeaderElectedAndLocksWork(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 19800, count: 3, minSize: 2})
	defer teardown(nodes)

	leaderID := waitOneLeader(t, nodes, 20*time.Second)
	t.Logf("leader: %s", poolOf(nodes, leaderID).label)

	// Exactly one node should consider itself leader.
	leaders := 0
	for _, n := range nodes {
		if n.pool.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaders)
	}

	waitGrantable(t, nodes[0].pool, 20*time.Second)

	// A follower takes the lock; everyone should agree it is held.
	follower := nodes[0]
	if follower.pool.IsLeader() {
		follower = nodes[1]
	}

	lk, err := follower.pool.TryAcquire("shared", 3*time.Second)
	if err != nil {
		t.Fatalf("%s acquire: %v", follower.label, err)
	}

	for _, n := range nodes {
		held, owner, tok, _, err := n.pool.Query("shared")
		if err != nil {
			t.Errorf("%s query: %v", n.label, err)
			continue
		}
		if !held {
			t.Errorf("%s does not see the lock as held", n.label)
		}
		if owner != follower.cluster.LocalNode().ID {
			t.Errorf("%s reports the wrong owner", n.label)
		}
		if !tok.Equal(lk.Token()) {
			t.Errorf("%s reports token %s, expected %s", n.label, tok, lk.Token())
		}
	}

	// Nobody else may take it.
	for _, n := range nodes {
		if n == follower {
			continue
		}
		if _, err := n.pool.TryAcquire("shared", 3*time.Second); err == nil {
			t.Errorf("%s was granted a held lock", n.label)
		}
	}

	if err := lk.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Now somebody else can have it.
	other := nodes[2]
	if other == follower {
		other = nodes[1]
	}
	lk2, err := other.pool.TryAcquire("shared", 3*time.Second)
	if err != nil {
		t.Fatalf("%s acquire after release: %v", other.label, err)
	}
	if !lk2.Token().After(lk.Token()) {
		t.Errorf("re-acquired token %s should outrank %s", lk2.Token(), lk.Token())
	}
	lk2.Release()
}

func TestExtendKeepsLockAgainstContention(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 19810, count: 3, minSize: 2})
	defer teardown(nodes)

	waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	lk, err := nodes[0].pool.TryAcquire("renewed", 1*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	stop := make(chan struct{})
	var stolen atomic.Int64
	var wg sync.WaitGroup

	for _, n := range nodes[1:] {
		wg.Add(1)
		go func(sn *node) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if l, err := sn.pool.TryAcquire("renewed", 1*time.Second); err == nil {
					stolen.Add(1)
					l.Release()
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(n)
	}

	for i := 0; i < 12; i++ {
		time.Sleep(300 * time.Millisecond)
		if err := lk.Extend(1 * time.Second); err != nil {
			t.Errorf("extend %d: %v", i, err)
		}
	}

	close(stop)
	wg.Wait()
	lk.Release()

	if n := stolen.Load(); n != 0 {
		t.Errorf("lock stolen %d times while being renewed", n)
	}
}

func TestExclusionUnderContention(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 19820, count: 4, minSize: 3})
	defer teardown(nodes)

	waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	mon := newMonitor()
	var acquired atomic.Int64
	var wg sync.WaitGroup

	for _, n := range nodes {
		wg.Add(1)
		go func(sn *node) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("c%d", i%3)
				lk, err := sn.pool.TryAcquire(key, 1*time.Second)
				if err != nil {
					continue
				}
				acquired.Add(1)
				if mon.enter(key, sn.label, lk.Token()) {
					time.Sleep(time.Millisecond)
					mon.exit(key, sn.label)
				}
				lk.Release()
			}
		}(n)
	}
	wg.Wait()

	t.Logf("acquired=%d overlaps=%d", acquired.Load(), mon.overlaps())

	if acquired.Load() == 0 {
		t.Fatal("nothing was acquired")
	}
	if v := mon.overlaps(); v != 0 {
		for _, s := range mon.report() {
			t.Errorf("  %s", s)
		}
		t.Fatalf("mutual exclusion broken %d times with a stable leader", v)
	}
}

// The scenario that broke the hashing design: nodes joining and leaving while
// keys are contended. With a single elected authority, joining confers no
// authority, so exclusion must be perfect.
func TestExclusionDuringMembershipChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	const base, churnBase = 19830, 19850

	nodes := build(t, harnessOpts{basePort: base, count: 3, minSize: 2})
	defer teardown(nodes)

	waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	mon := newMonitor()
	var acquired, denied atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// One worker per node: ownership is per-node, so one worker each keeps
	// node-level and worker-level ownership identical and any overlap the
	// monitor sees is a genuine cross-node breach.
	for _, n := range nodes {
		wg.Add(1)
		go func(sn *node) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("churn%d", i%2)
				i++

				lk, err := sn.pool.TryAcquire(key, 1*time.Second)
				if err != nil {
					denied.Add(1)
					continue
				}
				acquired.Add(1)
				if mon.enter(key, sn.label, lk.Token()) {
					time.Sleep(2 * time.Millisecond)
					mon.exit(key, sn.label)
				}
				lk.Release()
			}
		}(n)
	}

	// Churn a joiner in and out repeatedly.
	for round := 0; round < 5; round++ {
		addr := fmt.Sprintf("127.0.0.1:%d", churnBase+round)
		c, _ := newCluster(t, addr)
		c.Start()

		if err := c.Join([]string{fmt.Sprintf("127.0.0.1:%d", base)}); err != nil {
			c.Stop()
			continue
		}

		ec := leader.DefaultConfig()
		ec.LeaderCheckInterval = 100 * time.Millisecond
		ec.LeaderTimeout = 1 * time.Second
		ec.MinClusterSize = 2
		el := leader.NewLeaderElection(c, ec)
		el.Start()
		p := lock.NewPool(c, el, &lock.Config{
			Name: "t", MinTTL: 500 * time.Millisecond,
			MaxTTL: 3 * time.Second, RetryInterval: 10 * time.Millisecond,
		})

		time.Sleep(500 * time.Millisecond)

		p.Close()
		el.Stop()
		c.Leave()
		c.Stop()

		time.Sleep(500 * time.Millisecond)
	}

	close(stop)
	wg.Wait()

	t.Logf("acquired=%d denied=%d overlaps=%d inversions=%d",
		acquired.Load(), denied.Load(), mon.overlaps(), mon.inversions())

	if acquired.Load() == 0 {
		t.Fatal("nothing acquired during churn")
	}
	if v := mon.overlaps(); v != 0 {
		for _, s := range mon.report() {
			t.Errorf("  %s", s)
		}
		t.Fatalf("EXCLUSION BROKEN %d times during churn — joining must not confer authority", v)
	}
}

// --- unhappy paths ---

// Killing the leader abruptly is the case the replication exists for: the new
// leader recovers by merging replica state rather than waiting out MaxTTL, so
// locks issued by the dead leader survive with their tokens and service resumes
// in seconds even when MaxTTL is a minute.
func TestLeaderKilledLocksSurviveAndServiceResumes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	// MaxTTL far beyond any plausible recovery time: under the old warm-up
	// gate design this scenario would stall for the full minute.
	nodes := build(t, harnessOpts{
		basePort: 19860, count: 3, minSize: 2,
		lockCfg: func() *lock.Config {
			return &lock.Config{
				Name: "t", MinTTL: 1 * time.Second,
				MaxTTL: 60 * time.Second, RetryInterval: 10 * time.Millisecond,
			}
		},
	})
	defer func() {
		var live []*node
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		teardown(live)
	}()

	leaderID := waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 20*time.Second, 2*time.Second)

	var followers []*node
	for _, n := range nodes {
		if n.cluster.LocalNode().ID != leaderID {
			followers = append(followers, n)
		}
	}

	keys := []string{"k0", "k1", "k2", "k3", "k4"}
	var lastTokens []lock.Token
	held := map[string]*lock.Lock{}
	for _, k := range keys {
		lk, err := followers[0].pool.TryAcquire(k, 55*time.Second)
		if err != nil {
			t.Fatalf("acquire %s: %v", k, err)
		}
		held[k] = lk
		lastTokens = append(lastTokens, lk.Token())
	}

	// Kill the leader hard: severed first, so no departure is announced and
	// nothing is handed over. Everything the new leader learns must come from
	// the replicas.
	victim := poolOf(nodes, leaderID)
	t.Logf("killing leader %s", victim.label)
	crashStart := time.Now()
	victim.crash()

	// A new leader must emerge among the survivors once the death is detected.
	newLeader := waitNewLeader(t, followers, leaderID, 40*time.Second)
	t.Logf("new leader: %s", poolOf(followers, newLeader).label)

	// Service must resume well inside the 60s MaxTTL.
	waitGrantable(t, followers[0].pool, 45*time.Second, 2*time.Second)
	if elapsed := time.Since(crashStart); elapsed > 45*time.Second {
		t.Errorf("recovery took %v; replication should make this far shorter than MaxTTL", elapsed)
	}

	// Every previously-held key must still be held, by the same owner, under
	// the same token — the grants were durable and were recovered.
	for i, k := range keys {
		held, owner, tok, _, err := followers[1].pool.Query(k)
		if err != nil {
			t.Fatalf("query %s after failover: %v", k, err)
		}
		if !held || owner != followers[0].cluster.LocalNode().ID || !tok.Equal(lastTokens[i]) {
			t.Errorf("key %s did not survive the failover: held=%v owner=%v token=%s (want %s)",
				k, held, owner, tok, lastTokens[i])
		}
	}

	// And the survived locks still exclude others.
	if _, err := followers[1].pool.TryAcquire(keys[0], 2*time.Second); err == nil {
		t.Error("a survived lock must still exclude other nodes")
	}

	// Release them; re-acquire grants a strictly higher token.
	for _, k := range keys {
		if err := held[k].Release(); err != nil {
			t.Fatalf("release %s after failover: %v", k, err)
		}
	}

	for i, k := range keys {
		lk, err := followers[0].pool.TryAcquire(k, 5*time.Second)
		if err != nil {
			t.Fatalf("re-acquire %s: %v", k, err)
		}
		if !lk.Token().After(lastTokens[i]) {
			t.Errorf("key %s: re-acquired token %s does not outrank pre-failover %s",
				k, lk.Token(), lastTokens[i])
		}
		lk.Release()
	}
}

// A graceful leader shutdown keeps locks alive through the successor's
// recovery merge — replicas already hold every acked mutation. With W=3 every
// node is a replica, so this also exercises the higher-W write path.
func TestGracefulLeaderHandoverPreservesLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{
		basePort: 19870, count: 3, minSize: 2,
		lockCfg: func() *lock.Config {
			return &lock.Config{
				Name: "t", MinTTL: 1 * time.Second, WriteReplicas: 3,
				MaxTTL: 60 * time.Second, RetryInterval: 10 * time.Millisecond,
			}
		},
	})
	defer func() {
		var live []*node
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		teardown(live)
	}()

	leaderID := waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 20*time.Second, 2*time.Second)

	var followers []*node
	for _, n := range nodes {
		if n.cluster.LocalNode().ID != leaderID {
			followers = append(followers, n)
		}
	}

	// A follower holds a long lock.
	lk, err := followers[0].pool.TryAcquire("survivor", 55*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	origToken := lk.Token()

	// Graceful leader departure.
	victim := poolOf(nodes, leaderID)
	t.Logf("gracefully retiring leader %s", victim.label)
	victim.retire()

	waitNewLeader(t, followers, leaderID, 40*time.Second)

	// The lock should still be held, with its original token, and still exclude
	// the other node.
	var held bool
	var tok lock.Token
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		h, _, tk, _, err := followers[1].pool.Query("survivor")
		if err == nil && h {
			held, tok = true, tk
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !held {
		t.Error("lock did not survive a graceful leader handover")
	} else if !tok.Equal(origToken) {
		t.Errorf("token changed across handover: %s became %s", origToken, tok)
	}

	if _, err := followers[1].pool.TryAcquire("survivor", 2*time.Second); err == nil {
		t.Error("a survived lock must still exclude other nodes")
	}

	_ = lk.Release()
}

// A two-node cluster with default W=2: every acked grant is on both nodes, so
// when the leader crashes the survivor recovers from its own replica view and
// the locks survive. The election needs the baseline to shrink first (the crash
// looks like a partition), hence the short ShrinkDwell.
func TestTwoNodeClusterLeaderCrashKeepsLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	const base = 19950
	seed := fmt.Sprintf("127.0.0.1:%d", base)

	nodes := make([]*node, 0, 2)
	for i := 0; i < 2; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", base+i)
		c, ct := newCluster(t, addr)
		_ = ct
		c.Start()
		if i > 0 {
			if err := c.Join([]string{seed}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}
		nodes = append(nodes, &node{cluster: c, label: fmt.Sprintf("n%d", i), addr: addr})
	}
	defer func() {
		for _, n := range nodes {
			if n.cluster != nil {
				n.cluster.Stop()
			}
		}
	}()

	for _, n := range nodes {
		waitClusterSize(t, n.cluster, 2, 15*time.Second)
	}

	for _, n := range nodes {
		ec := leader.DefaultConfig()
		ec.LeaderCheckInterval = 100 * time.Millisecond
		ec.LeaderTimeout = 1 * time.Second
		ec.MinClusterSize = 1
		ec.ShrinkDwell = 1 * time.Second // let the survivor shrink past the crash quickly

		n.election = leader.NewLeaderElection(n.cluster, ec)
		n.election.Start()
		n.pool = lock.NewPool(n.cluster, n.election, &lock.Config{
			Name: "t", MinTTL: 1 * time.Second,
			MaxTTL: 60 * time.Second, RetryInterval: 10 * time.Millisecond,
		})
	}

	leaderID := waitOneLeader(t, nodes, 25*time.Second)
	waitGrantable(t, nodes[0].pool, 25*time.Second, 2*time.Second)

	follower := nodes[0]
	if follower.cluster.LocalNode().ID == leaderID {
		follower = nodes[1]
	}
	victim := poolOf(nodes, leaderID)

	lk, err := follower.pool.TryAcquire("gpu", 55*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	orig := lk.Token()

	victim.crash()

	// The survivor must eventually lead (after quorum shrinks) and recover.
	newLeader := waitNewLeader(t, []*node{follower}, leaderID, 30*time.Second)
	if newLeader != follower.cluster.LocalNode().ID {
		t.Fatalf("expected the survivor to lead, got %v", newLeader)
	}

	// The pool may observe the election up to a poll interval later, and its
	// recovery a little after that; Query is transient until then.
	var held bool
	var tok lock.Token
	var lastErr error
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		h, _, tk, _, err := follower.pool.Query("gpu")
		if err == nil {
			held, tok = h, tk
			break
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	if !held {
		t.Fatalf("query after crash never succeeded: %v", lastErr)
	}
	if !tok.Equal(orig) {
		t.Errorf("lock must survive on the survivor's replica view: token=%s (want %s)", tok, orig)
	}

	// And it is usable: releasable, then re-grantable at a higher token.
	if err := lk.Release(); err != nil {
		t.Fatalf("release after crash: %v", err)
	}
	next, err := follower.pool.TryAcquire("gpu", 5*time.Second)
	if err != nil {
		t.Fatalf("re-acquire after crash: %v", err)
	}
	if !next.Token().After(orig) {
		t.Errorf("re-acquired token %s must outrank %s", next.Token(), orig)
	}
	next.Release()
}

// A single-node cluster must serve: W degrades to the leader's own copy, since
// refusing every operation on a one-node deployment serves nobody.
func TestSingleNodeClusterServes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	c, _ := newCluster(t, "127.0.0.1:19955")
	c.Start()
	defer c.Stop()

	ec := leader.DefaultConfig()
	ec.LeaderCheckInterval = 100 * time.Millisecond
	ec.LeaderTimeout = 1 * time.Second
	ec.MinClusterSize = 1

	el := leader.NewLeaderElection(c, ec)
	el.Start()
	defer el.Stop()

	p := lock.NewPool(c, el, &lock.Config{
		Name: "t", MinTTL: 500 * time.Millisecond,
		MaxTTL: 3 * time.Second, RetryInterval: 10 * time.Millisecond,
	})
	defer p.Close()

	waitGrantable(t, p, 15*time.Second)

	lk, err := p.TryAcquire("solo", 2*time.Second)
	if err != nil {
		t.Fatalf("a single-node pool must grant: %v", err)
	}
	if err := lk.Extend(2 * time.Second); err != nil {
		t.Errorf("extend on a single-node pool: %v", err)
	}
	if err := lk.Release(); err != nil {
		t.Errorf("release on a single-node pool: %v", err)
	}
}

// A holder dying should free its locks quickly rather than waiting out the TTL.
func TestHolderDeathFreesLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{
		basePort: 19880, count: 3, minSize: 2,
		lockCfg: func() *lock.Config {
			return &lock.Config{
				Name: "t", MinTTL: 1 * time.Second,
				MaxTTL: 60 * time.Second, RetryInterval: 10 * time.Millisecond,
			}
		},
	})
	defer func() {
		var live []*node
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		teardown(live)
	}()

	leaderID := waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 40*time.Second, 2*time.Second)

	// Pick a holder that is NOT the leader, and take a long lock.
	var holderNode, survivor *node
	for _, n := range nodes {
		if n.cluster.LocalNode().ID != leaderID {
			if holderNode == nil {
				holderNode = n
			} else if survivor == nil {
				survivor = n
			}
		}
	}

	if _, err := holderNode.pool.TryAcquire("orphan", 55*time.Second); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Kill the holder without releasing, and without announcing.
	holderNode.crash()

	// The leader should notice the death and free the lock well inside the 55s
	// TTL. Allow generous time for failure detection.
	freed := false
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		lk, err := survivor.pool.TryAcquire("orphan", 2*time.Second)
		if err == nil {
			lk.Release()
			freed = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if !freed {
		t.Error("a dead holder's lock was not freed ahead of its TTL")
	}
}

// A minority partition must not elect its own leader, so it must not grant
// locks. Simulated by isolating a node so it can see only itself.
func TestMinorityCannotGrantLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	// A single node that believes it belongs to a 3-node cluster.
	c, _ := newCluster(t, "127.0.0.1:19890")
	c.Start()
	defer c.Stop()

	ec := leader.DefaultConfig()
	ec.LeaderCheckInterval = 100 * time.Millisecond
	ec.LeaderTimeout = 1 * time.Second
	ec.MinClusterSize = 2 // needs 2 of 3; it can only see 1

	el := leader.NewLeaderElection(c, ec)
	el.Start()
	defer el.Stop()

	p := lock.NewPool(c, el, &lock.Config{
		Name: "t", MinTTL: 500 * time.Millisecond, MaxTTL: 3 * time.Second,
	})
	defer p.Close()

	// Give it ample opportunity to (wrongly) elect itself.
	time.Sleep(2 * time.Second)

	if el.HasLeader() {
		t.Error("an isolated node must not elect a leader when it cannot reach quorum")
	}
	if p.IsLeader() {
		t.Error("an isolated node must not serve as the lock authority")
	}
	if _, err := p.TryAcquire("k", time.Second); err == nil {
		t.Fatal("a node without quorum must not grant locks")
	}
}

func TestNoLeaderMeansNoGrants(t *testing.T) {
	// Two nodes that think they are part of a 5-node cluster: 2 < 3, no quorum.
	nodes := build(t, harnessOpts{basePort: 19900, count: 2, minSize: 3})
	defer teardown(nodes)

	time.Sleep(2 * time.Second)

	for _, n := range nodes {
		if n.election.HasLeader() {
			t.Errorf("%s elected a leader without quorum", n.label)
		}
		if _, err := n.pool.TryAcquire("k", time.Second); err == nil {
			t.Errorf("%s granted a lock without a leader", n.label)
		}
	}
}

// --- NodeGroup scoping ---

// A pool driven by a group-scoped election must be coordinated by the group's
// leader, and nodes outside the group must still be able to take locks.
func TestNodeGroupScopedPool(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	// Four nodes; the first three are workers, the fourth is not.
	const base = 19910
	seed := fmt.Sprintf("127.0.0.1:%d", base)

	nodes := make([]*node, 0, 4)
	for i := 0; i < 4; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", base+i)
		c, ct := newCluster(t, addr)
		_ = ct
		if i < 3 {
			c.LocalMetadata().SetString("role", "worker")
		} else {
			c.LocalMetadata().SetString("role", "bystander")
		}
		c.Start()
		if i > 0 {
			if err := c.Join([]string{seed}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}
		nodes = append(nodes, &node{cluster: c, label: fmt.Sprintf("n%d", i), addr: addr})
	}
	defer teardown(nodes)

	for _, n := range nodes {
		waitClusterSize(t, n.cluster, 4, 15*time.Second)
	}
	time.Sleep(1500 * time.Millisecond) // metadata propagation

	// Every node runs a worker-scoped election, including the bystander so it
	// can discover the group's leader.
	for _, n := range nodes {
		ec := leader.DefaultConfig()
		ec.LeaderCheckInterval = 100 * time.Millisecond
		ec.LeaderTimeout = 1 * time.Second
		ec.MinClusterSize = 2 // three workers
		ec.MetadataCriteria = map[string]string{"role": "worker"}

		n.election = leader.NewLeaderElection(n.cluster, ec)
		n.election.Start()
		n.pool = lock.NewPool(n.cluster, n.election, &lock.Config{
			Name: "grouped", MinTTL: 500 * time.Millisecond,
			MaxTTL: 3 * time.Second, RetryInterval: 10 * time.Millisecond,
		})
	}

	leaderID := waitOneLeader(t, nodes, 25*time.Second)

	// The leader must be one of the workers, never the bystander.
	bystander := nodes[3]
	if leaderID == bystander.cluster.LocalNode().ID {
		t.Fatal("the group leader must be a group member")
	}
	if bystander.pool.IsLeader() {
		t.Fatal("a non-member must never serve as the group's lock authority")
	}
	t.Logf("group leader: %s", poolOf(nodes, leaderID).label)

	waitGrantable(t, nodes[0].pool, 25*time.Second)

	// The bystander can still take a lock, coordinated by the group's leader.
	lk, err := bystander.pool.TryAcquire("grouped-key", 3*time.Second)
	if err != nil {
		t.Fatalf("a non-member should still be able to lock: %v", err)
	}

	// And it excludes the workers.
	for i := 0; i < 3; i++ {
		if _, err := nodes[i].pool.TryAcquire("grouped-key", 2*time.Second); err == nil {
			t.Errorf("%s was granted a lock held by the bystander", nodes[i].label)
		}
	}

	held, owner, _, _, err := nodes[0].pool.Query("grouped-key")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !held || owner != bystander.cluster.LocalNode().ID {
		t.Errorf("expected the bystander to own the key, got held=%v owner=%v", held, owner)
	}

	lk.Release()
}

// Two differently scoped pools on one cluster must not interfere, even for the
// same key.
func TestGroupAndClusterPoolsAreIndependent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	const base = 19920
	seed := fmt.Sprintf("127.0.0.1:%d", base)

	nodes := make([]*node, 0, 3)
	for i := 0; i < 3; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", base+i)
		c, ct := newCluster(t, addr)
		_ = ct
		if i < 2 {
			c.LocalMetadata().SetString("tier", "db")
		}
		c.Start()
		if i > 0 {
			if err := c.Join([]string{seed}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}
		nodes = append(nodes, &node{cluster: c, label: fmt.Sprintf("n%d", i), addr: addr})
	}
	defer func() {
		for _, n := range nodes {
			if n.pool != nil {
				n.pool.Close()
			}
		}
		for _, n := range nodes {
			if n.election != nil {
				n.election.Stop()
			}
			n.cluster.Stop()
		}
	}()

	for _, n := range nodes {
		waitClusterSize(t, n.cluster, 3, 15*time.Second)
	}
	time.Sleep(1500 * time.Millisecond)

	// Cluster-wide election + pool, plus a db-scoped election + pool, on node 0.
	clusterEC := leader.DefaultConfig()
	clusterEC.LeaderCheckInterval = 100 * time.Millisecond
	clusterEC.LeaderTimeout = time.Second
	clusterEC.MinClusterSize = 2
	clusterEC.HeartbeatMessageType = gossip.ReservedMsgsStart + 40
	clusterEC.ForgetMessageType = gossip.ReservedMsgsStart + 42

	groupEC := leader.DefaultConfig()
	groupEC.LeaderCheckInterval = 100 * time.Millisecond
	groupEC.LeaderTimeout = time.Second
	groupEC.MinClusterSize = 2
	groupEC.MetadataCriteria = map[string]string{"tier": "db"}
	groupEC.HeartbeatMessageType = gossip.ReservedMsgsStart + 41
	groupEC.ForgetMessageType = gossip.ReservedMsgsStart + 43

	var clusterPools, groupPools []*lock.Pool
	for _, n := range nodes {
		ce := leader.NewLeaderElection(n.cluster, clusterEC)
		ce.Start()
		defer ce.Stop()

		ge := leader.NewLeaderElection(n.cluster, groupEC)
		ge.Start()
		defer ge.Stop()

		cp := lock.NewPool(n.cluster, ce, &lock.Config{
			Name: "cluster-pool", MinTTL: 500 * time.Millisecond, MaxTTL: 3 * time.Second,
		})
		gp := lock.NewPool(n.cluster, ge, &lock.Config{
			Name: "group-pool", MinTTL: 500 * time.Millisecond, MaxTTL: 3 * time.Second,
		})
		clusterPools = append(clusterPools, cp)
		groupPools = append(groupPools, gp)
	}
	defer func() {
		for _, p := range clusterPools {
			p.Close()
		}
		for _, p := range groupPools {
			p.Close()
		}
	}()

	waitGrantable(t, clusterPools[0], 25*time.Second)
	waitGrantable(t, groupPools[0], 25*time.Second)

	// The same key in both pools is two separate locks.
	a, err := clusterPools[0].TryAcquire("same-name", 3*time.Second)
	if err != nil {
		t.Fatalf("cluster pool acquire: %v", err)
	}
	b, err := groupPools[1].TryAcquire("same-name", 3*time.Second)
	if err != nil {
		t.Fatalf("group pool should hold the same key independently: %v", err)
	}

	// But within each pool it is still exclusive.
	if _, err := clusterPools[1].TryAcquire("same-name", 2*time.Second); err == nil {
		t.Error("cluster pool key should be exclusive")
	}
	if _, err := groupPools[0].TryAcquire("same-name", 2*time.Second); err == nil {
		t.Error("group pool key should be exclusive")
	}

	a.Release()
	b.Release()
}

// Two pools on one cluster replicate independently: each has its own replica
// store and its own recovery, so a leader crash recovers both and a key held
// in each pool survives in each. The same key name in different pools remains
// two unrelated locks.
func TestTwoPoolsBothSurviveLeaderCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{
		basePort: 19975, count: 3, minSize: 2,
		lockCfg: func() *lock.Config {
			return &lock.Config{
				Name: "a", MinTTL: 1 * time.Second,
				MaxTTL: 60 * time.Second, RetryInterval: 10 * time.Millisecond,
			}
		},
	})
	defer func() {
		var live []*node
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		teardown(live)
	}()

	// A second pool on the same election, on every node (replicas must exist
	// wherever pushes may land).
	poolB := make(map[*node]*lock.Pool, len(nodes))
	for _, n := range nodes {
		poolB[n] = lock.NewPool(n.cluster, n.election, &lock.Config{
			Name: "b", MinTTL: 1 * time.Second,
			MaxTTL: 60 * time.Second, RetryInterval: 10 * time.Millisecond,
		})
	}
	defer func() {
		for _, pb := range poolB {
			pb.Close()
		}
	}()

	leaderID := waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 20*time.Second, 2*time.Second)
	waitGrantable(t, poolB[nodes[0]], 20*time.Second, 2*time.Second)

	var followers []*node
	for _, n := range nodes {
		if n.cluster.LocalNode().ID != leaderID {
			followers = append(followers, n)
		}
	}

	lkA, err := followers[0].pool.TryAcquire("shared-key", 55*time.Second)
	if err != nil {
		t.Fatalf("pool a acquire: %v", err)
	}
	lkB, err := poolB[followers[0]].TryAcquire("shared-key", 55*time.Second)
	if err != nil {
		t.Fatalf("pool b acquire: %v", err)
	}
	tokA, tokB := lkA.Token(), lkB.Token()

	// The same key in the other pool was grantable simultaneously — proof the
	// namespaces are independent.
	if tokA.Equal(tokB) {
		t.Logf("note: pools minted identical tokens %s (same term and counter)", tokA)
	}

	victim := poolOf(nodes, leaderID)
	t.Logf("killing leader %s", victim.label)
	victim.crash()

	waitNewLeader(t, followers, leaderID, 40*time.Second)

	// Both pools must recover and report their lock held with its token.
	for name, check := range map[string]struct {
		tok lock.Token
		q   func() (bool, lock.Token, error)
	}{
		"a": {tokA, func() (bool, lock.Token, error) {
			h, _, tk, _, err := followers[1].pool.Query("shared-key")
			return h, tk, err
		}},
		"b": {tokB, func() (bool, lock.Token, error) {
			h, _, tk, _, err := poolB[followers[1]].Query("shared-key")
			return h, tk, err
		}},
	} {
		var held bool
		var tok lock.Token
		var lastErr error
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			h, tk, err := check.q()
			if err == nil {
				held, tok = h, tk
				break
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
		}
		if !held {
			t.Errorf("pool %s: lock did not survive the failover (last error: %v)", name, lastErr)
			continue
		}
		if !tok.Equal(check.tok) {
			t.Errorf("pool %s: token changed across failover: %s became %s", name, check.tok, tok)
		}
	}

	_ = lkA.Release()
	_ = lkB.Release()
}

// A node that joins after locks exist must become a useful replica promptly
// (catch-up at pool start), and fire-and-forget gossip lost while it was
// partitioned must be healed by the anti-entropy re-gossip once it returns.
func TestLateJoinerCatchesUpAndRegossipHeals(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	const base = 19985

	nodes := build(t, harnessOpts{
		basePort: base, count: 3, minSize: 2,
		lockCfg: func() *lock.Config {
			return &lock.Config{
				Name: "t", MinTTL: 1 * time.Second,
				MaxTTL: 60 * time.Second, RetryInterval: 10 * time.Millisecond,
			}
		},
	})
	defer func() {
		var live []*node
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		teardown(live)
	}()

	waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 20*time.Second, 2*time.Second)

	// Five locks before the late joiner exists.
	var held []*lock.Lock
	for i := 0; i < 5; i++ {
		lk, err := nodes[0].pool.TryAcquire(fmt.Sprintf("early-%d", i), 55*time.Second)
		if err != nil {
			t.Fatalf("acquire early-%d: %v", i, err)
		}
		held = append(held, lk)
	}

	// A fourth node joins late and creates its pool.
	addr := fmt.Sprintf("127.0.0.1:%d", base+10)
	c, ct := newCluster(t, addr)
	c.Start()
	if err := c.Join([]string{fmt.Sprintf("127.0.0.1:%d", base)}); err != nil {
		t.Fatalf("late joiner join: %v", err)
	}
	waitClusterSize(t, c, 4, 15*time.Second)

	ec := leader.DefaultConfig()
	ec.LeaderCheckInterval = 100 * time.Millisecond
	ec.LeaderTimeout = 1 * time.Second
	ec.MinClusterSize = 2
	el := leader.NewLeaderElection(c, ec)
	el.Start()
	joiner := &node{cluster: c, election: el, transport: ct, label: "late", addr: addr}
	joiner.pool = lock.NewPool(c, el, &lock.Config{
		Name: "t", MinTTL: 1 * time.Second,
		MaxTTL: 60 * time.Second, RetryInterval: 10 * time.Millisecond,
	})
	defer c.Stop()
	defer el.Stop()
	defer joiner.pool.Close()

	// Catch-up at pool start should populate the joiner's replica store.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && joiner.pool.ReplicaCount() < 5 {
		time.Sleep(50 * time.Millisecond)
	}
	if got := joiner.pool.ReplicaCount(); got < 5 {
		t.Fatalf("late joiner should catch up to 5 locks, knows %d", got)
	}

	// Partition the joiner (alive and running, just unreachable) and take more
	// locks: their fire-and-forget gossip cannot reach it.
	ct.Cut()
	for i := 0; i < 2; i++ {
		lk, err := nodes[0].pool.TryAcquire(fmt.Sprintf("late-%d", i), 55*time.Second)
		if err != nil {
			t.Fatalf("acquire late-%d while joiner partitioned: %v", i, err)
		}
		held = append(held, lk)
	}

	// Heal the partition: anti-entropy re-gossip must bring the joiner back.
	ct.Uncut()
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && joiner.pool.ReplicaCount() < 7 {
		time.Sleep(100 * time.Millisecond)
	}
	if got := joiner.pool.ReplicaCount(); got < 7 {
		t.Errorf("re-gossip should heal the partitioned replica to 7 locks, knows %d", got)
	}

	for _, lk := range held {
		_ = lk.Release()
	}
}

// --- concurrency / shutdown safety ---

func TestConcurrentTrafficDuringShutdown(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 19930, count: 3, minSize: 2})

	waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for _, n := range nodes {
		wg.Add(1)
		go func(sn *node) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("s%d", i%5)
				i++
				if lk, err := sn.pool.TryAcquire(key, time.Second); err == nil {
					_ = lk.Extend(time.Second)
					_ = lk.Release()
				}
			}
		}(n)
	}

	time.Sleep(400 * time.Millisecond)

	// Close the pools underneath the running traffic. The pool references are
	// deliberately left in place — a closed pool must return ErrPoolClosed
	// rather than become unusable, and clearing the field here would just race
	// with the traffic goroutines.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, n := range nodes {
			n.pool.Close()
			time.Sleep(30 * time.Millisecond)
		}
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		close(stop)
		wg.Wait()
		t.Fatal("closing pools under live traffic deadlocked")
	}

	close(stop)
	wg.Wait()

	for _, n := range nodes {
		if n.election != nil {
			n.election.Stop()
		}
		n.cluster.Stop()
	}
}

func TestBlockingAcquireMakesProgress(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 19940, count: 3, minSize: 2})
	defer teardown(nodes)

	waitOneLeader(t, nodes, 20*time.Second)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	mon := newMonitor()
	var done atomic.Int64
	var wg sync.WaitGroup

	for _, n := range nodes {
		wg.Add(1)
		go func(sn *node) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			for i := 0; i < 8; i++ {
				lk, err := sn.pool.Acquire(ctx, "queued", 1*time.Second)
				if err != nil {
					return
				}
				if mon.enter("queued", sn.label, lk.Token()) {
					time.Sleep(3 * time.Millisecond)
					mon.exit("queued", sn.label)
				}
				done.Add(1)
				lk.Release()
			}
		}(n)
	}
	wg.Wait()

	t.Logf("blocking acquisitions: %d", done.Load())

	if v := mon.overlaps(); v != 0 {
		for _, s := range mon.report() {
			t.Errorf("  %s", s)
		}
		t.Fatalf("blocking Acquire broke exclusion %d times", v)
	}
	if done.Load() < 12 {
		t.Errorf("expected steady progress across waiters, got %d", done.Load())
	}
}
