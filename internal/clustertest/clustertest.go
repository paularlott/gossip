// Package clustertest builds real multi-node clusters on loopback for the tests
// of the packages layered on the cluster (leader, kv, lock).
//
// Every node gets a free port, brisk failure detection and a faultnet
// transport, so a test can cut a node off (Crash, Net.Cut), split the cluster
// (Split) or shut a node down gracefully (Retire). Nodes stop with the test,
// in parallel (stopping a cluster takes 100 ms for its leave broadcast), so
// tests need no teardown of their own.
package clustertest

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/internal/faultnet"
	"github.com/paularlott/logger"
	"github.com/stretchr/testify/require"
)

// FreeAddr returns a loopback address whose port is free at the time of the
// call. It is only a hint: the port is released before it is returned.
func FreeAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// Option customises the nodes made by NewNode and Build.
type Option func(*settings)

type settings struct {
	addr   string
	meta   func(i int) map[string]string
	config []func(*gossip.Config)
}

// WithAddr sets the node's address instead of a free one, to restart a node in
// place (NewNode only).
func WithAddr(addr string) Option {
	return func(s *settings) { s.addr = addr }
}

// WithMeta sets metadata on every node.
func WithMeta(m map[string]string) Option {
	return WithMetaFunc(func(int) map[string]string { return m })
}

// WithMetaFunc sets each node's metadata from its index. Build waits until
// every node sees the others' metadata.
func WithMetaFunc(fn func(i int) map[string]string) Option {
	return func(s *settings) { s.meta = fn }
}

// WithConfig adjusts the cluster config before the cluster is created.
func WithConfig(fn func(*gossip.Config)) Option {
	return func(s *settings) { s.config = append(s.config, fn) }
}

func apply(opts []Option) *settings {
	s := &settings{}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Node is one running cluster with its fault-injecting transport.
type Node struct {
	Label   string
	Addr    string
	Cluster *gossip.Cluster
	Net     *faultnet.Transport

	mu      sync.Mutex
	stopped bool
	closers []func()
}

func start(t testing.TB, label string, i int, s *settings) *Node {
	t.Helper()
	addr := s.addr
	if addr == "" {
		addr = FreeAddr(t)
	}

	cfg := gossip.DefaultConfig()
	cfg.BindAddr = addr
	cfg.AdvertiseAddr = addr
	cfg.MsgCodec = codec.NewJSONCodec()
	cfg.Logger = logger.NewNullLogger()
	// Brisk failure detection keeps crash tests to a sane duration, and fast
	// gossip ticks drive anti-entropy catch-up quickly.
	cfg.SuspectTimeout = 500 * time.Millisecond
	cfg.DeadNodeTimeout = 2 * time.Second
	cfg.GossipInterval = 250 * time.Millisecond
	for _, fn := range s.config {
		fn(cfg)
	}

	ft := faultnet.New(gossip.NewSocketTransport(cfg), 1)
	cfg.Transport = ft

	c, err := gossip.NewCluster(cfg)
	require.NoError(t, err, "cluster on %s", addr)
	if s.meta != nil {
		for k, v := range s.meta(i) {
			c.LocalMetadata().SetString(k, v)
		}
	}
	c.Start()
	return &Node{Label: label, Addr: addr, Cluster: c, Net: ft}
}

// NewNode starts one node that stops with the test. It does not join anything.
func NewNode(t testing.TB, label string, opts ...Option) *Node {
	t.Helper()
	n := start(t, label, 0, apply(opts))
	t.Cleanup(n.shutdown)
	return n
}

// Build starts count nodes, joins them all to the first and waits until each
// sees every node (and every node's metadata, when WithMeta is used).
func Build(t testing.TB, count int, opts ...Option) []*Node {
	t.Helper()
	s := apply(opts)
	nodes := make([]*Node, 0, count)
	t.Cleanup(func() { StopAll(nodes) })

	for i := range count {
		n := start(t, fmt.Sprintf("n%d", i), i, s)
		nodes = append(nodes, n)
		if i > 0 {
			n.Join(t, nodes[0])
		}
	}
	for _, n := range nodes {
		WaitAlive(t, n.Cluster, count)
	}
	if s.meta != nil {
		for _, n := range nodes {
			for i, m := range nodes {
				WaitMetadata(t, n.Cluster, m.ID(), s.meta(i))
			}
		}
	}
	return nodes
}

// ID returns the node's ID.
func (n *Node) ID() gossip.NodeID { return n.Cluster.LocalNode().ID }

// Join joins the node to seed's cluster.
func (n *Node) Join(t testing.TB, seed *Node) {
	t.Helper()
	require.NoError(t, n.Cluster.Join([]string{seed.Addr}), "%s join", n.Label)
}

// Alive reports whether the node has not been stopped.
func (n *Node) Alive() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.stopped
}

// OnStop registers fn to run when the node stops, before its cluster does.
// Functions run in reverse order of registration.
func (n *Node) OnStop(fn func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closers = append(n.closers, fn)
}

// Crash severs the node from the cluster and then stops it. The transport is
// cut first, so the leave that stopping a cluster announces reaches nobody and
// the peers see only silence: a crash, not a graceful exit.
func (n *Node) Crash() {
	n.Net.Cut()
	n.shutdown()
}

// Retire shuts the node down gracefully: its departure is announced.
func (n *Node) Retire() {
	n.shutdown()
}

func (n *Node) shutdown() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	closers := n.closers
	n.mu.Unlock()

	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	// Stop announces the departure unless the node is already leaving.
	n.Cluster.Stop()
}

// StopAll stops the nodes that are still running, in parallel.
func StopAll(nodes []*Node) {
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.shutdown()
		}()
	}
	wg.Wait()
}

// Split severs every link between the two groups in both directions while the
// nodes inside a group still reach each other. Heal undoes it.
func Split(left, right []*Node) {
	for _, a := range left {
		for _, b := range right {
			a.Net.Block(b.ID())
			a.Net.BlockFrom(b.ID())
			b.Net.Block(a.ID())
			b.Net.BlockFrom(a.ID())
		}
	}
}

// Heal restores the links cut by Split.
func Heal(left, right []*Node) {
	for _, a := range left {
		for _, b := range right {
			a.Net.Unblock(b.ID())
			a.Net.UnblockFrom(b.ID())
			b.Net.Unblock(a.ID())
			b.Net.UnblockFrom(a.ID())
		}
	}
}

// WaitAlive waits until the cluster sees at least want alive nodes.
func WaitAlive(t testing.TB, c *gossip.Cluster, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return c.NumAliveNodes() >= want },
		15*time.Second, 50*time.Millisecond, "cluster stalled at %d alive nodes, wanted %d", c.NumAliveNodes(), want)
}

// WaitMetadata waits until the cluster knows the node with the given metadata.
func WaitMetadata(t testing.TB, c *gossip.Cluster, id gossip.NodeID, want map[string]string) {
	t.Helper()
	require.Eventually(t, func() bool {
		n := c.GetNode(id)
		if n == nil {
			return false
		}
		for k, v := range want {
			if n.Metadata.GetString(k) != v {
				return false
			}
		}
		return true
	}, 15*time.Second, 50*time.Millisecond, "metadata of %s never reached the cluster", id)
}

// Nodes returns n throwaway alive nodes with distinct IDs, for feeding code that
// takes a node list (the baseline tracker) without running a cluster.
func Nodes(n int) []*gossip.Node {
	out := make([]*gossip.Node, 0, n)
	for i := range n {
		out = append(out, gossip.NewTestNode(gossip.NodeID(uuid.New()), fmt.Sprintf("127.0.0.1:%d", 20000+i), nil))
	}
	return out
}
