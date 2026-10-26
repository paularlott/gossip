package partition

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/logger"
)

// freeAddr returns a localhost address whose TCP and UDP ports are both free.
func freeAddr(t *testing.T) string {
	t.Helper()
	for range 100 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		udp, err := net.ListenPacket("udp", addr)
		ln.Close()
		if err != nil {
			continue
		}
		udp.Close()
		return addr
	}
	t.Fatal("no free port")
	return ""
}

// newRealCluster starts a gossip cluster on the socket transport on addr.
func newRealCluster(t *testing.T, addr string) *gossip.Cluster {
	t.Helper()
	cfg := gossip.DefaultConfig()
	cfg.BindAddr = addr
	cfg.AdvertiseAddr = addr
	cfg.Logger = logger.NewNullLogger()
	if testing.Verbose() {
		cfg.Logger = tLogger{t: t, prefix: "CLUSTER "}
	}
	// Brisk failure detection keeps the crash part of the test short.
	cfg.SuspectTimeout = 500 * time.Millisecond
	cfg.DeadNodeTimeout = 2 * time.Second
	cfg.GossipInterval = 250 * time.Millisecond
	cfg.Transport = gossip.NewSocketTransport(cfg)
	c, err := gossip.NewCluster(cfg)
	if err != nil {
		t.Fatalf("cluster on %s: %v", addr, err)
	}
	c.Start()
	return c
}

type realNode struct {
	c     *gossip.Cluster
	m     *Manager
	store *memStore
}

// TestIntegrationThreeRealClusters runs the protocol over three clusters on localhost: every partition gets an
// owner, a node that starts late catches up over a real stream, writes through every node converge on all three, and
// after one node stops writes still succeed with two of three.
func TestIntegrationThreeRealClusters(t *testing.T) {
	const parts = 8
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	nodes := make([]*realNode, 3)
	for i := range nodes {
		c := newRealCluster(t, addrs[i])
		n := &realNode{c: c, store: newMemStore()}
		nodes[i] = n
		if i > 0 {
			if err := c.Join([]string{addrs[0]}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}
		m, err := New(c, Config{
			Partitions:        parts,
			ClusterSize:       3,
			HeartbeatInterval: 100 * time.Millisecond,
			OwnerTimeout:      400 * time.Millisecond,
			Tuning:            Tuning{BackoffMax: 200 * time.Millisecond, ReadyHoldDown: 300 * time.Millisecond},
			WriteTimeout:      8 * time.Second,
			Store:             n.store,
			Logger:            testLogger(t).With("node", i),
		})
		if err != nil {
			t.Fatal(err)
		}
		n.m = m
		t.Cleanup(c.Stop)
		t.Cleanup(m.Stop)
	}
	for _, n := range nodes {
		waitFor(t, 20*time.Second, "three alive nodes", func() bool { return n.c.NumAliveNodes() == 3 })
	}
	// Two of the three start first, so that the third has to catch up over a real stream when it joins.
	for _, n := range nodes[:2] {
		if err := n.m.Start(); err != nil {
			t.Fatal(err)
		}
	}
	// A second Manager on the same cluster is refused rather than panicking.
	if m2, err := New(nodes[0].c, Config{ClusterSize: 3, Store: newMemStore()}); err != nil {
		t.Fatal(err)
	} else if err := m2.Start(); err == nil {
		t.Fatal("a second Manager on one cluster was started")
	}

	owners := func(live ...*realNode) func() bool {
		return func() bool {
			for p := 0; p < parts; p++ {
				count := 0
				var owner gossip.NodeID
				for _, n := range live {
					if n.m.IsOwner(p) {
						count++
						owner = n.c.LocalNode().ID
					}
				}
				if count != 1 {
					return false
				}
				for _, n := range live {
					if o, ok := n.m.Owner(p); !ok || o != owner {
						return false
					}
				}
			}
			return true
		}
	}
	waitFor(t, 30*time.Second, "every partition to have an owner among the first two", owners(nodes[:2]...))

	write := func(n *realNode, p int, change string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			_, err := n.m.Write(t.Context(), p, []byte(change))
			if err == nil {
				return
			}
			// Between owners: nothing was written, try again (an unknown outcome is not retried).
			if errors.Is(err, ErrUnavailable) && time.Now().Before(deadline) {
				continue
			}
			t.Fatalf("Write(%d, %q): %v", p, change, err)
		}
	}
	same := func(p int, live ...*realNode) func() bool {
		return func() bool {
			ref := live[0].store.data(p)
			for _, n := range live[1:] {
				if !slices.Equal(ref, n.store.data(p)) {
					return false
				}
			}
			return true
		}
	}

	for p := 0; p < parts; p++ {
		for i, n := range nodes[:2] {
			write(n, p, fmt.Sprintf("early-p%d-n%d", p, i))
		}
	}
	// The third node starts now, empty: it must fetch what it missed before it is ready.
	if err := nodes[2].m.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "the late node to be ready", func() bool { return nodes[2].m.Ready() })
	for p := 0; p < parts; p++ {
		if d := nodes[2].store.data(p); len(d) < 2 {
			t.Fatalf("p%d: late node ready with %v, want what the others committed", p, d)
		}
	}
	waitFor(t, 30*time.Second, "every partition to have an owner", owners(nodes...))

	// Writes through every node, to every partition.
	for p := 0; p < parts; p++ {
		for i, n := range nodes {
			write(n, p, fmt.Sprintf("p%d-n%d", p, i))
		}
	}
	for p := 0; p < parts; p++ {
		waitFor(t, 15*time.Second, fmt.Sprintf("p%d to converge on all three", p), same(p, nodes...))
		if d := nodes[0].store.data(p); len(d) != 5 {
			t.Fatalf("p%d: data %v, want 5 writes", p, d)
		}
	}

	// One node goes away: the other two still have a majority of 3.
	nodes[2].m.Stop()
	nodes[2].c.Stop()
	live := nodes[:2]
	waitFor(t, 30*time.Second, "owners among the two survivors", owners(live...))
	for p := 0; p < parts; p++ {
		for i, n := range live {
			write(n, p, fmt.Sprintf("after-p%d-n%d", p, i))
		}
	}
	for p := 0; p < parts; p++ {
		waitFor(t, 15*time.Second, fmt.Sprintf("p%d to converge on the survivors", p), same(p, live...))
		d := live[0].store.data(p)
		if len(d) != 7 {
			t.Fatalf("p%d: data %v, want 7 writes", p, d)
		}
		for i := 0; i < 3; i++ {
			if !slices.Contains(d, fmt.Sprintf("p%d-n%d", p, i)) {
				t.Errorf("p%d: lost %q: %v", p, fmt.Sprintf("p%d-n%d", p, i), d)
			}
		}
		if err := live[0].store.checkInvariant(); err != nil {
			t.Error(err)
		}
	}

	// A write's position and reply, a refusal's code and WaitApplied cross the real codec and transport.
	for _, n := range live {
		for p := 0; p < parts; p++ {
			var res Result
			var err error
			for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
				if res, err = n.m.Write(t.Context(), p, []byte("result")); !errors.Is(err, ErrUnavailable) {
					break
				}
			}
			if err != nil || string(res.Reply) != "ok:result" || res.Pos.Seq == 0 {
				t.Fatalf("p%d: Write = %+v, %v", p, res, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			if err := n.m.WaitApplied(ctx, p, res.Pos); err != nil {
				t.Fatalf("p%d: WaitApplied(%s): %v", p, res.Pos, err)
			}
			cancel()
			var re *RefusedError
			if _, err := n.m.Write(t.Context(), p, []byte("conflict")); !errors.As(err, &re) || re.Code != "conflict" ||
				!errors.Is(err, ErrRefused) {
				t.Fatalf("p%d: a refusal over the network: %#v", p, err)
			}
		}
	}
}
