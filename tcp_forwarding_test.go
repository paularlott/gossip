package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/logger"
)

// Broadcasts received over TCP must be re-gossiped just like those received
// over UDP. Previously a fire-and-forget packet delivered over TCP carried its
// connection, CanReply() reported true, and the forwarding step skipped it —
// so in a TCP-only cluster larger than the fan-out a broadcast reached only
// the sender's direct peers.
func TestTCPOnlyBroadcastIsForwarded(t *testing.T) {
	const n = 8
	const msgs = 5

	var mu sync.Mutex
	perMsg := make(map[int]int)
	clusters := make([]*Cluster, 0, n)
	defer func() {
		for _, c := range clusters {
			c.Stop()
		}
	}()

	for i := 0; i < n; i++ {
		cfg := DefaultConfig()
		addr := getFreeTCPAddress(t)
		cfg.BindAddr, cfg.AdvertiseAddr = addr, addr
		cfg.MsgCodec = codec.NewJSONCodec()
		cfg.Logger = logger.NewNullLogger()
		cfg.ForceReliableTransport = true
		cfg.Transport = NewSocketTransport(cfg)
		c, err := NewCluster(cfg)
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		if err := c.HandleFunc(UserMsg+1, func(_ *Node, p *Packet) error {
			var m map[string]int
			if err := p.Unmarshal(&m); err != nil {
				return err
			}
			mu.Lock()
			perMsg[m["i"]]++
			mu.Unlock()
			return nil
		}); err != nil {
			t.Fatalf("register: %v", err)
		}
		c.Start()
		clusters = append(clusters, c)
		if i > 0 {
			if err := c.Join([]string{clusters[0].LocalNode().AdvertisedAddr()}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for _, c := range clusters {
		for time.Now().Before(deadline) && c.NumAliveNodes() < n {
			time.Sleep(50 * time.Millisecond)
		}
		if c.NumAliveNodes() < n {
			t.Fatalf("cluster did not converge: %d of %d alive", c.NumAliveNodes(), n)
		}
	}
	fan := clusters[0].CalcFanOut()
	if fan >= n-1 {
		t.Fatalf("fan-out %d covers every peer directly; the test needs forwarding to matter", fan)
	}

	for i := 0; i < msgs; i++ {
		if err := clusters[0].Send(UserMsg+1, map[string]int{"i": i}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	// Gossip is probabilistic, so full delivery is not guaranteed on every
	// run. What forwarding does guarantee is that a message reaches more
	// nodes than the sender's own fan-out — impossible if receivers never
	// re-gossip. Wait for that for every message.
	reached := func() (minReach, total int) {
		mu.Lock()
		defer mu.Unlock()
		minReach = n
		for i := 0; i < msgs; i++ {
			c := perMsg[i]
			total += c
			if c < minReach {
				minReach = c
			}
		}
		return minReach, total
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m, total := reached(); m > fan && total == msgs*(n-1) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	m, total := reached()
	t.Logf("fan-out %d: delivered %d of %d, every message reached at least %d nodes", fan, total, msgs*(n-1), m)
	if m <= fan {
		t.Fatalf("a TCP-only broadcast reached only %d nodes (fan-out %d): received packets are not being forwarded", m, fan)
	}
}
