package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Broadcasts received over TCP must be re-gossiped just like those received over UDP. Previously a fire-and-forget
// packet delivered over TCP carried its connection, CanReply() reported true, and the forwarding step skipped it, so
// in a TCP-only cluster larger than the fan-out a broadcast reached only the sender's direct peers.
func TestTCPOnlyBroadcastIsForwarded(t *testing.T) {
	const n = 8
	const msgs = 5

	var mu sync.Mutex
	perMsg := make(map[int]int)
	clusters := make([]*Cluster, 0, n)
	for i := 0; i < n; i++ {
		c := startLiveCluster(t, func(cfg *Config) { cfg.ForceReliableTransport = true })
		require.NoError(t, c.HandleFunc(UserMsg+1, func(_ *Node, p *Packet) error {
			var m map[string]int
			if err := p.Unmarshal(&m); err != nil {
				return err
			}
			mu.Lock()
			perMsg[m["i"]]++
			mu.Unlock()
			return nil
		}))
		clusters = append(clusters, c)
		if i > 0 {
			require.NoError(t, c.Join([]string{clusters[0].LocalNode().AdvertisedAddr()}))
		}
	}

	require.Eventually(t, func() bool {
		for _, c := range clusters {
			if c.NumAliveNodes() < n {
				return false
			}
		}
		return true
	}, 15*time.Second, 20*time.Millisecond, "the cluster did not converge")
	fan := clusters[0].CalcFanOut()
	require.Less(t, fan, n-1, "the fan-out covers every peer directly; the test needs forwarding to matter")

	for i := 0; i < msgs; i++ {
		require.NoError(t, clusters[0].Send(UserMsg+1, map[string]int{"i": i}))
	}

	// Gossip is probabilistic, so full delivery is not guaranteed on every run. What forwarding does guarantee is that
	// every message reaches more nodes than the sender's own fan-out, which is impossible if receivers never re-gossip.
	reached := func() (minReach, total int) {
		mu.Lock()
		defer mu.Unlock()
		minReach = n
		for i := 0; i < msgs; i++ {
			c := perMsg[i]
			total += c
			minReach = min(minReach, c)
		}
		return minReach, total
	}
	require.Eventually(t, func() bool {
		m, _ := reached()
		return m > fan
	}, 10*time.Second, 20*time.Millisecond, "a TCP-only broadcast did not get past the sender's fan-out (%d): received packets are not being forwarded", fan)
}
