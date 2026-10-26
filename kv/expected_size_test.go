package kv_test

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/kv"
	"github.com/stretchr/testify/require"
)

func pinnedCfg(expected int) func() *kv.Config {
	return func() *kv.Config {
		c := fastWaterCfg()
		c.ExpectedClusterSize = expected
		return c
	}
}

// A store pinned to a three-node group must not fall back to bootstrap
// (local-only) writes when it starts alone.
func TestExpectedSizeLoneNodeRefusesWrites(t *testing.T) {
	nodes := build(t, harnessOpts{count: 1, storeCfg: pinnedCfg(3)})

	err := nodes[0].store.Set("k", []byte("v"), 0)
	requireErr(t, err, kv.ErrWriteQuorum)
}

// Graceful leaves must not lower a pinned water mark: after draining to one
// node the survivor keeps refusing writes, even after the shrink dwell.
func TestExpectedSizeGracefulDrainKeepsBar(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3, storeCfg: pinnedCfg(3)})

	mustSet(t, nodes[0].store, "warm", "v")

	nodes[2].Retire()
	mustSet(t, nodes[0].store, "two-left", "v")

	nodes[1].Retire()
	// Leave propagation plus several dwells (600 ms): the bar never drops.
	require.Never(t, func() bool { return nodes[0].store.Set("alone", []byte("v"), 0) == nil },
		1500*time.Millisecond, 100*time.Millisecond, "the lone survivor of a pinned group accepted a write")
	err := nodes[0].store.Set("alone", []byte("v"), 0)
	requireErr(t, err, kv.ErrWriteQuorum)
}
