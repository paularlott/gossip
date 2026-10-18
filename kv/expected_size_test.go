package kv_test

import (
	"errors"
	"testing"
	"time"

	"github.com/paularlott/gossip/kv"
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
	nodes := build(t, harnessOpts{basePort: 21500, count: 1, storeCfg: pinnedCfg(3)})
	defer teardown(nodes)

	if err := nodes[0].store.Set("k", []byte("v"), 0); !errors.Is(err, kv.ErrWriteQuorum) {
		t.Fatalf("lone set on a pinned store = %v, want ErrWriteQuorum", err)
	}
}

// Graceful leaves must not lower a pinned water mark: after draining to one
// node the survivor keeps refusing writes, even after the shrink dwell.
func TestExpectedSizeGracefulDrainKeepsBar(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 21510, count: 3, storeCfg: pinnedCfg(3)})
	defer teardown(nodes)

	if err := nodes[0].store.Set("warm", []byte("v"), 0); err != nil {
		t.Fatalf("warm set: %v", err)
	}

	nodes[2].retire()
	time.Sleep(500 * time.Millisecond)
	if err := nodes[0].store.Set("two-left", []byte("v"), 0); err != nil {
		t.Fatalf("set with two of three: %v", err)
	}

	nodes[1].retire()
	time.Sleep(2 * time.Second) // leave propagation plus several dwells
	if err := nodes[0].store.Set("alone", []byte("v"), 0); !errors.Is(err, kv.ErrWriteQuorum) {
		t.Fatalf("set on the lone survivor of a pinned group = %v, want ErrWriteQuorum", err)
	}
}
