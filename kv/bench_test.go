package kv_test

import (
	"fmt"
	"testing"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/kv"
	"github.com/paularlott/logger"
)

// benchStore builds a single-node store — the memory-only path with no
// Persister registered. The cluster exists only because the store requires
// one; the write path never touches the network (W degrades to local-only
// on a one-node group).
func benchStore(b *testing.B) *kv.Store {
	b.Helper()
	cfg := gossip.DefaultConfig()
	cfg.BindAddr = "127.0.0.1:25190"
	cfg.AdvertiseAddr = "127.0.0.1:25190"
	cfg.MsgCodec = codec.NewJSONCodec()
	cfg.Logger = logger.NewNullLogger()
	cfg.Transport = gossip.NewSocketTransport(cfg)

	c, err := gossip.NewCluster(cfg)
	if err != nil {
		b.Fatalf("cluster: %v", err)
	}
	c.Start()
	b.Cleanup(func() { c.Stop() })

	return kv.NewStore(c, kv.ClusterMembership{Cluster: c}, kv.DefaultConfig())
}

// The value sized to sit mid-range for a cache entry.
var benchValue = []byte("0123456789012345678901234567890123456789")

func BenchmarkStoreSet(b *testing.B) {
	s := benchStore(b)
	b.Cleanup(func() { s.Close() })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Set("bench", benchValue, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreGet(b *testing.B) {
	s := benchStore(b)
	b.Cleanup(func() { s.Close() })
	if err := s.Set("bench", benchValue, 0); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := s.Get("bench"); !ok {
			b.Fatal("missing")
		}
	}
}

func BenchmarkStoreSetDistinctKeys(b *testing.B) {
	s := benchStore(b)
	b.Cleanup(func() { s.Close() })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Set(fmt.Sprintf("bench/%d", i), benchValue, 0); err != nil {
			b.Fatal(err)
		}
	}
}
