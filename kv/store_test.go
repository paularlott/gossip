package kv_test

import (
	"strings"
	"testing"
	"time"

	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/gossip/kv"
	"github.com/stretchr/testify/require"
)

func TestLoneNodeAcceptsWrites(t *testing.T) {
	// A store that starts alone is in bootstrap mode: its own copy is the
	// whole replica set and writes succeed.
	s := build(t, harnessOpts{count: 1})[0].store

	mustSet(t, s, "k", "v")
	v, ok := s.Get("k")
	require.True(t, ok && string(v) == "v", "lone get = %q,%v", v, ok)
	require.NoError(t, s.Delete("k"))
	_, ok = s.Get("k")
	require.False(t, ok, "lone delete did not take")
	require.True(t, s.Synced(), "lone store should be vacuously synced")
	require.True(t, s.Sync(), "a lone store has no peer to sync with and is trivially in sync")
}

func TestValidationErrors(t *testing.T) {
	nodes := build(t, harnessOpts{count: 1,
		storeCfg: func() *kv.Config {
			c := kv.DefaultConfig()
			c.MaxValueSize = 10
			c.MaxKeys = 2
			return c
		}})
	s := nodes[0].store

	requireErr(t, s.Set("", []byte("v"), 0), kv.ErrKeyEmpty)
	requireErr(t, s.Set("k", []byte("0123456789A"), 0), kv.ErrValueTooLarge)
	requireErr(t, s.Set("k", []byte("v"), -time.Second), kv.ErrTTLOutOfRange)
	requireErr(t, s.Set("k", []byte("v"), 25*time.Hour), kv.ErrTTLOutOfRange)

	mustSet(t, s, "k1", "v")
	mustSet(t, s, "k2", "v")
	requireErr(t, s.Set("k3", []byte("v"), 0), kv.ErrTooManyKeys)
	// Overwriting an existing key stays within the cap.
	mustSet(t, s, "k1", "v2")
	// Freeing a slot lets a new key in.
	require.NoError(t, s.Delete("k2"))
	mustSet(t, s, "k3", "v")
}

func TestClosedStore(t *testing.T) {
	n := build(t, harnessOpts{count: 1})[0]

	s := n.store
	s.Close()

	requireErr(t, s.Set("k", []byte("v"), 0), kv.ErrStoreClosed)
	requireErr(t, s.Delete("k"), kv.ErrStoreClosed)
	_, ok := s.Get("k")
	require.False(t, ok, "closed store served a value")

	// A fresh store on the same cluster must work (handlers survive).
	s2 := kv.NewStore(n.Cluster, kv.ClusterMembership{Cluster: n.Cluster}, kv.DefaultConfig())
	defer s2.Close()
	mustSet(t, s2, "k", "v")
	if err := s2.Delete("k"); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("delete on replacement store: %v", err)
	}
}

func TestDuplicateStoreNamePanics(t *testing.T) {
	n := build(t, harnessOpts{count: 1})[0]

	require.Panics(t, func() {
		kv.NewStore(n.Cluster, kv.ClusterMembership{Cluster: n.Cluster}, kv.DefaultConfig())
	}, "duplicate store name did not panic")
}

// A peer whose versions are far ahead of the receiver's clock is refused and
// counted (its write is neither applied nor acknowledged, and the receiver's
// clock stays put); one within MaxClockSkew is accepted.
func TestFutureVersions(t *testing.T) {
	writerClock := hlc.NewClock()
	writerClock.Witness(hlc.FromTime(time.Now().Add(10 * time.Second)))
	receiverClock := hlc.NewClock()
	var n int
	nodes := build(t, harnessOpts{count: 2, storeCfg: func() *kv.Config {
		cfg := kv.DefaultConfig()
		if n++; n == 1 {
			cfg.Clock = writerClock
		} else {
			cfg.Clock = receiverClock
		}
		return cfg
	}})

	// 10s ahead is within the bound.
	mustSet(t, nodes[0].store, "near", "v")
	waitValue(t, nodes[1].store, "near", "v", 5*time.Second)
	require.Zero(t, nodes[1].store.RejectedSkew())

	// Now the writer mints versions an hour ahead. The receiver refuses to
	// acknowledge, so the writer fails closed.
	writerClock.Witness(hlc.FromTime(time.Now().Add(time.Hour)))
	err := nodes[0].store.Set("far", []byte("v"), 0)
	requireErr(t, err, kv.ErrWriteQuorum)
	require.Eventually(t, func() bool { return nodes[1].store.RejectedSkew() > 0 },
		5*time.Second, 20*time.Millisecond, "the refused entry was not counted")
	_, ok := nodes[1].store.Get("far")
	require.False(t, ok, "an entry versioned 1h ahead was applied")
	// The refused version did not drag the receiver's clock forward.
	ahead := receiverClock.Now().Ahead(time.Now())
	require.True(t, ahead <= time.Minute, "the receiver's clock is %s ahead after refusing the entry", ahead)
}
