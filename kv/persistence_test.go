package kv_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/paularlott/gossip/internal/clustertest"
	"github.com/paularlott/gossip/kv"
	"github.com/stretchr/testify/require"
)

// fastSnapCfg makes the snapshot triggers quick for tests.
func fastSnapCfg(writes int, interval time.Duration) *kv.Config {
	c := kv.DefaultConfig()
	c.SnapshotWrites = writes
	c.SnapshotInterval = interval
	return c
}

func TestSnapshotRestoreAfterRestart(t *testing.T) {
	persisters := []*memPersister{{}, {}, {}}
	opts := harnessOpts{count: 3, persister: func(i int) kv.Persister { return persisters[i] }}
	nodes := build(t, opts)

	// A surviving key, and a deleted one whose tombstone must travel
	// through the snapshot or the delete resurrects from disk.
	mustSet(t, nodes[0].store, "keep", "v")
	mustSet(t, nodes[0].store, "gone", "v")
	require.NoError(t, nodes[0].store.Delete("gone"))
	waitMiss(t, nodes[2].store, "gone", 5*time.Second)

	// Save, then lose the node and its memory; write more while it is down.
	require.NoError(t, nodes[2].store.Snapshot())
	nodes[2].Crash()
	mustSet(t, nodes[0].store, "during-downtime", "v")
	waitValue(t, nodes[1].store, "during-downtime", "v", 5*time.Second)

	// Restart: the node loads its snapshot from "disk", then catches up.
	nodes[2] = restart(t, opts, nodes[2], nodes[0])
	waitValue(t, nodes[2].store, "keep", "v", 10*time.Second)
	waitValue(t, nodes[2].store, "during-downtime", "v", 10*time.Second)
	waitMiss(t, nodes[2].store, "gone", 10*time.Second) // tombstone survived the round trip
}

// A snapshot older than the live cluster must lose to the cluster's fresher
// writes on restore.
func TestRestoreOlderThanClusterState(t *testing.T) {
	persisters := []*memPersister{{}, {}, {}}
	opts := harnessOpts{count: 3, persister: func(i int) kv.Persister { return persisters[i] }}
	nodes := build(t, opts)

	mustSet(t, nodes[0].store, "shared", "old")
	waitValue(t, nodes[2].store, "shared", "old", 5*time.Second)

	require.NoError(t, nodes[2].store.Snapshot())
	nodes[2].Crash()

	mustSet(t, nodes[0].store, "shared", "new")
	waitValue(t, nodes[1].store, "shared", "new", 5*time.Second)

	nodes[2] = restart(t, opts, nodes[2], nodes[0])
	waitValue(t, nodes[2].store, "shared", "new", 10*time.Second)
}

// Every node crashes with its own snapshot; on restart each loads its disk and
// the group converges to the union.
func TestFullOutageRestoreUnion(t *testing.T) {
	persisters := []*memPersister{{}, {}}
	opts := harnessOpts{count: 2, persister: func(i int) kv.Persister { return persisters[i] }}
	nodes := build(t, opts)

	mustSet(t, nodes[0].store, "from-n0", "v")
	mustSet(t, nodes[1].store, "from-n1", "v")
	waitValue(t, nodes[0].store, "from-n1", "v", 5*time.Second)
	waitValue(t, nodes[1].store, "from-n0", "v", 5*time.Second)

	require.NoError(t, nodes[0].store.Snapshot())
	require.NoError(t, nodes[1].store.Snapshot())
	nodes[0].Crash()
	nodes[1].Crash()

	// Total outage: both come back with nothing but their disks.
	nodes[0] = restart(t, opts, nodes[0], nodes[1]) // seed via n1's address book
	waitValue(t, nodes[0].store, "from-n0", "v", 5*time.Second)

	nodes[1] = restart(t, opts, nodes[1], nodes[0])
	clustertest.WaitAlive(t, nodes[0].Cluster, 2)
	waitValue(t, nodes[0].store, "from-n1", "v", 10*time.Second)
	waitValue(t, nodes[1].store, "from-n0", "v", 10*time.Second)
}

func TestSnapshotTriggersCountIntervalAndIdle(t *testing.T) {
	const interval = 150 * time.Millisecond
	p := &memPersister{}
	nodes := build(t, harnessOpts{count: 1,
		storeCfg:  func() *kv.Config { return fastSnapCfg(3, interval) },
		persister: func(i int) kv.Persister { return p },
	})
	s := nodes[0].store

	// Three writes reach the write-count trigger.
	for i := range 3 {
		mustSet(t, s, fmt.Sprintf("k%d", i), "v")
	}
	require.Eventually(t, func() bool { return p.saveCount() >= 1 }, 5*time.Second, 25*time.Millisecond, "write-count trigger")

	// Clean store: the interval must not fire on its own.
	require.Never(t, func() bool { return p.saveCount() != 1 }, 3*interval, 25*time.Millisecond, "idle store saved again")

	// One write is below the count trigger but the interval catches it.
	mustSet(t, s, "k3", "v")
	require.Eventually(t, func() bool { return p.saveCount() >= 2 }, 5*time.Second, 25*time.Millisecond, "interval trigger")

	// Clean again: still exactly two.
	require.Never(t, func() bool { return p.saveCount() != 2 }, 3*interval, 25*time.Millisecond, "idle store saved again after the interval")
}

func TestCloseFlushesDirtyStore(t *testing.T) {
	// Triggers are far away (defaults); only the close-time flush saves.
	p := &memPersister{}
	opts := harnessOpts{count: 1, persister: func(i int) kv.Persister { return p }}
	n := build(t, opts)[0]

	mustSet(t, n.store, "survives", "v")
	n.Retire() // Close flushes the dirty store
	require.Equal(t, 1, p.saveCount(), "close flush saved the wrong number of times")

	// A fresh store on a fresh cluster restores from the same "disk".
	n2 := build(t, opts)[0]
	waitValue(t, n2.store, "survives", "v", time.Second)
}

func TestSnapshotSaveRetriesAfterFailure(t *testing.T) {
	p := &memPersister{}
	p.setFail(errors.New("disk full"))
	nodes := build(t, harnessOpts{count: 1,
		storeCfg:  func() *kv.Config { return fastSnapCfg(2, time.Hour) },
		persister: func(i int) kv.Persister { return p },
	})
	s := nodes[0].store

	// The write-count trigger fires but the save fails; the dirty count is
	// restored, so nothing is considered persisted.
	for i := range 2 {
		mustSet(t, s, fmt.Sprintf("k%d", i), "v")
	}
	require.Eventually(t, func() bool { return p.attemptCount() >= 1 }, 5*time.Second, 25*time.Millisecond, "the trigger never tried to save")
	require.Zero(t, p.saveCount(), "failing persister recorded saves")

	// The Persister recovers; one more write re-triggers with the carried
	// dirty count and the save lands.
	p.setFail(nil)
	mustSet(t, s, "k2", "v")
	require.Eventually(t, func() bool { return p.saveCount() >= 1 }, 5*time.Second, 25*time.Millisecond, "retry after recovery")
}

// A snapshot that cannot be loaded, or that belongs to another store, must not
// prevent a store from starting empty and working normally.
func TestLoadFailuresStartEmpty(t *testing.T) {
	foreign := &memPersister{snap: &kv.StoreSnapshot{Store: "other-store",
		Entries: []*kv.Entry{{Key: "k", Version: 1, Origin: [16]byte{1}, Value: []byte("v")}}}}
	corrupt := &memPersister{corrupt: true}

	n := build(t, harnessOpts{count: 1,
		storeCfg:  func() *kv.Config { c := kv.DefaultConfig(); c.Name = "corrupt"; return c },
		persister: func(i int) kv.Persister { return corrupt },
	})[0]
	foreignStore := func() *kv.Store {
		c := kv.DefaultConfig()
		c.Name = "foreign"
		c.Persister = foreign
		s := kv.NewStore(n.Cluster, kv.ClusterMembership{Cluster: n.Cluster}, c)
		n.OnStop(s.Close)
		return s
	}()

	mustSet(t, n.store, "k", "v")
	waitValue(t, n.store, "k", "v", time.Second)

	// The snapshot belongs to another store: ignored, not merged.
	_, ok := foreignStore.Get("k")
	require.False(t, ok, "foreign store's snapshot leaked into this store")
}
