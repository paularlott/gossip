package kv_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/paularlott/gossip/internal/clustertest"
	"github.com/paularlott/gossip/kv"
	"github.com/stretchr/testify/require"
)

func TestQuorumFailsClosedAndCompensates(t *testing.T) {
	opts := harnessOpts{count: 3}
	nodes := build(t, opts)

	// Form the group first so the high-water mark is 3.
	mustSet(t, nodes[0].store, "warm", "v")
	waitValue(t, nodes[2].store, "warm", "v", 5*time.Second)

	nodes[1].Crash()
	nodes[2].Crash()

	// W=2 against an observed group of 3: one peer ack is required, and no
	// peer is reachable: the write must be refused, not degraded to local
	// only (the sticky high-water bar).
	err := nodes[0].store.Set("k", []byte("v"), 0)
	requireErr(t, err, kv.ErrWriteQuorum)

	// The failed write was compensated: nothing is visible locally.
	_, ok := nodes[0].store.Get("k")
	require.False(t, ok, "failed write is visible locally")

	// The bar stays up: still refused later, not just in the instant after
	// the crashes.
	time.Sleep(time.Second)
	err = nodes[0].store.Set("k2", []byte("v"), 0)
	requireErr(t, err, kv.ErrWriteQuorum)

	// Healing (a restarted peer) lets writes flow again, and the compensated
	// key must not resurrect.
	nodes[2] = restart(t, opts, nodes[2], nodes[0])
	clustertest.WaitAlive(t, nodes[0].Cluster, 2)
	mustSet(t, nodes[0].store, "k3", "v")
	waitValue(t, nodes[2].store, "k3", "v", 10*time.Second)
	waitMiss(t, nodes[2].store, "k", 10*time.Second)
}

func TestDeleteWithoutQuorumStandsAndConverges(t *testing.T) {
	opts := harnessOpts{count: 3}
	nodes := build(t, opts)

	mustSet(t, nodes[0].store, "k", "v")
	waitValue(t, nodes[1].store, "k", "v", 5*time.Second)

	// Lose the peers; the delete falls short of quorum but stands: the
	// safe direction under uncertainty.
	nodes[1].Crash()
	nodes[2].Crash()
	err := nodes[0].store.Delete("k")
	requireErr(t, err, kv.ErrWriteQuorum)
	_, ok := nodes[0].store.Get("k")
	require.False(t, ok, "delete that fell short of quorum did not stand")

	// The tombstone must win once the peer returns and syncs.
	nodes[1] = restart(t, opts, nodes[1], nodes[0])
	clustertest.WaitAlive(t, nodes[1].Cluster, 2)
	waitMiss(t, nodes[1].store, "k", 10*time.Second)
}

// fastWaterCfg returns a store config whose water-mark timings are fast
// enough for tests: growth settles quickly, shrinkage dwells briefly.
func fastWaterCfg() *kv.Config {
	c := kv.DefaultConfig()
	c.StabilityPeriod = 300 * time.Millisecond
	c.ShrinkDwell = 600 * time.Millisecond
	return c
}

func TestScaleDownOneAtATimeReopensWrites(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3, storeCfg: fastWaterCfg})

	mustSet(t, nodes[0].store, "warm", "v")

	// Drop the first node silently (a crash produces no departure signal).
	// With one peer still alive, W=2 writes continue immediately, and the
	// surviving peer holds them...
	nodes[2].Crash()
	mustSet(t, nodes[0].store, "after-first-loss", "v")
	waitValue(t, nodes[1].store, "after-first-loss", "v", 5*time.Second)

	// ...and after the shrink dwell the water mark itself follows down, so
	// the loss is now absorbed. Drop the second node: with the mark still at
	// 2 the lone node must refuse, then after another dwell it re-opens.
	time.Sleep(2500 * time.Millisecond) // suspect detection + dwell + margin
	nodes[1].Crash()

	err := nodes[0].store.Set("lone", []byte("v"), 0)
	requireErr(t, err, kv.ErrWriteQuorum)

	// One member short of the mark, steady for the dwell: the mark follows,
	// the bar drops, and the surviving node writes again on its own.
	require.Eventually(t, func() bool { return nodes[0].store.Set("lone", []byte("v"), 0) == nil },
		10*time.Second, 100*time.Millisecond, "writes never re-opened after one-at-a-time scale-down")
	v, ok := nodes[0].store.Get("lone")
	require.True(t, ok && string(v) == "v", "reopened write not readable: %q,%v", v, ok)
}

func TestGracefulDrainToOneNode(t *testing.T) {
	// Five nodes drained with clean Leave() events, one at a time, down to
	// a single survivor. The water mark follows each graceful departure
	// immediately, so writes must keep working at every step of the drain,
	// including the last node writing entirely alone.
	nodes := build(t, harnessOpts{count: 5})

	mustSet(t, nodes[0].store, "keeper", "v")
	for i := 1; i <= 4; i++ {
		mustSet(t, nodes[0].store, fmt.Sprintf("step-%d", i), "v")
	}
	waitValue(t, nodes[4].store, "step-4", "v", 5*time.Second)

	// Drain from the top, writing on the intended survivor after each leave
	// (once its broadcast has arrived).
	survivor := nodes[0]
	for i := 4; i >= 1; i-- {
		nodes[i].Retire()
		key := fmt.Sprintf("after-drain-%d", i)
		require.Eventually(t, func() bool { return survivor.store.Set(key, []byte("v"), 0) == nil },
			5*time.Second, 50*time.Millisecond, "write after draining node %d", i)
	}

	// The last node stands alone: water 1, need 0, and everything written
	// before and during the drain is still there.
	lone := survivor.store
	_, ok := lone.Get("keeper")
	require.True(t, ok, "data lost across the drain")
	for i := 1; i <= 4; i++ {
		_, ok := lone.Get(fmt.Sprintf("after-drain-%d", i))
		require.True(t, ok, "drain-step write %d missing on the survivor", i)
	}
	mustSet(t, lone, "final-lone-write", "v")
}

func TestSplitPartitionBothSidesWriteHealByLWW(t *testing.T) {
	// First a single node is isolated: it fails closed even though four
	// replacement peers exist to retry against, the group keeps writing, and
	// the isolated node's failed write is compensated everywhere.
	//
	// Then a true 2|3 split: both sides satisfy W=2 internally, so both accept
	// writes during the partition, which is the documented semantics. On heal,
	// LWW must drive every node to the same winner for a contended key, each
	// side's uncontended keys must flow to the other side, and a value the
	// right side only has in its stale form must give way to the left's newer
	// write.
	nodes := build(t, harnessOpts{count: 5})
	mustSet(t, nodes[0].store, "warm", "v")
	waitValue(t, nodes[4].store, "warm", "v", 5*time.Second)

	nodes[4].Net.Cut()
	mustSet(t, nodes[0].store, "group-key", "v")
	requireErr(t, nodes[4].store.Set("iso-key", []byte("v"), 0), kv.ErrWriteQuorum)
	nodes[4].Net.Uncut()
	waitValue(t, nodes[4].store, "group-key", "v", 15*time.Second)
	waitMiss(t, nodes[4].store, "iso-key", 15*time.Second)
	waitMiss(t, nodes[0].store, "iso-key", 5*time.Second)

	left := nodes[:2]  // n0, n1
	right := nodes[2:] // n2, n3, n4
	clustertest.Split(bases(left), bases(right))

	// Both sides write through the partition.
	mustSet(t, left[0].store, "contended", "left-value")
	mustSet(t, left[0].store, "warm", "v2")
	mustSet(t, left[1].store, "left-only", "v")
	mustSet(t, right[0].store, "contended", "right-value")
	mustSet(t, right[1].store, "right-only", "v")

	// Neither side may see the other's writes while partitioned.
	time.Sleep(300 * time.Millisecond)
	_, ok := left[0].store.Get("right-only")
	require.False(t, ok, "partition leaks: left sees right's write")
	_, ok = right[0].store.Get("left-only")
	require.False(t, ok, "partition leaks: right sees left's write")
	v, _ := right[0].store.Get("warm")
	require.Equal(t, "v", string(v), "partition leaks: right sees left's newer write")

	clustertest.Heal(bases(left), bases(right))

	// The contended key converges to one identical winner on every node,
	// stable across two reads.
	waitConverged(t, nodes, "contended", 20*time.Second)

	// Uncontended keys from each side flow everywhere, and the newer write
	// replaces the stale copy.
	for _, n := range nodes {
		waitValue(t, n.store, "left-only", "v", 15*time.Second)
		waitValue(t, n.store, "right-only", "v", 15*time.Second)
		waitValue(t, n.store, "warm", "v2", 15*time.Second)
	}
}

func TestFlappingPartition(t *testing.T) {
	// Repeated cut/heal cycles on one node: the group keeps writing while it
	// is isolated (the first time round), the isolated node fails closed (and its failed writes are
	// compensated), and after each heal it writes again and converges to the
	// group. Nothing corrupts along the way.
	nodes := build(t, harnessOpts{count: 3})
	isolated := nodes[2]
	mustSet(t, nodes[0].store, "warm", "v")
	waitValue(t, isolated.store, "warm", "v", 5*time.Second)

	for i := range 3 {
		isolated.Net.Cut()

		if i == 0 {
			// W=2 needs one ack from the peer that is still reachable.
			mustSet(t, nodes[0].store, "group-key", "v")
		}

		err := isolated.store.Set(fmt.Sprintf("iso-%d", i), []byte("v"), 0)
		requireErr(t, err, kv.ErrWriteQuorum)

		isolated.Net.Uncut()
		key := fmt.Sprintf("flap-%d", i)
		require.Eventually(t, func() bool { return isolated.store.Set(key, []byte("v"), 0) == nil },
			15*time.Second, 100*time.Millisecond, "flap %d: no healed set", i)
		waitValue(t, nodes[0].store, key, "v", 15*time.Second)
		waitValue(t, nodes[1].store, key, "v", 15*time.Second)
		waitValue(t, isolated.store, "group-key", "v", 15*time.Second)
	}

	// Everything written while connected is consistent cluster-wide, and the
	// isolated node's failed writes took effect nowhere.
	for _, n := range nodes {
		for _, k := range []string{"warm", "group-key", "flap-0", "flap-1", "flap-2"} {
			waitValue(t, n.store, k, "v", 5*time.Second)
		}
		for _, k := range []string{"iso-0", "iso-1", "iso-2"} {
			waitMiss(t, n.store, k, 5*time.Second)
		}
	}
}
