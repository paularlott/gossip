package lock_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/internal/clustertest"
	"github.com/paularlott/gossip/leader"
	"github.com/paularlott/gossip/lock"
	"github.com/stretchr/testify/require"
)

// --- happy paths ---

func TestOneLeaderElectedAndLocksWork(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3, minSize: 2})

	leaderID := waitOneLeader(t, nodes)
	ldr := nodeOf(nodes, leaderID)
	t.Logf("leader: %s", ldr.Label)

	// Exactly one node should consider itself leader.
	leaders := 0
	for _, n := range nodes {
		if n.pool.IsLeader() {
			leaders++
		}
	}
	require.Equal(t, 1, leaders, "expected exactly 1 leader")

	waitGrantable(t, nodes[0].pool, 20*time.Second)

	// A follower takes the lock; everyone should agree it is held.
	follower := others(nodes, leaderID)[0]
	lk, err := follower.pool.TryAcquire("shared", 3*time.Second)
	require.NoError(t, err, "%s acquire", follower.Label)

	// The election term reaches the lock table, so fencing tokens carry it.
	require.NotZero(t, ldr.election.Term(), "the term never advanced")
	require.Equal(t, ldr.election.Term(), lk.Token().Term, "token %s does not carry the election term", lk.Token())

	for _, n := range nodes {
		held, owner, tok, _, err := n.pool.Query("shared")
		if err != nil {
			t.Errorf("%s query: %v", n.Label, err)
			continue
		}
		if !held {
			t.Errorf("%s does not see the lock as held", n.Label)
		}
		if owner != follower.ID() {
			t.Errorf("%s reports the wrong owner", n.Label)
		}
		if !tok.Equal(lk.Token()) {
			t.Errorf("%s reports token %s, expected %s", n.Label, tok, lk.Token())
		}
	}

	// Nobody else may take it.
	for _, n := range others(nodes, follower.ID()) {
		if _, err := n.pool.TryAcquire("shared", 3*time.Second); err == nil {
			t.Errorf("%s was granted a held lock", n.Label)
		}
	}

	require.NoError(t, lk.Release())

	// Now somebody else can have it.
	other := others(nodes, follower.ID())[0]
	lk2, err := other.pool.TryAcquire("shared", 3*time.Second)
	require.NoError(t, err, "%s acquire after release", other.Label)
	require.True(t, lk2.Token().After(lk.Token()), "re-acquired token %s should outrank %s", lk2.Token(), lk.Token())
	lk2.Release()
}

func TestExtendKeepsLockAgainstContention(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3, minSize: 2})

	waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	lk, err := nodes[0].pool.TryAcquire("renewed", 1*time.Second)
	require.NoError(t, err)

	stop := make(chan struct{})
	var stolen atomic.Int64
	var wg sync.WaitGroup

	for _, n := range nodes[1:] {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if l, err := n.pool.TryAcquire("renewed", 1*time.Second); err == nil {
					stolen.Add(1)
					l.Release()
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	// Renewed for more than two TTLs.
	for i := range 8 {
		time.Sleep(300 * time.Millisecond)
		if err := lk.Extend(1 * time.Second); err != nil {
			t.Errorf("extend %d: %v", i, err)
		}
	}

	close(stop)
	wg.Wait()
	lk.Release()

	if n := stolen.Load(); n != 0 {
		t.Errorf("lock stolen %d times while being renewed", n)
	}
}

func TestExclusionUnderContention(t *testing.T) {
	nodes := build(t, harnessOpts{count: 4, minSize: 3})

	waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	mon := newMonitor()
	var acquired atomic.Int64
	var wg sync.WaitGroup

	for _, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				key := fmt.Sprintf("c%d", i%3)
				lk, err := n.pool.TryAcquire(key, 1*time.Second)
				if err != nil {
					continue
				}
				acquired.Add(1)
				if mon.enter(key, n.Label, lk.Token()) {
					time.Sleep(time.Millisecond)
					mon.exit(key, n.Label)
				}
				lk.Release()
			}
		}()
	}
	wg.Wait()

	t.Logf("acquired=%d overlaps=%d", acquired.Load(), mon.overlaps())

	require.NotZero(t, acquired.Load(), "nothing was acquired")
	mon.requireExclusive(t, "stable leader")
}

// The scenario that broke the hashing design: nodes joining and leaving while
// keys are contended. With a single elected authority, joining confers no
// authority, so exclusion must be perfect.
func TestExclusionDuringMembershipChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{count: 3, minSize: 2})

	waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	mon := newMonitor()
	var acquired, denied atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// One worker per node: ownership is per-node, so one worker each keeps
	// node-level and worker-level ownership identical and any overlap the
	// monitor sees is a genuine cross-node breach.
	for _, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("churn%d", i%2)
				i++

				lk, err := n.pool.TryAcquire(key, 1*time.Second)
				if err != nil {
					denied.Add(1)
					continue
				}
				acquired.Add(1)
				if mon.enter(key, n.Label, lk.Token()) {
					time.Sleep(2 * time.Millisecond)
					mon.exit(key, n.Label)
				}
				lk.Release()
			}
		}()
	}

	// Churn a joiner in and out repeatedly.
	for round := range 5 {
		joiner := &node{Node: clustertest.NewNode(t, fmt.Sprintf("churn%d", round))}
		if err := joiner.Cluster.Join([]string{nodes[0].Addr}); err != nil {
			joiner.Retire()
			continue
		}
		harnessOpts{minSize: 2}.attach(joiner)

		time.Sleep(300 * time.Millisecond)
		joiner.Retire()
		time.Sleep(300 * time.Millisecond)
	}

	close(stop)
	wg.Wait()

	t.Logf("acquired=%d denied=%d overlaps=%d inversions=%d",
		acquired.Load(), denied.Load(), mon.overlaps(), mon.inversions())

	require.NotZero(t, acquired.Load(), "nothing acquired during churn")
	mon.requireExclusive(t, "membership churn (joining must not confer authority)")
}

// --- unhappy paths ---

// Killing the leader abruptly is the case the replication exists for: the new
// leader recovers by merging replica state rather than waiting out MaxTTL, so
// locks issued by the dead leader survive with their tokens and service resumes
// in seconds even when MaxTTL is a minute.
func TestLeaderKilledLocksSurviveAndServiceResumes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	// MaxTTL far beyond any plausible recovery time: under the old warm-up
	// gate design this scenario would stall for the full minute.
	nodes := build(t, harnessOpts{count: 3, minSize: 2, lockCfg: longLockCfg})

	leaderID := waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 20*time.Second, 2*time.Second)

	victim := nodeOf(nodes, leaderID)
	termBefore := victim.election.Term()
	followers := others(nodes, leaderID)

	keys := []string{"k0", "k1", "k2", "k3", "k4"}
	var lastTokens []lock.Token
	held := map[string]*lock.Lock{}
	for _, k := range keys {
		lk, err := followers[0].pool.TryAcquire(k, 55*time.Second)
		require.NoError(t, err, "acquire %s", k)
		held[k] = lk
		lastTokens = append(lastTokens, lk.Token())
	}

	// Kill the leader hard: severed first, so no departure is announced and
	// nothing is handed over. Everything the new leader learns must come from
	// the replicas.
	t.Logf("killing leader %s", victim.Label)
	crashStart := time.Now()
	victim.Crash()

	// A new leader must emerge among the survivors once the death is detected,
	// at a later term (what makes its first token outrank the old leader's).
	newLeaderID := waitNewLeader(t, followers, leaderID)
	newLeader := nodeOf(followers, newLeaderID)
	t.Logf("new leader: %s", newLeader.Label)
	termAfter := newLeader.election.Term()
	require.Greater(t, termAfter, termBefore, "term did not advance across the leadership change")

	// Service must resume well inside the 60s MaxTTL.
	waitGrantable(t, followers[0].pool, 45*time.Second, 2*time.Second)
	if elapsed := time.Since(crashStart); elapsed > 45*time.Second {
		t.Errorf("recovery took %v; replication should make this far shorter than MaxTTL", elapsed)
	}

	// Every previously-held key must still be held, by the same owner, under
	// the same token: the grants were durable and were recovered.
	for i, k := range keys {
		held, owner, tok, _, err := followers[1].pool.Query(k)
		require.NoError(t, err, "query %s after failover", k)
		if !held || owner != followers[0].ID() || !tok.Equal(lastTokens[i]) {
			t.Errorf("key %s did not survive the failover: held=%v owner=%v token=%s (want %s)",
				k, held, owner, tok, lastTokens[i])
		}
	}

	// And the survived locks still exclude others.
	if _, err := followers[1].pool.TryAcquire(keys[0], 2*time.Second); err == nil {
		t.Error("a survived lock must still exclude other nodes")
	}

	// Release them; re-acquire grants a strictly higher token, minted in the
	// new leader's term.
	for _, k := range keys {
		require.NoError(t, held[k].Release(), "release %s after failover", k)
	}
	for i, k := range keys {
		lk, err := followers[0].pool.TryAcquire(k, 5*time.Second)
		require.NoError(t, err, "re-acquire %s", k)
		if !lk.Token().After(lastTokens[i]) {
			t.Errorf("key %s: re-acquired token %s does not outrank pre-failover %s", k, lk.Token(), lastTokens[i])
		}
		if lk.Token().Term != termAfter {
			t.Errorf("key %s: token term %d != election term %d", k, lk.Token().Term, termAfter)
		}
		lk.Release()
	}
}

// A graceful leader shutdown keeps locks alive through the successor's
// recovery merge: replicas already hold every acked mutation. With W=3 every
// node is a replica, so this also exercises the higher-W write path.
func TestGracefulLeaderHandoverPreservesLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{count: 3, minSize: 2, lockCfg: func() *lock.Config {
		cfg := longLockCfg()
		cfg.WriteReplicas = 3
		return cfg
	}})

	leaderID := waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 20*time.Second, 2*time.Second)

	followers := others(nodes, leaderID)

	// A follower holds a long lock.
	lk, err := followers[0].pool.TryAcquire("survivor", 55*time.Second)
	require.NoError(t, err)
	origToken := lk.Token()

	// Graceful leader departure.
	victim := nodeOf(nodes, leaderID)
	t.Logf("gracefully retiring leader %s", victim.Label)
	victim.Retire()

	waitNewLeader(t, followers, leaderID)

	// The lock should still be held, with its original token, and still exclude
	// the other node.
	var held bool
	var tok lock.Token
	require.Eventually(t, func() bool {
		h, _, tk, _, err := followers[1].pool.Query("survivor")
		held, tok = h, tk
		return err == nil && h
	}, 15*time.Second, 100*time.Millisecond, "lock did not survive a graceful leader handover")
	require.True(t, held)
	require.True(t, tok.Equal(origToken), "token changed across handover: %s became %s", origToken, tok)

	if _, err := followers[1].pool.TryAcquire("survivor", 2*time.Second); err == nil {
		t.Error("a survived lock must still exclude other nodes")
	}

	_ = lk.Release()
}

// A two-node cluster with default W=2: every acked grant is on both nodes, so
// when the leader crashes the survivor recovers from its own replica view and
// the locks survive. The election needs the baseline to shrink first (the crash
// looks like a partition), hence the short ShrinkDwell.
func TestTwoNodeClusterLeaderCrashKeepsLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{count: 2, minSize: 1, lockCfg: longLockCfg,
		election: func(ec *leader.Config) { ec.ShrinkDwell = 1 * time.Second }})

	leaderID := waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 25*time.Second, 2*time.Second)

	follower := others(nodes, leaderID)[0]
	victim := nodeOf(nodes, leaderID)

	lk, err := follower.pool.TryAcquire("gpu", 55*time.Second)
	require.NoError(t, err)
	orig := lk.Token()

	victim.Crash()

	// The survivor must eventually lead (after quorum shrinks) and recover.
	newLeader := waitNewLeader(t, []*node{follower}, leaderID)
	require.Equal(t, follower.ID(), newLeader, "expected the survivor to lead")

	// The pool may observe the election up to a poll interval later, and its
	// recovery a little after that; Query is transient until then.
	var held bool
	var tok lock.Token
	var lastErr error
	require.Eventually(t, func() bool {
		h, _, tk, _, err := follower.pool.Query("gpu")
		held, tok, lastErr = h, tk, err
		return err == nil
	}, 15*time.Second, 100*time.Millisecond, "query after crash never succeeded: %v", lastErr)
	require.True(t, held)
	require.True(t, tok.Equal(orig), "lock must survive on the survivor's replica view: token=%s (want %s)", tok, orig)

	// And it is usable: releasable, then re-grantable at a higher token.
	require.NoError(t, lk.Release(), "release after crash")
	next, err := follower.pool.TryAcquire("gpu", 5*time.Second)
	require.NoError(t, err, "re-acquire after crash")
	require.True(t, next.Token().After(orig), "re-acquired token %s must outrank %s", next.Token(), orig)
	next.Release()
}

// A single-node cluster must serve: W degrades to the leader's own copy, since
// refusing every operation on a one-node deployment serves nobody.
func TestSingleNodeClusterServes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{count: 1, minSize: 1})
	p := nodes[0].pool

	waitGrantable(t, p, 15*time.Second)

	lk, err := p.TryAcquire("solo", 2*time.Second)
	require.NoError(t, err, "a single-node pool must grant")
	require.NoError(t, lk.Extend(2*time.Second), "extend on a single-node pool")
	require.NoError(t, lk.Release(), "release on a single-node pool")
}

// A holder dying should free its locks quickly rather than waiting out the TTL.
func TestHolderDeathFreesLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{count: 3, minSize: 2, lockCfg: longLockCfg})

	leaderID := waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 40*time.Second, 2*time.Second)

	// Pick a holder that is NOT the leader, and take a long lock.
	followers := others(nodes, leaderID)
	holderNode, survivor := followers[0], followers[1]

	_, err := holderNode.pool.TryAcquire("orphan", 55*time.Second)
	require.NoError(t, err)

	// Kill the holder without releasing, and without announcing.
	holderNode.Crash()

	// The leader should notice the death and free the lock well inside the 55s
	// TTL. Allow generous time for failure detection.
	require.Eventually(t, func() bool {
		lk, err := survivor.pool.TryAcquire("orphan", 2*time.Second)
		if err != nil {
			return false
		}
		lk.Release()
		return true
	}, 45*time.Second, 200*time.Millisecond, "a dead holder's lock was not freed ahead of its TTL")
}

// A node that cannot reach quorum (two nodes that think they are part of a
// 5-node cluster: 2 < 3) must not elect a leader, so it must not grant locks.
func TestNoQuorumMeansNoLeaderAndNoGrants(t *testing.T) {
	nodes := build(t, harnessOpts{count: 2, minSize: 3})

	// Give them ample opportunity to (wrongly) elect themselves.
	require.Never(t, func() bool {
		for _, n := range nodes {
			if n.election.HasLeader() || n.pool.IsLeader() {
				return true
			}
			if lk, err := n.pool.TryAcquire("k", time.Second); err == nil {
				lk.Release()
				return true
			}
		}
		return false
	}, time.Second, 100*time.Millisecond, "a node without quorum elected a leader or granted a lock")
}

// --- NodeGroup scoping ---

// A pool driven by a group-scoped election must be coordinated by the group's
// leader, and nodes outside the group must still be able to take locks.
func TestNodeGroupScopedPool(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	// Four nodes; the first three are workers, the fourth is not. Every node
	// runs a worker-scoped election, including the bystander so it can
	// discover the group's leader.
	nodes := build(t, harnessOpts{
		count: 4, minSize: 2,
		metadata: func(i int) map[string]string {
			if i < 3 {
				return map[string]string{"role": "worker"}
			}
			return map[string]string{"role": "bystander"}
		},
		criteria: map[string]string{"role": "worker"},
		lockCfg: func() *lock.Config {
			cfg := defaultLockCfg()
			cfg.Name = "grouped"
			return cfg
		},
	})

	leaderID := waitOneLeader(t, nodes)

	// The leader must be one of the workers, never the bystander.
	bystander := nodes[3]
	require.NotEqual(t, bystander.ID(), leaderID, "the group leader must be a group member")
	require.False(t, bystander.pool.IsLeader(), "a non-member must never serve as the group's lock authority")
	t.Logf("group leader: %s", nodeOf(nodes, leaderID).Label)

	waitGrantable(t, nodes[0].pool, 25*time.Second)

	// The bystander can still take a lock, coordinated by the group's leader.
	lk, err := bystander.pool.TryAcquire("grouped-key", 3*time.Second)
	require.NoError(t, err, "a non-member should still be able to lock")

	// And it excludes the workers.
	for _, n := range nodes[:3] {
		if _, err := n.pool.TryAcquire("grouped-key", 2*time.Second); err == nil {
			t.Errorf("%s was granted a lock held by the bystander", n.Label)
		}
	}

	held, owner, _, _, err := nodes[0].pool.Query("grouped-key")
	require.NoError(t, err)
	if !held || owner != bystander.ID() {
		t.Errorf("expected the bystander to own the key, got held=%v owner=%v", held, owner)
	}

	lk.Release()
}

// Two differently scoped pools on one cluster must not interfere, even for the
// same key.
func TestGroupAndClusterPoolsAreIndependent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	// A cluster-wide election and pool on every node (from the harness), plus a
	// db-scoped election and pool.
	nodes := build(t, harnessOpts{
		count: 3, minSize: 2,
		metadata: func(i int) map[string]string {
			if i < 2 {
				return map[string]string{"tier": "db"}
			}
			return nil
		},
		lockCfg: func() *lock.Config {
			cfg := defaultLockCfg()
			cfg.Name = "cluster-pool"
			return cfg
		},
	})

	groupEC := leader.DefaultConfig()
	groupEC.LeaderCheckInterval = 100 * time.Millisecond
	groupEC.LeaderTimeout = time.Second
	groupEC.MinClusterSize = 2
	groupEC.MetadataCriteria = map[string]string{"tier": "db"}
	groupEC.HeartbeatMessageType = gossip.ReservedMsgsStart + 41
	groupEC.ForgetMessageType = gossip.ReservedMsgsStart + 43

	var groupPools []*lock.Pool
	for _, n := range nodes {
		ge := leader.NewLeaderElection(n.Cluster, groupEC)
		ge.Start()
		n.OnStop(ge.Stop)
		gp := lock.NewPool(n.Cluster, ge, &lock.Config{
			Name: "group-pool", MinTTL: 500 * time.Millisecond, MaxTTL: 3 * time.Second,
		})
		n.OnStop(gp.Close)
		groupPools = append(groupPools, gp)
	}

	waitGrantable(t, nodes[0].pool, 25*time.Second)
	waitGrantable(t, groupPools[0], 25*time.Second)

	// The same key in both pools is two separate locks.
	a, err := nodes[0].pool.TryAcquire("same-name", 3*time.Second)
	require.NoError(t, err, "cluster pool acquire")
	b, err := groupPools[1].TryAcquire("same-name", 3*time.Second)
	require.NoError(t, err, "group pool should hold the same key independently")

	// But within each pool it is still exclusive.
	if _, err := nodes[1].pool.TryAcquire("same-name", 2*time.Second); err == nil {
		t.Error("cluster pool key should be exclusive")
	}
	if _, err := groupPools[0].TryAcquire("same-name", 2*time.Second); err == nil {
		t.Error("group pool key should be exclusive")
	}

	a.Release()
	b.Release()
}

// Two pools on one cluster replicate independently: each has its own replica
// store and its own recovery, so a leader crash recovers both and a key held
// in each pool survives in each. The same key name in different pools remains
// two unrelated locks.
func TestTwoPoolsBothSurviveLeaderCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{count: 3, minSize: 2, lockCfg: func() *lock.Config {
		cfg := longLockCfg()
		cfg.Name = "a"
		return cfg
	}})

	// A second pool on the same election, on every node (replicas must exist
	// wherever pushes may land).
	poolB := make(map[*node]*lock.Pool, len(nodes))
	for _, n := range nodes {
		cfg := longLockCfg()
		cfg.Name = "b"
		poolB[n] = lock.NewPool(n.Cluster, n.election, cfg)
		n.OnStop(poolB[n].Close)
	}

	leaderID := waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 20*time.Second, 2*time.Second)
	waitGrantable(t, poolB[nodes[0]], 20*time.Second, 2*time.Second)

	followers := others(nodes, leaderID)

	lkA, err := followers[0].pool.TryAcquire("shared-key", 55*time.Second)
	require.NoError(t, err, "pool a acquire")
	lkB, err := poolB[followers[0]].TryAcquire("shared-key", 55*time.Second)
	require.NoError(t, err, "pool b acquire (the same key in the other pool is grantable simultaneously)")
	tokA, tokB := lkA.Token(), lkB.Token()

	victim := nodeOf(nodes, leaderID)
	t.Logf("killing leader %s", victim.Label)
	victim.Crash()

	waitNewLeader(t, followers, leaderID)

	// Both pools must recover and report their lock held with its token.
	for name, check := range map[string]struct {
		tok lock.Token
		q   func() (bool, lock.Token, error)
	}{
		"a": {tokA, func() (bool, lock.Token, error) {
			h, _, tk, _, err := followers[1].pool.Query("shared-key")
			return h, tk, err
		}},
		"b": {tokB, func() (bool, lock.Token, error) {
			h, _, tk, _, err := poolB[followers[1]].Query("shared-key")
			return h, tk, err
		}},
	} {
		var held bool
		var tok lock.Token
		var lastErr error
		if !assertEventually(t, func() bool {
			h, tk, err := check.q()
			held, tok, lastErr = h, tk, err
			return err == nil
		}, 15*time.Second) || !held {
			t.Errorf("pool %s: lock did not survive the failover (last error: %v)", name, lastErr)
			continue
		}
		if !tok.Equal(check.tok) {
			t.Errorf("pool %s: token changed across failover: %s became %s", name, check.tok, tok)
		}
	}

	_ = lkA.Release()
	_ = lkB.Release()
}

// A node that joins after locks exist must become a useful replica promptly
// (catch-up at pool start), and fire-and-forget gossip lost while it was
// partitioned must be healed by the anti-entropy re-gossip once it returns.
func TestLateJoinerCatchesUpAndRegossipHeals(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	nodes := build(t, harnessOpts{count: 3, minSize: 2, lockCfg: longLockCfg})

	waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 20*time.Second, 2*time.Second)

	// Five locks before the late joiner exists.
	var held []*lock.Lock
	for i := range 5 {
		lk, err := nodes[0].pool.TryAcquire(fmt.Sprintf("early-%d", i), 55*time.Second)
		require.NoError(t, err, "acquire early-%d", i)
		held = append(held, lk)
	}

	// A fourth node joins late and creates its pool.
	joiner := &node{Node: clustertest.NewNode(t, "late")}
	joiner.Join(t, nodes[0].Node)
	clustertest.WaitAlive(t, joiner.Cluster, 4)
	harnessOpts{minSize: 2, lockCfg: longLockCfg}.attach(joiner)

	// Catch-up at pool start should populate the joiner's replica store.
	require.Eventually(t, func() bool { return joiner.pool.ReplicaCount() >= 5 },
		5*time.Second, 50*time.Millisecond, "late joiner should catch up to 5 locks, knows %d", joiner.pool.ReplicaCount())

	// Partition the joiner (alive and running, just unreachable) and take more
	// locks: their fire-and-forget gossip cannot reach it.
	joiner.Net.Cut()
	for i := range 2 {
		lk, err := nodes[0].pool.TryAcquire(fmt.Sprintf("late-%d", i), 55*time.Second)
		require.NoError(t, err, "acquire late-%d while joiner partitioned", i)
		held = append(held, lk)
	}

	// Heal the partition: anti-entropy re-gossip must bring the joiner back.
	joiner.Net.Uncut()
	require.Eventually(t, func() bool { return joiner.pool.ReplicaCount() >= 7 },
		15*time.Second, 100*time.Millisecond, "re-gossip should heal the partitioned replica to 7 locks, knows %d", joiner.pool.ReplicaCount())

	for _, lk := range held {
		_ = lk.Release()
	}
}

// --- concurrency / shutdown safety ---

func TestConcurrentTrafficDuringShutdown(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3, minSize: 2})

	waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for _, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("s%d", i%5)
				i++
				if lk, err := n.pool.TryAcquire(key, time.Second); err == nil {
					_ = lk.Extend(time.Second)
					_ = lk.Release()
				}
			}
		}()
	}

	time.Sleep(400 * time.Millisecond)

	// Close the pools underneath the running traffic. The pool references are
	// deliberately left in place: a closed pool must return ErrPoolClosed
	// rather than become unusable, and clearing the field here would just race
	// with the traffic goroutines.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, n := range nodes {
			n.pool.Close()
			time.Sleep(30 * time.Millisecond)
		}
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		close(stop)
		wg.Wait()
		t.Fatal("closing pools under live traffic deadlocked")
	}

	close(stop)
	wg.Wait()
}

func TestBlockingAcquireMakesProgress(t *testing.T) {
	nodes := build(t, harnessOpts{count: 3, minSize: 2})

	waitOneLeader(t, nodes)
	waitGrantable(t, nodes[0].pool, 20*time.Second)

	mon := newMonitor()
	var done atomic.Int64
	var wg sync.WaitGroup

	for _, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			for range 8 {
				lk, err := n.pool.Acquire(ctx, "queued", 1*time.Second)
				if err != nil {
					return
				}
				if mon.enter("queued", n.Label, lk.Token()) {
					time.Sleep(3 * time.Millisecond)
					mon.exit("queued", n.Label)
				}
				done.Add(1)
				lk.Release()
			}
		}()
	}
	wg.Wait()

	t.Logf("blocking acquisitions: %d", done.Load())

	mon.requireExclusive(t, "blocking Acquire")
	if done.Load() < 12 {
		t.Errorf("expected steady progress across waiters, got %d", done.Load())
	}
}
