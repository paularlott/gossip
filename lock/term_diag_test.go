package lock_test

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/leader"
	"github.com/paularlott/gossip/lock"
)

// TestDiagTermPropagation checks that the election term reaches the lock table,
// so fencing tokens carry a non-zero term.
func TestDiagTermPropagation(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 19960, count: 3, minSize: 2})
	defer teardown(nodes)

	leaderID := waitOneLeader(t, nodes, 20*time.Second)

	for _, n := range nodes {
		t.Logf("%s: electionTerm=%d isLeader=%v hasLeader=%v",
			n.label, n.election.Term(), n.pool.IsLeader(), n.election.HasLeader())
	}

	ldr := poolOf(nodes, leaderID)
	if ldr.election.Term() == 0 {
		t.Errorf("leader %s reports term 0 — the term never advanced", ldr.label)
	}

	waitGrantable(t, ldr.pool, 20*time.Second)

	lk, err := ldr.pool.TryAcquire("term-probe", 2*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lk.Release()

	t.Logf("granted token=%s (election term %d)", lk.Token(), ldr.election.Term())

	if lk.Token().Term == 0 {
		t.Errorf("token %s has term 0; election term is %d — term is not reaching the table",
			lk.Token(), ldr.election.Term())
	}
	if lk.Token().Term != ldr.election.Term() {
		t.Errorf("token term %d does not match election term %d",
			lk.Token().Term, ldr.election.Term())
	}
}

// TestDiagTermAdvancesOnLeaderChange confirms the term increases when leadership
// moves, which is what makes a new leader's first token outrank the old leader's.
func TestDiagTermAdvancesOnLeaderChange(t *testing.T) {
	nodes := build(t, harnessOpts{basePort: 19970, count: 3, minSize: 2})
	defer func() {
		var live []*node
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		teardown(live)
	}()

	leaderID := waitOneLeader(t, nodes, 20*time.Second)
	ldr := poolOf(nodes, leaderID)
	termBefore := ldr.election.Term()
	t.Logf("initial leader %s at term %d", ldr.label, termBefore)

	var survivors []*node
	for _, n := range nodes {
		if n.cluster.LocalNode().ID != leaderID {
			survivors = append(survivors, n)
		}
	}

	ldr.cluster.Stop()
	ldr.pool = nil
	ldr.election = nil
	ldr.cluster = nil

	newID := waitOneLeader(t, survivors, 30*time.Second)
	nl := poolOf(survivors, newID)
	termAfter := nl.election.Term()

	t.Logf("new leader %s at term %d (was %d)", nl.label, termAfter, termBefore)

	if termAfter <= termBefore {
		t.Errorf("term did not advance across leadership change: %d -> %d", termBefore, termAfter)
	}

	// And the table should be serving that same term.
	waitGrantableTTL(t, nl.pool, 40*time.Second, 1*time.Second)
	lk, err := nl.pool.TryAcquire("after-failover", 1*time.Second)
	if err != nil {
		t.Fatalf("acquire after failover: %v", err)
	}
	defer lk.Release()

	t.Logf("post-failover token=%s", lk.Token())
	if lk.Token().Term != termAfter {
		t.Errorf("token term %d != election term %d", lk.Token().Term, termAfter)
	}
}

// waitGrantableTTL is waitGrantable with an explicit probe TTL, for pools whose
// MinTTL rules out the default probe.
func waitGrantableTTL(t *testing.T, p *lock.Pool, limit, ttl time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	var last error
	for time.Now().Before(deadline) {
		lk, err := p.TryAcquire("__probe__", ttl)
		if err == nil {
			lk.Release()
			return
		}
		last = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("pool never became grantable within %v (last: %v)", limit, last)
}

var _ = leader.DefaultConfig
