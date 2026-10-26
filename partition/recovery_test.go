package partition

import (
	"sync/atomic"
	"testing"
	"time"
)

// A forming peer may be stale: a majority could have committed without it.
// Neither retaining nor losing the recovering node's established marker makes
// that peer a substitute for the recovery quorum. Only ForceResync opts out.
func TestResyncFormingPeerRequiresQuorum(t *testing.T) {
	for _, established := range []bool{false, true} {
		name := "fresh-marker"
		if established {
			name = "retained-marker"
		}
		t.Run(name, func(t *testing.T) {
			fc := newFakeCluster(t, 3, 3, func(i int, cfg *Config) {
				cfg.Established = func() bool { return i == 0 && established }
				cfg.SetEstablished = func() error { return nil }
			})
			fc.attach()
			recovering, unavailable, forming := fc.nodes[0], fc.nodes[1], fc.nodes[2]
			unavailable.store.seedCommitted(0, 1, ep(1, 2))
			unavailable.down.Store(true)
			recovering.store.intact = false
			recovering.m.resync.Store(true)
			if r := forming.m.handleState(&stateReq{Size: 3}); !r.Intact || !r.Forming {
				t.Fatalf("expected an intact forming peer: %+v", r)
			}
			if recovering.m.syncOnce() || !recovering.m.Resyncing() {
				t.Fatal("recovered from a stale forming peer without the recovery quorum")
			}
			// The operator may explicitly accept this risk for one start.
			recovering.m.cfg.ForceResync = true
			if !recovering.m.syncOnce() || recovering.m.Resyncing() {
				t.Fatal("explicit ForceResync did not recover from the intact forming peer")
			}
		})
	}
}

// Exercise lost-disk recovery over real sockets. Nodes 0 and 1 acknowledge a
// write before intact node 2 ever participates. After node 0 loses its disk and
// node 1 goes offline, node 2 must not let node 0 recover from stale data alone.
func TestDTFormingPeerCannotLoseAcknowledgedWrite(t *testing.T) {
	var established [3]atomic.Bool
	c := dtNewCluster(t, dtOpts{Nodes: 3, Parts: 1, Tweak: func(i int, cfg *Config) {
		cfg.Established = func() bool { return established[i].Load() }
		cfg.SetEstablished = func() error { established[i].Store(true); return nil }
	}})
	c.start(0)
	c.start(1)
	c.awaitReady(15*time.Second, 0, 1)
	c.awaitOwners(15*time.Second, 0, 1)
	original := c.increment(0, 0, dtKey(0, 0))
	dtRequire(t, original.res == dtOK, "original write was not acknowledged: %+v", original)
	dtRequire(t, original.pos.Seq == 1, "original write at %s, want sequence 1", original.pos)
	dtRequire(t, !established[2].Load(), "offline peer unexpectedly established")

	c.crash(0)
	c.crash(1)
	c.start(2)
	c.restart(0, false)
	c.awaitMeshed(15 * time.Second)
	// Allow repeated startup syncs and the ready hold-down to run. A forming
	// answer previously let the lost-disk node become ready during this window.
	time.Sleep(3 * dtOwnerTimeout)
	m, _ := c.node(0)
	dtRequire(t, m.Resyncing() && !m.Ready(), "lost-disk node recovered from a stale forming peer")
	dtRequire(t, !m.CanRead(0) && !m.IsOwner(0), "lost-disk node served stale data")
	blocked := c.increment(0, 0, dtKey(0, 0))
	dtRequire(t, blocked.res == dtUnavailable, "write without a recovery quorum returned %s (%v)", blocked.res, blocked.err)

	// Restoring the intact peer supplies the missing recovery quorum. The
	// acknowledged write must be restored before the next change is prepared.
	c.restart(1, true)
	c.awaitReady(15 * time.Second)
	c.awaitOwners(15 * time.Second)
	c.awaitConverged(15 * time.Second)
	for i := range 3 {
		_, s := c.node(i)
		dtRequire(t, s.version(0, dtKey(0, 0)) == 1, "node %d lost the acknowledged write", i)
	}
	next := c.increment(0, 0, dtKey(0, 0))
	dtRequire(t, next.res == dtOK, "write after recovery returned %s (%v)", next.res, next.err)
	dtRequire(t, next.pos.Seq == 2, "write after recovery at %s, want sequence 2", next.pos)
	c.awaitConverged(15 * time.Second)
	c.verify()
}
