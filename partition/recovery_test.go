package partition

import (
	"testing"
	"time"
)

// An intact peer may be stale: a majority could have committed without it. It is not a substitute for the recovery
// quorum; only ForceResync opts out.
func TestResyncOneIntactPeerRequiresQuorum(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.attach()
	recovering, unavailable, peer := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	unavailable.store.seedCommitted(0, 1, ep(1, 2))
	unavailable.down.Store(true)
	recovering.store.intact = false
	recovering.m.resync.Store(true)
	if r := peer.m.handleState(&stateReq{Size: 3}); !r.Intact {
		t.Fatalf("expected an intact peer: %+v", r)
	}
	if recovering.m.syncOnce() || !recovering.m.Resyncing() {
		t.Fatal("recovered from one stale peer without the recovery quorum")
	}
	// The operator may explicitly accept this risk for one start.
	recovering.m.cfg.ForceResync = true
	if !recovering.m.syncOnce() || recovering.m.Resyncing() {
		t.Fatal("explicit ForceResync did not recover from the intact peer")
	}
}

// Exercise lost-disk recovery over real sockets. Nodes 0 and 1 acknowledge a
// write before intact node 2 ever participates. After node 0 loses its disk and
// node 1 goes offline, node 2 must not let node 0 recover from stale data alone.
func TestDTStalePeerCannotLoseAcknowledgedWrite(t *testing.T) {
	c := dtNewCluster(t, dtOpts{Nodes: 3, Parts: 1})
	c.start(0)
	c.start(1)
	c.awaitReady(15*time.Second, 0, 1)
	c.awaitOwners(15*time.Second, 0, 1)
	original := c.increment(0, 0, dtKey(0, 0))
	dtRequire(t, original.res == dtOK, "original write was not acknowledged: %+v", original)
	dtRequire(t, original.pos.Seq == 1, "original write at %s, want sequence 1", original.pos)

	c.crash(0)
	c.crash(1)
	c.start(2)
	c.restart(0, false)
	c.awaitMeshed(15 * time.Second)
	// Allow repeated startup syncs and the ready hold-down to run. Node 2 alone, which never took part, must not let
	// the lost-disk node become ready during this window.
	time.Sleep(3 * dtOwnerTimeout)
	m, _ := c.node(0)
	dtRequire(t, m.Resyncing() && !m.Ready(), "lost-disk node recovered from a stale peer")
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
