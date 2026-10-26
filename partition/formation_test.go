package partition

import (
	"sync/atomic"
	"testing"
)

// formationCluster is a fake cluster of three whose nodes keep their formed marker in an array.
func formationCluster(t *testing.T) (*fakeCluster, *[3]atomic.Bool) {
	var formed [3]atomic.Bool
	fc := newFakeCluster(t, 3, 3, func(i int, cfg *Config) {
		cfg.Formed = func() bool { return formed[i].Load() }
		cfg.SetFormed = func() error { formed[i].Store(true); return nil }
	})
	fc.attach()
	return fc, &formed
}

func loseData(n *fakeNode) {
	n.store.intact = false
	n.m.resync.Store(true)
}

// A new data set forms around its founder: the founder is intact and unformed, the joiners are empty and unformed, and
// the first joiner copies from the founder alone. Afterwards every node has formed.
func TestFormationFromTheFounder(t *testing.T) {
	fc, formed := formationCluster(t)
	founder, j1, j2 := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	founder.store.seedCommitted(1, 2, ep(1, 1))
	loseData(j1)
	loseData(j2)
	if !j1.m.syncOnce() || j1.m.Resyncing() {
		t.Fatal("the first joiner did not copy the data set from the founder")
	}
	if st := j1.store.state(1); st.Committed.Seq != 2 {
		t.Fatalf("the joiner holds %s, want the founder's seq 2", st.Committed)
	}
	if !formed[1].Load() {
		t.Fatal("the joiner has not formed after syncing with the founder")
	}
	if !founder.m.syncOnce() || !formed[0].Load() {
		t.Fatal("the founder did not form once a joiner is intact")
	}
	if !j2.m.syncOnce() || j2.m.Resyncing() || !formed[2].Load() {
		t.Fatal("the second joiner did not copy the data set")
	}
}

// Without the formation exception, an intact answer from an unformed node does not count toward the recovery quorum
// while a formed node has answered too: a node that has taken part in no majority may be stale.
func TestUnformedIntactAnswerDoesNotCount(t *testing.T) {
	fc, _ := formationCluster(t)
	r, formedPeer, unformedPeer := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	formedPeer.m.formed.Store(true)
	loseData(r)
	if r.m.syncOnce() || !r.m.Resyncing() {
		t.Fatal("recovered from one formed and one unformed answer: the unformed node's answer counted")
	}
	unformedPeer.m.formed.Store(true)
	if !r.m.syncOnce() || r.m.Resyncing() {
		t.Fatal("did not recover from two formed intact answers")
	}
}

// The formation exception needs every answer unformed, formed nodes that are resyncing themselves included, and this
// node unformed as well.
func TestFormationNotTakenWhenAnyAnswerIsFormed(t *testing.T) {
	t.Run("a resyncing formed peer answers", func(t *testing.T) {
		fc, _ := formationCluster(t)
		r, founder, lost := fc.nodes[0], fc.nodes[1], fc.nodes[2]
		_ = founder
		loseData(r)
		loseData(lost)
		lost.m.formed.Store(true)
		if r.m.syncOnce() || !r.m.Resyncing() {
			t.Fatal("formed from the founder although a formed node answered")
		}
	})
	t.Run("this node has formed", func(t *testing.T) {
		fc, _ := formationCluster(t)
		r := fc.nodes[0]
		loseData(r)
		r.m.formed.Store(true)
		if r.m.syncOnce() || !r.m.Resyncing() {
			t.Fatal("a formed node resynced from unformed answers")
		}
	})
	t.Run("a node is down", func(t *testing.T) {
		// The two joiners formed and committed writes the founder never saw; then one joiner lost its disk while the
		// other is down. The stale founder answers alone, unformed: it must not be recovered from.
		fc, _ := formationCluster(t)
		r, founder, down := fc.nodes[0], fc.nodes[1], fc.nodes[2]
		founder.store.seedCommitted(1, 2, ep(1, 1))
		loseData(r)
		down.down.Store(true)
		if r.m.syncOnce() || !r.m.Resyncing() {
			t.Fatal("recovered from the founder alone while another node of the cluster did not answer")
		}
	})
	t.Run("no intact answer", func(t *testing.T) {
		fc, _ := formationCluster(t)
		loseData(fc.nodes[0])
		loseData(fc.nodes[1])
		loseData(fc.nodes[2])
		if fc.nodes[0].m.syncOnce() {
			t.Fatal("formed with no intact node at all")
		}
	})
}
