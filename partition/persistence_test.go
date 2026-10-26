package partition

import (
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestCommitThatIsLostOnRestartConverges(t *testing.T) {
	for _, victims := range [][]int{{0}, {1}, {2}, {0, 1, 2}} {
		t.Run(fmt.Sprint(victims), func(t *testing.T) {
			fc := newFakeCluster(t, 3, 3, nil)
			fc.startAll()
			fc.waitOwners(10 * time.Second)
			waitRankZeroOwners(t, fc, 10*time.Second)
			const p = 1
			var want []string
			for i := 0; i < 5; i++ {
				w := fmt.Sprintf("w%d", i)
				writeRetry(t, fc.nodes[i%3], p, w)
				want = append(want, w)
			}
			converge(t, p, fc.nodes...)
			time.Sleep(3 * fc.nodes[0].m.cfg.HeartbeatInterval)

			// The nodes crash and lose their last Commit or Import: neither is durable. The owner, and every follower that
			// acknowledged the last write, stored it as pending before committing it, and holds it as pending again. A
			// follower whose write request lost the race with the commit notice got the write by catch-up instead (an
			// Import, after which the late request is acknowledged by Seq without storing anything): it loses the Import
			// and holds nothing of the write, which a majority still holds as pending. With every node lost, the write
			// exists only as pending.
			owner := fc.owners(p)[0]
			for _, v := range victims {
				fc.crash(fc.nodes[v])
			}
			for _, v := range victims {
				st := fc.nodes[v].store
				if !st.loseLastCommit() {
					t.Fatalf("node %d: nothing to lose", v)
				}
				s := st.state(p)
				lostCommit := s.Committed.Seq == 4 && s.Pending != nil && s.Pending.Pos.Seq == 5
				if s.Committed.Seq >= 5 || (fc.nodes[v] == owner && !lostCommit) {
					t.Fatalf("node %d after the lost commit: %+v", v, s)
				}
				if err := st.checkInvariant(); err != nil {
					t.Fatal(err)
				}
			}
			holders := 0
			for _, fn := range fc.nodes {
				if s := fn.store.state(p); s.Committed.Seq >= 5 || (s.Pending != nil && s.Pending.Pos.Seq == 5) {
					holders++
				}
			}
			if holders < majority(3) {
				t.Fatalf("only %d nodes hold the acknowledged write after the crash", holders)
			}
			for _, v := range victims {
				fc.restart(fc.nodes[v], nil)
			}
			waitFor(t, 20*time.Second, "every node ready", func() bool {
				for _, fn := range fc.nodes {
					if !fn.m.Ready() {
						return false
					}
				}
				return true
			})
			fc.waitOwners(15 * time.Second)
			writeRetry(t, fc.nodes[(victims[0]+1)%3], p, "after")
			want = append(want, "after")
			if got := converge(t, p, fc.nodes...); !slices.Equal(got, want) {
				t.Errorf("data = %v, want %v: an acknowledged write was lost or applied twice", got, want)
			}
		})
	}
}

func TestResyncingAndHandleResyncedFunc(t *testing.T) {
	fc := newFakeCluster(t, 3, 3, nil)
	fc.startAll()
	fc.waitOwners(10 * time.Second)
	for i := 0; i < 3; i++ {
		writeRetry(t, fc.nodes[i], 1, fmt.Sprintf("w%d", i))
	}
	converge(t, 1, fc.nodes...)
	for _, fn := range fc.nodes {
		if fn.m.Resyncing() {
			t.Error("an intact node reports resyncing")
		}
	}

	victim := fc.nodes[2]
	fc.crash(victim)
	victim.store = newMemStore()
	victim.store.intact = false
	var calls atomic.Int32
	var readyWhenCalled, resyncingWhenCalled atomic.Bool
	victim.beforeStart = func(m *Manager) {
		m.HandleResyncedFunc(func() {
			calls.Add(1)
			readyWhenCalled.Store(m.Ready())
			resyncingWhenCalled.Store(m.Resyncing())
		})
	}
	fc.isolate(victim, true) // it cannot reach a majority of the others: it stays in resync mode
	fc.restart(victim, nil)
	time.Sleep(300 * time.Millisecond)
	if !victim.m.Resyncing() || calls.Load() != 0 || victim.m.Ready() {
		t.Fatalf("resyncing %v, handler called %d times, ready %v: want resyncing, never called, not ready",
			victim.m.Resyncing(), calls.Load(), victim.m.Ready())
	}
	fc.isolate(victim, false)
	waitFor(t, 10*time.Second, "the resync to finish", func() bool { return !victim.m.Resyncing() })
	waitFor(t, 10*time.Second, "the node to be ready", func() bool { return victim.m.Ready() })
	if calls.Load() != 1 || readyWhenCalled.Load() || resyncingWhenCalled.Load() {
		t.Errorf("handler called %d times (ready when called: %v, resyncing when called: %v); want once, after the resync and before ready",
			calls.Load(), readyWhenCalled.Load(), resyncingWhenCalled.Load())
	}
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 1 {
		t.Errorf("handler called %d times", calls.Load())
	}
}
