package partition

import "testing"

// Independent joiners may form without the founder hearing either one's intact
// state. That founder must not later seed recovery of a lost joiner by itself.
func TestFresh9StaleUnformedFounderCannotRecoverLostJoiner(t *testing.T) {
	fc, formed := formationCluster(t)
	a, b, c := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	a.store.seedCommitted(1, 2, ep(1, 1))
	loseData(b)
	loseData(c)
	fc.cutLink(b.id, c.id, true)
	if !b.m.syncOnce() || !c.m.syncOnce() {
		t.Fatal("initial independent join failed")
	}
	if formed[0].Load() || !formed[1].Load() || !formed[2].Load() {
		t.Fatal("unexpected formation state")
	}
	b.m.ready.Store(true)
	c.m.ready.Store(true)
	// The founder is now unreachable. Its two formed joiners can take over and
	// acknowledge a write without ever promising anything on the founder.
	fc.cutLink(b.id, c.id, false)
	fc.isolate(a, true)
	if err := b.m.takeover(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	res, err := b.m.Write(t.Context(), 1, []byte("acknowledged-before-disk-loss"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Pos.Seq != 3 || formed[0].Load() {
		t.Fatalf("unexpected committed state: %s founder formed=%v", res.Pos, formed[0].Load())
	}
	// C loses its data; B is unreachable while A answers. Use a fresh manager as
	// at restart, retaining the fixture's durable formed callbacks.
	c.m.Stop()
	formed[2].Store(false)
	c.store = newMemStore()
	c.store.intact = false
	cfg := c.m.cfg
	cfg.Store = c.store
	c.m, err = newManager(cfg, &fakeNet{c})
	if err != nil {
		t.Fatal(err)
	}
	c.mgr.Store(c.m)
	fc.isolate(b, true)
	fc.cutLink(a.id, c.id, false)
	if c.m.syncOnce() || !c.m.Resyncing() {
		t.Fatalf("lost joiner recovered from stale unformed founder: recovered seq=%d, acknowledged seq=%d", c.store.state(1).Committed.Seq, res.Pos.Seq)
	}
}
