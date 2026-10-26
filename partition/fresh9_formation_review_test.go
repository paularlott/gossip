package partition

import "testing"

// Independent joiners may not form without hearing every node: a founder that saw neither joiner's formation could
// otherwise later seed the recovery of a lost joiner by itself, missing writes the two joiners acknowledged. With the
// link between the joiners cut, neither forms from the founder; once every node answers, both do.
func TestFresh9StaleUnformedFounderCannotRecoverLostJoiner(t *testing.T) {
	fc, formed := formationCluster(t)
	a, b, c := fc.nodes[0], fc.nodes[1], fc.nodes[2]
	a.store.seedCommitted(1, 2, ep(1, 1))
	loseData(b)
	loseData(c)
	fc.cutLink(b.id, c.id, true)
	if b.m.syncOnce() || c.m.syncOnce() || formed[1].Load() || formed[2].Load() {
		t.Fatal("a joiner formed from the founder without hearing the other joiner")
	}
	fc.cutLink(b.id, c.id, false)
	if !b.m.syncOnce() || !formed[1].Load() {
		t.Fatal("the first joiner did not form once every node answered")
	}
	if !a.m.syncOnce() || !formed[0].Load() {
		t.Fatal("the founder did not form once a joiner is intact")
	}
	if !c.m.syncOnce() || !formed[2].Load() {
		t.Fatal("the second joiner did not form")
	}
}
