package lock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/leader"
	"github.com/paularlott/logger"
)

// Compile-time proof that the real election satisfies the Pool's dependency.
var _ Leadership = (*leader.LeaderElection)(nil)

// --- fakes ---

// fakeLeadership is a controllable Leadership for deterministic tests. It
// notifies its watcher on every change, mirroring the real election, so pools
// built on it exercise the same event-driven path as production.
type fakeLeadership struct {
	mu         sync.Mutex
	has        bool
	isLeader   bool
	leaderID   gossip.NodeID
	term       uint64
	watcher    func(isLeader bool, term uint64)
	candidates []*gossip.Node
}

func (f *fakeLeadership) HasLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.has
}

func (f *fakeLeadership) IsLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.isLeader
}

func (f *fakeLeadership) GetLeaderID() gossip.NodeID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaderID
}

func (f *fakeLeadership) Term() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.term
}

// Candidates reports the configured peer set; nil by default, which runs the
// pool in its single-node degraded mode.
func (f *fakeLeadership) Candidates() []*gossip.Node {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.candidates
}

// WatchLeadership registers the callback and immediately reports current
// state, matching the real election's contract.
func (f *fakeLeadership) WatchLeadership(fn func(isLeader bool, term uint64)) (cancel func()) {
	f.mu.Lock()
	f.watcher = fn
	is, term := f.isLeader, f.term
	f.mu.Unlock()
	fn(is, term)
	return func() {}
}

// notify must be called after every state change, outside f.mu.
func (f *fakeLeadership) notify() {
	f.mu.Lock()
	fn, is, term := f.watcher, f.isLeader, f.term
	f.mu.Unlock()
	if fn != nil {
		fn(is, term)
	}
}

// become makes this node the leader at the given term.
func (f *fakeLeadership) become(id gossip.NodeID, term uint64) {
	f.mu.Lock()
	f.has, f.isLeader, f.leaderID, f.term = true, true, id, term
	f.mu.Unlock()
	f.notify()
}

// followTo points at a remote leader.
func (f *fakeLeadership) followTo(id gossip.NodeID, term uint64) {
	f.mu.Lock()
	f.has, f.isLeader, f.leaderID, f.term = true, false, id, term
	f.mu.Unlock()
	f.notify()
}

// lose drops all leadership knowledge.
func (f *fakeLeadership) lose() {
	f.mu.Lock()
	f.has, f.isLeader, f.leaderID = false, false, gossip.EmptyNodeID
	f.mu.Unlock()
	f.notify()
}

type mockTransport struct{ ch chan *gossip.Packet }

func newMockTransport() *mockTransport {
	return &mockTransport{ch: make(chan *gossip.Packet, 16)}
}
func (t *mockTransport) Name() string { return "mock" }
func (t *mockTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	return nil
}
func (t *mockTransport) PacketChannel() chan *gossip.Packet { return t.ch }
func (t *mockTransport) Send(tt gossip.TransportType, n *gossip.Node, p *gossip.Packet) error {
	return nil
}
func (t *mockTransport) SendWithReply(n *gossip.Node, p *gossip.Packet) (*gossip.Packet, error) {
	return nil, nil
}

func newTestCluster(t *testing.T) *gossip.Cluster {
	t.Helper()
	cfg := gossip.DefaultConfig()
	cfg.NodeID = uuid.New().String()
	cfg.Transport = newMockTransport()
	cfg.MsgCodec = codec.NewJSONCodec()
	cfg.Logger = logger.NewNullLogger()
	c, err := gossip.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	return c
}

// newLeaderPool builds a started cluster with a pool whose fake leadership is
// leader at term 1 and whose recovery has completed.
//
// The mock cluster has no peers, so the pool runs degraded — the leader's copy
// is the only replica — and recovery is instantaneous.
func newLeaderPool(t *testing.T, cfg *Config) (*Pool, *fakeLeadership, *gossip.Cluster) {
	t.Helper()

	c := newTestCluster(t)
	c.Start()

	f := &fakeLeadership{}
	f.become(c.LocalNode().ID, 1)

	p := NewPool(c, f, cfg)
	waitFor(t, 2*time.Second, func() bool { return p.tbl.servingNow() })

	return p, f, c
}

// takeOffice moves the fake leadership to a new term and waits for recovery.
func takeOffice(t *testing.T, p *Pool, f *fakeLeadership, c *gossip.Cluster, term uint64) {
	t.Helper()
	f.become(c.LocalNode().ID, term)
	waitFor(t, 2*time.Second, func() bool { return p.tbl.servingNow() })
}

func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", limit)
}

// serve makes a bare table act as a serving leader for the given term.
func serve(tbl *table, term uint64) {
	tbl.assumeLeadership(term)
	tbl.openForService()
}

// setNow overrides a table's clock for GC and expiry tests.
func setNow(tbl *table, now func() time.Time) {
	tbl.mu.Lock()
	tbl.nowFn = now
	tbl.mu.Unlock()
}

// wireGrant builds a wire entry for a live grant.
func wireGrant(key string, tok Token, owner gossip.NodeID, ttl time.Duration) replicaEntry {
	return replicaEntry{
		Key: key, Owner: owner, Token: tok, Held: true,
		ExpiresAtMs: time.Now().Add(ttl).UnixMilli(),
	}
}

// wireTomb builds a wire entry for a release.
func wireTomb(key string, tok Token) replicaEntry {
	return replicaEntry{Key: key, Token: tok, Held: false, ReleasedAtMs: time.Now().UnixMilli()}
}

// --- Token ---

func TestTokenOrdering(t *testing.T) {
	a := Token{Term: 1, Counter: 5}
	b := Token{Term: 1, Counter: 6}
	c := Token{Term: 2, Counter: 1}

	if !b.After(a) {
		t.Error("higher counter in same term must sort after")
	}
	if !c.After(b) {
		t.Error("higher term must dominate a lower counter")
	}
	if !a.Before(c) {
		t.Error("Before should mirror After")
	}
	if a.After(a) || a.Before(a) {
		t.Error("a token must not sort against itself")
	}
	if !a.Equal(Token{Term: 1, Counter: 5}) {
		t.Error("identical tokens must compare equal")
	}
}

// The whole point of term-first ordering: a new leader's very first token still
// outranks everything its predecessor issued, no matter how high that got.
func TestTokenNewTermBeatsHighCounter(t *testing.T) {
	old := Token{Term: 1, Counter: 1_000_000}
	fresh := Token{Term: 2, Counter: 1}

	if !fresh.After(old) {
		t.Fatal("a new term's first token must outrank the previous term's highest")
	}
}

func TestTokenZeroAndString(t *testing.T) {
	if !(Token{}).IsZero() {
		t.Error("zero token should report IsZero")
	}
	if (Token{Term: 1}).IsZero() {
		t.Error("non-zero token should not report IsZero")
	}
	if got := (Token{Term: 3, Counter: 42}).String(); got != "3.42" {
		t.Errorf("expected \"3.42\", got %q", got)
	}
	if got := (Token{}).String(); got != "0.0" {
		t.Errorf("expected \"0.0\", got %q", got)
	}
}

// --- Config ---

func TestConfigDefaults(t *testing.T) {
	var nilCfg *Config
	got := nilCfg.validate()
	want := DefaultConfig()

	if got.Name != want.Name || got.MinTTL != want.MinTTL ||
		got.MaxTTL != want.MaxTTL || got.RetryInterval != want.RetryInterval {
		t.Errorf("nil config should yield defaults, got %+v", got)
	}
	if got.WriteReplicas != want.WriteReplicas {
		t.Errorf("default WriteReplicas should be %d, got %d", want.WriteReplicas, got.WriteReplicas)
	}
}

func TestConfigNormalisesBadValues(t *testing.T) {
	got := (&Config{MinTTL: -1, MaxTTL: -1, RetryInterval: -1, WriteReplicas: -3}).validate()
	d := DefaultConfig()

	if got.MinTTL != d.MinTTL || got.MaxTTL != d.MaxTTL || got.RetryInterval != d.RetryInterval {
		t.Errorf("negative values should fall back to defaults, got %+v", got)
	}
	if got.WriteReplicas != d.WriteReplicas {
		t.Errorf("non-positive WriteReplicas should default to %d, got %d", d.WriteReplicas, got.WriteReplicas)
	}
	if got.Name != "default" {
		t.Errorf("empty name should default, got %q", got.Name)
	}
}

func TestConfigMaxTTLRaisedToMinTTL(t *testing.T) {
	got := (&Config{MinTTL: 10 * time.Second, MaxTTL: 2 * time.Second}).validate()
	if got.MaxTTL < got.MinTTL {
		t.Errorf("MaxTTL %v must not be below MinTTL %v", got.MaxTTL, got.MinTTL)
	}
}

func TestChunkEntries(t *testing.T) {
	mk := func(n int) []replicaEntry {
		out := make([]replicaEntry, n)
		for i := range out {
			out[i] = replicaEntry{Key: fmt.Sprintf("k%d", i)}
		}
		return out
	}

	// Fits one batch.
	got := chunkEntries(mk(3), 5)
	if len(got) != 1 || len(got[0]) != 3 {
		t.Errorf("small set should be a single batch, got %d batches", len(got))
	}

	// Splits on the boundary.
	got = chunkEntries(mk(7), 3)
	if len(got) != 3 || len(got[0]) != 3 || len(got[1]) != 3 || len(got[2]) != 1 {
		t.Errorf("7 entries at 3 per packet should be 3+3+1, got %+v", got)
	}

	// Non-positive per means one batch.
	got = chunkEntries(mk(9), 0)
	if len(got) != 1 || len(got[0]) != 9 {
		t.Errorf("per<=0 should not split, got %d batches", len(got))
	}

	// Nothing lost.
	total := 0
	for _, b := range chunkEntries(mk(10), 4) {
		total += len(b)
	}
	if total != 10 {
		t.Errorf("chunking lost entries: %d of 10", total)
	}
}

// --- table: happy paths ---

func TestTableAcquireReleaseCycle(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())

	ent, granted, reason := tbl.tryAcquire("k", owner, 5*time.Second)
	if !granted {
		t.Fatalf("expected grant, refused: %s", reason)
	}
	if ent.Token.Term != 1 || ent.Token.Counter != 1 {
		t.Errorf("expected token 1.1, got %s", ent.Token)
	}
	if !ent.Held || ent.Owner != owner {
		t.Errorf("grant entry malformed: %+v", ent)
	}

	held, gotOwner, gotTok, ms, ready := tbl.query("k")
	if !ready {
		t.Fatal("a serving table must be ready to answer")
	}
	if !held || gotOwner != owner || !gotTok.Equal(ent.Token) || ms <= 0 {
		t.Errorf("query disagrees with the grant: held=%v owner=%v tok=%s ms=%d", held, gotOwner, gotTok, ms)
	}

	tomb, released, reason := tbl.release("k", ent.Token)
	if !released {
		t.Fatalf("expected release, refused: %s", reason)
	}
	if tomb.Held || !tomb.Token.Equal(ent.Token) {
		t.Errorf("release should produce a tombstone of the grant, got %+v", tomb)
	}
	if held, _, _, _, _ := tbl.query("k"); held {
		t.Error("key should be free after release")
	}
}

func TestTableTokensIncreaseWithinTerm(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	var prev Token

	for i := 0; i < 25; i++ {
		ent, _, _ := tbl.tryAcquire("k", owner, 5*time.Second)
		if i > 0 && !ent.Token.After(prev) {
			t.Fatalf("iteration %d: token %s did not advance past %s", i, ent.Token, prev)
		}
		// Re-acquiring as the same owner keeps the token, so release between.
		tbl.release("k", ent.Token)
		prev = ent.Token
	}
}

func TestTableSameOwnerReacquireKeepsToken(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())

	first, _, _ := tbl.tryAcquire("k", owner, 1*time.Second)
	second, granted, _ := tbl.tryAcquire("k", owner, 10*time.Second)

	if !granted {
		t.Fatal("same owner should be allowed to refresh its own hold")
	}
	if !first.Token.Equal(second.Token) {
		t.Errorf("refreshing should keep the token: %s then %s", first.Token, second.Token)
	}
	if second.ExpiresAtMs <= first.ExpiresAtMs {
		t.Error("refresh should push the expiry out")
	}
}

func TestTableExtend(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	ent, _, _ := tbl.tryAcquire("k", owner, 1*time.Second)

	updated, ok, reason := tbl.extend("k", ent.Token, 30*time.Second)
	if !ok {
		t.Fatalf("expected extend to succeed, refused: %s", reason)
	}
	if !updated.Token.Equal(ent.Token) {
		t.Errorf("extend must keep the token: %s became %s", ent.Token, updated.Token)
	}

	_, _, _, ms, _ := tbl.query("k")
	if ms < 20_000 {
		t.Errorf("extend did not push expiry out; remaining %dms", ms)
	}
}

// --- table: unhappy paths ---

func TestTableRejectsOtherOwner(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	a := gossip.NodeID(uuid.New())
	b := gossip.NodeID(uuid.New())

	tbl.tryAcquire("k", a, 5*time.Second)

	_, granted, reason := tbl.tryAcquire("k", b, 5*time.Second)
	if granted {
		t.Fatal("a second owner must not be granted a held key")
	}
	if reason == "" {
		t.Error("refusal should carry a reason")
	}
}

func TestTableReleaseWrongToken(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	ent, granted, _ := tbl.tryAcquire("k", owner, 5*time.Second)
	if !granted || ent.Token.IsZero() {
		t.Fatal("expected a granted token to test against")
	}

	if _, released, _ := tbl.release("k", Token{Term: 1, Counter: 999}); released {
		t.Fatal("release with a mismatched token must be refused")
	}
	if held, _, _, _, _ := tbl.query("k"); !held {
		t.Error("the original hold should survive a bad release")
	}
}

func TestTableReleaseUnheldIsIdempotent(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	if _, released, _ := tbl.release("never-held", Token{Term: 1, Counter: 1}); !released {
		t.Error("releasing an unheld key should succeed silently")
	}
}

func TestTableExtendFailures(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())

	if _, ok, reason := tbl.extend("absent", Token{Term: 1, Counter: 1}, time.Second); ok || reason == "" {
		t.Error("extending an unheld key must fail with a reason")
	}

	ent, _, _ := tbl.tryAcquire("k", owner, 5*time.Second)
	if _, ok, _ := tbl.extend("k", Token{Term: 1, Counter: ent.Token.Counter + 50}, time.Second); ok {
		t.Error("extending with a mismatched token must fail")
	}
}

func TestTableExpiryFreesKey(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	a := gossip.NodeID(uuid.New())
	b := gossip.NodeID(uuid.New())

	tbl.tryAcquire("k", a, 20*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	ent, granted, _ := tbl.tryAcquire("k", b, 5*time.Second)
	if !granted {
		t.Fatal("an expired key must be grantable again")
	}
	if ent.Token.Counter < 2 {
		t.Errorf("expected a freshly issued token, got %s", ent.Token)
	}
}

func TestTableExtendAfterExpiryFails(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	ent, _, _ := tbl.tryAcquire("k", owner, 20*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	if _, ok, reason := tbl.extend("k", ent.Token, 5*time.Second); ok {
		t.Errorf("extending an expired lock must fail (reason %q)", reason)
	}
}

func TestTableReleaseByOwner(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	a := gossip.NodeID(uuid.New())
	b := gossip.NodeID(uuid.New())

	tbl.tryAcquire("a1", a, 5*time.Second)
	tbl.tryAcquire("a2", a, 5*time.Second)
	tbl.tryAcquire("b1", b, 5*time.Second)

	tombs := tbl.releaseByOwner(a)
	if len(tombs) != 2 {
		t.Fatalf("expected 2 tombstones, got %d", len(tombs))
	}
	if held, _, _, _, _ := tbl.query("b1"); !held {
		t.Error("the other owner's lock must be untouched")
	}
}

func TestTableReap(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	tbl.tryAcquire("k", owner, 10*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	tbl.reap()

	if tbl.count() != 0 {
		t.Errorf("expired entries should be reaped, %d remain", tbl.count())
	}
}

// --- table: serving and recovery ---

// Grants must be refused until the table has completed recovery, so decisions
// are never made against incomplete replica state.
func TestTableRefusesUntilServing(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	tbl.assumeLeadership(1)

	owner := gossip.NodeID(uuid.New())

	ent, granted, reason := tbl.tryAcquire("k", owner, 5*time.Second)
	if granted {
		t.Fatalf("an unrecovered table must not grant (token %s)", ent.Token)
	}
	if reason != reasonRecovering {
		t.Errorf("expected %q, got %q", reasonRecovering, reason)
	}

	tbl.openForService()
	if _, granted, _ := tbl.tryAcquire("k", owner, 5*time.Second); !granted {
		t.Error("should grant once recovery completes")
	}
}

// Query must not answer authoritatively while unrecovered; release, by
// contrast, is always safe — giving a lock back can never break exclusion.
func TestTableQueryNotReadyUntilServing(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	tbl.assumeLeadership(1)

	if _, _, _, _, ready := tbl.query("k"); ready {
		t.Error("an unrecovered table must not claim to answer authoritatively")
	}

	tbl.openForService()
	owner := gossip.NodeID(uuid.New())
	ent, _, _ := tbl.tryAcquire("k", owner, 5*time.Second)
	tbl.relinquishLeadership()

	if _, _, _, _, ready := tbl.query("k"); ready {
		t.Error("a stood-down table must not claim authority")
	}
	if _, released, _ := tbl.release("k", ent.Token); !released {
		t.Error("release must work on a stood-down table's replica state")
	}
}

func TestTableAssumeLeadershipResetsCounter(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	tbl.tryAcquire("k", owner, 5*time.Second)
	e1, _, _ := tbl.tryAcquire("k2", owner, 5*time.Second)
	t1 := e1.Token

	serve(tbl, 2)
	e2, _, _ := tbl.tryAcquire("k3", owner, 5*time.Second)
	t2 := e2.Token

	if t2.Term != 2 || t2.Counter != 1 {
		t.Errorf("new term should restart the counter, got %s", t2)
	}
	// Still strictly ordered despite the lower counter.
	if !t2.After(t1) {
		t.Errorf("token %s from term 2 must outrank %s from term 1", t2, t1)
	}
}

// Standing down stops the table serving but keeps the entries — they are this
// node's replica view, consulted by a future recovery.
func TestTableRelinquishKeepsEntries(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	tbl.tryAcquire("k", owner, 5*time.Second)

	tbl.relinquishLeadership()

	if tbl.count() != 1 {
		t.Errorf("standing down must keep the replica view, have %d live", tbl.count())
	}
	if _, granted, reason := tbl.tryAcquire("other", owner, time.Second); granted {
		t.Error("a stood-down table must not grant")
	} else if reason != reasonRecovering {
		t.Errorf("expected %q, got %q", reasonRecovering, reason)
	}
}

// --- table: merge rules ---

// Rule 1: a higher token dominates, which is what makes late-arriving writes
// from a previous term harmless.
func TestMergeHigherTokenWins(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 2)

	owner := gossip.NodeID(uuid.New())
	tbl.applyEntry(wireGrant("k", Token{Term: 2, Counter: 1}, owner, 30*time.Second))

	// A late write from the previous term's leader must not displace it.
	if tbl.applyEntry(wireGrant("k", Token{Term: 1, Counter: 9_999}, owner, 30*time.Second)) {
		t.Error("a lower-token entry must be dominated")
	}

	held, gotOwner, tok, _, _ := tbl.query("k")
	if !held || gotOwner != owner || tok.Counter != 1 {
		t.Errorf("merge let a stale write through: held=%v owner=%v tok=%s", held, gotOwner, tok)
	}
}

// Rule 2: equal token, the tombstone wins — this is what stops a released
// grant being resurrected by a lagging replica that never saw the release.
func TestMergeTombstoneBeatsGrant(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	tok := Token{Term: 1, Counter: 1}

	tbl.applyEntry(wireTomb("k", tok))
	// The grant arrives after its own tombstone (arrival order is arbitrary).
	tbl.applyEntry(wireGrant("k", tok, owner, 30*time.Second))

	if tbl.count() != 0 {
		t.Fatal("a tombstoned grant must not resurrect, regardless of arrival order")
	}
	if held, _, _, _, _ := tbl.query("k"); held {
		t.Error("the key must read as free")
	}
}

// A new-term grant legitimately supersedes an old-term tombstone: the release
// referred to the old grant, not the new one.
func TestMergeNewGrantBeatsOldTombstone(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 2)

	owner := gossip.NodeID(uuid.New())
	tbl.applyEntry(wireTomb("k", Token{Term: 1, Counter: 1}))
	tbl.applyEntry(wireGrant("k", Token{Term: 2, Counter: 1}, owner, 30*time.Second))

	if tbl.count() != 1 {
		t.Fatalf("the new grant must stand, live=%d", tbl.count())
	}
}

// Rule 3: equal token, both held, the later expiry wins — extends serialize at
// the leader, so its deadlines compare without cross-node clocks.
func TestMergeExtendsTakeLaterExpiry(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	tok := Token{Term: 1, Counter: 1}
	tbl.applyEntry(wireGrant("k", tok, owner, 10*time.Second))

	tbl.applyEntry(wireGrant("k", tok, owner, 60*time.Second))
	_, _, _, ms, _ := tbl.query("k")
	if ms < 50_000 {
		t.Errorf("the later expiry must win, remaining %dms", ms)
	}

	// And a stale earlier expiry arriving late must not shorten it.
	tbl.applyEntry(wireGrant("k", tok, owner, 10*time.Second))
	_, _, _, ms, _ = tbl.query("k")
	if ms < 50_000 {
		t.Errorf("a late short extend must not shorten the lease, remaining %dms", ms)
	}
}

// The local counter advances past recovered tokens of the current term so
// freshly minted tokens cannot collide with them.
func TestMergeAdvancesCounterPastRecoveredTokens(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 5)

	owner := gossip.NodeID(uuid.New())
	tbl.applyEntry(wireGrant("a", Token{Term: 5, Counter: 100}, owner, 30*time.Second))

	ent, _, _ := tbl.tryAcquire("b", owner, 5*time.Second)
	if ent.Token.Counter <= 100 {
		t.Errorf("token %s collides with or undercuts the recovered counter 100", ent.Token)
	}
}

func TestTableConcurrentAcquireSingleWinner(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	var granted atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner := gossip.NodeID(uuid.New())
			if ent, ok, _ := tbl.tryAcquire("contested", owner, 10*time.Second); ok {
				if ent.Owner == owner {
					granted.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if granted.Load() != 1 {
		t.Errorf("exactly one of 200 racers should win, got %d", granted.Load())
	}
}

// --- tombstone GC ---

// Tombstones must survive until any grant they killed is provably expired
// everywhere, then go.
func TestTombstoneGC(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()

	now := time.Now()
	setNow(tbl, func() time.Time { return now })

	tok := Token{Term: 1, Counter: 1}
	tbl.applyEntry(wireTomb("k", tok))
	if n := tbl.entryCount(); n != 1 {
		t.Fatalf("tombstone must be retained, have %d entries", n)
	}

	// Still retained just inside the MaxTTL bound.
	setNow(tbl, func() time.Time { return now.Add(29 * time.Second) })
	tbl.reap()
	if n := tbl.entryCount(); n != 1 {
		t.Fatalf("tombstone must be kept until release+MaxTTL, dropped to %d entries", n)
	}

	setNow(tbl, func() time.Time { return now.Add(31 * time.Second) })
	tbl.reap()
	if n := tbl.entryCount(); n != 0 {
		t.Errorf("tombstone past release+MaxTTL must be collected, %d remain", n)
	}
}

// A snapshot must carry unexpired tombstones: they are what stop resurrection
// on the node that receives the snapshot.
func TestSnapshotCarriesTombstones(t *testing.T) {
	tbl := newTable(30 * time.Second)
	defer tbl.close()
	serve(tbl, 1)

	owner := gossip.NodeID(uuid.New())
	tbl.applyEntry(wireGrant("held", Token{Term: 1, Counter: 1}, owner, 30*time.Second))
	tbl.applyEntry(wireTomb("gone", Token{Term: 1, Counter: 2}))

	snap := tbl.snapshot()
	var haveTomb, haveGrant bool
	for _, w := range snap {
		switch w.Key {
		case "held":
			haveGrant = w.Held
		case "gone":
			haveTomb = !w.Held
		}
	}
	if !haveGrant || !haveTomb {
		t.Errorf("snapshot must carry live grants and tombstones: grant=%v tomb=%v", haveGrant, haveTomb)
	}
}

// --- Pool with fake leadership ---

func TestPoolLeaderServesLocally(t *testing.T) {
	p, _, c := newLeaderPool(t, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer c.Stop()
	defer p.Close()

	lk, err := p.TryAcquire("k", 5*time.Second)
	if err != nil {
		t.Fatalf("leader should grant locally: %v", err)
	}
	if lk.Key() != "k" {
		t.Errorf("unexpected key %q", lk.Key())
	}
	if lk.Token().IsZero() {
		t.Error("a granted lock must carry a token")
	}

	held, owner, tok, remaining, err := p.Query("k")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if !held || owner != c.LocalNode().ID || !tok.Equal(lk.Token()) || remaining <= 0 {
		t.Errorf("query disagrees: held=%v owner=%v tok=%s remaining=%v", held, owner, tok, remaining)
	}

	if err := lk.Release(); err != nil {
		t.Fatalf("release failed: %v", err)
	}
}

func TestPoolNoLeaderIsTransient(t *testing.T) {
	c := newTestCluster(t)
	c.Start()
	defer c.Stop()

	f := &fakeLeadership{} // nobody is leader
	p := NewPool(c, f, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer p.Close()

	_, err := p.TryAcquire("k", 5*time.Second)
	if !errors.Is(err, ErrNoLeader) {
		t.Fatalf("expected ErrNoLeader, got %v", err)
	}
	if !isRetryable(err) {
		t.Error("ErrNoLeader must be retryable so blocking Acquire waits it out")
	}
}

// Acquire should ride out a leaderless period and succeed once one appears.
func TestPoolAcquireWaitsForLeader(t *testing.T) {
	c := newTestCluster(t)
	c.Start()
	defer c.Stop()

	f := &fakeLeadership{}
	p := NewPool(c, f, &Config{
		Name: "p", MinTTL: time.Second, MaxTTL: time.Minute,
		RetryInterval: 10 * time.Millisecond,
	})
	defer p.Close()

	go func() {
		time.Sleep(200 * time.Millisecond)
		f.become(c.LocalNode().ID, 1)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lk, err := p.Acquire(ctx, "k", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire should have waited for a leader: %v", err)
	}
	lk.Release()
}

func TestPoolAcquireRespectsContext(t *testing.T) {
	c := newTestCluster(t)
	c.Start()
	defer c.Stop()

	f := &fakeLeadership{} // never becomes leader
	p := NewPool(c, f, &Config{
		Name: "p", MinTTL: time.Second, MaxTTL: time.Minute,
		RetryInterval: 10 * time.Millisecond,
	})
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := p.Acquire(ctx, "k", 5*time.Second)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the context deadline, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("Acquire overran its context by a long way: %v", elapsed)
	}
}

// A leader that has not completed recovery must refuse rather than answer from
// incomplete state; the refusal is transient.
func TestPoolRefusesWhileRecovering(t *testing.T) {
	c := newTestCluster(t)
	c.Start()
	defer c.Stop()

	f := &fakeLeadership{}
	f.become(c.LocalNode().ID, 2)

	p := NewPool(c, f, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer p.Close()

	// Recovery may already have completed (no peers, so it is instant); force
	// the unrecovered state deterministically.
	p.tbl.assumeLeadership(3)

	err := classifyDenial(reasonRecovering)
	if _, err2 := p.TryAcquire("k", 5*time.Second); !errors.Is(err2, ErrWarmingUp) {
		t.Fatalf("expected ErrWarmingUp while recovering, got %v", err2)
	}
	if !isRetryable(err) {
		t.Error("the recovering refusal must be retryable")
	}
	if _, _, _, _, err := p.Query("k"); !errors.Is(err, ErrWarmingUp) {
		t.Fatalf("Query while recovering must refuse, got %v", err)
	}

	p.tbl.openForService()
	if _, err := p.TryAcquire("k", 5*time.Second); err != nil {
		t.Fatalf("grants should resume after recovery: %v", err)
	}
}

func TestPoolLosingLeadershipStopsServingButKeepsState(t *testing.T) {
	p, f, c := newLeaderPool(t, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer c.Stop()
	defer p.Close()

	if _, err := p.TryAcquire("k", 30*time.Second); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	if p.tbl.count() != 1 {
		t.Fatalf("expected 1 lock, got %d", p.tbl.count())
	}

	f.followTo(gossip.NodeID(uuid.New()), 2)

	waitFor(t, 2*time.Second, func() bool { return !p.tbl.servingNow() })

	if p.IsLeader() {
		t.Error("pool should no longer consider itself leader")
	}
	if p.tbl.count() != 1 {
		t.Errorf("the replica view must survive standing down, have %d", p.tbl.count())
	}
}

func TestPoolTTLValidation(t *testing.T) {
	p, _, c := newLeaderPool(t, &Config{
		Name: "p", MinTTL: 2 * time.Second, MaxTTL: 30 * time.Second,
	})
	defer c.Stop()
	defer p.Close()

	if _, err := p.TryAcquire("k", 500*time.Millisecond); !errors.Is(err, ErrTTLOutOfRange) {
		t.Errorf("TTL below MinTTL should be rejected, got %v", err)
	}
	if _, err := p.TryAcquire("k", time.Hour); !errors.Is(err, ErrTTLOutOfRange) {
		t.Errorf("TTL above MaxTTL should be rejected, got %v", err)
	}
	if _, err := p.TryAcquire("k", 0); !errors.Is(err, ErrTTLOutOfRange) {
		t.Errorf("a zero TTL must be rejected — TTL is mandatory, got %v", err)
	}

	lk, err := p.TryAcquire("k", 5*time.Second)
	if err != nil {
		t.Fatalf("a valid TTL should be accepted: %v", err)
	}
	lk.Release()
}

func TestPoolExtendTTLValidation(t *testing.T) {
	p, _, c := newLeaderPool(t, &Config{
		Name: "p", MinTTL: 2 * time.Second, MaxTTL: 30 * time.Second,
	})
	defer c.Stop()
	defer p.Close()

	lk, err := p.TryAcquire("k", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	defer lk.Release()

	if err := lk.Extend(10 * time.Millisecond); !errors.Is(err, ErrTTLOutOfRange) {
		t.Errorf("extend must validate TTL too, got %v", err)
	}
	if err := lk.Extend(10 * time.Second); err != nil {
		t.Errorf("a valid extend should succeed: %v", err)
	}
}

func TestPoolClosed(t *testing.T) {
	p, _, c := newLeaderPool(t, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer c.Stop()

	p.Close()

	if _, err := p.TryAcquire("k", 5*time.Second); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}
	if _, _, _, _, err := p.Query("k"); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Query on a closed pool should fail, got %v", err)
	}
	if _, err := p.Acquire(context.Background(), "k", 5*time.Second); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Acquire on a closed pool should fail, got %v", err)
	}
}

func TestPoolCloseIsIdempotent(t *testing.T) {
	p, _, c := newLeaderPool(t, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer c.Stop()

	p.Close()
	p.Close() // must not panic or block
}

func TestLockReleaseIsIdempotent(t *testing.T) {
	p, _, c := newLeaderPool(t, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer c.Stop()
	defer p.Close()

	lk, _ := p.TryAcquire("k", 5*time.Second)

	if err := lk.Release(); err != nil {
		t.Fatalf("first release failed: %v", err)
	}
	if err := lk.Release(); err != nil {
		t.Errorf("second release should be a no-op, got %v", err)
	}
}

func TestLockExtendAfterReleaseFails(t *testing.T) {
	p, _, c := newLeaderPool(t, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer c.Stop()
	defer p.Close()

	lk, _ := p.TryAcquire("k", 5*time.Second)
	lk.Release()

	if err := lk.Extend(5 * time.Second); !errors.Is(err, ErrLockNotAcquired) {
		t.Errorf("extending a released lock should fail, got %v", err)
	}
}

func TestPoolReentrantAcquireSameNode(t *testing.T) {
	p, _, c := newLeaderPool(t, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer c.Stop()
	defer p.Close()

	// Ownership is per-node, so the same node taking the key twice succeeds and
	// gets the same token. This is documented behaviour, asserted so it cannot
	// change silently.
	a, err := p.TryAcquire("k", 5*time.Second)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	b, err := p.TryAcquire("k", 5*time.Second)
	if err != nil {
		t.Fatalf("re-entrant acquire on the same node should succeed: %v", err)
	}
	if !a.Token().Equal(b.Token()) {
		t.Errorf("re-entrant acquire should return the same token: %s vs %s", a.Token(), b.Token())
	}
}

// A pool whose W exceeds the group size must still work — the leader's own
// copy is the whole replica set when nobody else is available.
func TestPoolDegradesWhenGroupSmallerThanW(t *testing.T) {
	p, _, c := newLeaderPool(t, &Config{
		Name: "p", MinTTL: time.Second, MaxTTL: time.Minute, WriteReplicas: 5,
	})
	defer c.Stop()
	defer p.Close()

	if p.WriteReplicas() != 5 {
		t.Fatalf("W should be honoured as configured, got %d", p.WriteReplicas())
	}

	lk, err := p.TryAcquire("k", 5*time.Second)
	if err != nil {
		t.Fatalf("a group smaller than W must still serve: %v", err)
	}
	if err := lk.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
}

// The write-quorum error must be retryable so blocking Acquire rides out a
// transient replication shortfall.
func TestWriteQuorumErrorIsRetryable(t *testing.T) {
	err := fmt.Errorf("%w: need 2 replica acks, got 1", ErrWriteQuorum)
	if !isRetryable(err) {
		t.Error("ErrWriteQuorum must be retryable")
	}
	if !isRetryable(classifyDenial(reasonWriteQuorum)) {
		t.Error("the wire reason for a write-quorum failure must classify as retryable")
	}
}

// --- registry ---

func TestMultiplePoolsAreIndependent(t *testing.T) {
	c := newTestCluster(t)
	c.Start()
	defer c.Stop()

	f := &fakeLeadership{}
	f.become(c.LocalNode().ID, 1)

	p1 := NewPool(c, f, &Config{Name: "alpha", MinTTL: time.Second, MaxTTL: time.Minute})
	defer p1.Close()
	p2 := NewPool(c, f, &Config{Name: "beta", MinTTL: time.Second, MaxTTL: time.Minute})
	defer p2.Close()

	waitFor(t, 2*time.Second, func() bool {
		return p1.tbl.servingNow() && p2.tbl.servingNow()
	})

	if _, err := p1.TryAcquire("shared", 5*time.Second); err != nil {
		t.Fatalf("alpha acquire failed: %v", err)
	}
	if _, err := p2.TryAcquire("shared", 5*time.Second); err != nil {
		t.Fatalf("beta should hold the same key independently: %v", err)
	}

	if held, _, _, _, _ := p2.Query("shared"); !held {
		t.Error("beta should see its own lock")
	}
}

func TestDuplicatePoolNamePanics(t *testing.T) {
	c := newTestCluster(t)
	c.Start()
	defer c.Stop()

	f := &fakeLeadership{}
	p := NewPool(c, f, &Config{Name: "dup"})
	defer p.Close()

	defer func() {
		if recover() == nil {
			t.Error("registering a duplicate pool name should panic")
		}
	}()
	_ = NewPool(c, f, &Config{Name: "dup"})
}

func TestRegistryTearsDownAndRebuilds(t *testing.T) {
	c := newTestCluster(t)
	c.Start()
	defer c.Stop()

	f := &fakeLeadership{}
	f.become(c.LocalNode().ID, 1)

	a := NewPool(c, f, &Config{Name: "one"})
	b := NewPool(c, f, &Config{Name: "two"})
	a.Close()
	b.Close()

	// With the registry gone, a fresh pool must re-register cleanly.
	fresh := NewPool(c, f, &Config{Name: "three", MinTTL: time.Second, MaxTTL: time.Minute})
	defer fresh.Close()

	waitFor(t, 2*time.Second, func() bool { return fresh.tbl.servingNow() })
	if _, err := fresh.TryAcquire("k", 5*time.Second); err != nil {
		t.Fatalf("a pool created after teardown should work: %v", err)
	}
}

func TestNewPoolRejectsNilArguments(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	func() {
		defer func() {
			if recover() == nil {
				t.Error("nil leadership should panic")
			}
		}()
		_ = NewPool(c, nil, nil)
	}()

	func() {
		defer func() {
			if recover() == nil {
				t.Error("nil cluster should panic")
			}
		}()
		_ = NewPool(nil, &fakeLeadership{}, nil)
	}()
}

// A pool whose leadership never materialises must never issue a token, and in
// particular never a term-0 one.
func TestPoolNeverIssuesZeroTermToken(t *testing.T) {
	c := newTestCluster(t)
	c.Start()
	defer c.Stop()

	f := &fakeLeadership{}
	f.become(c.LocalNode().ID, 7)

	p := NewPool(c, f, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer p.Close()

	waitFor(t, 2*time.Second, func() bool { return p.tbl.servingNow() })

	lk, err := p.TryAcquire("k", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lk.Release()

	if lk.Token().Term == 0 {
		t.Errorf("token %s carries term 0; it must reflect the election term", lk.Token())
	}
	if lk.Token().Term != 7 {
		t.Errorf("expected term 7, got token %s", lk.Token())
	}
}

// A new term on the same node must re-run recovery before serving again:
// between the terms, another leader may have granted.
func TestPoolNewTermReRecovers(t *testing.T) {
	p, f, c := newLeaderPool(t, &Config{Name: "p", MinTTL: time.Second, MaxTTL: time.Minute})
	defer c.Stop()
	defer p.Close()

	if _, err := p.TryAcquire("k", 30*time.Second); err != nil {
		t.Fatalf("acquire at term 1: %v", err)
	}

	// Simulate a peer's grant from term 2 landing in our replica view.
	other := gossip.NodeID(uuid.New())
	p.tbl.applyEntry(wireGrant("k2", Token{Term: 2, Counter: 1}, other, 30*time.Second))

	takeOffice(t, p, f, c, 3)

	held, owner, _, _, err := p.Query("k2")
	if err != nil || !held || owner != other {
		t.Errorf("recovered state must be served: held=%v owner=%v err=%v", held, owner, err)
	}

	// And the old lock from term 1 must have survived too.
	if held, _, _, _, _ := p.Query("k"); !held {
		t.Error("our own term-1 grant must survive re-election")
	}
}
