package kv

import (
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/hlc"
)

// newTestTable builds a table with a controllable clock and injectable time.
func newTestTable(now time.Time) (*table, *hlc.Clock, *time.Time) {
	cfg := DefaultConfig()
	cfg.Clock = hlc.NewClock()
	tNow := now
	cfg.NowFn = func() time.Time { return tNow }
	return newTable(cfg, gossip.NodeID{0x01}), cfg.Clock, &tNow
}

func nodeID(byte byte) gossip.NodeID {
	return gossip.NodeID{byte}
}

func TestTableSetGetOverwriteDelete(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())

	e := tbl.setLocal("k", []byte("v1"), 0)
	if e == nil {
		t.Fatal("setLocal returned nil")
	}
	if v, ok := tbl.get("k"); !ok || string(v) != "v1" {
		t.Fatalf("get = %q,%v want v1,true", v, ok)
	}

	tbl.setLocal("k", []byte("v2"), 0)
	if v, _ := tbl.get("k"); string(v) != "v2" {
		t.Fatalf("overwrite failed: %q", v)
	}

	tbl.deleteLocal("k")
	if _, ok := tbl.get("k"); ok {
		t.Fatal("deleted key still readable")
	}
}

func TestTableLocalWritesDominate(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())

	tbl.setLocal("k", []byte("v1"), 0)
	tbl.setLocal("k", []byte("v2"), 0)

	// A local write must always beat what the table held: the table
	// witnesses the current version before minting.
	e, ok := tbl.entries["k"]
	if !ok || string(e.Value) != "v2" {
		t.Fatalf("local overwrite lost: %+v", e)
	}
}

func TestTableTTLLazyExpiry(t *testing.T) {
	start := time.Now()
	tbl, _, now := newTestTable(start)

	tbl.setLocal("k", []byte("v"), 500*time.Millisecond)
	if _, ok := tbl.get("k"); !ok {
		t.Fatal("entry missing before expiry")
	}

	*now = start.Add(600 * time.Millisecond)
	if _, ok := tbl.get("k"); ok {
		t.Fatal("entry readable past its TTL")
	}
	// Lazy expiry: the entry is still present for gossip until reaped.
	if tbl.entryCount() != 1 {
		t.Fatalf("lazy expiry removed the entry early: %d", tbl.entryCount())
	}
}

func TestTableApplyVersionOrdering(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())

	older := &Entry{Key: "k", Version: 100, Origin: nodeID(1), Value: []byte("old")}
	newer := &Entry{Key: "k", Version: 200, Origin: nodeID(1), Value: []byte("new")}

	if !tbl.apply(older) {
		t.Fatal("first apply was rejected")
	}
	if !tbl.apply(newer) {
		t.Fatal("newer write was dominated")
	}
	if tbl.apply(&Entry{Key: "k", Version: 150, Origin: nodeID(2), Value: []byte("stale")}) {
		t.Fatal("stale write was adopted over newer version")
	}
	if v, _ := tbl.get("k"); string(v) != "new" {
		t.Fatalf("value = %q want new", v)
	}
}

func TestTableApplyTieTombstoneWins(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())

	value := &Entry{Key: "k", Version: 100, Origin: nodeID(1), Value: []byte("v")}
	tomb := &Entry{Key: "k", Version: 100, Origin: nodeID(1), Tombstone: true, DeletedAtMs: 1}

	tbl.apply(value)
	if !tbl.apply(tomb) {
		t.Fatal("tie-breaking tombstone was rejected")
	}
	if _, ok := tbl.get("k"); ok {
		t.Fatal("tombstone did not kill the value")
	}

	// An equal-version value arriving afterwards must not resurrect.
	if tbl.apply(value) {
		t.Fatal("equal-version value resurrected over tombstone")
	}
}

func TestTableApplyTieOriginBreaks(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())

	low := &Entry{Key: "k", Version: 100, Origin: nodeID(1), Value: []byte("low")}
	high := &Entry{Key: "k", Version: 100, Origin: nodeID(2), Value: []byte("high")}

	// Apply in both orders: the outcome must be identical — the higher
	// origin wins regardless of arrival order.
	tbl.apply(low)
	if !tbl.apply(high) {
		t.Fatal("higher-origin tie-break lost")
	}
	if v, _ := tbl.get("k"); string(v) != "high" {
		t.Fatalf("winner = %q want high", v)
	}

	tbl2, _, _ := newTestTable(time.Now())
	tbl2.apply(high)
	if tbl2.apply(low) {
		t.Fatal("lower-origin write displaced the winner")
	}
	if v, _ := tbl2.get("k"); string(v) != "high" {
		t.Fatalf("reverse-order winner = %q want high", v)
	}
}

func TestTableApplyIdempotentRedelivery(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())

	e := &Entry{Key: "k", Version: 100, Origin: nodeID(1), Value: []byte("v")}
	tbl.apply(e)
	// The same write redelivered is accepted (no-op) and does not change state.
	tbl.apply(e)
	if tbl.entryCount() != 1 {
		t.Fatalf("redelivery duplicated the entry: %d", tbl.entryCount())
	}
	if v, _ := tbl.get("k"); string(v) != "v" {
		t.Fatalf("value changed on redelivery: %q", v)
	}
}

func TestTableApplyEmptyKeyRejected(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())
	if tbl.apply(&Entry{Key: "", Version: 1}) {
		t.Fatal("empty key accepted")
	}
}

func TestTableStaleValueCannotResurrectAfterTombstone(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())

	// The classic resurrection scenario: a replica that never saw the delete
	// offers the old value during a later full sync.
	tbl.apply(&Entry{Key: "k", Version: 300, Origin: nodeID(1), Value: []byte("v")})
	tbl.apply(&Entry{Key: "k", Version: 400, Origin: nodeID(2), Tombstone: true, DeletedAtMs: 2})
	if tbl.apply(&Entry{Key: "k", Version: 300, Origin: nodeID(1), Value: []byte("v")}) {
		t.Fatal("stale value resurrected over tombstone")
	}
	if _, ok := tbl.get("k"); ok {
		t.Fatal("key readable after resurrection attempt")
	}
}

func TestTableReap(t *testing.T) {
	start := time.Now()
	tbl, _, now := newTestTable(start)
	tbl.cfg.TombstoneRetention = 2 * time.Minute

	tbl.setLocal("expiring", []byte("v"), time.Minute) // expires at start+1m
	tbl.setLocal("forever", []byte("v"), 0)            // never expires
	tomb := tbl.deleteLocal("gone")                    // deleted at start
	tomb.DeletedAtMs = start.UnixMilli()               // pin for arithmetic below
	tbl.entries["gone"] = tomb

	// Nothing reapable at start + grace.
	*now = start.Add(gcGrace / 2)
	tbl.reap()
	if got := tbl.entryCount(); got != 3 {
		t.Fatalf("premature reap: %d entries remain, want 3", got)
	}

	// Past expiry + grace the entry is reaped; the tombstone — retention 2m — stays.
	*now = start.Add(time.Minute + gcGrace + time.Second)
	tbl.reap()
	if _, ok := tbl.entries["expiring"]; ok {
		t.Fatal("expired entry survived reap")
	}
	if _, ok := tbl.entries["gone"]; !ok {
		t.Fatal("tombstone reaped before its retention elapsed")
	}
	if _, ok := tbl.entries["forever"]; !ok {
		t.Fatal("no-expiry entry reaped")
	}

	// Past retention the tombstone goes too.
	*now = start.Add(3 * time.Minute)
	tbl.reap()
	if _, ok := tbl.entries["gone"]; ok {
		t.Fatal("tombstone survived past retention")
	}
}

func TestTableSnapshotExcludesReapable(t *testing.T) {
	start := time.Now()
	tbl, _, now := newTestTable(start)

	tbl.setLocal("live", []byte("v"), 0)
	tomb := tbl.deleteLocal("gone")
	tomb.DeletedAtMs = start.Add(-2 * tbl.cfg.TombstoneRetention).UnixMilli()
	tbl.entries["gone"] = tomb
	expired := tbl.setLocal("old", []byte("v"), time.Second)
	expired.ExpiresAtMs = start.Add(-time.Hour).UnixMilli()
	tbl.entries["old"] = expired

	_ = now
	snap := tbl.snapshot()
	got := map[string]bool{}
	for _, e := range snap {
		got[e.Key] = true
	}
	if !got["live"] {
		t.Fatal("live entry missing from snapshot")
	}
	if got["gone"] || got["old"] {
		t.Fatal("reapable entries included in snapshot")
	}
}

func TestTableKeysPrefixSortedLiveOnly(t *testing.T) {
	start := time.Now()
	tbl, _, now := newTestTable(start)

	tbl.setLocal("a/1", []byte("v"), 0)
	tbl.setLocal("a/2", []byte("v"), 0)
	tbl.setLocal("b/1", []byte("v"), 0)
	tbl.setLocal("gone", []byte("v"), 0)
	tbl.deleteLocal("gone")
	tbl.setLocal("expired", []byte("v"), time.Second)
	*now = start.Add(time.Hour)

	if got := tbl.keys("a/"); len(got) != 2 || got[0] != "a/1" || got[1] != "a/2" {
		t.Fatalf("prefix keys = %v", got)
	}
	if got := tbl.keys(""); len(got) != 3 {
		t.Fatalf("all keys = %v want a/1 a/2 b/1", got)
	}
}

func TestTableDeletePrefixLocalOnlyLive(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())

	tbl.setLocal("a/1", []byte("v"), 0)
	tbl.setLocal("a/2", []byte("v"), 0)
	tbl.setLocal("b/1", []byte("v"), 0)
	tbl.deleteLocal("a/2") // already deleted: must not produce a second tombstone batch entry

	tombs := tbl.deletePrefixLocal("a/")
	if len(tombs) != 1 || tombs[0].Key != "a/1" {
		t.Fatalf("deletePrefixLocal = %v want exactly [a/1]", tombs)
	}
	if _, ok := tbl.get("b/1"); !ok {
		t.Fatal("prefix delete escaped its prefix")
	}
}

func TestTableWitnessMakesLocalWin(t *testing.T) {
	tbl, clock, _ := newTestTable(time.Now())

	// A remote write arrives from a node with a far-ahead clock.
	remoteVersion := uint64(clock.Now()) + (1 << 24)
	tbl.apply(&Entry{Key: "k", Version: remoteVersion, Origin: nodeID(9), Value: []byte("remote")})

	// The local write that follows must dominate: the table witnessed the
	// remote version, so its mint is strictly greater.
	e := tbl.setLocal("k", []byte("local"), 0)
	if e.Version <= remoteVersion {
		t.Fatalf("local mint %d did not beat witnessed %d", e.Version, remoteVersion)
	}
	if v, _ := tbl.get("k"); string(v) != "local" {
		t.Fatalf("value = %q want local", v)
	}
}

func TestTableCloseStopsWrites(t *testing.T) {
	tbl, _, _ := newTestTable(time.Now())
	tbl.close()

	if tbl.setLocal("k", []byte("v"), 0) != nil {
		t.Fatal("setLocal accepted after close")
	}
	if tbl.deleteLocal("k") != nil {
		t.Fatal("deleteLocal accepted after close")
	}
	if tbl.apply(&Entry{Key: "k", Version: 1}) {
		t.Fatal("apply accepted after close")
	}
}
