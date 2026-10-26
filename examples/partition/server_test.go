package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paularlott/gossip/partition"
)

// freeAddr returns a localhost address whose TCP and UDP ports are both free.
func freeAddr(t *testing.T) string {
	t.Helper()
	for range 100 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		udp, err := net.ListenPacket("udp", addr)
		ln.Close()
		if err != nil {
			continue
		}
		udp.Close()
		return addr
	}
	t.Fatal("no free port")
	return ""
}

// testNode is one node of the in-process cluster: its fixed configuration, and the running server (nil while killed).
type testNode struct {
	cfg serverConfig
	srv *server
}

func (n *testNode) start(t *testing.T, newCluster, resync bool) {
	t.Helper()
	cfg := n.cfg
	cfg.NewCluster, cfg.Resync = newCluster, resync
	srv, err := newServer(cfg)
	if err != nil {
		t.Fatalf("starting %s: %v", cfg.NodeID, err)
	}
	n.srv = srv
	n.cfg.HTTPAddr = srv.ln.Addr().String() // the same address after a restart
	t.Cleanup(srv.Close)
}

func (n *testNode) kill() {
	n.srv.Close()
	n.srv = nil
}

func (n *testNode) url(path string) string { return "http://" + n.cfg.HTTPAddr + path }

// do makes one HTTP request and returns the status and body; a transport error is a 0 status.
func (n *testNode) do(method, path, body string, hdr ...string) (int, string) {
	req, err := http.NewRequest(method, n.url(path), strings.NewReader(body))
	if err != nil {
		return 0, err.Error()
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// put retries while the cluster is between owners (503), which writes nothing; any other failure is fatal.
func (n *testNode) put(t *testing.T, key, val string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, body := n.do("PUT", "/kv/"+key, val)
		switch {
		case code == http.StatusNoContent:
			return
		case code == http.StatusServiceUnavailable && time.Now().Before(deadline):
			time.Sleep(50 * time.Millisecond)
		default:
			t.Fatalf("PUT %s via %s: %d %s", key, n.cfg.NodeID, code, body)
		}
	}
}

// etag makes a request that must succeed and returns the ETag header of its answer.
func (n *testNode) etag(t *testing.T, method, path, body string) string {
	t.Helper()
	req, _ := http.NewRequest(method, n.url(path), strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("%s %s: %d", method, path, resp.StatusCode)
	}
	return resp.Header.Get("ETag")
}

func eventually(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// expectValue waits until a GET of key on n returns want (reads are local, so a node other than the owner can be a
// heartbeat behind), or 404 if want is empty.
func (n *testNode) expectValue(t *testing.T, key, want string) {
	t.Helper()
	var code int
	var body string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		code, body = n.do("GET", "/kv/"+key, "")
		if (want == "" && code == http.StatusNotFound) || (want != "" && code == http.StatusOK && body == want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s never held %s=%q (last answer: %d %q)", n.cfg.NodeID, key, want, code, body)
}

func (n *testNode) status(t *testing.T) statusResp {
	t.Helper()
	code, body := n.do("GET", "/status", "")
	var st statusResp
	if code != http.StatusOK || json.Unmarshal([]byte(body), &st) != nil {
		t.Fatalf("status of %s: %d %s", n.cfg.NodeID, code, body)
	}
	return st
}

// TestThreeNodesKillRestartWipe runs three servers in one process: it writes through every node, kills one and keeps
// writing, restarts it from its data directory and checks it converged, then wipes another node's data and restarts it
// to see it resync, and checks everything through GET.
func TestThreeNodesKillRestartWipe(t *testing.T) {
	const parts = 8
	root := t.TempDir()
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	nodes := make([]*testNode, 3)
	for i := range nodes {
		nodes[i] = &testNode{cfg: serverConfig{
			NodeID:      fmt.Sprintf("00000000-0000-4000-8000-00000000000%d", i+1),
			Bind:        addrs[i],
			HTTPAddr:    "127.0.0.1:0",
			Peers:       addrs,
			ClusterSize: 3,
			Partitions:  parts,
			DataDir:     filepath.Join(root, fmt.Sprintf("node%d", i+1)),
			Heartbeat:   100 * time.Millisecond,
		}}
	}
	for _, n := range nodes {
		n.start(t, true, false)
	}
	eventually(t, 30*time.Second, "every node ready and every partition owned", func() bool {
		owned := 0
		for _, n := range nodes {
			st := n.status(t)
			if !st.Ready {
				return false
			}
			owned += len(st.Owned)
		}
		return owned == parts
	})

	// Writes through every node land on the owners, wherever the partitions are.
	want := map[string]string{}
	for i := 0; i < 30; i++ {
		k, v := fmt.Sprintf("key%d", i), fmt.Sprintf("v%d-via-%d", i, i%3)
		nodes[i%3].put(t, k, v)
		want[k] = v
	}
	check := func(live ...*testNode) {
		t.Helper()
		for k, v := range want {
			for _, n := range live {
				n.expectValue(t, k, v)
			}
		}
	}
	check(nodes...)

	// A write returns the key's new version as its ETag, and the node that took the write has applied it when it answers.
	for i := 0; i < 9; i++ {
		n, key := nodes[i%3], fmt.Sprintf("etag%d", i)
		code, _ := n.do("PUT", "/kv/"+key, "one")
		for code == http.StatusServiceUnavailable { // between owners: nothing was written
			time.Sleep(50 * time.Millisecond)
			code, _ = n.do("PUT", "/kv/"+key, "one")
		}
		if code != http.StatusNoContent {
			t.Fatalf("PUT %s: %d", key, code)
		}
		want[key] = "one"
		if tag := n.etag(t, "PUT", "/kv/"+key, "two"); tag != `"2"` {
			t.Fatalf("PUT %s: ETag %s, want the new version \"2\"", key, tag)
		}
		want[key] = "two"
		resp, err := http.Get(n.url("/kv/" + key))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != "two" || resp.Header.Get("ETag") != `"2"` {
			t.Fatalf("GET of %s on the node that took the PUT, right after it: %d %q ETag %s", key, resp.StatusCode, body, resp.Header.Get("ETag"))
		}
	}

	// A compare-and-swap with a stale version is refused, with the current one it goes through.
	if code, body := nodes[0].do("GET", "/kv/cas", ""); code != http.StatusNotFound {
		t.Fatalf("GET of a missing key: %d %s", code, body)
	}
	if code, body := nodes[1].do("PUT", "/kv/cas", "one", "If-None-Match", "*"); code != http.StatusNoContent {
		t.Fatalf("create-only PUT: %d %s", code, body)
	}
	if code, _ := nodes[2].do("PUT", "/kv/cas", "again", "If-None-Match", "*"); code != http.StatusPreconditionFailed {
		t.Fatalf("second create-only PUT: %d, want 412", code)
	}
	if code, _ := nodes[2].do("PUT", "/kv/cas", "two", "If-Match", `"7"`); code != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match: %d, want 412", code)
	}
	if code, body := nodes[2].do("PUT", "/kv/cas", "two", "If-Match", `"1"`); code != http.StatusNoContent {
		t.Fatalf("If-Match with the current version: %d %s", code, body)
	}
	want["cas"] = "two"
	if code, body := nodes[0].do("DELETE", "/kv/key0", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE: %d %s", code, body)
	}
	if code, _ := nodes[0].do("DELETE", "/kv/key0", ""); code != http.StatusNotFound {
		t.Fatalf("DELETE of a deleted key: %d, want 404", code)
	}
	delete(want, "key0")
	check(nodes...)
	for _, n := range nodes {
		n.expectValue(t, "key0", "")
	}

	// A consistent read (GET ?consistent=1) is current on every node at once: after a PUT through one node, a read through
	// another sees it with no waiting, which a plain GET cannot promise. A missing key is a 404 either way.
	consistent := func(n *testNode, key string) (code int, body, etag string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			resp, err := http.Get(n.url("/kv/" + key + "?consistent=1"))
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			// 503 is "between owners, nothing was served": ask again.
			if resp.StatusCode != http.StatusServiceUnavailable || time.Now().After(deadline) {
				return resp.StatusCode, string(b), resp.Header.Get("ETag")
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	for i := 0; i < 12; i++ {
		writer, reader := nodes[i%3], nodes[(i+1)%3]
		val := fmt.Sprintf("v%d", i)
		writer.put(t, "cons", val)
		if code, body, tag := consistent(reader, "cons"); code != http.StatusOK || body != val || tag != fmt.Sprintf(`"%d"`, i+1) {
			t.Fatalf("consistent GET via %s right after a PUT of %q via %s: %d %q ETag %s", reader.cfg.NodeID, val, writer.cfg.NodeID, code, body, tag)
		}
	}
	want["cons"] = "v11"
	for _, n := range nodes {
		if code, body, _ := consistent(n, "no-such-key"); code != http.StatusNotFound {
			t.Fatalf("consistent GET of a missing key via %s: %d %s", n.cfg.NodeID, code, body)
		}
	}

	// Kill one node: the other two are a majority of three. Its partitions move to them, and writes continue.
	nodes[2].kill()
	for i := 30; i < 60; i++ {
		k, v := fmt.Sprintf("key%d", i), fmt.Sprintf("v%d-while-down", i)
		nodes[i%2].put(t, k, v)
		want[k] = v
	}
	nodes[0].put(t, "key1", "rewritten-while-down")
	want["key1"] = "rewritten-while-down"
	check(nodes[0], nodes[1])
	// A consistent read still works with a majority up, after the partitions of the killed node moved.
	if code, body, _ := consistent(nodes[1], "key1"); code != http.StatusOK || body != "rewritten-while-down" {
		t.Fatalf("consistent GET with one node down: %d %q", code, body)
	}

	// Restart it from its data directory: no flags, the directory is trusted. It catches up and is ready again.
	nodes[2].start(t, false, false)
	if !nodes[2].srv.store.Intact() {
		t.Fatal("a restarted node with its data directory must be intact")
	}
	eventually(t, 30*time.Second, "the restarted node to be ready", func() bool { return nodes[2].status(t).Ready })
	check(nodes...)

	// Wipe another node's data and restart it: it is not intact, so it resyncs from the other two before taking part.
	nodes[1].kill()
	if err := os.RemoveAll(nodes[1].cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	nodes[0].put(t, "key2", "rewritten-while-wiped")
	want["key2"] = "rewritten-while-wiped"
	nodes[1].start(t, false, false)
	if nodes[1].srv.store.Intact() {
		t.Fatal("a node with an empty data directory that was not told it starts a new cluster must not be intact")
	}
	eventually(t, 30*time.Second, "the wiped node to resync and be ready", func() bool { return nodes[1].status(t).Ready })
	check(nodes...)
	if _, err := os.Stat(filepath.Join(nodes[1].cfg.DataDir, "intact")); err != nil {
		// MarkIntact runs a moment after the node is ready.
		eventually(t, 5*time.Second, "the intact marker", func() bool {
			_, err := os.Stat(filepath.Join(nodes[1].cfg.DataDir, "intact"))
			return err == nil
		})
	}

	// And one more write through each node, with everything back.
	for i, n := range nodes {
		k, v := fmt.Sprintf("final%d", i), fmt.Sprintf("last-via-%d", i)
		n.put(t, k, v)
		want[k] = v
	}
	check(nodes...)
}

func commit(t *testing.T, s *fileStore, p int, seq uint64, prev partition.Pos, c change) partition.Pos {
	t.Helper()
	raw, _ := json.Marshal(c)
	data, _, err := s.Prepare(p, raw)
	if err != nil {
		t.Fatalf("Prepare %+v: %v", c, err)
	}
	pos := partition.Pos{Epoch: partition.Epoch{Counter: 1}, Seq: seq}
	if err := s.Commit(&partition.Write{Partition: p, Pos: pos, Prev: prev, Data: data}); err != nil {
		t.Fatal(err)
	}
	return pos
}

// TestFileStoreRead covers Store.Read: a live key answers with its value and version, a missing or deleted key is a
// refusal with the code not-found, and a pending write is not seen.
func TestFileStoreRead(t *testing.T) {
	s, err := openFileStore(t.TempDir(), 2, true, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Sync() })
	read := func(key string) (readAnswer, error) {
		raw, err := s.Read(1, []byte(key))
		var ans readAnswer
		if err == nil {
			if jerr := json.Unmarshal(raw, &ans); jerr != nil {
				t.Fatal(jerr)
			}
		}
		return ans, err
	}
	var refused *partition.RefusedError
	if _, err := read("k"); !errors.As(err, &refused) || refused.Code != refusalNotFound {
		t.Fatalf("read of a missing key: %v", err)
	}
	pos := commit(t, s, 1, 1, partition.Pos{}, change{Op: "set", Key: "k", Val: []byte("v1")})
	if ans, err := read("k"); err != nil || string(ans.Val) != "v1" || ans.Ver != 1 {
		t.Fatalf("read after a set: %+v, %v", ans, err)
	}
	// A pending write is not part of the committed data.
	raw, _ := json.Marshal(change{Op: "set", Key: "k", Val: []byte("v2")})
	data, _, err := s.Prepare(1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetPending(1, &partition.Write{Partition: 1, Pos: partition.Pos{Epoch: pos.Epoch, Seq: 2}, Prev: pos, Data: data}); err != nil {
		t.Fatal(err)
	}
	if ans, err := read("k"); err != nil || string(ans.Val) != "v1" {
		t.Fatalf("read with a pending write: %+v, %v", ans, err)
	}
	commit(t, s, 1, 3, pos, change{Op: "delete", Key: "k"})
	if _, err := read("k"); !errors.As(err, &refused) || refused.Code != refusalNotFound {
		t.Fatalf("read of a deleted key: %v", err)
	}
}

// TestFileStore covers the store without a cluster: what survives a reopen, export and import by sequence, a full
// import replacing the data, the lazily cleared pending write, leftovers of a crash, and the trust rules.
func TestFileStore(t *testing.T) {
	dir := t.TempDir()
	s, err := openFileStore(dir, 2, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Intact() {
		t.Fatal("a new cluster's first node must be intact")
	}
	e := partition.Epoch{Counter: 3}
	if err := s.SetPromised(1, e); err != nil {
		t.Fatal(err)
	}
	p1 := commit(t, s, 1, 1, partition.Pos{}, change{Op: "set", Key: "a", Val: []byte("1")})
	p2 := commit(t, s, 1, 2, p1, change{Op: "set", Key: "b", Val: []byte("2")})
	p3 := commit(t, s, 1, 3, p2, change{Op: "delete", Key: "a"})
	pending := &partition.Write{Partition: 1, Pos: partition.Pos{Epoch: e, Seq: 4}, Prev: p3, Data: []byte("[]")}
	if err := s.SetPending(1, pending); err != nil {
		t.Fatal(err)
	}

	// Refusals: a version that moved, a key that is not there.
	var refused *partition.RefusedError
	if _, _, err := s.Prepare(1, []byte(`{"op":"set","key":"b","val":"Mw==","expect":9}`)); !errors.Is(err, partition.ErrRefused) ||
		!errors.As(err, &refused) || refused.Code != refusalConflict {
		t.Fatalf("stale expect: %v", err)
	}
	if _, _, err := s.Prepare(1, []byte(`{"op":"delete","key":"a"}`)); !errors.Is(err, partition.ErrRefused) ||
		!errors.As(err, &refused) || refused.Code != refusalNotFound {
		t.Fatalf("delete of a deleted key: %v", err)
	}
	// A deleted key counts as absent for If-None-Match, and its version goes on; the reply is the new version (a tombstone
	// was version 2 of a, so this is 3).
	if _, reply, err := s.Prepare(1, []byte(`{"op":"set","key":"a","val":"Mw==","expect":0}`)); err != nil || string(reply) != "3" {
		t.Fatalf("recreate: reply %q, %v", reply, err)
	}

	// A crash left a temporary file behind; reopening ignores it and the state is as it was.
	if err := os.WriteFile(filepath.Join(dir, "p01", "data.json.123.tmp"), []byte("{torn"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err = openFileStore(dir, 2, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Intact() {
		t.Fatal("a directory with the marker must be intact")
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "p01", "*.tmp")); len(m) != 0 {
		t.Fatalf("temporary files left: %v", m)
	}
	st, _ := s.State(1)
	if st.Promised != e || st.Committed != p3 || st.Pending == nil || st.Pending.Pos.Seq != 4 {
		t.Fatalf("state after reopen: %+v", st)
	}
	if _, _, ok := s.get(1, "a"); ok {
		t.Fatal("deleted key is visible")
	}
	if v, ver, ok := s.get(1, "b"); !ok || string(v) != "2" || ver != 1 {
		t.Fatalf("b = %q v%d %v", v, ver, ok)
	}

	// A pending write that the committed position reached is hidden without anything clearing it.
	p4 := commit(t, s, 1, 4, p3, change{Op: "set", Key: "c", Val: []byte("4")})
	if st, _ := s.State(1); st.Pending != nil || st.Committed != p4 {
		t.Fatalf("pending after the commit it was for: %+v", st)
	}

	// Export what changed after 2: b is older than that, a's tombstone and c are not.
	pos, rc, err := s.Export(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(rc)
	var ex exportBody
	if err := json.Unmarshal(body, &ex); err != nil {
		t.Fatal(err)
	}
	if pos != p4 || len(ex.Records) != 2 || !ex.Records["a"].Del || string(ex.Records["c"].Val) != "4" {
		t.Fatalf("export after 2: %v %s", pos, body)
	}

	// Import it into a node that has writes 1 and 2 (a truncated stream applies nothing; a full import replaces).
	o, err := openFileStore(t.TempDir(), 2, true, false)
	if err != nil {
		t.Fatal(err)
	}
	q1 := commit(t, o, 1, 1, partition.Pos{}, change{Op: "set", Key: "a", Val: []byte("1")})
	commit(t, o, 1, 2, q1, change{Op: "set", Key: "b", Val: []byte("2")})
	if err := o.Import(1, bytes.NewReader(body[:len(body)/2]), p4, false); err == nil {
		t.Fatal("a truncated import was applied")
	}
	if st, _ := o.State(1); st.Committed.Seq != 2 {
		t.Fatalf("committed moved by a failed import: %+v", st)
	}
	if err := o.Import(1, bytes.NewReader(body), p4, false); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := o.get(1, "a"); ok {
		t.Fatal("tombstone not applied by the import")
	}
	if v, _, ok := o.get(1, "c"); !ok || string(v) != "4" {
		t.Fatal("c missing after the import")
	}
	_, rc, _ = s.Export(1, 0)
	full, _ := io.ReadAll(rc)
	empty, _ := openFileStore(t.TempDir(), 2, true, false)
	commit(t, empty, 1, 1, partition.Pos{}, change{Op: "set", Key: "stale", Val: []byte("x")})
	if err := empty.Import(1, bytes.NewReader(full), p4, true); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := empty.get(1, "stale"); ok {
		t.Fatal("a full import must replace the data")
	}
	if err := empty.Import(1, bytes.NewReader(full), p4, true); err == nil {
		t.Fatal("an import that does not move committed forward was applied")
	}

	// Trust: no marker and no -new-cluster is not intact; -resync withdraws the marker until MarkIntact.
	fresh, _ := openFileStore(t.TempDir(), 1, false, false)
	if fresh.Intact() {
		t.Fatal("an empty directory on a node that was not told it starts a cluster must not be intact")
	}
	rs, _ := openFileStore(dir, 2, false, true)
	if rs.Intact() {
		t.Fatal("-resync must make the store not intact")
	}
	if again, _ := openFileStore(dir, 2, false, false); again.Intact() {
		t.Fatal("a crash before the resync finished must leave the node needing one")
	}
	if err := rs.MarkIntact(); err != nil {
		t.Fatal(err)
	}
	if again, _ := openFileStore(dir, 2, false, false); !again.Intact() {
		t.Fatal("after MarkIntact the directory is trusted again")
	}
}

// TestFileStoreFlushesCommitsBeforeStateAndOnSync covers the contract that Commit need not be durable: a reopen (a crash)
// right after a Commit may lose it, but never after a SetPending or SetPromised, and never after Sync.
func TestFileStoreFlushesCommitsBeforeStateAndOnSync(t *testing.T) {
	dir := t.TempDir()
	s, err := openFileStore(dir, 1, true, false)
	if err != nil {
		t.Fatal(err)
	}
	p1 := commit(t, s, 0, 1, partition.Pos{}, change{Op: "set", Key: "a", Val: []byte("1")})
	// A durable SetPending makes the commit before it durable: a crash that keeps one keeps the other.
	e := partition.Epoch{Counter: 2}
	if err := s.SetPending(0, &partition.Write{Partition: 0, Pos: partition.Pos{Epoch: e, Seq: 2}, Prev: p1, Data: []byte("[]")}); err != nil {
		t.Fatal(err)
	}
	crashed, err := openFileStore(dir, 1, false, false) // what a crash leaves on disk
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := crashed.State(0); st.Committed != p1 || st.Pending == nil || st.Pending.Pos.Seq != 2 {
		t.Fatalf("a pending write on disk without the commit before it: %+v", st)
	}
	// Sync (a graceful stop) makes a commit durable by itself; so does SetPromised.
	p2 := commit(t, s, 0, 2, p1, change{Op: "set", Key: "b", Val: []byte("2")})
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if reopened, _ := openFileStore(dir, 1, false, false); func() bool { st, _ := reopened.State(0); return st.Committed != p2 }() {
		t.Fatal("a commit was lost by Sync")
	}
	p3 := commit(t, s, 0, 3, p2, change{Op: "set", Key: "c", Val: []byte("3")})
	if err := s.SetPromised(0, partition.Epoch{Counter: 5}); err != nil {
		t.Fatal(err)
	}
	if reopened, _ := openFileStore(dir, 1, false, false); func() bool { st, _ := reopened.State(0); return st.Committed != p3 }() {
		t.Fatal("a commit was lost by SetPromised")
	}
	// Left alone, the background flush gets there by itself.
	p4 := commit(t, s, 0, 4, p3, change{Op: "set", Key: "d", Val: []byte("4")})
	eventually(t, 5*time.Second, "the background flush", func() bool {
		reopened, _ := openFileStore(dir, 1, false, false)
		st, _ := reopened.State(0)
		return st.Committed == p4
	})
}
