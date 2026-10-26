package partition

// Destruction-test harness: N real gossip clusters on localhost (socket transport wrapped in faultnet), each running a
// Manager over a dtStore that survives restarts, with every fault of faultnet available at runtime. Everything here is
// prefixed dt so it cannot collide with other test files of the package.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/internal/faultnet"
	"github.com/paularlott/logger"
)

// Timings shared by the destruction tests: short, so that failovers take fractions of a second.
const (
	dtHeartbeat    = 100 * time.Millisecond
	dtOwnerTimeout = 400 * time.Millisecond
	dtBackoff      = 200 * time.Millisecond
	dtHoldDown     = 300 * time.Millisecond
	dtWriteTimeout = 3 * time.Second
	// dtStale is Config.StaleAfter (its default, OwnerTimeout) and dtStaleSlack the scheduling slack allowed on top of
	// it when checking that a cut-off node stops serving reads.
	dtStale      = dtOwnerTimeout
	dtStaleSlack = 800 * time.Millisecond
	// dtLiveness bounds how long writes may take to succeed again once the network is healed and the nodes are up.
	dtLiveness = 15 * time.Second
)

// --- log capture -----------------------------------------------------------------------------------------------

// dtLogBuf keeps the most recent log lines of every node, so a failing test can show what the protocol did.
type dtLogBuf struct {
	mu    sync.Mutex
	start time.Time
	lines []string
	max   int
}

func (b *dtLogBuf) add(line string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines = append(b.lines, fmt.Sprintf("%7.3f %s", time.Since(b.start).Seconds(), line))
	if len(b.lines) > b.max {
		b.lines = append([]string(nil), b.lines[len(b.lines)-b.max/2:]...)
	}
}

func (b *dtLogBuf) tail(n int) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.lines) < n {
		n = len(b.lines)
	}
	return append([]string(nil), b.lines[len(b.lines)-n:]...)
}

// dtLogger is a logger.Logger writing into a dtLogBuf.
type dtLogger struct {
	b      *dtLogBuf
	prefix string
}

func (l dtLogger) log(level, msg string, kv []any) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %s%s", level, l.prefix, msg)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(&sb, " %v=%v", kv[i], kv[i+1])
	}
	l.b.add(sb.String())
}

func (l dtLogger) Trace(msg string, kv ...any) {}
func (l dtLogger) Debug(msg string, kv ...any) { l.log("DEBUG", msg, kv) }
func (l dtLogger) Info(msg string, kv ...any)  { l.log("INFO", msg, kv) }
func (l dtLogger) Warn(msg string, kv ...any)  { l.log("WARN", msg, kv) }
func (l dtLogger) Error(msg string, kv ...any) { l.log("ERROR", msg, kv) }
func (l dtLogger) Fatal(msg string, kv ...any) { l.log("FATAL", msg, kv) }
func (l dtLogger) With(key string, value any) logger.Logger {
	return dtLogger{l.b, l.prefix + fmt.Sprintf("%s=%v ", key, value)}
}
func (l dtLogger) WithError(err error) logger.Logger { return l.With("error", err) }
func (l dtLogger) WithGroup(group string) logger.Logger {
	return dtLogger{l.b, l.prefix + group + ": "}
}

// --- ledger ------------------------------------------------------------------------------------------------------

// dtCommit is one committed write as some node recorded it.
type dtCommit struct {
	Node int
	Pos  Pos
	Data string
}

// dtLedger records every commit and import of every store of the cluster, across restarts and lost disks, so that
// invariant 2 (same Seq, same data on every node) and invariant 1 (nothing ever committed is lost) can be checked even
// for a disk that no longer exists.
type dtLedger struct {
	mu        sync.Mutex
	bySeq     map[[2]uint64]dtCommit // (partition, seq) -> first record
	conflicts []string
}

func newDTLedger() *dtLedger { return &dtLedger{bySeq: map[[2]uint64]dtCommit{}} }

func (l *dtLedger) record(node, p int, pos Pos, data []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	k := [2]uint64{uint64(p), pos.Seq}
	cur, ok := l.bySeq[k]
	if !ok {
		l.bySeq[k] = dtCommit{Node: node, Pos: pos, Data: string(data)}
		return
	}
	if cur.Data != string(data) {
		l.conflicts = append(l.conflicts, fmt.Sprintf("partition %d seq %d: node %d committed %q at %s, node %d committed %q at %s",
			p, pos.Seq, cur.Node, cur.Data, cur.Pos, node, data, pos))
	}
}

// --- store -------------------------------------------------------------------------------------------------------

// dtChange is the application change of the workload: key k, expecting version Exp (compare-and-swap increment).
type dtChange struct {
	K   string `json:"k"`
	Exp int    `json:"exp"`
	ID  string `json:"id"`
}

// dtData is the Data of a committed write: key K is now at version V; ID names the operation.
type dtData struct {
	K  string `json:"k"`
	V  int    `json:"v"`
	ID string `json:"id"`
}

// dtStore is the Store of a node of the destruction tests: a memStore (which keeps the committed log per partition) with
// a compare-and-swap Prepare, a record of every commit in the shared ledger and hooks. The object is kept across a
// restart of the node: it is the disk.
type dtStore struct {
	*memStore
	led  *dtLedger
	node int

	dmu  sync.Mutex
	vers map[int]map[string]int // committed version per partition and key

	hmu             sync.Mutex
	afterSetPending func(p int, w *Write)
	beforePending   func(p int, w *Write) error
	beforePrepare   func(p int, change []byte)
	beforeCommit    func(p int, w *Write)
	beforePromised  func(p int, e Epoch)
}

func newDTStore(led *dtLedger, node int, intact bool) *dtStore {
	s := &dtStore{memStore: newMemStore(), led: led, node: node, vers: map[int]map[string]int{}}
	s.memStore.intact = intact
	return s
}

func (s *dtStore) setIntact(v bool) {
	s.memStore.mu.Lock()
	s.memStore.intact = v
	s.memStore.mu.Unlock()
}

// version returns the committed version of key in p.
func (s *dtStore) version(p int, key string) int {
	s.dmu.Lock()
	defer s.dmu.Unlock()
	return s.vers[p][key]
}

func (s *dtStore) setHook(fn func(p int, w *Write)) {
	s.hmu.Lock()
	s.afterSetPending = fn
	s.hmu.Unlock()
}

// setPrepareHook installs fn, which runs at the start of Prepare (it may block). Prepare runs on the owner, under the
// partition's mutex, after the owner has checked that it still leads and before replicate: the last point at which a
// scenario can hold or cut off the owner without the write having left it (replicate stores the owner's own pending write
// at the same time as it sends the write, so a SetPending hook cannot).
func (s *dtStore) setPrepareHook(fn func(p int, change []byte)) {
	s.hmu.Lock()
	s.beforePrepare = fn
	s.hmu.Unlock()
}

// setPendingFault installs fn, which runs at the start of SetPending of a write (not of a clear), before it takes
// effect; an error it returns is the call's, and nothing is stored. It may block.
func (s *dtStore) setPendingFault(fn func(p int, w *Write) error) {
	s.hmu.Lock()
	s.beforePending = fn
	s.hmu.Unlock()
}

// setPromisedHook installs fn, which runs at the start of SetPromised, before it takes effect (it may block).
func (s *dtStore) setPromisedHook(fn func(p int, e Epoch)) {
	s.hmu.Lock()
	s.beforePromised = fn
	s.hmu.Unlock()
}

func (s *dtStore) SetPromised(p int, e Epoch) error {
	s.hmu.Lock()
	hook := s.beforePromised
	s.hmu.Unlock()
	if hook != nil {
		hook(p, e)
	}
	return s.memStore.SetPromised(p, e)
}

// setCommitHook installs fn, which runs just before a Commit takes effect (it may block).
func (s *dtStore) setCommitHook(fn func(p int, w *Write)) {
	s.hmu.Lock()
	s.beforeCommit = fn
	s.hmu.Unlock()
}

func (s *dtStore) SetPending(p int, w *Write) error {
	if w != nil {
		s.hmu.Lock()
		fault := s.beforePending
		s.hmu.Unlock()
		if fault != nil {
			if err := fault(p, copyWrite(w)); err != nil {
				return err
			}
		}
	}
	if err := s.memStore.SetPending(p, w); err != nil {
		return err
	}
	if w != nil {
		s.hmu.Lock()
		fn := s.afterSetPending
		s.hmu.Unlock()
		if fn != nil {
			fn(p, copyWrite(w))
		}
	}
	return nil
}

func (s *dtStore) Commit(w *Write) error {
	s.hmu.Lock()
	hook := s.beforeCommit
	s.hmu.Unlock()
	if hook != nil {
		hook(w.Partition, copyWrite(w))
	}
	if err := s.memStore.Commit(w); err != nil {
		return err
	}
	var d dtData
	if err := json.Unmarshal(w.Data, &d); err == nil {
		s.dmu.Lock()
		if s.vers[w.Partition] == nil {
			s.vers[w.Partition] = map[string]int{}
		}
		s.vers[w.Partition][d.K] = d.V
		s.dmu.Unlock()
	}
	s.led.record(s.node, w.Partition, w.Pos, w.Data)
	return nil
}

func (s *dtStore) Import(p int, r io.Reader, upTo Pos, full bool) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := s.memStore.Import(p, bytes.NewReader(body), upTo, full); err != nil {
		return err
	}
	var in []memEntry
	if err := json.Unmarshal(body, &in); err == nil {
		for _, e := range in {
			s.led.record(s.node, p, Pos{Seq: e.Seq}, e.Data)
		}
	}
	// Rebuild the versions of p from its log (imports are rare).
	s.memStore.mu.Lock()
	entries := append([]memEntry(nil), s.memStore.part(p).entries...)
	s.memStore.mu.Unlock()
	vers := map[string]int{}
	for _, e := range entries {
		var d dtData
		if json.Unmarshal(e.Data, &d) == nil {
			vers[d.K] = d.V
		}
	}
	s.dmu.Lock()
	s.vers[p] = vers
	s.dmu.Unlock()
	return nil
}

// Prepare is a compare-and-swap; its reply is the operation's ID, so that a workload can check that the reply reaches the
// caller of Write unchanged.
func (s *dtStore) Prepare(p int, change []byte) ([]byte, []byte, error) {
	s.hmu.Lock()
	hook := s.beforePrepare
	s.hmu.Unlock()
	if hook != nil {
		hook(p, change)
	}
	var ch dtChange
	if err := json.Unmarshal(change, &ch); err != nil {
		return nil, nil, fmt.Errorf("dtStore: bad change: %w", err)
	}
	if cur := s.version(p, ch.K); cur != ch.Exp {
		return nil, nil, Refuse("conflict", fmt.Sprintf("dtStore: %s is at version %d, expected %d", ch.K, cur, ch.Exp))
	}
	data, err := json.Marshal(dtData{K: ch.K, V: ch.Exp + 1, ID: ch.ID})
	return data, []byte(ch.ID), err
}

// dtReadResult is what dtStore.Read answers for a key: its committed version and the partition's committed sequence number.
type dtReadResult struct {
	V   int    `json:"v"`
	Seq uint64 `json:"seq"`
}

// Read answers a key (req is its name) with its committed version and the partition's committed sequence number. A
// write is visible to it once its Commit has returned, as the Store contract asks.
func (s *dtStore) Read(p int, req []byte) ([]byte, error) {
	v := s.version(p, string(req))
	st, err := s.memStore.State(p)
	if err != nil {
		return nil, err
	}
	return json.Marshal(dtReadResult{V: v, Seq: st.Committed.Seq})
}

// dtPartSnap is a consistent copy of one partition of a store.
type dtPartSnap struct {
	St      State
	Entries []memEntry
}

func (s *dtStore) snap(p int) dtPartSnap {
	s.memStore.mu.Lock()
	defer s.memStore.mu.Unlock()
	mp := s.memStore.part(p)
	st := mp.st
	st.Pending = copyWrite(st.Pending)
	return dtPartSnap{St: st, Entries: append([]memEntry(nil), mp.entries...)}
}

// --- cluster -----------------------------------------------------------------------------------------------------

// dtOpts configures a destruction-test cluster.
type dtOpts struct {
	Nodes int
	Parts int   // default 4
	Sizes []int // ClusterSize per node; default Nodes for every node
	Seed  int64 // default 1
	// Tweak adjusts the Manager config of node i before it starts.
	Tweak func(i int, cfg *Config)
}

// dtLink is the fault configuration of node i's transport towards (or from) node j.
type dtLink struct {
	blockTo, blockFrom bool
	dmin, dmax         time.Duration
	drop, dropReply    float64
	dup                float64
}

// dtNode is one node of a dtCluster.
type dtNode struct {
	idx   int
	addr  string
	idStr string
	id    gossip.NodeID
	size  int
	gen   int

	store *dtStore

	// The fields below are guarded by dtCluster.mu.
	up      bool
	cluster *gossip.Cluster
	ft      *faultnet.Transport
	mgr     *Manager
}

// dtCluster is a set of real clusters with fault injection and a monitor of the invariants that can be checked while it
// runs.
type dtCluster struct {
	t     testing.TB
	opts  dtOpts
	parts int
	net   *faultnet.Network
	led   *dtLedger
	ops   *dtOps
	logs  *dtLogBuf
	rng   *rand.Rand

	mu       sync.Mutex
	nodes    []*dtNode
	links    [][]dtLink
	cut      []bool
	isoSince []time.Time

	vmu        sync.Mutex
	violations []string

	closeOnce sync.Once
	stopMon   chan struct{}
	monDone   chan struct{}
}

func dtNewCluster(t testing.TB, o dtOpts) *dtCluster {
	t.Helper()
	if o.Parts == 0 {
		o.Parts = 4
	}
	if o.Seed == 0 {
		o.Seed = 1
	}
	c := &dtCluster{
		t: t, opts: o, parts: o.Parts, net: faultnet.NewNetwork(), led: newDTLedger(), ops: &dtOps{},
		logs:     &dtLogBuf{start: time.Now(), max: 40000},
		rng:      rand.New(rand.NewSource(o.Seed)),
		links:    make([][]dtLink, o.Nodes),
		cut:      make([]bool, o.Nodes),
		isoSince: make([]time.Time, o.Nodes),
		stopMon:  make(chan struct{}),
		monDone:  make(chan struct{}),
	}
	for i := 0; i < o.Nodes; i++ {
		var raw [16]byte
		c.rng.Read(raw[:])
		raw[6] = raw[6]&0x0f | 0x40
		raw[8] = raw[8]&0x3f | 0x80
		id := gossip.NodeID(raw)
		size := o.Nodes
		if i < len(o.Sizes) {
			size = o.Sizes[i]
		}
		c.nodes = append(c.nodes, &dtNode{idx: i, id: id, idStr: id.String(), size: size, store: newDTStore(c.led, i, true)})
		c.links[i] = make([]dtLink, o.Nodes)
	}
	go c.monitor()
	t.Cleanup(c.close)
	return c
}

// config returns the Manager config of node i.
func (c *dtCluster) config(n *dtNode) Config {
	cfg := Config{
		Partitions:        c.parts,
		ClusterSize:       n.size,
		HeartbeatInterval: dtHeartbeat,
		OwnerTimeout:      dtOwnerTimeout,
		Tuning:            Tuning{BackoffMax: dtBackoff, ReadyHoldDown: dtHoldDown},
		WriteTimeout:      dtWriteTimeout,
		Store:             n.store,
		Logger:            dtLogger{b: c.logs, prefix: fmt.Sprintf("n%d ", n.idx)},
	}
	if c.opts.Tweak != nil {
		c.opts.Tweak(n.idx, &cfg)
	}
	return cfg
}

// start brings node i up (a first start, or a restart over the store it has): a new gossip cluster on its address and
// ID, joined to the nodes that are up, and a new Manager.
func (c *dtCluster) start(i int) {
	c.t.Helper()
	n := c.nodes[i]
	c.mu.Lock()
	if n.up {
		c.mu.Unlock()
		c.t.Fatalf("node %d is already up", i)
	}
	c.mu.Unlock()
	if n.addr == "" {
		n.addr = dtFreeAddr(c.t)
	}
	n.gen++

	gcfg := gossip.DefaultConfig()
	gcfg.NodeID = n.idStr
	gcfg.BindAddr = n.addr
	gcfg.AdvertiseAddr = n.addr
	gcfg.Logger = dtLogger{b: c.logs, prefix: fmt.Sprintf("n%d CLUSTER ", i)}
	// Brisk failure detection and recovery keep the tests short.
	gcfg.SuspectTimeout = 500 * time.Millisecond
	gcfg.DeadNodeTimeout = 2 * time.Second
	gcfg.GossipInterval = 250 * time.Millisecond
	gcfg.DeadNodeRetryInterval = time.Second
	gcfg.PeerRecoveryInterval = 2 * time.Second
	// The transport must let a forwarded change wait for its reply as long as its landing node does (README: "The
	// transport's deadline"); checked against the Manager below.
	mcfg := c.config(n)
	gcfg.TCPDeadline = mcfg.WriteTimeout + mcfg.HeartbeatInterval
	ft := faultnet.New(gossip.NewSocketTransport(gcfg), c.opts.Seed*1000+int64(i)*100+int64(n.gen))
	gcfg.Transport = ft
	cl, err := gossip.NewCluster(gcfg)
	if err != nil {
		c.t.Fatalf("node %d: cluster on %s: %v", i, n.addr, err)
	}
	if cl.LocalNode().ID != n.id {
		c.t.Fatalf("node %d: id %s, want %s", i, cl.LocalNode().ID, n.id)
	}
	c.net.Add(n.id, ft)
	c.net.SetAddress(n.id, n.addr)

	mgr, err := New(cl, mcfg)
	if err != nil {
		c.t.Fatalf("node %d: %v", i, err)
	}
	if gcfg.TCPDeadline < mgr.MinTransportDeadline() {
		c.t.Fatalf("node %d: TCPDeadline %v below the Manager's MinTransportDeadline %v", i, gcfg.TCPDeadline, mgr.MinTransportDeadline())
	}

	c.mu.Lock()
	n.ft, n.cluster, n.mgr, n.up = ft, cl, mgr, true
	c.cut[i] = false
	for j := range c.nodes {
		c.applyLocked(i, j)
	}
	var peers []string
	for _, o := range c.nodes {
		if o.up && o.idx != i {
			peers = append(peers, o.addr)
		}
	}
	c.recomputeLocked()
	c.mu.Unlock()

	cl.Start()
	if len(peers) > 0 {
		if err := cl.Join(peers); err != nil {
			c.logs.add(fmt.Sprintf("HARNESS node %d: join: %v", i, err))
		}
		go c.rejoin(n, cl, peers)
	}
	if err := mgr.Start(); err != nil {
		c.t.Fatalf("node %d: manager start: %v", i, err)
	}
}

// rejoin retries a node's join while it knows nobody: a join attempted through a fault is not repeated by the library.
func (c *dtCluster) rejoin(n *dtNode, cl *gossip.Cluster, peers []string) {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		same := n.up && n.cluster == cl
		var addrs []string
		for _, o := range c.nodes {
			if o.up && o.idx != n.idx {
				addrs = append(addrs, o.addr)
			}
		}
		c.mu.Unlock()
		if !same {
			return
		}
		if len(addrs) == 0 || cl.NumAliveNodes() > 1 {
			continue
		}
		cl.Join(addrs)
	}
}

func (c *dtCluster) startAll() {
	c.t.Helper()
	for i := range c.nodes {
		c.start(i)
	}
}

// stop takes node i down. A crash cuts it first, so that nothing it does while stopping reaches the others (no goodbye,
// no relinquish); otherwise it is a clean shutdown.
func (c *dtCluster) stop(i int, crash bool) {
	c.t.Helper()
	c.mu.Lock()
	n := c.nodes[i]
	if !n.up {
		c.mu.Unlock()
		return
	}
	mgr, cl, ft := n.mgr, n.cluster, n.ft
	c.mu.Unlock()

	if crash {
		ft.Cut()
	}
	// A node that finished its resync has an intact disk from then on.
	if mgr.Ready() {
		n.store.setIntact(true)
	}
	mgr.Stop()
	cl.Stop()

	c.mu.Lock()
	n.up = false
	n.mgr, n.cluster, n.ft = nil, nil, nil
	c.recomputeLocked()
	c.mu.Unlock()
}

// crash stops node i as a crash would (cut, then stop); its disk survives.
func (c *dtCluster) crash(i int) { c.stop(i, true) }

// shutdown stops node i cleanly.
func (c *dtCluster) shutdown(i int) { c.stop(i, false) }

// restart starts node i again: with its data, or with a new empty disk that reports Intact() == false.
func (c *dtCluster) restart(i int, withData bool) {
	c.t.Helper()
	n := c.nodes[i]
	if !withData {
		c.mu.Lock()
		n.store = newDTStore(c.led, i, false)
		c.mu.Unlock()
	}
	c.start(i)
}

func (c *dtCluster) close() {
	c.closeOnce.Do(c.closeAll)
}

func (c *dtCluster) closeAll() {
	close(c.stopMon)
	<-c.monDone
	var wg sync.WaitGroup
	for i := range c.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.stop(i, true)
		}()
	}
	wg.Wait()
}

// dtFreeAddr returns a localhost address whose TCP and UDP ports are both free.
func dtFreeAddr(t testing.TB) string {
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

// --- accessors ---------------------------------------------------------------------------------------------------

// node returns the live parts of node i (nil mgr when it is down).
func (c *dtCluster) node(i int) (mgr *Manager, store *dtStore) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[i]
	return n.mgr, n.store
}

func (c *dtCluster) isUp(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i].up
}

// upNodes lists the nodes that are running.
func (c *dtCluster) upNodes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int
	for _, n := range c.nodes {
		if n.up {
			out = append(out, n.idx)
		}
	}
	return out
}

func (c *dtCluster) idx(id gossip.NodeID) int {
	for _, n := range c.nodes {
		if n.id == id {
			return n.idx
		}
	}
	return -1
}

// leaders lists the up nodes that currently believe they own p.
func (c *dtCluster) leaders(p int) []int {
	var out []int
	for _, i := range c.upNodes() {
		if m, _ := c.node(i); m != nil && m.IsOwner(p) {
			out = append(out, i)
		}
	}
	return out
}

// ownerOf returns the single node that owns p, or -1 if there is none or several.
func (c *dtCluster) ownerOf(p int) int {
	if l := c.leaders(p); len(l) == 1 {
		return l[0]
	}
	return -1
}

func (c *dtCluster) allParts() []int {
	out := make([]int, c.parts)
	for p := range out {
		out[p] = p
	}
	return out
}

// --- waiting -----------------------------------------------------------------------------------------------------

// await polls cond until it holds; on a timeout it dumps the state of the cluster and fails the test.
func (c *dtCluster) await(limit time.Duration, what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.dump()
	c.t.Fatalf("timed out after %v waiting for %s", limit, what)
}

// awaitReady waits until the given nodes (default: every node that is up) are Ready.
func (c *dtCluster) awaitReady(limit time.Duration, nodes ...int) {
	c.t.Helper()
	if len(nodes) == 0 {
		nodes = c.upNodes()
	}
	c.await(limit, fmt.Sprintf("nodes %v to be ready", nodes), func() bool {
		for _, i := range nodes {
			if m, _ := c.node(i); m == nil || !m.Ready() {
				return false
			}
		}
		return true
	})
}

// awaitOwners waits until every partition has exactly one owner among the given nodes (default: those that are up), no
// other up node believes it owns one, and every node of the group knows who it is.
func (c *dtCluster) awaitOwners(limit time.Duration, nodes ...int) {
	c.t.Helper()
	if len(nodes) == 0 {
		nodes = c.upNodes()
	}
	c.await(limit, fmt.Sprintf("one owner per partition among %v", nodes), func() bool {
		for p := 0; p < c.parts; p++ {
			var owner = -1
			for _, i := range nodes {
				if m, _ := c.node(i); m != nil && m.IsOwner(p) {
					if owner >= 0 {
						return false
					}
					owner = i
				}
			}
			if owner < 0 {
				return false
			}
			for _, i := range nodes {
				m, _ := c.node(i)
				if m == nil {
					return false
				}
				if o, ok := m.Owner(p); !ok || c.idx(o) != owner {
					return false
				}
			}
		}
		return true
	})
}

// dump logs the state of every node and the tail of the protocol log.
func (c *dtCluster) dump() {
	c.t.Helper()
	var b strings.Builder
	for _, i := range c.upNodes() {
		m, s := c.node(i)
		if m == nil {
			continue
		}
		fmt.Fprintf(&b, "node %d (%s) ready=%v resync=%v:\n", i, c.nodes[i].id.String()[:8], m.Ready(), m.resync.Load())
		for p := 0; p < c.parts; p++ {
			st, _ := s.State(p)
			fmt.Fprintf(&b, "  p%d leading=%v canRead=%v promised=%s committed=%s pending=%v\n", p, m.IsOwner(p), m.CanRead(p),
				st.Promised, st.Committed, st.Pending != nil)
		}
	}
	c.t.Logf("cluster state:\n%s", b.String())
	c.t.Logf("protocol log tail:\n%s", strings.Join(c.logs.tail(300), "\n"))
}

// --- faults ------------------------------------------------------------------------------------------------------

// applyLocked pushes the recorded fault configuration of node i towards j onto node i's transport.
func (c *dtCluster) applyLocked(i, j int) {
	n := c.nodes[i]
	if !n.up || n.ft == nil || i == j {
		return
	}
	l, id, ft := c.links[i][j], c.nodes[j].id, n.ft
	if l.blockTo {
		ft.Block(id)
	} else {
		ft.Unblock(id)
	}
	if l.blockFrom {
		ft.BlockFrom(id)
	} else {
		ft.UnblockFrom(id)
	}
	ft.Delay(id, l.dmin, l.dmax)
	ft.DropRate(id, l.drop)
	ft.DropReplyRate(id, l.dropReply)
	ft.Duplicate(id, l.dup)
	if c.cut[i] {
		ft.Cut()
	} else {
		ft.Uncut()
	}
}

func (c *dtCluster) fault(i, j int, fn func(l *dtLink)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(&c.links[i][j])
	c.applyLocked(i, j)
	c.recomputeLocked()
}

// Cut silences node i completely, as a crashed process would be (it keeps running).
func (c *dtCluster) Cut(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cut[i] = true
	if ft := c.nodes[i].ft; ft != nil {
		ft.Cut()
	}
	c.recomputeLocked()
}

// Uncut ends Cut.
func (c *dtCluster) Uncut(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cut[i] = false
	if ft := c.nodes[i].ft; ft != nil {
		ft.Uncut()
	}
	c.recomputeLocked()
}

// Block makes node i's requests and packets to j fail (one way).
func (c *dtCluster) Block(i, j int)   { c.fault(i, j, func(l *dtLink) { l.blockTo = true }) }
func (c *dtCluster) Unblock(i, j int) { c.fault(i, j, func(l *dtLink) { l.blockTo = false }) }

// BlockFrom makes node i ignore everything that arrives from j.
func (c *dtCluster) BlockFrom(i, j int) { c.fault(i, j, func(l *dtLink) { l.blockFrom = true }) }

// Delay delays node i's traffic to j by a random time in [min, max].
func (c *dtCluster) Delay(i, j int, min, max time.Duration) {
	c.fault(i, j, func(l *dtLink) { l.dmin, l.dmax = min, max })
}

// DropRate drops a fraction of node i's requests and packets to j.
func (c *dtCluster) DropRate(i, j int, p float64) { c.fault(i, j, func(l *dtLink) { l.drop = p }) }

// DropReply loses a fraction of the replies node i receives from j (j handled the request).
func (c *dtCluster) DropReply(i, j int, p float64) {
	c.fault(i, j, func(l *dtLink) { l.dropReply = p })
}

// Duplicate duplicates a fraction of node i's fire-and-forget packets to j.
func (c *dtCluster) Duplicate(i, j int, p float64) { c.fault(i, j, func(l *dtLink) { l.dup = p }) }

// Split severs every link between the two groups, both ways.
func (c *dtCluster) Split(a, b []int) {
	for _, i := range a {
		for _, j := range b {
			c.Block(i, j)
			c.Block(j, i)
		}
	}
}

// Isolate severs every link of node i to the others (both ways) without cutting it.
func (c *dtCluster) Isolate(i int) {
	for j := range c.nodes {
		if j != i {
			c.Block(i, j)
			c.Block(j, i)
		}
	}
}

// Heal clears every fault on every node.
func (c *dtCluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.links {
		c.cut[i] = false
		for j := range c.links[i] {
			c.links[i][j] = dtLink{}
		}
		if ft := c.nodes[i].ft; ft != nil {
			ft.Heal()
		}
	}
	c.recomputeLocked()
}

// linkDownLocked reports whether node a's traffic can no longer reach b: a or b is cut or down, a blocks b, or b ignores a.
func (c *dtCluster) linkDownLocked(a, b int) bool {
	return !c.nodes[a].up || !c.nodes[b].up || c.cut[a] || c.cut[b] || c.links[a][b].blockTo || c.links[b][a].blockFrom
}

// recomputeLocked updates which nodes are isolated: up, with every link to and from every other node down.
func (c *dtCluster) recomputeLocked() {
	for i, n := range c.nodes {
		iso := n.up
		if iso {
			for j := range c.nodes {
				if j != i && (!c.linkDownLocked(i, j) || !c.linkDownLocked(j, i)) {
					iso = false
					break
				}
			}
		}
		switch {
		case iso && c.isoSince[i].IsZero():
			c.isoSince[i] = time.Now()
		case !iso:
			c.isoSince[i] = time.Time{}
		}
	}
}

// --- monitor -----------------------------------------------------------------------------------------------------

func (c *dtCluster) violate(format string, args ...any) {
	c.vmu.Lock()
	defer c.vmu.Unlock()
	if len(c.violations) < 50 {
		c.violations = append(c.violations, fmt.Sprintf(format, args...))
	}
}

// monitor checks, while the cluster runs, the invariants that must hold at every moment: a node that is isolated stops
// serving reads within StaleAfter (invariant 5), and no store ever holds a pending write that does not follow its
// committed one.
func (c *dtCluster) monitor() {
	defer close(c.monDone)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	n := 0
	for {
		select {
		case <-c.stopMon:
			return
		case <-tick.C:
		}
		n++
		c.mu.Lock()
		for i, node := range c.nodes {
			if !node.up || c.isoSince[i].IsZero() || time.Since(c.isoSince[i]) < dtStale+dtStaleSlack {
				continue
			}
			for p := 0; p < c.parts; p++ {
				if node.mgr.CanRead(p) {
					c.violate("invariant 5: node %d isolated for %v but CanRead(%d) is true", i, time.Since(c.isoSince[i]).Round(time.Millisecond), p)
				}
			}
		}
		stores := make([]*dtStore, 0, len(c.nodes))
		for _, node := range c.nodes {
			stores = append(stores, node.store)
		}
		c.mu.Unlock()
		if n%8 == 0 {
			for _, s := range stores {
				if err := s.checkInvariant(); err != nil {
					c.violate("store invariant (node %d): %v", s.node, err)
				}
			}
		}
	}
}

// sortedKeys is a small helper for deterministic output.
func dtSortedKeys[K ~string, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}
