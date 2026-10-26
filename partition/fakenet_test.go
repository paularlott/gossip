package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip"
)

// fakeCluster is an in-process network of Managers: a request is run on the target Manager's handler, with the
// request and the reply passed through JSON as they would be on the wire.
type fakeCluster struct {
	t     *testing.T
	mu    sync.Mutex
	nodes []*fakeNode
	byID  map[gossip.NodeID]*fakeNode
	cut   map[[2]gossip.NodeID]bool
	hide  map[[2]gossip.NodeID]bool // membership has noticed the failure: the pair no longer lists each other
}

type fakeNode struct {
	fc    *fakeCluster
	id    gossip.NodeID
	store *memStore
	m     *Manager

	mgr atomic.Pointer[Manager] // set while the Manager is registered
	// changeErr, if set, is returned by change requests from this node instead of sending them.
	changeErr atomic.Pointer[error]
	down      atomic.Bool
	// dropReplies makes this node lose the replies to its promise and write requests (the peers handle the requests): a node
	// that can send but not receive those.
	dropReplies atomic.Bool
	// busyReplies counts the busy replies this node's change requests got.
	busyReplies atomic.Int64
	// heartbeatGate, if set, holds heartbeat requests to this node until it is closed (or the request's context ends):
	// a node that is too slow to answer.
	heartbeatGate atomic.Pointer[chan struct{}]
	// beforeStart, if set, runs on a Manager restart creates, before it is started.
	beforeStart func(m *Manager)
	size        atomic.Int64
	ready       atomic.Bool
}

// fakeNet is one node's view of the fakeCluster; it implements network.
type fakeNet struct{ fn *fakeNode }

func testConfig(store Store, size int) Config {
	return Config{
		Partitions:        4,
		ClusterSize:       size,
		HeartbeatInterval: 20 * time.Millisecond,
		OwnerTimeout:      100 * time.Millisecond,
		Tuning:            Tuning{BackoffMax: 40 * time.Millisecond, ReadyHoldDown: 10 * time.Millisecond},
		WriteTimeout:      2 * time.Second,
		Store:             store,
	}
}

// newFakeCluster creates n nodes (not started) of a cluster configured with the given size.
func newFakeCluster(t *testing.T, n, size int, tweak func(i int, c *Config)) *fakeCluster {
	t.Helper()
	fc := &fakeCluster{t: t, byID: map[gossip.NodeID]*fakeNode{}, cut: map[[2]gossip.NodeID]bool{}, hide: map[[2]gossip.NodeID]bool{}}
	for i := 1; i <= n; i++ {
		fn := &fakeNode{fc: fc, id: nid(i), store: newMemStore()}
		cfg := testConfig(fn.store, size)
		if tweak != nil {
			tweak(i-1, &cfg)
		}
		fn.size.Store(int64(cfg.ClusterSize))
		m, err := newManager(cfg, &fakeNet{fn})
		if err != nil {
			t.Fatal(err)
		}
		fn.m = m
		fc.nodes = append(fc.nodes, fn)
		fc.byID[fn.id] = fn
	}
	t.Cleanup(func() {
		for _, fn := range fc.nodes {
			fn.m.Stop()
		}
	})
	return fc
}

func (fc *fakeCluster) startAll() {
	fc.t.Helper()
	for _, fn := range fc.nodes {
		if err := fn.m.Start(); err != nil {
			fc.t.Fatal(err)
		}
	}
}

// cutLink makes requests between a and b fail (as a failed dial) in both directions.
func (fc *fakeCluster) cutLink(a, b gossip.NodeID, cut bool) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.cut[[2]gossip.NodeID{a, b}] = cut
	fc.cut[[2]gossip.NodeID{b, a}] = cut
}

// hideLink makes a and b drop each other from membership (failure detection has caught up with a cut link).
func (fc *fakeCluster) hideLink(a, b gossip.NodeID, hide bool) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.hide[[2]gossip.NodeID{a, b}] = hide
	fc.hide[[2]gossip.NodeID{b, a}] = hide
}

func (fc *fakeCluster) isHidden(a, b gossip.NodeID) bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.hide[[2]gossip.NodeID{a, b}]
}

func (fc *fakeCluster) isCut(a, b gossip.NodeID) bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.cut[[2]gossip.NodeID{a, b}]
}

// isolate cuts a node from every other node; they stay in each other's membership until detect is called.
func (fc *fakeCluster) isolate(fn *fakeNode, cut bool) {
	for _, o := range fc.nodes {
		if o != fn {
			fc.cutLink(fn.id, o.id, cut)
		}
	}
}

// detect makes an isolated node and the rest drop each other from membership, or (false) see each other again.
func (fc *fakeCluster) detect(fn *fakeNode, detected bool) {
	for _, o := range fc.nodes {
		if o != fn {
			fc.hideLink(fn.id, o.id, detected)
		}
	}
}

// crash takes a node down: it vanishes from membership and answers nothing; its Manager is stopped.
func (fc *fakeCluster) crash(fn *fakeNode) {
	fn.down.Store(true)
	fn.m.Stop()
}

func (n *fakeNet) self() gossip.NodeID { return n.fn.id }

func (n *fakeNet) peers() []peerInfo {
	var out []peerInfo
	for _, o := range n.fn.fc.nodes {
		if o == n.fn || o.down.Load() || n.fn.fc.isHidden(n.fn.id, o.id) {
			continue
		}
		out = append(out, peerInfo{ID: o.id, Size: int(o.size.Load()), Ready: o.ready.Load()})
	}
	return out
}

func (n *fakeNet) publish(size int, cluster string, ready bool) {
	n.fn.size.Store(int64(size))
	n.fn.ready.Store(ready)
}

func (n *fakeNet) register(m *Manager) error { n.fn.mgr.Store(m); return nil }
func (n *fakeNet) unregister()               { n.fn.mgr.Store(nil) }

// target returns the Manager that would handle a request to `to`, or the error a failed dial gives.
func (n *fakeNet) target(ctx context.Context, to gossip.NodeID) (*Manager, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o := n.fn.fc.byID[to]
	if o == nil || o.down.Load() || n.fn.down.Load() || n.fn.fc.isCut(n.fn.id, to) {
		return nil, fmt.Errorf("%w: fake dial to %s failed", errNotSent, to)
	}
	m := o.mgr.Load()
	if m == nil {
		return nil, fmt.Errorf("%w: %s has no handlers", errNotSent, to)
	}
	return m, nil
}

// wire passes v through JSON, as the codec would.
func wire[T any](v *T) *T {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	out := new(T)
	if err := json.Unmarshal(b, out); err != nil {
		panic(err)
	}
	return out
}

// call runs the request on the target Manager's handler, with the request and the reply passed through JSON.
func (n *fakeNet) call(ctx context.Context, to gossip.NodeID, mt gossip.MessageType, req, resp interface{}) error {
	if mt == partChangeMsg || mt == partReadMsg {
		if e := n.fn.changeErr.Load(); e != nil {
			return *e
		}
	}
	m, err := n.target(ctx, to)
	if err != nil {
		return err
	}
	var out interface{}
	switch r := req.(type) {
	case *promiseReq:
		out = m.handlePromise(n.fn.id, wire(r))
	case *writeReq:
		out = m.handleWrite(n.fn.id, wire(r))
	case *changeReq:
		cr := m.handleChange(ctx, wire(r))
		if cr.Status == statusBusy {
			n.fn.busyReplies.Add(1)
		}
		out = cr
	case *readReq:
		out = m.handleRead(ctx, wire(r))
	case *stateReq:
		out = m.handleState(wire(r))
	case *heartbeatMsg:
		if gate := n.fn.fc.byID[to].heartbeatGate.Load(); gate != nil {
			select {
			case <-*gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		out = m.handleHeartbeat(n.fn.id, wire(r))
	case *claimReq:
		out = m.handleClaim(n.fn.id, wire(r))
	default:
		return fmt.Errorf("fake: unknown request %T", req)
	}
	if n.fn.dropReplies.Load() && (mt == partPromiseMsg || mt == partWriteMsg) {
		return errors.New("fake: reply dropped")
	}
	b, err := json.Marshal(out)
	if err != nil {
		panic(err)
	}
	return json.Unmarshal(b, resp)
}

func (n *fakeNet) send(to gossip.NodeID, mt gossip.MessageType, msg interface{}) error {
	m, err := n.target(context.Background(), to)
	if err != nil {
		return err
	}
	switch r := msg.(type) {
	case *commitMsg:
		m.handleCommit(n.fn.id, wire(r))
	case *relinquishMsg:
		m.handleRelinquish(n.fn.id, wire(r))
	default:
		return fmt.Errorf("fake: unknown message %T", msg)
	}
	return nil
}

func (n *fakeNet) catchUp(ctx context.Context, to gossip.NodeID, req *catchUpReq) (io.ReadCloser, error) {
	m, err := n.target(ctx, to)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	r := wire(req)
	go func() { pw.CloseWithError(m.serveCatchUp(r, pw)) }()
	return pr, nil
}

// waitFor polls cond until it holds or the limit passes.
func waitFor(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// attach registers every node's Manager with the fake network without starting any loop, so a test can drive the
// handlers and takeover by hand.
func (fc *fakeCluster) attach() {
	for _, fn := range fc.nodes {
		fn.mgr.Store(fn.m)
	}
}

// ep makes an epoch of the counter proposed by node n.
func ep(counter uint64, n int) Epoch { return Epoch{Counter: counter, Node: nid(n)} }

// restart replaces a crashed node's Manager with a new one over the same Store and brings the node back up.
func (fc *fakeCluster) restart(fn *fakeNode, tweak func(c *Config)) {
	fc.t.Helper()
	cfg := testConfig(fn.store, int(fn.size.Load()))
	if tweak != nil {
		tweak(&cfg)
	}
	m, err := newManager(cfg, &fakeNet{fn})
	if err != nil {
		fc.t.Fatal(err)
	}
	fn.ready.Store(false)
	fn.m = m
	fn.down.Store(false)
	if fn.beforeStart != nil {
		fn.beforeStart(m)
	}
	if err := m.Start(); err != nil {
		fc.t.Fatal(err)
	}
}

// owners returns, per partition, the nodes that currently lead it (among the nodes that are up).
func (fc *fakeCluster) owners(p int) []*fakeNode {
	var out []*fakeNode
	for _, fn := range fc.nodes {
		if !fn.down.Load() && fn.m.IsOwner(p) {
			out = append(out, fn)
		}
	}
	return out
}

// waitOwners waits until every partition has exactly one owner among the nodes that are up, and every such node
// agrees who it is.
func (fc *fakeCluster) waitOwners(limit time.Duration) {
	fc.t.Helper()
	defer func() {
		if fc.t.Failed() {
			fc.t.Log(fc.describe())
		}
	}()
	waitFor(fc.t, limit, "every partition to have one owner everyone agrees on", func() bool {
		for p := 0; p < fc.nodes[0].m.cfg.Partitions; p++ {
			owners := fc.owners(p)
			if len(owners) != 1 {
				return false
			}
			for _, fn := range fc.nodes {
				if fn.down.Load() {
					continue
				}
				if o, ok := fn.m.Owner(p); !ok || o != owners[0].id {
					return false
				}
			}
		}
		return true
	})
}

// describe summarises every node's view of every partition, for failure messages.
func (fc *fakeCluster) describe() string {
	var b strings.Builder
	for i, fn := range fc.nodes {
		fmt.Fprintf(&b, "node %d (%s) down=%v ready=%v resync=%v\n", i+1, fn.id, fn.down.Load(), fn.m.Ready(), fn.m.resync.Load())
		for p := range fn.m.parts {
			ps := fn.m.parts[p]
			ps.vmu.RLock()
			st := fn.store.state(p)
			fmt.Fprintf(&b, "  p%d leading=%v myEpoch=%v owner=%v/%v seen=%v committed=%v promised=%v pending=%v maxCounter=%d\n",
				p, ps.leading, ps.myEpoch, ps.owner, ps.ownerEpoch, time.Since(ps.ownerSeen).Round(time.Millisecond), st.Committed, st.Promised, st.Pending != nil, ps.maxCounter.Load())
			ps.vmu.RUnlock()
		}
	}
	return b.String()
}

// errNotSentWrap is an error that errors.Is reports as errNotSent, like a failed dial.
type errNotSentWrap struct{ error }

func (e errNotSentWrap) Is(target error) bool { return target == errNotSent }

var gossipEmpty = gossip.EmptyNodeID

// catchingUp sets or clears the in-flight flag of p's asynchronous catch-up (to keep one from starting in a test).
func (m *Manager) catchingUp(p int, v bool) { m.parts[p].catchingUp.Store(v) }

// holdHeartbeats makes heartbeat requests to fn wait until the returned function is called (also at the end of the
// test).
func holdHeartbeats(t *testing.T, fn *fakeNode) func() {
	gate := make(chan struct{})
	fn.heartbeatGate.Store(&gate)
	var once sync.Once
	rel := func() {
		once.Do(func() {
			fn.heartbeatGate.Store(nil)
			close(gate)
		})
	}
	t.Cleanup(rel)
	return rel
}
