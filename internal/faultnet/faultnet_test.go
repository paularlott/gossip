package faultnet_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/gossip/internal/faultnet"
	"github.com/paularlott/logger"
)

const (
	msgEcho   = gossip.UserMsg + 1 // request/response: replies with the receiving node's index
	msgCount  = gossip.UserMsg + 2 // one-way: counted and timestamped
	msgStream = gossip.UserMsg + 3 // stream: writes "hello"
	msgSlow   = gossip.UserMsg + 4 // stream: writes a 64KB frame every 10ms until the write fails
)

type echoReq struct{ N int }
type echoResp struct{ From int }

// node is one real cluster whose socket transport is wrapped in faultnet.
type node struct {
	idx   int
	c     *gossip.Cluster
	ft    *faultnet.Transport
	id    gossip.NodeID
	count atomic.Int64
	// handled counts the msgEcho requests the handler ran for.
	handled atomic.Int64
	// arrivals receives the time each msgCount packet reached the handler.
	arrivals chan time.Time
	// slowEnded receives the error that ended a msgSlow stream on the serving side.
	slowEnded chan error
}

type fixture struct {
	t     *testing.T
	nodes []*node
	net   *faultnet.Network
}

// freeAddr returns a localhost address whose TCP and UDP ports are both free.
func freeAddr(t *testing.T) string {
	t.Helper()
	for range 50 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		u, err := net.ListenPacket("udp", addr)
		if err != nil {
			continue
		}
		u.Close()
		return addr
	}
	t.Fatal("no free port")
	return ""
}

func newFixture(t *testing.T, n int) *fixture {
	t.Helper()
	f := &fixture{t: t, net: faultnet.NewNetwork()}
	var seed string
	for i := range n {
		addr := freeAddr(t)
		cfg := gossip.DefaultConfig()
		cfg.BindAddr = addr
		cfg.AdvertiseAddr = addr
		cfg.MsgCodec = codec.NewJSONCodec()
		cfg.Logger = logger.NewNullLogger()
		cfg.SuspectTimeout = 500 * time.Millisecond
		cfg.DeadNodeTimeout = 2 * time.Second
		cfg.GossipInterval = 250 * time.Millisecond

		ft := faultnet.New(gossip.NewSocketTransport(cfg), int64(i+1))
		cfg.Transport = ft
		c, err := gossip.NewCluster(cfg)
		if err != nil {
			t.Fatalf("cluster on %s: %v", addr, err)
		}
		nd := &node{idx: i, c: c, ft: ft, id: c.LocalNode().ID,
			arrivals: make(chan time.Time, 1024), slowEnded: make(chan error, 8)}
		f.net.Add(nd.id, ft)
		f.net.SetAddress(nd.id, addr)
		f.register(nd)
		c.Start()
		t.Cleanup(c.Stop)
		if i == 0 {
			seed = addr
		} else if err := c.Join([]string{seed}); err != nil {
			t.Fatalf("node %d join: %v", i, err)
		}
		f.nodes = append(f.nodes, nd)
	}
	f.waitAllAlive(15 * time.Second)
	return f
}

func (f *fixture) register(n *node) {
	must := func(err error) {
		if err != nil {
			f.t.Fatal(err)
		}
	}
	must(n.c.HandleFuncWithReply(msgEcho, func(_ *gossip.Node, p *gossip.Packet) (interface{}, error) {
		var req echoReq
		if err := p.Unmarshal(&req); err != nil {
			return nil, err
		}
		n.handled.Add(1)
		return &echoResp{From: n.idx}, nil
	}))
	must(n.c.HandleFunc(msgCount, func(_ *gossip.Node, _ *gossip.Packet) error {
		n.count.Add(1)
		select {
		case n.arrivals <- time.Now():
		default:
		}
		return nil
	}))
	must(n.c.HandleStreamFunc(msgStream, func(_ *gossip.Node, _ *gossip.Packet, w io.Writer) error {
		_, err := w.Write([]byte("hello"))
		return err
	}))
	must(n.c.HandleStreamFunc(msgSlow, func(_ *gossip.Node, _ *gossip.Packet, w io.Writer) error {
		chunk := make([]byte, 64*1024) // a full frame, so each write reaches the reader
		for {
			if _, err := w.Write(chunk); err != nil {
				n.slowEnded <- err
				return err
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
}

func (f *fixture) ids(idx ...int) []gossip.NodeID {
	out := make([]gossip.NodeID, len(idx))
	for i, x := range idx {
		out[i] = f.nodes[x].id
	}
	return out
}

// state is how observer sees target; NodeRemoved when it has forgotten it.
func state(observer, target *node) gossip.NodeState {
	n := observer.c.GetNode(target.id)
	if n == nil {
		return gossip.NodeRemoved
	}
	return n.GetObservedState()
}

func (f *fixture) waitState(observer, target *node, want gossip.NodeState, limit time.Duration) {
	f.t.Helper()
	eventually(f.t, limit, fmt.Sprintf("n%d to see n%d as %v", observer.idx, target.idx, want), func() bool {
		s := state(observer, target)
		return s == want || (want == gossip.NodeDead && s == gossip.NodeRemoved)
	})
}

func (f *fixture) waitAllAlive(limit time.Duration) {
	f.t.Helper()
	for _, o := range f.nodes {
		for _, tg := range f.nodes {
			if o != tg {
				f.waitState(o, tg, gossip.NodeAlive, limit)
			}
		}
	}
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
	t.Fatalf("timed out after %v waiting for %s", limit, what)
}

// echo does a request/response from a to b.
func echo(ctx context.Context, a, b *node) (int, error) {
	var resp echoResp
	err := a.c.SendToWithResponseContext(ctx, a.c.GetNode(b.id), msgEcho, &echoReq{N: 1}, &resp)
	return resp.From, err
}

func mustEcho(t *testing.T, a, b *node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	from, err := echo(ctx, a, b)
	if err != nil {
		t.Fatalf("n%d -> n%d request failed: %v", a.idx, b.idx, err)
	}
	if from != b.idx {
		t.Fatalf("n%d -> n%d answered by n%d", a.idx, b.idx, from)
	}
}

func mustFail(t *testing.T, a, b *node, within time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	_, err := echo(ctx, a, b)
	if err == nil {
		t.Fatalf("n%d -> n%d request succeeded, wanted failure", a.idx, b.idx)
	}
	return err
}

// readStream opens a msgStream stream from a to b and reads it to the end.
func readStream(a, b *node) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := a.c.OpenStream(ctx, a.c.GetNode(b.id), msgStream, &echoReq{})
	if err != nil {
		return "", err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	return string(data), err
}

func mustStream(t *testing.T, a, b *node) {
	t.Helper()
	got, err := readStream(a, b)
	if err != nil || got != "hello" {
		t.Fatalf("n%d -> n%d stream: %q, %v", a.idx, b.idx, got, err)
	}
}

func send(a, b *node, n int) {
	for range n {
		a.c.SendTo(a.c.GetNode(b.id), msgCount, &echoReq{})
	}
}

func TestCutAndUncut(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1, n2 := f.nodes[0], f.nodes[1], f.nodes[2]

	n2.ft.Cut()
	f.waitState(n0, n2, gossip.NodeDead, 15*time.Second)
	f.waitState(n1, n2, gossip.NodeDead, 15*time.Second)
	if state(n0, n1) != gossip.NodeAlive {
		t.Fatalf("n0 sees n1 as %v while only n2 is cut", state(n0, n1))
	}
	// The cut node cannot send or receive.
	if err := mustFail(t, n2, n0, 2*time.Second); !errors.Is(err, faultnet.ErrCut) {
		t.Fatalf("cut node's request failed with %v, want ErrCut", err)
	}
	mustFail(t, n0, n2, 2*time.Second)
	if _, err := readStream(n2, n0); !errors.Is(err, faultnet.ErrCut) {
		t.Fatalf("cut node's stream: %v, want ErrCut", err)
	}

	n2.ft.Uncut()
	f.waitAllAlive(15 * time.Second)
	mustEcho(t, n0, n2)
	mustEcho(t, n2, n1)
	mustStream(t, n2, n0)
}

func TestSplit(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1, n2 := f.nodes[0], f.nodes[1], f.nodes[2]

	f.net.Split(f.ids(0, 1), f.ids(2))
	// Each side sees the other dead.
	f.waitState(n2, n0, gossip.NodeDead, 15*time.Second)
	f.waitState(n2, n1, gossip.NodeDead, 15*time.Second)
	f.waitState(n0, n2, gossip.NodeDead, 15*time.Second)
	f.waitState(n1, n2, gossip.NodeDead, 15*time.Second)
	// Within a group the nodes still talk.
	if state(n0, n1) != gossip.NodeAlive || state(n1, n0) != gossip.NodeAlive {
		t.Fatalf("nodes in the same group lost each other: %v %v", state(n0, n1), state(n1, n0))
	}
	mustEcho(t, n0, n1)
	mustFail(t, n0, n2, 2*time.Second)
	mustFail(t, n2, n1, 2*time.Second)

	f.net.Heal()
	f.waitAllAlive(15 * time.Second)
	mustEcho(t, n0, n2)
	mustEcho(t, n2, n1)
}

func TestOneWayBlock(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1 := f.nodes[0], f.nodes[1]

	f.net.OneWay(n0.id, n1.id) // n0's packets to n1 are dropped

	// Request/response.
	if err := mustFail(t, n0, n1, 2*time.Second); !errors.Is(err, faultnet.ErrBlocked) {
		t.Fatalf("blocked request failed with %v, want ErrBlocked", err)
	}
	mustEcho(t, n1, n0) // the other direction is untouched

	// Streams.
	if _, err := readStream(n0, n1); !errors.Is(err, faultnet.ErrBlocked) {
		t.Fatalf("blocked stream open: %v, want ErrBlocked", err)
	}
	mustStream(t, n1, n0)

	// Fire-and-forget packets.
	send(n0, n1, 5)
	send(n1, n0, 5)
	eventually(t, 5*time.Second, "n1's packets to reach n0", func() bool { return n0.count.Load() == 5 })
	time.Sleep(300 * time.Millisecond)
	if got := n1.count.Load(); got != 0 {
		t.Fatalf("n1 received %d packets through a one-way block", got)
	}

	n0.ft.Unblock(n1.id)
	mustEcho(t, n0, n1)
	mustStream(t, n0, n1)
}

func TestBlockFrom(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1 := f.nodes[0], f.nodes[1]

	n0.ft.BlockFrom(n1.id) // n0 is deaf to n1

	// n1's requests are discarded at n0 and time out; n0's own requests work.
	mustFail(t, n1, n0, 1*time.Second)
	mustEcho(t, n0, n1)
	// n1's stream request is refused by n0.
	if got, err := readStream(n1, n0); err == nil {
		t.Fatalf("stream from a blocked peer succeeded: %q", got)
	}
	mustStream(t, n0, n1)
	// One-way packets from n1 are dropped, packets from n0 to n1 flow.
	send(n1, n0, 5)
	send(n0, n1, 5)
	eventually(t, 5*time.Second, "n0's packets to reach n1", func() bool { return n1.count.Load() == 5 })
	time.Sleep(300 * time.Millisecond)
	if got := n0.count.Load(); got != 0 {
		t.Fatalf("n0 received %d packets from a peer it blocks", got)
	}

	n0.ft.UnblockFrom(n1.id)
	mustEcho(t, n1, n0)
}

func TestInProgressStreamAborted(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1 := f.nodes[0], f.nodes[1]

	open := func() io.ReadCloser {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		r, err := n0.c.OpenStream(ctx, n0.c.GetNode(n1.id), msgSlow, &echoReq{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		buf := make([]byte, 1)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("stream did not start: %v", err)
		}
		return r
	}
	drain := func(r io.Reader) error {
		done := make(chan error, 1)
		go func() { _, err := io.Copy(io.Discard, r); done <- err }()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("stream was not aborted")
			return nil
		}
	}

	// The opener blocks the peer: its read fails.
	r := open()
	n0.ft.Block(n1.id)
	if err := drain(r); !errors.Is(err, faultnet.ErrBlocked) {
		t.Fatalf("aborted stream ended with %v, want ErrBlocked", err)
	}
	n0.ft.Unblock(n1.id)
	select {
	case <-n1.slowEnded: // the serving handler's writes failed after the abort closed the connection
	case <-time.After(5 * time.Second):
		t.Fatal("serving handler never ended")
	}

	// The server stops hearing the opener: the handler's writes fail and the stream ends in error.
	r = open()
	n1.ft.BlockFrom(n0.id)
	if err := drain(r); err == nil || err == io.EOF {
		t.Fatalf("stream ended with %v, want an error", err)
	}
	select {
	case err := <-n1.slowEnded:
		if !errors.Is(err, faultnet.ErrBlocked) {
			t.Fatalf("handler's write failed with %v, want ErrBlocked", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serving handler never ended")
	}
}

func TestDelay(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1, n2 := f.nodes[0], f.nodes[1], f.nodes[2]

	const min, max = 400 * time.Millisecond, 600 * time.Millisecond
	n0.ft.Delay(n1.id, min, max)

	// Request/response is delayed, and only towards n1.
	start := time.Now()
	mustEcho(t, n0, n1)
	if took := time.Since(start); took < min || took > max+2*time.Second {
		t.Fatalf("delayed request took %v, want between %v and %v", took, min, max)
	}
	start = time.Now()
	mustEcho(t, n0, n2)
	if took := time.Since(start); took >= min {
		t.Fatalf("undelayed request took %v", took)
	}
	start = time.Now()
	mustEcho(t, n1, n0) // the other direction is not delayed
	if took := time.Since(start); took >= min {
		t.Fatalf("reverse request took %v", took)
	}
	// Streams too.
	start = time.Now()
	mustStream(t, n0, n1)
	if took := time.Since(start); took < min {
		t.Fatalf("delayed stream took %v", took)
	}

	// One-way packets arrive late and do not hold up sends to others.
	sent := time.Now()
	send(n0, n1, 1)
	send(n0, n2, 1)
	eventually(t, 5*time.Second, "n2's packet", func() bool { return n2.count.Load() == 1 })
	if since := time.Since(sent); since >= min {
		t.Fatalf("send to n2 held up %v by the delay to n1", since)
	}
	eventually(t, 5*time.Second, "n1's delayed packet", func() bool { return n1.count.Load() == 1 })
	if arrived := <-n1.arrivals; arrived.Sub(sent) < min {
		t.Fatalf("delayed packet arrived after %v, want at least %v", arrived.Sub(sent), min)
	}

	// Delay(to, 0, 0) removes it.
	n0.ft.Delay(n1.id, 0, 0)
	start = time.Now()
	mustEcho(t, n0, n1)
	if took := time.Since(start); took >= min {
		t.Fatalf("request still delayed: %v", took)
	}
}

func TestDelaySpreadsPackets(t *testing.T) {
	f := newFixture(t, 2)
	n0, n1 := f.nodes[0], f.nodes[1]
	// A wide random delay on 40 sequential packets spreads them out (and so reorders them); with a fixed seed
	// the delays are the same on every run. The count handler cannot see order, so use arrival times only to
	// check that the packets were spread out rather than delivered in one burst.
	n0.ft.Delay(n1.id, 0, 600*time.Millisecond)
	const total = 40
	start := time.Now()
	send(n0, n1, total)
	eventually(t, 10*time.Second, "all delayed packets", func() bool { return n1.count.Load() == total })
	var first, last time.Time
	for i := range total {
		a := <-n1.arrivals
		if i == 0 || a.Before(first) {
			first = a
		}
		if a.After(last) {
			last = a
		}
	}
	if spread := last.Sub(first); spread < 200*time.Millisecond {
		t.Fatalf("packets arrived within %v of each other (started %v ago), delays not applied", spread, time.Since(start))
	}
}

func TestDropRate(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1 := f.nodes[0], f.nodes[1]

	n0.ft.DropRate(n1.id, 1.0)
	err := mustFail(t, n0, n1, 2*time.Second)
	var te interface{ Timeout() bool }
	if !errors.Is(err, faultnet.ErrDropped) || !errors.As(err, &te) || !te.Timeout() {
		t.Fatalf("dropped request failed with %v, want a timeout-like ErrDropped", err)
	}
	if _, err := readStream(n0, n1); !errors.Is(err, faultnet.ErrDropped) {
		t.Fatalf("dropped stream open: %v", err)
	}
	mustEcho(t, n1, n0) // other direction unaffected
	send(n0, n1, 20)
	time.Sleep(500 * time.Millisecond)
	if got := n1.count.Load(); got != 0 {
		t.Fatalf("%d packets got through a drop rate of 1", got)
	}

	n0.ft.DropRate(n1.id, 0)
	mustEcho(t, n0, n1)
	mustStream(t, n0, n1)
	send(n0, n1, 20)
	eventually(t, 5*time.Second, "all packets with a drop rate of 0", func() bool { return n1.count.Load() == 20 })

	// A fraction: roughly half of 100.
	n1.count.Store(0)
	n0.ft.DropRate(n1.id, 0.5)
	send(n0, n1, 100)
	time.Sleep(time.Second)
	if got := n1.count.Load(); got < 20 || got > 80 {
		t.Fatalf("%d of 100 packets delivered at a drop rate of 0.5", got)
	}
}

func TestDropReplyRate(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1, n2 := f.nodes[0], f.nodes[1], f.nodes[2]

	n0.ft.DropReplyRate(n1.id, 1.0)
	before := n1.handled.Load()
	err := mustFail(t, n0, n1, 2*time.Second)
	var te interface{ Timeout() bool }
	if !errors.Is(err, faultnet.ErrDropped) || !errors.As(err, &te) || !te.Timeout() {
		t.Fatalf("request with a lost reply failed with %v, want a timeout-like ErrDropped", err)
	}
	if got := n1.handled.Load() - before; got != 1 {
		t.Fatalf("handler ran %d times for a request whose reply was lost, want 1", got)
	}
	// The context variant of the call is the one the echo helper uses; the plain one goes through the same filter.
	jc := codec.NewJSONCodec()
	body, err := jc.Marshal(&echoReq{N: 2})
	if err != nil {
		t.Fatal(err)
	}
	pkt := gossip.NewPacket()
	defer pkt.Release()
	pkt.MessageType = msgEcho
	pkt.SenderID = n0.id
	target := n1.id
	pkt.TargetNodeID = &target
	pkt.MessageID = gossip.MessageID(hlc.Now())
	pkt.TTL = 1
	pkt.SetCodec(jc)
	pkt.SetPayload(body)
	if reply, err := n0.ft.SendWithReply(n0.c.GetNode(n1.id), pkt); !errors.Is(err, faultnet.ErrDropped) || reply != nil {
		t.Fatalf("SendWithReply with a lost reply: %v, %v", reply, err)
	}
	if got := n1.handled.Load() - before; got != 2 {
		t.Fatalf("handler ran %d times, want 2", got)
	}

	// Only the replies to this node's requests to that peer are lost.
	mustEcho(t, n1, n0)
	mustEcho(t, n0, n2)
	if _, err := readStream(n0, n1); err != nil {
		t.Fatalf("stream affected by DropReplyRate: %v", err)
	}

	n0.ft.DropReplyRate(n1.id, 0)
	mustEcho(t, n0, n1)

	// The Network helper, and Heal.
	f.net.DropReply(n0.id, n2.id, 1.0)
	mustFail(t, n0, n2, 2*time.Second)
	f.net.Heal()
	mustEcho(t, n0, n2)

	// A fraction: roughly half of 100 replies are lost, every request is handled.
	n1.handled.Store(0)
	n0.ft.DropReplyRate(n1.id, 0.5)
	failed := 0
	for range 100 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if _, err := echo(ctx, n0, n1); err != nil {
			failed++
		}
		cancel()
	}
	if failed < 20 || failed > 80 {
		t.Fatalf("%d of 100 replies lost at a rate of 0.5", failed)
	}
	if got := n1.handled.Load(); got != 100 {
		t.Fatalf("handler ran %d times, want all 100 requests handled", got)
	}
}

func TestDuplicate(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1 := f.nodes[0], f.nodes[1]

	n0.ft.Duplicate(n1.id, 1.0)
	send(n0, n1, 5)
	eventually(t, 5*time.Second, "every packet twice", func() bool { return n1.count.Load() == 10 })
	time.Sleep(300 * time.Millisecond)
	if got := n1.count.Load(); got != 10 {
		t.Fatalf("%d deliveries of 5 packets, want exactly 10", got)
	}

	n0.ft.Duplicate(n1.id, 0)
	send(n0, n1, 5)
	eventually(t, 5*time.Second, "5 more", func() bool { return n1.count.Load() == 15 })
	time.Sleep(300 * time.Millisecond)
	if got := n1.count.Load(); got != 15 {
		t.Fatalf("%d deliveries, want 15 once duplication is off", got)
	}
}

func TestHeal(t *testing.T) {
	f := newFixture(t, 3)
	n0, n1, n2 := f.nodes[0], f.nodes[1], f.nodes[2]

	f.net.Isolate(n2.id)
	f.net.OneWay(n0.id, n1.id)
	f.net.Delay(n1.id, n0.id, time.Second, 2*time.Second)
	f.net.DropRate(n1.id, n2.id, 1)
	f.net.Duplicate(n0.id, n2.id, 1)
	n1.ft.BlockFrom(n2.id)
	mustFail(t, n0, n1, time.Second)
	mustFail(t, n0, n2, time.Second)

	f.net.Heal()
	f.waitAllAlive(15 * time.Second)
	start := time.Now()
	for _, a := range f.nodes {
		for _, b := range f.nodes {
			if a != b {
				mustEcho(t, a, b)
				mustStream(t, a, b)
			}
		}
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("healed network still slow: %v", took)
	}
	send(n0, n2, 3)
	eventually(t, 5*time.Second, "exactly 3 packets after heal", func() bool { return n2.count.Load() >= 3 })
	time.Sleep(300 * time.Millisecond)
	if got := n2.count.Load(); got != 3 {
		t.Fatalf("%d deliveries after heal, want 3 (duplication not cleared?)", got)
	}
}

// ---------------------------------------------------------------------------
// Unit tests with a fake inner transport
// ---------------------------------------------------------------------------

type fakeInner struct {
	mu   sync.Mutex
	sent []gossip.MessageID
	ch   chan *gossip.Packet
}

func newFake() *fakeInner { return &fakeInner{ch: make(chan *gossip.Packet, 16)} }

func (f *fakeInner) Name() string                                 { return "fake" }
func (f *fakeInner) Start(context.Context, *sync.WaitGroup) error { return nil }
func (f *fakeInner) PacketChannel() chan *gossip.Packet           { return f.ch }
func (f *fakeInner) SendWithReply(*gossip.Node, *gossip.Packet) (*gossip.Packet, error) {
	return nil, errors.New("not used")
}
func (f *fakeInner) Send(_ gossip.TransportType, _ *gossip.Node, p *gossip.Packet) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, p.MessageID)
	return nil
}

func TestSeedReplays(t *testing.T) {
	run := func(seed int64) []bool {
		inner := newFake()
		ft := faultnet.New(inner, seed)
		to := gossip.NodeID{1}
		ft.DropRate(to, 0.5)
		node := &gossip.Node{ID: to}
		var delivered []bool
		for i := range 200 {
			p := gossip.NewPacket()
			p.MessageID = gossip.MessageID(i + 1)
			before := len(inner.sent)
			if err := ft.Send(gossip.TransportBestEffort, node, p); err != nil {
				t.Fatal(err)
			}
			delivered = append(delivered, len(inner.sent) > before)
			p.Release()
		}
		return delivered
	}
	a, b, c := run(42), run(42), run(43)
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatal("same seed gave different drops")
	}
	if fmt.Sprint(a) == fmt.Sprint(c) {
		t.Fatal("different seeds gave identical drops")
	}
	if n := strings.Count(fmt.Sprint(a), "true"); n < 60 || n > 140 {
		t.Fatalf("%d of 200 delivered at 0.5", n)
	}
}

func TestDuplicateCopyHasFreshID(t *testing.T) {
	inner := newFake()
	ft := faultnet.New(inner, 1)
	to := gossip.NodeID{1}
	ft.Duplicate(to, 1)
	p := gossip.NewPacket()
	p.MessageID = 7
	if err := ft.Send(gossip.TransportBestEffort, &gossip.Node{ID: to}, p); err != nil {
		t.Fatal(err)
	}
	if len(inner.sent) != 2 || inner.sent[0] != 7 || inner.sent[1] == 7 {
		t.Fatalf("sent %v, want the original then a copy with another id", inner.sent)
	}
	p.Release()
}

// A join is sent to an address before the peer's ID is known: a Block of the node recorded at that address covers it, as
// a real split would.
func TestBlockCoversSendsToTheNodesAddress(t *testing.T) {
	inner := newFake()
	ft := faultnet.New(inner, 1)
	nw := faultnet.NewNetwork()
	to := gossip.NodeID{1}
	nw.Add(gossip.NodeID{9}, ft)
	nw.SetAddress(to, "127.0.0.1:7001")
	byAddr := gossip.NewTestNode(gossip.EmptyNodeID, "127.0.0.1:7001", nil)
	elsewhere := gossip.NewTestNode(gossip.EmptyNodeID, "127.0.0.1:7002", nil)

	ft.Block(to)
	p := gossip.NewPacket()
	defer p.Release()
	if err := ft.Send(gossip.TransportReliable, byAddr, p); !errors.Is(err, faultnet.ErrBlocked) {
		t.Fatalf("send to the blocked node's address = %v, want ErrBlocked", err)
	}
	if _, err := ft.SendWithReply(byAddr, p); !errors.Is(err, faultnet.ErrBlocked) {
		t.Fatalf("request to the blocked node's address = %v, want ErrBlocked", err)
	}
	if err := ft.Send(gossip.TransportReliable, elsewhere, p); err != nil || len(inner.sent) != 1 {
		t.Fatalf("send to another address = %v (%d sent), want it through", err, len(inner.sent))
	}
	ft.Unblock(to)
	if err := ft.Send(gossip.TransportReliable, byAddr, p); err != nil || len(inner.sent) != 2 {
		t.Fatalf("send after Unblock = %v (%d sent), want it through", err, len(inner.sent))
	}
}
