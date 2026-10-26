// Package faultnet provides a fault-injecting transport wrapper for tests that
// destroy-test protocols running over real gossip clusters on localhost.
//
// A Transport wraps the cluster's real (socket) transport. Faults are changed at
// runtime and apply per peer, so a test can cut a node off, split a cluster,
// make a link one-way, delay, drop or duplicate traffic, and heal it all again
// while the cluster code runs unchanged. A Network holds the transports of many
// nodes and offers cluster-wide operations (Split, OneWay, Isolate, Heal).
//
// # What a fault applies to
//
// Fire-and-forget packets (Send) are filtered in both directions: outbound by
// Block, DropRate, Delay and Duplicate on the sender, inbound by BlockFrom on
// the receiver. A request/response exchange (SendWithReply, SendWithReplyContext)
// and a stream (OpenStream) belong to the node that opens them and are
// filtered on their request leg only: the opener's outbound rules toward the
// peer (Block, DropRate, Delay) and the server's inbound rule from the opener
// (BlockFrom). The reply travels back on the opener's own connection and cannot
// be intercepted on the wire, so it is not filtered separately; DropReplyRate
// instead makes the opener discard a reply it has received (the request was
// handled, the answer is lost). Hence a.Block(b) fails
// a's requests to b while b's requests to a are served normally, and
// a.BlockFrom(b) makes a ignore whatever b sends it (b's requests time out)
// while a's requests to b still work. Block both ways (see Network.Split) to
// sever a link completely.
//
// BlockFrom matches Packet.SenderID, which is the original author of a packet,
// not the hop that delivered it: a packet authored by b and relayed to a by a
// third node is dropped too.
//
// Duplicate delivers a copy of the packet with a fresh MessageID. A byte
// identical copy would be discarded by the cluster's own duplicate suppression
// before any handler saw it, which is not what a replication protocol's retry
// or network-level duplication looks like from above.
package faultnet

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/hlc"
)

var (
	// ErrCut is returned for traffic attempted while the node is cut (for a
	// request or a stream open, wrapped as a failed dial: see admit).
	ErrCut = errors.New("faultnet: node cut (simulated crash)")
	// ErrBlocked is returned for traffic to a blocked peer (for a request or a
	// stream open, wrapped as a failed dial), or for a stream aborted because its
	// peer was blocked.
	ErrBlocked = errors.New("faultnet: peer blocked")
	// ErrDropped is returned when DropRate discards a request/response exchange or
	// a stream request. It reports Timeout() as true, like a request that
	// never got an answer.
	ErrDropped error = droppedError{}
)

type droppedError struct{}

func (droppedError) Error() string   { return "faultnet: packet dropped" }
func (droppedError) Timeout() bool   { return true }
func (droppedError) Temporary() bool { return true }

// streamTransport mirrors the unexported interface of the gossip package that
// a transport must implement to carry streams.
type streamTransport interface {
	OpenStream(ctx context.Context, node *gossip.Node, packet *gossip.Packet) (io.ReadCloser, error)
	SetStreamHandler(handler func(packet *gossip.Packet, w io.Writer) error)
}

// faults is what is configured for one peer.
type faults struct {
	blockTo, blockFrom bool
	delayMin, delayMax time.Duration
	drop, dup          float64
	dropReply          float64
}

func (f faults) zero() bool { return f == faults{} }

// Transport wraps a gossip.Transport and injects faults. It implements
// gossip.Transport, gossip.ContextTransport and the stream methods.
type Transport struct {
	inner  gossip.Transport
	ictx   gossip.ContextTransport // nil when inner is not one
	stream streamTransport         // nil when inner cannot carry streams
	ch     chan *gossip.Packet
	cut    atomic.Bool
	// network is the Network the transport is registered in (nil if none): it names the node at an address (peer).
	network atomic.Pointer[Network]

	mu      sync.Mutex
	rng     *rand.Rand
	peers   map[gossip.NodeID]*faults
	streams map[*faultStream]struct{}
	ctx     context.Context // the transport's lifetime, set by Start
}

// New wraps inner. The seed makes the random choices (delays, drops,
// duplicates) replay: with the same seed and the same sequence of packets the
// same ones are affected.
func New(inner gossip.Transport, seed int64) *Transport {
	t := &Transport{
		inner:   inner,
		ch:      make(chan *gossip.Packet, 256),
		rng:     rand.New(rand.NewPCG(uint64(seed), 0x9e3779b97f4a7c15)),
		peers:   make(map[gossip.NodeID]*faults),
		streams: make(map[*faultStream]struct{}),
		ctx:     context.Background(),
	}
	t.ictx, _ = inner.(gossip.ContextTransport)
	t.stream, _ = inner.(streamTransport)
	return t
}

// Inner returns the wrapped transport.
func (t *Transport) Inner() gossip.Transport { return t.inner }

func (t *Transport) Name() string { return "faultnet" }

func (t *Transport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	if err := t.inner.Start(ctx, wg); err != nil {
		return err
	}
	t.mu.Lock()
	t.ctx = ctx
	t.mu.Unlock()

	wg.Add(1)
	go func() {
		defer wg.Done()
		src := t.inner.PacketChannel()
		for {
			select {
			case <-ctx.Done():
				return
			case p, ok := <-src:
				if !ok {
					return
				}
				if t.inboundBlocked(p.SenderID) {
					p.Release()
					continue
				}
				select {
				case t.ch <- p:
				case <-ctx.Done():
					p.Release()
					return
				}
			}
		}
	}()
	return nil
}

func (t *Transport) PacketChannel() chan *gossip.Packet { return t.ch }

func (t *Transport) lifetime() context.Context {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ctx
}

// ---------------------------------------------------------------------------
// Fault configuration
// ---------------------------------------------------------------------------

// update changes the faults for one peer under the lock; update may return
// true to ask for open streams to that peer to be aborted.
func (t *Transport) update(id gossip.NodeID, fn func(f *faults) (abort bool)) {
	t.mu.Lock()
	f := t.peers[id]
	if f == nil {
		f = &faults{}
	}
	abort := fn(f)
	if f.zero() {
		delete(t.peers, id)
	} else {
		t.peers[id] = f
	}
	var doomed []*faultStream
	if abort {
		doomed = t.collect(func(s *faultStream) bool { return s.peer == id })
	}
	t.mu.Unlock()
	abortAll(doomed)
}

func (t *Transport) get(id gossip.NodeID) faults {
	t.mu.Lock()
	defer t.mu.Unlock()
	if f := t.peers[id]; f != nil {
		return *f
	}
	return faults{}
}

// Cut silences the node both ways, like a crashed process that announced
// nothing: every send fails, nothing is delivered inbound and open streams
// are aborted. The cluster keeps running and believes it is healthy.
func (t *Transport) Cut() {
	t.cut.Store(true)
	t.mu.Lock()
	doomed := t.collect(func(*faultStream) bool { return true })
	t.mu.Unlock()
	abortAll(doomed)
}

// Uncut ends Cut.
func (t *Transport) Uncut() { t.cut.Store(false) }

// IsCut reports whether the node is cut.
func (t *Transport) IsCut() bool { return t.cut.Load() }

// Block drops this node's packets to the peer. Requests and streams it opens
// to the peer fail and streams open to the peer are aborted. Traffic from the
// peer still flows (see BlockFrom).
func (t *Transport) Block(to gossip.NodeID) {
	t.update(to, func(f *faults) bool { f.blockTo = true; return true })
}

// Unblock ends Block.
func (t *Transport) Unblock(to gossip.NodeID) {
	t.update(to, func(f *faults) bool { f.blockTo = false; return false })
}

// BlockFrom discards everything arriving from the peer, whatever it is
// (packets, requests, stream requests). Replies to this node's own requests
// are not affected (see the package comment).
func (t *Transport) BlockFrom(from gossip.NodeID) {
	t.update(from, func(f *faults) bool { f.blockFrom = true; return false })
}

// UnblockFrom ends BlockFrom.
func (t *Transport) UnblockFrom(from gossip.NodeID) {
	t.update(from, func(f *faults) bool { f.blockFrom = false; return false })
}

// Delay holds each packet to the peer back by a random duration in [min, max].
// Fire-and-forget sends are delayed asynchronously, so they do not block other
// sends and may overtake each other; a request or stream open waits in its
// caller before it goes out. Delay(to, 0, 0) removes the delay.
func (t *Transport) Delay(to gossip.NodeID, min, max time.Duration) {
	if min < 0 {
		min = 0
	}
	if max < min {
		min, max = max, min
	}
	if max < 0 {
		max = 0
	}
	t.update(to, func(f *faults) bool { f.delayMin, f.delayMax = min, max; return false })
}

// DropRate drops the fraction p (0 to 1) of packets to the peer. Fire-and-forget
// packets vanish silently; a request/response exchange or stream open fails
// with ErrDropped.
func (t *Transport) DropRate(to gossip.NodeID, p float64) {
	t.update(to, func(f *faults) bool { f.drop = clamp(p); return false })
}

// DropReplyRate loses the fraction p (0 to 1) of the replies to this node's
// request/response exchanges with the peer: the request is delivered and handled
// by the peer, then the reply is discarded and the caller gets ErrDropped (a
// timeout-like error). It simulates "the peer did the work but the answer was
// lost". Streams and fire-and-forget packets are not affected (see DropRate).
func (t *Transport) DropReplyRate(to gossip.NodeID, p float64) {
	t.update(to, func(f *faults) bool { f.dropReply = clamp(p); return false })
}

// Duplicate delivers the fraction p (0 to 1) of fire-and-forget packets to the
// peer twice (the copy carries a fresh MessageID, see the package comment).
func (t *Transport) Duplicate(to gossip.NodeID, p float64) {
	t.update(to, func(f *faults) bool { f.dup = clamp(p); return false })
}

// Heal clears every fault on this node: the cut, all blocks, delays, drop,
// reply drop and duplicate rates.
func (t *Transport) Heal() {
	t.cut.Store(false)
	t.mu.Lock()
	clear(t.peers)
	t.mu.Unlock()
}

func clamp(p float64) float64 { return min(max(p, 0), 1) }

// outbound reports why traffic to the peer must not go out, nil when it may.
func (t *Transport) outbound(to gossip.NodeID) error {
	if t.cut.Load() {
		return ErrCut
	}
	if t.get(to).blockTo {
		return ErrBlocked
	}
	return nil
}

// inboundBlocked reports whether traffic from the peer is discarded.
func (t *Transport) inboundBlocked(from gossip.NodeID) bool {
	return t.cut.Load() || t.get(from).blockFrom
}

func (t *Transport) roll(p float64) bool {
	switch {
	case p <= 0:
		return false
	case p >= 1:
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rng.Float64() < p
}

func (t *Transport) pickDelay(f faults) time.Duration {
	if f.delayMax <= 0 {
		return 0
	}
	if f.delayMax == f.delayMin {
		return f.delayMin
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return f.delayMin + time.Duration(t.rng.Int64N(int64(f.delayMax-f.delayMin)+1))
}

// wait sleeps d, ending early with an error when ctx or the transport ends.
func (t *Transport) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.lifetime().Done():
		return context.Canceled
	}
}

// peer returns the node a send goes to: the node's ID or, for a send to an address alone (a join, before the peer's ID is
// known), the node registered in the Network at that address (Network.SetAddress). The faults aimed at a node then also
// cover what is sent to its address, as they would on a real network: otherwise a join would cross a Block, and a
// simulated split would let joins through.
func (t *Transport) peer(node *gossip.Node) gossip.NodeID {
	if node.ID != gossip.EmptyNodeID {
		return node.ID
	}
	if n := t.network.Load(); n != nil {
		if id, ok := n.idAt(node.AdvertisedAddr()); ok {
			return id
		}
	}
	return node.ID
}

// admit applies the outbound faults to a request/response exchange or stream
// open to the peer, waiting out its delay. It returns the error to fail with.
//
// A cut or blocked request never left this node, as when a real connection cannot be made: its error (ErrCut or
// ErrBlocked) comes wrapped in a *net.OpError whose Op is "dial", which is how callers tell a request that certainly was
// not delivered from one that may have been. A dropped request (ErrDropped) is a loss in transit, which the sender cannot
// tell from a lost reply.
func (t *Transport) admit(ctx context.Context, to gossip.NodeID) error {
	if err := t.outbound(to); err != nil {
		return notSent(err)
	}
	f := t.get(to)
	if t.roll(f.drop) {
		return ErrDropped
	}
	if err := t.wait(ctx, t.pickDelay(f)); err != nil {
		return err
	}
	if err := t.outbound(to); err != nil { // blocked while it waited
		return notSent(err)
	}
	return nil
}

// notSent wraps the error of a request that never left this node as a failed dial.
func notSent(err error) error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: err}
}

// ---------------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------------

func (t *Transport) Send(tt gossip.TransportType, node *gossip.Node, packet *gossip.Packet) error {
	to := t.peer(node)
	if err := t.outbound(to); err != nil {
		return err
	}
	f := t.get(to)
	if t.roll(f.drop) {
		return nil
	}
	copies := 1
	if t.roll(f.dup) {
		copies = 2
	}
	for i := range copies {
		p := packet
		if i == 1 {
			p = clonePacket(packet)
		}
		if d := t.pickDelay(f); d > 0 {
			t.sendLater(d, tt, node, p, i == 1)
			continue
		}
		err := t.inner.Send(tt, node, p)
		if i == 1 {
			p.Release()
		} else if err != nil {
			return err
		}
	}
	return nil
}

// sendLater sends p after d without blocking the caller. The caller keeps its
// reference to the original packet; the delayed send takes its own (owned says
// p is a copy made here, which already carries one).
func (t *Transport) sendLater(d time.Duration, tt gossip.TransportType, node *gossip.Node, p *gossip.Packet, owned bool) {
	if !owned {
		p.AddRef()
	}
	go func() {
		defer p.Release()
		if t.wait(t.lifetime(), d) != nil {
			return
		}
		if t.outbound(t.peer(node)) != nil {
			return // cut or blocked while in flight
		}
		t.inner.Send(tt, node, p)
	}()
}

// clonePacket copies a packet under a fresh MessageID.
func clonePacket(p *gossip.Packet) *gossip.Packet {
	c := gossip.NewPacket()
	c.MessageType = p.MessageType
	c.SenderID = p.SenderID
	if p.TargetNodeID != nil {
		id := *p.TargetNodeID
		c.TargetNodeID = &id
	}
	if p.Tag != nil {
		tag := *p.Tag
		c.Tag = &tag
	}
	c.MessageID = gossip.MessageID(hlc.Now())
	c.TTL = p.TTL
	c.SetCodec(p.Codec())
	c.SetPayload(p.Payload())
	return c
}

func (t *Transport) SendWithReply(node *gossip.Node, packet *gossip.Packet) (*gossip.Packet, error) {
	to := t.peer(node)
	if err := t.admit(t.lifetime(), to); err != nil {
		return nil, err
	}
	reply, err := t.inner.SendWithReply(node, packet)
	return t.maybeDropReply(to, reply, err)
}

// maybeDropReply discards a successful reply with the peer's DropReplyRate.
func (t *Transport) maybeDropReply(to gossip.NodeID, reply *gossip.Packet, err error) (*gossip.Packet, error) {
	if err != nil || !t.roll(t.get(to).dropReply) {
		return reply, err
	}
	if reply != nil {
		reply.Release()
	}
	return nil, ErrDropped
}

// SendWithReplyContext is bounded by ctx when the inner transport is a
// gossip.ContextTransport (the socket transport is); otherwise ctx is only
// checked before the exchange starts.
func (t *Transport) SendWithReplyContext(ctx context.Context, node *gossip.Node, packet *gossip.Packet) (*gossip.Packet, error) {
	to := t.peer(node)
	if err := t.admit(ctx, to); err != nil {
		return nil, err
	}
	if t.ictx != nil {
		reply, err := t.ictx.SendWithReplyContext(ctx, node, packet)
		return t.maybeDropReply(to, reply, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reply, err := t.inner.SendWithReply(node, packet)
	return t.maybeDropReply(to, reply, err)
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

// faultStream is a stream this node opened, tracked so a fault can abort it.
type faultStream struct {
	t       *Transport
	peer    gossip.NodeID
	rc      io.ReadCloser
	aborted atomic.Bool
}

func (s *faultStream) Read(p []byte) (int, error) {
	if s.aborted.Load() {
		return 0, ErrBlocked
	}
	n, err := s.rc.Read(p)
	if err != nil && s.aborted.Load() {
		return n, ErrBlocked
	}
	return n, err
}

func (s *faultStream) Close() error {
	s.t.mu.Lock()
	delete(s.t.streams, s)
	s.t.mu.Unlock()
	return s.rc.Close()
}

// collect removes and returns the tracked streams matching sel. Hold t.mu.
func (t *Transport) collect(sel func(*faultStream) bool) []*faultStream {
	var out []*faultStream
	for s := range t.streams {
		if sel(s) {
			out = append(out, s)
			delete(t.streams, s)
		}
	}
	return out
}

func abortAll(streams []*faultStream) {
	for _, s := range streams {
		s.aborted.Store(true)
		s.rc.Close()
	}
}

// OpenStream fails with ErrCut, ErrBlocked or ErrDropped when a fault applies
// to the peer, and waits out Delay first. A stream open when the peer is
// blocked, or the node cut, is aborted: its reads fail with ErrBlocked.
func (t *Transport) OpenStream(ctx context.Context, node *gossip.Node, packet *gossip.Packet) (io.ReadCloser, error) {
	if t.stream == nil {
		return nil, gossip.ErrStreamsUnsupported
	}
	to := t.peer(node)
	if err := t.admit(ctx, to); err != nil {
		return nil, err
	}
	rc, err := t.stream.OpenStream(ctx, node, packet)
	if err != nil {
		return nil, err
	}
	s := &faultStream{t: t, peer: to, rc: rc}
	t.mu.Lock()
	f := t.peers[to]
	blocked := t.cut.Load() || (f != nil && f.blockTo)
	if !blocked {
		t.streams[s] = struct{}{}
	}
	t.mu.Unlock()
	if blocked { // blocked between the check and now
		rc.Close()
		return nil, ErrBlocked
	}
	return s, nil
}

// SetStreamHandler installs the handler for streams opened by peers. A stream
// request from a peer this node does not hear (cut, or BlockFrom) is refused,
// and a stream being served is aborted by failing the handler's writes once
// that happens.
func (t *Transport) SetStreamHandler(handler func(packet *gossip.Packet, w io.Writer) error) {
	if t.stream == nil {
		return
	}
	t.stream.SetStreamHandler(func(packet *gossip.Packet, w io.Writer) error {
		from := packet.SenderID
		if t.inboundBlocked(from) {
			packet.Release()
			return ErrBlocked
		}
		return handler(packet, &faultWriter{t: t, from: from, w: w})
	})
}

type faultWriter struct {
	t    *Transport
	from gossip.NodeID
	w    io.Writer
}

func (w *faultWriter) Write(p []byte) (int, error) {
	if w.t.inboundBlocked(w.from) {
		return 0, ErrBlocked
	}
	return w.w.Write(p)
}
