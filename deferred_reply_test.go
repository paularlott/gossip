package gossip

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/logger"
	"github.com/stretchr/testify/require"
)

const (
	deferredMsg    MessageType = UserMsg + 70
	deferredFFMsg  MessageType = UserMsg + 71
	deferredPlain  MessageType = UserMsg + 72
	deferredSlowMs             = 50
	deferredWaitFn             = 5 * time.Second
	deferredTickFn             = 5 * time.Millisecond
)

// ---- unit level: the replier against a packet with a reply box, exact reference accounting ----

// deferredUnit is a cluster that never runs, a sender it knows, and a way to make request packets.
type deferredUnit struct {
	t      *testing.T
	c      *Cluster
	sender *Node
}

func newDeferredUnit(t *testing.T) *deferredUnit {
	t.Helper()
	return &deferredUnit{t: t, c: newUnitCluster(t), sender: newNode(NodeID(uuid.New()), "127.0.0.1:1")}
}

// request makes a request packet. With a box it is awaiting a reply there; without, it is fire-and-forget. The test
// keeps one reference of its own (the probe), so the packet is never recycled while its count is read.
func (u *deferredUnit) request(box *replyBox) (p, probe *Packet) {
	p = NewPacket()
	p.MessageType = deferredMsg
	p.SenderID = u.sender.ID
	p.MessageID = MessageID(time.Now().UnixNano())
	p.TTL = 1
	p.codec = u.c.config.MsgCodec
	p.payload = []byte(`"ping"`)
	if box != nil {
		p.setReply(box)
	}
	return p, p.AddRef()
}

// dispatchTo runs the packet through a deferred handler that decodes the request and hands back its Replier.
func (u *deferredUnit) dispatchTo(p *Packet) Replier {
	u.t.Helper()
	var got Replier
	mh := &msgHandler{deferredHandler: func(_ *Node, packet *Packet, r Replier) {
		var s string
		require.NoError(u.t, packet.Unmarshal(&s))
		require.Equal(u.t, "ping", s)
		got = r
	}}
	require.NoError(u.t, mh.dispatch(u.c, u.sender, p))
	return got
}

func refs(p *Packet) int32 { return p.refCount.Load() }

// A reply turns the request into the reply exactly as a synchronous handler's return value does, and the reference
// the dispatcher kept for the Replier travels with it to the transport.
func TestDeferredReplier_Reply(t *testing.T) {
	u := newDeferredUnit(t)
	box := newReplyBox()
	p, probe := u.request(box)
	r := u.dispatchTo(p)
	require.EqualValues(t, 2, refs(probe), "dispatch released its own reference and the replier holds one")

	r.Reply("pong", nil)

	select {
	case reply := <-box.C():
		require.Same(t, probe, reply)
		require.Equal(t, replyMsg, reply.MessageType)
		require.Equal(t, u.c.localNode.ID, reply.SenderID)
		require.EqualValues(t, 1, reply.TTL)
		require.NotZero(t, reply.MessageID)
		var s string
		require.NoError(t, reply.Unmarshal(&s))
		require.Equal(t, "pong", s)
		require.EqualValues(t, 2, refs(probe), "the reference now belongs to the transport")
		reply.Release() // the transport, after writing it
	default:
		t.Fatal("no reply in the box")
	}
	require.EqualValues(t, 1, refs(probe))
	probe.Release()
}

// A reply to a requester that stopped waiting is refused by the box and the packet is released.
func TestDeferredReplier_RequesterGone(t *testing.T) {
	u := newDeferredUnit(t)
	box := newReplyBox()
	p, probe := u.request(box)
	r := u.dispatchTo(p)
	box.close() // the transport gave up

	r.Reply("pong", nil)

	require.EqualValues(t, 1, refs(probe), "the refused reply released the replier's reference")
	select {
	case <-box.C():
		t.Fatal("a closed box took a reply")
	default:
	}
	probe.Release()
}

// The second Reply does nothing: one reply, one release, whatever the second carries.
func TestDeferredReplier_SecondReplyIgnored(t *testing.T) {
	u := newDeferredUnit(t)
	box := newReplyBox()
	p, probe := u.request(box)
	r := u.dispatchTo(p)

	r.Reply("first", nil)
	r.Reply("second", nil)
	r.Reply(nil, errors.New("third"))

	reply := <-box.C()
	var s string
	require.NoError(t, reply.Unmarshal(&s))
	require.Equal(t, "first", s)
	require.EqualValues(t, 2, refs(probe))
	reply.Release()
	require.EqualValues(t, 1, refs(probe), "released once, not once per call")
	probe.Release()
}

// An error and a nil payload send nothing (the requester times out, as with a ReplyHandler) and release.
func TestDeferredReplier_ErrorAndNilSendNothing(t *testing.T) {
	for name, reply := range map[string]func(Replier){
		"error":         func(r Replier) { r.Reply(nil, errors.New("handler failed")) },
		"error+value":   func(r Replier) { r.Reply("ignored", errors.New("handler failed")) },
		"nil payload":   func(r Replier) { r.Reply(nil, nil) },
		"unmarshalable": func(r Replier) { r.Reply(make(chan int), nil) }, // the codec cannot marshal it
	} {
		t.Run(name, func(t *testing.T) {
			u := newDeferredUnit(t)
			box := newReplyBox()
			p, probe := u.request(box)
			r := u.dispatchTo(p)
			reply(r)
			select {
			case <-box.C():
				t.Fatal("a reply was sent")
			default:
			}
			require.EqualValues(t, 1, refs(probe))
			require.Equal(t, deferredMsg, probe.MessageType, "the request was not turned into a reply")
			probe.Release()
		})
	}
}

// A packet nobody waits for (fire-and-forget) is never turned into a reply: it may be shared with the broadcast
// queue. Reply only releases it, and then a connection it owns is closed.
func TestDeferredReplier_NotAwaitingReply(t *testing.T) {
	u := newDeferredUnit(t)
	p := NewPacket()
	p.MessageType = deferredMsg
	p.SenderID = u.sender.ID
	p.TTL = 1
	p.codec = u.c.config.MsgCodec
	p.payload = []byte(`"ping"`)
	conn := &closeRecorder{}
	p.SetConn(conn)
	require.False(t, p.awaitingReply())

	r := u.dispatchTo(p)
	require.Equal(t, deferredMsg, p.MessageType)
	require.False(t, conn.closed, "the replier still holds the packet")
	r.Reply("pong", nil)
	require.True(t, conn.closed, "Reply released the last reference")
}

// A handler that answers before it returns works too.
func TestDeferredReplier_ReplyInsideHandler(t *testing.T) {
	u := newDeferredUnit(t)
	box := newReplyBox()
	p, probe := u.request(box)
	mh := &msgHandler{deferredHandler: func(_ *Node, _ *Packet, r Replier) { r.Reply("now", nil) }}
	require.NoError(t, mh.dispatch(u.c, u.sender, p))
	reply := <-box.C()
	require.EqualValues(t, 2, refs(probe))
	reply.Release()
	require.EqualValues(t, 1, refs(probe))
	probe.Release()
}

// Replying while the requester gives up, and replying from many goroutines at once, release exactly once and race
// with nothing (meaningful under -race).
func TestDeferredReplier_Races(t *testing.T) {
	u := newDeferredUnit(t)
	for range 1000 {
		box := newReplyBox()
		p, probe := u.request(box)
		r := u.dispatchTo(p)
		var wg sync.WaitGroup
		wg.Add(4)
		go func() { defer wg.Done(); box.close() }()
		for range 3 {
			go func() { defer wg.Done(); r.Reply("pong", nil) }()
		}
		wg.Wait()
		if n := refs(probe); n != 1 {
			t.Fatalf("%d references after the race, want 1 (the probe)", n)
		}
		probe.Release()
	}
}

// Through the cluster's registration: the message type must be a reserved or user one.
func TestHandleFuncWithDeferredReply_Registration(t *testing.T) {
	u := newDeferredUnit(t)
	noop := func(*Node, *Packet, Replier) {}
	require.Error(t, u.c.HandleFuncWithDeferredReply(nodeJoinMsg, noop))
	require.NoError(t, u.c.HandleFuncWithDeferredReply(deferredMsg, noop))
	require.True(t, u.c.UnregisterMessageType(deferredMsg))
}

// ---- end to end over the socket and the HTTP transports ----

type deferredEnv struct {
	server, client *Cluster
	node           *Node // the server, as the client knows it
}

// deferredCluster starts a cluster on the named transport. The socket one waits tcpDeadline for a reply; HTTP ignores it.
func deferredCluster(t *testing.T, kind string, workers int, tcpDeadline time.Duration) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.MsgCodec = shamaton.New()
	cfg.Logger = logger.NewNullLogger()
	cfg.NumIncomingWorkers = workers
	cfg.TCPDeadline = tcpDeadline
	switch kind {
	case "socket":
		addr := getFreeTCPAddress(t)
		cfg.BindAddr, cfg.AdvertiseAddr = addr, addr
		cfg.Transport = NewSocketTransport(cfg)
	case "http":
		ht := NewHTTPTransport(cfg)
		srv := httptest.NewServer(http.HandlerFunc(ht.HandleGossipRequest))
		t.Cleanup(srv.Close)
		cfg.BindAddr, cfg.AdvertiseAddr = srv.Listener.Addr().String(), srv.URL
		cfg.Transport = ht
	default:
		t.Fatalf("unknown transport %q", kind)
	}
	c, err := NewCluster(cfg)
	require.NoError(t, err)
	c.Start()
	t.Cleanup(func() { stopQuietly(c) })
	return c
}

func newDeferredEnv(t *testing.T, kind string, workers int, tcpDeadline time.Duration) *deferredEnv {
	t.Helper()
	server := deferredCluster(t, kind, workers, tcpDeadline)
	client := deferredCluster(t, kind, 8, 5*time.Second)
	joinAndWait(t, server, client)
	return &deferredEnv{server: server, client: client, node: client.GetNode(server.LocalNode().ID)}
}

// forTransports runs the test on both transports that wait on a reply box.
func forTransports(t *testing.T, f func(t *testing.T, kind string)) {
	for _, kind := range []string{"socket", "http"} {
		t.Run(kind, func(t *testing.T) { f(t, kind) })
	}
}

// heldPackets lets a handler keep one reference of its own to every request it gets, so a test can see when the
// reference the Replier held has been given up (the count is back to one) without the pool recycling the packet.
type heldPackets struct {
	mu   sync.Mutex
	list []*Packet
}

func (h *heldPackets) hold(p *Packet) {
	h.mu.Lock()
	h.list = append(h.list, p.AddRef())
	h.mu.Unlock()
}

// settled waits until every held packet is referenced by the test alone, then drops those references.
func (h *heldPackets) settled(t *testing.T, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		if len(h.list) < want {
			return false
		}
		for _, p := range h.list {
			if refs(p) != 1 {
				return false
			}
		}
		return true
	}, deferredWaitFn, deferredTickFn, "a request packet is still referenced")
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range h.list {
		p.Release()
	}
	h.list = nil
}

// A handler that returns at once and answers from another goroutine after a delay.
func TestDeferredReply_FromAnotherGoroutine(t *testing.T) {
	forTransports(t, func(t *testing.T, kind string) {
		env := newDeferredEnv(t, kind, 8, 2*time.Second)
		var held heldPackets
		require.NoError(t, env.server.HandleFuncWithDeferredReply(deferredMsg, func(_ *Node, p *Packet, r Replier) {
			var q string
			if err := p.Unmarshal(&q); err != nil {
				r.Reply(nil, err)
				return
			}
			held.hold(p)
			go func() {
				time.Sleep(deferredSlowMs * time.Millisecond)
				r.Reply("pong:"+q, nil)
			}()
		}))

		start := time.Now()
		var reply string
		require.NoError(t, env.client.SendToWithResponse(env.node, deferredMsg, "a", &reply))
		require.Equal(t, "pong:a", reply)
		require.True(t, time.Since(start) >= deferredSlowMs*time.Millisecond, "answered before the handler replied")

		// With a context, and again on the same connection.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, env.client.SendToWithResponseContext(ctx, env.node, deferredMsg, "b", &reply))
		require.Equal(t, "pong:b", reply)
		held.settled(t, 2)
	})
}

// The requester's context expires before the answer: it gets its own error, the late Reply is refused by the
// transport (no panic, no race) and the packet is released.
func TestDeferredReply_RequesterContextExpired(t *testing.T) {
	forTransports(t, func(t *testing.T, kind string) {
		// The socket server stops waiting after TCPDeadline; HTTP when the client's connection closes.
		env := newDeferredEnv(t, kind, 8, 80*time.Millisecond)
		var held heldPackets
		late := make(chan Replier, 1)
		require.NoError(t, env.server.HandleFuncWithDeferredReply(deferredMsg, func(_ *Node, p *Packet, r Replier) {
			held.hold(p)
			late <- r
		}))

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		var reply string
		err := env.client.SendToWithResponseContext(ctx, env.node, deferredMsg, "slow", &reply)
		require.True(t, errors.Is(err, context.DeadlineExceeded), "err = %v", err)

		// Wait until the server's transport has given up on the request, then answer.
		time.Sleep(250 * time.Millisecond) // three times the server's TCPDeadline
		r := <-late
		r.Reply("too late", nil)
		held.settled(t, 1)

		// Nothing is left behind: the next request is served.
		require.NoError(t, env.server.HandleFuncWithDeferredReply(deferredPlain, func(_ *Node, p *Packet, r Replier) {
			r.Reply("ok", nil)
		}))
		require.NoError(t, env.client.SendToWithResponse(env.node, deferredPlain, "x", &reply))
		require.Equal(t, "ok", reply)
	})
}

// Reply called twice answers once; the second call changes nothing.
func TestDeferredReply_DoubleReply(t *testing.T) {
	forTransports(t, func(t *testing.T, kind string) {
		env := newDeferredEnv(t, kind, 8, 2*time.Second)
		var held heldPackets
		require.NoError(t, env.server.HandleFuncWithDeferredReply(deferredMsg, func(_ *Node, p *Packet, r Replier) {
			held.hold(p)
			go func() {
				r.Reply("first", nil)
				r.Reply("second", nil)
				r.Reply(nil, errors.New("third"))
			}()
		}))
		var reply string
		require.NoError(t, env.client.SendToWithResponse(env.node, deferredMsg, "x", &reply))
		require.Equal(t, "first", reply)
		held.settled(t, 1)
	})
}

// An error or a nil payload sends nothing: the requester times out, and the packet is released.
func TestDeferredReply_ErrorAndNilPayloadTimeOut(t *testing.T) {
	forTransports(t, func(t *testing.T, kind string) {
		env := newDeferredEnv(t, kind, 8, 2*time.Second)
		var held heldPackets
		require.NoError(t, env.server.HandleFuncWithDeferredReply(deferredMsg, func(_ *Node, p *Packet, r Replier) {
			held.hold(p)
			var mode string
			if err := p.Unmarshal(&mode); err != nil {
				r.Reply(nil, err)
				return
			}
			go func() {
				switch mode {
				case "error":
					r.Reply(nil, errors.New("deliberate"))
				case "nil":
					r.Reply(nil, nil)
				default:
					r.Reply("ok", nil)
				}
			}()
		}))
		for _, mode := range []string{"error", "nil"} {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
			var reply string
			err := env.client.SendToWithResponseContext(ctx, env.node, deferredMsg, mode, &reply)
			cancel()
			require.True(t, errors.Is(err, context.DeadlineExceeded), "%s: err = %v", mode, err)
			require.Empty(t, reply)
		}
		held.settled(t, 2)

		var reply string
		require.NoError(t, env.client.SendToWithResponse(env.node, deferredMsg, "fine", &reply))
		require.Equal(t, "ok", reply)
		held.settled(t, 1)
	})
}

// A message sent without waiting for a reply reaches a deferred handler, which can call Reply as usual: nothing is
// sent and the packet is released.
func TestDeferredReply_FireAndForget(t *testing.T) {
	forTransports(t, func(t *testing.T, kind string) {
		env := newDeferredEnv(t, kind, 8, 2*time.Second)
		var held heldPackets
		var calls atomic.Int32
		require.NoError(t, env.server.HandleFuncWithDeferredReply(deferredFFMsg, func(_ *Node, p *Packet, r Replier) {
			var s string
			if err := p.Unmarshal(&s); err != nil || s != "hello" {
				r.Reply(nil, err)
				return
			}
			held.hold(p)
			calls.Add(1)
			go r.Reply("ignored", nil)
		}))
		require.Eventually(t, func() bool {
			if calls.Load() == 0 {
				env.client.SendToReliable(env.node, deferredFFMsg, "hello")
			}
			return calls.Load() > 0
		}, deferredWaitFn, deferredTickFn, "the message did not arrive")
		held.settled(t, int(calls.Load()))
	})
}

// The point of the feature: with a single incoming worker, requests whose handlers are all still waiting have all been
// accepted (no worker is held), and all are answered once released.
func TestDeferredReply_OneWorkerNotHeld(t *testing.T) {
	forTransports(t, func(t *testing.T, kind string) {
		env := newDeferredEnv(t, kind, 1, 5*time.Second)
		const requests = 40
		var entered atomic.Int32
		release := make(chan struct{})
		require.NoError(t, env.server.HandleFuncWithDeferredReply(deferredMsg, func(_ *Node, p *Packet, r Replier) {
			var n int
			if err := p.Unmarshal(&n); err != nil {
				r.Reply(nil, err)
				return
			}
			entered.Add(1)
			go func() {
				<-release
				r.Reply(n*2, nil)
			}()
		}))

		var wg sync.WaitGroup
		errs := make(chan error, requests)
		for i := range requests {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				var reply int
				if err := env.client.SendToWithResponseContext(ctx, env.node, deferredMsg, i, &reply); err != nil {
					errs <- fmt.Errorf("request %d: %w", i, err)
				} else if reply != i*2 {
					errs <- fmt.Errorf("request %d: reply %d", i, reply)
				}
			}()
		}
		// Every handler has run and none has answered: had the first held the one worker, the rest would still be queued.
		require.Eventually(t, func() bool { return entered.Load() == requests }, 10*time.Second, 10*time.Millisecond,
			"only %d of %d handlers ran while the others waited: a worker is held", entered.Load(), requests)
		close(release)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})
}

// The same request count against a handler that waits in place (HandleFuncWithResponse) does hold the worker: the
// contrast that makes the test above meaningful.
func TestDeferredReply_SynchronousHandlerHoldsTheWorker(t *testing.T) {
	env := newDeferredEnv(t, "socket", 1, 5*time.Second)
	var entered atomic.Int32
	release := make(chan struct{})
	require.NoError(t, env.server.HandleFuncWithResponse(deferredMsg, func(_ *Node, p *Packet) (interface{}, error) {
		entered.Add(1)
		<-release
		return "ok", nil
	}))
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var reply string
			_ = env.client.SendToWithResponse(env.node, deferredMsg, "x", &reply)
		}()
	}
	require.Eventually(t, func() bool { return entered.Load() >= 1 }, deferredWaitFn, deferredTickFn)
	time.Sleep(150 * time.Millisecond)
	require.EqualValues(t, 1, entered.Load(), "the one worker is inside the first handler")
	close(release)
	wg.Wait()
}
