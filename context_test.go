package gossip

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/logger"
	"github.com/stretchr/testify/require"
)

const ctxQueryMsg MessageType = UserMsg + 40

// twoNodes starts two connected clusters; the first answers ctxQueryMsg, "slow" only once the test is over. wrap, if
// set, wraps the client's transport.
func twoNodes(t *testing.T, wrap ...func(Transport) Transport) (server, client *Cluster, node *Node) {
	t.Helper()
	server = startLiveCluster(t)
	client = startLiveCluster(t, func(c *Config) {
		if len(wrap) > 0 {
			c.Transport = wrap[0](NewSocketTransport(c))
		}
	})
	// Closed before the clusters stop (cleanups run last in, first out): a handler still waiting holds a worker that
	// Stop waits for.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	require.NoError(t, server.HandleFuncWithResponse(ctxQueryMsg, func(_ *Node, p *Packet) (interface{}, error) {
		var q string
		if err := p.Unmarshal(&q); err == nil && q == "slow" {
			<-release
		}
		return "pong", nil
	}))
	joinAndWait(t, server, client)
	return server, client, client.GetNode(server.LocalNode().ID)
}

// A caller's deadline ends the exchange with the deadline's error, long before the transport's TCPDeadline; without
// one the exchange is bounded by the transport as before.
func TestSendToWithResponseContext_Deadline(t *testing.T) {
	_, client, node := twoNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	var reply string
	err := client.SendToWithResponseContext(ctx, node, ctxQueryMsg, "slow", &reply)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the deadline's", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("exchange ran %s past a 50ms deadline", elapsed)
	}
	// The interrupted connection is not reused: the next exchange works.
	require.NoError(t, client.SendToWithResponse(node, ctxQueryMsg, "ping", &reply))
	require.Equal(t, "pong", reply)
}

// A context already ended sends nothing.
func TestSendToWithResponseContext_Cancelled(t *testing.T) {
	_, client, node := twoNodes(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var reply string
	if err := client.SendToWithResponseContext(ctx, node, ctxQueryMsg, "slow", &reply); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

// replyOnly is a transport that is not a ContextTransport (as a custom one written before it may be).
type replyOnly struct{ Transport }

// The deadline holds with a transport that cannot take a context: the cluster stops waiting.
func TestSendToWithResponseContext_PlainTransport(t *testing.T) {
	_, client, node := twoNodes(t, func(tr Transport) Transport { return replyOnly{tr} })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	var reply string
	if err := client.SendToWithResponseContext(ctx, node, ctxQueryMsg, "slow", &reply); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("waited %s past a 50ms deadline", elapsed)
	}
}

// Stopping the socket transport aborts a dial in progress instead of leaving it to run out TCPDialTimeout.
func TestSocketTransport_StopAbortsDial(t *testing.T) {
	dialing := make(chan struct{})
	var once sync.Once
	slow := func(ctx context.Context, network, addr string) (net.Conn, error) {
		once.Do(func() { close(dialing) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
			return nil, errors.New("dial ran its full timeout")
		}
	}
	st := NewSocketTransport(&Config{
		BindAddr: "127.0.0.1:0", IncomingPacketQueueDepth: 10, Logger: logger.NewNullLogger(),
		MsgCodec: codec.NewJSONCodec(), TCPDeadline: 5 * time.Second, UDPDeadline: 5 * time.Second,
		TCPDialTimeout: 3 * time.Second, UDPMaxPacketSize: 1024, TCPMaxPacketSize: 65536, DialFunc: slow,
		ForceReliableTransport: true,
	})
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	require.NoError(t, st.Start(ctx, &wg))
	node := newNode(NodeID(uuid.New()), "127.0.0.1:1")
	node.address = Address{IP: net.ParseIP("127.0.0.1"), Port: 1}
	packet := testPacket(st.config.MsgCodec, "to nowhere")
	defer packet.Release()
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		_, _ = st.SendWithReply(node, packet)
		done <- time.Since(start)
	}()
	<-dialing
	stop()
	select {
	case d := <-done:
		if d > time.Second {
			t.Fatalf("dial ran %s after the transport stopped", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dial not aborted by stopping the transport")
	}
	wg.Wait()
}

// The HTTP transport bounds a request by the caller's context too.
func TestHTTPTransport_SendWithReplyContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	transport := NewHTTPTransport(&Config{MsgCodec: codec.NewJSONCodec(), Logger: logger.NewNullLogger(),
		TCPMaxPacketSize: 65535, TCPDialTimeout: 3 * time.Second})
	node := &Node{ID: NodeID(uuid.New()), advertiseAddr: server.URL, address: Address{URL: server.URL}}
	packet := testPacket(transport.config.MsgCodec, "slow")
	defer packet.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := transport.SendWithReplyContext(ctx, node, packet); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("request ran %s past a 50ms deadline", elapsed)
	}
}
