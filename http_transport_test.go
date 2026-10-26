package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func httpTestConfig(opts ...func(*Config)) *Config {
	cfg := &Config{
		IncomingPacketQueueDepth: 10,
		MsgCodec:                 codec.NewJSONCodec(),
		Logger:                   logger.NewNullLogger(),
		TCPMaxPacketSize:         65535,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func httpTestNode(url string) *Node {
	return &Node{ID: NodeID(uuid.New()), advertiseAddr: url, address: Address{URL: url}}
}

func TestHTTPTransportName(t *testing.T) {
	assert.Equal(t, "http", NewHTTPTransport(httpTestConfig()).Name())
}

// What the gossip endpoint answers: it authenticates before anything else, takes only POSTs of whole packets, and
// answers 204 once a packet is queued (or with the reply when one is expected) and 503 when it cannot queue it.
func TestHTTPHandleGossipRequest(t *testing.T) {
	const token = "secret-token"
	tests := []struct {
		name      string
		token     string // the configured BearerToken
		method    string
		auth      string // Authorization header
		body      string // "packet", "reply packet", or the raw body
		queueFull bool
		wantCode  int
		delivered bool
	}{
		{name: "packet", method: "POST", body: "packet", wantCode: 204, delivered: true},
		{name: "not a POST", method: "GET", body: "packet", wantCode: 405},
		{name: "garbage", method: "POST", body: "invalid data", wantCode: 400},
		{name: "empty", method: "POST", body: "", wantCode: 400},
		{name: "queue full", method: "POST", body: "packet", queueFull: true, wantCode: 503},
		{name: "reply expected and queue full", method: "POST", body: "reply packet", queueFull: true, wantCode: 503},
		{name: "reply expected and answered", method: "POST", body: "reply packet", wantCode: 200, delivered: true},
		{name: "token missing", token: token, method: "POST", body: "packet", wantCode: 401},
		{name: "token of another scheme", token: token, method: "POST", body: "packet", auth: "Basic abc", wantCode: 401},
		{name: "wrong token", token: token, method: "POST", body: "packet", auth: "Bearer wrong-token", wantCode: 401},
		{name: "right token", token: token, method: "POST", body: "packet", auth: "Bearer " + token, wantCode: 204, delivered: true},
		{name: "authentication comes before the method check", token: token, method: "GET", body: "packet", wantCode: 401},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := httpTestConfig(func(c *Config) { c.BearerToken = tt.token })
			ht := NewHTTPTransport(cfg)

			p := NewPacket()
			defer p.Release()
			p.MessageType, p.SenderID, p.MessageID, p.TTL = UserMsg, NodeID(uuid.New()), MessageID(hlc.Now()), 3
			p.SetCodec(cfg.MsgCodec)
			p.SetPayload([]byte("http-request-body"))
			body := tt.body
			if body == "packet" || body == "reply packet" {
				buf, err := ht.packetToBuffer(p, body == "reply packet")
				require.NoError(t, err)
				body = string(buf)
			}
			if tt.queueFull {
				for range cap(ht.packetChannel) {
					ht.packetChannel <- NewPacket()
				}
			}

			// The node's side: takes the queued packet, and answers it if it wants a reply.
			seen := make(chan string, 1)
			if tt.delivered {
				go func() {
					got := <-ht.packetChannel
					defer got.Release()
					seen <- string(got.Payload())
					if got.awaitingReply() {
						got.SetPayload([]byte("the reply"))
						got.AddRef()
						got.SendReply()
					}
				}()
			}

			req := httptest.NewRequest(tt.method, "/gossip", strings.NewReader(body))
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			w := httptest.NewRecorder()
			ht.HandleGossipRequest(w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.delivered {
				assert.Equal(t, "http-request-body", <-seen)
			} else if !tt.queueFull {
				assert.Empty(t, ht.packetChannel, "nothing was queued")
			}
			if tt.wantCode == 200 {
				reply, err := ht.packetFromBuffer(w.Body.Bytes())
				require.NoError(t, err)
				assert.Equal(t, "the reply", string(reply.Payload()))
				reply.Release()
			}
		})
	}
}

// Send and SendWithReply put the reply flag, the bearer token and the packet on the wire, and read what comes back.
func TestHTTPSendWithReply(t *testing.T) {
	replyPacket := func(cfg *Config) []byte {
		p := NewPacket()
		defer p.Release()
		p.MessageType, p.SenderID, p.MessageID, p.TTL = replyMsg, NodeID(uuid.New()), MessageID(hlc.Now()), 5
		p.SetCodec(cfg.MsgCodec)
		p.SetPayload([]byte("server reply"))
		buf, err := NewHTTPTransport(cfg).packetToBuffer(p, false)
		require.NoError(t, err)
		return buf
	}

	tests := []struct {
		name      string
		status    int
		withReply bool // SendWithReply rather than Send
		wantReply bool
		wantErr   bool
	}{
		{name: "reply", status: 200, withReply: true, wantReply: true},
		{name: "no content", status: 204, withReply: true},
		{name: "server error", status: 500, withReply: true, wantErr: true},
		{name: "send", status: 204},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := httpTestConfig(func(c *Config) { c.BearerToken = "secret-token" })
			var mu sync.Mutex
			var flags uint16
			var auth string
			var gotPayload []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				flags, auth = binary.LittleEndian.Uint16(body), r.Header.Get("Authorization")
				gotPayload = body
				mu.Unlock()
				w.WriteHeader(tt.status)
				if tt.status == 200 {
					w.Write(replyPacket(cfg))
				}
			}))
			defer server.Close()

			ht := NewHTTPTransport(cfg)
			p := NewPacket()
			defer p.Release()
			p.MessageType, p.SenderID, p.MessageID, p.TTL = UserMsg, NodeID(uuid.New()), MessageID(hlc.Now()), 5
			p.SetCodec(cfg.MsgCodec)
			p.SetPayload([]byte("client request"))

			var reply *Packet
			var err error
			if tt.withReply {
				reply, err = ht.SendWithReply(httpTestNode(server.URL), p)
			} else {
				err = ht.Send(TransportReliable, httpTestNode(server.URL), p)
			}

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tt.wantReply {
				require.NotNil(t, reply)
				assert.Equal(t, "server reply", string(reply.Payload()))
				reply.Release()
			} else {
				assert.Nil(t, reply)
			}
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, tt.withReply, flags&replyExpectedFlag != 0, "the reply flag on the request")
			assert.Equal(t, "Bearer secret-token", auth)
			assert.True(t, bytes.Contains(gotPayload, []byte("client request")))
		})
	}
}

// Send reads the response to its end, which is what returns the connection to the pool: three sends to a server that
// answers with a large body use one connection.
func TestHTTPSendReusesConnection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Write(bytes.Repeat([]byte("x"), 256<<10))
	}))
	defer server.Close()

	d := &recordingDialer{}
	cfg := httpTestConfig(func(c *Config) { c.DialFunc = d.dial })
	ht := NewHTTPTransport(cfg)
	p := testPacket(cfg.MsgCodec, "drain")
	defer p.Release()

	for range 3 {
		require.NoError(t, ht.Send(TransportBestEffort, httpTestNode(server.URL), p.AddRef()))
	}

	addrs, _ := d.calls()
	assert.Len(t, addrs, 1, "the connection was dialled once and reused")
}

// The size of a request body is bounded by the cap, whatever Content-Length claims.
func TestReadBoundedBody(t *testing.T) {
	tests := []struct {
		name          string
		size          int
		contentLength int64 // what the sender claims
		max           int64
		want          int
	}{
		{"content length known", 4096, 4096, 1 << 16, 4096},
		{"content length missing", 31, -1, 1 << 16, 31},
		{"claims more than is sent", 21, 1024, 1 << 16, 21},
		{"body over the cap is cut", 8192, 8192, 1024, 1024},
		{"claim over the cap is not allocated", 2048, 1 << 30, 1024, 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte("y"), tt.size)
			got, err := readBoundedBody(bytes.NewReader(payload), tt.contentLength, tt.max)
			require.NoError(t, err)
			assert.Equal(t, payload[:tt.want], got)
		})
	}
}

// Stopping the transport (cancelling the context it was started with) aborts a request in flight.
func TestHTTPTransport_SendRespectsContextCancellation(t *testing.T) {
	cfg := httpTestConfig()
	ht := NewHTTPTransport(cfg)

	// The handler waits for the client to give up, or for the test to release it.
	started := make(chan struct{}, 1)
	releaseServer := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-releaseServer:
		}
	}))
	defer func() {
		close(releaseServer)
		server.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, ht.Start(ctx, &sync.WaitGroup{}))

	p := testPacket(cfg.MsgCodec, "will be cancelled")
	defer p.Release()
	errCh := make(chan error, 1)
	go func() { errCh <- ht.Send(TransportBestEffort, httpTestNode(server.URL), p.AddRef()) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("server never received the request")
	}
	cancel()

	select {
	case err := <-errCh:
		assert.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not return after context cancellation")
	}
}
