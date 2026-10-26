package gossip

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startSocket starts a socket transport on a free port and returns it with a node that addresses it. It stops with
// the test.
func startSocket(t testing.TB, opts ...func(*Config)) (*SocketTransport, *Node) {
	t.Helper()
	addr := getFreeTCPAddress(t)
	cfg := DefaultConfig()
	cfg.BindAddr, cfg.AdvertiseAddr = addr, addr
	cfg.MsgCodec = codec.NewJSONCodec()
	for _, opt := range opts {
		opt(cfg)
	}
	st := NewSocketTransport(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	require.NoError(t, st.Start(ctx, &wg))
	t.Cleanup(func() { cancel(); wg.Wait() })
	return st, newNode(NodeID(uuid.New()), addr)
}

func TestSocketTransportLifecycle(t *testing.T) {
	tests := []struct {
		name         string
		opts         []func(*Config)
		wantUDP      bool
		wantReliable bool // everything goes over TCP
	}{
		{"default", nil, true, false},
		{"forced reliable has no UDP listener", []func(*Config){func(c *Config) { c.ForceReliableTransport = true }}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.BindAddr = getFreeTCPAddress(t)
			cfg.MsgCodec = codec.NewJSONCodec()
			for _, opt := range tt.opts {
				opt(cfg)
			}
			st := NewSocketTransport(cfg)
			assert.Equal(t, "socket", st.Name())
			require.NotNil(t, st.PacketChannel())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var wg sync.WaitGroup
			require.NoError(t, st.Start(ctx, &wg))
			assert.NotNil(t, st.tcpListener)
			assert.Equal(t, tt.wantUDP, st.udpListener != nil)
			addr := st.tcpListener.Addr().String()

			// A node whose address cannot be resolved cannot be sent to.
			p := testPacket(cfg.MsgCodec, "x")
			defer p.Release()
			unresolved := &Node{ID: NodeID(uuid.New())}
			assert.Error(t, st.Send(TransportBestEffort, unresolved, p.AddRef()))
			_, err := st.SendWithReply(unresolved, p.AddRef())
			assert.Error(t, err)

			cancel()
			wg.Wait()
			_, err = net.DialTimeout("tcp", addr, time.Second)
			assert.Error(t, err, "the TCP listener is closed on shutdown")
		})
	}
}

// Packets go over TCP when sent reliably, over UDP when best effort, and over TCP again when too large for UDP.
func TestSocketTransportSend(t *testing.T) {
	tests := []struct {
		name         string
		transport    TransportType
		udpMax       int
		payload      string
		wantReliable bool
	}{
		{"reliable goes over TCP", TransportReliable, 1400, "tcp-test-data", true},
		{"best effort goes over UDP", TransportBestEffort, 1400, "udp-test", false},
		{"best effort too large for UDP falls back to TCP", TransportBestEffort, 10, "this is larger than 10 bytes", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receiver, node := startSocket(t)
			sender, _ := startSocket(t, func(c *Config) { c.UDPMaxPacketSize = tt.udpMax })
			p := testPacket(sender.config.MsgCodec, tt.payload)
			defer p.Release()

			require.NoError(t, sender.Send(tt.transport, node, p.AddRef()))

			select {
			case got := <-receiver.PacketChannel():
				defer got.Release()
				assert.Equal(t, UserMsg, got.MessageType)
				assert.Equal(t, tt.payload, string(got.Payload()))
				assert.Equal(t, p.SenderID, got.SenderID)
				assert.Equal(t, tt.wantReliable, got.arrivedReliably(), "arrived over TCP")
			case <-time.After(5 * time.Second):
				t.Fatal("no packet received")
			}
		})
	}
}

func TestSocketTransportSendWithReply(t *testing.T) {
	server, node := startSocket(t)
	client, _ := startSocket(t)
	go func() {
		got := <-server.PacketChannel()
		defer got.Release()
		got.SetCodec(server.config.MsgCodec)
		got.SetPayload([]byte("pong"))
		got.AddRef()
		got.SendReply()
	}()
	p := testPacket(client.config.MsgCodec, "ping")
	defer p.Release()

	reply, err := client.SendWithReply(node, p)

	require.NoError(t, err)
	require.NotNil(t, reply)
	defer reply.Release()
	assert.Equal(t, "pong", string(reply.Payload()))
}

func TestSocketTransportSendFailures(t *testing.T) {
	t.Run("nobody listening", func(t *testing.T) {
		st := NewSocketTransport(&Config{MsgCodec: codec.NewJSONCodec(), TCPDialTimeout: 200 * time.Millisecond})
		_, err := st.dialPeer(context.Background(), newNode(NodeID(uuid.New()), "127.0.0.1:1"))
		assert.Error(t, err)
	})

	t.Run("udp to a name that does not resolve", func(t *testing.T) {
		st, _ := startSocket(t, func(c *Config) { c.Resolver = &mockResolver{ipErr: fmt.Errorf("DNS failure")} })
		p := testPacket(st.config.MsgCodec, "test")
		defer p.Release()
		buf, err := st.packetToBuffer(p, false)
		require.NoError(t, err)
		assert.Error(t, st.sendUDP(newNode(NodeID(uuid.New()), "unreachable.host:7946"), buf))
	})
}
