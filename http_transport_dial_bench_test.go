package gossip

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
)

// benchmarkHTTPSend measures Send over loopback. Fresh connections are forced
// on every iteration when newConns is set, so the dial path is exercised.
func benchmarkHTTPSend(b *testing.B, newConns bool) {
	config := &Config{
		MsgCodec:         codec.NewJSONCodec(),
		Logger:           logger.NewNullLogger(),
		TCPMaxPacketSize: 65535,
	}
	transport := NewHTTPTransport(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	node := &Node{
		ID:            NodeID(uuid.New()),
		advertiseAddr: server.URL,
		address:       Address{URL: server.URL},
	}
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SenderID = NodeID(uuid.New())
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 5
	packet.SetPayload([]byte("benchmark payload"))
	packet.SetCodec(config.MsgCodec)
	defer packet.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if newConns {
			transport.client.CloseIdleConnections()
		}
		if err := transport.Send(TransportBestEffort, node, packet.AddRef()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTPTransportSend(b *testing.B)        { benchmarkHTTPSend(b, false) }
func BenchmarkHTTPTransportSendNewConn(b *testing.B) { benchmarkHTTPSend(b, true) }
