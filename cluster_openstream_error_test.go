package gossip

import (
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"testing"
)

// transport that returns a pipe and writes wrong ack after WritePacket
type openStreamFailTransport struct{}

func (t *openStreamFailTransport) PacketChannel() chan *Packet { return make(chan *Packet) }
func (t *openStreamFailTransport) DialPeer(_ *Node) (net.Conn, error) {
	client, server := net.Pipe()
	// keep server and write wrong ack when WritePacket is called via closure capture
	tserver = server
	return client, nil
}

var tserver net.Conn

func (t *openStreamFailTransport) WritePacket(_ net.Conn, _ *Packet) error {
	// Immediately write an unexpected ack type to the server side so client sees it
	go func() {
		if tserver != nil {
			// write msgType != streamOpenAckMsg
			_ = binary.Write(tserver, binary.BigEndian, uint16(UserMsg))
			tserver.Close()
		}
	}()
	return nil
}
func (t *openStreamFailTransport) ReadPacket(_ net.Conn) (*Packet, error)                 { return nil, nil }
func (t *openStreamFailTransport) SendPacket(_ TransportType, _ []*Node, _ *Packet) error { return nil }
func (t *openStreamFailTransport) WebsocketHandler(_ context.Context, _ http.ResponseWriter, _ *http.Request) {
}
func (t *openStreamFailTransport) ResolveAddress(_ string) ([]Address, error) {
	return []Address{{URL: "ws://dummy"}}, nil
}

func TestOpenStream_AckMismatchError(t *testing.T) {
	tr := &openStreamFailTransport{}
	c := newTestClusterWithTransport(t, tr)
	defer c.Stop()

	dst := newNode(NodeID{1}, "")
	conn, err := c.OpenStream(dst, UserMsg, struct{}{})
	if err == nil || conn != nil {
		t.Fatalf("expected error and nil conn, got conn=%v err=%v", conn, err)
	}
}
