package gossip

import (
	"context"
	"net"
	"net/http"
)

// fakeTransport is a lightweight Transport test double.
type fakeTransport struct {
	onSend func(packet *Packet) error
}

func (f *fakeTransport) PacketChannel() chan *Packet                     { return make(chan *Packet) }
func (f *fakeTransport) DialPeer(node *Node) (net.Conn, error)           { return nil, ErrNoTransportAvailable }
func (f *fakeTransport) WritePacket(conn net.Conn, packet *Packet) error { return nil }
func (f *fakeTransport) ReadPacket(conn net.Conn) (*Packet, error)       { return nil, nil }
func (f *fakeTransport) SendPacket(_ TransportType, _ []*Node, packet *Packet) error {
	if f.onSend != nil {
		return f.onSend(packet)
	}
	return nil
}
func (f *fakeTransport) WebsocketHandler(_ context.Context, _ http.ResponseWriter, _ *http.Request) {}
func (f *fakeTransport) ResolveAddress(addressStr string) ([]Address, error) {
	return []Address{{URL: addressStr}}, nil
}
