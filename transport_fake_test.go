package gossip

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
)

// fakeTransport is a lightweight Transport test double.
type fakeTransport struct {
	onSend atomic.Value // of type func(*Packet) error
}

func (f *fakeTransport) PacketChannel() chan *Packet                     { return make(chan *Packet) }
func (f *fakeTransport) DialPeer(node *Node) (net.Conn, error)           { return nil, ErrNoTransportAvailable }
func (f *fakeTransport) WritePacket(conn net.Conn, packet *Packet) error { return nil }
func (f *fakeTransport) ReadPacket(conn net.Conn) (*Packet, error)       { return nil, nil }
func (f *fakeTransport) SendPacket(_ TransportType, _ []*Node, packet *Packet) error {
	if v := f.onSend.Load(); v != nil {
		return v.(func(*Packet) error)(packet)
	}
	return nil
}
func (f *fakeTransport) WebsocketHandler(_ context.Context, _ http.ResponseWriter, _ *http.Request) {}
func (f *fakeTransport) ResolveAddress(addressStr string) ([]Address, error) {
	return []Address{{URL: addressStr}}, nil
}

// SetOnSend sets the send callback in a race-safe manner for tests.
func (f *fakeTransport) SetOnSend(cb func(*Packet) error) { f.onSend.Store(cb) }
