package gossip

import (
	"context"
	"sync"
	"testing"
)

type mockCodec struct{}

func (m *mockCodec) Marshal(v interface{}) ([]byte, error) {
	return []byte("mock"), nil
}

func (m *mockCodec) Unmarshal(data []byte, v interface{}) error {
	return nil
}

func (m *mockCodec) Name() string {
	return "mock"
}

func TestNewTransportInterface(t *testing.T) {
	config := DefaultConfig()
	config.Logger = NewNullLogger()
	config.MsgCodec = &mockCodec{}
	config.BindAddr = "127.0.0.1:0"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	transport, err := NewSocketTransport(ctx, &wg, config)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	if transport == nil {
		t.Fatal("Transport should not be nil")
	}

	if transport.PacketChannel() == nil {
		t.Fatal("PacketChannel should not be nil")
	}

	// Test basic interface compliance
	packet := NewPacket()
	packet.MessageType = UserMsg
	packet.SetCodec(config.MsgCodec)
	packet.SetPayload([]byte("test"))
	defer packet.Release()

	node := &Node{ID: NodeID{}}

	// Test Send method
	err = transport.Send(TransportBestEffort, node, packet)
	if err == nil {
		t.Error("Expected error for unresolved node address")
	}

	// Test SendWithReply method
	_, err = transport.SendWithReply(node, packet)
	if err == nil {
		t.Error("Expected error for unresolved node address")
	}

	cancel()
	wg.Wait()
}

func TestTransportTypes(t *testing.T) {
	// Test that transport types are defined correctly
	if TransportBestEffort != 0 {
		t.Errorf("Expected TransportBestEffort to be 0, got %d", TransportBestEffort)
	}

	if TransportReliable != 1 {
		t.Errorf("Expected TransportReliable to be 1, got %d", TransportReliable)
	}
}
