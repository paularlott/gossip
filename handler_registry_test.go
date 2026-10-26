package gossip

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlerRegistryRegisterHandler(t *testing.T) {
	hr := newHandlerRegistry()

	handler := func(*Node, *Packet) error {
		return nil
	}

	// Register handler
	hr.registerHandler(UserMsg, handler)

	// Verify handler is registered
	h := hr.getHandler(UserMsg)
	if h == nil {
		t.Fatal("Handler not registered")
	}

	if h.handler == nil {
		t.Fatal("Handler function not set")
	}

	if h.replyHandler != nil {
		t.Fatal("Reply handler should be nil")
	}
}

func TestHandlerRegistryRegisterHandlerWithReply(t *testing.T) {
	hr := newHandlerRegistry()

	replyHandler := func(*Node, *Packet) (interface{}, error) {
		return "reply", nil
	}

	// Register reply handler
	hr.registerHandlerWithReply(UserMsg+1, replyHandler)

	// Verify handler is registered
	h := hr.getHandler(UserMsg + 1)
	if h == nil {
		t.Fatal("Reply handler not registered")
	}

	if h.handler != nil {
		t.Fatal("Regular handler should be nil")
	}

	if h.replyHandler == nil {
		t.Fatal("Reply handler function not set")
	}
}

func TestHandlerRegistryDuplicateRegistration(t *testing.T) {
	hr := newHandlerRegistry()

	handler := func(*Node, *Packet) error { return nil }

	// Register first handler
	hr.registerHandler(UserMsg, handler)

	// Attempt to register duplicate should panic
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic for duplicate handler registration")
		}
	}()

	hr.registerHandler(UserMsg, handler)
}

func TestHandlerRegistryUnregister(t *testing.T) {
	hr := newHandlerRegistry()

	handler := func(*Node, *Packet) error { return nil }
	hr.registerHandler(UserMsg, handler)

	// Verify handler exists
	if hr.getHandler(UserMsg) == nil {
		t.Fatal("Handler should exist before unregistration")
	}

	// Unregister handler
	if !hr.unregister(UserMsg) {
		t.Fatal("Unregister should return true for existing handler")
	}

	// Verify handler is gone
	if hr.getHandler(UserMsg) != nil {
		t.Fatal("Handler should not exist after unregistration")
	}

	// Unregister non-existent handler
	if hr.unregister(UserMsg) {
		t.Fatal("Unregister should return false for non-existent handler")
	}
}

func TestMsgHandlerDispatchNilPacket(t *testing.T) {
	mh := &msgHandler{
		handler: func(*Node, *Packet) error { return nil },
	}

	err := mh.dispatch(nil, nil, nil)
	if err == nil || err.Error() != "packet is nil" {
		t.Fatal("Should return error for nil packet")
	}
}

func TestMsgHandlerDispatchNoHandler(t *testing.T) {
	mh := &msgHandler{}

	packet := NewPacket()
	defer packet.Release()

	err := mh.dispatch(nil, nil, packet)
	if err == nil || err.Error() != "no handler registered" {
		t.Fatal("Should return error when no handler is registered")
	}
}

func TestMsgHandlerDispatchRegularHandler(t *testing.T) {
	called := false
	var receivedNode *Node
	var receivedPacket *Packet

	mh := &msgHandler{
		handler: func(node *Node, packet *Packet) error {
			called = true
			receivedNode = node
			receivedPacket = packet
			return nil
		},
	}

	node := &Node{ID: NodeID(uuid.New())}
	packet := NewPacket()

	err := mh.dispatch(nil, node, packet)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !called {
		t.Fatal("Handler not called")
	}

	if receivedNode != node {
		t.Fatal("Wrong node passed to handler")
	}

	if receivedPacket != packet {
		t.Fatal("Wrong packet passed to handler")
	}
}

func TestMsgHandlerDispatchRegularHandlerError(t *testing.T) {
	expectedErr := errors.New("handler error")

	mh := &msgHandler{
		handler: func(*Node, *Packet) error {
			return expectedErr
		},
	}

	packet := NewPacket()

	err := mh.dispatch(nil, nil, packet)
	if err != expectedErr {
		t.Fatalf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestMsgHandlerDispatchReplyHandlerError(t *testing.T) {
	expectedErr := errors.New("reply handler error")

	mh := &msgHandler{
		replyHandler: func(*Node, *Packet) (interface{}, error) {
			return nil, expectedErr
		},
	}

	mockConn := &mockConn{}
	packet := NewPacket()
	packet.conn = mockConn

	err := mh.dispatch(nil, nil, packet)
	if err != expectedErr {
		t.Fatalf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestMsgHandlerDispatchReplyHandlerNilReply(t *testing.T) {
	mh := &msgHandler{
		replyHandler: func(*Node, *Packet) (interface{}, error) {
			return nil, nil
		},
	}

	mockConn := &mockConn{}
	packet := NewPacket()
	packet.conn = mockConn

	err := mh.dispatch(nil, nil, packet)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(mockConn.writeData) > 0 {
		t.Fatal("Should not write data for nil reply")
	}
}

// A reply handler's value is sent to a requester that is waiting for it, as a reply from this node; nothing is sent for
// a nil value, an error, or when nobody is waiting, and a reply that cannot be encoded or delivered is an error.
func TestMsgHandlerDispatchReply(t *testing.T) {
	boom := errors.New("handler error")
	tests := []struct {
		name      string
		value     interface{}
		err       error
		waiting   bool
		full      bool // the requester is no longer reading its reply channel
		failCodec bool
		wantErr   bool
		wantReply bool
	}{
		{name: "value", value: map[string]string{"reply": "ok"}, waiting: true, wantReply: true},
		{name: "nil value", waiting: true},
		{name: "error", value: map[string]string{"reply": "ok"}, err: boom, waiting: true, wantErr: true},
		{name: "nobody waiting", value: map[string]string{"reply": "ok"}},
		{name: "requester gone", value: map[string]string{"reply": "ok"}, waiting: true, full: true, wantErr: true},
		{name: "reply cannot be encoded", value: map[string]string{"reply": "ok"}, waiting: true, failCodec: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newUnitCluster(t)
			mh := &msgHandler{replyHandler: func(*Node, *Packet) (interface{}, error) { return tt.value, tt.err }}
			replies := make(chan *Packet, 1)
			if tt.full {
				replies = make(chan *Packet) // nobody receives
			}
			p := NewPacket()
			p.SetCodec(c.config.MsgCodec)
			if tt.failCodec {
				p.SetCodec(&failCodec{})
			}
			if tt.waiting {
				p.SetReplyChan(replies)
			}

			err := mh.dispatch(c, nil, p)

			assert.Equal(t, tt.wantErr, err != nil, "error %v", err)
			if tt.err != nil {
				assert.Equal(t, tt.err, err)
			}
			if tt.wantReply {
				assert.EqualValues(t, 1, refs(p), "the reply holds the packet")
			} else {
				assert.Zero(t, refs(p), "the packet is released on every path")
			}
			select {
			case reply := <-replies:
				require.True(t, tt.wantReply, "an unexpected reply")
				defer reply.Release()
				assert.Equal(t, replyMsg, reply.MessageType)
				assert.Equal(t, c.localNode.ID, reply.SenderID)
				var got map[string]string
				require.NoError(t, reply.Unmarshal(&got))
				assert.Equal(t, map[string]string{"reply": "ok"}, got)
			default:
				assert.False(t, tt.wantReply, "no reply was sent")
			}
		})
	}
}

func TestHandlerRegistryConcurrentAccess(t *testing.T) {
	hr := newHandlerRegistry()

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	var registeredCount atomic.Int64

	// Concurrent registrations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				msgType := MessageType(int(UserMsg) + goroutineID*numOperations + j)
				handler := func(*Node, *Packet) error { return nil }
				hr.registerHandler(msgType, handler)
				registeredCount.Add(1)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				msgType := MessageType(int(UserMsg) + j)
				hr.getHandler(msgType)
			}
		}()
	}

	wg.Wait()

	// Verify all handlers were registered
	handlers := hr.handlers.Load()
	if int64(len(*handlers)) != registeredCount.Load() {
		t.Fatalf("Expected %d handlers, got %d", registeredCount.Load(), len(*handlers))
	}
}

func TestHandlerRegistryConcurrentUnregister(t *testing.T) {
	hr := newHandlerRegistry()

	// Register handlers first
	const numHandlers = 100
	for i := 0; i < numHandlers; i++ {
		msgType := MessageType(int(UserMsg) + i)
		handler := func(*Node, *Packet) error { return nil }
		hr.registerHandler(msgType, handler)
	}

	var wg sync.WaitGroup
	var unregisteredCount atomic.Int64

	// Concurrent unregistrations
	for i := 0; i < numHandlers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			msgType := MessageType(int(UserMsg) + index)
			if hr.unregister(msgType) {
				unregisteredCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if unregisteredCount.Load() != numHandlers {
		t.Fatalf("Expected %d unregistrations, got %d", numHandlers, unregisteredCount.Load())
	}

	// Verify all handlers are gone
	handlers := hr.handlers.Load()
	if len(*handlers) != 0 {
		t.Fatalf("Expected 0 handlers after unregistration, got %d", len(*handlers))
	}
}

func TestPacketReleaseInDispatch(t *testing.T) {
	mh := &msgHandler{
		handler: func(*Node, *Packet) error { return nil },
	}

	packet := NewPacket()
	initialRefCount := packet.refCount.Load()

	err := mh.dispatch(nil, nil, packet)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Packet should be released after dispatch
	if packet.refCount.Load() >= initialRefCount {
		t.Fatal("Packet should be released after dispatch")
	}
}
