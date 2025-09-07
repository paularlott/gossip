package gossip

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
)

// Mock connection for testing
type mockConn struct {
	writeData []byte
	closed    bool
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestNewHandlerRegistry(t *testing.T) {
	hr := newHandlerRegistry()

	if hr == nil {
		t.Fatal("newHandlerRegistry returned nil")
	}

	if hr.handlers.Load() == nil {
		t.Fatal("handlers map not initialized")
	}

	if len(*hr.handlers.Load()) != 0 {
		t.Fatal("handlers map should be empty initially")
	}
}

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

func TestHandlerRegistryGetHandlerNonExistent(t *testing.T) {
	hr := newHandlerRegistry()

	// Get non-existent handler
	h := hr.getHandler(UserMsg + 999)
	if h != nil {
		t.Fatal("Should return nil for non-existent handler")
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

func TestMsgHandlerDispatchReplyHandler(t *testing.T) {
	called := false
	expectedReply := "test reply"

	mh := &msgHandler{
		replyHandler: func(*Node, *Packet) (interface{}, error) {
			called = true
			return expectedReply, nil
		},
	}

	// Create mock cluster and transport
	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	mockConn := &mockConn{}
	packet := NewPacket()
	packet.conn = mockConn
	packet.codec = config.MsgCodec

	err = mh.dispatch(cluster, nil, packet)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !called {
		t.Fatal("Reply handler not called")
	}

	if len(mockConn.writeData) == 0 {
		t.Fatal("No reply data written")
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

func TestHandlerRegistryMultipleHandlers(t *testing.T) {
	hr := newHandlerRegistry()

	// Register multiple handlers
	handler1 := func(*Node, *Packet) error { return nil }
	handler2 := func(*Node, *Packet) error { return nil }
	replyHandler := func(*Node, *Packet) (interface{}, error) { return nil, nil }

	hr.registerHandler(UserMsg, handler1)
	hr.registerHandler(UserMsg+1, handler2)
	hr.registerHandlerWithReply(UserMsg+2, replyHandler)

	// Verify all handlers are registered
	h1 := hr.getHandler(UserMsg)
	h2 := hr.getHandler(UserMsg + 1)
	h3 := hr.getHandler(UserMsg + 2)

	if h1 == nil || h2 == nil || h3 == nil {
		t.Fatal("Not all handlers registered")
	}

	if h1.handler == nil || h2.handler == nil || h3.replyHandler == nil {
		t.Fatal("Handler functions not set correctly")
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

// Benchmarks

func BenchmarkHandlerRegistryRegister(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}
}

func BenchmarkHandlerRegistryGetHandler(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	// Pre-populate with handlers
	for i := 0; i < 1000; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgType := MessageType(int(UserMsg) + (i % 1000))
		hr.getHandler(msgType)
	}
}

func BenchmarkHandlerRegistryUnregister(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	// Pre-populate with handlers
	for i := 0; i < b.N; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.unregister(msgType)
	}
}

func BenchmarkMsgHandlerDispatch(b *testing.B) {
	mh := &msgHandler{
		handler: func(*Node, *Packet) error { return nil },
	}

	node := &Node{ID: NodeID(uuid.New())}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packet := NewPacket()
		mh.dispatch(nil, node, packet)
	}
}

func BenchmarkMsgHandlerDispatchReply(b *testing.B) {
	mh := &msgHandler{
		replyHandler: func(*Node, *Packet) (interface{}, error) {
			return "reply", nil
		},
	}

	config := DefaultConfig()
	config.MsgCodec = codec.NewJsonCodec()

	cluster, err := NewCluster(config)
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}

	node := &Node{ID: NodeID(uuid.New())}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packet := NewPacket()
		packet.conn = &mockConn{}
		packet.codec = config.MsgCodec
		mh.dispatch(cluster, node, packet)
	}
}

func BenchmarkHandlerRegistryConcurrentRead(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	// Pre-populate with handlers
	for i := 0; i < 1000; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msgType := MessageType(int(UserMsg) + (b.N % 1000))
			hr.getHandler(msgType)
		}
	})
}

func BenchmarkHandlerRegistryConcurrentWrite(b *testing.B) {
	handler := func(*Node, *Packet) error { return nil }

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		hr := newHandlerRegistry()
		i := 0
		for pb.Next() {
			msgType := MessageType(int(UserMsg) + i)
			hr.registerHandler(msgType, handler)
			i++
		}
	})
}

func BenchmarkHandlerRegistryMixed(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	// Pre-populate
	for i := 0; i < 100; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 3 {
		case 0:
			msgType := MessageType(int(UserMsg) + 1000 + i)
			hr.registerHandler(msgType, handler)
		case 1:
			msgType := MessageType(int(UserMsg) + (i % 100))
			hr.getHandler(msgType)
		case 2:
			msgType := MessageType(int(UserMsg) + (i % 100))
			hr.unregister(msgType)
		}
	}
}
