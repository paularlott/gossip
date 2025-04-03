package gossip

import (
	"net"
	"testing"
	"time"
)

// Mock types for testing
type mockNode struct {
	ID string
}

func (m *mockNode) String() string {
	return m.ID
}

type mockPacket struct {
	ID          string
	MessageType MessageType
}

type mockConn struct {
	net.Conn
	isClosed bool
}

func newMockConn() *mockConn {
	return &mockConn{}
}

func (m *mockConn) Close() error {
	m.isClosed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	return len(b), nil
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

// Test creating a new handler registry
func TestNewHandlerRegistry(t *testing.T) {
	registry := newHandlerRegistry()

	if registry == nil {
		t.Fatal("newHandlerRegistry returned nil")
	}

	// Check that the initial handlers map is empty
	handlers := registry.handlers.Load().(map[MessageType]msgHandler)
	if len(handlers) != 0 {
		t.Errorf("Expected empty handlers map, got %d items", len(handlers))
	}
}

// Test registering and retrieving a basic handler
func TestRegisterHandler(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(1)

	handlerCalled := false
	handler := func(n *Node, p *Packet) error {
		handlerCalled = true
		return nil
	}

	// Register the handler
	registry.registerHandler(msgType, false, handler)

	// Verify it was registered
	msgHandler := registry.getHandler(msgType)
	if msgHandler == nil {
		t.Fatal("Failed to retrieve registered handler")
	}

	if msgHandler.forward != false {
		t.Error("Forward flag not set correctly")
	}

	if msgHandler.handler == nil {
		t.Error("Handler function not stored")
	}

	if msgHandler.replyHandler != nil {
		t.Error("Connection handler should be nil")
	}

	// Test calling the handler
	node := &Node{}
	packet := &Packet{MessageType: msgType}
	msgHandler.dispatch(nil, nil, node, packet)

	if !handlerCalled {
		t.Error("Handler was not called during dispatch")
	}
}

// Test registering and retrieving a connection handler
func TestRegisterConnHandler(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(2)

	handlerCalled := false
	handler := func(n *Node, p *Packet) (MessageType, interface{}, error) {
		handlerCalled = true
		return nilMsg, nil, nil
	}

	// Register the handler
	registry.registerHandlerWithReply(msgType, handler)

	// Verify it was registered
	msgHandler := registry.getHandler(msgType)
	if msgHandler == nil {
		t.Fatal("Failed to retrieve registered connection handler")
	}

	if msgHandler.forward != false {
		t.Error("Forward flag not set correctly")
	}

	if msgHandler.replyHandler == nil {
		t.Error("Connection handler function not stored")
	}

	if msgHandler.handler != nil {
		t.Error("Regular handler should be nil")
	}

	// Test calling the handler
	node := &Node{}
	packet := &Packet{MessageType: msgType}
	conn := newMockConn()
	msgHandler.dispatch(conn, nil, node, packet)

	if !handlerCalled {
		t.Error("Connection handler was not called during dispatch")
	}
}

// Test dispatcher with nil connection
func TestDispatchWithNilConnection(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(3)

	regularCalled := false
	connCalled := false

	// Register both handlers for the same message type
	registry.register(msgType, msgHandler{
		handler: func(n *Node, p *Packet) error {
			regularCalled = true
			return nil
		},
		replyHandler: func(n *Node, p *Packet) (MessageType, interface{}, error) {
			connCalled = true
			return nilMsg, nil, nil
		},
	})

	// Get the handler
	msgHandler := registry.getHandler(msgType)
	if msgHandler == nil {
		t.Fatal("Failed to retrieve registered handlers")
	}

	// Test dispatching with nil connection
	node := &Node{}
	packet := &Packet{MessageType: msgType}
	msgHandler.dispatch(nil, nil, node, packet)

	if !regularCalled {
		t.Error("Regular handler was not called when connection is nil")
	}

	if connCalled {
		t.Error("Connection handler was incorrectly called when connection is nil")
	}
}

// Test dispatcher with connection
func TestDispatchWithConnection(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(4)

	regularCalled := false
	connCalled := false

	// Register both handlers for the same message type
	registry.register(msgType, msgHandler{
		handler: func(n *Node, p *Packet) error {
			regularCalled = true
			return nil
		},
		replyHandler: func(n *Node, p *Packet) (MessageType, interface{}, error) {
			connCalled = true
			return nilMsg, nil, nil
		},
	})

	// Get the handler
	msgHandler := registry.getHandler(msgType)
	if msgHandler == nil {
		t.Fatal("Failed to retrieve registered handlers")
	}

	// Test dispatching with connection
	node := &Node{}
	packet := &Packet{MessageType: msgType}
	conn := newMockConn()
	msgHandler.dispatch(conn, nil, node, packet)

	if regularCalled {
		t.Error("Regular handler was incorrectly called when connection is available")
	}

	if !connCalled {
		t.Error("Connection handler was not called when connection is available")
	}
}

// Test retrieval of non-existent handler
func TestGetNonExistentHandler(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(5)

	// Try to get a handler for a message type that hasn't been registered
	msgHandler := registry.getHandler(msgType)

	if msgHandler != nil {
		t.Error("getHandler returned non-nil for unregistered message type")
	}
}

// Test for concurrent reading
func TestConcurrentReading(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(7)

	// Register a handler
	registry.registerHandler(msgType, false, func(n *Node, p *Packet) error {
		return nil
	})

	// Number of concurrent reads to perform
	const iterations = 100

	// Create a wait group to synchronize goroutines
	done := make(chan struct{})

	// Get handlers concurrently
	for i := 0; i < iterations; i++ {
		go func() {
			msgHandler := registry.getHandler(msgType)
			if msgHandler == nil {
				t.Error("Failed to retrieve handler in concurrent read")
			}
			done <- struct{}{}
		}()
	}

	// Wait for all reads to complete
	for i := 0; i < iterations; i++ {
		select {
		case <-done:
			// Read completed
		case <-time.After(2 * time.Second):
			t.Fatal("Test timed out - possible issue in concurrent reading")
		}
	}
}

// Test a case with no appropriate handler
func TestDispatchNoMatchingHandler(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(8)

	// Register only a connection handler
	connCalled := false
	registry.registerHandlerWithReply(msgType, func(n *Node, p *Packet) (MessageType, interface{}, error) {
		connCalled = true
		return nilMsg, nil, nil
	})

	// Get the handler
	msgHandler := registry.getHandler(msgType)
	if msgHandler == nil {
		t.Fatal("Failed to retrieve registered handler")
	}

	// Test dispatching with nil connection when only a connection handler exists
	node := &Node{}
	packet := &Packet{MessageType: msgType}
	msgHandler.dispatch(nil, nil, node, packet)

	// Connection handler should not be called since connection is nil and no regular handler exists
	if connCalled {
		t.Error("Connection handler should not be called with nil connection")
	}
}

// Test dispatching with nil node or packet
func TestDispatchWithNilParameters(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(9)

	receivedNode := false
	receivedPacket := false

	registry.registerHandler(msgType, false, func(n *Node, p *Packet) error {
		if n != nil {
			receivedNode = true
		}
		if p != nil {
			receivedPacket = true
		}
		return nil
	})

	// Get the handler
	msgHandler := registry.getHandler(msgType)
	if msgHandler == nil {
		t.Fatal("Failed to retrieve registered handler")
	}

	// Test with nil node
	msgHandler.dispatch(nil, nil, nil, &Packet{MessageType: msgType})
	if receivedNode {
		t.Error("Handler received non-nil node when nil was passed")
	}
	if !receivedPacket {
		t.Error("Handler did not receive packet")
	}

	// Reset
	receivedNode = false
	receivedPacket = false

	// Test with nil packet
	msgHandler.dispatch(nil, nil, &Node{}, nil)
	if !receivedNode {
		t.Error("Handler did not receive node")
	}
	if receivedPacket {
		t.Error("Handler received non-nil packet when nil was passed")
	}
}

// Test handler registration with only a conn handler and dispatch with no conn
func TestConnectionOnlyHandlerWithNoConn(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(10)

	handlerCalled := false

	// Register only a connection handler
	registry.registerHandlerWithReply(msgType, func(n *Node, p *Packet) (MessageType, interface{}, error) {
		handlerCalled = true
		return nilMsg, nil, nil
	})

	// Get the handler
	msgHandler := registry.getHandler(msgType)
	if msgHandler == nil {
		t.Fatal("Failed to retrieve registered handler")
	}

	// Test dispatch with no connection
	node := &Node{}
	packet := &Packet{MessageType: msgType}
	msgHandler.dispatch(nil, nil, node, packet)

	// The handler should not be called
	if handlerCalled {
		t.Error("Connection handler was incorrectly called when no connection was provided")
	}
}

// Test handler registration with only a regular handler and dispatch with conn
func TestRegularOnlyHandlerWithConn(t *testing.T) {
	registry := newHandlerRegistry()
	const msgType = MessageType(11)

	handlerCalled := false

	// Register only a regular handler
	registry.registerHandler(msgType, false, func(n *Node, p *Packet) error {
		handlerCalled = true
		return nil
	})

	// Get the handler
	msgHandler := registry.getHandler(msgType)
	if msgHandler == nil {
		t.Fatal("Failed to retrieve registered handler")
	}

	// Test dispatch with connection
	node := &Node{}
	packet := &Packet{MessageType: msgType}
	conn := newMockConn()
	msgHandler.dispatch(conn, nil, node, packet)

	// The regular handler should be called since there's no connection handler
	if !handlerCalled {
		t.Error("Regular handler was not called when connection was provided but no connection handler exists")
	}
}
