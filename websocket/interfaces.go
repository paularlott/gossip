package websocket

import (
	"io"
	"net"
	"net/http"
)

const (
	TextMessage = iota + 1
	BinaryMessage
)

// WebSocket defines the common interface for WebSocket connections
type WebSocket interface {
	// Core WebSocket operations
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
	NextReader() (messageType int, reader io.Reader, err error)
	Close() error
}

// Provider defines methods for creating WebSocket connections
type Provider interface {
	// Dial connects to a WebSocket server
	Dial(url string) (WebSocket, error)

	// Upgrade upgrades an HTTP connection to WebSocket
	Upgrade(w http.ResponseWriter, r *http.Request) (WebSocket, error)

	// ToNetConn wraps a WebSocket as a net.Conn
	ToNetConn(ws WebSocket) net.Conn
}

// Conn extends net.Conn with WebSocket-specific information
type Conn interface {
	net.Conn
	IsSecure() bool
}
