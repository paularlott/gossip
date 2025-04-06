package websocket

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	gorillaws "github.com/gorilla/websocket"
)

// GorillaWebSocketProvider implements WebsocketProvider using gorilla/websocket
type GorillaWebSocketProvider struct {
	// Configuration options
	ReadBufferSize    int
	WriteBufferSize   int
	HandshakeTimeout  time.Duration
	EnableCompression bool
	SkipTLSVerify     bool
	BearerToken       string
}

// NewGorillaProvider creates a new WebSocket provider using gorilla/websocket
func NewGorillaProvider(timeout time.Duration, skipTLSVerify bool, bearerToken string) *GorillaWebSocketProvider {
	return &GorillaWebSocketProvider{
		ReadBufferSize:    32768,
		WriteBufferSize:   32768,
		HandshakeTimeout:  timeout,
		EnableCompression: true,
		SkipTLSVerify:     skipTLSVerify,
		BearerToken:       bearerToken,
	}
}

// gorillaWebsocketConn adapts a gorilla WebSocket connection to a net.Conn interface
type gorillaWebsocketConn struct {
	conn     *gorillaws.Conn
	reader   io.Reader
	readLock sync.Mutex
	closed   bool
	isSecure bool // Whether this connection is over WSS
}

func (w *gorillaWebsocketConn) IsSecure() bool {
	return w.isSecure
}

// Read implements the net.Conn Read method
func (w *gorillaWebsocketConn) Read(b []byte) (n int, err error) {
	w.readLock.Lock()
	defer w.readLock.Unlock()

	// If we don't have a reader yet, get one
	if w.reader == nil {
		for {
			messageType, reader, err := w.conn.NextReader()
			if err != nil {
				// Convert close errors to EOF to match net.Conn behavior
				if gorillaws.IsCloseError(err, gorillaws.CloseNormalClosure) {
					return 0, io.EOF
				}
				return 0, err
			}

			if messageType != gorillaws.BinaryMessage {
				// Consume and skip non-binary messages
				_, _ = io.Copy(io.Discard, reader)
				continue
			}

			// Found a binary message
			w.reader = reader
			break
		}
	}

	// Read from the current message
	n, err = w.reader.Read(b)

	// If we've reached EOF for this message, clear the reader
	// so we'll get a new one on the next call
	if err == io.EOF {
		w.reader = nil
		return n, nil // Return data without the EOF
	}

	return n, err
}

// Implement rest of net.Conn methods...
func (w *gorillaWebsocketConn) Write(b []byte) (n int, err error) {
	err = w.conn.WriteMessage(gorillaws.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *gorillaWebsocketConn) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	err := w.conn.WriteControl(gorillaws.CloseMessage,
		gorillaws.FormatCloseMessage(gorillaws.CloseNormalClosure, ""),
		time.Now().Add(time.Second))

	closeErr := w.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (w *gorillaWebsocketConn) LocalAddr() net.Addr {
	return w.conn.LocalAddr()
}

func (w *gorillaWebsocketConn) RemoteAddr() net.Addr {
	return w.conn.RemoteAddr()
}

func (w *gorillaWebsocketConn) SetDeadline(t time.Time) error {
	err := w.conn.SetReadDeadline(t)
	if err != nil {
		return err
	}
	return w.conn.SetWriteDeadline(t)
}

func (w *gorillaWebsocketConn) SetReadDeadline(t time.Time) error {
	return w.conn.SetReadDeadline(t)
}

func (w *gorillaWebsocketConn) SetWriteDeadline(t time.Time) error {
	return w.conn.SetWriteDeadline(t)
}

// DialWebsocket implements WebSocketProvider.DialWebsocket
func (p *GorillaWebSocketProvider) DialWebsocket(url string) (net.Conn, error) {
	if url == "" {
		return nil, fmt.Errorf("empty WebSocket URL")
	}

	// Set up dialer with appropriate timeouts
	dialer := gorillaws.Dialer{
		HandshakeTimeout:  p.HandshakeTimeout,
		ReadBufferSize:    p.ReadBufferSize,
		WriteBufferSize:   p.WriteBufferSize,
		EnableCompression: p.EnableCompression,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: p.SkipTLSVerify,
		},
	}

	header := http.Header{}

	if p.BearerToken != "" {
		header.Set("Authorization", "Bearer "+p.BearerToken)
	}

	// Dial the WebSocket server
	wsConn, _, err := dialer.Dial(url, header)
	if err != nil {
		return nil, fmt.Errorf("failed to dial WebSocket: %w", err)
	}

	// Create a net.Conn adapter for the WebSocket connection
	conn := &gorillaWebsocketConn{
		conn:     wsConn,
		isSecure: strings.HasPrefix(strings.ToLower(url), "wss://"),
	}

	return conn, nil
}

// UpgradeHTTPToWebsocket implements WebSocketProvider.UpgradeHTTPToWebsocket
func (p *GorillaWebSocketProvider) UpgradeHTTPToWebsocket(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	upgrader := gorillaws.Upgrader{
		CheckOrigin:       func(r *http.Request) bool { return true },
		ReadBufferSize:    p.ReadBufferSize,
		WriteBufferSize:   p.WriteBufferSize,
		HandshakeTimeout:  p.HandshakeTimeout,
		EnableCompression: p.EnableCompression,
	}

	// Upgrade the HTTP connection to WebSocket
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade connection to WebSocket: %w", err)
	}

	// Create a net.Conn adapter for the WebSocket connection
	conn := &gorillaWebsocketConn{
		conn: wsConn,
	}

	return conn, nil
}
