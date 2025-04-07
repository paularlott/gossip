// websocket/coder.go
package websocket

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	coderws "github.com/coder/websocket"
)

// coderWebsocketConn wraps a net.Conn to add websocket identification capabilities
type coderWebsocketConn struct {
	net.Conn      // Embed the net.Conn interface
	isSecure bool // Whether this connection is over WSS
}

func (c *coderWebsocketConn) IsSecure() bool {
	return c.isSecure
}

// CoderWebSocket implements the WebSocket interface for coder/websocket
type CoderWebSocket struct {
	conn     *coderws.Conn
	ctx      context.Context
	isSecure bool
}

// WriteMessage implements WebSocket.WriteMessage
func (c *CoderWebSocket) WriteMessage(messageType int, data []byte) error {
	return c.conn.Write(c.ctx, coderws.MessageType(messageType), data)
}

// ReadMessage implements WebSocket.ReadMessage
func (c *CoderWebSocket) ReadMessage() (messageType int, p []byte, err error) {
	typ, r, err := c.conn.Reader(c.ctx)
	if err != nil {
		return 0, nil, err
	}

	data, err := io.ReadAll(r)
	return int(typ), data, err
}

// NextReader implements WebSocket.NextReader
func (c *CoderWebSocket) NextReader() (messageType int, reader io.Reader, err error) {
	mt, reader, err := c.conn.Reader(c.ctx)
	return int(mt), reader, err
}

// Close implements WebSocket.Close
func (c *CoderWebSocket) Close() error {
	return c.conn.Close(coderws.StatusNormalClosure, "")
}

// CoderProvider implements Provider for coder/websocket
type CoderProvider struct {
	HandshakeTimeout  time.Duration
	EnableCompression bool
	SkipTLSVerify     bool
	BearerToken       string
}

// NewCoderProvider creates a new Provider using coder/websocket
func NewCoderProvider(timeout time.Duration, skipTLSVerify bool, bearerToken string) *CoderProvider {
	return &CoderProvider{
		HandshakeTimeout:  timeout,
		EnableCompression: true,
		SkipTLSVerify:     skipTLSVerify,
		BearerToken:       bearerToken,
	}
}

// Dial implements Provider.Dial
func (p *CoderProvider) Dial(url string) (WebSocket, error) {
	if url == "" {
		return nil, fmt.Errorf("empty WebSocket URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.HandshakeTimeout)
	defer cancel()

	opts := &coderws.DialOptions{
		CompressionMode: coderws.CompressionDisabled,
	}
	if p.EnableCompression {
		opts.CompressionMode = coderws.CompressionContextTakeover
	}

	if p.BearerToken != "" {
		opts.HTTPHeader.Set("Authorization", "Bearer "+p.BearerToken)
	}

	wsConn, _, err := coderws.Dial(ctx, url, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to dial WebSocket: %w", err)
	}

	// Create context for the websocket
	connCtx, _ := context.WithTimeout(context.Background(), p.HandshakeTimeout)

	return &CoderWebSocket{
		conn:     wsConn,
		ctx:      connCtx,
		isSecure: strings.HasPrefix(strings.ToLower(url), "wss://"),
	}, nil
}

// Upgrade implements Provider.Upgrade
func (p *CoderProvider) Upgrade(w http.ResponseWriter, r *http.Request) (WebSocket, error) {
	opts := &coderws.AcceptOptions{
		CompressionMode:    coderws.CompressionDisabled,
		InsecureSkipVerify: p.SkipTLSVerify,
	}
	if p.EnableCompression {
		opts.CompressionMode = coderws.CompressionContextTakeover
	}

	wsConn, err := coderws.Accept(w, r, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade connection to WebSocket: %w", err)
	}

	connCtx, _ := context.WithTimeout(context.Background(), p.HandshakeTimeout)

	return &CoderWebSocket{
		conn:     wsConn,
		ctx:      connCtx,
		isSecure: r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https"),
	}, nil
}

// ToNetConn implements Provider.ToNetConn
func (p *CoderProvider) ToNetConn(ws WebSocket) net.Conn {
	coderWS, ok := ws.(*CoderWebSocket)
	if !ok {
		panic("websocket is not a CoderWebSocket")
	}

	netConn := coderws.NetConn(coderWS.ctx, coderWS.conn, coderws.MessageBinary)
	return &coderWebsocketConn{
		Conn:     netConn,
		isSecure: coderWS.isSecure,
	}
}
