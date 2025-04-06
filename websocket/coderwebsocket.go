package websocket

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	coderws "github.com/coder/websocket"
)

// CoderWebSocketProvider implements WebsocketProvider using coder/websocket
type CoderWebSocketProvider struct {
	// Configuration options
	HandshakeTimeout  time.Duration
	EnableCompression bool
	SkipTLSVerify     bool
	BearerToken       string
}

// coderWebsocketConn wraps a net.Conn to add websocket identification capabilities
type coderWebsocketConn struct {
	net.Conn      // Embed the net.Conn interface
	isSecure bool // Whether this connection is over WSS
}

func (c *coderWebsocketConn) IsSecure() bool {
	return c.isSecure
}

// NewCoderProvider creates a new WebSocket provider using coder/websocket
func NewCoderProvider(timeout time.Duration, skipTLSVerify bool, bearerToken string) *CoderWebSocketProvider {
	return &CoderWebSocketProvider{
		HandshakeTimeout:  timeout,
		EnableCompression: true,
		SkipTLSVerify:     skipTLSVerify,
		BearerToken:       bearerToken,
	}
}

// DialWebsocket implements WebSocketProvider.DialWebsocket
func (p *CoderWebSocketProvider) DialWebsocket(url string) (net.Conn, error) {
	if url == "" {
		return nil, fmt.Errorf("empty WebSocket URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.HandshakeTimeout)
	defer cancel()

	// Set up dialer options
	opts := &coderws.DialOptions{
		CompressionMode: coderws.CompressionDisabled,
	}
	if p.EnableCompression {
		opts.CompressionMode = coderws.CompressionContextTakeover
	}

	if p.BearerToken != "" {
		opts.HTTPHeader.Set("Authorization", "Bearer "+p.BearerToken)
	}

	// Dial the WebSocket server
	wsConn, _, err := coderws.Dial(ctx, url, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to dial WebSocket: %w", err)
	}

	ctx, _ = context.WithTimeout(context.Background(), p.HandshakeTimeout)
	netConn := coderws.NetConn(ctx, wsConn, coderws.MessageBinary)

	return &coderWebsocketConn{
		Conn:     netConn,
		isSecure: strings.HasPrefix(strings.ToLower(url), "wss://"),
	}, nil
}

// UpgradeHTTPToWebsocket implements WebSocketProvider.UpgradeHTTPToWebsocket
func (p *CoderWebSocketProvider) UpgradeHTTPToWebsocket(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	// Set up upgrader options
	opts := &coderws.AcceptOptions{
		CompressionMode:    coderws.CompressionDisabled,
		InsecureSkipVerify: p.SkipTLSVerify,
	}
	if p.EnableCompression {
		opts.CompressionMode = coderws.CompressionContextTakeover
	}

	// Upgrade the HTTP connection to WebSocket
	wsConn, err := coderws.Accept(w, r, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade connection to WebSocket: %w", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), p.HandshakeTimeout)
	conn := coderws.NetConn(ctx, wsConn, coderws.MessageBinary)

	return conn, nil
}
