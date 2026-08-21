package gossip

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paularlott/logger"
)

const (
	replyExpectedFlag    = 0x4000
	headerSizeMask       = 0x3FFF
	transportMaxWaitTime = 5 * time.Second
	// readBoundedBodyFallback is used when TCPMaxPacketSize is not configured.
	readBoundedBodyFallback = 1 << 20 // 1 MiB
)

type HTTPTransport struct {
	config        *Config
	logger        logger.Logger
	packetChannel chan *Packet
	client        *http.Client

	// ctx is the parent context for outbound requests. It defaults to
	// context.Background and is replaced with the cluster's shutdown context
	// when Start is called, so in-flight requests are cancelled on shutdown.
	ctx context.Context
}

func NewHTTPTransport(config *Config) *HTTPTransport {
	// Per-stage timeouts; the overall request budget is bounded by the
	// per-call context derived in sendRequest. No http.Client.Timeout is set
	// so that a slow body read cannot eat into the dial/handshake budget.
	timeout := transportMaxWaitTime
	if config.TCPDialTimeout > 0 {
		timeout = config.TCPDialTimeout
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   timeout,          // Connection timeout
			KeepAlive: 30 * time.Second, // Keep-alive probe interval
		}).DialContext,
		TLSHandshakeTimeout:   timeout,          // TLS handshake timeout
		ResponseHeaderTimeout: timeout,          // Time to receive response headers
		ExpectContinueTimeout: 1 * time.Second,  // Time to wait for 100-continue
		IdleConnTimeout:       90 * time.Second, // How long idle connections stay open
		MaxIdleConns:          100,              // Max idle connections across all hosts
		MaxIdleConnsPerHost:   10,               // Max idle connections per host
		MaxConnsPerHost:       50,               // Max total connections per host
		DisableKeepAlives:     false,            // Enable connection reuse
		ForceAttemptHTTP2:     true,             // Multiplex in-flight requests when TLS is used
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: config.InsecureSkipVerify,
		},
	}

	// Create logger with transport group
	var lgr logger.Logger
	if config.Logger != nil {
		lgr = config.Logger.WithGroup("gossip")
	} else {
		lgr = logger.NewNullLogger()
	}

	return &HTTPTransport{
		config:        config,
		logger:        lgr,
		packetChannel: make(chan *Packet, config.IncomingPacketQueueDepth),
		client: &http.Client{
			Transport: transport,
		},
		ctx: context.Background(),
	}
}

func (ht *HTTPTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	// Adopt the cluster's shutdown context so that in-flight requests are
	// cancelled when the cluster shuts down.
	ht.ctx = ctx
	return nil
}

func (ht *HTTPTransport) PacketChannel() chan *Packet {
	return ht.packetChannel
}

// requestTimeout returns the per-request deadline derived from the config.
func (ht *HTTPTransport) requestTimeout() time.Duration {
	if ht.config.TCPDialTimeout > 0 {
		return ht.config.TCPDialTimeout
	}
	return transportMaxWaitTime
}

// sendRequest is the shared request builder used by Send and SendWithReply.
// It resolves the node address, builds the request with auth headers, and
// executes it. The caller is responsible for closing the returned response
// body (and draining it if connection reuse is desired).
func (ht *HTTPTransport) sendRequest(node *Node, packet *Packet, replyExpected bool) (*http.Response, error) {
	rawPacket, err := ht.packetToBuffer(packet, replyExpected)
	if err != nil {
		return nil, err
	}

	if err := ht.ensureNodeAddressResolved(node); err != nil {
		return nil, fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	ctx, cancel := context.WithTimeout(ht.ctx, ht.requestTimeout())

	addr := node.GetAddress()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr.URL, bytes.NewReader(rawPacket))
	if err != nil {
		cancel()
		node.ClearAddress()
		return nil, err
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	if ht.config.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+ht.config.BearerToken)
	}

	resp, err := ht.client.Do(req)
	if err != nil {
		cancel()
		node.ClearAddress()
		return nil, err
	}

	// Wrap the body so that closing it also cancels the per-request context.
	resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

func (ht *HTTPTransport) Send(transportType TransportType, node *Node, packet *Packet) error {
	resp, err := ht.sendRequest(node, packet, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Drain the body so the underlying connection is returned to the pool.
	io.Copy(io.Discard, resp.Body)
	return nil
}

func (ht *HTTPTransport) Name() string {
	return "http"
}

func (ht *HTTPTransport) SendWithReply(node *Node, packet *Packet) (*Packet, error) {
	resp, err := ht.sendRequest(node, packet, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		io.Copy(io.Discard, resp.Body)
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	body, err := readBoundedBody(resp.Body, resp.ContentLength, int64(ht.config.TCPMaxPacketSize))
	if err != nil {
		return nil, err
	}

	return ht.packetFromBuffer(body)
}

func (ht *HTTPTransport) HandleGossipRequest(w http.ResponseWriter, r *http.Request) {
	if ht.config.BearerToken != "" {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}

		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(authHeader, bearerPrefix) {
			http.Error(w, "Invalid authorization format", http.StatusUnauthorized)
			return
		}

		token := authHeader[len(bearerPrefix):]
		if token != ht.config.BearerToken {
			http.Error(w, "Invalid bearer token", http.StatusUnauthorized)
			return
		}
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read with a size cap to prevent memory exhaustion.
	body, err := readBoundedBody(r.Body, r.ContentLength, int64(ht.config.TCPMaxPacketSize))
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	packet, err := ht.packetFromBuffer(body)
	if err != nil {
		ht.logger.WithError(err).Error("failed to decode incoming packet")
		http.Error(w, "Invalid packet format", http.StatusBadRequest)
		return
	}

	flags := binary.LittleEndian.Uint16(body[:2])
	replyExpected := flags&replyExpectedFlag != 0

	if replyExpected {
		replyChan := make(chan *Packet, 1)
		packet.SetReplyChan(replyChan)
		defer close(replyChan)

		select {
		case ht.packetChannel <- packet:
			select {
			case replyPacket := <-replyChan:
				replyData, err := ht.packetToBuffer(replyPacket, false)
				if err != nil {
					http.Error(w, "Failed to encode reply", http.StatusInternalServerError)
					replyPacket.Release()
					return
				}

				w.Header().Set("Content-Type", "application/octet-stream")
				w.WriteHeader(http.StatusOK)
				w.Write(replyData)
				replyPacket.Release()

			case <-time.After(transportMaxWaitTime):
				w.WriteHeader(http.StatusNoContent)
				return

			case <-r.Context().Done():
				return
			}

		default:
			http.Error(w, "Server busy", http.StatusServiceUnavailable)
			packet.Release()
		}
	} else {
		select {
		case ht.packetChannel <- packet:
			w.WriteHeader(http.StatusNoContent)
		default:
			packet.Release()
			http.Error(w, "Server busy", http.StatusServiceUnavailable)
		}
	}
}

func (ht *HTTPTransport) ensureNodeAddressResolved(node *Node) error {
	if !node.IsAddressEmpty() {
		return nil
	}

	if node.AdvertisedAddr() == "" {
		return fmt.Errorf("no advertise address available")
	}

	uri := node.AdvertisedAddr()

	var err error
	var u *url.URL

	// If url starts with srv+ then remove it and resolve the actual url
	if strings.HasPrefix(uri, "srv+") || strings.HasPrefix(uri, "SRV+") {
		// Parse the url excluding the srv+ prefix
		u, err = url.Parse(uri[4:])
		if err != nil {
			return fmt.Errorf("failed to parse SRV URL %s: %v", uri[4:], err)
		}

		srv, err := ht.config.Resolver.LookupSRV(u.Host)
		if err != nil {
			return fmt.Errorf("failed to lookup SRV record for %s: %v", u.Host, err)
		}

		if len(srv) == 0 {
			return fmt.Errorf("no SRV records found for %s", u.Host)
		}

		// Update the URL with the service-selected port and hostname for SNI
		host := net.JoinHostPort(u.Hostname(), strconv.Itoa(int(srv[0].Port)))
		u.Host = host
	} else {
		u, err = url.Parse(uri)
		if err != nil {
			return fmt.Errorf("failed to parse URL %s: %v", uri, err)
		}
	}

	// Replace path
	if ht.config.BindAddr == "" {
		u.Path = "/"
	} else {
		u.Path = ht.config.BindAddr
	}

	uri = u.String()

	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		uri = "https://" + uri
	}

	node.SetAddress(Address{URL: uri})
	return nil
}

// packetToBuffer encodes a packet into the wire format:
//
//	[2 bytes: flags+headerSize][header bytes][payload bytes]
//
// The flags occupy the high two bits (compression, reply-expected) and the
// header size occupies the low 14 bits. When a Compressor is configured and
// the payload is large enough, the payload is compressed and the
// compressionFlag bit is set.
//
// Encryption is intentionally NOT applied here: HTTPS already provides
// transport-layer encryption.
func (ht *HTTPTransport) packetToBuffer(packet *Packet, replyExpected bool) ([]byte, error) {
	headerBytes, err := ht.config.MsgCodec.Marshal(packet)
	if err != nil {
		return nil, err
	}

	flags := uint16(len(headerBytes))
	if replyExpected {
		flags |= replyExpectedFlag
	}

	payload := packet.Payload()
	if ht.config.Compressor != nil && len(payload) >= ht.config.CompressMinSize {
		compressed, cerr := ht.config.Compressor.Compress(payload)
		if cerr != nil {
			return nil, cerr
		}
		if len(compressed) < len(payload) {
			flags |= compressionFlag
			payload = compressed
		}
	}

	// Pre-size once: 2 (flags) + header + payload.
	buf := make([]byte, 2+len(headerBytes)+len(payload))
	binary.LittleEndian.PutUint16(buf, flags)
	copy(buf[2:], headerBytes)
	copy(buf[2+len(headerBytes):], payload)
	return buf, nil
}

func (ht *HTTPTransport) packetFromBuffer(data []byte) (*Packet, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("packet too small")
	}

	flags := binary.LittleEndian.Uint16(data[:2])
	isCompressed := flags&compressionFlag != 0
	headerSize := flags & headerSizeMask

	if len(data) < int(headerSize)+2 {
		return nil, fmt.Errorf("packet too small for header")
	}

	body := data[2:]
	payload := body[headerSize:]

	if isCompressed {
		if ht.config.Compressor == nil {
			return nil, fmt.Errorf("packet is compressed but no compressor configured")
		}
		decompressed, err := ht.config.Compressor.Decompress(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress packet: %w", err)
		}
		payload = decompressed
	}

	packet := NewPacket()
	if err := ht.config.MsgCodec.Unmarshal(body[:headerSize], &packet); err != nil {
		packet.Release()
		return nil, err
	}

	packet.SetCodec(ht.config.MsgCodec)
	packet.SetPayload(payload)

	return packet, nil
}

// readBoundedBody reads up to max bytes from r, pre-allocating based on
// contentLength when it is present and within the cap. It is robust to a
// missing or incorrect Content-Length: a wrong hint wastes at most one
// allocation and never reads more than max bytes, so callers are protected
// from memory exhaustion regardless of what the peer claims.
func readBoundedBody(r io.Reader, contentLength int64, max int64) ([]byte, error) {
	if max <= 0 {
		max = readBoundedBodyFallback
	}

	var buf bytes.Buffer
	if contentLength > 0 && contentLength <= max {
		// Hint capacity to avoid regrowth on the common path.
		buf.Grow(int(contentLength))
	}

	if _, err := buf.ReadFrom(io.LimitReader(r, max)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// cancelBody wraps a response body so that Close also cancels the per-request
// context. This keeps the request lifecycle tied to body ownership without
// requiring callers to manage a cancel func separately.
type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelBody) Close() error {
	c.cancel()
	return c.ReadCloser.Close()
}
