package gossip

import (
	"bytes"
	"context"
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
)

type HTTPTransport struct {
	config        *Config
	logger        logger.Logger
	packetChannel chan *Packet
	client        *http.Client
}

func NewHTTPTransport(config *Config) *HTTPTransport {
	// Use configured timeout or default to 5 seconds
	timeout := transportMaxWaitTime
	if config.TCPDialTimeout > 0 {
		timeout = config.TCPDialTimeout
	}

	// Create a custom transport with proper timeouts and connection limits
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
		ForceAttemptHTTP2:     true,             // Try HTTP/2
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
			Timeout:   timeout,
			Transport: transport,
		},
	}
}

func (ht *HTTPTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	return nil
}

func (ht *HTTPTransport) PacketChannel() chan *Packet {
	return ht.packetChannel
}

func (ht *HTTPTransport) Send(transportType TransportType, node *Node, packet *Packet) error {
	rawPacket, err := ht.packetToBuffer(packet, false)
	if err != nil {
		return err
	}

	if err := ht.ensureNodeAddressResolved(node); err != nil {
		return fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ht.client.Timeout)
	defer cancel()

	// Fire and forget HTTP POST
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, node.Address().URL, bytes.NewReader(rawPacket))
	if err != nil {
		node.Address().Clear()
		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	if ht.config.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+ht.config.BearerToken)
	}

	resp, err := ht.client.Do(req)
	if err != nil {
		node.Address().Clear()
		return err
	}
	resp.Body.Close()

	return nil
}

func (ht *HTTPTransport) Name() string {
	return "http"
}

func (ht *HTTPTransport) SendWithReply(node *Node, packet *Packet) (*Packet, error) {
	rawPacket, err := ht.packetToBuffer(packet, true)
	if err != nil {
		return nil, err
	}

	if err := ht.ensureNodeAddressResolved(node); err != nil {
		return nil, fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ht.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, node.Address().URL, bytes.NewReader(rawPacket))
	if err != nil {
		node.Address().Clear()
		return nil, err
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	if ht.config.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+ht.config.BearerToken)
	}

	resp, err := ht.client.Do(req)
	if err != nil {
		node.Address().Clear()
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	// Read with a size limit to prevent memory exhaustion
	limitedReader := io.LimitReader(resp.Body, int64(ht.config.TCPMaxPacketSize))
	replyBody, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, err
	}

	return ht.packetFromBuffer(replyBody)
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

	// Read with a size limit to prevent memory exhaustion
	limitedReader := io.LimitReader(r.Body, int64(ht.config.TCPMaxPacketSize))
	body, err := io.ReadAll(limitedReader)
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

	flags := binary.BigEndian.Uint16(body[:2])
	replyExpected := flags&replyExpectedFlag != 0

	if replyExpected {
		replyChan := make(chan *Packet, 1)
		packet.SetReplyChan(replyChan)

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

				// Try to drain any reply that might arrive to prevent leak
				go func() {
					select {
					case replyPacket := <-replyChan:
						replyPacket.Release()
					case <-time.After(1 * time.Second):
						// Give up after 1 second
					}
					close(replyChan)
				}()

				return

			case <-r.Context().Done():
				// Client disconnected, drain any pending reply
				go func() {
					select {
					case replyPacket := <-replyChan:
						replyPacket.Release()
					case <-time.After(1 * time.Second):
						// Give up after 1 second
					}
					close(replyChan)
				}()
				return
			}

		default:
			http.Error(w, "Server busy", http.StatusServiceUnavailable)
			packet.Release()
		}

		close(replyChan)
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
	if !node.Address().IsEmpty() {
		return nil
	}

	if node.AdvertiseAddr() == "" {
		return fmt.Errorf("no advertise address available")
	}

	uri := node.AdvertiseAddr()

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

	*node.Address() = Address{URL: uri}
	return nil
}

func (ht *HTTPTransport) packetToBuffer(packet *Packet, replyExpected bool) ([]byte, error) {
	headerBytes, err := ht.config.MsgCodec.Marshal(packet)
	if err != nil {
		return nil, err
	}

	headerSize := uint16(len(headerBytes))
	if replyExpected {
		headerSize |= replyExpectedFlag
	}

	var buf bytes.Buffer

	err = binary.Write(&buf, binary.BigEndian, headerSize)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(headerBytes)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(packet.Payload())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (ht *HTTPTransport) packetFromBuffer(data []byte) (*Packet, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("packet too small")
	}

	flags := binary.BigEndian.Uint16(data[:2])
	headerSize := flags & headerSizeMask

	if len(data) < int(headerSize)+2 {
		return nil, fmt.Errorf("packet too small for header")
	}

	packet := NewPacket()
	err := ht.config.MsgCodec.Unmarshal(data[2:2+headerSize], &packet)
	if err != nil {
		return nil, err
	}

	packet.SetCodec(ht.config.MsgCodec)

	if len(data) > int(headerSize)+2 {
		packet.SetPayload(data[2+headerSize:])
	}

	return packet, nil
}
