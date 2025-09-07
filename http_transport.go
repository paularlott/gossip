package gossip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	replyExpectedFlag    = 0x4000
	headerSizeMask       = 0x3FFF
	transportMaxWaitTime = 5 * time.Second
)

type HTTPTransport struct {
	config        *Config
	packetChannel chan *Packet
	client        *http.Client
}

func NewHTTPTransport(config *Config) *HTTPTransport {
	return &HTTPTransport{
		config:        config,
		packetChannel: make(chan *Packet, config.IncomingPacketQueueDepth),
		client: &http.Client{
			Timeout: transportMaxWaitTime,
		},
	}
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

	// Fire and forget HTTP POST
	go func(n *Node) {
		req, err := http.NewRequest(http.MethodPost, n.Address().URL, bytes.NewReader(rawPacket))
		if err != nil {
			n.Address().Clear()
			return
		}

		req.Header.Set("Content-Type", "application/octet-stream")

		if ht.config.BearerToken != "" {
			req.Header.Set("Authorization", "Bearer "+ht.config.BearerToken)
		}

		resp, err := ht.client.Do(req)
		if err != nil {
			n.Address().Clear()
			return
		}
		resp.Body.Close()
	}(node)

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

	req, err := http.NewRequest(http.MethodPost, node.Address().URL, bytes.NewReader(rawPacket))
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

	replyBody, err := io.ReadAll(resp.Body)
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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	packet, err := ht.packetFromBuffer(body)
	if err != nil {
		ht.config.Logger.Err(err).Errorf("Failed to decode incoming packet")
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

			case <-r.Context().Done():
				return
			}

		default:
			http.Error(w, "Server busy", http.StatusServiceUnavailable)
		}

		close(replyChan)
	} else {
		select {
		case ht.packetChannel <- packet:
			w.WriteHeader(http.StatusNoContent)
		default:
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

	*node.Address() = Address{URL: node.AdvertiseAddr()}
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
