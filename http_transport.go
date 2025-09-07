package gossip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"time"
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
			Timeout: 30 * time.Second,
		},
	}
}

func (ht *HTTPTransport) PacketChannel() chan *Packet {
	return ht.packetChannel
}

func (ht *HTTPTransport) Send(transportType TransportType, node *Node, packet *Packet) error {
	rawPacket, err := ht.packetToBuffer(packet)
	if err != nil {
		return err
	}

	if err := ht.ensureNodeAddressResolved(node); err != nil {
		return fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	// Fire and forget HTTP POST
	go func(n *Node) {
		resp, err := ht.client.Post(n.Address().URL, "application/octet-stream", bytes.NewReader(rawPacket))
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
	rawPacket, err := ht.packetToBuffer(packet)
	if err != nil {
		return nil, err
	}

	if err := ht.ensureNodeAddressResolved(node); err != nil {
		return nil, fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	resp, err := ht.client.Post(node.Address().URL, "application/octet-stream", bytes.NewReader(rawPacket))
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
	// TODO Need to validate bearer token against the configured bearer token

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

	replyChan := make(chan *Packet, 1)
	packet.SetReplyChan(replyChan)

	select {
	case ht.packetChannel <- packet:
		select {
		case replyPacket := <-replyChan:
			replyData, err := ht.packetToBuffer(replyPacket)
			if err != nil {
				http.Error(w, "Failed to encode reply", http.StatusInternalServerError)
				replyPacket.Release()
				return
			}

			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			w.Write(replyData)
			replyPacket.Release()

		case <-time.After(30 * time.Second):
			w.WriteHeader(http.StatusNoContent)

		case <-r.Context().Done():
			return
		}

	default:
		http.Error(w, "Server busy", http.StatusServiceUnavailable)
	}

	close(replyChan)
}

func (ht *HTTPTransport) ensureNodeAddressResolved(node *Node) error {
	if !node.Address().IsEmpty() {
		return nil
	}

	// TODO handle srv+ records and append the config path to the gossip handler

	if node.AdvertiseAddr() == "" {
		return fmt.Errorf("no advertise address available")
	}

	*node.Address() = Address{URL: node.AdvertiseAddr()}
	return nil
}

func (ht *HTTPTransport) packetToBuffer(packet *Packet) ([]byte, error) {
	headerBytes, err := ht.config.MsgCodec.Marshal(packet)
	if err != nil {
		return nil, err
	}

	headerSize := uint16(len(headerBytes))

	var buf bytes.Buffer

	// Write header size first (2 bytes, big endian)
	err = binary.Write(&buf, binary.BigEndian, headerSize)
	if err != nil {
		return nil, err
	}

	// Write header
	buf.Write(headerBytes)

	// Write payload
	buf.Write(packet.Payload())

	return buf.Bytes(), nil
}

func (ht *HTTPTransport) packetFromBuffer(data []byte) (*Packet, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("packet too small")
	}

	// Read the header size (first 2 bytes)
	headerSize := binary.BigEndian.Uint16(data[:2])

	if len(data) < int(headerSize)+2 {
		return nil, fmt.Errorf("packet too small for header")
	}

	// Create new packet and unmarshal just the header portion
	packet := NewPacket()
	err := ht.config.MsgCodec.Unmarshal(data[2:2+headerSize], &packet)
	if err != nil {
		return nil, err
	}

	packet.SetCodec(ht.config.MsgCodec)

	// Set the payload (everything after the header)
	if len(data) > int(headerSize)+2 {
		packet.SetPayload(data[2+headerSize:])
	}

	return packet, nil
}
