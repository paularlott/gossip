package gossip

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type TransportType uint8

const (
	TransportBestEffort TransportType = iota // Best effort transport, uses UDP
	TransportReliable                        // Reliable transport, uses TCP
)

var (
	ErrNoTransportAvailable = fmt.Errorf("no transport available") // When there's no available transport between two nodes
)

// Interface to define the transport layer, the transport layer is responsible for placing packets onto the wire and reading them off
// it also handles encryption and compression of packets.
type Transport interface {
	PacketChannel() chan *IncomingPacket
	DialPeer(node *Node) (net.Conn, error)
	WritePacket(conn net.Conn, packet *Packet) error
	ReadPacket(conn net.Conn) (*Packet, error)
	SendPacket(transportType TransportType, node *Node, packet *Packet) error
	WebsocketHandler(ctx context.Context, w http.ResponseWriter, r *http.Request)
}

type transport struct {
	config        *Config
	localNode     *Node
	tcpListener   *net.TCPListener
	udpListener   *net.UDPConn
	packetChannel chan *IncomingPacket
	wsProvider    WebsocketProvider
}

// Holds the information for an incoming packet waiting on the queue for processing
type IncomingPacket struct {
	Conn   net.Conn
	Packet *Packet
}

func NewTransport(ctx context.Context, wg *sync.WaitGroup, config *Config, bindAddress Address, localNode *Node) (*transport, error) {
	var err error

	// Check we have a bind address
	if bindAddress.Port == 0 && bindAddress.URL == "" {
		return nil, fmt.Errorf("no bind address given or websocket url")
	}

	if bindAddress.URL != "" && config.WebsocketProvider == nil {
		return nil, fmt.Errorf("no websocket provider")
	}

	// Create the transport
	transport := &transport{
		config:        config,
		localNode:     localNode,
		packetChannel: make(chan *IncomingPacket, config.IncomingPacketQueueDepth),
		wsProvider:    config.WebsocketProvider,
	}

	if bindAddress.Port == 0 {
		config.Logger.Infof("Using WebSockets for communication")
	} else {
		config.Logger.
			Field("bind_addr", bindAddress.IP.String()).
			Field("bind_port", bindAddress.Port).
			Infof("Binding to address")

		// Create a TCP listener
		tcpAddr := &net.TCPAddr{
			IP:   bindAddress.IP,
			Port: int(bindAddress.Port),
		}
		transport.tcpListener, err = net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to create TCP listener: %w", err)
		}

		// Create a UDP listener
		udpAddr := &net.UDPAddr{
			IP:   bindAddress.IP,
			Port: int(bindAddress.Port),
		}
		transport.udpListener, err = net.ListenUDP("udp", udpAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to create UDP listener: %w", err)
		}

		// Start the transports
		wg.Add(2)
		go transport.tcpListen(ctx, wg)
		go transport.udpListen(ctx, wg)
	}

	wg.Add(1)
	// Monitor the context for cancellation
	go func() {
		<-ctx.Done()
		transport.shutdown(wg)
	}()

	config.Logger.Debugf("Transport started")

	return transport, nil
}

func (t *transport) shutdown(wg *sync.WaitGroup) {
	defer wg.Done()

	if t.tcpListener != nil {
		t.tcpListener.Close()
	}
	if t.udpListener != nil {
		t.udpListener.Close()
	}

	close(t.packetChannel)

	t.config.Logger.Debugf("Transport stopped")
}

func (t *transport) PacketChannel() chan *IncomingPacket {
	return t.packetChannel
}

func isNetClosedError(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}

func (t *transport) packetToQueue(conn net.Conn, ctx context.Context) {
	packet, err := t.ReadPacket(conn)
	if err != nil {
		t.config.Logger.Err(err).Errorf("Failed to read TCP packet")
		conn.Close()
		return
	}

	// Create an incoming packet
	incomingPacket := &IncomingPacket{
		Conn:   conn,
		Packet: packet,
	}

	select {
	case <-ctx.Done():
		conn.Close()
		return
	case t.packetChannel <- incomingPacket:
	}
}

func (t *transport) tcpListen(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		conn, err := t.tcpListener.Accept()
		if err != nil {
			if isNetClosedError(err) {
				return
			}

			t.config.Logger.Err(err).Errorf("Failed to accept TCP connection")
			continue
		}

		go t.packetToQueue(conn, ctx)
	}
}

func (t *transport) WebsocketHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if t.wsProvider == nil {
		http.Error(w, "WebSocket provider not configured", http.StatusInternalServerError)
		return
	}

	conn, err := t.wsProvider.UpgradeHTTPToWebsocket(w, r)
	if err != nil {
		t.config.Logger.Err(err).Errorf("Failed to upgrade to WebSocket")
		http.Error(w, "Failed to upgrade to WebSocket", http.StatusInternalServerError)
		return
	}

	t.packetToQueue(conn, ctx)
}

func (t *transport) udpListen(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	buf := make([]byte, 65535)

	for {
		n, _, err := t.udpListener.ReadFromUDP(buf)
		if err != nil {
			if isNetClosedError(err) {
				return
			}

			t.config.Logger.Err(err).Errorf("Failed to read from UDP connection")
			continue
		}

		// Check there's data in the buffer
		if n > 0 {
			packetData := make([]byte, n)
			copy(packetData, buf[:n])

			go func() {
				packet, err := t.packetFromBuffer(packetData, false)
				if err != nil {
					t.config.Logger.Err(err).Errorf("Failed to decode UDP packet")
					return
				}

				// Create an incoming packet
				incomingPacket := &IncomingPacket{
					Conn:   nil,
					Packet: packet,
				}

				select {
				case <-ctx.Done():
					return
				case t.packetChannel <- incomingPacket:
				}
			}()
		}
	}
}

func (t *transport) DialPeer(node *Node) (net.Conn, error) {
	if node.address.Port > 0 {
		// Create a TCP connection
		tcpAddr := &net.TCPAddr{
			IP:   node.address.IP,
			Port: node.address.Port,
		}
		conn, err := net.DialTimeout("tcp", tcpAddr.String(), t.config.TCPDialTimeout)
		if err != nil {
			return nil, err
		}

		return conn, nil
	} else if node.address.URL != "" {
		if t.wsProvider == nil {
			return nil, fmt.Errorf("no websocket provider configured")
		}

		return t.wsProvider.DialWebsocket(node.address.URL)
	} else {
		return nil, ErrNoTransportAvailable
	}
}

// Assemble the packet and payload into a buffer, compression and encryption applied if needed
// skipCompression and skipEncryption are used to skip compression and encryption when the underlying transport already applies these e.g. wss
func (t *transport) packetToBuffer(packet *Packet, skipCompression bool, skipEncryption bool) ([]byte, error) {
	// Marshal the packet to a byte buffer
	headerBytes, err := t.config.MsgCodec.Marshal(packet)
	if err != nil {
		return nil, err
	}

	headerSize := uint16(len(headerBytes))

	// If we have a compressor then compress the packet
	var compressedData []byte
	isCompressed := false
	if !skipCompression && t.config.Compressor != nil && len(packet.payload) >= t.config.CompressMinSize {
		compressedData, err = t.config.Compressor.Compress(packet.payload)
		if err != nil {
			return nil, err
		}

		// If compressed data is smaller than the original data then use it
		if len(compressedData) < len(packet.payload) {
			isCompressed = true
			headerSize |= 0x8000 // Bit 15: Compression flag
		}
	}

	// Determine if encryption is needed
	isEncrypted := !skipEncryption && t.config.EncryptionKey != ""
	if isEncrypted {
		headerSize |= 0x4000 // Bit 14: Encryption flag
	}

	// Create a buffer to hold the packet data
	var buf bytes.Buffer

	// Write the header size with flags
	err = binary.Write(&buf, binary.BigEndian, headerSize)
	if err != nil {
		return nil, err
	}

	// Prepare the payload data that will potentially be encrypted
	var payloadBuf bytes.Buffer

	// Write the header bytes
	_, err = payloadBuf.Write(headerBytes)
	if err != nil {
		return nil, err
	}

	// Write the payload
	if isCompressed {
		_, err = payloadBuf.Write(compressedData)
	} else {
		_, err = payloadBuf.Write(packet.payload)
	}
	if err != nil {
		return nil, err
	}

	// If encryption is enabled, encrypt just the payload portion
	payloadBytes := payloadBuf.Bytes()
	if isEncrypted {
		payloadBytes, err = encrypt([]byte(t.config.EncryptionKey), payloadBytes)
		if err != nil {
			return nil, err
		}
	}

	// Write the encrypted or unencrypted payload to the main buffer
	_, err = buf.Write(payloadBytes)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (t *transport) packetFromBuffer(data []byte, lowLevelTransportIsSecure bool) (*Packet, error) {
	var err error

	// Get the header size and flags from the first 2 bytes
	if len(data) < 2 {
		return nil, fmt.Errorf("packet too small")
	}

	// Read header size and flags
	flags := binary.BigEndian.Uint16(data[:2])

	// Extract flags
	isCompressed := flags&0x8000 != 0
	isEncrypted := flags&0x4000 != 0

	// Get actual header size (mask out the flag bits)
	headerSize := flags & 0x3FFF

	// Extract the encrypted portion (header + payload)
	encryptedPortion := data[2:]

	// If encrypted, decrypt the data
	if isEncrypted {
		if t.config.EncryptionKey == "" {
			return nil, fmt.Errorf("received encrypted packet but no encryption key configured")
		}

		encryptedPortion, err = decrypt([]byte(t.config.EncryptionKey), encryptedPortion)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt packet: %w", err)
		}
	} else if t.config.EncryptionKey != "" && !lowLevelTransportIsSecure {
		// Log warning but continue processing, websockets will use TLS so we don't encrypt the packet even if encryption is used for TCP/UDP
		t.config.Logger.Warnf("Received unencrypted packet but encryption is configured")
	}

	// Make sure we have enough data after decryption
	if len(encryptedPortion) < int(headerSize) {
		return nil, fmt.Errorf("decrypted packet too small for header")
	}

	// Unmarshal the header
	packet := Packet{}
	err = t.config.MsgCodec.Unmarshal(encryptedPortion[:headerSize], &packet)
	if err != nil {
		return nil, err
	}

	// Attach the payload to the packet
	packet.codec = t.config.MsgCodec

	if t.config.Compressor != nil && isCompressed {
		packet.payload, err = t.config.Compressor.Decompress(encryptedPortion[headerSize:])
		if err != nil {
			return nil, err
		}
	} else {
		packet.payload = encryptedPortion[headerSize:]
	}

	return &packet, nil
}

func (t *transport) WritePacket(conn net.Conn, packet *Packet) error {

	// If running on wss then we need to skip compression and encryption to avoid double compression/encryption
	skipCompression := false
	skipEncryption := false
	if wsConn, ok := conn.(WSConn); ok {
		skipCompression = true
		skipEncryption = wsConn.IsSecure()
	}

	// Marshal the packet to a byte buffer
	buf, err := t.packetToBuffer(packet, skipCompression, skipEncryption)
	if err != nil {
		return err
	}

	return t.writeRawPacket(conn, buf)
}

func (t *transport) writeRawPacket(conn net.Conn, rawPacket []byte) error {
	// Prepare the complete response in a single buffer (length + data)
	var writeBuffer bytes.Buffer
	err := binary.Write(&writeBuffer, binary.BigEndian, uint32(len(rawPacket)))
	if err != nil {
		return err
	}

	_, err = writeBuffer.Write(rawPacket)
	if err != nil {
		return err
	}

	// Send the packet
	bytesWritten := 0
	data := writeBuffer.Bytes()
	for bytesWritten < len(data) {
		n, err := conn.Write(data[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += n
	}

	return nil
}

func (t *transport) ReadPacket(conn net.Conn) (*Packet, error) {
	// Set the deadline for the connection
	err := conn.SetReadDeadline(time.Now().Add(t.config.TCPDeadline))
	if err != nil {
		return nil, err
	}

	// Create a buffered reader for more efficient reading
	bufferedReader := bufio.NewReader(conn)

	// Read the length prefix (4 bytes) to determine data size
	lengthBytes := make([]byte, 4)
	_, err = io.ReadFull(bufferedReader, lengthBytes)
	if err != nil {
		return nil, err
	}

	dataLen := binary.BigEndian.Uint32(lengthBytes)

	// Check the length is not too large
	if dataLen > uint32(t.config.TCPMaxPacketSize) {
		return nil, fmt.Errorf("packet size too large: %d bytes", dataLen)
	}

	// Allocate buffer based on the expected data size
	receivedData := make([]byte, dataLen)

	// Read the full message using io.ReadFull (handles partial reads automatically)
	_, err = io.ReadFull(bufferedReader, receivedData)
	if err != nil {
		return nil, err
	}

	// Test if we're using a secure websocket connection
	underlyingTransportIsSecure := false
	if wsConn, ok := conn.(WSConn); ok {
		underlyingTransportIsSecure = wsConn.IsSecure()
	}

	return t.packetFromBuffer(receivedData, underlyingTransportIsSecure)
}

func (t *transport) SendPacket(transportType TransportType, node *Node, packet *Packet) error {
	skipCompression := false
	skipEncryption := false

	// If using websockets then we need to use reliable transport and avoid double compression/encryption
	if t.udpListener == nil || node.address.Port == 0 {
		transportType = TransportReliable
		skipCompression = true
		skipEncryption = strings.HasPrefix(node.address.URL, "wss://")
	}

	// Marshal the packet to a byte buffer
	rawPacket, err := t.packetToBuffer(packet, skipCompression, skipEncryption)
	if err != nil {
		return err
	}

	// If transport type is best effort but the packet is too large, switch to reliable or if not using TCP/UDP
	if transportType == TransportBestEffort && (len(rawPacket) >= t.config.UDPMaxPacketSize) {
		transportType = TransportReliable
	}

	// If using reliable transport then use TCP
	if transportType == TransportReliable {
		conn, err := t.DialPeer(node)
		if err != nil {
			return err
		}
		defer conn.Close()

		// Write the packet to the connection
		err = t.writeRawPacket(conn, rawPacket)
		if err != nil {
			return err
		}
	} else { // Send the message over UDP
		err = t.udpListener.SetWriteDeadline(time.Now().Add(t.config.UDPDeadline))
		if err != nil {
			return err
		}

		_, err = t.udpListener.WriteToUDP(rawPacket, &net.UDPAddr{
			IP:   node.address.IP,
			Port: node.address.Port,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
