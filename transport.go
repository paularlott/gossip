package gossip

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/shamaton/msgpack/v2"
)

type TransportType uint8

const (
	TransportBestEffort TransportType = iota // Best effort transport, uses UDP
	TransportReliable                        // Reliable transport, uses TCP
)

type transport struct {
	config        *Config
	isStopping    bool
	messageIdGen  atomic.Pointer[MessageID]
	tcpListener   *net.TCPListener
	udpListener   *net.UDPConn
	packetChannel chan *incomingPacket
}

// Holds the information for an incoming packet waiting on the queue for processing
type incomingPacket struct {
	conn   net.Conn
	packet *Packet
}

func newTransport(config *Config) (*transport, error) {

	// Check we have a bind address
	if config.BindAddr == "" {
		return nil, fmt.Errorf("no bind address given")
	}

	// Get the address and port from the bind address
	addr, err := ResolveAddress(config.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve bind address: %w", err)
	}

	log.Info().Str("bind_addr", addr.IP.String()).Uint16("bind_port", addr.Port).Msg("Binding to address")

	// Create a TCP listener
	tcpAddr := &net.TCPAddr{
		IP:   addr.IP,
		Port: int(addr.Port),
	}
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP listener: %w", err)
	}
	log.Debug().Str("tcp_addr", tcpListener.Addr().String()).Msg("TCP listener created")

	// Create a UDP listener
	udpAddr := &net.UDPAddr{
		IP:   addr.IP,
		Port: int(addr.Port),
	}
	udpListener, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP listener: %w", err)
	}
	log.Debug().Str("udp_addr", udpListener.LocalAddr().String()).Msg("UDP listener created")

	// Create the transport
	transport := &transport{
		config:        config,
		isStopping:    false,
		tcpListener:   tcpListener,
		udpListener:   udpListener,
		packetChannel: make(chan *incomingPacket, 128),
	}

	initialMessageID := MessageID{
		Timestamp: time.Now().UnixNano(),
		Seq:       0,
	}
	transport.messageIdGen.Store(&initialMessageID)

	// Start the transports
	go transport.tcpListen()
	go transport.udpListen()

	log.Info().Msg("Transport started")

	return transport, nil
}

func (t *transport) stop() {
	t.isStopping = true

	if t.tcpListener != nil {
		t.tcpListener.Close()
	}
	if t.udpListener != nil {
		t.udpListener.Close()
	}

	close(t.packetChannel)

	log.Info().Msg("Transport stopped")
}

func (t *transport) PacketChannel() chan *incomingPacket {
	return t.packetChannel
}

func (t *transport) tcpListen() {
	for {
		conn, err := t.tcpListener.Accept()
		if err != nil {
			if t.isStopping {
				return
			}

			log.Error().Err(err).Msg("Failed to accept TCP connection")
			continue
		}

		go func() {
			raw, err := t.readPacketFromConn(conn)
			if err != nil {
				log.Error().Err(err).Msg("Failed to read TCP packet")
				conn.Close()
				return
			}

			packet, err := t.packetFromBuffer(raw)
			if err != nil {
				log.Error().Err(err).Msg("Failed to decode TCP packet")
				conn.Close()
				return
			}

			// Create an incoming packet
			incomingPacket := &incomingPacket{
				conn:   conn,
				packet: packet,
			}

			t.packetChannel <- incomingPacket
		}()
	}
}

func (t *transport) udpListen() {
	for {
		buf := make([]byte, 65535)
		n, _, err := t.udpListener.ReadFromUDP(buf)
		if err != nil {
			if t.isStopping {
				return
			}

			log.Error().Err(err).Msg("Failed to read from UDP connection")
			continue
		}

		// Check there's data in the buffer
		if n > 0 {
			packet, err := t.packetFromBuffer(buf[:n])
			if err != nil {
				log.Error().Err(err).Msg("Failed to decode UDP packet")
				continue
			}

			// Create an incoming packet
			incomingPacket := &incomingPacket{
				conn:   nil,
				packet: packet,
			}

			t.packetChannel <- incomingPacket
		}
	}
}

func (t *transport) nextMessageID() MessageID {
	// Get the current time once before entering the loop
	now := time.Now().UnixNano()

	for {
		current := t.messageIdGen.Load()

		// Create the next message ID (avoid allocating until needed)
		var nextSeq uint16
		var nextTimestamp int64

		// If timestamp is the same, increment sequence
		if current.Timestamp == now {
			nextTimestamp = now
			nextSeq = current.Seq + 1
			// Handle overflow
			if nextSeq == 0 {
				// In case of overflow, move to next nanosecond
				nextTimestamp = now + 1
			}
		} else if current.Timestamp < now {
			// If our timestamp is newer, start with sequence 0
			nextTimestamp = now
			nextSeq = 0
		} else {
			// Current timestamp is in the future (clock went backward)
			// Continue using the current timestamp but increment sequence
			nextTimestamp = current.Timestamp
			nextSeq = current.Seq + 1
		}

		// Create the next ID value (only allocate once we know the values)
		next := &MessageID{
			Timestamp: nextTimestamp,
			Seq:       nextSeq,
		}

		// Try to update atomically
		if t.messageIdGen.CompareAndSwap(current, next) {
			return *next
		}
	}
}

func (t *transport) createPacket(sender NodeID, msgType MessageType, ttl uint8, payload interface{}) (*Packet, error) {
	packet := &Packet{
		MessageType: msgType,
		SenderID:    sender,
		MessageID:   t.nextMessageID(),
		TTL:         ttl,
	}

	// Marshal the payload to a byte buffer
	var err error
	packet.Payload, err = msgpack.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return packet, nil
}

// Send a message to the peer then accept a response message.
// Uses a TCP connection to send the packet and receive the response.
func (t *transport) sendMessageWithResponse(node *Node, sender NodeID, msgType MessageType, payload interface{}, responseMsgType MessageType, responsePayload interface{}) error {
	packet, err := t.createPacket(sender, msgType, 1, payload)
	if err != nil {
		return err
	}

	conn, err := t.dialPeer(node)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Write the packet to the connection
	err = t.writePacket(conn, packet)
	if err != nil {
		return err
	}

	responsePacket, err := t.readPacket(conn)
	if err != nil {
		return err
	}

	// If the response message type doesn't match the expected type, return an error
	if responsePacket.MessageType != responseMsgType {
		return fmt.Errorf("unexpected response message type: got %d, want %d", responsePacket.MessageType, responseMsgType)
	}

	// Unmarshal the response payload
	err = responsePacket.Unmarshal(responsePayload)
	if err != nil {
		return err
	}

	return nil
}

func (t *transport) dialPeer(node *Node) (net.Conn, error) {
	addr, err := node.ResolveConnectAddr()
	if err != nil {
		return nil, fmt.Errorf("dns: failed to resolve node address: %w", err)
	}

	// Create a TCP connection
	tcpAddr := &net.TCPAddr{
		IP:   addr.IP,
		Port: int(addr.Port),
	}
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), t.config.TCPDialTimeout)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (t *transport) writeMessage(conn net.Conn, sender NodeID, msgType MessageType, payload interface{}) error {
	packet, err := t.createPacket(sender, msgType, 1, payload)
	if err != nil {
		return fmt.Errorf("failed to build message: %w", err)
	}

	return t.writePacket(conn, packet)
}

func (t *transport) packetToBuffer(packet *Packet) ([]byte, error) {
	// Use msgpack to marshal the packet to a byte buffer
	headerBytes, err := msgpack.Marshal(packet)
	if err != nil {
		return nil, err
	}

	// Create a buffer to hold the packet data
	var buf bytes.Buffer

	// Write the size of the packet as a 16-bit big endian integer
	packetSize := uint16(len(headerBytes))
	err = binary.Write(&buf, binary.BigEndian, packetSize)
	if err != nil {
		return nil, err
	}

	// Write the packet data
	_, err = buf.Write(headerBytes)
	if err != nil {
		return nil, err
	}

	// Write the payload byte slice from the packet
	_, err = buf.Write(packet.Payload)
	if err != nil {
		return nil, err
	}

	// If encryption is enabled, encrypt the packet
	if t.config.EncryptionKey != "" {
		return encrypt([]byte(t.config.EncryptionKey), buf.Bytes())
	}

	return buf.Bytes(), nil
}

func (t *transport) packetFromBuffer(data []byte) (*Packet, error) {
	var err error

	// If encryption is enabled, decrypt the packet
	if t.config.EncryptionKey != "" {
		data, err = decrypt([]byte(t.config.EncryptionKey), data)
		if err != nil {
			return nil, err
		}
	}

	// Get the header size from the first 2 bytes (big endian)
	if len(data) < 2 {
		return nil, fmt.Errorf("packet too small 1")
	}

	headerSize := binary.BigEndian.Uint16(data[:2])
	if len(data) < int(headerSize) {
		return nil, fmt.Errorf("packet too small 2")
	}

	// Unmarshal the header from the buffer to a Packet struct
	packet := Packet{}
	err = msgpack.Unmarshal(data[2:headerSize+2], &packet)
	if err != nil {
		return nil, err
	}

	// Attach the payload to the packet
	packet.Payload = data[headerSize+2:]

	return &packet, nil
}

func (t *transport) writePacket(conn net.Conn, packet *Packet) error {

	// Marshal the packet to a byte buffer
	buf, err := t.packetToBuffer(packet)
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

func (t *transport) readPacketFromConn(conn net.Conn) ([]byte, error) {
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

	return receivedData, nil
}

func (t *transport) readPacket(conn net.Conn) (*Packet, error) {
	receivedData, err := t.readPacketFromConn(conn)
	if err != nil {
		return nil, err
	}

	return t.packetFromBuffer(receivedData)
}

func (t *transport) sendMessage(transportType TransportType, node *Node, sender NodeID, msgType MessageType, ttl uint8, payload interface{}) error {
	packet, err := t.createPacket(sender, msgType, ttl, payload)
	if err != nil {
		return err
	}

	return t.sendPacket(transportType, node, packet)
}

func (t *transport) sendPacket(transportType TransportType, node *Node, packet *Packet) error {

	// Marshal the packet to a byte buffer
	rawPacket, err := t.packetToBuffer(packet)
	if err != nil {
		return err
	}

	// If transport type is best effort but the packet is too large, switch to reliable
	if transportType == TransportBestEffort && len(rawPacket) >= t.config.UDPMaxPacketSize {
		transportType = TransportReliable
	}

	// If using reliable transport then use TCP
	if transportType == TransportReliable {
		conn, err := t.dialPeer(node)
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

		addr, err := node.ResolveConnectAddr()
		if err != nil {
			return err
		}

		err = t.udpListener.SetWriteDeadline(time.Now().Add(t.config.UDPDeadline))
		if err != nil {
			return err
		}

		_, err = t.udpListener.WriteToUDP(rawPacket, &net.UDPAddr{
			IP:   addr.IP,
			Port: int(addr.Port),
		})
		if err != nil {
			return err
		}
	}

	return nil
}
