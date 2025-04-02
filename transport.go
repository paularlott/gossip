package gossip

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type TransportType uint8

const (
	TransportBestEffort TransportType = iota // Best effort transport, uses UDP
	TransportReliable                        // Reliable transport, uses TCP
)

// Interface to define the transport layer, the transport layer is responsible for placing packets onto the wire and reading them off
// it also handles encryption and compression of packets.
type Transport interface {
	PacketChannel() chan *IncomingPacket
	DialPeer(node *Node) (net.Conn, error)
	WritePacket(conn net.Conn, packet *Packet) error
	ReadPacket(conn net.Conn) (*Packet, error)
	SendPacket(transportType TransportType, node *Node, packet *Packet) error
}

type transport struct {
	config        *Config
	tcpListener   *net.TCPListener
	udpListener   *net.UDPConn
	packetChannel chan *IncomingPacket
}

// Holds the information for an incoming packet waiting on the queue for processing
type IncomingPacket struct {
	Conn   net.Conn
	Packet *Packet
}

func NewTransport(ctx context.Context, wg *sync.WaitGroup, config *Config) (*transport, error) {

	// Check we have a bind address
	if config.BindAddr == "" {
		return nil, fmt.Errorf("no bind address given")
	}

	// Get the address and port from the bind address
	addr, err := ResolveAddress(config.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve bind address: %w", err)
	}

	config.Logger.
		Field("bind_addr", addr.IP.String()).
		Field("bind_port", addr.Port).
		Infof("Binding to address")

	// Create a TCP listener
	tcpAddr := &net.TCPAddr{
		IP:   addr.IP,
		Port: int(addr.Port),
	}
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP listener: %w", err)
	}
	config.Logger.Field("tcp_addr", tcpListener.Addr().String()).Debugf("TCP listener created")

	// Create a UDP listener
	udpAddr := &net.UDPAddr{
		IP:   addr.IP,
		Port: int(addr.Port),
	}
	udpListener, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP listener: %w", err)
	}
	config.Logger.Field("udp_addr", udpListener.LocalAddr().String()).Debugf("UDP listener created")

	// Create the transport
	transport := &transport{
		config:        config,
		tcpListener:   tcpListener,
		udpListener:   udpListener,
		packetChannel: make(chan *IncomingPacket, 128),
	}

	// Start the transports
	wg.Add(3)
	go transport.tcpListen(ctx, wg)
	go transport.udpListen(ctx, wg)

	// Monitor the context for cancellation
	go func() {
		<-ctx.Done()
		transport.shutdown(wg)
	}()

	config.Logger.Infof("Transport started")

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

	t.config.Logger.Infof("Transport stopped")
}

func (t *transport) PacketChannel() chan *IncomingPacket {
	return t.packetChannel
}

func isNetClosedError(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
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

		go func() {
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
		}()
	}
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
				packet, err := t.packetFromBuffer(packetData)
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

func (t *transport) packetToBuffer(packet *Packet) ([]byte, error) {
	// Marshal the packet to a byte buffer
	headerBytes, err := t.config.MsgCodec.Marshal(packet)
	if err != nil {
		return nil, err
	}

	// Create a buffer to hold the packet data
	var buf bytes.Buffer

	// Write the size of the packet header as a 16-bit big endian integer
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
	_, err = buf.Write(packet.payload)
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
		return nil, fmt.Errorf("packet too small")
	}

	headerSize := binary.BigEndian.Uint16(data[:2])
	if len(data) < int(headerSize)+2 {
		return nil, fmt.Errorf("packet too small")
	}

	// Unmarshal the header from the buffer to a Packet struct
	packet := Packet{}
	err = t.config.MsgCodec.Unmarshal(data[2:headerSize+2], &packet)
	if err != nil {
		return nil, err
	}

	// Attach the payload to the packet
	packet.payload = data[headerSize+2:]
	packet.codec = t.config.MsgCodec

	return &packet, nil
}

func (t *transport) WritePacket(conn net.Conn, packet *Packet) error {

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

	return t.packetFromBuffer(receivedData)
}

func (t *transport) SendPacket(transportType TransportType, node *Node, packet *Packet) error {

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
