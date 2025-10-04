package gossip

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SocketTransport struct {
	config        *Config
	tcpListener   *net.TCPListener
	udpListener   *net.UDPConn
	packetChannel chan *Packet
	resolver      Resolver
}

func NewSocketTransport(config *Config) *SocketTransport {
	st := &SocketTransport{
		config:        config,
		packetChannel: make(chan *Packet, config.IncomingPacketQueueDepth),
		resolver:      config.Resolver,
	}

	if st.resolver == nil {
		st.resolver = NewDefaultResolver()
	}

	return st
}

func (st *SocketTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	bindAddress, err := st.parseBindAddress(st.config.BindAddr)
	if err != nil {
		return err
	}

	st.config.Logger.
		Field("bind_addr", bindAddress.IP.String()).
		Field("bind_port", bindAddress.Port).
		Infof("transport: Bind to address")

	tcpAddr := &net.TCPAddr{
		IP:   bindAddress.IP,
		Port: int(bindAddress.Port),
	}
	st.tcpListener, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("failed to create TCP listener: %w", err)
	}

	if !st.config.ForceReliableTransport {
		udpAddr := &net.UDPAddr{
			IP:   bindAddress.IP,
			Port: int(bindAddress.Port),
		}
		st.udpListener, err = net.ListenUDP("udp", udpAddr)
		if err != nil {
			return fmt.Errorf("failed to create UDP listener: %w", err)
		}
	}

	wg.Add(1)
	go st.tcpListen(ctx, wg)

	if !st.config.ForceReliableTransport {
		wg.Add(1)
		go st.udpListen(ctx, wg)
	}

	wg.Add(1)
	go func() {
		<-ctx.Done()
		st.shutdown(wg)
	}()

	return nil
}

func (st *SocketTransport) parseBindAddress(bindAddr string) (*net.TCPAddr, error) {
	// Handle cases like ":8080"
	if strings.HasPrefix(bindAddr, ":") {
		bindAddr = "0.0.0.0" + bindAddr
	}

	// Use ResolveTCPAddr which handles various formats
	tcpAddr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve TCP address: %w", err)
	}

	return tcpAddr, nil
}

func (st *SocketTransport) Name() string {
	return "socket"
}

func (st *SocketTransport) shutdown(wg *sync.WaitGroup) {
	defer wg.Done()

	if st.tcpListener != nil {
		st.tcpListener.Close()
	}
	if st.udpListener != nil {
		st.udpListener.Close()
	}

	close(st.packetChannel)
}

func (st *SocketTransport) PacketChannel() chan *Packet {
	return st.packetChannel
}

func (st *SocketTransport) Send(transportType TransportType, node *Node, packet *Packet) error {
	rawPacket, err := st.packetToBuffer(packet, false)
	if err != nil {
		return err
	}

	if st.config.ForceReliableTransport || (transportType == TransportBestEffort && len(rawPacket) >= st.config.UDPMaxPacketSize) {
		transportType = TransportReliable
	}

	if err := st.ensureNodeAddressResolved(node); err != nil {
		return fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	if transportType == TransportReliable {
		if err := st.sendTCP(node, rawPacket); err != nil {
			return err
		}
	} else {
		if err := st.sendUDP(node, rawPacket); err != nil {
			return err
		}
	}

	return nil
}

func (st *SocketTransport) SendWithReply(node *Node, packet *Packet) (*Packet, error) {
	conn, err := st.dialPeer(node)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := st.writePacket(conn, packet, true); err != nil {
		node.Address().Clear()
		return nil, err
	}

	replyPacket, _, err := st.readPacket(conn)
	if err != nil {
		node.Address().Clear()
		return nil, err
	}

	return replyPacket, nil
}

func (st *SocketTransport) tcpListen(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		conn, err := st.tcpListener.Accept()
		if err != nil {
			if st.isNetClosedError(err) {
				return
			}
			st.config.Logger.Err(err).Errorf("transport: Failed to accept TCP connection")
			continue
		}

		go st.packetToQueue(conn, ctx)
	}
}

func (st *SocketTransport) udpListen(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	buf := make([]byte, 65535)

	for {
		n, _, err := st.udpListener.ReadFromUDP(buf)
		if err != nil {
			if st.isNetClosedError(err) {
				return
			}
			st.config.Logger.Err(err).Errorf("transport: Failed to read from UDP connection")
			continue
		}

		if n > 0 {
			packetData := make([]byte, n)
			copy(packetData, buf[:n])

			go func() {
				packet, _, err := st.packetFromBuffer(packetData)
				if err != nil {
					st.config.Logger.Err(err).Errorf("Failed to decode UDP packet")
					return
				}

				select {
				case <-ctx.Done():
					packet.Release()
					return
				case st.packetChannel <- packet:
				}
			}()
		}
	}
}

func (st *SocketTransport) packetToQueue(conn net.Conn, ctx context.Context) {
	packet, replyExpected, err := st.readPacket(conn)
	if err != nil {
		st.config.Logger.Err(err).Errorf("transport: Failed to read packet")
		conn.Close()
		return
	}

	packet.SetConn(conn)

	if replyExpected {
		replyChan := make(chan *Packet, 1)
		packet.SetReplyChan(replyChan)

		go func() {
			defer close(replyChan)

			select {
			case replyPacket := <-replyChan:
				if replyPacket != nil {
					if err := st.writePacket(conn, replyPacket, false); err != nil {
						st.config.Logger.Err(err).Errorf("Failed to write reply packet")
					}
					replyPacket.Release()
				}
			case <-time.After(st.config.TCPDeadline):
				// Timeout - try to drain any late reply
				select {
				case replyPacket := <-replyChan:
					if replyPacket != nil {
						replyPacket.Release()
					}
				case <-time.After(100 * time.Millisecond):
					// Give up
				}
			case <-ctx.Done():
				// Context cancelled - try to drain any pending reply
				select {
				case replyPacket := <-replyChan:
					if replyPacket != nil {
						replyPacket.Release()
					}
				case <-time.After(100 * time.Millisecond):
					// Give up
				}
			}
		}()
	}

	select {
	case <-ctx.Done():
		packet.Release()
		return
	case st.packetChannel <- packet:
	}
}

func (st *SocketTransport) sendTCP(node *Node, rawPacket []byte) error {
	conn, err := st.dialPeer(node)
	if err != nil {
		node.Address().Clear()
		return err
	}
	defer conn.Close()

	return st.writeRawPacket(conn, rawPacket)
}

func (st *SocketTransport) sendUDP(node *Node, rawPacket []byte) error {
	tryWrite := func(addr Address) error {
		if err := st.udpListener.SetWriteDeadline(time.Now().Add(st.config.UDPDeadline)); err != nil {
			return err
		}
		_, err := st.udpListener.WriteToUDP(rawPacket, &net.UDPAddr{IP: addr.IP, Port: addr.Port})
		return err
	}

	if node.Address().IP != nil {
		if err := tryWrite(*node.Address()); err == nil {
			return nil
		}
		node.Address().Clear()
	}

	addrs, err := st.resolveAddress(node.AdvertiseAddr())
	if err != nil || len(addrs) == 0 {
		return fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	var sendErr error
	for _, addr := range addrs {
		if err := tryWrite(addr); err == nil {
			*node.Address() = addr
			return nil
		} else if sendErr == nil {
			sendErr = err
		}
	}
	return sendErr
}

func (st *SocketTransport) dialPeer(node *Node) (net.Conn, error) {
	if err := st.ensureNodeAddressResolved(node); err != nil {
		return nil, fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	tryDial := func(addr Address) (net.Conn, error) {
		tcpAddr := &net.TCPAddr{IP: addr.IP, Port: addr.Port}
		return net.DialTimeout("tcp", tcpAddr.String(), st.config.TCPDialTimeout)
	}

	if node.Address().IP != nil {
		if conn, err := tryDial(*node.Address()); err == nil {
			return conn, nil
		}
		node.Address().Clear()
	}

	addrs, err := st.resolveAddress(node.AdvertiseAddr())
	if err != nil || len(addrs) == 0 {
		return nil, fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	var firstErr error
	for _, addr := range addrs {
		if conn, err := tryDial(addr); err == nil {
			*node.Address() = addr
			return conn, nil
		} else if firstErr == nil {
			firstErr = err
		}
	}
	return nil, firstErr
}

func (st *SocketTransport) isNetClosedError(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection")
}

func (st *SocketTransport) ensureNodeAddressResolved(node *Node) error {
	if !node.Address().IsEmpty() {
		return nil
	}

	if node.AdvertiseAddr() == "" {
		return fmt.Errorf("no advertise address available")
	}

	addresses, err := st.resolveAddress(node.AdvertiseAddr())
	if err != nil {
		return fmt.Errorf("failed to resolve address %s: %v", node.AdvertiseAddr(), err)
	}

	if len(addresses) == 0 {
		return fmt.Errorf("no addresses resolved for %s", node.AdvertiseAddr())
	}

	*node.Address() = addresses[0]
	return nil
}

func (st *SocketTransport) resolveAddress(addressStr string) ([]Address, error) {
	if strings.HasPrefix(addressStr, "srv+") {
		serviceName := addressStr[4:]
		return st.lookupSRV(serviceName, true)
	} else {
		return st.lookupIP(addressStr)
	}
}

func (st *SocketTransport) lookupSRV(serviceName string, resolveToIPs bool) ([]Address, error) {
	addresses := make([]Address, 0)

	if !strings.HasSuffix(serviceName, ".") {
		serviceName += "."
	}

	addrs, err := st.resolver.LookupSRV(serviceName)
	if err != nil {
		return addresses, err
	}

	if len(addrs) == 0 {
		return addresses, fmt.Errorf("no SRV records found for service")
	}

	if resolveToIPs {
		var v4, v6 []Address
		for _, srv := range addrs {
			if srv.IP != nil {
				if srv.IP.To4() != nil {
					v4 = append(v4, Address{IP: srv.IP, Port: srv.Port})
				} else {
					v6 = append(v6, Address{IP: srv.IP, Port: srv.Port})
				}
			}
		}
		if st.config.PreferIPv6 {
			addresses = append(addresses, v6...)
			addresses = append(addresses, v4...)
		} else {
			addresses = append(addresses, v4...)
			addresses = append(addresses, v6...)
		}
	} else {
		for _, srv := range addrs {
			addresses = append(addresses, Address{Port: srv.Port})
		}
	}

	return addresses, nil
}

func (st *SocketTransport) lookupIP(host string) ([]Address, error) {
	addresses := make([]Address, 0)

	// Handle port-only format like ":8080"
	if _, err := strconv.Atoi(host); err == nil {
		host = ":" + host
	}

	// Require port to be specified for advertise addresses
	if !strings.Contains(host, ":") {
		return addresses, fmt.Errorf("port must be specified in address: %s", host)
	}

	hostStr, portStr, err := net.SplitHostPort(host)
	if err != nil {
		return addresses, err
	}

	if hostStr == "" {
		hostStr = "127.0.0.1"
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return addresses, fmt.Errorf("invalid port: %s", portStr)
	}

	var ip net.IP
	if ip = net.ParseIP(hostStr); ip == nil {
		ipStrs, err := st.resolver.LookupIP(hostStr)
		if err != nil {
			return addresses, err
		}

		var v4, v6 []net.IP
		for _, ipStr := range ipStrs {
			if ip = net.ParseIP(ipStr); ip != nil {
				if ip.To4() != nil {
					v4 = append(v4, ip)
				} else {
					v6 = append(v6, ip)
				}
			}
		}

		ordered := make([]net.IP, 0, len(v4)+len(v6))
		if st.config.PreferIPv6 {
			ordered = append(ordered, v6...)
			ordered = append(ordered, v4...)
		} else {
			ordered = append(ordered, v4...)
			ordered = append(ordered, v6...)
		}

		for _, ip := range ordered {
			addresses = append(addresses, Address{IP: ip, Port: int(port)})
		}
	} else {
		addresses = append(addresses, Address{IP: ip, Port: int(port)})
	}

	return addresses, nil
}

const (
	compressionFlag = 0x8000
)

func (st *SocketTransport) packetToBuffer(packet *Packet, replyExpected bool) ([]byte, error) {
	headerBytes, err := st.config.MsgCodec.Marshal(packet)
	if err != nil {
		return nil, err
	}

	headerSize := uint16(len(headerBytes))

	// Set reply expected flag if requested
	if replyExpected {
		headerSize |= replyExpectedFlag
	}

	var compressedData []byte
	isCompressed := false
	if st.config.Compressor != nil && len(packet.Payload()) >= st.config.CompressMinSize {
		compressedData, err = st.config.Compressor.Compress(packet.Payload())
		if err != nil {
			return nil, err
		}

		if len(compressedData) < len(packet.Payload()) {
			isCompressed = true
			headerSize |= compressionFlag
		}
	}

	var buf bytes.Buffer

	err = binary.Write(&buf, binary.BigEndian, headerSize)
	if err != nil {
		return nil, err
	}

	var payloadBuf bytes.Buffer

	_, err = payloadBuf.Write(headerBytes)
	if err != nil {
		return nil, err
	}

	if isCompressed {
		_, err = payloadBuf.Write(compressedData)
	} else {
		_, err = payloadBuf.Write(packet.Payload())
	}
	if err != nil {
		return nil, err
	}

	payloadBytes := payloadBuf.Bytes()
	if st.config.Cipher != nil {
		payloadBytes, err = st.config.Cipher.Encrypt(st.config.EncryptionKey, payloadBytes)
		if err != nil {
			return nil, err
		}
	}

	_, err = buf.Write(payloadBytes)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (st *SocketTransport) packetFromBuffer(data []byte) (*Packet, bool, error) {
	if len(data) < 2 {
		return nil, false, fmt.Errorf("packet too small")
	}

	flags := binary.BigEndian.Uint16(data[:2])
	isCompressed := flags&compressionFlag != 0
	replyExpected := flags&replyExpectedFlag != 0
	headerSize := flags & headerSizeMask

	encryptedPortion := data[2:]

	if st.config.Cipher != nil {
		var err error
		encryptedPortion, err = st.config.Cipher.Decrypt(st.config.EncryptionKey, encryptedPortion)
		if err != nil {
			return nil, false, fmt.Errorf("failed to decrypt packet: %w", err)
		}
	}

	if len(encryptedPortion) < int(headerSize) {
		return nil, false, fmt.Errorf("decrypted packet too small for header")
	}

	packet := NewPacket()
	err := st.config.MsgCodec.Unmarshal(encryptedPortion[:headerSize], &packet)
	if err != nil {
		packet.Release()
		return nil, false, err
	}

	packet.SetCodec(st.config.MsgCodec)

	if st.config.Compressor != nil && isCompressed {
		payload, err := st.config.Compressor.Decompress(encryptedPortion[headerSize:])
		if err != nil {
			packet.Release()
			return nil, false, err
		}
		packet.SetPayload(payload)
	} else {
		packet.SetPayload(encryptedPortion[headerSize:])
	}

	return packet, replyExpected, nil
}

func (st *SocketTransport) writePacket(conn net.Conn, packet *Packet, replyExpected bool) error {
	buf, err := st.packetToBuffer(packet, replyExpected)
	if err != nil {
		return err
	}
	return st.writeRawPacket(conn, buf)
}

func (st *SocketTransport) writeRawPacket(conn net.Conn, rawPacket []byte) error {
	err := conn.SetWriteDeadline(time.Now().Add(st.config.TCPDeadline))
	if err != nil {
		return err
	}

	var writeBuffer bytes.Buffer
	err = binary.Write(&writeBuffer, binary.BigEndian, uint32(len(rawPacket)))
	if err != nil {
		return err
	}

	_, err = writeBuffer.Write(rawPacket)
	if err != nil {
		return err
	}

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

func (st *SocketTransport) readPacket(conn net.Conn) (*Packet, bool, error) {
	err := conn.SetReadDeadline(time.Now().Add(st.config.TCPDeadline))
	if err != nil {
		return nil, false, err
	}

	bufferedReader := bufio.NewReader(conn)

	lengthBytes := make([]byte, 4)
	_, err = io.ReadFull(bufferedReader, lengthBytes)
	if err != nil {
		return nil, false, err
	}

	dataLen := binary.BigEndian.Uint32(lengthBytes)

	if dataLen > uint32(st.config.TCPMaxPacketSize) {
		return nil, false, fmt.Errorf("packet size too large: %d bytes", dataLen)
	}

	receivedData := make([]byte, dataLen)
	_, err = io.ReadFull(bufferedReader, receivedData)
	if err != nil {
		return nil, false, err
	}

	return st.packetFromBuffer(receivedData)
}
