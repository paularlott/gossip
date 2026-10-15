package gossip

import (
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

	"github.com/paularlott/logger"
)

type SocketTransport struct {
	config        *Config
	logger        logger.Logger
	tcpListener   net.Listener
	udpListener   *net.UDPConn
	packetChannel chan *Packet
	resolver      Resolver
	udpBufferPool sync.Pool // Pool for UDP packet buffers
	streamHandler func(packet *Packet, w io.Writer) error
	pool          *connPool // idle request/response connections per peer
}

func NewSocketTransport(config *Config) *SocketTransport {
	// Create logger with transport group
	var lgr logger.Logger
	if config.Logger != nil {
		lgr = config.Logger.WithGroup("gossip")
	} else {
		lgr = logger.NewNullLogger()
	}

	st := &SocketTransport{
		config:        config,
		logger:        lgr,
		packetChannel: make(chan *Packet, config.IncomingPacketQueueDepth),
		resolver:      config.Resolver,
		udpBufferPool: sync.Pool{
			New: func() interface{} {
				// Allocate max UDP packet size (64KB)
				buf := make([]byte, 65535)
				return &buf
			},
		},
	}

	if st.resolver == nil {
		st.resolver = NewDefaultResolver()
	}
	st.pool = newConnPool(config.TCPPoolSize, config.TCPIdleTimeout)

	return st
}

func (st *SocketTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	// Custom listeners (e.g. tsnet) don't support UDP, force reliable transport
	if st.config.ListenFunc != nil {
		st.config.ForceReliableTransport = true
	}

	bindAddress, err := st.parseBindAddress(st.config.BindAddr)
	if err != nil {
		return err
	}

	st.logger.Info("bind to address", "bind_addr", bindAddress.IP.String(), "bind_port", bindAddress.Port)

	tcpAddr := &net.TCPAddr{
		IP:   bindAddress.IP,
		Port: int(bindAddress.Port),
	}
	if st.config.ListenFunc != nil {
		st.tcpListener, err = st.config.ListenFunc("tcp", tcpAddr.String())
	} else {
		st.tcpListener, err = net.ListenTCP("tcp", tcpAddr)
	}
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
	st.pool.closeAll()

	// packetChannel is deliberately NOT closed.
	//
	// Per-connection packetToQueue goroutines may still be mid-send when
	// shutdown runs. Those sends use a select against ctx.Done(), but when both
	// cases are ready Go picks at random, so a send can still be chosen after
	// the channel is closed — which panics with "send on closed channel" and
	// takes down the process.
	//
	// Nothing requires the close: Cluster.acceptPackets exits on
	// shutdownContext.Done(), not on channel closure, and the channel becomes
	// garbage once the transport is unreachable.
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

// SendWithReply sends packet and waits for the reply. Connections are pooled per peer address and reused for later
// requests. A pooled connection is checked before use; one the peer has closed is discarded and never written to.
// Nothing is retried after the request has been sent, so a request is never delivered twice.
func (st *SocketTransport) SendWithReply(node *Node, packet *Packet) (*Packet, error) {
	raw, err := st.packetToBuffer(packet, true)
	if err != nil {
		return nil, err
	}
	conn, addr, uses, err := st.replyConn(node)
	if err != nil {
		return nil, err
	}
	reply, err := st.roundTrip(conn, raw)
	if err != nil {
		conn.Close()
		node.ClearAddress()
		return nil, err
	}
	st.pool.put(addr, conn, uses+1)
	return reply, nil
}

// replyConn returns a live pooled connection to node (with the number of replies it has carried), or dials one.
func (st *SocketTransport) replyConn(node *Node) (net.Conn, string, int, error) {
	if err := st.ensureNodeAddressResolved(node); err == nil {
		addr := node.GetAddress()
		key := addr.String()
		for {
			conn, uses := st.pool.get(key)
			if conn == nil {
				break
			}
			if alive, _ := connAlive(conn); alive {
				return conn, key, uses, nil
			}
			conn.Close()
			if uses == 1 {
				// Closed right after its first reply: an older peer that does not keep connections open.
				st.pool.refuse(key)
			}
		}
	}
	conn, err := st.dialPeer(node)
	if err != nil {
		return nil, "", 0, err
	}
	addr := node.GetAddress()
	return conn, addr.String(), 0, nil
}

// roundTrip writes a request and reads its reply.
func (st *SocketTransport) roundTrip(conn net.Conn, raw []byte) (*Packet, error) {
	if err := st.writeRawPacket(conn, raw); err != nil {
		return nil, err
	}
	reply, _, _, err := st.readPacketFlags(conn, st.config.TCPDeadline)
	return reply, err
}

// SetStreamHandler implements streamTransport.
func (st *SocketTransport) SetStreamHandler(handler func(packet *Packet, w io.Writer) error) {
	st.streamHandler = handler
}

// OpenStream implements streamTransport: the request goes as a packet on a
// new connection and the reply comes back on it as frames.
func (st *SocketTransport) OpenStream(ctx context.Context, node *Node, packet *Packet) (io.ReadCloser, error) {
	raw, err := st.packetToBuffer(packet, false)
	if err != nil {
		return nil, err
	}
	setStreamFlag(raw)

	conn, err := st.dialPeer(node)
	if err != nil {
		return nil, err
	}
	if err := st.writeRawPacket(conn, raw); err != nil {
		conn.Close()
		node.ClearAddress()
		return nil, err
	}

	fr := newFrameReader(conn, conn, st.openFrame(), func() {
		conn.SetReadDeadline(time.Now().Add(st.config.StreamIdleTimeout))
	})
	fr.closeWith(ctx)
	return fr, nil
}

// serveStream answers a stream request on its connection; shutting down
// the transport ends it.
func (st *SocketTransport) serveStream(ctx context.Context, conn net.Conn, packet *Packet) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	// The packet does not own the connection; the reply is written here.
	packet.SetConn(nil)

	fw := newFrameWriter(ctx, conn, st.sealFrame(), func() {
		conn.SetWriteDeadline(time.Now().Add(st.config.StreamIdleTimeout))
	})
	if st.streamHandler == nil {
		packet.Release()
		fw.finish(ErrStreamsUnsupported)
		return
	}
	fw.finish(st.streamHandler(packet, fw))
}

// sealFrame and openFrame encrypt and decrypt stream frames with the cluster
// key, as packets are; nil when the cluster is not encrypted.
func (st *SocketTransport) sealFrame() func([]byte) ([]byte, error) {
	if st.config.Cipher == nil {
		return nil
	}
	return func(b []byte) ([]byte, error) { return st.config.Cipher.Encrypt(st.config.EncryptionKey, b) }
}

func (st *SocketTransport) openFrame() func([]byte) ([]byte, error) {
	if st.config.Cipher == nil {
		return nil
	}
	return func(b []byte) ([]byte, error) { return st.config.Cipher.Decrypt(st.config.EncryptionKey, b) }
}

func (st *SocketTransport) tcpListen(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		conn, err := st.tcpListener.Accept()
		if err != nil {
			if st.isNetClosedError(err) {
				return
			}
			st.logger.WithError(err).Error("failed to accept TCP connection")
			continue
		}

		go st.packetToQueue(conn, ctx)
	}
}

func (st *SocketTransport) udpListen(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		// Get buffer from pool
		bufPtr := st.udpBufferPool.Get().(*[]byte)
		buf := *bufPtr

		n, _, err := st.udpListener.ReadFromUDP(buf)
		if err != nil {
			// Return buffer to pool before handling error
			st.udpBufferPool.Put(bufPtr)

			if st.isNetClosedError(err) {
				return
			}
			st.logger.WithError(err).Error("failed to read from UDP connection")
			continue
		}

		if n > 0 {
			packetData := make([]byte, n)
			copy(packetData, buf[:n])
			st.udpBufferPool.Put(bufPtr)

			packet, _, err := st.packetFromBuffer(packetData)
			if err != nil {
				st.logger.WithError(err).Error("failed to decode UDP packet")
				continue
			}

			select {
			case <-ctx.Done():
				packet.Release()
				return
			case st.packetChannel <- packet:
			}
		} else {
			// No data received, return buffer to pool
			st.udpBufferPool.Put(bufPtr)
		}
	}
}

// packetToQueue reads packets from an accepted connection. A fire-and-forget packet owns the connection (it is
// closed when the packet is released). After answering a request the connection stays open for the client's next
// request until it is closed or idle for TCPIdleTimeout, so clients can reuse it; older clients simply close it.
func (st *SocketTransport) packetToQueue(conn net.Conn, ctx context.Context) {
	timeout := st.config.TCPDeadline
	for first := true; ; first = false {
		packet, flags, n, err := st.readPacketFlags(conn, timeout)
		if err != nil {
			// EOF or an idle timeout between requests is the normal end of a reused connection.
			if first || n > 0 {
				st.logger.WithError(err).Error("failed to read packet")
			}
			conn.Close()
			return
		}

		if flags&streamFlag != 0 {
			st.serveStream(ctx, conn, packet)
			return
		}

		if flags&replyExpectedFlag == 0 {
			packet.SetConn(conn)
			select {
			case <-ctx.Done():
				packet.Release()
			case st.packetChannel <- packet:
			}
			return
		}

		if !st.serveRequest(ctx, conn, packet) {
			conn.Close()
			return
		}
		timeout = st.config.TCPIdleTimeout
		if timeout <= 0 {
			timeout = st.config.TCPDeadline
		}
	}
}

// serveRequest queues a request, waits for its reply and writes it to conn. The packet does not own the
// connection, so releasing it leaves the connection open. It reports whether conn can carry another request; after
// a timeout it cannot, since a late reply would be read as the answer to the next request.
func (st *SocketTransport) serveRequest(ctx context.Context, conn net.Conn, packet *Packet) bool {
	replyChan := make(chan *Packet, 1)
	packet.SetReplyChan(replyChan)
	defer close(replyChan)

	select {
	case <-ctx.Done():
		packet.Release()
		return false
	case st.packetChannel <- packet:
	}

	select {
	case replyPacket := <-replyChan:
		if replyPacket == nil {
			return false
		}
		err := st.writePacket(conn, replyPacket, false)
		replyPacket.Release()
		if err != nil {
			st.logger.WithError(err).Error("failed to write reply packet")
			return false
		}
		return true
	case <-time.After(st.config.TCPDeadline):
		return false
	case <-ctx.Done():
		select {
		case replyPacket := <-replyChan:
			if replyPacket != nil {
				replyPacket.Release()
			}
		default:
		}
		return false
	}
}

func (st *SocketTransport) sendTCP(node *Node, rawPacket []byte) error {
	conn, err := st.dialPeer(node)
	if err != nil {
		node.ClearAddress()
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

	addr := node.GetAddress()
	if addr.IP != nil {
		if err := tryWrite(addr); err == nil {
			return nil
		}
		node.ClearAddress()
	}

	addrs, err := st.resolveAddress(node.AdvertisedAddr())
	if err != nil || len(addrs) == 0 {
		return fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	var sendErr error
	for _, addr := range addrs {
		if err := tryWrite(addr); err == nil {
			node.SetAddress(addr)
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
		if st.config.DialFunc != nil {
			ctx, cancel := context.WithTimeout(context.Background(), st.config.TCPDialTimeout)
			defer cancel()
			return st.config.DialFunc(ctx, "tcp", tcpAddr.String())
		}
		return net.DialTimeout("tcp", tcpAddr.String(), st.config.TCPDialTimeout)
	}

	addr := node.GetAddress()
	if addr.IP != nil {
		if conn, err := tryDial(addr); err == nil {
			return conn, nil
		}
		node.ClearAddress()
	}

	addrs, err := st.resolveAddress(node.AdvertisedAddr())
	if err != nil || len(addrs) == 0 {
		return nil, fmt.Errorf("failed to resolve address for node %s: %v", node.ID, err)
	}

	var firstErr error
	for _, addr := range addrs {
		if conn, err := tryDial(addr); err == nil {
			node.SetAddress(addr)
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
	if !node.IsAddressEmpty() {
		return nil
	}

	if node.AdvertisedAddr() == "" {
		return fmt.Errorf("no advertise address available")
	}

	addresses, err := st.resolveAddress(node.AdvertisedAddr())
	if err != nil {
		return fmt.Errorf("failed to resolve address %s: %v", node.AdvertisedAddr(), err)
	}

	if len(addresses) == 0 {
		return fmt.Errorf("no addresses resolved for %s", node.AdvertisedAddr())
	}

	node.SetAddress(addresses[0])
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

	err = binary.Write(&buf, binary.LittleEndian, headerSize)
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

	flags := binary.LittleEndian.Uint16(data[:2])
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
	err = binary.Write(&writeBuffer, binary.LittleEndian, uint32(len(rawPacket)))
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

// readPacketFlags reads one length-prefixed packet and its wire flags, waiting at most timeout. It reads exactly
// one packet's bytes (no read-ahead), so the connection can carry further packets. n is the number of bytes read.
func (st *SocketTransport) readPacketFlags(conn net.Conn, timeout time.Duration) (*Packet, uint16, int, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, 0, 0, err
	}

	lengthBytes := make([]byte, 4)
	n, err := io.ReadFull(conn, lengthBytes)
	if err != nil {
		return nil, 0, n, err
	}

	dataLen := binary.LittleEndian.Uint32(lengthBytes)
	if dataLen > uint32(st.config.TCPMaxPacketSize) {
		return nil, 0, n, fmt.Errorf("packet size too large: %d bytes", dataLen)
	}

	receivedData := make([]byte, dataLen)
	m, err := io.ReadFull(conn, receivedData)
	n += m
	if err != nil {
		return nil, 0, n, err
	}

	packet, _, err := st.packetFromBuffer(receivedData)
	if err != nil {
		return nil, 0, n, err
	}
	return packet, binary.LittleEndian.Uint16(receivedData[:2]), n, nil
}
