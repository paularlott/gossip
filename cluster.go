package gossip

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	ErrUnsupportedAddressFormat = fmt.Errorf("unsupported address format")
)

type Cluster struct {
	config          *Config
	shutdownContext context.Context
	cancelFunc      context.CancelFunc
	shutdownWg      sync.WaitGroup
	msgHistory      *messageHistory
	transport       Transport
	nodes           *nodeList
	localNode       *Node
	handlers        *handlerRegistry
	broadcastQueue  chan *broadcastQItem
	healthMonitor   *healthMonitor
	messageIdGen    atomic.Pointer[MessageID]
}

type broadcastQItem struct {
	packet        *Packet
	transportType TransportType
	excludePeers  []NodeID
}

func NewCluster(config *Config) (*Cluster, error) {

	// Merge the config with the default so all fields are set
	if config == nil {
		config = DefaultConfig()
	}

	if config.AdvertiseAddr == "" {
		config.AdvertiseAddr = config.BindAddr
	}

	if config.Logger == nil {
		config.Logger = NewNullLogger()
	}

	// Check we have a codec for encoding and decoding messages
	if config.MsgCodec == nil {
		return nil, fmt.Errorf("missing MsgCodec")
	}

	// Check the encrypt key, it must be either 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.
	if len(config.EncryptionKey) != 0 && len(config.EncryptionKey) != 16 && len(config.EncryptionKey) != 24 && len(config.EncryptionKey) != 32 {
		return nil, fmt.Errorf("invalid encrypt key length: must be 0, 16, 24, or 32 bytes")
	}

	var u uuid.UUID
	var err error

	// If Node ID is given in the config, use it
	if config.NodeID != "" {
		u, err = uuid.Parse(config.NodeID)
		if err != nil {
			return nil, fmt.Errorf("invalid Node ID: %v", err)
		}
	} else {
		// Generate a new UUID for the node ID
		u, err = uuid.NewV7()
		if err != nil {
			u = uuid.New()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cluster := &Cluster{
		config:          config,
		shutdownContext: ctx,
		cancelFunc:      cancel,
		msgHistory:      newMessageHistory(config),
		nodes:           newNodeList(config),
		localNode:       newNode(NodeID(u), Address{}),
		handlers:        newHandlerRegistry(),
		broadcastQueue:  make(chan *broadcastQItem, config.SendQueueSize),
	}

	// Resolve the local node's address
	addresses, err := cluster.ResolveAddress(config.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve local address: %v", err)
	}
	cluster.localNode.address = addresses[0]

	initialMessageID := MessageID{
		Timestamp: time.Now().UnixNano(),
		Seq:       0,
	}
	cluster.messageIdGen.Store(&initialMessageID)

	// Trigger the event listener
	if config.EventListener != nil {
		config.EventListener.OnInit(cluster)
	}

	// Add the local node to the node list
	cluster.nodes.addOrUpdate(cluster.localNode)

	// Resolve the local node's address
	addresses, err = cluster.ResolveAddress(config.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve local address: %v", err)
	}

	if config.Transport == nil {
		cluster.transport, err = NewTransport(ctx, &cluster.shutdownWg, config, addresses[0], cluster.localNode)
		if err != nil {
			return nil, fmt.Errorf("failed to create transport: %v", err)
		}
	} else {
		cluster.transport = config.Transport
	}

	// Add all background goroutines to the WaitGroup
	cluster.shutdownWg.Add(1 + config.NumSendWorkers)

	// Start the workers
	for range config.NumSendWorkers {
		go cluster.broadcastWorker()
	}
	go cluster.acceptPackets()

	// Start the health monitor
	cluster.healthMonitor = newHealthMonitor(cluster)

	// Register the system message handlers
	cluster.registerSystemHandlers()

	// Start periodic state synchronization
	cluster.startStateSync()

	cluster.config.Logger.Infof("Cluster initialized with Node ID: %s", u.String())

	return cluster, nil
}

func (c *Cluster) Shutdown() {
	if c.localNode.state != nodeLeaving {
		c.Leave()
	}

	if c.healthMonitor != nil {
		c.healthMonitor.stop()
	}

	if c.msgHistory != nil {
		c.msgHistory.stop()
	}

	if c.cancelFunc != nil {
		c.cancelFunc()
	}

	// Wait for all goroutines to finish
	c.shutdownWg.Wait()

	c.config.Logger.Infof("Cluster stopped")
}

// Handler for incoming WebSocket connections when gossiping over web sockets
func (c *Cluster) WebsocketHandler(w http.ResponseWriter, r *http.Request) {
	c.transport.WebsocketHandler(c.shutdownContext, w, r)
}

func (c *Cluster) ResolveAddress(addressStr string) ([]Address, error) {

	// If the address has a | in it then split it as we're giving a IP:port|url combination
	if strings.Contains(addressStr, "|") {
		parts := strings.Split(addressStr, "|")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid address format: %s", addressStr)
		}

		// Part 1 must not be http(s)://
		if strings.Contains(parts[0], "http://") || strings.Contains(parts[0], "https://") || strings.Contains(parts[0], "ws://") || strings.Contains(parts[0], "wss://") {
			return nil, fmt.Errorf("invalid address format: %s", addressStr)
		}
		// Part 2 must be http(s)://
		if !strings.Contains(parts[1], "http://") && !strings.Contains(parts[1], "https://") && !strings.Contains(parts[1], "ws://") && !strings.Contains(parts[1], "wss://") {
			return nil, fmt.Errorf("invalid address format: %s", addressStr)
		}

		// Resolve the 2 parts
		part1, err := c.ResolveAddress(parts[0])
		if err != nil {
			return nil, fmt.Errorf("failed to resolve address: %s", err)
		}
		part2, err := c.ResolveAddress(parts[1])
		if err != nil {
			return nil, fmt.Errorf("failed to resolve address: %s", err)
		}

		// Combine the 2 parts, we only take the first address from each
		addr := Address{
			IP:   part1[0].IP,
			Port: part1[0].Port,
			URL:  part2[0].URL,
		}

		return []Address{addr}, nil
	}

	// If SRV record
	if strings.HasPrefix(addressStr, "srv+") {
		serviceName := addressStr[4:] // Remove the "srv+" prefix

		// If http(s):// then we're doing web sockets, lets resolve the address
		if strings.HasPrefix(serviceName, "http://") || strings.HasPrefix(serviceName, "https://") || strings.HasPrefix(serviceName, "ws://") || strings.HasPrefix(serviceName, "wss://") {
			addresses := make([]Address, 0)

			// If no web socket provider is set then we can't do this
			if c.config.WebsocketProvider == nil {
				return addresses, ErrUnsupportedAddressFormat
			}

			// Extract the host from the url
			parsedURL, err := url.Parse(serviceName)
			if err != nil {
				return addresses, fmt.Errorf("failed to parse URL: %v", err)
			}

			addr, err := c.lookupSRV(parsedURL.Hostname(), false)
			if err != nil {
				return addresses, fmt.Errorf("failed to lookup SRV record: %v", err)
			}

			if len(addr) > 0 {
				// Fix the schema to be ws:// or wss://
				if strings.HasPrefix(parsedURL.Scheme, "http") {
					parsedURL.Scheme = "ws" + strings.TrimPrefix(parsedURL.Scheme, "http")
				}

				addresses = append(addresses, Address{
					URL: fmt.Sprintf("%s://%s:%d%s", parsedURL.Scheme, parsedURL.Hostname(), addr[0].Port, parsedURL.Path),
				})
			}

			return addresses, nil
		} else {
			// If socket transport not enabled then we can't do this
			if !c.config.SocketTransportEnabled {
				return []Address{}, ErrUnsupportedAddressFormat
			}

			return c.lookupSRV(serviceName, true)
		}
	}

	// If http(s):// then we're doing web sockets, lets resolve the address
	if strings.HasPrefix(addressStr, "http://") || strings.HasPrefix(addressStr, "https://") || strings.HasPrefix(addressStr, "ws://") || strings.HasPrefix(addressStr, "wss://") {
		if c.config.WebsocketProvider == nil {
			return []Address{}, ErrUnsupportedAddressFormat
		}

		if strings.HasPrefix(addressStr, "http") {
			addressStr = "ws" + strings.TrimPrefix(addressStr, "http")
		}

		return []Address{
			{
				URL: addressStr,
			},
		}, nil
	} else {
		if !c.config.SocketTransportEnabled {
			return []Address{}, ErrUnsupportedAddressFormat
		}

		return c.lookupIP(addressStr, c.config.DefaultPort)
	}
}

func (c *Cluster) lookupSRV(serviceName string, resolveToIPs bool) ([]Address, error) {
	addresses := make([]Address, 0)

	// Make sure the service ends with a dot
	if !strings.HasSuffix(serviceName, ".") {
		serviceName += "."
	}

	// Look up the SRV record
	_, addrs, err := net.LookupSRV("", "", serviceName)
	if err != nil {
		return addresses, fmt.Errorf("failed to lookup SRV record")
	}

	if len(addrs) == 0 {
		return addresses, fmt.Errorf("no SRV records found for service")
	}

	if resolveToIPs {
		for _, srv := range addrs {
			addr, err := c.lookupIP(srv.Target, int(srv.Port))
			if err == nil {
				addresses = append(addresses, addr...)
			}
		}
	} else {
		for _, srv := range addrs {
			addresses = append(addresses, Address{
				Port: int(srv.Port),
			})
		}
	}

	return addresses, nil
}

func (c *Cluster) lookupIP(host string, defaultPort int) ([]Address, error) {
	addresses := make([]Address, 0)

	// If the address string contains only numbers then assume it's a port and prefix with :
	if _, err := strconv.Atoi(host); err == nil {
		host = ":" + host
	}

	// If the host doesn't contain a port then use the default port
	if !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, defaultPort)
	}

	// Split the address into host and port
	hostStr, portStr, err := net.SplitHostPort(host)
	if err != nil {
		return addresses, err
	}

	// If host is empty then use loopback address
	if hostStr == "" {
		hostStr = "127.0.0.1"
	}

	// Parse port value
	var port int
	portVal, err := strconv.ParseUint(portStr, 10, 16)
	if err == nil {
		port = int(portVal)
	} else {
		port = defaultPort
	}

	// Resolve the IP address
	var ip net.IP
	if ip = net.ParseIP(hostStr); ip == nil {
		// Host is a hostname, resolve it
		ips, err := net.LookupIP(hostStr)
		if err != nil {
			return addresses, fmt.Errorf("failed to resolve hostname")
		}

		for _, ip = range ips {
			addresses = append(addresses, Address{
				IP:   ip,
				Port: port,
			})
		}
	} else {
		addresses = append(addresses, Address{
			IP:   ip,
			Port: port,
		})
	}

	return addresses, nil
}

func (c *Cluster) Join(peers []string) error {
	if len(peers) == 0 {
		return fmt.Errorf("no peers provided")
	}

	// Shuffle the peers, many nodes may be using the same peer list so shuffling helps to spread the load
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	// Join the cluster by attempting to connect to as many peers as possible
	for _, peerAddr := range peers {
		c.config.Logger.Debugf("Attempting to join peer: %s", peerAddr)

		// Resolve the address
		addresses, err := c.ResolveAddress(peerAddr)
		if err != nil {
			c.config.Logger.Err(err).Warnf("Failed to resolve address: %s", peerAddr)
			continue
		}

		for _, addr := range addresses {
			joinMsg := &joinMessage{
				ID:                c.localNode.ID,
				Address:           c.localNode.address,
				MetadataTimestamp: c.localNode.metadata.GetTimestamp(),
				Metadata:          c.localNode.metadata.GetAll(),
			}

			node := newNode(c.localNode.ID, addr)
			err := c.sendToWithResponse(node, nodeJoinMsg, &joinMsg, nodeJoinAckMsg, &joinMsg)
			if err != nil {
				c.config.Logger.Err(err).Warnf("Failed to join peer %s", peerAddr)
				continue
			}

			// Update the node with the peer's advertised address and ID then save it
			node.ID = joinMsg.ID
			node.address = joinMsg.Address
			node.metadata.update(joinMsg.Metadata, joinMsg.MetadataTimestamp, true)
			if c.nodes.addIfNotExists(node) {
				err = c.exchangeState(node, []NodeID{c.localNode.ID})
				if err != nil {
					c.config.Logger.Err(err).Warnf("Failed to exchange state with peer %s", peerAddr)
				}
			}

			c.config.Logger.Infof("Joined peer: %s (%s)", peerAddr, addr.String())
		}
	}

	return nil
}

// MMarks the local node as leaving and broadcasts this state to the cluster
func (c *Cluster) Leave() {
	c.config.Logger.Infof("Local node is leaving the cluster")
	c.healthMonitor.MarkNodeLeaving(c.localNode)
}

func (c *Cluster) acceptPackets() {
	defer c.shutdownWg.Done()

	for {
		select {
		case incomingPacket := <-c.transport.PacketChannel():
			if incomingPacket != nil {
				go c.handleIncomingPacket(incomingPacket)
			}

		case <-c.shutdownContext.Done():
			return
		}
	}
}

func (c *Cluster) handleIncomingPacket(incomingPacket *IncomingPacket) {
	var transportType TransportType
	if incomingPacket.Conn != nil {
		defer incomingPacket.Conn.Close()
		transportType = TransportReliable
	} else {
		transportType = TransportBestEffort
	}

	// If the sender is us or already seen then ignore the message
	packet := incomingPacket.Packet
	if packet.SenderID == c.localNode.ID || c.msgHistory.contains(packet.SenderID, packet.MessageID) {
		return
	}

	// Record the message in the message history
	c.msgHistory.recordMessage(packet.SenderID, packet.MessageID)

	// Run the message handler
	h := c.handlers.getHandler(packet.MessageType)
	if h != nil {
		if h.forward {
			c.enqueuePacketForBroadcast(packet, transportType, []NodeID{c.localNode.ID, packet.SenderID})
		}

		senderNode := c.nodes.get(packet.SenderID)
		if senderNode != nil {
			senderNode.updateLastActivity()
		}

		err := h.dispatch(incomingPacket.Conn, c, senderNode, packet)
		if err != nil {
			c.config.Logger.Err(err).Warnf("Error dispatching packet: %d", packet.MessageType)
		}
	} else {
		c.config.Logger.Warnf("No handler registered for message type: %d", packet.MessageType)
	}
}

// GetPeerSubsetSize calculates the number of peers to use for operations based on cluster size and purpose
func (c *Cluster) getPeerSubsetSize(totalNodes int, purpose peerSelectionPurpose) int {
	if totalNodes <= 0 {
		return 0
	}

	// Get the base count
	basePeerCount := math.Log2(float64(totalNodes))
	cap := 10.0

	// Apply purpose-specific adjustments
	switch purpose {
	case purposeBroadcast:
		basePeerCount = math.Ceil(basePeerCount * c.config.BroadcastMultiplier)

	case purposeStateExchange:
		// Add 2 to the base for more aggressive state propagation
		basePeerCount = math.Ceil(basePeerCount*c.config.StateExchangeMultiplier) + 2
		cap = 16

	case purposeIndirectPing:
		basePeerCount = math.Ceil(basePeerCount * c.config.IndirectPingMultiplier)
		cap = 6

	case purposeTTL:
		basePeerCount = math.Ceil(basePeerCount * c.config.TTLMultiplier)
		cap = 8

	default:
		basePeerCount = math.Ceil(basePeerCount)
	}

	// Apply the cap
	return int(math.Max(1, math.Min(basePeerCount, cap)))
}

func (c *Cluster) getMaxTTL() uint8 {
	return uint8(c.getPeerSubsetSize(c.nodes.getLiveCount(), purposeTTL))
}

// Exchange the state of a random subset of nodes with the given node
func (c *Cluster) exchangeState(node *Node, exclude []NodeID) error {
	// Determine how many nodes to include in the exchange
	sampleSize := c.getPeerSubsetSize(c.nodes.getTotalCount(), purposeStateExchange)

	// Get a random selection of nodes, excluding specified nodes
	randomNodes := c.nodes.getRandomNodes(sampleSize, exclude)

	// Create the state exchange message
	var peerStates []exchangeNodeState
	for _, n := range randomNodes {
		peerStates = append(peerStates, exchangeNodeState{
			ID:                n.ID,
			Address:           n.address,
			State:             n.state,
			StateChangeTime:   n.stateChangeTime.UnixNano(),
			MetadataTimestamp: n.metadata.GetTimestamp(),
			Metadata:          n.metadata.GetAll(),
		})
	}

	// No nodes to exchange, this is fine
	if len(peerStates) == 0 {
		return nil
	}

	// Exchange state with the peer
	err := c.sendToWithResponse(
		node,
		pushPullStateMsg,
		&peerStates,
		pushPullStateAckMsg,
		&peerStates)
	if err != nil {
		return err
	}

	// Process the received states
	c.healthMonitor.combineRemoteNodeState(node, peerStates)
	return nil
}

// Enqueue a packet for broadcasting to peers.
// If useReliable is true, the packet will be sent reliably, if false it will be sent over UDP if it's small enough or TCP otherwise.
func (c *Cluster) enqueuePacketForBroadcast(packet *Packet, transportType TransportType, excludePeers []NodeID) {

	// Once the packets TTL is 0 we don't forward it stops it bouncing around the cluster
	if packet.TTL == 0 {
		return
	}
	packet.TTL--

	item := &broadcastQItem{
		packet:        packet,
		transportType: transportType,
		excludePeers:  excludePeers,
	}

	// Use non-blocking send to avoid getting stuck if queue is full
	select {
	case c.broadcastQueue <- item:
		// Successfully queued
	default:
		// Queue full, log and skip this message
		c.config.Logger.Errorf("Broadcast queue is full, skipping message")
	}
}

func (c *Cluster) broadcastWorker() {
	defer c.shutdownWg.Done()

	for {
		select {
		case item := <-c.broadcastQueue:

			// Get the peer subset to send the packet to
			peerSubset := c.nodes.getRandomLiveNodes(c.getPeerSubsetSize(c.nodes.getLiveCount(), purposeBroadcast), item.excludePeers)
			for _, peer := range peerSubset {
				if err := c.transport.SendPacket(item.transportType, peer, item.packet); err != nil {
					c.config.Logger.Err(err).Debugf("Failed to send packet to peer %s", peer.ID)
				}
			}

		case <-c.shutdownContext.Done():
			return
		}
	}
}

// Start periodic state synchronization with random peers
func (c *Cluster) startStateSync() {
	go func() {
		// Add jitter to prevent all nodes syncing at the same time
		jitter := time.Duration(rand.Int63n(int64(c.config.StateSyncInterval / 4)))
		time.Sleep(jitter)

		ticker := time.NewTicker(c.config.StateSyncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Calculate appropriate number of peers based on cluster size
				peerCount := c.getPeerSubsetSize(c.nodes.getLiveCount(), purposeStateExchange)
				if peerCount == 0 {
					continue
				}

				// Get random subset, excluding ourselves
				peers := c.nodes.getRandomLiveNodes(peerCount, []NodeID{c.localNode.ID})

				// Perform state exchange with selected peers
				for _, peer := range peers {
					go func(p *Node) {
						err := c.exchangeState(p, []NodeID{c.localNode.ID, p.ID})
						if err != nil {
							c.config.Logger.Err(err).Field("peer", p.ID.String()).Debugf("Periodic state exchange failed")
						} else {
							c.config.Logger.Field("peer", p.ID.String()).Debugf("Completed periodic state exchange")
						}
					}(peer)
				}

			case <-c.shutdownContext.Done():
				return
			}
		}
	}()
}

func (c *Cluster) GetLocalNode() *Node {
	return c.localNode
}

// Get the local nodes metadata for read and write access
func (c *Cluster) LocalMetadata() *Metadata {
	return c.localNode.metadata
}

func (c *Cluster) GetAllNodes() []*Node {
	return c.nodes.getAll()
}

func (c *Cluster) GetNodeByID(id NodeID) *Node {
	return c.nodes.get(id)
}

func (c *Cluster) NumNodes() int {
	return c.nodes.getTotalCount()
}

// Get the number of nodes that are currently alive or suspect
func (c *Cluster) NumLiveNodes() int {
	return c.nodes.getLiveCount()
}

// Get the number of nodes that are currently alive
func (c *Cluster) NumAliveNodes() int {
	return c.nodes.getAliveCount()
}

// Get the number of nodes that are currently suspect
func (c *Cluster) NumSuspectNodes() int {
	return c.nodes.getSuspectCount()
}

// Get the number of nodes that are currently dead
func (c *Cluster) NumDeadNodes() int {
	return c.nodes.getDeadCount()
}

// Registers a handler to accept a message and automatically forward it to other nodes
func (c *Cluster) HandleFunc(msgType MessageType, handler Handler) error {
	if msgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}
	c.handlers.registerHandler(msgType, true, handler)
	return nil
}

// Registers a handler to accept a message without automatically forwarding it to other nodes
func (c *Cluster) HandleFuncNoForward(msgType MessageType, handler Handler) error {
	if msgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}
	c.handlers.registerHandler(msgType, false, handler)
	return nil
}

// Registers a handler to accept a message and reply to the sender, always uses the reliable transport
func (c *Cluster) HandleFuncWithReply(msgType MessageType, replyHandler ReplyHandler) error {
	if msgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}
	c.handlers.registerHandlerWithReply(msgType, replyHandler)
	return nil
}

func (c *Cluster) NodeIsLocal(node *Node) bool {
	return node.ID == c.localNode.ID
}
