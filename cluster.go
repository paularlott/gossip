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

const (
	PROTOCOL_VERSION = 1
)

type Cluster struct {
	config                      *Config
	shutdownContext             context.Context
	cancelFunc                  context.CancelFunc
	shutdownWg                  sync.WaitGroup
	msgHistory                  *messageHistory
	transport                   Transport
	nodes                       *nodeList
	localNode                   *Node
	handlers                    *handlerRegistry
	broadcastQueue              chan *broadcastQItem
	healthMonitor               *healthMonitor
	messageIdGen                atomic.Pointer[MessageID]
	stateEventHandlers          *eventHandlers[NodeStateChangeHandler]
	metadataChangeEventHandlers *eventHandlers[NodeMetadataChangeHandler]
	gossipEventHandlers         *eventHandlers[GossipHandler]
	gossipTicker                *time.Ticker
	gossipInterval              time.Duration
	broadcastQItemPool          sync.Pool
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

	// If both socket and websocket transports are enabled then block this
	if config.SocketTransportEnabled && config.WebsocketProvider != nil {
		return nil, fmt.Errorf("both socket and websocket transports are enabled")
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
		config:                      config,
		shutdownContext:             ctx,
		cancelFunc:                  cancel,
		msgHistory:                  newMessageHistory(config),
		localNode:                   newNode(NodeID(u), Address{}),
		handlers:                    newHandlerRegistry(),
		broadcastQueue:              make(chan *broadcastQItem, config.SendQueueSize),
		stateEventHandlers:          NewEventHandlers[NodeStateChangeHandler](),
		metadataChangeEventHandlers: NewEventHandlers[NodeMetadataChangeHandler](),
		gossipEventHandlers:         NewEventHandlers[GossipHandler](),
		broadcastQItemPool:          sync.Pool{New: func() interface{} { return new(broadcastQItem) }},
	}

	cluster.nodes = newNodeList(cluster)

	cluster.localNode.ProtocolVersion = PROTOCOL_VERSION
	cluster.localNode.ApplicationVersion = config.ApplicationVersion

	if len(cluster.config.EncryptionKey) == 0 && cluster.config.Cipher != nil {
		return nil, fmt.Errorf("crypter is set but no encryption key is provided")
	}

	// Resolve the local node's address
	addresses, err := cluster.ResolveAddress(config.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve advertise address: %v", err)
	}
	cluster.localNode.address = addresses[0]

	initialMessageID := MessageID{
		Timestamp: time.Now().UnixNano(),
		Seq:       0,
	}
	cluster.messageIdGen.Store(&initialMessageID)

	// Add the local node to the node list
	cluster.nodes.addOrUpdate(cluster.localNode)

	// Resolve the local node's address
	if config.SocketTransportEnabled {
		addresses, err = cluster.ResolveAddress(config.BindAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve bind address: %v", err)
		}
	} else {
		addresses = []Address{cluster.localNode.address}
	}

	// Check we have a bind port or advertised address
	if addresses[0].Port == 0 && cluster.localNode.address.URL == "" {
		return nil, fmt.Errorf("no bind port or advertised WebSocket address specified")
	}

	if config.Transport == nil {
		cluster.transport, err = NewTransport(ctx, &cluster.shutdownWg, config, addresses[0])
		if err != nil {
			return nil, fmt.Errorf("failed to create transport: %v", err)
		}
	} else {
		cluster.transport = config.Transport
	}

	// If using websockets then see if we should disable compression
	if cluster.config.WebsocketProvider != nil && cluster.config.WebsocketProvider.CompressionEnabled() {
		cluster.config.Compressor = nil
	}

	return cluster, nil
}

func (c *Cluster) Start() {
	// Start the send workers
	for range c.config.NumSendWorkers {
		c.shutdownWg.Add(1)
		go c.broadcastWorker()
	}

	// Start the incoming workers
	for range c.config.NumIncomingWorkers {
		c.shutdownWg.Add(1)
		go c.acceptPackets()
	}

	// Start the health monitor
	c.healthMonitor = newHealthMonitor(c)

	// Register the system message handlers
	c.registerSystemHandlers()

	// Start periodic state synchronization
	c.periodicStateSync()

	// Start the gossip notification manager
	c.gossipManager()

	c.config.Logger.Infof("gossip: Cluster started, local node ID: %s", c.localNode.ID.String())
}

func (c *Cluster) Stop() {
	if c.localNode.state != NodeLeaving {
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
}

// Handler for incoming WebSocket connections when gossiping over web sockets
func (c *Cluster) WebsocketHandler(w http.ResponseWriter, r *http.Request) {
	c.transport.WebsocketHandler(c.shutdownContext, w, r)
}

func (c *Cluster) ResolveAddress(addressStr string) ([]Address, error) {
	// If SRV record
	if strings.HasPrefix(addressStr, "srv+") {
		serviceName := addressStr[4:] // Remove the "srv+" prefix

		// If http(s):// then we're doing web sockets, lets resolve the address
		if strings.HasPrefix(serviceName, "http://") || strings.HasPrefix(serviceName, "https://") || strings.HasPrefix(serviceName, "ws://") || strings.HasPrefix(serviceName, "wss://") {
			addresses := make([]Address, 0)

			// If no web socket provider is set then we can't do this
			if c.config.WebsocketProvider == nil || (!c.config.AllowInsecureWebsockets && (strings.HasPrefix(serviceName, "ws://") || strings.HasPrefix(serviceName, "http://"))) {
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
		if c.config.WebsocketProvider == nil || (!c.config.AllowInsecureWebsockets && (strings.HasPrefix(addressStr, "ws://") || strings.HasPrefix(addressStr, "http://"))) {
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
		// c.config.Logger.Debugf("Attempting to join peer: %s", peerAddr)

		// Resolve the address
		addresses, err := c.ResolveAddress(peerAddr)
		if err != nil {
			c.config.Logger.Err(err).Warnf("Failed to resolve address: %s", peerAddr)
			continue
		}

		joinMsg := &joinMessage{
			ID:                 c.localNode.ID,
			Address:            c.localNode.address,
			MetadataTimestamp:  c.localNode.metadata.GetTimestamp(),
			Metadata:           c.localNode.metadata.GetAll(),
			ProtocolVersion:    PROTOCOL_VERSION,
			ApplicationVersion: c.config.ApplicationVersion,
		}

		for _, addr := range addresses {
			joinReply := &joinReplyMessage{}

			node := newNode(c.localNode.ID, addr)
			err := c.sendToWithResponse(node, nodeJoinMsg, &joinMsg, nodeJoinAckMsg, &joinReply)
			if err != nil {
				//c.config.Logger.Err(err).Debugf("Failed to join peer %s", peerAddr)
				continue
			}

			if !joinReply.Accepted {
				c.config.Logger.Warnf("gossip: Peer %s rejected our join request", peerAddr)
				continue
			}

			// Update the node with the peer's advertised address and ID then save it
			node.ID = joinReply.ID
			node.address = joinReply.Address
			node.ProtocolVersion = joinReply.ProtocolVersion
			node.ApplicationVersion = joinReply.ApplicationVersion
			node.metadata.update(joinReply.Metadata, joinReply.MetadataTimestamp, true)
			if c.nodes.addIfNotExists(node) {
				err = c.exchangeState(node, []NodeID{c.localNode.ID})
				if err != nil {
					c.config.Logger.Err(err).Warnf("gossip: Failed to exchange state with peer %s", peerAddr)
				}
			}

			c.config.Logger.Debugf("gossip: Joined peer: %s (%s)", peerAddr, addr.String())
		}
	}

	// Add the list of peers to the health monitor so they can be used for recovery
	for _, peerAddr := range peers {
		c.healthMonitor.addJoinPeer(peerAddr)
	}

	return nil
}

// MMarks the local node as leaving and broadcasts this state to the cluster
func (c *Cluster) Leave() {
	c.config.Logger.Debugf("gossip: Local node is leaving the cluster")
	if c.healthMonitor != nil {
		c.healthMonitor.MarkNodeLeaving(c.localNode)
	}
}

func (c *Cluster) acceptPackets() {
	defer c.shutdownWg.Done()

	for {
		select {
		case incomingPacket := <-c.transport.PacketChannel():
			if incomingPacket != nil {
				c.handleIncomingPacket(incomingPacket)
			}

		case <-c.shutdownContext.Done():
			return
		}
	}
}

func (c *Cluster) handleIncomingPacket(packet *Packet) {
	// If the sender is us or already seen then ignore the message
	if packet.SenderID == c.localNode.ID || c.msgHistory.contains(packet.SenderID, packet.MessageID) {
		packet.Release()
		return
	}

	// Record the message in the message history
	c.msgHistory.recordMessage(packet.SenderID, packet.MessageID)

	// Run the message handler
	h := c.handlers.getHandler(packet.MessageType)
	if h != nil {
		if h.forward {
			var transportType TransportType
			if packet.conn != nil {
				transportType = TransportReliable
			} else {
				transportType = TransportBestEffort
			}
			c.enqueuePacketForBroadcast(packet.AddRef(), transportType, []NodeID{c.localNode.ID, packet.SenderID})
		}

		senderNode := c.nodes.get(packet.SenderID)
		if senderNode != nil {
			senderNode.updateLastActivity()
		}

		err := h.dispatch(c, senderNode, packet)
		if err != nil {
			c.config.Logger.Err(err).Warnf("gossip: Error dispatching packet: %d", packet.MessageType)
		}
	} else {
		packet.Release()
		c.config.Logger.Warnf("gossip: No handler registered for message type: %d", packet.MessageType)
	}
}

func (c *Cluster) getPeerSubsetSizeBroadcast(totalNodes int) int {
	if totalNodes <= 0 {
		return 0
	}

	basePeerCount := math.Ceil(math.Log2(float64(totalNodes)) * c.config.BroadcastMultiplier)
	cap := 10.0

	return int(math.Max(1, math.Min(basePeerCount, cap)))
}

func (c *Cluster) getPeerSubsetSizeStateExchange(totalNodes int) int {
	if totalNodes <= 0 {
		return 0
	}

	basePeerCount := math.Ceil(math.Log2(float64(totalNodes))*c.config.StateExchangeMultiplier) + 2
	cap := 16.0

	return int(math.Max(1, math.Min(basePeerCount, cap)))
}

func (c *Cluster) getPeerSubsetSizeIndirectPing(totalNodes int) int {
	if totalNodes <= 0 {
		return 0
	}

	basePeerCount := math.Ceil(math.Log2(float64(totalNodes)) * c.config.IndirectPingMultiplier)
	cap := 6.0

	return int(math.Max(1, math.Min(basePeerCount, cap)))
}

func (c *Cluster) getMaxTTL() uint8 {
	totalNodes := c.nodes.getLiveCount()
	if totalNodes <= 0 {
		return 0
	}

	basePeerCount := math.Ceil(math.Log2(float64(totalNodes))*c.config.TTLMultiplier) + 2
	cap := 8.0

	return uint8(math.Max(1, math.Min(basePeerCount, cap)))
}

// Exchange the state of a random subset of nodes with the given node
func (c *Cluster) exchangeState(node *Node, exclude []NodeID) error {
	// Determine how many nodes to include in the exchange
	sampleSize := c.getPeerSubsetSizeStateExchange(c.nodes.getTotalCount())

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
		packet.Release()
		return
	}
	packet.TTL--

	item := c.broadcastQItemPool.Get().(*broadcastQItem)
	item.packet = packet
	item.transportType = transportType
	item.excludePeers = excludePeers

	// Use non-blocking send to avoid getting stuck if queue is full
	select {
	case c.broadcastQueue <- item:
		// Successfully queued
	default:
		// Queue full, log and skip this message
		c.config.Logger.Warnf("gossip: Broadcast queue is full, skipping message")
	}
}

func (c *Cluster) broadcastWorker() {
	defer c.shutdownWg.Done()

	for {
		select {
		case item := <-c.broadcastQueue:

			// Get the peer subset to send the packet to
			peerSubset := c.nodes.getRandomLiveNodes(c.getPeerSubsetSizeBroadcast(c.nodes.getLiveCount()), item.excludePeers)
			if err := c.transport.SendPacket(item.transportType, peerSubset, item.packet); err != nil {
				c.config.Logger.Err(err).Debugf("gossip:Failed to send packet to peers")
			}

			// Release the broadcast item back to the pool
			item.packet.Release()
			item.packet = nil
			c.broadcastQItemPool.Put(item)

		case <-c.shutdownContext.Done():
			return
		}
	}
}

// Start periodic state synchronization with random peers
func (c *Cluster) periodicStateSync() {
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
				peerCount := c.getPeerSubsetSizeStateExchange(c.nodes.getLiveCount())
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
							c.config.Logger.Err(err).Field("peer", p.ID.String()).Debugf("gossip: Periodic state exchange failed")
						} else {
							c.config.Logger.Field("peer", p.ID.String()).Debugf("gossip: Completed periodic state exchange")
						}
					}(peer)
				}

			case <-c.shutdownContext.Done():
				return
			}
		}
	}()
}

func (c *Cluster) LocalNode() *Node {
	return c.localNode
}

// Get the local nodes metadata for read and write access
func (c *Cluster) LocalMetadata() *Metadata {
	return c.localNode.metadata
}

func (c *Cluster) Nodes() []*Node {
	return c.nodes.getAll()
}

func (c *Cluster) AliveNodes() []*Node {
	return c.nodes.getAllInStates([]NodeState{NodeAlive})
}

func (c *Cluster) GetNode(id NodeID) *Node {
	return c.nodes.get(id)
}

func (c *Cluster) GetNodeByIDString(id string) *Node {
	nodeID, err := uuid.Parse(id)
	if err != nil {
		return nil
	}
	return c.nodes.get(NodeID(nodeID))
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
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	c.handlers.registerHandler(msgType, true, handler)
	return nil
}

// Registers a handler to accept a message without automatically forwarding it to other nodes
func (c *Cluster) HandleFuncNoForward(msgType MessageType, handler Handler) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	c.handlers.registerHandler(msgType, false, handler)
	return nil
}

// Registers a handler to accept a message and reply to the sender, always uses the reliable transport
func (c *Cluster) HandleFuncWithReply(msgType MessageType, replyHandler ReplyHandler) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	c.handlers.registerHandlerWithReply(msgType, replyHandler)
	return nil
}

// Registers a handler to accept a message and open a stream between sender and destination, always uses the reliable transport
func (c *Cluster) HandleStreamFunc(msgType MessageType, handler StreamHandler) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	c.handlers.registerStreamHandler(msgType, handler)
	return nil
}

func (c *Cluster) HandleNodeStateChangeFunc(handler NodeStateChangeHandler) {
	c.stateEventHandlers.Add(handler)
}

func (c *Cluster) HandleNodeMetadataChangeFunc(handler NodeMetadataChangeHandler) {
	c.metadataChangeEventHandlers.Add(handler)
}

func (c *Cluster) notifyNodeStateChanged(node *Node, prevState NodeState) {
	currentHandlers := c.stateEventHandlers.handlers.Load().([]NodeStateChangeHandler)
	for _, handler := range currentHandlers {
		go handler(node, prevState)
	}
}

func (c *Cluster) notifyMetadataChanged(node *Node) {
	currentHandlers := c.metadataChangeEventHandlers.handlers.Load().([]NodeMetadataChangeHandler)
	for _, handler := range currentHandlers {
		go handler(node)
	}
}

func (c *Cluster) NodeIsLocal(node *Node) bool {
	return node.ID == c.localNode.ID
}

// Get a random subset of nodes to use for gossiping or exchanging states with, excluding ourselves
func (c *Cluster) GetCandidates() []*Node {
	return c.nodes.getRandomLiveNodes(c.getPeerSubsetSizeStateExchange(c.nodes.getLiveCount()), []NodeID{c.localNode.ID})
}

// Get the amount of data to send in a gossip message
func (c *Cluster) GetBatchSize(size int) int {
	batchSize := c.getPeerSubsetSizeStateExchange(size)
	if batchSize > size {
		batchSize = size
	}
	return batchSize
}

func (c *Cluster) HandleGossipFunc(handler GossipHandler) {
	c.gossipEventHandlers.Add(handler)
}

func (c *Cluster) notifyDoGossip() {
	currentHandlers := c.gossipEventHandlers.handlers.Load().([]GossipHandler)
	for _, handler := range currentHandlers {
		handler()
	}
}

func (c *Cluster) gossipManager() {
	// Add jitter to prevent all nodes syncing at the same time
	jitter := time.Duration(rand.Int63n(int64(c.config.GossipInterval / 4)))
	time.Sleep(jitter)

	c.gossipInterval = c.config.GossipInterval
	c.gossipTicker = time.NewTicker(c.config.GossipInterval)

	go func() {
		for {
			select {
			case <-c.gossipTicker.C:
				start := time.Now()
				c.notifyDoGossip()
				elapsed := time.Since(start)

				c.adjustGossipInterval(elapsed)

			case <-c.shutdownContext.Done():
				return
			}
		}
	}()
}

func (c *Cluster) adjustGossipInterval(duration time.Duration) {
	// If handlers take >80% of interval, increase the interval
	if duration > c.gossipInterval {
		// Set interval to slightly more than the duration (25% buffer)
		newInterval := duration * 5 / 4 // 25% more than duration

		// Apply a maximum bound
		if newInterval > c.config.GossipMaxInterval {
			newInterval = c.config.GossipMaxInterval
		}

		c.gossipInterval = newInterval
		c.gossipTicker.Reset(c.gossipInterval)

		c.config.Logger.Field("duration", duration.String()).Field("newInterval", c.gossipInterval.String()).Debugf("gossip: interval increased")
	} else if c.gossipInterval > c.config.GossipInterval && duration < time.Duration(float64(c.gossipInterval)*0.8) {
		// If handlers are fast and we're above original interval, gradually return to base

		// Move halfway back toward original interval
		c.gossipInterval = (duration + c.gossipInterval) / 2

		// Never go below original interval
		if c.gossipInterval < c.config.GossipInterval {
			c.gossipInterval = c.config.GossipInterval
		}

		c.gossipTicker.Reset(c.gossipInterval)
		c.config.Logger.Field("duration", duration.String()).Field("newInterval", c.gossipInterval.String()).Debugf("gossip: interval decreased")
	}
}

func (c *Cluster) Logger() Logger {
	return c.config.Logger
}

// Checks the connections is of Stream type and enables compression if supported
// This is a no-op if the connection is not a Stream
func (c *Cluster) EnableStreamCompression(s net.Conn) *Cluster {
	if stream, ok := s.(*Stream); ok {
		stream.EnableCompression()
	}
	return c
}

// Checks the connections is of Stream type and disables compression if supported
// This is a no-op if the connection is not a Stream
func (c *Cluster) DisableStreamCompression(s net.Conn) *Cluster {
	if stream, ok := s.(*Stream); ok {
		stream.DisableCompression()
	}
	return c
}
