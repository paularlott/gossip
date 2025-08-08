package gossip

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
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
	stateEventHandlers          *EventHandlers[NodeStateChangeHandler]
	metadataChangeEventHandlers *EventHandlers[NodeMetadataChangeHandler]
	gossipEventHandlers         *EventHandlers[GossipHandler]
	gossipTicker                *time.Ticker
	gossipInterval              time.Duration
	broadcastQItemPool          sync.Pool
}

type broadcastQItem struct {
	packet        *Packet
	transportType TransportType
	excludePeers  []NodeID
	peers         []*Node
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
		localNode:                   newNode(NodeID(u), config.AdvertiseAddr),
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

	// Add the local node to the node list
	cluster.nodes.addOrUpdate(cluster.localNode)

	// Create transport first so we can use it for address resolution
	if config.Transport == nil {
		// For bind address, we need to resolve it to create the transport
		var bindAddresses []Address
		if config.SocketTransportEnabled {
			// Create a temporary transport to resolve bind address
			tempTransport := &transport{config: config}
			bindAddresses, err = tempTransport.ResolveAddress(config.BindAddr)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve bind address: %v", err)
			}
		} else {
			// For WebSocket-only mode, we don't need a bind address with IP/Port
			bindAddresses = []Address{{}} // Empty address for WebSocket-only
		}

		// Check we have a bind port or are using WebSocket-only mode
		if config.SocketTransportEnabled && (len(bindAddresses) == 0 || bindAddresses[0].Port == 0) {
			return nil, fmt.Errorf("no bind port specified for socket transport")
		}

		cluster.transport, err = NewTransport(ctx, &cluster.shutdownWg, config, bindAddresses[0])
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

	c.config.Logger.Infof("gossip: Cluster stopped")
}

// Handler for incoming WebSocket connections when gossiping over web sockets
func (c *Cluster) WebsocketHandler(w http.ResponseWriter, r *http.Request) {
	if c.config.BearerToken != "" {
		authHeader := r.Header.Get("Authorization")
		const bearerPrefix = "Bearer "

		if !strings.HasPrefix(authHeader, bearerPrefix) {
			c.config.Logger.Debugf("gossip: Missing or invalid Authorization header format")
			http.Error(w, "Invalid authorization header", http.StatusUnauthorized)
			return
		}

		bearer := strings.TrimSpace(authHeader[len(bearerPrefix):])
		if bearer != c.config.BearerToken {
			c.config.Logger.Debugf("gossip: Invalid bearer token")
			http.Error(w, "Invalid bearer token", http.StatusUnauthorized)
			return
		}
	}

	c.transport.WebsocketHandler(c.shutdownContext, w, r)
}

func (c *Cluster) Join(peers []string) error {
	if len(peers) == 0 {
		return fmt.Errorf("no peers provided")
	}

	// Shuffle the peers, many nodes may be using the same peer list so shuffling helps to spread the load
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	wg := sync.WaitGroup{}
	wg.Add(len(peers))

	// Join the cluster by attempting to connect to as many peers as possible
	for _, peerAddr := range peers {
		go func(peerAddr string) {
			defer wg.Done()

			c.config.Logger.Tracef("Attempting to join peer: %s", peerAddr)

			// If address matches our advertise address then skip it
			if peerAddr == c.config.AdvertiseAddr {
				return
			}

			c.joinPeer(peerAddr)
		}(peerAddr)
	}

	wg.Wait()

	// Add the list of peers to the health monitor so they can be used for recovery
	for _, peerAddr := range peers {
		c.healthMonitor.addJoinPeer(peerAddr)
	}

	return nil
}

func (c *Cluster) joinPeer(peerAddr string) {
	joinMsg := &joinMessage{
		ID:                 c.localNode.ID,
		AdvertiseAddr:      c.localNode.advertiseAddr,
		MetadataTimestamp:  c.localNode.metadata.GetTimestamp(),
		Metadata:           c.localNode.metadata.GetAll(),
		ProtocolVersion:    PROTOCOL_VERSION,
		ApplicationVersion: c.config.ApplicationVersion,
	}

	addresses, err := c.transport.ResolveAddress(peerAddr)
	if err != nil {
		c.config.Logger.Err(err).Warnf("Failed to resolve address: %s", peerAddr)
		return
	}

	// Create a node which we'll use for connection attempts
	node := newNode(c.localNode.ID, peerAddr)

	for _, addr := range addresses {
		joinReply := &joinReplyMessage{}

		// Apply address to node and attempt to join
		node.address = addr
		err := c.sendToWithResponse(node, nodeJoinMsg, &joinMsg, &joinReply)
		if err != nil {
			continue
		}

		if !joinReply.Accepted {
			c.config.Logger.Field("reason", joinReply.RejectReason).Warnf("gossip: Peer rejected our join request")
			continue
		}

		// Update the node with the peer's information
		node.ID = joinReply.ID
		node.advertiseAddr = joinReply.AdvertiseAddr // Use the address the node advertises
		node.ProtocolVersion = joinReply.ProtocolVersion
		node.ApplicationVersion = joinReply.ApplicationVersion
		node.metadata.update(joinReply.Metadata, joinReply.MetadataTimestamp, true)
		if c.nodes.addOrUpdate(node) {
			c.config.Logger.Debugf("gossip: Joined peer: %s", addr.String())
			err = c.exchangeState(node, []NodeID{c.localNode.ID})
			if err != nil {
				c.config.Logger.Err(err).Warnf("gossip: Failed to exchange state with peer")
			}
			break
		}
	}
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

	// Record activity from the sender
	senderNode := c.nodes.get(packet.SenderID)
	if senderNode != nil {
		senderNode.updateLastActivity()

		// If not marked alive then we need to recover it to alive
		if senderNode.state != NodeAlive {
			c.config.Logger.Tracef("gossip: Recovering node via message: %s", senderNode.ID.String())
			c.nodes.updateState(senderNode.ID, NodeAlive)
		}
	}

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
			c.enqueuePacketForBroadcast(packet.AddRef(), transportType, []NodeID{c.localNode.ID, packet.SenderID}, nil)
		}

		err := h.dispatch(c, senderNode, packet)
		if err != nil {
			c.config.Logger.Err(err).Warnf("gossip: Error dispatching packet: %d", packet.MessageType)
		}
	} else {
		var transportType TransportType
		if packet.conn != nil {
			transportType = TransportReliable
		} else {
			transportType = TransportBestEffort
		}
		c.enqueuePacketForBroadcast(packet, transportType, []NodeID{c.localNode.ID, packet.SenderID}, nil)
	}
}

// CalcFanOut determines how many peers should receive a message
// when broadcasting information throughout the cluster. It implements a
// logarithmic scaling approach to balance network traffic and propagation speed.
func (c *Cluster) CalcFanOut() int {
	totalNodes := float64(c.nodes.getAliveCount() + c.nodes.getSuspectCount())
	if totalNodes <= 0 {
		return 0
	}

	basePeerCount := math.Ceil(math.Log2(totalNodes) * c.config.FanOutMultiplier)
	cap := 10.0

	return int(math.Min(totalNodes, math.Max(1, math.Min(basePeerCount, cap))))
}

// CalcPayloadSize determines how many items should be included
// in a gossip payload. This is used when exchanging state information to limit
// the size of individual gossip messages.
//
// This is meant for controlling the volume of data, not the number of nodes
// to contact.
func (c *Cluster) CalcPayloadSize(totalItems int) int {
	if totalItems <= 0 {
		return 0
	}

	basePeerCount := math.Ceil(math.Log2(float64(totalItems))*c.config.StateExchangeMultiplier) + 2
	cap := 16.0

	return int(math.Min(float64(totalItems), math.Max(1, math.Min(basePeerCount, cap))))
}

// getPeerSubsetSizeIndirectPing The number of peers to use for indirect pings.
func (c *Cluster) getPeerSubsetSizeIndirectPing() int {
	totalNodes := float64(c.nodes.getAliveCount() + c.nodes.getSuspectCount())
	if totalNodes <= 0 {
		return 0
	}

	basePeerCount := math.Ceil(math.Log2(totalNodes) * c.config.IndirectPingMultiplier)
	cap := 6.0

	return int(math.Min(totalNodes, math.Max(1, math.Min(basePeerCount, cap))))
}

func (c *Cluster) getMaxTTL() uint8 {
	totalNodes := c.nodes.getAliveCount()
	if totalNodes <= 0 {
		return 0
	}

	basePeerCount := math.Ceil(math.Log2(float64(totalNodes))*c.config.TTLMultiplier) + 2
	cap := 8.0

	return uint8(math.Max(1, math.Min(basePeerCount, cap)))
}

// Exchange the state of a random subset of nodes with the given node
func (c *Cluster) exchangeState(node *Node, exclude []NodeID) error {

	// Get a random selection of nodes, excluding specified nodes
	randomNodes := c.nodes.getRandomNodesForGossip(
		c.CalcPayloadSize(c.nodes.getAliveCount()+c.nodes.getLeavingCount()+c.nodes.getSuspectCount()+c.nodes.getDeadCount()),
		exclude,
	)

	// Create the state exchange message
	var peerStates []exchangeNodeState
	for _, n := range randomNodes {
		peerStates = append(peerStates, exchangeNodeState{
			ID:                n.ID,
			AdvertiseAddr:     n.advertiseAddr,
			State:             n.state,
			StateChangeTime:   n.stateChangeTime,
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
func (c *Cluster) enqueuePacketForBroadcast(packet *Packet, transportType TransportType, excludePeers []NodeID, peers []*Node) {

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
	item.peers = peers

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
			if item.peers == nil {
				item.peers = c.nodes.getRandomNodes(c.CalcFanOut(), item.excludePeers)
			}

			if err := c.transport.SendPacket(item.transportType, item.peers, item.packet); err != nil {
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
				peerCount := c.CalcFanOut()
				if peerCount == 0 {
					continue
				}

				// Get random subset, excluding ourselves
				peers := c.nodes.getRandomNodes(peerCount, []NodeID{c.localNode.ID})

				// Perform state exchange with selected peers
				for _, peer := range peers {
					go func(p *Node) {
						err := c.exchangeState(p, []NodeID{c.localNode.ID, p.ID})
						if err != nil {
							c.config.Logger.Err(err).Field("peer", p.ID.String()).Tracef("gossip: Periodic state exchange failed")
						} else {
							c.config.Logger.Field("peer", p.ID.String()).Tracef("gossip: Completed periodic state exchange")
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
	return c.nodes.getAliveCount() + c.nodes.getSuspectCount() + c.nodes.getLeavingCount() + c.nodes.getDeadCount()
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

func (c *Cluster) UnregisterMessageType(msgType MessageType) bool {
	if msgType < ReservedMsgsStart {
		return false
	}
	return c.handlers.unregister(msgType)
}

func (c *Cluster) HandleNodeStateChangeFunc(handler NodeStateChangeHandler) HandlerID {
	return c.stateEventHandlers.Add(handler)
}

func (c *Cluster) RemoveNodeStateChangeHandler(id HandlerID) bool {
	return c.stateEventHandlers.Remove(id)
}

func (c *Cluster) HandleNodeMetadataChangeFunc(handler NodeMetadataChangeHandler) HandlerID {
	return c.metadataChangeEventHandlers.Add(handler)
}

func (c *Cluster) RemoveNodeMetadataChangeHandler(id HandlerID) bool {
	return c.metadataChangeEventHandlers.Remove(id)
}

func (c *Cluster) notifyNodeStateChanged(node *Node, prevState NodeState) {
	c.stateEventHandlers.ForEach(func(handler NodeStateChangeHandler) {
		go handler(node, prevState)
	})
}

func (c *Cluster) notifyMetadataChanged(node *Node) {
	c.metadataChangeEventHandlers.ForEach(func(handler NodeMetadataChangeHandler) {
		go handler(node)
	})
}

func (c *Cluster) NodeIsLocal(node *Node) bool {
	return node.ID == c.localNode.ID
}

// Get a random subset of nodes to use for gossiping or exchanging states with, excluding ourselves
func (c *Cluster) GetCandidates() []*Node {
	return c.nodes.getRandomNodes(c.CalcFanOut(), []NodeID{c.localNode.ID})
}

func (c *Cluster) HandleGossipFunc(handler GossipHandler) HandlerID {
	return c.gossipEventHandlers.Add(handler)
}

func (c *Cluster) RemoveGossipHandler(id HandlerID) bool {
	return c.gossipEventHandlers.Remove(id)
}

func (c *Cluster) notifyDoGossip() {
	c.gossipEventHandlers.ForEach(func(handler GossipHandler) {
		handler()
	})
}

func (c *Cluster) gossipManager() {
	go func() {
		// Add jitter to prevent all nodes syncing at the same time
		jitter := time.Duration(rand.Int63n(int64(c.config.GossipInterval / 2)))
		time.Sleep(jitter)

		c.gossipInterval = c.config.GossipInterval
		c.gossipTicker = time.NewTicker(c.config.GossipInterval)

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

func (c *Cluster) NodesToIDs(nodes []*Node) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}
