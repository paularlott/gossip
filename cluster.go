package gossip

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
)

var (
	ErrUnsupportedAddressFormat = fmt.Errorf("unsupported address format")
)

const (
	PROTOCOL_VERSION = 1
)

type Cluster struct {
	config               *Config
	logger               logger.Logger
	shutdownContext      context.Context
	cancelFunc           context.CancelFunc
	shutdownWg           sync.WaitGroup
	msgHistory           *messageHistory
	transport            Transport
	nodes                *nodeList
	localNode            *Node
	handlers             *handlerRegistry
	broadcastQueue       chan *broadcastQItem
	metadataGossipTicker *time.Ticker
	stateGossipTicker    *time.Ticker
	gossipEventHandlers  *EventHandlers[GossipHandler]
	gossipTicker         *time.Ticker
	gossipInterval       time.Duration
	broadcastQItemPool   sync.Pool
	seedPeers            []string
	lastPeerRecovery     time.Time
	peerRecoveryTicker   *time.Ticker
	healthMonitor        *HealthMonitor
	joinQueue            chan *joinRequest
}

type broadcastQItem struct {
	packet        *Packet
	transportType TransportType
	excludePeers  []NodeID
	peers         []*Node
}

type joinRequest struct {
	nodeAddr string
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
		config.Logger = logger.NewNullLogger()
	}

	// Check we have a codec for encoding and decoding messages
	if config.MsgCodec == nil {
		return nil, fmt.Errorf("missing MsgCodec")
	}

	// Check we have a transport
	if config.Transport == nil {
		return nil, fmt.Errorf("missing Transport")
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
		config:              config,
		logger:              config.Logger.WithGroup("gossip"),
		shutdownContext:     ctx,
		cancelFunc:          cancel,
		msgHistory:          newMessageHistory(config),
		localNode:           newNodeWithTags(NodeID(u), config.AdvertiseAddr, config.Tags),
		handlers:            newHandlerRegistry(),
		broadcastQueue:      make(chan *broadcastQItem, config.SendQueueSize),
		gossipEventHandlers: NewEventHandlers[GossipHandler](),
		broadcastQItemPool:  sync.Pool{New: func() interface{} { return new(broadcastQItem) }},
		transport:           config.Transport,
		seedPeers:           make([]string, 0),
		joinQueue:           make(chan *joinRequest, config.JoinQueueSize),
	}

	cluster.nodes = newNodeList(cluster)
	cluster.healthMonitor = newHealthMonitor(cluster)

	cluster.localNode.ProtocolVersion = PROTOCOL_VERSION
	cluster.localNode.ApplicationVersion = config.ApplicationVersion
	cluster.localNode.metadata.update(make(map[string]interface{}, 0), hlc.Now(), true)

	if len(cluster.config.EncryptionKey) == 0 && cluster.config.Cipher != nil {
		return nil, fmt.Errorf("crypter is set but no encryption key is provided")
	}

	// Add the local node to the node list and setup handlers
	cluster.nodes.addOrUpdate(cluster.localNode)
	cluster.localNode.metadata.SetOnLocalChange(func(ts hlc.Timestamp, data map[string]interface{}) {
		// Handle the meta data change locally
		cluster.nodes.notifyMetadataChanged(cluster.localNode)

		// Send to the cluster
		cluster.sendMessage(nil, TransportBestEffort, cluster.getMaxTTL(), metadataUpdateMsg, metadataUpdateMessage{
			MetadataTimestamp: ts,
			Metadata:          data,
			NodeState:         cluster.localNode.observedState,
		})
	})

	cluster.logger.Info("cluster selected transport", "transport", cluster.transport.Name())

	return cluster, nil
}

func (c *Cluster) Start() {
	// Start the transport
	if err := c.transport.Start(c.shutdownContext, &c.shutdownWg); err != nil {
		c.logger.WithError(err).Error("failed to start transport")
		panic("Failed to start cluster")
	}

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

	// Start join workers
	for range c.config.NumJoinWorkers {
		c.shutdownWg.Add(1)
		go c.joinWorker()
	}

	// Register the system message handlers
	c.registerSystemHandlers()

	// Start the gossip runners
	c.metadataGossipManager()
	c.stateGossipManager()
	c.gossipManager()
	c.nodeCleanupManager()
	c.peerRecoveryManager()
	c.healthMonitor.start()

	c.logger.Info("cluster started", "node_id", c.localNode.ID.String())
}

func (c *Cluster) Stop() {
	c.logger.Info("starting cluster shutdown")

	if c.localNode.observedState != NodeLeaving {
		c.Leave()
	}

	if c.msgHistory != nil {
		c.msgHistory.stop()
	}

	if c.cancelFunc != nil {
		c.cancelFunc()
	}

	// Wait for all goroutines to finish
	c.shutdownWg.Wait()

	c.logger.Info("cluster stopped")
}

func (c *Cluster) joinWorker() {
	defer c.shutdownWg.Done()

	for {
		select {
		case req := <-c.joinQueue:
			c.joinPeer(req.nodeAddr)
		case <-c.shutdownContext.Done():
			return
		}
	}
}

func (c *Cluster) Join(peers []string) error {
	joinList := make([]string, 0, len(peers))

	// If http then we have to compare at the domain / port level
	if strings.HasPrefix(c.localNode.advertiseAddr, "http://") || strings.HasPrefix(c.localNode.advertiseAddr, "https://") {
		self, err := url.Parse(c.localNode.advertiseAddr)
		if err != nil {
			return fmt.Errorf("invalid advertise address: %v", err)
		}

		for _, peerAddr := range peers {
			peer, err := url.Parse(peerAddr)
			if err != nil {
				return fmt.Errorf("invalid peer address: %v", err)
			}

			if self.Host == peer.Host && self.Port() == peer.Port() {
				continue
			}

			joinList = append(joinList, peerAddr)
		}
	} else {
		// Look through the list of peers and skip our own address
		for _, peerAddr := range peers {
			if peerAddr == c.config.AdvertiseAddr {
				continue
			}
			joinList = append(joinList, peerAddr)
		}
	}

	if len(joinList) == 0 {
		return fmt.Errorf("no peers provided")
	}

	// Shuffle the peers, many nodes may be using the same peer list so shuffling helps to spread the load
	rand.Shuffle(len(joinList), func(i, j int) {
		joinList[i], joinList[j] = joinList[j], joinList[i]
	})

	wg := sync.WaitGroup{}
	wg.Add(len(joinList))

	// Join the cluster by attempting to connect to as many peers as possible
	for _, peerAddr := range joinList {
		go func(peerAddr string) {
			defer wg.Done()

			c.logger.Trace("attempting to join peer", "address", peerAddr)
			c.joinPeer(peerAddr)
		}(peerAddr)
	}

	wg.Wait()

	// Remember the peers so we can use them for recovery
	c.seedPeers = append(c.seedPeers, joinList...)

	return nil
}

func (c *Cluster) joinPeer(peerAddr string) {
	joinMsg := &joinMessage{
		ID:                 c.localNode.ID,
		AdvertiseAddr:      c.localNode.advertiseAddr,
		State:              c.localNode.observedState,
		Tags:               c.localNode.GetTags(),
		MetadataTimestamp:  c.localNode.metadata.GetTimestamp(),
		Metadata:           c.localNode.metadata.GetAll(),
		ProtocolVersion:    PROTOCOL_VERSION,
		ApplicationVersion: c.config.ApplicationVersion,
	}

	// Create a node for the peer
	node := newNode(EmptyNodeID, peerAddr)

	// Attempt to join the peer
	joinReply := &joinReplyMessage{}
	err := c.sendToWithResponse(node, nodeJoinMsg, &joinMsg, &joinReply)
	if err != nil {
		c.logger.Warn("failed to join peer", "error", err, "address", peerAddr)
		return
	}

	if !joinReply.Accepted {
		c.logger.Warn("peer rejected join request", "address", peerAddr, "reason", joinReply.RejectReason)
		return
	}

	c.logger.Debug("received join reply", "peer_id", joinReply.NodeID.String(), "peer_tags", joinReply.Tags)

	// Create node with tags from join reply
	node = newNodeWithTags(joinReply.NodeID, joinReply.AdvertiseAddr, joinReply.Tags)
	node = c.nodes.addIfNotExists(node)

	// Update the copy of the nodes metadata
	node.metadata.update(joinReply.Metadata, joinReply.MetadataTimestamp, false)

	// Run the list of nodes that we've been given and attempt to join with any we don't know
	for _, peer := range joinReply.Nodes {
		if existing := c.nodes.get(peer.ID); existing == nil {
			c.logger.Trace("joining unknown peer", "peer_id", peer.ID.String(), "address", peer.AdvertiseAddr)

			// Create a preliminary node with tags from the join reply
			prelimNode := newNodeWithTags(peer.ID, peer.AdvertiseAddr, peer.Tags)
			c.nodes.addIfNotExists(prelimNode)

			req := &joinRequest{
				nodeAddr: peer.AdvertiseAddr,
			}

			select {
			case c.joinQueue <- req:
			default:
				c.logger.Warn("join queue full, dropping join request for node", "peer_id", peer.ID.String(), "address", peer.AdvertiseAddr)
			}
		}
	}
}

func (c *Cluster) peerRecoveryManager() {
	go func() {
		// Initial delay with jitter
		jitter := time.Duration(rand.Int63n(int64(c.config.PeerRecoveryInterval / 4)))
		time.Sleep(jitter)

		c.peerRecoveryTicker = time.NewTicker(c.config.PeerRecoveryInterval)
		defer c.peerRecoveryTicker.Stop()

		for {
			select {
			case <-c.peerRecoveryTicker.C:
				c.checkPeerConnectivity()
			case <-c.shutdownContext.Done():
				return
			}
		}
	}()
}

func (c *Cluster) checkPeerConnectivity() {
	// Skip if no seed peers configured
	if len(c.seedPeers) == 0 {
		return
	}

	// Skip if we recently attempted recovery (prevent spam)
	if time.Since(c.lastPeerRecovery) < c.config.PeerRecoveryInterval/2 {
		return
	}

	aliveCount := c.nodes.getAliveCount()
	seedPeerCount := len(c.seedPeers)

	// Calculate the threshold (50% of seed peers)
	threshold := seedPeerCount / 2
	if seedPeerCount%2 == 1 {
		threshold++ // Round up for odd numbers
	}

	// Trigger recovery if alive nodes <= 50% of seed peers
	if aliveCount <= threshold {
		reason := fmt.Sprintf("alive nodes (%d) <= 50%% of seed peers (%d/%d)", aliveCount, threshold, seedPeerCount)
		c.logger.Warn("triggering peer recovery", "reason", reason)
		c.lastPeerRecovery = time.Now()

		for _, peer := range c.seedPeers {
			req := &joinRequest{
				nodeAddr: peer,
			}

			select {
			case c.joinQueue <- req:
			default:
				c.logger.Warn("join queue full, dropping join request for node", "address", peer)
			}
		}
	} else {
		c.logger.Trace("cluster health good", "alive_nodes", aliveCount, "threshold", threshold, "seed_peers", seedPeerCount)
	}
}

// Marks the local node as leaving and broadcasts this state to the cluster
func (c *Cluster) Leave() {
	c.logger.Debug("local node is leaving the cluster")

	// Update our local node state to leaving
	c.nodes.updateState(c.localNode.ID, NodeLeaving)

	// Broadcast the leaving message
	c.sendMessage(nil, TransportBestEffort, c.getMaxTTL(), nodeLeaveMsg, nil)

	// Give some time for the leave messages to be sent before shutting down
	time.Sleep(100 * time.Millisecond)

	c.logger.Debug("leave message broadcast completed")
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

	// If message has a target node ID and it's not us, ignore it
	if packet.MessageType != nodeJoinMsg && packet.TargetNodeID != nil && *packet.TargetNodeID != c.localNode.ID {
		packet.Release()
		return
	}

	// If message has a tag and we don't have that tag, ignore it (but still forward)
	hasTag := packet.Tag == nil || c.localNode.HasTag(*packet.Tag)

	// Record the message in the message history
	c.msgHistory.recordMessage(packet.SenderID, packet.MessageID)

	// Record activity from the sender
	senderNode := c.nodes.get(packet.SenderID)
	if senderNode != nil {
		senderNode.updateLastActivity()
	}

	// Forward packets to nodes with matching tags
	if !packet.CanReply() {
		var transportType TransportType
		if packet.conn != nil {
			transportType = TransportReliable
		} else {
			transportType = TransportBestEffort
		}
		c.enqueuePacketForBroadcast(packet.AddRef(), transportType, []NodeID{c.localNode.ID, packet.SenderID}, nil)
	}

	// If we don't have the tag, skip processing
	if !hasTag {
		packet.Release()
		return
	}

	// If we don't know the sender then unless it's a join message ignore it
	if senderNode == nil && packet.MessageType != nodeJoinMsg && packet.MessageType != pingMsg {
		packet.Release()
		return
	}

	// Run the message handler
	h := c.handlers.getHandler(packet.MessageType)
	if h != nil {
		err := h.dispatch(c, senderNode, packet)
		if err != nil {
			c.logger.Warn("error dispatching packet", "error", err, "message_type", packet.MessageType)
		}
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

	return int(math.Min(totalNodes, math.Max(3, math.Min(basePeerCount, cap))))
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

	basePayloadSize := math.Ceil(math.Log2(float64(totalItems))*c.config.StateExchangeMultiplier) + 2
	cap := 16.0

	return int(math.Min(float64(totalItems), math.Max(3, math.Min(basePayloadSize, cap))))
}

// getMaxTTL calculates the maximum Time-To-Live (hop count) for gossip messages
// based on the current cluster size. TTL determines how many times a message
// can be forwarded before being dropped, preventing infinite message loops
// while ensuring adequate propagation coverage.
//
// The TTL is calculated using logarithmic scaling to balance message reach
// with network overhead:
// - Small clusters (2-4 nodes): TTL of 3-4 hops
// - Medium clusters (16-32 nodes): TTL of 5-6 hops
// - Large clusters (100+ nodes): TTL of 7-8 hops (capped)
//
// Returns 0 if no alive nodes exist, ensuring messages don't propagate
// in empty clusters.
func (c *Cluster) getMaxTTL() uint8 {
	totalNodes := c.nodes.getAliveCount()
	if totalNodes <= 0 {
		return 0
	}

	basePeerCount := math.Ceil(math.Log2(float64(totalNodes))*c.config.TTLMultiplier) + 2
	cap := 8.0

	return uint8(math.Max(1, math.Min(basePeerCount, cap)))
}

// Exchange the state of a random subset of nodes with the given nodes
func (c *Cluster) exchangeState(nodes []*Node, exclude []NodeID) {

	// Get a random selection of nodes, excluding specified nodes
	randomNodes := c.nodes.getRandomNodesInStates(
		c.CalcPayloadSize(c.nodes.getAliveCount()+c.nodes.getLeavingCount()+c.nodes.getSuspectCount()),
		[]NodeState{NodeAlive, NodeSuspect, NodeLeaving},
		exclude,
	)

	// No nodes to exchange, this is fine
	if len(randomNodes) == 0 {
		return
	}

	// Create the state exchange message
	var peerStates []exchangeNodeState
	for _, n := range randomNodes {
		peerStates = append(peerStates, exchangeNodeState{
			ID:             n.ID,
			AdvertiseAddr:  n.advertiseAddr,
			State:          n.observedState,
			StateTimestamp: n.observedStateTime,
		})
	}

	for _, node := range nodes {
		var peerResponseStates []exchangeNodeState

		// Exchange state with the peer
		err := c.sendToWithResponse(
			node,
			pushPullStateMsg,
			&peerStates,
			&peerResponseStates,
		)
		if err != nil {
			c.logger.Warn("failed to exchange state with node", "node_id", node.ID.String())
			c.nodes.updateState(node.ID, NodeSuspect)
		} else {
			if node.observedState == NodeSuspect {
				c.nodes.updateState(node.ID, NodeAlive)
				c.logger.Warn("node recovered from suspect", "node_id", node.ID.String())
			}
			node.updateLastActivity()

			c.combineStates(peerResponseStates)
		}
	}
}

// Combine the received states with our own to form a complete view of the cluster state
func (c *Cluster) combineStates(remoteStates []exchangeNodeState) {
	for _, state := range remoteStates {
		// Skip us
		if state.ID == c.localNode.ID {
			continue
		}

		localNode := c.nodes.get(state.ID)
		if localNode == nil {
			// Skip dead or leaving nodes when creating new nodes
			if state.State == NodeDead || state.State == NodeLeaving {
				continue
			}

			req := &joinRequest{
				nodeAddr: state.AdvertiseAddr,
			}

			select {
			case c.joinQueue <- req:
			default:
				c.logger.Warn("join queue full, dropping join request for node", "peer_id", state.ID.String(), "address", state.AdvertiseAddr)
			}
		} else {
			// Node exists locally, need to merge states intelligently
			c.logger.Trace("merging state for existing node", "node_id", state.ID.String())

			// Update advertise address if it has changed (but log it)
			if localNode.advertiseAddr != state.AdvertiseAddr {
				c.logger.Debug("node advertise address changed", "node_id", state.ID.String(), "old_address", localNode.advertiseAddr, "new_address", state.AdvertiseAddr)
				localNode.advertiseAddr = state.AdvertiseAddr
				localNode.address.Clear() // Force re-resolution
			}

			// If the remote state timestamp is newer then we need to consider what's being reported
			if state.StateTimestamp.After(localNode.observedStateTime) && state.State != localNode.observedState {
				if state.State == NodeLeaving {
					c.nodes.updateState(localNode.ID, NodeLeaving)
				}
			}
		}
	}
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
		c.logger.Warn("broadcast queue is full, skipping message")

		item.packet.Release()
		item.packet = nil
		c.broadcastQItemPool.Put(item)
	}
}

func (c *Cluster) broadcastWorker() {
	defer c.shutdownWg.Done()

	for {
		select {
		case item := <-c.broadcastQueue:

			// Get the peer subset to send the packet to
			if item.peers == nil {
				if item.packet.Tag != nil {
					// For tagged messages, only send to nodes with that tag
					item.peers = c.nodes.getRandomNodesWithTag(c.CalcFanOut(), *item.packet.Tag, item.excludePeers)
				} else {
					// For untagged messages, send to all nodes
					item.peers = c.nodes.getRandomNodes(c.CalcFanOut(), item.excludePeers)
				}
			}

			for _, node := range item.peers {
				if err := c.transport.Send(item.transportType, node, item.packet); err != nil {
					c.logger.Debug("failed to send packet to peers", "error", err)
				}
			} // Release the broadcast item back to the pool
			item.packet.Release()
			item.packet = nil
			c.broadcastQItemPool.Put(item)

		case <-c.shutdownContext.Done():
			return
		}
	}
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

func (c *Cluster) GetNodesByTag(tag string) []*Node {
	return c.nodes.getByTag(tag)
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
	c.handlers.registerHandler(msgType, handler)
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

func (c *Cluster) HandleFuncWithResponse(msgType MessageType, replyHandler ReplyHandler) error {
	return c.HandleFuncWithReply(msgType, replyHandler)
}

func (c *Cluster) UnregisterMessageType(msgType MessageType) bool {
	if msgType < ReservedMsgsStart {
		return false
	}
	return c.handlers.unregister(msgType)
}

func (c *Cluster) HandleNodeStateChangeFunc(handler NodeStateChangeHandler) HandlerID {
	return c.nodes.OnNodeStateChange(handler)
}

func (c *Cluster) RemoveNodeStateChangeHandler(id HandlerID) bool {
	return c.nodes.RemoveStateChangeHandler(id)
}

func (c *Cluster) HandleNodeMetadataChangeFunc(handler NodeMetadataChangeHandler) HandlerID {
	return c.nodes.OnNodeMetadataChange(handler)
}

func (c *Cluster) RemoveNodeMetadataChangeHandler(id HandlerID) bool {
	return c.nodes.RemoveMetadataChangeHandler(id)
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

func (c *Cluster) metadataGossipManager() {
	go func() {
		// Small jitter
		jitter := time.Duration(rand.Int63n(int64(c.config.MetadataGossipInterval / 4)))
		time.Sleep(jitter)

		ticker := time.NewTicker(c.config.MetadataGossipInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.gossipMetadata()
			case <-c.shutdownContext.Done():
				return
			}
		}
	}()
}

func (c *Cluster) stateGossipManager() {
	go func() {
		// Larger jitter for state sync
		jitter := time.Duration(rand.Int63n(int64(c.config.StateGossipInterval / 2)))
		time.Sleep(jitter)

		ticker := time.NewTicker(c.config.StateGossipInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.exchangeState(c.GetCandidates(), []NodeID{c.localNode.ID})
			case <-c.shutdownContext.Done():
				return
			}
		}
	}()
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
				c.gossipEventHandlers.ForEach(func(handler GossipHandler) {
					handler()
				})
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

		c.logger.Debug("interval increased", "duration", duration.String(), "new_interval", c.gossipInterval.String())
	} else if c.gossipInterval > c.config.GossipInterval && duration < time.Duration(float64(c.gossipInterval)*0.8) {
		// If handlers are fast and we're above original interval, gradually return to base

		// Move halfway back toward original interval
		c.gossipInterval = (duration + c.gossipInterval) / 2

		// Never go below original interval
		if c.gossipInterval < c.config.GossipInterval {
			c.gossipInterval = c.config.GossipInterval
		}

		c.gossipTicker.Reset(c.gossipInterval)
		c.logger.Debug("interval decreased", "duration", duration.String(), "new_interval", c.gossipInterval.String())
	}
}

func (c *Cluster) gossipMetadata() {
	// Only send periodic metadata if we haven't sent any metadata updates recently
	// This avoids redundant traffic when metadata is actively changing
	lastUpdate := c.localNode.metadata.GetTimestamp().Time()
	if time.Since(lastUpdate) <= c.config.MetadataGossipInterval/2 {
		return
	}

	// Send lightweight metadata updates to a small subset of nodes
	candidates := c.nodes.getRandomNodes(3, []NodeID{c.localNode.ID})
	c.sendMessage(
		candidates,
		TransportBestEffort,
		c.getMaxTTL(),
		metadataUpdateMsg,
		metadataUpdateMessage{
			MetadataTimestamp: c.localNode.metadata.GetTimestamp(),
			Metadata:          c.localNode.metadata.GetAll(),
			NodeState:         c.localNode.GetObservedState(),
		},
	)
}

func (c *Cluster) Logger() logger.Logger {
	return c.logger
}

func (c *Cluster) NodesToIDs(nodes []*Node) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func (c *Cluster) nodeCleanupManager() {
	go func() {
		// Add jitter to prevent all nodes cleaning up at the same time
		jitter := time.Duration(rand.Int63n(int64(c.config.NodeCleanupInterval / 4)))
		time.Sleep(jitter)

		ticker := time.NewTicker(c.config.NodeCleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.cleanupNodes()
			case <-c.shutdownContext.Done():
				return
			}
		}
	}()
}

func (c *Cluster) cleanupNodes() {
	now := time.Now()

	// Get all leaving and dead nodes
	leavingNodes := c.nodes.getAllInStates([]NodeState{NodeLeaving})
	deadNodes := c.nodes.getAllInStates([]NodeState{NodeDead})

	// Move old leaving nodes to dead
	for _, node := range leavingNodes {
		if now.Sub(node.observedStateTime.Time()) > c.config.LeavingNodeTimeout {
			c.nodes.updateState(node.ID, NodeDead)
			c.logger.Debug("moved leaving node to dead", "node_id", node.ID.String())
		}
	}

	// Remove old dead nodes
	for _, node := range deadNodes {
		if now.Sub(node.observedStateTime.Time()) > c.config.NodeRetentionTime {
			c.nodes.remove(node.ID)
			c.logger.Debug("removed dead node", "node_id", node.ID.String())
		}
	}
}
