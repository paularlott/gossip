package gossip

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
)

var (
	ErrUnsupportedAddressFormat = fmt.Errorf("unsupported address format")
)

const (
	PROTOCOL_VERSION = 1
)

type Cluster struct {
	config              *Config
	shutdownContext     context.Context
	cancelFunc          context.CancelFunc
	shutdownWg          sync.WaitGroup
	msgHistory          *messageHistory
	transport           Transport
	nodes               *nodeList
	localNode           *Node
	handlers            *handlerRegistry
	broadcastQueue      chan *broadcastQItem
	gossipEventHandlers *EventHandlers[GossipHandler]
	gossipTicker        *time.Ticker
	gossipInterval      time.Duration
	broadcastQItemPool  sync.Pool
	peerList            []string
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
		shutdownContext:     ctx,
		cancelFunc:          cancel,
		msgHistory:          newMessageHistory(config),
		localNode:           newNode(NodeID(u), config.AdvertiseAddr),
		handlers:            newHandlerRegistry(),
		broadcastQueue:      make(chan *broadcastQItem, config.SendQueueSize),
		gossipEventHandlers: NewEventHandlers[GossipHandler](),
		broadcastQItemPool:  sync.Pool{New: func() interface{} { return new(broadcastQItem) }},
		transport:           config.Transport,
		peerList:            make([]string, 0),
	}

	cluster.nodes = newNodeList(cluster)

	cluster.localNode.ProtocolVersion = PROTOCOL_VERSION
	cluster.localNode.ApplicationVersion = config.ApplicationVersion

	if len(cluster.config.EncryptionKey) == 0 && cluster.config.Cipher != nil {
		return nil, fmt.Errorf("crypter is set but no encryption key is provided")
	}

	// Add the local node to the node list and setup handlers
	cluster.nodes.addOrUpdate(cluster.localNode)
	cluster.localNode.metadata.SetOnLocalChange(func(ts hlc.Timestamp, data map[string]interface{}) {
		// Handle the meta data change locally
		cluster.nodes.notifyMetadataChanged(cluster.localNode)

		// Send to the cluster
		cluster.sendMessage(nil, TransportBestEffort, cluster.getMaxTTL(), metadataUpdateMsg, metadataUpdateMessage{MetadataTimestamp: ts, Metadata: data})
	})

	config.Logger.Field("transport", cluster.transport.Name()).Infof("gossip: Cluster selected transport")

	return cluster, nil
}

func (c *Cluster) Start() {
	// Start the transport
	if err := c.transport.Start(c.shutdownContext, &c.shutdownWg); err != nil {
		c.config.Logger.Err(err).Errorf("Failed to start transport")
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

	// Register the system message handlers
	c.registerSystemHandlers()

	// State sync
	c.HandleGossipFunc(c.periodicStateSync)

	// Start the gossip notification manager
	c.gossipManager()

	c.config.Logger.Infof("gossip: Cluster started, local node ID: %s", c.localNode.ID.String())
}

func (c *Cluster) Stop() {
	c.config.Logger.Infof("gossip: Starting cluster shutdown")

	if c.localNode.state != NodeLeaving {
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

	c.config.Logger.Infof("gossip: Cluster stopped")
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

	// Remember the peers so we can use them for recovery
	c.peerList = append(c.peerList, peers...)

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

	// Create a node for the peer
	node := newNode(c.localNode.ID, peerAddr)
	joinReply := &joinReplyMessage{}

	// Attempt to join the peer
	err := c.sendToWithResponse(node, nodeJoinMsg, &joinMsg, &joinReply)
	if err != nil {
		c.config.Logger.Err(err).Warnf("Failed to join peer: %s", peerAddr)
		return
	}

	if !joinReply.Accepted {
		c.config.Logger.Field("reason", joinReply.RejectReason).Warnf("gossip: Peer rejected our join request")
		return
	}

	// Update the node with the peer's information
	node.ID = joinReply.ID
	node.advertiseAddr = joinReply.AdvertiseAddr
	node.ProtocolVersion = joinReply.ProtocolVersion
	node.ApplicationVersion = joinReply.ApplicationVersion
	node.metadata.update(joinReply.Metadata, joinReply.MetadataTimestamp, true)
	if c.nodes.addOrUpdate(node) {
		c.config.Logger.Debugf("gossip: Joined peer: %s", peerAddr)
		err = c.exchangeState([]*Node{node}, []NodeID{c.localNode.ID})
		if err != nil {
			c.config.Logger.Err(err).Warnf("gossip: Failed to exchange state with peer")
		}
	}
}

// Marks the local node as leaving and broadcasts this state to the cluster
func (c *Cluster) Leave() {
	c.config.Logger.Debugf("gossip: Local node is leaving the cluster")

	// Update our local node state to leaving
	c.nodes.updateState(c.localNode.ID, NodeLeaving)

	// Create a leaving message to broadcast to all peers
	leaveMsg := &leaveMessage{
		ID: c.localNode.ID,
	}

	// Broadcast the leaving message
	c.sendMessage(nil, TransportBestEffort, c.getMaxTTL(), nodeLeaveMsg, leaveMsg)

	// Give some time for the leave messages to be sent before shutting down
	time.Sleep(100 * time.Millisecond)

	c.config.Logger.Debugf("gossip: Leave message broadcast completed")
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

	// Forward packets
	if !packet.CanReply() {
		var transportType TransportType
		if packet.conn != nil {
			transportType = TransportReliable
		} else {
			transportType = TransportBestEffort
		}
		c.enqueuePacketForBroadcast(packet.AddRef(), transportType, []NodeID{c.localNode.ID, packet.SenderID}, nil)
	}

	// If we don't know the sender then unless it's a join message ignore it
	if senderNode == nil && packet.MessageType != nodeJoinMsg {
		packet.Release()
		return
	}

	// Run the message handler
	h := c.handlers.getHandler(packet.MessageType)
	if h != nil {
		err := h.dispatch(c, senderNode, packet)
		if err != nil {
			c.config.Logger.Err(err).Warnf("gossip: Error dispatching packet: %d", packet.MessageType)
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
func (c *Cluster) exchangeState(nodes []*Node, exclude []NodeID) error {

	// Get a random selection of nodes, excluding specified nodes
	randomNodes := c.nodes.getRandomNodesInStates(
		c.CalcPayloadSize(c.nodes.getAliveCount()+c.nodes.getLeavingCount()+c.nodes.getSuspectCount()+c.nodes.getDeadCount()),
		[]NodeState{NodeAlive, NodeSuspect, NodeSuspect, NodeDead},
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
			if len(nodes) == 1 {
				return err
			}
			continue // multiple nodes so skip failed
		}

		c.combineStates(peerResponseStates)
	}

	return nil
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
			// Skip dead or leaving nodes
			if state.State == NodeDead || state.State == NodeLeaving {
				continue
			}

			localNode = newNode(state.ID, state.AdvertiseAddr)
			localNode.state = state.State
			localNode.stateChangeTime = state.StateChangeTime
			localNode.metadata.update(state.Metadata, state.MetadataTimestamp, true)
			c.nodes.add(localNode, true)
		} else {
			if state.State != localNode.state {

				// TODO Be smarter about accepting states

				c.nodes.updateState(localNode.ID, state.State)
			}
			localNode.metadata.update(state.Metadata, state.MetadataTimestamp, false)
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

			for _, node := range item.peers {
				if err := c.transport.Send(item.transportType, node, item.packet); err != nil {
					c.config.Logger.Err(err).Debugf("gossip:Failed to send packet to peers")
				}
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
		c.exchangeState(c.GetCandidates(), []NodeID{c.localNode.ID})
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

func (c *Cluster) NodesToIDs(nodes []*Node) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}
