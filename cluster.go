package gossip

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type Cluster struct {
	config          *Config
	shutdownContext context.Context
	cancelFunc      context.CancelFunc
	msgHistory      *messageHistory
	transport       *transport
	nodes           *nodeList
	localNode       *Node
	handlers        *handlerRegistry
	broadcastQueue  chan *broadcastQItem
	healthMonitor   *healthMonitor
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
		localNode:       newNode(NodeID(u), config.AdvertiseAddr),
		handlers:        newHandlerRegistry(),
		broadcastQueue:  make(chan *broadcastQItem, config.SendQueueSize),
	}

	// Add the local node to the node list
	cluster.nodes.addOrUpdate(cluster.localNode)

	cluster.transport, err = newTransport(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %v", err)
	}

	// Start the workers
	for range config.NumSendWorkers {
		go cluster.broadcastWorker()
	}
	go cluster.acceptPackets()

	// Start the health monitor
	cluster.healthMonitor = newHealthMonitor(cluster)

	// Register the system message handlers
	cluster.registerSystemHandlers()

	// Trigger the event listener
	if config.EventListener != nil {
		config.EventListener.OnInit(cluster)
	}

	// Start periodic state synchronization
	cluster.startStateSync()

	log.Info().Msgf("Cluster created with Node ID: %s", u.String())

	return cluster, nil
}

func (c *Cluster) Stop() {
	if c.localNode.state != nodeLeaving {
		c.Leave()
	}

	if c.healthMonitor != nil {
		c.healthMonitor.stop()
	}

	if c.msgHistory != nil {
		c.msgHistory.stop()
	}

	if c.transport != nil {
		c.transport.stop()
	}

	if c.cancelFunc != nil {
		c.cancelFunc()
	}

	log.Info().Msg("Cluster stopped")
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
		log.Debug().Msgf("Attempting to join peer: %s", peerAddr)

		joinMsg := &joinMessage{
			ID:             c.localNode.ID,
			AdvertisedAddr: c.localNode.advertisedAddr,
		}

		node := newNode(c.localNode.ID, peerAddr)
		err := c.transport.sendMessageWithResponse(node, c.localNode.ID, nodeJoinMsg, &joinMsg, nodeJoinAckMsg, &joinMsg)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to join peer %s", peerAddr)
			continue
		}

		// Update the node with the peer's advertised address and ID then save it
		node.ID = joinMsg.ID
		node.advertisedAddr = joinMsg.AdvertisedAddr
		if c.nodes.addIfNotExists(node) {
			err = c.exchangeState(node, []NodeID{c.localNode.ID})
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to exchange state with peer %s", peerAddr)
			}
		}

		log.Info().Msgf("Joined peer: %s", peerAddr)
	}

	return nil
}

// MMarks the local node as leaving and broadcasts this state to the cluster
func (c *Cluster) Leave() {
	log.Info().Msg("Local node is leaving the cluster")
	c.healthMonitor.MarkNodeLeaving(c.localNode)
}

func (c *Cluster) acceptPackets() {
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

func (c *Cluster) handleIncomingPacket(incomingPacket *incomingPacket) {
	if incomingPacket.conn != nil {
		defer incomingPacket.conn.Close()
	}

	// If the sender is us or already seen then ignore the message
	packet := incomingPacket.packet
	if packet.SenderID == c.localNode.ID || c.msgHistory.contains(packet.SenderID, packet.MessageID) {
		return
	}

	// Record the message in the message history
	c.msgHistory.recordMessage(packet.SenderID, packet.MessageID)

	// Run the message handler
	h := c.handlers.getHandler(packet.MessageType)
	if h != nil {
		if h.forward {
			var transportType TransportType
			if incomingPacket.conn != nil {
				transportType = TransportReliable
			} else {
				transportType = TransportBestEffort
			}
			c.enqueuePacketForBroadcast(packet, transportType, []NodeID{c.localNode.ID, packet.SenderID})
		}

		senderNode := c.nodes.get(packet.SenderID)
		if senderNode != nil {
			senderNode.updateLastActivity()
		}

		err := h.dispatch(incomingPacket.conn, c.localNode, c.transport, senderNode, packet)
		if err != nil {
			log.Warn().Err(err).Msgf("Error dispatching packet: %d", packet.MessageType)
		}
	} else {
		log.Warn().Msgf("No handler registered for message type: %d", packet.MessageType)
	}
}

func (c *Cluster) GetLocalNode() *Node {
	return c.localNode
}

func (c *Cluster) GetAllNodes() []*Node {
	return c.nodes.getAll()
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
			ID:              n.ID,
			AdvertisedAddr:  n.advertisedAddr,
			State:           n.state,
			StateChangeTime: n.stateChangeTime.UnixNano(),
		})
	}

	// No nodes to exchange, this is fine
	if len(peerStates) == 0 {
		return nil
	}

	// Exchange state with the peer
	err := c.transport.sendMessageWithResponse(
		node,
		c.localNode.ID,
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
func (cluster *Cluster) enqueuePacketForBroadcast(packet *Packet, transportType TransportType, excludePeers []NodeID) {

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
	case cluster.broadcastQueue <- item:
		// Successfully queued
	default:
		// Queue full, log and skip this message
		log.Error().
			Msg("Broadcast queue is full, skipping message")
	}
}

func (c *Cluster) broadcastWorker() {
	for {
		select {
		case item := <-c.broadcastQueue:

			// Get the peer subset to send the packet to
			peerSubset := c.nodes.getRandomLiveNodes(c.getPeerSubsetSize(c.nodes.getLiveCount(), purposeBroadcast), item.excludePeers)
			for _, peer := range peerSubset {
				if err := c.transport.sendPacket(item.transportType, peer, item.packet); err != nil {
					log.Warn().Err(err).Msgf("Failed to send packet to peer %s", peer.ID)
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
							log.Debug().Err(err).Str("peer", p.ID.String()).
								Msg("Periodic state exchange failed")
						} else {
							log.Trace().Str("peer", p.ID.String()).
								Msg("Completed periodic state exchange")
						}
					}(peer)
				}

			case <-c.shutdownContext.Done():
				return
			}
		}
	}()
}

func (c *Cluster) HandleFunc(msgType MessageType, forward bool, handler Handler) {
	c.handlers.registerHandler(msgType, forward, handler)
}

func (c *Cluster) HandleFuncWithReply(msgType MessageType, replyHandler ReplyHandler) {
	c.handlers.registerHandlerWithReply(msgType, replyHandler)
}

func (c *Cluster) SendMessage(transport TransportType, msgType MessageType, data interface{}, excludePeers []NodeID) error {
	packet, err := c.transport.createPacket(c.localNode.ID, msgType, uint8(c.getPeerSubsetSize(c.nodes.getLiveCount(), purposeTTL)), data)
	if err != nil {
		return err
	}

	c.enqueuePacketForBroadcast(packet, transport, []NodeID{c.localNode.ID})
	return nil
}

func (c *Cluster) SendMessageWithResponse(dstNode *Node, msgType MessageType, payload interface{}, responseMsgType MessageType, responsePayload interface{}) error {
	return c.transport.sendMessageWithResponse(
		dstNode,
		c.localNode.ID,
		msgType,
		&payload,
		responseMsgType,
		&responsePayload,
	)
}
