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
		config = defaultConfig()
	} else {
		config.MergeDefault()
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
		nodes:           newNodeList(config.NodeShardCount),
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
	for i := 0; i < config.SendWorkers; i++ {
		go cluster.broadcastWorker()
	}
	go cluster.acceptPackets()

	// Start the health monitor
	cluster.healthMonitor = newHealthMonitor(cluster)

	// Register the system message handlers
	cluster.registerSystemHandlers()

	log.Info().Msgf("Cluster created with Node ID: %s", u.String())

	return cluster, nil
}

func (c *Cluster) Stop() {
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

			// TODO implement node state exchange
			log.Debug().Msg("do a state exchange here")

			err = c.exchangeState(node)
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to exchange state with peer %s", peerAddr)
			}

			// TODO testing ping
			alive, err := c.healthMonitor.pingNode(node)
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to ping peer %s", peerAddr)
			}
			if alive {
				log.Debug().Msgf("Ping to peer %s was successful", peerAddr)
			} else {
				log.Warn().Msgf("Ping to peer %s failed", peerAddr)
			}

		}

		log.Info().Msgf("Joined peer: %s", peerAddr)
	}

	return nil
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
			log.Debug().Msgf("Forwarding message: %d from %s", packet.MessageType, packet.SenderID)

			var transportType TransportType
			if incomingPacket.conn != nil {
				transportType = TransportReliable
			} else {
				transportType = TransportBestEffort
			}
			c.enqueuePacketForBroadcast(packet, transportType, []NodeID{packet.SenderID})
		}

		err := h.dispatch(incomingPacket.conn, c.nodes.get(packet.SenderID), packet)
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

// Get the number of peers to use for a subset
func (c *Cluster) getPeerSubsetSize(k int, multiplier float64) int {
	if k <= 0 {
		return 0
	}
	return int(math.Ceil(math.Log2(float64(k)) * multiplier))
}

func (c *Cluster) getMaxTTL() uint8 {
	ttl := c.getPeerSubsetSize(c.nodes.getLiveCount(), c.config.TTLMultiplier)
	return uint8(math.Max(1, math.Min(float64(ttl), 10)))
}

// Exchange the state of a random subset of nodes with the given node
func (c *Cluster) exchangeState(node *Node) error {
	// TODO implement state exchange
	log.Debug().Msg("State exchange not implemented yet")

	nodes := c.nodes.getRandomNodes(10, []NodeID{c.localNode.ID})
	fmt.Println("Random nodes:", nodes)
	fmt.Println("to get ", c.nodes.getLiveCount(), c.getPeerSubsetSize(c.nodes.getLiveCount(), c.config.StatePushPullMultiplier), "nodes")

	var peerStates []pushPullState
	for _, n := range nodes {
		peerStates = append(peerStates, pushPullState{
			ID:              n.ID,
			AdvertisedAddr:  n.advertisedAddr,
			State:           n.state,
			LastStateUpdate: time.Now().UnixNano(), // TODO this needs to be from the state
		})
	}

	err := c.transport.sendMessageWithResponse(node, c.localNode.ID, pushPullStateMsg, &peerStates, pushPullStateAckMsg, &peerStates)
	if err != nil {
		return err
	}

	fmt.Println("Received push pull state from", node.ID, "with", len(peerStates), "states")

	// TODO implement state merge
	// if node is suspect then add a vote from the peer we're exchanging with

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

	cluster.broadcastQueue <- item
}

func (c *Cluster) broadcastWorker() {
	for {
		select {
		case item := <-c.broadcastQueue:

			// Get the peer subset to send the packet to
			peerSubset := c.nodes.getRandomLiveNodes(c.getPeerSubsetSize(c.nodes.getLiveCount(), 1), item.excludePeers)
			for _, peer := range peerSubset {
				if err := c.transport.sendPacket(item.transportType, peer, item.packet); err != nil {
					log.Warn().Err(err).Msgf("Failed to send packet to peer %s", peer.ID)
				}
			}

			// TODO Implement waiting for leave message
			/* 			// If this node sent a leave packet then notify that it has been sent
			   			if item.packet.MessageType == peerLeaveMsg && item.packet.SenderID == cluster.localPeer.ID {
			   				c.leaveSent <- struct{}{}
			   			} */

		case <-c.shutdownContext.Done():
			return
		}
	}
}
