package gossip

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

type healthPingID struct {
	TargetID NodeID
	Seq      uint32
}

type healthMonitor struct {
	cluster        *Cluster
	config         *Config
	shutdownCtx    context.Context    // Parent context for shutdown
	shutdownCancel context.CancelFunc // Function to cancel the context
	peerPingMutex  sync.Mutex
	pingSeq        uint32
	peerPingAck    map[healthPingID]chan bool
	nodeFailures   sync.Map // NodeID -> *nodeFailureCount
	suspicionMap   sync.Map // NodeID -> *suspicionEvidence
}

type nodeFailureCount struct {
	count         atomic.Int32
	lastCheckTime atomic.Int64 // Unix timestamp of last check
}

// Track suspicion confirmations from other peers
type suspicionEvidence struct {
	suspectTime   time.Time
	confirmations map[NodeID]bool // Peers that have confirmed suspicion
	refutations   map[NodeID]bool // Peers that have refuted suspicion (seen the node alive)
	mutex         sync.RWMutex
}

func newHealthMonitor(c *Cluster) *healthMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	hm := &healthMonitor{
		cluster:        c,
		config:         c.config,
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
		peerPingMutex:  sync.Mutex{},
		pingSeq:        0,
		peerPingAck:    make(map[healthPingID]chan bool),
	}

	go hm.healthCheckLoop()

	return hm
}

func (hm *healthMonitor) stop() {
	hm.shutdownCancel()
}

func (hm *healthMonitor) healthCheckLoop() {
	// Create a ticker for periodic health checks
	checkTicker := time.NewTicker(hm.config.HealthCheckInterval)
	suspectTicker := time.NewTicker(hm.config.SuspectTimeout)
	deadNodeTicker := time.NewTicker(hm.config.DeadNodeTimeout)

	defer checkTicker.Stop()
	defer suspectTicker.Stop()
	defer deadNodeTicker.Stop()

	for {
		select {
		case <-hm.shutdownCtx.Done():
			return

		case <-checkTicker.C:
			hm.checkRandomNodes()

		case <-suspectTicker.C:
			hm.processSuspectNodes()

		case <-deadNodeTicker.C:
			hm.cleanupDeadNodes()
		}
	}
}

func (hm *healthMonitor) checkRandomNodes() {
	// Get a random selection of nodes to check
	nodesToCheck := hm.cluster.nodes.getRandomLiveNodes(
		hm.config.HealthCheckSampleSize,
		[]NodeID{hm.cluster.localNode.ID},
	)

	// Check each node
	for _, node := range nodesToCheck {
		go hm.checkNodeHealth(node)
	}
}

// Check health for a single node
func (hm *healthMonitor) checkNodeHealth(node *Node) {
	// Skip if node is already marked dead
	if node.state == nodeDead {
		return
	}

	// Get or create failure tracker
	failCountObj, _ := hm.nodeFailures.LoadOrStore(
		node.ID,
		&nodeFailureCount{},
	)
	failTracker := failCountObj.(*nodeFailureCount)

	// Update last check time
	failTracker.lastCheckTime.Store(time.Now().Unix())

	// Direct ping first
	alive, err := hm.pingNode(node)
	if alive {
		// Node is alive, reset failure counter
		failTracker.count.Store(0)

		// If node was suspect, restore to alive
		if node.state == nodeSuspect {
			log.Info().Str("node", node.ID.String()).Msg("Suspect node is now reachable, marking as alive")
			hm.cluster.nodes.updateState(node.ID, nodeAlive)
		}
		return
	}

	// Direct ping failed, try indirect ping if enabled
	if hm.config.EnableIndirectPings {
		alive, err = hm.indirectPingNode(node)
		if alive {
			// Node is alive but unreachable directly, keep track but don't immediately mark suspect
			log.Debug().Str("node", node.ID.String()).Msg("Node reachable indirectly")
			failTracker.count.Store(0)
			return
		}
	}

	// Both direct and indirect pings failed
	currentFailures := failTracker.count.Add(1)

	// Log the failure
	if err != nil {
		log.Debug().Err(err).Str("node", node.ID.String()).
			Int32("failures", currentFailures).
			Msg("Node health check failed")
	} else {
		log.Debug().Str("node", node.ID.String()).
			Int32("failures", currentFailures).
			Msg("Node did not respond to health check")
	}

	// Mark node as suspect after sufficient failures
	if currentFailures >= int32(hm.config.SuspectThreshold) && node.state == nodeAlive {
		log.Info().Str("node", node.ID.String()).
			Int32("failures", currentFailures).
			Msg("Node exceeded failure threshold, marking as suspect")

		hm.cluster.nodes.updateState(node.ID, nodeSuspect)

		// Broadcast the suspect status
		hm.broadcastSuspicion(node)
	}
}

func (hm *healthMonitor) processSuspectNodes() {
	// Get all suspect nodes
	suspectNodes := hm.cluster.nodes.getAllInState(nodeSuspect)

	// Don't bother with parallelization for small numbers
	if len(suspectNodes) <= 2 {
		for _, node := range suspectNodes {
			hm.evaluateSuspectNode(node)
		}
		return
	}

	// Use a wait group to track completion
	var wg sync.WaitGroup

	// Control the level of parallelism - don't spawn too many goroutines
	// Use the smaller of: number of suspect nodes or MaxParallelSuspectEvaluations
	maxWorkers := hm.config.MaxParallelSuspectEvaluations
	if maxWorkers <= 0 {
		// Default to a reasonable number if not configured
		maxWorkers = 4
	}
	if len(suspectNodes) < maxWorkers {
		maxWorkers = len(suspectNodes)
	}

	// Create a channel to feed nodes to workers
	nodeChan := make(chan *Node, len(suspectNodes))

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for node := range nodeChan {
				hm.evaluateSuspectNode(node)
			}
		}()
	}

	// Feed nodes to workers
	for _, node := range suspectNodes {
		nodeChan <- node
	}
	close(nodeChan)

	// Wait for all evaluations to complete
	wg.Wait()
}

func (hm *healthMonitor) evaluateSuspectNode(node *Node) {
	// Get suspicion tracking data
	evidenceObj, _ := hm.suspicionMap.LoadOrStore(
		node.ID,
		&suspicionEvidence{
			suspectTime:   time.Now(),
			confirmations: make(map[NodeID]bool),
			refutations:   make(map[NodeID]bool),
		},
	)
	evidence := evidenceObj.(*suspicionEvidence)

	evidence.mutex.RLock()
	confirmationCount := len(evidence.confirmations)
	refutationCount := len(evidence.refutations)
	evidence.mutex.RUnlock()

	// Check if we have enough peer confirmations to mark node as dead
	quorum := hm.getSuspicionQuorum()
	if confirmationCount >= quorum {
		log.Info().Str("node", node.ID.String()).
			Int("confirmations", confirmationCount).
			Int("quorum", quorum).
			Msg("Suspect node confirmed dead by quorum, marking as dead")

		hm.cluster.nodes.updateState(node.ID, nodeDead)
		return
	}

	// Or check if enough peers reported the node as alive
	if refutationCount >= hm.config.RefutationThreshold {
		log.Info().Str("node", node.ID.String()).
			Int("refutations", refutationCount).
			Msg("Suspect node reported alive by multiple peers, restoring to alive")

		hm.cluster.nodes.updateState(node.ID, nodeAlive)
		return
	}

	// If the node has been suspect for too long, check it one more time
	suspectDuration := time.Since(evidence.suspectTime)
	if suspectDuration > hm.config.SuspectTimeout {
		// Try one final ping
		alive, _ := hm.pingNode(node)
		if !alive && hm.config.EnableIndirectPings {
			alive, _ = hm.indirectPingNode(node)
		}

		if alive {
			log.Info().Str("node", node.ID.String()).
				Msg("Suspect node responded after timeout, marking as alive")

			hm.cluster.nodes.updateState(node.ID, nodeAlive)
		} else {
			log.Info().Str("node", node.ID.String()).
				Msg("Suspect node timed out and is unreachable, marking as dead")

			hm.cluster.nodes.updateState(node.ID, nodeDead)
		}
	}
}

// Calculate the required confirmations based on cluster size
func (hm *healthMonitor) getSuspicionQuorum() int {
	aliveCount := hm.cluster.nodes.getAliveCount()

	// At minimum, require 2 nodes to agree
	minQuorum := 2

	// For larger clusters, use a percentage (e.g., 25% of alive nodes)
	quorum := int(math.Max(float64(minQuorum), math.Ceil(float64(aliveCount)*0.25)))

	return quorum
}

func (hm *healthMonitor) cleanupDeadNodes() {
	deadNodes := hm.cluster.nodes.getAllInState(nodeDead)

	now := time.Now()
	for _, node := range deadNodes {
		// Calculate how long the node has been dead
		deadDuration := now.Sub(node.stateChangeTime)

		// After a certain timeout, permanently remove the node
		if deadDuration > hm.config.DeadNodeTimeout {
			log.Info().Str("node", node.ID.String()).
				Msg("Dead node timeout expired, removing from cluster")

			hm.cluster.nodes.remove(node.ID)

			// Clean up any suspicion tracking
			hm.suspicionMap.Delete(node.ID)
			hm.nodeFailures.Delete(node.ID)
		}
	}
}

// Handle incoming suspicion message from another node
func (hm *healthMonitor) handleSuspicion(sender *Node, packet *Packet) error {
	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	msg := suspicionMessage{}
	if err := packet.Unmarshal(&msg); err != nil {
		return err
	}

	// Check if we know about this node
	suspectNode := hm.cluster.nodes.get(msg.SuspectID)
	if suspectNode == nil {
		log.Debug().Str("suspect", msg.SuspectID.String()).
			Str("from", sender.ID.String()).
			Msg("Received suspicion for unknown node")
		return nil
	}

	// If we've seen this node alive recently, refute the suspicion
	if suspectNode.state == nodeAlive {
		// Try to ping to confirm it's still alive
		alive, _ := hm.pingNode(suspectNode)

		if alive {
			// Refute the suspicion by reporting node is alive
			hm.broadcastAlive(suspectNode)
			return nil
		}
	}

	// Get suspicion tracking data
	evidenceObj, _ := hm.suspicionMap.LoadOrStore(
		msg.SuspectID,
		&suspicionEvidence{
			suspectTime:   time.Now(),
			confirmations: make(map[NodeID]bool),
			refutations:   make(map[NodeID]bool),
		},
	)
	evidence := evidenceObj.(*suspicionEvidence)

	// Record this confirmation
	evidence.mutex.Lock()
	evidence.confirmations[sender.ID] = true
	evidence.mutex.Unlock()

	// Update node state if needed
	if suspectNode.state == nodeAlive {
		log.Info().Str("node", suspectNode.ID.String()).
			Str("from", sender.ID.String()).
			Msg("Node reported suspect by peer, marking as suspect")

		hm.cluster.nodes.updateState(suspectNode.ID, nodeSuspect)
	}

	// Re-evaluate with the new evidence
	hm.evaluateSuspectNode(suspectNode)

	return nil
}

// Handle incoming alive refutation message
func (hm *healthMonitor) handleAlive(sender *Node, packet *Packet) error {
	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	msg := aliveMessage{}
	if err := packet.Unmarshal(&msg); err != nil {
		return err
	}

	// Check if we know about this node
	aliveNode := hm.cluster.nodes.get(msg.NodeID)
	if aliveNode == nil {
		log.Debug().Str("node", msg.NodeID.String()).
			Str("from", sender.ID.String()).
			Msg("Received alive message for unknown node")
		return nil
	}

	// If node is suspect or dead, try a direct ping to confirm
	if aliveNode.state == nodeSuspect || aliveNode.state == nodeDead {
		// Try to ping to verify
		alive, _ := hm.pingNode(aliveNode)

		if alive {
			log.Info().Str("node", aliveNode.ID.String()).
				Str("from", sender.ID.String()).
				Str("state", aliveNode.state.String()).
				Msg("Node reported alive and confirmed by ping, marking as alive")

			hm.cluster.nodes.updateState(aliveNode.ID, nodeAlive)
			return nil
		}

		// If direct ping fails, record the refutation
		evidenceObj, _ := hm.suspicionMap.LoadOrStore(
			msg.NodeID,
			&suspicionEvidence{
				suspectTime:   time.Now(),
				confirmations: make(map[NodeID]bool),
				refutations:   make(map[NodeID]bool),
			},
		)
		evidence := evidenceObj.(*suspicionEvidence)

		// Record this refutation
		evidence.mutex.Lock()
		evidence.refutations[sender.ID] = true
		evidence.mutex.Unlock()

		// Re-evaluate with the new evidence
		if aliveNode.state == nodeSuspect {
			hm.evaluateSuspectNode(aliveNode)
		}
	}

	return nil
}

// Broadcast alive status about a node to refute suspicion
func (hm *healthMonitor) broadcastAlive(aliveNode *Node) {
	log.Debug().Str("node", aliveNode.ID.String()).Msg("Broadcasting alive status to cluster")

	msg := &aliveMessage{
		NodeID: aliveNode.ID,
	}

	packet, err := hm.cluster.transport.buildPacket(hm.cluster.localNode.ID, aliveMsg, hm.cluster.getMaxTTL(), &msg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to build alive message packet")
		return
	}

	hm.cluster.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{hm.cluster.localNode.ID})
}

// Broadcast suspicion about a node to the cluster
func (hm *healthMonitor) broadcastSuspicion(suspectNode *Node) {
	log.Debug().Str("node", suspectNode.ID.String()).Msg("Broadcasting suspicion to cluster")

	msg := &suspicionMessage{
		SuspectID: suspectNode.ID,
	}

	packet, err := hm.cluster.transport.buildPacket(hm.cluster.localNode.ID, suspicionMsg, hm.cluster.getMaxTTL(), &msg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to build suspicion message packet")
		return
	}

	hm.cluster.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{hm.cluster.localNode.ID})
}

// pingNode sends a ping message direct to the specified node and waits for an acknowledgment.
func (hm *healthMonitor) pingNode(node *Node) (bool, error) {
	// Create a context that can be cancelled either by timeout or cluster shutdown
	// This creates a context hierarchy: parent (shutdown) -> child (timeout)
	parentCtx := context.Background()
	if hm.shutdownCtx != nil {
		parentCtx = hm.shutdownCtx
	}
	ctx, cancel := context.WithTimeout(parentCtx, hm.config.PingTimeout)
	defer cancel()

	ackChannel := make(chan bool, 1)

	hm.peerPingMutex.Lock()

	seq := hm.pingSeq
	hm.pingSeq++

	key := healthPingID{
		TargetID: node.ID,
		Seq:      seq,
	}
	hm.peerPingAck[key] = ackChannel
	hm.peerPingMutex.Unlock()

	// Cleanup on exit
	defer func() {
		hm.peerPingMutex.Lock()
		delete(hm.peerPingAck, key)
		hm.peerPingMutex.Unlock()
	}()

	// Send the ping message
	ping := pingMessage{
		TargetID: node.ID,
		Seq:      seq,
	}
	if err := hm.cluster.transport.sendMessage(TransportBestEffort, node, hm.cluster.localNode.ID, pingMsg, 1, &ping); err != nil {
		log.Error().Err(err).Msgf("Failed to send ping message to peer %s", node.ID)
		return false, err
	}

	// Wait for ack or context done (timeout or shutdown)
	var result bool
	select {
	case <-ctx.Done():
		// Check if it was cancelled due to shutdown or timeout
		if errors.Is(ctx.Err(), context.Canceled) {
			return false, fmt.Errorf("ping cancelled due to shutdown")
		}
		// It was a timeout
		result = false
	case ack := <-ackChannel:
		result = ack
	}

	return result, nil
}

func (hm *healthMonitor) pingAckReceived(nodeID NodeID, seq uint32, ack bool) {
	key := healthPingID{
		TargetID: nodeID,
		Seq:      seq,
	}

	// First check if we're waiting for an ack from this peer
	hm.peerPingMutex.Lock()
	ackChannel, exists := hm.peerPingAck[key]
	hm.peerPingMutex.Unlock()

	if exists {
		// Non-blocking send to the ack channel
		select {
		case ackChannel <- ack:
			// Successfully sent the ack
		default:
			// Channel already has a value or is closed
		}
	}
}

// indirectPingNode sends a request to a selection of nodes asking them to ping the specified node and waits for an acknowledgment.
func (hm *healthMonitor) indirectPingNode(node *Node) (bool, error) {

	// Create a context that can be cancelled either by timeout or cluster shutdown
	// This creates a context hierarchy: parent (shutdown) -> child (timeout)
	parentCtx := context.Background()
	if hm.shutdownCtx != nil {
		parentCtx = hm.shutdownCtx
	}
	ctx, cancel := context.WithTimeout(parentCtx, hm.cluster.config.PingTimeout)
	defer cancel()

	ackChannel := make(chan bool, 1)

	hm.peerPingMutex.Lock()

	seq := hm.pingSeq
	hm.pingSeq++

	key := healthPingID{
		TargetID: node.ID,
		Seq:      seq,
	}
	hm.peerPingAck[key] = ackChannel
	hm.peerPingMutex.Unlock()

	// Cleanup on exit
	defer func() {
		hm.peerPingMutex.Lock()
		delete(hm.peerPingAck, key)
		hm.peerPingMutex.Unlock()
	}()

	// Send the ping message
	ping := indirectPingMessage{
		TargetID:       node.ID,
		AdvertisedAddr: node.advertisedAddr, // Send the advertised address in case the node we're using doesn't know about the target node yet
		Seq:            seq,
	}

	sentCount := 0
	indirectPeers := hm.cluster.nodes.getRandomLiveNodes(hm.config.MaxNodesIndirectPing, []NodeID{hm.cluster.localNode.ID, node.ID})
	if len(indirectPeers) == 0 {
		return false, fmt.Errorf("no indirect peers found")
	}

	for _, indirectPeer := range indirectPeers {
		if err := hm.cluster.transport.sendMessage(TransportBestEffort, indirectPeer, hm.cluster.localNode.ID, indirectPingMsg, 1, &ping); err == nil {
			sentCount++
		} else {
			log.Warn().Err(err).Msgf("Failed to send indirect ping message to peer %s", indirectPeer.ID)
		}
	}

	if sentCount == 0 {
		return false, fmt.Errorf("no indirect peers found")
	}

	// Wait for ack or context done (timeout or shutdown)
	var result bool
	select {
	case <-ctx.Done():
		// Check if it was cancelled due to shutdown or timeout
		if errors.Is(ctx.Err(), context.Canceled) {
			return false, fmt.Errorf("ping cancelled due to shutdown")
		}
		// It was a timeout
		result = false
	case <-ackChannel:
		result = true // Received acknowledgment
	}

	return result, nil
}
