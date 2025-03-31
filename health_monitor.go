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
	cluster          *Cluster
	config           *Config
	shutdownCtx      context.Context    // Parent context for shutdown
	shutdownCancel   context.CancelFunc // Function to cancel the context
	peerPingMutex    sync.Mutex
	pingSeq          uint32
	peerPingAck      map[healthPingID]chan bool
	healthCheckQueue chan *Node
	nodeFailures     sync.Map // NodeID -> *nodeFailureCount
	suspicionMap     sync.Map // NodeID -> *suspicionEvidence
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
		cluster:          c,
		config:           c.config,
		shutdownCtx:      ctx,
		shutdownCancel:   cancel,
		peerPingMutex:    sync.Mutex{},
		pingSeq:          0,
		peerPingAck:      make(map[healthPingID]chan bool),
		healthCheckQueue: make(chan *Node, c.config.HealthCheckSampleSize),
	}

	// Start health check workers
	for i := 0; i < hm.config.HealthCheckSampleSize; i++ {
		go func() {
			for {
				select {
				case node, ok := <-hm.healthCheckQueue:
					if !ok {
						// Channel closed, exit worker
						return
					}
					hm.checkNodeHealth(node)

				case <-hm.shutdownCtx.Done():
					return
				}
			}
		}()
	}

	go hm.healthCheckLoop()

	return hm
}

func (hm *healthMonitor) stop() {
	hm.shutdownCancel()
	close(hm.healthCheckQueue)
}

func (hm *healthMonitor) cleanNodeState(nodeID NodeID) {
	hm.suspicionMap.Delete(nodeID)
	hm.nodeFailures.Delete(nodeID)
}

func (hm *healthMonitor) healthCheckLoop() {
	// Create a ticker for periodic health checks
	checkTicker := time.NewTicker(hm.config.HealthCheckInterval)
	suspectTicker := time.NewTicker(hm.config.SuspectTimeout)
	deadNodeTicker := time.NewTicker(hm.config.DeadNodeTimeout)
	cleanupTicker := time.NewTicker(hm.config.DeadNodeTimeout * 5)

	defer checkTicker.Stop()
	defer suspectTicker.Stop()
	defer deadNodeTicker.Stop()
	defer cleanupTicker.Stop()

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

		case <-cleanupTicker.C:
			hm.cleanupNodeFailures()
		}
	}
}

// cleanupNodeFailures removes entries from the nodeFailures map that
// are no longer needed (nodes are healthy or no longer in the cluster)
func (hm *healthMonitor) cleanupNodeFailures() {
	// Track nodes to clean
	var nodesToClean []NodeID

	// First pass: find candidates for removal
	hm.nodeFailures.Range(func(key, value interface{}) bool {
		nodeID := key.(NodeID)
		failTracker := value.(*nodeFailureCount)

		// Check if node exists
		node := hm.cluster.nodes.get(nodeID)
		if node == nil {
			// Node doesn't exist, mark for cleanup
			nodesToClean = append(nodesToClean, nodeID)
			return true
		}

		// If node is healthy and failure count is 0, clean it up
		if node.state == nodeAlive && failTracker.count.Load() == 0 {
			nodesToClean = append(nodesToClean, nodeID)
		}

		return true
	})

	// Second pass: delete entries
	for _, nodeID := range nodesToClean {
		hm.nodeFailures.Delete(nodeID)
	}

	if len(nodesToClean) > 0 {
		log.Debug().Int("cleaned_count", len(nodesToClean)).
			Msg("Cleaned up node failure trackers")
	}
}

func (hm *healthMonitor) checkRandomNodes() {
	// Get a random selection of nodes to check
	nodesToCheck := hm.cluster.nodes.getRandomLiveNodes(
		hm.config.HealthCheckSampleSize,
		[]NodeID{hm.cluster.localNode.ID},
	)

	// Queue nodes for checking by the worker pool
	for _, node := range nodesToCheck {
		// Use non-blocking send to avoid getting stuck if queue is full
		select {
		case hm.healthCheckQueue <- node:
			// Successfully queued
		default:
			// Queue full, log and skip this node
			log.Warn().
				Str("node", node.ID.String()).
				Msg("Health check queue full, skipping check")
		}
	}
}

// Check health for a single node
func (hm *healthMonitor) checkNodeHealth(node *Node) {
	// Skip if node is already marked dead or leaving
	if node.state == nodeDead || node.state == nodeLeaving {
		return
	}

	// Check if we've received a message from this node recently
	lastActivityTime := node.getLastActivity()
	timeSinceActivity := time.Since(lastActivityTime)

	// If we've heard from this node within half the health check interval, consider it alive without pinging
	//activityThreshold := hm.config.HealthCheckInterval / 2
	activityThreshold := time.Duration(float64(hm.config.HealthCheckInterval) * hm.config.ActivityThresholdPercent)
	if timeSinceActivity < activityThreshold {
		// If node was previously marked suspect, restore to alive
		if node.state == nodeSuspect {
			log.Info().Str("node", node.ID.String()).
				Dur("since_activity", timeSinceActivity).
				Msg("Recent activity from suspect node, marking as alive")

			hm.cluster.nodes.updateState(node.ID, nodeAlive)
			hm.cleanNodeState(node.ID)
		}

		// Clean up failure tracking if it exists
		if _, exists := hm.nodeFailures.Load(node.ID); exists {
			hm.nodeFailures.Delete(node.ID)
		}

		return
	}

	alive, err := hm.pingAny(node)
	if alive {
		if _, exists := hm.nodeFailures.Load(node.ID); exists {
			hm.nodeFailures.Delete(node.ID)
		}

		// If node was suspect, restore to alive
		if node.state == nodeSuspect {
			log.Info().Str("node", node.ID.String()).Msg("Suspect node is now reachable, marking as alive")
			hm.cluster.nodes.updateState(node.ID, nodeAlive)

			hm.cleanNodeState(node.ID)
		}

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
	suspectNodes := hm.cluster.nodes.getAllInStates([]NodeState{nodeSuspect})

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
	for range maxWorkers {
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

		// Clean up any suspicion evidence
		hm.cleanNodeState(node.ID)
		return
	}

	// If the node has been suspect for too long, check it one more time
	suspectDuration := time.Since(evidence.suspectTime)
	if suspectDuration > hm.config.SuspectTimeout {
		// Try one final ping
		if alive, _ := hm.pingAny(node); alive {
			log.Info().Str("node", node.ID.String()).
				Msg("Suspect node responded after timeout, marking as alive")

			hm.cluster.nodes.updateState(node.ID, nodeAlive)

			// Clean up tracking
			hm.nodeFailures.Delete(node.ID)
		} else {
			log.Info().Str("node", node.ID.String()).
				Msg("Suspect node timed out and is unreachable, marking as dead")

			hm.cluster.nodes.updateState(node.ID, nodeDead)
		}

		// Clean up any suspicion evidence
		hm.suspicionMap.Delete(node.ID)
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
	deadNodes := hm.cluster.nodes.getAllInStates([]NodeState{nodeDead, nodeLeaving})

	now := time.Now()
	for _, node := range deadNodes {
		// Calculate how long the node has been dead
		deadDuration := now.Sub(node.stateChangeTime)

		// After a certain timeout, permanently remove the node
		if deadDuration > hm.config.DeadNodeTimeout {
			log.Info().Str("node", node.ID.String()).
				Msg("Dead node timeout expired, removing from cluster")

			hm.removeNodeEvidenceFromAllNodes(node.ID)
			hm.cluster.nodes.remove(node.ID)

			// Clean up any suspicion tracking
			hm.cleanNodeState(node.ID)
		}
	}
}

// Helper function to remove evidence from a specific node from all other nodes' suspicion tracking
func (hm *healthMonitor) removeNodeEvidenceFromAllNodes(nodeID NodeID) {
	// Track which nodes had evidence removed and may need re-evaluation
	nodesNeedingEvaluation := make([]*Node, 0)

	// Iterate through all entries in the suspicion map
	hm.suspicionMap.Range(func(key, value interface{}) bool {
		suspectID := key.(NodeID)
		evidence := value.(*suspicionEvidence)

		// Skip the node's own entry
		if suspectID == nodeID {
			return true
		}

		// Check if this node has evidence from the node being removed
		evidence.mutex.Lock()
		hadEvidence := false

		if _, exists := evidence.confirmations[nodeID]; exists {
			delete(evidence.confirmations, nodeID)
			hadEvidence = true
		}

		if _, exists := evidence.refutations[nodeID]; exists {
			delete(evidence.refutations, nodeID)
			hadEvidence = true
		}
		evidence.mutex.Unlock()

		// If we removed evidence, this node might need re-evaluation
		if hadEvidence {
			if node := hm.cluster.nodes.get(suspectID); node != nil {
				// Only add suspect nodes for re-evaluation
				if node.state == nodeSuspect {
					nodesNeedingEvaluation = append(nodesNeedingEvaluation, node)
				}
			}
		}

		return true
	})

	log.Debug().Str("node", nodeID.String()).
		Int("affected_nodes", len(nodesNeedingEvaluation)).
		Msg("Removed node's evidence from all suspicion tracking")

	// Re-evaluate all affected nodes
	for _, node := range nodesNeedingEvaluation {
		log.Debug().Str("node", node.ID.String()).
			Msg("Re-evaluating node after evidence removal")
		hm.evaluateSuspectNode(node)
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
	suspectNode := hm.cluster.nodes.get(msg.NodeID)
	if suspectNode == nil {
		log.Debug().Str("suspect", msg.NodeID.String()).
			Str("from", sender.ID.String()).
			Msg("Received suspicion for unknown node")
		return nil
	}

	// If we've seen this node alive recently, refute the suspicion
	if suspectNode.state == nodeAlive {
		// Try to ping to confirm it's still alive
		if alive, _ := hm.pingAny(suspectNode); alive {
			// Refute the suspicion by reporting node is alive
			hm.broadcastAlive(suspectNode)
			return nil
		}
	}

	// Get suspicion tracking data
	evidenceObj, _ := hm.suspicionMap.LoadOrStore(
		msg.NodeID,
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
			Msg("Received alive message for unknown node, adding to cluster")

		newNode := newNode(msg.NodeID, msg.AdvertisedAddr)
		hm.cluster.nodes.addIfNotExists(newNode)

		return nil
	}

	// If node is suspect or dead, try a direct ping to confirm
	if aliveNode.state == nodeSuspect || aliveNode.state == nodeDead {
		// Try to ping to verify
		if alive, _ := hm.pingAny(aliveNode); alive {
			log.Info().Str("node", aliveNode.ID.String()).
				Str("from", sender.ID.String()).
				Str("state", aliveNode.state.String()).
				Msg("Node reported alive and confirmed by ping, marking as alive")

			hm.cluster.nodes.updateState(aliveNode.ID, nodeAlive)

			// Clean up any suspicion evidence
			hm.cleanNodeState(aliveNode.ID)

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

// Handle incoming suspicion message from another node
func (hm *healthMonitor) handleLeaving(sender *Node, packet *Packet) error {
	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	msg := leavingMessage{}
	if err := packet.Unmarshal(&msg); err != nil {
		return err
	}

	// Ignore self-leaving messages
	if msg.NodeID == hm.cluster.localNode.ID {
		return nil
	}

	// Check if we know about this node
	node := hm.cluster.nodes.get(msg.NodeID)
	if node == nil {
		log.Debug().Str("node", msg.NodeID.String()).
			Msg("Received leaving message for unknown node, ignoring")
		return nil
	}

	// A leaving message is authoritative - it overrides any current state
	// as it represents an explicit, intentional departure
	if node.state != nodeLeaving {
		log.Info().Str("node", node.ID.String()).
			Str("old_state", node.state.String()).
			Msg("Moving node directly to leaving state")

		// Update node state to leaving
		hm.cluster.nodes.updateState(node.ID, nodeLeaving)
	}

	// Clean up any monitoring state for this node
	// We don't need to track failures or suspicions for a node that's explicitly leaving
	hm.cleanNodeState(node.ID)

	return nil
}

// Broadcast alive status about a node to refute suspicion
func (hm *healthMonitor) broadcastAlive(aliveNode *Node) {
	log.Debug().Str("node", aliveNode.ID.String()).Msg("Broadcasting alive status to cluster")

	msg := &aliveMessage{
		NodeID:         aliveNode.ID,
		AdvertisedAddr: aliveNode.advertisedAddr,
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
		NodeID: suspectNode.ID,
	}

	packet, err := hm.cluster.transport.buildPacket(hm.cluster.localNode.ID, suspicionMsg, hm.cluster.getMaxTTL(), &msg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to build suspicion message packet")
		return
	}

	hm.cluster.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{hm.cluster.localNode.ID})
}

// Broadcast leaving status about a node to the cluster
func (hm *healthMonitor) broadcastLeaving(leavingNode *Node) {
	msg := &leavingMessage{
		NodeID: leavingNode.ID,
	}

	packet, err := hm.cluster.transport.buildPacket(hm.cluster.localNode.ID, leavingMsg, hm.cluster.getMaxTTL(), &msg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to build leaving message packet")
		return
	}

	// Use reliable transport for leaving messages to ensure delivery
	hm.cluster.enqueuePacketForBroadcast(packet, TransportReliable, []NodeID{hm.cluster.localNode.ID})
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
		FromAddr: hm.cluster.localNode.advertisedAddr,
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
		FromAddr:       hm.cluster.localNode.advertisedAddr,
	}

	peerCount := hm.cluster.getPeerSubsetSize(
		hm.cluster.nodes.getLiveCount(),
		purposeIndirectPing,
	)

	sentCount := 0
	indirectPeers := hm.cluster.nodes.getRandomLiveNodes(peerCount, []NodeID{hm.cluster.localNode.ID, node.ID})
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

// Tries to ping a node directly first, and if that fails, it tries to ping it indirectly.
func (hm *healthMonitor) pingAny(node *Node) (bool, error) {
	// Try direct ping first
	alive, err := hm.pingNode(node)
	if alive {
		return true, nil
	}

	// If direct ping fails, try indirect ping
	if hm.config.EnableIndirectPings {
		return hm.indirectPingNode(node)
	}

	return false, err
}

// Combine the node state from remote peers with the local state.
func (hm *healthMonitor) combineRemoteNodeState(sender *Node, remoteStates []exchangeNodeState) {
	if len(remoteStates) == 0 {
		return
	}

	log.Debug().Int("state_count", len(remoteStates)).Msg("Combining remote node states")

	// Process each remote node state
	for _, remoteState := range remoteStates {
		// Handle self states
		if remoteState.ID == hm.cluster.localNode.ID {
			// Sates don't match so refute the remote state
			if hm.cluster.localNode.state != remoteState.State {
				if hm.cluster.localNode.state == nodeAlive {
					log.Warn().Str("remote_state", remoteState.State.String()).
						Msg("Remote node state does not match local state, broadcasting alive status")

					hm.broadcastAlive(hm.cluster.localNode)
				} else if hm.cluster.localNode.state == nodeLeaving {
					log.Warn().Str("remote_state", remoteState.State.String()).
						Msg("Remote node state does not match local state, broadcasting leaving status")

					hm.broadcastLeaving(hm.cluster.localNode)
				}
			}

			// Always skip further processing for our own node
			continue
		}

		// Get the local node if we know about it
		localNode := hm.cluster.nodes.get(remoteState.ID)

		// If we don't know this node, add it
		if localNode == nil {
			// Ignore unknown nodes that are leaving or dead, otherwise add the node
			if remoteState.State == nodeAlive || remoteState.State == nodeSuspect {
				log.Debug().Str("node", remoteState.ID.String()).
					Str("remote_state", remoteState.State.String()).
					Msg("Discovered new node from remote state")

				// Create a new node with the remote information
				newNode := newNode(remoteState.ID, remoteState.AdvertisedAddr)
				newNode.state = remoteState.State

				// Add the node to our list
				hm.cluster.nodes.addIfNotExists(newNode)
			}

			// No further processing needed for new nodes
			continue
		}

		// We know about this node, determine if we should update our state
		log.Debug().Str("node", remoteState.ID.String()).
			Str("local_state", localNode.state.String()).
			Str("remote_state", remoteState.State.String()).
			Msg("Comparing local and remote node state")

		// Handle each combination of local/remote states
		switch {
		// Remote reports node as dead
		case remoteState.State == nodeDead:
			// If we think it's alive, try to confirm with a ping
			if localNode.state == nodeAlive {
				// Ping to verify
				alive, _ := hm.pingAny(localNode)
				if !alive {
					log.Info().Str("node", localNode.ID.String()).
						Msg("Remote reports node as dead, marking as suspect after ping failure")
					hm.cluster.nodes.updateState(localNode.ID, nodeSuspect)

					// Create suspicion tracking and record this confirmation
					evidenceObj, _ := hm.suspicionMap.LoadOrStore(
						localNode.ID,
						&suspicionEvidence{
							suspectTime:   time.Now(),
							confirmations: make(map[NodeID]bool),
							refutations:   make(map[NodeID]bool),
						},
					)
					evidence := evidenceObj.(*suspicionEvidence)

					// Record this confirmation (use a dummy ID for the remote peer)
					evidence.mutex.Lock()
					evidence.confirmations[sender.ID] = true
					evidence.mutex.Unlock()
				} else {
					// Our node is alive, refute the suspicion
					log.Debug().Str("node", localNode.ID.String()).
						Msg("Remote reports node as dead, but node is reachable - keeping alive")
					hm.broadcastAlive(localNode)
				}
			} else if localNode.state == nodeSuspect {
				// If we already think it's suspect, record the remote confirmation
				log.Debug().Str("node", localNode.ID.String()).
					Msg("Remote confirms our suspicion of node")

				evidenceObj, _ := hm.suspicionMap.LoadOrStore(
					localNode.ID,
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

				// Re-evaluate with the new evidence
				hm.evaluateSuspectNode(localNode)
			}
			// If we already think it's dead, nothing to do

		// Remote reports node as suspect
		case remoteState.State == nodeSuspect:
			// If we think it's alive, verify
			if localNode.state == nodeAlive {
				// Ping to verify
				alive, _ := hm.pingAny(localNode)
				if !alive {
					log.Info().Str("node", localNode.ID.String()).
						Msg("Remote reports node as suspect, marking as suspect after ping failure")
					hm.cluster.nodes.updateState(localNode.ID, nodeSuspect)
				} else {
					// Node is alive, refute the suspicion
					log.Debug().Str("node", localNode.ID.String()).
						Msg("Remote reports node as suspect, but node is reachable - keeping alive")
					hm.broadcastAlive(localNode)
				}
			}
			// If we think it's dead or leaving, our state takes precedence

		// Remote reports node as alive
		case remoteState.State == nodeAlive:
			// If we think it's suspect or dead, verify
			if localNode.state == nodeSuspect || localNode.state == nodeDead {
				// Try to ping and verify
				alive, _ := hm.pingAny(localNode)
				if alive {
					log.Info().Str("node", localNode.ID.String()).
						Str("local_state", localNode.state.String()).
						Msg("Remote reports node as alive, restored to alive state after ping success")
					hm.cluster.nodes.updateState(localNode.ID, nodeAlive)

					// Clean up any failure tracking and suspicion evidence
					hm.cleanNodeState(localNode.ID)
				} else {
					// Record the refutation even though our ping failed
					if localNode.state == nodeSuspect {
						log.Debug().Str("node", localNode.ID.String()).
							Msg("Remote reports node as alive but ping failed, recording refutation")

						evidenceObj, _ := hm.suspicionMap.LoadOrStore(
							localNode.ID,
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
						hm.evaluateSuspectNode(localNode)
					}
				}
			}

		// Remote reports node as leaving
		case remoteState.State == nodeLeaving:
			if localNode.state != nodeLeaving {
				log.Info().Str("node", localNode.ID.String()).
					Msg("Remote reports node as leaving, updating state")
				hm.cluster.nodes.updateState(localNode.ID, nodeLeaving)

				// Clean up any failure tracking and suspicion evidence
				hm.cleanNodeState(localNode.ID)
			}
		}
	}
}

// MarkNodeLeaving marks a node as leaving and broadcasts this to the cluster
func (hm *healthMonitor) MarkNodeLeaving(node *Node) {
	// Update state
	hm.cluster.nodes.updateState(node.ID, nodeLeaving)

	// Clean up any tracking state
	hm.cleanNodeState(node.ID)

	// Broadcast leaving message multiple times to increase chance of delivery
	for i := 0; i < 3; i++ {
		hm.cluster.healthMonitor.broadcastLeaving(hm.cluster.localNode)
		time.Sleep(100 * time.Millisecond)
	}

	// Give some time for the message to propagate
	time.Sleep(200 * time.Millisecond)
}
