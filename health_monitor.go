package gossip

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
}

func newHealthMonitor(c *Cluster) *healthMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	return &healthMonitor{
		cluster:        c,
		config:         c.config,
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
		peerPingMutex:  sync.Mutex{},
		pingSeq:        0,
		peerPingAck:    make(map[healthPingID]chan bool),
	}
}

func (hm *healthMonitor) stop() {
	hm.shutdownCancel()
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
