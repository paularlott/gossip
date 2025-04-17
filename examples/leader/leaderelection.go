package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paularlott/gossip"
)

type Config struct {
	LeaderCheckInterval  time.Duration      // How often to check if we need to elect a leader
	LeaderTimeout        time.Duration      // How long a leader is considered valid without updates
	HeartbeatMessageType gossip.MessageType // Message type for heartbeat messages
	QuorumPercentage     int                // Percentage of nodes required for quorum (1-100)
}

func DefaultConfig() *Config {
	return &Config{
		LeaderCheckInterval:  1 * time.Second,
		LeaderTimeout:        3 * time.Second,
		HeartbeatMessageType: gossip.ReservedMsgsStart + 1,
		QuorumPercentage:     51,
	}
}

type LeaderEventType int

const (
	LeaderElectedEvent LeaderEventType = iota
	LeaderLostEvent
	BecameLeaderEvent
	SteppedDownEvent
)

func (le LeaderEventType) String() string {
	switch le {
	case LeaderElectedEvent:
		return "Leader Elected"
	case LeaderLostEvent:
		return "Leader Lost"
	case BecameLeaderEvent:
		return "Became Leader"
	case SteppedDownEvent:
		return "Stepped Down"
	default:
		return "Unknown"
	}
}

type LeaderEventHandler func(LeaderEventType, gossip.NodeID)

type leaderEventHandlers struct {
	handlers atomic.Value
}

func newLeaderEventHandlers() *leaderEventHandlers {
	handlers := &leaderEventHandlers{}
	handlers.handlers.Store(make(map[LeaderEventType][]LeaderEventHandler))
	return handlers
}

func (h *leaderEventHandlers) add(eventType LeaderEventType, handler LeaderEventHandler) {
	currentHandlers := h.handlers.Load().(map[LeaderEventType][]LeaderEventHandler)

	// Create a copy of the handlers map
	newHandlers := make(map[LeaderEventType][]LeaderEventHandler)
	for t, handlers := range currentHandlers {
		newHandlers[t] = append([]LeaderEventHandler{}, handlers...)
	}

	// Add the new handler to the appropriate event type
	newHandlers[eventType] = append(newHandlers[eventType], handler)

	// Store the updated map
	h.handlers.Store(newHandlers)
}

func (h *leaderEventHandlers) dispatch(eventType LeaderEventType, leaderID gossip.NodeID) {
	handlers := h.handlers.Load().(map[LeaderEventType][]LeaderEventHandler)

	// Get handlers for this event type
	eventHandlers, ok := handlers[eventType]
	if !ok || len(eventHandlers) == 0 {
		return
	}

	// Call each handler
	for _, handler := range eventHandlers {
		go handler(eventType, leaderID)
	}
}

type heartbeatMessage struct {
	LeaderTime time.Time `msgpack:"ts" json:"ts"`
	Term       uint64    `msgpack:"term" json:"term"` // Election term/epoch number
}

// LeaderElection handles the logic for electing a leader in the cluster
type LeaderElection struct {
	cluster       *gossip.Cluster
	config        *Config
	leaderID      gossip.NodeID
	leaderTime    time.Time
	lastHeartbeat time.Time
	hasLeader     bool
	currentTerm   uint64
	isLeader      bool
	lock          sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	eventHandlers *leaderEventHandlers
}

// NewLeaderElection creates a new leader election manager
func NewLeaderElection(cluster *gossip.Cluster, config *Config) *LeaderElection {
	ctx, cancel := context.WithCancel(context.Background())

	election := &LeaderElection{
		cluster:       cluster,
		config:        config,
		hasLeader:     false,
		currentTerm:   0,
		ctx:           ctx,
		cancel:        cancel,
		eventHandlers: newLeaderEventHandlers(),
	}

	// Register event listeners
	cluster.HandleNodeStateChangeFunc(election.handleNodeStateChange)
	cluster.HandleFunc(config.HeartbeatMessageType, election.handleLeaderHeartbeat)

	return election
}

func (le *LeaderElection) HandleEventFunc(eventType LeaderEventType, handler LeaderEventHandler) {
	le.eventHandlers.add(eventType, handler)
}

// Start the election check process
func (le *LeaderElection) Start() {
	go func() {
		ticker := time.NewTicker(le.config.LeaderCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				le.checkAndElectLeader()
			case <-le.ctx.Done():
				return
			}
		}
	}()
}

// Stop terminates the leader election process
func (le *LeaderElection) Stop() {
	le.cancel()
}

// checkAndElectLeader checks if we need to elect a new leader and does so if necessary
func (le *LeaderElection) checkAndElectLeader() {
	// First, check if there's already a valid leader
	if le.HasLeader() {
		// We have a valid leader, but if it's us we should send a heartbeat
		if le.isLeader {
			le.sendLeaderHeartbeat()
		}
		return
	}

	// We need to elect a leader
	le.electLeader()
}

// IsLeader returns true if the local node is the current leader
func (le *LeaderElection) IsLeader() bool {
	le.lock.RLock()
	defer le.lock.RUnlock()
	return le.isLeader
}

// GetLeaderID returns the ID of the current leader node
func (le *LeaderElection) GetLeaderID() gossip.NodeID {
	le.lock.RLock()
	defer le.lock.RUnlock()
	return le.leaderID
}

// hasValidLeader checks if there's already a leader that has sent a heartbeat recently
func (le *LeaderElection) HasLeader() bool {
	le.lock.RLock()
	defer le.lock.RUnlock()

	// If we don't have a leader yet, no valid leader
	if !le.hasLeader {
		return false
	}

	// If the last heartbeat is too old, the leader is not valid anymore
	if time.Since(le.lastHeartbeat) > le.config.LeaderTimeout {
		return false
	}

	// Check if the leader node still exists in the cluster
	leader := le.cluster.GetNode(le.leaderID)
	if leader == nil || leader.GetState() != gossip.NodeAlive {
		return false
	}

	return true
}

// electLeader chooses a new leader from the alive nodes
func (le *LeaderElection) electLeader() {
	// Get all alive nodes
	aliveNodes := le.cluster.AliveNodes()
	if len(aliveNodes) == 0 {
		// No alive nodes, can't elect a leader
		return
	}

	// Simple leader election strategy: use the node with the "lowest" ID
	var candidateNode *gossip.Node
	for _, node := range aliveNodes {
		if candidateNode == nil || node.ID.String() < candidateNode.ID.String() {
			candidateNode = node
		}
	}

	localNode := le.cluster.LocalNode()

	le.lock.Lock()
	wasLeader := le.isLeader

	if !le.hasLeader {
		le.currentTerm++
	}

	le.leaderID = candidateNode.ID
	le.hasLeader = true
	le.lastHeartbeat = time.Now()
	le.isLeader = (candidateNode.ID == localNode.ID)
	le.lock.Unlock()

	le.cluster.Logger().
		Field("leaderId", candidateNode.ID.String()).
		Field("term", le.currentTerm).
		Field("isLocal", le.isLeader).
		Debugf("New leader elected")

	if !wasLeader && le.isLeader {
		le.eventHandlers.dispatch(BecameLeaderEvent, localNode.ID)
	} else if wasLeader && !le.isLeader {
		le.eventHandlers.dispatch(SteppedDownEvent, localNode.ID)
		le.eventHandlers.dispatch(LeaderElectedEvent, candidateNode.ID)
	} else {
		le.eventHandlers.dispatch(LeaderElectedEvent, candidateNode.ID)
	}

	// If we're the leader, announce ourselves
	if le.isLeader {
		le.sendLeaderHeartbeat()
	}
}

// sendLeaderHeartbeat announces our leadership to the cluster
func (le *LeaderElection) sendLeaderHeartbeat() {
	// Announce leadership to the cluster
	leaderTime := time.Now()

	le.lock.RLock()
	currentTerm := le.currentTerm
	le.lock.RUnlock()

	msg := heartbeatMessage{
		LeaderTime: leaderTime,
		Term:       currentTerm,
	}
	le.cluster.Send(le.config.HeartbeatMessageType, &msg)

	le.lock.Lock()
	le.leaderTime = leaderTime
	le.lastHeartbeat = time.Now()
	le.lock.Unlock()
}

// handleLeaderHeartbeat is called to process incoming heartbeat messages
func (le *LeaderElection) handleLeaderHeartbeat(sender *gossip.Node, packet *gossip.Packet) error {
	if sender == nil {
		return nil
	}

	// Handle the heartbeat message
	var msg heartbeatMessage
	if err := packet.Unmarshal(&msg); err != nil {
		le.cluster.Logger().Errorf("Failed to unmarshal heartbeat message: %v", err)
		return err
	}

	le.lock.Lock()

	// Priority order for deciding leadership:
	// 1. Higher term always wins
	// 2. Within the same term:
	//    a. If we have no leader, accept this one
	//    b. If timestamp is newer, accept this one
	//    c. If timestamps are equal, use lexicographical node ID as tiebreaker
	if msg.Term > le.currentTerm ||
		(msg.Term == le.currentTerm && (!le.hasLeader ||
			msg.LeaderTime.After(le.leaderTime) ||
			(msg.LeaderTime.Equal(le.leaderTime) && sender.ID.String() < le.leaderID.String()))) {

		wasLeader := le.isLeader
		prevLeaderID := le.leaderID
		le.leaderID = sender.ID
		le.hasLeader = true
		le.leaderTime = msg.LeaderTime
		le.lastHeartbeat = time.Now()
		le.currentTerm = msg.Term
		le.isLeader = (sender.ID == le.cluster.LocalNode().ID)

		if wasLeader && !le.isLeader {
			le.cluster.Logger().Debugf("Stepping down as leader")
			le.eventHandlers.dispatch(SteppedDownEvent, le.cluster.LocalNode().ID)
			le.eventHandlers.dispatch(LeaderElectedEvent, sender.ID)
		} else if !wasLeader && le.isLeader {
			le.cluster.Logger().Debugf("Taking leadership")
			le.eventHandlers.dispatch(BecameLeaderEvent, le.cluster.LocalNode().ID)
		} else if prevLeaderID != sender.ID {
			le.cluster.Logger().
				Field("leaderId", sender.ID.String()).
				Field("term", le.currentTerm).
				Debugf("Leader detected")
			le.eventHandlers.dispatch(LeaderElectedEvent, sender.ID)
		}

	}

	le.lock.Unlock()

	return nil
}

// handleNodeStateChange is called when any node's state changes
func (le *LeaderElection) handleNodeStateChange(node *gossip.Node, prevState gossip.NodeState) {
	le.cluster.Logger().
		Field("nodeId", node.ID.String()).
		Field("prevState", prevState.String()).
		Field("newState", node.GetState().String()).
		Debugf("Node state changed")

	le.lock.RLock()
	isCurrentLeader := le.hasLeader && (node.ID == le.leaderID)
	leaderID := le.leaderID
	le.lock.RUnlock()

	// If the current leader has failed, trigger a new election
	if isCurrentLeader && node.GetState() != gossip.NodeAlive {
		le.cluster.Logger().
			Field("leaderId", node.ID.String()).
			Field("currentTerm", le.currentTerm).
			Debugf("Leader node is down, will elect new leader")

		le.eventHandlers.dispatch(LeaderLostEvent, leaderID)

		le.lock.Lock()
		le.hasLeader = false // Clear the leader flag to trigger a new election
		le.lock.Unlock()

		le.electLeader()
	}
}
