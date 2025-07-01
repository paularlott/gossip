package leader

import (
	"context"
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

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
	nodeGroup     *gossip.NodeGroup
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

	if len(config.MetadataCriteria) > 0 {
		election.nodeGroup = gossip.NewNodeGroup(cluster, config.MetadataCriteria, nil)
	}

	return election
}

func (le *LeaderElection) HandleEventFunc(eventType EventType, handler LeaderEventHandler) {
	le.eventHandlers.add(eventType, handler)
}

// Start the election check process
func (le *LeaderElection) Start() {
	go func() {
		// Kick off election
		le.checkAndElectLeader()

		// Start periodic checks and elections
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
	if le.nodeGroup != nil {
		le.nodeGroup.Close()
	}
}

// getEligibleNodes returns nodes that are eligible for leader election
// If metadata filtering is enabled, only nodes with matching metadata are considered
func (le *LeaderElection) getEligibleNodes() []*gossip.Node {
	if le.nodeGroup != nil {
		return le.nodeGroup.GetNodes(nil)
	}

	return le.cluster.AliveNodes()
}

// checkAndElectLeader checks if we need to elect a new leader and does so if necessary
func (le *LeaderElection) checkAndElectLeader() {
	// If metadata filtering is enabled and local node is not eligible, don't participate
	if le.nodeGroup != nil && !le.nodeGroup.Contains(le.cluster.LocalNode().ID) {
		// Clear any leader state since we're not eligible to participate,
		// however maintain the leader information
		le.lock.Lock()
		if le.isLeader {
			le.isLeader = false
		}
		le.lock.Unlock()
		return
	}

	// First, check if there's already a valid leader
	if le.HasLeader() {
		// If we are the leader then send a heartbeat
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

func (le *LeaderElection) GetLeader() *gossip.Node {
	if !le.HasLeader() {
		return nil // No leader currently
	}

	le.lock.RLock()
	defer le.lock.RUnlock()
	return le.cluster.GetNode(le.leaderID)
}

// hasLeader checks if there's already a leader that has sent a heartbeat recently
func (le *LeaderElection) HasLeader() bool {
	le.lock.RLock()
	defer le.lock.RUnlock()

	// If we don't have a leader yet, no valid leader
	if !le.hasLeader {
		return false
	}

	// For non-participating nodes, use all alive nodes for quorum calculation
	// For participating nodes, use eligible nodes
	var eligibleNodes []*gossip.Node
	var isParticipating bool

	if le.nodeGroup != nil {
		isParticipating = le.nodeGroup.Contains(le.cluster.LocalNode().ID)
		if isParticipating {
			eligibleNodes = le.getEligibleNodes()
		} else {
			// Non-participating nodes should use the eligible nodes for quorum calculation
			// but they don't participate in the election themselves
			eligibleNodes = le.nodeGroup.GetNodes(nil)
		}
	} else {
		eligibleNodes = le.cluster.AliveNodes()
		isParticipating = true
	}

	requiredQuorum := le.calculateQuorumForNodes(len(eligibleNodes))
	numEligible := len(eligibleNodes)

	if numEligible < requiredQuorum {
		// Log loss of quorum if desired
		le.cluster.Logger().
			Field("eligibleNodes", numEligible).
			Field("requiredQuorum", requiredQuorum).
			Field("participating", isParticipating).
			Warnf("Quorum lost among eligible nodes")
		return false // Not enough nodes for quorum
	}

	// If the last heartbeat is too old, the leader is not valid anymore
	if time.Since(le.lastHeartbeat) > le.config.LeaderTimeout {
		return false
	}

	// Check if the leader node still exists and is eligible
	leader := le.cluster.GetNode(le.leaderID)
	if leader == nil || leader.GetState() != gossip.NodeAlive {
		return false
	}

	// If metadata filtering is enabled, check if current leader is still eligible
	// This applies to both participating and non-participating nodes
	if le.nodeGroup != nil {
		if !le.nodeGroup.Contains(leader.ID) {
			le.cluster.Logger().
				Field("leaderId", le.leaderID).
				Debugf("Current leader no longer eligible due to metadata mismatch")
			return false
		}
	}

	return true
}

// electLeader chooses a new leader from the alive nodes if quorum is met
func (le *LeaderElection) electLeader() {
	// Get eligible nodes based on metadata filtering
	eligibleNodes := le.getEligibleNodes()
	numEligible := len(eligibleNodes)

	// Check for quorum among eligible nodes
	requiredQuorum := le.calculateQuorumForNodes(numEligible)

	if numEligible < requiredQuorum {
		le.cluster.Logger().
			Field("eligibleNodes", numEligible).
			Field("requiredQuorum", requiredQuorum).
			Debugf("Quorum not met, cannot elect leader")

		// Optional: If we previously had a leader but lost quorum, clear the leader state.
		le.lock.Lock()
		if le.hasLeader {
			le.cluster.Logger().Warnf("Lost leader %s due to lack of quorum", le.leaderID)
			if le.isLeader {
				le.eventHandlers.dispatch(SteppedDownEvent, le.cluster.LocalNode().ID)
			}
			le.eventHandlers.dispatch(LeaderLostEvent, le.leaderID)
			le.hasLeader = false
			le.isLeader = false
		}
		le.lock.Unlock()
		return
	}

	// Quorum met, proceed with election
	// Simple leader election strategy: use the node with the "lowest" ID
	var candidateNode *gossip.Node
	for _, node := range eligibleNodes {
		if candidateNode == nil || node.ID.String() < candidateNode.ID.String() {
			candidateNode = node
		}
	}
	if candidateNode == nil {
		le.cluster.Logger().Errorf("No candidate node found despite meeting quorum")
		return
	}

	localNode := le.cluster.LocalNode()

	le.lock.Lock()
	wasLeader := le.isLeader
	prevLeaderID := le.leaderID
	hadLeader := le.hasLeader

	le.currentTerm++

	le.leaderID = candidateNode.ID
	le.hasLeader = true
	le.lastHeartbeat = time.Now()
	le.leaderTime = le.lastHeartbeat
	le.isLeader = (candidateNode.ID == localNode.ID)
	le.lock.Unlock()

	le.cluster.Logger().
		Field("leaderId", candidateNode.ID.String()).
		Field("term", le.currentTerm).
		Field("isLocal", le.isLeader).
		Debugf("New leader elected (quorum: %d/%d)", numEligible, requiredQuorum)

	// Dispatch events based on state changes
	leaderChanged := !hadLeader || prevLeaderID != candidateNode.ID
	becameLeader := !wasLeader && le.isLeader
	steppedDown := wasLeader && !le.isLeader

	if steppedDown {
		le.eventHandlers.dispatch(SteppedDownEvent, localNode.ID)
	}
	if becameLeader {
		le.eventHandlers.dispatch(BecameLeaderEvent, localNode.ID)
	}

	// Dispatch LeaderElectedEvent if the leader actually changed or if we didn't have one before
	if leaderChanged {
		le.eventHandlers.dispatch(LeaderElectedEvent, candidateNode.ID)
	}

	// If we're the leader, announce ourselves immediately
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

	// If metadata filtering is enabled, only accept heartbeats from eligible nodes
	if le.nodeGroup != nil && !le.nodeGroup.Contains(sender.ID) {
		return nil
	}

	// Handle the heartbeat message
	var msg heartbeatMessage
	if err := packet.Unmarshal(&msg); err != nil {
		le.cluster.Logger().Errorf("Failed to unmarshal heartbeat message: %v", err)
		return err
	}

	le.lock.Lock()
	defer le.lock.Unlock() // Use defer for cleaner exit paths

	// Priority order for deciding leadership:
	// 1. Higher term always wins
	// 2. Within the same term:
	//    a. If we have no leader, accept this one
	//    b. If timestamp is newer, accept this one
	//    c. If timestamps are equal, use lexicographical node ID as tiebreaker
	acceptHeartbeat := false
	if msg.Term > le.currentTerm {
		acceptHeartbeat = true
		le.cluster.Logger().
			Field("senderId", sender.ID.String()).
			Field("senderTerm", msg.Term).
			Field("currentTerm", le.currentTerm).
			Debugf("Accepting heartbeat due to higher term")
	} else if msg.Term == le.currentTerm {
		if !le.hasLeader {
			acceptHeartbeat = true
			le.cluster.Logger().
				Field("senderId", sender.ID.String()).
				Field("term", msg.Term).
				Debugf("Accepting heartbeat as we have no current leader")
		} else if msg.LeaderTime.After(le.leaderTime) {
			acceptHeartbeat = true
		} else if msg.LeaderTime.Equal(le.leaderTime) && sender.ID.String() < le.leaderID.String() {
			acceptHeartbeat = true
			le.cluster.Logger().
				Field("senderId", sender.ID.String()).
				Field("leaderId", le.leaderID.String()).
				Field("term", msg.Term).
				Debugf("Accepting heartbeat due to tie-breaker (lower ID)")
		}
	}

	if acceptHeartbeat {
		wasLeader := le.isLeader
		prevLeaderID := le.leaderID
		hadLeader := le.hasLeader

		le.leaderID = sender.ID
		le.hasLeader = true
		le.leaderTime = msg.LeaderTime
		le.lastHeartbeat = time.Now() // Update last heartbeat time based on receipt time
		le.currentTerm = msg.Term
		le.isLeader = (sender.ID == le.cluster.LocalNode().ID)

		leaderChanged := !hadLeader || prevLeaderID != sender.ID
		becameLeader := !wasLeader && le.isLeader
		steppedDown := wasLeader && !le.isLeader

		// Log state changes and dispatch events
		if steppedDown {
			le.cluster.Logger().Debugf("Stepping down as leader due to heartbeat from %s", sender.ID)
			le.eventHandlers.dispatch(SteppedDownEvent, le.cluster.LocalNode().ID)
		}
		if becameLeader {
			le.cluster.Logger().Warnf("Became leader unexpectedly via heartbeat from self?")
			le.eventHandlers.dispatch(BecameLeaderEvent, le.cluster.LocalNode().ID)
		}

		if leaderChanged {
			le.cluster.Logger().
				Field("leaderId", sender.ID.String()).
				Field("term", le.currentTerm).
				Debugf("Leader updated via heartbeat")
			le.eventHandlers.dispatch(LeaderElectedEvent, sender.ID)
		} /*else if le.leaderID == sender.ID {
			le.cluster.Logger().
				Field("senderId", sender.ID.String()).
				Debugf("Heartbeat refresh from leader")
		}*/
	} /*else {
		// Log why the heartbeat was ignored
		le.cluster.Logger().
			Field("senderId", sender.ID.String()).
			Field("senderTerm", msg.Term).
			Field("currentTerm", le.currentTerm).
			Field("senderTime", msg.LeaderTime).
			Field("leaderTime", le.leaderTime).
			Field("leaderId", le.leaderID).
			Debugf("Ignoring stale or lower priority heartbeat")
	}*/

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
	currentLeaderID := le.leaderID
	le.lock.RUnlock()

	// If the current leader has failed...
	if isCurrentLeader && node.GetState() != gossip.NodeAlive {
		le.cluster.Logger().
			Field("leaderId", node.ID.String()).
			Field("currentTerm", le.currentTerm).
			Warnf("Leader node is down, clearing leader state")

		le.eventHandlers.dispatch(LeaderLostEvent, currentLeaderID)

		le.lock.Lock()
		if le.hasLeader && le.leaderID == node.ID {
			le.hasLeader = false
			le.isLeader = false
		}
		le.lock.Unlock()
	}
}

// calculateQuorumForNodes calculates the minimum number of nodes required for quorum
// from a specific set of nodes
func (le *LeaderElection) calculateQuorumForNodes(numNodes int) int {
	if numNodes == 0 {
		return 0 // Cannot have quorum with zero nodes
	}

	// Calculate quorum: (total_nodes * percentage + 99) / 100 for ceiling division.
	requiredQuorum := (numNodes*le.config.QuorumPercentage + 99) / 100

	// Ensure minimum quorum of 1 if percentage is low but nodes exist,
	// unless percentage is explicitly 0 (which might imply no quorum needed).
	if requiredQuorum == 0 && le.config.QuorumPercentage > 0 {
		requiredQuorum = 1
	}

	return requiredQuorum
}

// GetNodeGroup returns the node group used for leader election, if any
func (le *LeaderElection) GetNodeGroup() *gossip.NodeGroup {
	return le.nodeGroup
}

func (le *LeaderElection) SendToPeers(msgType gossip.MessageType, data interface{}) error {
	if le.nodeGroup != nil {
		return le.nodeGroup.SendToPeers(msgType, data)
	}
	return le.cluster.Send(msgType, data)
}

func (le *LeaderElection) SendToPeersReliable(msgType gossip.MessageType, data interface{}) error {
	if le.nodeGroup != nil {
		return le.nodeGroup.SendToPeersReliable(msgType, data)
	}
	return le.cluster.SendReliable(msgType, data)
}
