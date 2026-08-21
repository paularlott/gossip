package leader

import (
	"context"
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

// forgetMessage carries an operator assertion that a node is permanently gone.
type forgetMessage struct {
	NodeID gossip.NodeID `msgpack:"nid" json:"nid"`
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
	nodeGroup     *gossip.NodeGroup
	stateHandler  gossip.HandlerID
	baseline      *baselineTracker
	startStopMu   sync.Mutex
	wg            sync.WaitGroup
	started       bool
	stopped       bool

	// watchers receive a callback on every leadership mutation — including
	// transitions the four public events do not cover, such as a term
	// advancing while the same node stays leader, or a leader silently
	// becoming ineligible. Consumers that must react to every change (the
	// lock pool's recovery, for instance) subscribe here instead of polling.
	watchersMu    sync.Mutex
	watchers      []watcherReg
	nextWatcherID uint64
}

// LeadershipWatcherFn is notified with a consistent snapshot whenever the
// local node's leadership state changes. It is invoked after the election's
// lock is released, so it may call back into the election freely — but it runs
// on the goroutine that observed the change (the election loop or an inbound
// heartbeat handler), so it must not block.
//
// It is an alias so that implementations satisfy interfaces requiring the
// plain function type.
type LeadershipWatcherFn = func(isLeader bool, term uint64)

// watcherReg pairs a watcher with an identity, since function values cannot be
// compared for removal.
type watcherReg struct {
	id uint64
	fn LeadershipWatcherFn
}

// WatchLeadership registers fn to be called on every leadership change, and
// immediately invokes it once with the current state so callers can dispense
// with a separate initial poll.
//
// The returned cancel unregisters fn and must be called before the consumer is
// discarded; without it the election retains the consumer for its own lifetime.
func (le *LeaderElection) WatchLeadership(fn LeadershipWatcherFn) (cancel func()) {
	le.watchersMu.Lock()
	le.nextWatcherID++
	id := le.nextWatcherID
	le.watchers = append(le.watchers, watcherReg{id: id, fn: fn})
	le.watchersMu.Unlock()

	le.lock.RLock()
	is, term := le.isLeader, le.currentTerm
	le.lock.RUnlock()
	fn(is, term)

	var once sync.Once
	return func() {
		once.Do(func() {
			le.watchersMu.Lock()
			for i, r := range le.watchers {
				if r.id == id {
					le.watchers = append(le.watchers[:i], le.watchers[i+1:]...)
					break
				}
			}
			le.watchersMu.Unlock()
		})
	}
}

// notifyLeadershipWatchers snapshots the current state and invokes every
// watcher. Callers must not hold le.lock.
func (le *LeaderElection) notifyLeadershipWatchers() {
	le.lock.RLock()
	is, term := le.isLeader, le.currentTerm
	le.lock.RUnlock()

	le.watchersMu.Lock()
	regs := make([]LeadershipWatcherFn, 0, len(le.watchers))
	for _, r := range le.watchers {
		regs = append(regs, r.fn)
	}
	le.watchersMu.Unlock()

	for _, fn := range regs {
		fn(is, term)
	}
}

// applyState mutates leadership state under the election lock and notifies
// watchers when fn reports a change. Every mutation of the watcher-visible
// fields — isLeader, currentTerm, hasLeader, leaderID — goes through here,
// which makes watcher notification structural: no call site can change
// leadership state without the notification following. fn must not dispatch
// events or otherwise re-enter the election; it runs under the writer lock.
// Public events are the caller's concern and are dispatched after applyState
// returns.
func (le *LeaderElection) applyState(fn func() bool) bool {
	le.lock.Lock()
	changed := fn()
	le.lock.Unlock()
	if changed {
		le.notifyLeadershipWatchers()
	}
	return changed
}

// NewLeaderElection creates a new leader election manager
func NewLeaderElection(cluster *gossip.Cluster, config *Config) *LeaderElection {
	config = normalizeConfig(config)

	ctx, cancel := context.WithCancel(context.Background())

	// The stability period must outlast failure detection, otherwise a count
	// sampled mid-transition gets latched as the cluster's real size.
	stability := config.StabilityPeriod
	if stability <= 0 {
		stability = 2 * cluster.DeadNodeTimeout()
		if stability <= 0 {
			stability = 30 * time.Second
		}
	}

	// The shrink dwell is deliberately much longer than the stability period: a
	// transient partition must have healed before the baseline follows the
	// observed count downward.
	shrinkDwell := config.ShrinkDwell
	if shrinkDwell <= 0 {
		shrinkDwell = 4 * cluster.DeadNodeTimeout()
		if shrinkDwell <= 0 {
			shrinkDwell = 60 * time.Second
		}
	}

	election := &LeaderElection{
		cluster:       cluster,
		config:        config,
		hasLeader:     false,
		currentTerm:   0,
		ctx:           ctx,
		cancel:        cancel,
		eventHandlers: newLeaderEventHandlers(cluster.Logger()),
		baseline:      newBaselineTracker(stability, shrinkDwell, !config.AutoShrinkDisabled, cluster),
	}

	// The node group must exist before any handler is registered. Handlers run on
	// the cluster's inbound goroutines, and both the heartbeat and state-change
	// paths read election.nodeGroup — registering first leaves a window where an
	// arriving heartbeat races that assignment and, worse, sees a nil group and
	// skips the eligibility filter entirely.
	if len(config.MetadataCriteria) > 0 {
		election.nodeGroup = gossip.NewNodeGroup(cluster, config.MetadataCriteria, nil)
	}

	// Register event listeners
	election.stateHandler = cluster.HandleNodeStateChangeFunc(election.handleNodeStateChange)
	if err := cluster.HandleFunc(config.HeartbeatMessageType, election.handleLeaderHeartbeat); err != nil {
		panic(err)
	}
	// A failure here means the reserved message type is occupied by something
	// other than library code — either an application violating the reserved
	// range (applications start from UserMsg) or a library bug. Fail fast
	// rather than run a half-configured election.
	if err := cluster.HandleFunc(config.ForgetMessageType, election.handleForget); err != nil {
		panic(err)
	}

	return election
}

func normalizeConfig(config *Config) *Config {
	defaults := DefaultConfig()
	if config == nil {
		return defaults
	}

	normalized := *config

	if normalized.LeaderCheckInterval <= 0 {
		normalized.LeaderCheckInterval = defaults.LeaderCheckInterval
	}
	if normalized.LeaderTimeout <= 0 {
		normalized.LeaderTimeout = defaults.LeaderTimeout
	}
	if normalized.LeaderTimeout < normalized.LeaderCheckInterval {
		normalized.LeaderTimeout = normalized.LeaderCheckInterval
	}
	if normalized.HeartbeatMessageType < gossip.ReservedMsgsStart {
		normalized.HeartbeatMessageType = defaults.HeartbeatMessageType
	}
	if normalized.ForgetMessageType < gossip.ReservedMsgsStart {
		normalized.ForgetMessageType = defaults.ForgetMessageType
	}
	if normalized.ForgetMessageType == normalized.HeartbeatMessageType {
		normalized.ForgetMessageType = normalized.HeartbeatMessageType + 1
	}

	return &normalized
}

func (le *LeaderElection) HandleEventFunc(eventType EventType, handler LeaderEventHandler) {
	le.eventHandlers.add(eventType, handler)
}

// Start the election check process
func (le *LeaderElection) Start() {
	le.startStopMu.Lock()
	defer le.startStopMu.Unlock()

	if le.started || le.stopped {
		return
	}

	le.started = true

	// Kick off initial election
	le.checkAndElectLeader()

	// Start periodic checks in a goroutine
	le.wg.Add(1)
	go le.runElectionLoop()
}

// runElectionLoop runs the periodic election checks
func (le *LeaderElection) runElectionLoop() {
	defer le.wg.Done()
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
}

// Stop terminates the leader election process
func (le *LeaderElection) Stop() {
	le.startStopMu.Lock()
	if le.stopped {
		le.startStopMu.Unlock()
		return
	}
	le.stopped = true
	le.startStopMu.Unlock()

	le.cancel()
	le.wg.Wait()
	if le.nodeGroup != nil {
		le.nodeGroup.Close()
	}
	le.cluster.RemoveNodeStateChangeHandler(le.stateHandler)
	le.cluster.UnregisterMessageType(le.config.HeartbeatMessageType)
	le.cluster.UnregisterMessageType(le.config.ForgetMessageType)
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
	// Keep the quorum baseline current before any quorum decision is taken.
	if le.baseline != nil {
		le.baseline.observe(le.getEligibleNodes(), time.Now())
	}

	// If metadata filtering is enabled and local node is not eligible, don't participate
	if le.nodeGroup != nil && !le.nodeGroup.Contains(le.cluster.LocalNode().ID) {
		// Clear any leader state since we're not eligible to participate,
		// however maintain the leader information
		stepped := le.applyState(func() bool {
			if !le.isLeader {
				return false
			}
			le.isLeader = false
			return true
		})

		if stepped {
			le.eventHandlers.dispatch(SteppedDownEvent, le.cluster.LocalNode().ID)
		}
		return
	}

	// First, check if there's already a valid leader
	if le.HasLeader() {
		// If we are the leader then send a heartbeat
		if le.IsLeader() {
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

	// Cache eligible nodes to avoid multiple calls
	var eligibleNodes []*gossip.Node
	var isParticipating bool

	if le.nodeGroup != nil {
		isParticipating = le.nodeGroup.Contains(le.cluster.LocalNode().ID)
		eligibleNodes = le.nodeGroup.GetNodes(nil)
	} else {
		eligibleNodes = le.cluster.AliveNodes()
		isParticipating = true
	}

	requiredQuorum := le.calculateQuorumForNodes(len(eligibleNodes))
	numEligible := len(eligibleNodes)

	if numEligible < requiredQuorum {
		// Log loss of quorum if desired
		le.cluster.Logger().
			With("eligibleNodes", numEligible).
			With("requiredQuorum", requiredQuorum).
			With("participating", isParticipating).
			Warn("Quorum lost among eligible nodes")
		return false // Not enough nodes for quorum
	}

	// If the last heartbeat is too old, the leader is not valid anymore
	if time.Since(le.lastHeartbeat) > le.config.LeaderTimeout {
		return false
	}

	// Check if the leader node still exists and is eligible
	leader := le.cluster.GetNode(le.leaderID)
	if leader == nil || leader.GetObservedState() != gossip.NodeAlive {
		return false
	}

	// If metadata filtering is enabled, check if current leader is still eligible
	// This applies to both participating and non-participating nodes
	if le.nodeGroup != nil {
		if !le.nodeGroup.Contains(leader.ID) {
			le.cluster.Logger().
				With("leaderId", le.leaderID).
				Debug("Current leader no longer eligible due to metadata mismatch")
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
			With("eligibleNodes", numEligible).
			With("requiredQuorum", requiredQuorum).
			Debug("Quorum not met, cannot elect leader")

		// Optional: If we previously had a leader but lost quorum, clear the leader state.
		var hadLeader, wasLeader bool
		var lostID gossip.NodeID
		le.applyState(func() bool {
			hadLeader = le.hasLeader
			wasLeader = le.isLeader
			lostID = le.leaderID
			if !hadLeader {
				return false
			}
			le.cluster.Logger().Warn("lost leader due to lack of quorum", "leader_id", lostID)
			le.hasLeader = false
			le.isLeader = false
			return true
		})

		if hadLeader {
			if wasLeader {
				le.eventHandlers.dispatch(SteppedDownEvent, le.cluster.LocalNode().ID)
			}
			le.eventHandlers.dispatch(LeaderLostEvent, lostID)
		}
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
		le.cluster.Logger().Error("no candidate node found despite meeting quorum")
		return
	}

	localNode := le.cluster.LocalNode()

	var wasLeader, hadLeader bool
	var prevLeaderID gossip.NodeID
	le.applyState(func() bool {
		wasLeader = le.isLeader
		prevLeaderID = le.leaderID
		hadLeader = le.hasLeader

		le.currentTerm++

		le.leaderID = candidateNode.ID
		le.hasLeader = true
		le.lastHeartbeat = time.Now()
		le.leaderTime = le.lastHeartbeat
		le.isLeader = (candidateNode.ID == localNode.ID)
		return true
	})

	le.cluster.Logger().
		With("leaderId", candidateNode.ID.String()).
		With("term", le.currentTerm).
		With("isLocal", le.IsLeader()).
		Debug("New leader elected", "quorum_eligible", numEligible, "quorum_required", requiredQuorum)

	// Dispatch events based on state changes
	leaderChanged := !hadLeader || prevLeaderID != candidateNode.ID
	becameLeader := !wasLeader && le.IsLeader()
	steppedDown := wasLeader && !le.IsLeader()

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
	if le.IsLeader() {
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
	if err := le.cluster.Send(le.config.HeartbeatMessageType, &msg); err != nil {
		le.cluster.Logger().WithError(err).Warn("failed to send leader heartbeat")
		return
	}

	le.lock.Lock()
	le.leaderTime = leaderTime
	le.lastHeartbeat = time.Now()
	le.lock.Unlock()
}

// handleLeaderHeartbeat is called to process incoming heartbeat messages
func (le *LeaderElection) handleLeaderHeartbeat(sender *gossip.Node, packet *gossip.Packet) error {
	select {
	case <-le.ctx.Done():
		return nil
	default:
	}

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
		le.cluster.Logger().Error("failed to unmarshal heartbeat message", "error", err)
		return err
	}

	var wasLeader, hadLeader bool
	var prevLeaderID gossip.NodeID
	var localNodeID gossip.NodeID

	accepted := le.applyState(func() bool {
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
				With("senderId", sender.ID.String()).
				With("senderTerm", msg.Term).
				With("currentTerm", le.currentTerm).
				Debug("Accepting heartbeat due to higher term")
		} else if msg.Term == le.currentTerm {
			if !le.hasLeader {
				acceptHeartbeat = true
				le.cluster.Logger().
					With("senderId", sender.ID.String()).
					With("term", msg.Term).
					Debug("Accepting heartbeat as we have no current leader")
			} else if msg.LeaderTime.After(le.leaderTime) {
				acceptHeartbeat = true
			} else if msg.LeaderTime.Equal(le.leaderTime) && sender.ID.String() < le.leaderID.String() {
				acceptHeartbeat = true
				le.cluster.Logger().
					With("senderId", sender.ID.String()).
					With("leaderId", le.leaderID.String()).
					With("term", msg.Term).
					Debug("Accepting heartbeat due to tie-breaker (lower ID)")
			}
		}

		if !acceptHeartbeat {
			return false
		}

		wasLeader = le.isLeader
		prevLeaderID = le.leaderID
		hadLeader = le.hasLeader

		le.leaderID = sender.ID
		le.hasLeader = true
		le.leaderTime = msg.LeaderTime
		le.lastHeartbeat = time.Now() // Update last heartbeat time based on receipt time
		le.currentTerm = msg.Term
		localNodeID = le.cluster.LocalNode().ID
		le.isLeader = (sender.ID == localNodeID)
		return true
	})

	if !accepted {
		return nil
	}

	// Events run outside the lock — a handler may call back into the election
	// (IsLeader and friends), which would deadlock under our writer lock.
	leaderChanged := !hadLeader || prevLeaderID != sender.ID
	becameLeader := !wasLeader && le.isLeader
	steppedDown := wasLeader && !le.isLeader

	if steppedDown {
		le.cluster.Logger().Debug("stepping down as leader due to heartbeat", "sender_id", sender.ID)
		le.eventHandlers.dispatch(SteppedDownEvent, localNodeID)
	}
	if becameLeader {
		le.cluster.Logger().Warn("became leader unexpectedly via heartbeat from self")
		le.eventHandlers.dispatch(BecameLeaderEvent, localNodeID)
	}

	if leaderChanged {
		le.cluster.Logger().
			Debug("leader updated via heartbeat", "leaderId", sender.ID.String(), "term", le.currentTerm)
		le.eventHandlers.dispatch(LeaderElectedEvent, sender.ID)
	}

	return nil
}

// handleNodeStateChange is called when any node's state changes
func (le *LeaderElection) handleNodeStateChange(node *gossip.Node, prevState gossip.NodeState) {
	select {
	case <-le.ctx.Done():
		return
	default:
	}

	le.cluster.Logger().
		With("nodeId", node.ID.String()).
		With("prevState", prevState.String()).
		With("newState", node.GetObservedState().String()).
		Debug("Node state changed")

	// A node that announced its departure is a positive signal from outside the
	// failure domain, so it is safe to discount from the quorum baseline. A
	// crash or partition produces no such announcement and is deliberately not
	// discounted, keeping quorum conservative.
	if node.GetObservedState() == gossip.NodeLeaving && le.baseline != nil &&
		le.wasEligibleForBaseline(node, prevState) {
		if le.baseline.noteGracefulDeparture(node.ID) {
			le.cluster.Logger().Info("node left gracefully; quorum baseline reduced",
				"node_id", node.ID.String(),
				"baseline", le.baseline.size())
		}
	}

	le.lock.RLock()
	isCurrentLeader := le.hasLeader && (node.ID == le.leaderID)
	currentLeaderID := le.leaderID
	le.lock.RUnlock()

	// If the current leader has failed...
	if isCurrentLeader && node.GetObservedState() != gossip.NodeAlive {
		le.cluster.Logger().
			With("leaderId", node.ID.String()).
			With("currentTerm", le.currentTerm).
			Warn("Leader node is down, clearing leader state")

		le.eventHandlers.dispatch(LeaderLostEvent, currentLeaderID)

		le.applyState(func() bool {
			if !le.hasLeader || le.leaderID != node.ID {
				return false
			}
			le.hasLeader = false
			le.isLeader = false
			return true
		})
	}
}

// wasEligibleForBaseline reports whether a node that has just transitioned away
// was, immediately before the transition, part of this election's eligible set.
//
// prevState is the state the node held a moment before the change; Alive and
// Suspect are the states eligibility is drawn from (node groups track both, and
// a Suspect node was Alive until shortly before). When the election is scoped by
// MetadataCriteria the node's metadata is matched directly against them — the
// group itself cannot be asked here, because its state-change handler runs
// before this one and has already removed the departing node from the group.
func (le *LeaderElection) wasEligibleForBaseline(node *gossip.Node, prevState gossip.NodeState) bool {
	if prevState != gossip.NodeAlive && prevState != gossip.NodeSuspect {
		return false
	}
	if len(le.config.MetadataCriteria) == 0 {
		return true
	}
	return gossip.NodeMatchesCriteria(node, le.config.MetadataCriteria)
}

// calculateQuorumForNodes returns the minimum number of eligible nodes required
// to elect or retain a leader, given how many are currently observed.
//
// Two ingredients:
//
//   - A strict majority of the observed count. This term varies per node, so on
//     its own it cannot prevent split brain: two sides of a partition each
//     compute a majority of their own view and both pass.
//
//   - MinClusterSize, a constant floor. Because it does not depend on the local
//     view it is a lower bound on every node's threshold, so both sides of a
//     split can only qualify if N >= 2*MinClusterSize. Keeping the floor above
//     half the cluster makes two simultaneous leaders impossible.
//
// The result is the larger of the two. The varying term only ever adds
// strictness, which is always safe; the floor is what supplies the guarantee.
func (le *LeaderElection) calculateQuorumForNodes(numNodes int) int {
	required := 0

	// Never accept half or fewer of what we can see.
	if numNodes > 0 {
		required = numNodes/2 + 1
	}

	// The adaptive baseline: a majority of the cluster size we have settled on.
	// This is what removes the need to re-tune MinClusterSize as the cluster
	// grows, and it only ever shrinks on evidence of a deliberate departure.
	if le.baseline != nil {
		if bl := le.baseline.size(); bl > 0 {
			if majority := bl/2 + 1; majority > required {
				required = majority
			}
		}
	}

	if le.config.MinClusterSize > required {
		required = le.config.MinClusterSize
	}

	return required
}

// BaselineSize returns the cluster size quorum is currently measured against.
// Exposed for diagnostics: when leadership cannot be established, this is
// usually the number to look at first.
func (le *LeaderElection) BaselineSize() int {
	if le.baseline == nil {
		return 0
	}
	return le.baseline.size()
}

// QuorumSize returns the number of eligible nodes currently required to elect or
// retain a leader.
func (le *LeaderElection) QuorumSize() int {
	return le.calculateQuorumForNodes(len(le.getEligibleNodes()))
}

// ForgetNode discounts a node from the quorum baseline across the whole cluster
// and drops it from the node list.
//
// A crashed node cannot be distinguished from a partitioned one, so the cluster
// deliberately keeps counting it and quorum stays conservative. This is the
// operator's way of asserting, from outside the cluster, that a node is
// permanently gone — which allows quorum to shrink safely.
//
// The assertion is broadcast, so a single call on any one node reaches them all.
// That matters at scale: reducing a 100-node cluster should not mean visiting 100
// nodes. Delivery is best-effort gossip, so a node that misses it simply keeps
// the higher baseline — the strict, safe direction — until it is told again or
// the node ages out of its list.
//
// Returns true if the local node discounted it. Use ForgetNodeLocal to apply the
// change only here. In either form, only nodes this cluster knows about can be
// forgotten: unknown IDs are rejected rather than decremented.
func (le *LeaderElection) ForgetNode(id gossip.NodeID) bool {
	applied := le.ForgetNodeLocal(id)

	// Tell everyone else. Best-effort: quorum only ever errs strict if this is
	// missed.
	if err := le.cluster.Send(le.config.ForgetMessageType, &forgetMessage{NodeID: id}); err != nil {
		le.cluster.Logger().WithError(err).Warn("failed to broadcast node forget",
			"node_id", id.String())
	}

	return applied
}

// ForgetNodeLocal applies a forget to this node only, without broadcasting.
//
// Only nodes this node already knows about can be forgotten: a cluster member
// asserting an ID that was never seen must not be able to spend baseline
// decrements, since each one lowers quorum. Unknown IDs are rejected outright,
// whether the call originates locally or from a peer's broadcast.
func (le *LeaderElection) ForgetNodeLocal(id gossip.NodeID) bool {
	if le.baseline == nil {
		return false
	}
	if id == le.cluster.LocalNode().ID {
		return false // a node cannot forget itself
	}
	if le.cluster.GetNode(id) == nil {
		return false // unknown here — nothing to forget, no decrement
	}

	discounted := le.baseline.forget(id)
	le.cluster.ForgetNode(id)

	if discounted {
		le.cluster.Logger().Info("node forgotten; quorum baseline reduced",
			"node_id", id.String(),
			"baseline", le.baseline.size(),
			"quorum", le.QuorumSize())
	}
	return discounted
}

// handleForget applies a forget assertion received from a peer.
func (le *LeaderElection) handleForget(sender *gossip.Node, packet *gossip.Packet) error {
	var msg forgetMessage
	if err := packet.Unmarshal(&msg); err != nil {
		return err
	}

	// Applied locally only; the broadcast is already being relayed by the
	// cluster's own forwarding, so re-broadcasting here would amplify it.
	le.ForgetNodeLocal(msg.NodeID)
	return nil
}

// ResetQuorumBaseline discards the tracked baseline so it is re-derived from
// what is currently visible. Use after a deliberate resize when the accumulated
// baseline no longer reflects reality.
func (le *LeaderElection) ResetQuorumBaseline() {
	if le.baseline != nil {
		le.baseline.reset()
	}
}

// Candidates returns the nodes currently eligible to become leader.
//
// When the election is scoped by MetadataCriteria this is the matching node
// group; otherwise it is all alive nodes. Useful for predicting a successor
// during a planned handover.
func (le *LeaderElection) Candidates() []*gossip.Node {
	return le.getEligibleNodes()
}

// Term returns the current election term. The term increments on every
// leadership change, giving a strictly increasing, clock-independent ordering
// that callers can use for fencing.
func (le *LeaderElection) Term() uint64 {
	le.lock.RLock()
	defer le.lock.RUnlock()
	return le.currentTerm
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
