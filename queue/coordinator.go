package queue

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
)

// pendingMessage is a message waiting to be delivered to a consumer.
type pendingMessage struct {
	messageID     string
	payload       []byte
	replyTo       gossip.NodeID
	correlationID string
	attempts      int
}

// inflightMessage is a message currently being processed by a consumer.
type inflightMessage struct {
	pendingMessage
	deliveryID string
	consumerID gossip.NodeID
	expiresAt  time.Time
}

// consumerState tracks a registered consumer and its outstanding work, so the
// coordinator can push messages without exceeding the consumer's prefetch limit.
type consumerState struct {
	nodeID   gossip.NodeID
	prefetch int
	inflight int
	lastSeen time.Time
}

// hasCapacity reports whether this consumer can accept another message.
func (cs *consumerState) hasCapacity() bool {
	return cs.inflight < cs.prefetch
}

// queueCoordinator manages message state for a queue partition on this node.
//
// Delivery is push-based: the coordinator tracks registered consumers and their
// prefetch capacity, and signals a dispatcher whenever work becomes assignable
// (a message arrives, a consumer registers, or capacity frees up).
type queueCoordinator struct {
	mu       sync.Mutex
	pending  []*pendingMessage
	inflight map[string]*inflightMessage // deliveryID → message

	consumers     map[gossip.NodeID]*consumerState
	consumerOrder []gossip.NodeID // round-robin ordering for fairness
	rrIndex       int

	config  *Config
	closeCh chan struct{}
	closed  bool

	// dispatchSignal is a coalescing wake-up for the Queue's dispatcher.
	// Buffered(1) and never closed.
	dispatchSignal chan struct{}
}

func newQueueCoordinator(config *Config) *queueCoordinator {
	qc := &queueCoordinator{
		pending:        make([]*pendingMessage, 0),
		inflight:       make(map[string]*inflightMessage),
		consumers:      make(map[gossip.NodeID]*consumerState),
		config:         config,
		closeCh:        make(chan struct{}),
		dispatchSignal: make(chan struct{}, 1),
	}
	go qc.maintenanceLoop()
	return qc
}

func (qc *queueCoordinator) close() {
	qc.mu.Lock()
	if !qc.closed {
		qc.closed = true
		close(qc.closeCh)
	}
	qc.mu.Unlock()
}

// signalDispatch wakes the dispatcher. Non-blocking; safe to call while holding
// qc.mu since the channel is buffered and never closed.
func (qc *queueCoordinator) signalDispatch() {
	select {
	case qc.dispatchSignal <- struct{}{}:
	default:
	}
}

// --- Consumer registry ---

// registerConsumer adds or refreshes a consumer registration. Calling it again
// acts as a heartbeat and updates the prefetch limit.
func (qc *queueCoordinator) registerConsumer(nodeID gossip.NodeID, prefetch int) {
	if prefetch <= 0 {
		prefetch = 1
	}

	qc.mu.Lock()
	cs, exists := qc.consumers[nodeID]
	if exists {
		cs.prefetch = prefetch
		cs.lastSeen = time.Now()
	} else {
		qc.consumers[nodeID] = &consumerState{
			nodeID:   nodeID,
			prefetch: prefetch,
			lastSeen: time.Now(),
		}
		qc.consumerOrder = append(qc.consumerOrder, nodeID)
	}
	qc.mu.Unlock()

	// New capacity may be available — try to dispatch
	qc.signalDispatch()
}

// unregisterConsumer removes a consumer and re-queues anything it held.
func (qc *queueCoordinator) unregisterConsumer(nodeID gossip.NodeID) {
	qc.mu.Lock()
	qc.removeConsumerLocked(nodeID)
	qc.requeueByConsumerLocked(nodeID)
	qc.mu.Unlock()

	qc.signalDispatch()
}

// removeConsumerLocked drops a consumer from the registry. Caller holds qc.mu.
func (qc *queueCoordinator) removeConsumerLocked(nodeID gossip.NodeID) {
	if _, exists := qc.consumers[nodeID]; !exists {
		return
	}
	delete(qc.consumers, nodeID)

	for i, id := range qc.consumerOrder {
		if id == nodeID {
			qc.consumerOrder = append(qc.consumerOrder[:i], qc.consumerOrder[i+1:]...)
			break
		}
	}
	if qc.rrIndex >= len(qc.consumerOrder) {
		qc.rrIndex = 0
	}
}

// consumerCount returns the number of registered consumers.
func (qc *queueCoordinator) consumerCount() int {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	return len(qc.consumers)
}

// decConsumerInflightLocked decrements a consumer's outstanding count.
// Caller holds qc.mu.
func (qc *queueCoordinator) decConsumerInflightLocked(nodeID gossip.NodeID) {
	if cs, exists := qc.consumers[nodeID]; exists && cs.inflight > 0 {
		cs.inflight--
	}
}

// --- Enqueue / dispatch ---

// enqueue adds a message to the pending queue. Returns false if queue is full.
func (qc *queueCoordinator) enqueue(msg *pendingMessage) bool {
	qc.mu.Lock()
	if qc.config.MaxSize > 0 && len(qc.pending) >= qc.config.MaxSize {
		qc.mu.Unlock()
		return false
	}
	qc.pending = append(qc.pending, msg)
	qc.mu.Unlock()

	qc.signalDispatch()
	return true
}

// reserveForDispatch atomically pairs the oldest pending message with a
// registered consumer that has spare prefetch capacity, marks it inflight, and
// returns it ready to be pushed. Returns nil when there is nothing assignable.
//
// Consumers are selected round-robin so work spreads evenly.
func (qc *queueCoordinator) reserveForDispatch() *inflightMessage {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	if len(qc.pending) == 0 || len(qc.consumerOrder) == 0 {
		return nil
	}

	// Find the next consumer with capacity, starting from rrIndex
	var chosen *consumerState
	for i := 0; i < len(qc.consumerOrder); i++ {
		idx := (qc.rrIndex + i) % len(qc.consumerOrder)
		cs, exists := qc.consumers[qc.consumerOrder[idx]]
		if exists && cs.hasCapacity() {
			chosen = cs
			qc.rrIndex = (idx + 1) % len(qc.consumerOrder)
			break
		}
	}
	if chosen == nil {
		return nil // every consumer is at its prefetch limit
	}

	msg := qc.pending[0]
	qc.pending = qc.pending[1:]

	inflight := &inflightMessage{
		pendingMessage: *msg,
		deliveryID:     uuid.New().String(),
		consumerID:     chosen.nodeID,
		expiresAt:      time.Now().Add(qc.config.VisibilityTimeout),
	}
	inflight.attempts++

	qc.inflight[inflight.deliveryID] = inflight
	chosen.inflight++

	return inflight
}

// releaseReservation returns a reserved message to the front of the pending
// queue. Used when the push to the consumer fails, so ordering is preserved and
// the attempt is not counted against the message.
func (qc *queueCoordinator) releaseReservation(deliveryID string) {
	qc.mu.Lock()

	msg, exists := qc.inflight[deliveryID]
	if !exists {
		qc.mu.Unlock()
		return
	}
	delete(qc.inflight, deliveryID)
	qc.decConsumerInflightLocked(msg.consumerID)

	// The push never landed, so don't hold this attempt against the message.
	restored := msg.pendingMessage
	if restored.attempts > 0 {
		restored.attempts--
	}

	qc.pending = append([]*pendingMessage{&restored}, qc.pending...)
	qc.mu.Unlock()

	qc.signalDispatch()
}

// ack confirms successful processing. Returns false if deliveryID not found.
func (qc *queueCoordinator) ack(deliveryID string) bool {
	qc.mu.Lock()

	msg, exists := qc.inflight[deliveryID]
	if !exists {
		qc.mu.Unlock()
		return false
	}

	delete(qc.inflight, deliveryID)
	qc.decConsumerInflightLocked(msg.consumerID)
	qc.mu.Unlock()

	// Capacity freed — another message may now be dispatchable
	qc.signalDispatch()
	return true
}

// nack explicitly re-queues a message for redelivery. Returns false if
// deliveryID not found. Dead-letters the message if it has exhausted MaxRetries.
func (qc *queueCoordinator) nack(deliveryID string) bool {
	qc.mu.Lock()

	msg, exists := qc.inflight[deliveryID]
	if !exists {
		qc.mu.Unlock()
		return false
	}

	delete(qc.inflight, deliveryID)
	qc.decConsumerInflightLocked(msg.consumerID)

	if msg.attempts >= qc.config.MaxRetries {
		qc.deadLetter(msg.payload, msg.attempts)
		qc.mu.Unlock()
		qc.signalDispatch()
		return true
	}

	qc.pending = append(qc.pending, &msg.pendingMessage)
	qc.mu.Unlock()

	qc.signalDispatch()
	return true
}

// pendingCount returns the number of messages waiting to be delivered.
func (qc *queueCoordinator) pendingCount() int {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	return len(qc.pending)
}

// inflightCount returns the number of messages currently being processed.
func (qc *queueCoordinator) inflightCount() int {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	return len(qc.inflight)
}

// --- Maintenance ---

// maintenanceLoop handles visibility-timeout redelivery and expiry of consumers
// that have stopped heartbeating.
func (qc *queueCoordinator) maintenanceLoop() {
	ticker := time.NewTicker(coordinatorMaintenanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			qc.redeliverExpired()
			qc.expireStaleConsumers()
		case <-qc.closeCh:
			return
		}
	}
}

// redeliverExpired moves expired inflight messages back to pending (or
// dead-letters them if max retries exceeded).
func (qc *queueCoordinator) redeliverExpired() {
	qc.mu.Lock()

	now := time.Now()
	changed := false
	for id, msg := range qc.inflight {
		if now.After(msg.expiresAt) {
			delete(qc.inflight, id)
			qc.decConsumerInflightLocked(msg.consumerID)
			changed = true

			if msg.attempts >= qc.config.MaxRetries {
				qc.deadLetter(msg.payload, msg.attempts)
				continue
			}

			qc.pending = append(qc.pending, &msg.pendingMessage)
		}
	}
	qc.mu.Unlock()

	if changed {
		qc.signalDispatch()
	}
}

// expireStaleConsumers drops consumers that have not heartbeated recently and
// re-queues whatever they were holding.
func (qc *queueCoordinator) expireStaleConsumers() {
	ttl := qc.config.ConsumerHeartbeatInterval * 3

	qc.mu.Lock()

	now := time.Now()
	var stale []gossip.NodeID
	for id, cs := range qc.consumers {
		if now.Sub(cs.lastSeen) > ttl {
			stale = append(stale, id)
		}
	}

	for _, id := range stale {
		qc.removeConsumerLocked(id)
		qc.requeueByConsumerLocked(id)
	}
	qc.mu.Unlock()

	if len(stale) > 0 {
		qc.signalDispatch()
	}
}

// deadLetter invokes the user's dead-letter handler in a goroutine, guarding
// against a panic in user code taking down the process.
func (qc *queueCoordinator) deadLetter(payload []byte, attempts int) {
	handler := qc.config.DeadLetterHandler
	if handler == nil {
		return
	}

	go func() {
		defer func() {
			// Swallow panics from user-supplied handlers
			_ = recover()
		}()
		handler(payload, attempts)
	}()
}

// releaseByConsumer re-queues all inflight messages held by a specific consumer
// and removes its registration. Used when a consumer node dies.
func (qc *queueCoordinator) releaseByConsumer(consumerID gossip.NodeID) {
	qc.mu.Lock()
	qc.removeConsumerLocked(consumerID)
	qc.requeueByConsumerLocked(consumerID)
	qc.mu.Unlock()

	qc.signalDispatch()
}

// requeueByConsumerLocked moves a consumer's inflight messages back to pending.
// Caller holds qc.mu.
func (qc *queueCoordinator) requeueByConsumerLocked(consumerID gossip.NodeID) {
	for id, msg := range qc.inflight {
		if msg.consumerID == consumerID {
			delete(qc.inflight, id)
			qc.pending = append(qc.pending, &msg.pendingMessage)
		}
	}
}

// --- Handoff ---

// exportAll removes all messages (pending + inflight) and returns them as
// handoff entries for transfer to a new coordinator.
func (qc *queueCoordinator) exportAll() []handoffEntry {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	entries := make([]handoffEntry, 0, len(qc.pending)+len(qc.inflight))

	for _, msg := range qc.pending {
		entries = append(entries, handoffEntry{
			MessageID:     msg.messageID,
			Payload:       msg.payload,
			ReplyTo:       msg.replyTo,
			CorrelationID: msg.correlationID,
			Attempts:      msg.attempts,
		})
	}
	qc.pending = qc.pending[:0]

	for id, msg := range qc.inflight {
		entries = append(entries, handoffEntry{
			MessageID:     msg.messageID,
			Payload:       msg.payload,
			ReplyTo:       msg.replyTo,
			CorrelationID: msg.correlationID,
			Attempts:      msg.attempts,
		})
		qc.decConsumerInflightLocked(msg.consumerID)
		delete(qc.inflight, id)
	}

	return entries
}

// importEntries adds handoff entries to the pending queue.
func (qc *queueCoordinator) importEntries(entries []handoffEntry) int {
	qc.mu.Lock()

	accepted := 0
	for _, e := range entries {
		qc.pending = append(qc.pending, &pendingMessage{
			messageID:     e.MessageID,
			payload:       e.Payload,
			replyTo:       e.ReplyTo,
			correlationID: e.CorrelationID,
			attempts:      e.Attempts,
		})
		accepted++
	}
	qc.mu.Unlock()

	if accepted > 0 {
		qc.signalDispatch()
	}
	return accepted
}

// totalCount returns the total number of messages (pending + inflight).
func (qc *queueCoordinator) totalCount() int {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	return len(qc.pending) + len(qc.inflight)
}
