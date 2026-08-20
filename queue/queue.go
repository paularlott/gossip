package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
)

var (
	ErrQueueFull     = fmt.Errorf("queue: full")
	ErrQueueClosed   = fmt.Errorf("queue: closed")
	ErrNoConsumers   = fmt.Errorf("queue: no consumers")
	ErrTimeout       = fmt.Errorf("queue: timeout")
	ErrWorkerError   = fmt.Errorf("queue: worker error")
	ErrNoCoordinator = fmt.Errorf("queue: no coordinator available")
)

// Rejection reasons sent over the wire.
const (
	reasonQueueFull    = "queue full"
	reasonUnknownQueue = "unknown queue"
	reasonNoCapacity   = "consumer at capacity"
)

// Queue provides a distributed message queue with at-least-once delivery.
// Messages are coordinated via rendezvous hashing — the queue name maps to a
// coordinator node that holds the authoritative message state.
//
// Multiple queues can coexist on the same cluster, each with a unique Name.
type Queue struct {
	cluster  *gossip.Cluster
	config   *Config
	coord    *queueCoordinator
	consumer *consumerManager
	replies  *replyMap
	registry *registry
	mu       sync.Mutex
	closed   bool

	handoffMu   sync.Mutex    // serializes performHandoff / drainAll
	handoffReq  chan struct{} // signals a handoff is needed (never closed)
	handoffStop chan struct{} // closed once on Close to stop the worker
	handoffDone chan struct{} // closed by the worker when it has exited

	dispatchStop chan struct{} // closed once on Close to stop the dispatcher
	dispatchDone chan struct{} // closed by the dispatcher when it has exited
}

// handoffDebounce is how long the handoff worker waits after a membership
// change signal before acting, so rapid-fire changes coalesce into one pass.
const handoffDebounce = 50 * time.Millisecond

// handoffReconcileInterval is how often the handoff worker re-checks whether it
// is still the coordinator, even without a membership-change signal. This
// catches messages published to a just-retired coordinator by a producer with a
// stale cluster view — unlike locks, queued messages have no TTL to fall back on.
const handoffReconcileInterval = 5 * time.Second

// New creates a new distributed queue attached to the given cluster.
func New(cluster *gossip.Cluster, config *Config) *Queue {
	config = config.validate()

	q := &Queue{
		cluster:      cluster,
		config:       config,
		coord:        newQueueCoordinator(config),
		consumer:     newConsumerManager(config.Prefetch),
		replies:      newReplyMap(),
		handoffReq:   make(chan struct{}, 1),
		handoffStop:  make(chan struct{}),
		handoffDone:  make(chan struct{}),
		dispatchStop: make(chan struct{}),
		dispatchDone: make(chan struct{}),
	}

	// Start the handoff worker and the push dispatcher
	go q.handoffWorker()
	go q.dispatchLoop()

	q.registry = getOrCreateRegistry(cluster)
	q.registry.registerQueue(config.Name, q)

	return q
}

// Close gracefully shuts down the queue. Pending messages are handed off to
// the new coordinator before cleanup.
func (q *Queue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.mu.Unlock()

	// Stop the consumer first (this also withdraws our registration from the
	// coordinator) and wait for in-flight handlers to finish.
	q.consumer.stop()

	// Stop the dispatcher so no new pushes are issued while we drain.
	close(q.dispatchStop)
	<-q.dispatchDone

	// Stop the handoff worker and wait for it to finish so that no concurrent
	// performHandoff can re-import messages after we have drained.
	close(q.handoffStop)
	<-q.handoffDone

	q.drainAll()
	q.coord.close()
	q.registry.unregisterQueue(q.config.Name)
}

// Name returns the queue's name.
func (q *Queue) Name() string {
	return q.config.Name
}

// Publish sends a message to the queue for processing by a consumer. This is
// fire-and-forget — the call returns once the coordinator acknowledges receipt.
func (q *Queue) Publish(ctx context.Context, payload []byte) error {
	if err := q.checkClosed(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	coord := q.getCoordinator()
	if coord == nil {
		return ErrNoCoordinator
	}

	msgID := uuid.New().String()
	localID := q.cluster.LocalNode().ID

	// Fast path: we are the coordinator
	if coord.ID == localID {
		msg := &pendingMessage{
			messageID: msgID,
			payload:   payload,
		}
		if !q.coord.enqueue(msg) {
			return ErrQueueFull
		}
		return nil
	}

	req := &publishRequest{
		QueueName: q.config.Name,
		MessageID: msgID,
		Payload:   payload,
	}
	var resp publishResponse

	err := q.cluster.SendToWithResponse(coord, queuePublishMsg, req, &resp)
	if err != nil {
		return fmt.Errorf("queue: publish failed: %w", err)
	}
	if !resp.Accepted {
		if resp.Reason == reasonQueueFull {
			return ErrQueueFull
		}
		return fmt.Errorf("queue: publish rejected: %s", resp.Reason)
	}

	return nil
}

// Request sends a message and waits for the consumer to reply. Returns the
// reply payload or an error if the timeout expires or the worker reports an error.
//
// If ctx is cancelled the context's error is returned; if the timeout elapses
// first, ErrTimeout is returned.
func (q *Queue) Request(ctx context.Context, payload []byte, timeout time.Duration) ([]byte, error) {
	if err := q.checkClosed(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	coord := q.getCoordinator()
	if coord == nil {
		return nil, ErrNoCoordinator
	}

	msgID := uuid.New().String()
	correlationID := uuid.New().String()
	localID := q.cluster.LocalNode().ID

	// Register reply channel
	replyCh := q.replies.register(correlationID)
	defer q.replies.unregister(correlationID)

	// Publish with reply metadata
	if coord.ID == localID {
		msg := &pendingMessage{
			messageID:     msgID,
			payload:       payload,
			replyTo:       localID,
			correlationID: correlationID,
		}
		if !q.coord.enqueue(msg) {
			return nil, ErrQueueFull
		}
	} else {
		req := &publishRequest{
			QueueName:     q.config.Name,
			MessageID:     msgID,
			Payload:       payload,
			ReplyTo:       localID,
			CorrelationID: correlationID,
		}
		var resp publishResponse

		err := q.cluster.SendToWithResponse(coord, queuePublishMsg, req, &resp)
		if err != nil {
			return nil, fmt.Errorf("queue: request publish failed: %w", err)
		}
		if !resp.Accepted {
			if resp.Reason == reasonQueueFull {
				return nil, ErrQueueFull
			}
			return nil, fmt.Errorf("queue: request rejected: %s", resp.Reason)
		}
	}

	// Wait for reply, distinguishing caller cancellation from reply timeout.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result := <-replyCh:
		if result.Error != "" {
			return nil, fmt.Errorf("%w: %s", ErrWorkerError, result.Error)
		}
		return result.Payload, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrTimeout
	}
}

// Consume registers a handler that processes messages from the queue.
//
// Delivery is push-based: this node announces itself (and its prefetch limit)
// to the coordinator, which then pushes messages as they arrive. There is no
// polling, so an idle queue generates no traffic beyond a heartbeat every
// ConsumerHeartbeatInterval, and delivery latency is a single network hop.
//
// Only one consumer per queue per node. Calling Consume again replaces the
// handler without restarting the consumer.
//
// The handler should return nil to acknowledge the message. Returning an error
// nacks it, re-queueing it for another consumer (or dead-lettering it once
// MaxRetries is exhausted).
func (q *Queue) Consume(handler MessageHandler) {
	if q.checkClosed() != nil {
		return
	}
	if handler == nil {
		return
	}

	q.consumer.setHandler(handler)

	// markRunning returns false if already running (handler swap) or stopped.
	if !q.consumer.markRunning() {
		return
	}

	// Start the local worker pool that drains the inbox
	for i := 0; i < q.consumer.prefetch; i++ {
		q.consumer.wg.Add(1)
		go q.consumerWorker()
	}

	// Announce ourselves and keep the registration fresh
	go q.consumerHeartbeatLoop()

	// Register immediately so the first message doesn't wait for a heartbeat
	q.registerConsumer()
}

// consumerWorker drains pushed messages from the local inbox and runs the
// handler. Handlers run concurrently up to the prefetch limit.
func (q *Queue) consumerWorker() {
	defer q.consumer.wg.Done()

	for {
		select {
		case <-q.consumer.stopCh:
			return
		case msg := <-q.consumer.inbox:
			if msg == nil {
				continue
			}

			handler := q.consumer.getHandler()
			if handler == nil {
				q.nackMessage(msg.DeliveryID)
				continue
			}

			err := q.runHandler(handler, msg)
			if err == nil {
				q.ackMessage(msg.DeliveryID)
			} else {
				q.nackMessage(msg.DeliveryID)
			}
		}
	}
}

// runHandler invokes a user handler, converting a panic into an error so a bad
// handler nacks the message instead of taking down the node.
func (q *Queue) runHandler(handler MessageHandler, msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("queue: handler panicked: %v", r)
		}
	}()
	return handler(msg)
}

// consumerHeartbeatLoop periodically re-announces this consumer. This refreshes
// the registration and, crucially, is how the consumer attaches to a new
// coordinator after a membership change.
func (q *Queue) consumerHeartbeatLoop() {
	ticker := time.NewTicker(q.config.ConsumerHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.consumer.stopCh:
			// Best-effort withdrawal so the coordinator can reassign our work
			// immediately rather than waiting for the registration to go stale.
			q.unregisterConsumer()
			return
		case <-ticker.C:
			q.registerConsumer()
		}
	}
}

// registerConsumer announces this node as a consumer to the current coordinator.
func (q *Queue) registerConsumer() {
	if q.checkClosed() != nil {
		return
	}

	coord := q.getCoordinator()
	if coord == nil {
		return
	}

	localID := q.cluster.LocalNode().ID

	// Local coordinator — register in-process, no network
	if coord.ID == localID {
		q.coord.registerConsumer(localID, q.config.Prefetch)
		return
	}

	req := &registerRequest{
		QueueName:  q.config.Name,
		ConsumerID: localID,
		Prefetch:   q.config.Prefetch,
	}
	var resp registerResponse
	_ = q.cluster.SendToWithResponse(coord, queueRegisterMsg, req, &resp)
}

// unregisterConsumer withdraws this node's consumer registration.
func (q *Queue) unregisterConsumer() {
	coord := q.getCoordinator()
	if coord == nil {
		return
	}

	localID := q.cluster.LocalNode().ID

	if coord.ID == localID {
		q.coord.unregisterConsumer(localID)
		return
	}

	req := &unregisterRequest{
		QueueName:  q.config.Name,
		ConsumerID: localID,
	}
	var resp unregisterResponse
	_ = q.cluster.SendToWithResponse(coord, queueUnregisterMsg, req, &resp)
}

// deliverToLocalConsumer hands a reserved message to the local consumer's
// inbox. Returns false if the consumer isn't running or its buffer is full.
func (q *Queue) deliverToLocalConsumer(msg *Message) bool {
	return q.consumer.offer(msg)
}

// --- Dispatcher ---

// dispatchLoop pushes reserved messages to consumers. It wakes only when the
// coordinator signals that work may be assignable, so an idle queue is silent.
func (q *Queue) dispatchLoop() {
	defer close(q.dispatchDone)

	// Bounded concurrency so one slow consumer can't stall the others
	sem := make(chan struct{}, dispatchWorkers)
	var wg sync.WaitGroup

	for {
		select {
		case <-q.dispatchStop:
			wg.Wait()
			return
		case <-q.coord.dispatchSignal:
		}

		// Drain all currently assignable work
		for {
			select {
			case <-q.dispatchStop:
				wg.Wait()
				return
			default:
			}

			inflight := q.coord.reserveForDispatch()
			if inflight == nil {
				break // nothing pending, or no consumer has capacity
			}

			sem <- struct{}{}
			wg.Add(1)
			go func(m *inflightMessage) {
				defer wg.Done()
				defer func() { <-sem }()
				q.pushToConsumer(m)
			}(inflight)
		}
	}
}

// pushToConsumer delivers a reserved message to its assigned consumer. On
// failure the reservation is released so the message returns to pending.
func (q *Queue) pushToConsumer(inflight *inflightMessage) {
	localID := q.cluster.LocalNode().ID

	// Local consumer — hand over in-process, no network hop
	if inflight.consumerID == localID {
		msg := &Message{
			queue:         q,
			DeliveryID:    inflight.deliveryID,
			MessageID:     inflight.messageID,
			Payload:       inflight.payload,
			Attempt:       inflight.attempts,
			replyTo:       inflight.replyTo,
			correlationID: inflight.correlationID,
		}
		if !q.deliverToLocalConsumer(msg) {
			q.coord.releaseReservation(inflight.deliveryID)
		}
		return
	}

	node := q.cluster.GetNode(inflight.consumerID)
	if node == nil {
		q.coord.releaseByConsumer(inflight.consumerID)
		return
	}

	req := &deliverPush{
		QueueName:     q.config.Name,
		DeliveryID:    inflight.deliveryID,
		MessageID:     inflight.messageID,
		Payload:       inflight.payload,
		ReplyTo:       inflight.replyTo,
		CorrelationID: inflight.correlationID,
		Attempt:       inflight.attempts,
	}
	var resp deliverPushResponse

	err := q.cluster.SendToWithResponse(node, queueDeliverMsg, req, &resp)
	if err != nil {
		// Consumer unreachable — drop its registration so we stop trying, and
		// return everything it held to the pending queue.
		q.coord.releaseByConsumer(inflight.consumerID)
		return
	}
	if !resp.Accepted {
		// Consumer is full or no longer consuming — put it back
		q.coord.releaseReservation(inflight.deliveryID)
	}
}

// ackMessage sends an ack to the coordinator.
func (q *Queue) ackMessage(deliveryID string) {
	coord := q.getCoordinator()
	if coord == nil {
		return
	}

	localID := q.cluster.LocalNode().ID

	if coord.ID == localID {
		q.coord.ack(deliveryID)
		return
	}

	req := &ackRequest{
		QueueName:  q.config.Name,
		DeliveryID: deliveryID,
	}
	var resp ackResponse
	_ = q.cluster.SendToWithResponse(coord, queueAckMsg, req, &resp)
}

// nackMessage rejects a message so it is re-queued for redelivery.
func (q *Queue) nackMessage(deliveryID string) {
	coord := q.getCoordinator()
	if coord == nil {
		return
	}

	localID := q.cluster.LocalNode().ID

	if coord.ID == localID {
		q.coord.nack(deliveryID)
		return
	}

	req := &nackRequest{
		QueueName:  q.config.Name,
		DeliveryID: deliveryID,
	}
	var resp nackResponse
	_ = q.cluster.SendToWithResponse(coord, queueNackMsg, req, &resp)
}

// sendReply sends a reply directly to the caller node.
func (q *Queue) sendReply(callerNode gossip.NodeID, correlationID string, payload []byte, errMsg string) error {
	node := q.cluster.GetNode(callerNode)
	if node == nil {
		return fmt.Errorf("queue: reply target node not found")
	}

	req := &replyRequest{
		QueueName:     q.config.Name,
		CorrelationID: correlationID,
		Payload:       payload,
		Error:         errMsg,
	}
	var resp replyResponse

	return q.cluster.SendToWithResponse(node, queueReplyMsg, req, &resp)
}

// getCoordinator returns the coordinator node for this queue.
func (q *Queue) getCoordinator() *gossip.Node {
	var nodes []*gossip.Node
	if q.config.NodeGroup != nil {
		nodes = q.config.NodeGroup.GetNodes(nil)
	} else {
		nodes = q.cluster.AliveNodes()
	}
	return coordinatorFor(q.config.Name, nodes)
}

// performHandoff recalculates coordinator assignment and hands off messages
// if this node is no longer the coordinator.
func (q *Queue) performHandoff() {
	q.handoffMu.Lock()
	defer q.handoffMu.Unlock()

	if q.coord.totalCount() == 0 {
		return
	}

	localID := q.cluster.LocalNode().ID
	coord := q.getCoordinator()

	// If we're still the coordinator, nothing to do
	if coord == nil || coord.ID == localID {
		return
	}

	// We're no longer the coordinator — hand off everything
	entries := q.coord.exportAll()
	if len(entries) == 0 {
		return
	}

	req := &handoffRequest{
		QueueName: q.config.Name,
		Entries:   entries,
	}
	var resp handoffResponse

	err := q.cluster.SendToWithResponse(coord, queueHandoffMsg, req, &resp)
	if err != nil || resp.Accepted < len(entries) {
		// Failed, or the destination did not accept every entry (e.g. it has no
		// queue registered under this name yet). Re-import so messages are not
		// silently lost — the periodic reconcile will retry.
		q.coord.importEntries(entries)
	}
}

// requestHandoff signals the handoff worker. Non-blocking and safe to call at
// any time, including after Close.
func (q *Queue) requestHandoff() {
	select {
	case q.handoffReq <- struct{}{}:
	default:
	}
}

// handoffWorker serializes handoff operations with debouncing, and periodically
// reconciles coordinator ownership even without membership-change signals.
//
// Shutdown is driven by handoffStop (a dedicated close-only channel).
// handoffReq is never closed, so no receive on it can ever succeed spuriously.
func (q *Queue) handoffWorker() {
	defer close(q.handoffDone)

	reconcile := time.NewTicker(handoffReconcileInterval)
	defer reconcile.Stop()

	for {
		select {
		case <-q.handoffStop:
			return

		case <-reconcile.C:
			// Periodic safety net — no debounce needed
			q.performHandoff()
			continue

		case <-q.handoffReq:
		}

		// Debounce: wait briefly for rapid membership changes to settle, but
		// abort promptly if we're shutting down.
		select {
		case <-q.handoffStop:
			return
		case <-time.After(handoffDebounce):
		}

		// Drain additional signals so we perform a single consolidated pass.
	drain:
		for {
			select {
			case <-q.handoffReq:
			default:
				break drain
			}
		}

		// Bail out if Close happened while we were debouncing/draining.
		select {
		case <-q.handoffStop:
			return
		default:
		}

		q.performHandoff()
	}
}

// drainAll hands off all messages to surviving nodes during graceful shutdown.
func (q *Queue) drainAll() {
	q.handoffMu.Lock()
	defer q.handoffMu.Unlock()

	if q.coord.totalCount() == 0 {
		return
	}

	localID := q.cluster.LocalNode().ID

	var nodes []*gossip.Node
	if q.config.NodeGroup != nil {
		nodes = q.config.NodeGroup.GetNodes([]gossip.NodeID{localID})
	} else {
		allNodes := q.cluster.AliveNodes()
		nodes = make([]*gossip.Node, 0, len(allNodes))
		for _, n := range allNodes {
			if n.ID != localID {
				nodes = append(nodes, n)
			}
		}
	}

	if len(nodes) == 0 {
		return
	}

	// Find new coordinator excluding self
	newCoord := coordinatorFor(q.config.Name, nodes)
	if newCoord == nil {
		return
	}

	entries := q.coord.exportAll()
	if len(entries) == 0 {
		return
	}

	req := &handoffRequest{
		QueueName: q.config.Name,
		Entries:   entries,
	}
	var resp handoffResponse

	err := q.cluster.SendToWithResponse(newCoord, queueHandoffMsg, req, &resp)
	if err != nil || resp.Accepted < len(entries) {
		// Re-import rather than silently dropping. Note this is a last-ditch
		// drain during Close, so if it fails these messages are lost — this is
		// documented in the README's failure-mode table.
		q.coord.importEntries(entries)
	}
}

// PendingCount returns the number of messages pending on the local coordinator.
func (q *Queue) PendingCount() int {
	return q.coord.pendingCount()
}

// ConsumerCount returns the number of consumers currently registered with the
// local coordinator. Returns 0 if this node is not the coordinator.
func (q *Queue) ConsumerCount() int {
	return q.coord.consumerCount()
}

// InflightCount returns the number of inflight messages on the local coordinator.
func (q *Queue) InflightCount() int {
	return q.coord.inflightCount()
}

func (q *Queue) checkClosed() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	return nil
}
