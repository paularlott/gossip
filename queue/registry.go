package queue

import (
	"fmt"
	"sync"

	"github.com/paularlott/gossip"
)

// registry is a per-cluster singleton that registers message handlers once and
// dispatches incoming requests to the correct queue based on the queue name.
type registry struct {
	mu           sync.RWMutex
	queues       map[string]*Queue
	cluster      *gossip.Cluster
	stateHandler gossip.HandlerID
}

var (
	registriesMu sync.Mutex
	registries   = make(map[*gossip.Cluster]*registry)
)

func getOrCreateRegistry(cluster *gossip.Cluster) *registry {
	registriesMu.Lock()
	defer registriesMu.Unlock()

	r, exists := registries[cluster]
	if exists {
		return r
	}

	r = &registry{
		queues:  make(map[string]*Queue),
		cluster: cluster,
	}

	if err := cluster.HandleFuncWithReply(queuePublishMsg, r.handlePublish); err != nil {
		panic(fmt.Sprintf("queue: failed to register publish handler: %v", err))
	}
	if err := cluster.HandleFuncWithReply(queueAckMsg, r.handleAck); err != nil {
		panic(fmt.Sprintf("queue: failed to register ack handler: %v", err))
	}
	if err := cluster.HandleFuncWithReply(queueNackMsg, r.handleNack); err != nil {
		panic(fmt.Sprintf("queue: failed to register nack handler: %v", err))
	}
	if err := cluster.HandleFuncWithReply(queueRegisterMsg, r.handleRegister); err != nil {
		panic(fmt.Sprintf("queue: failed to register consumer-register handler: %v", err))
	}
	if err := cluster.HandleFuncWithReply(queueUnregisterMsg, r.handleUnregister); err != nil {
		panic(fmt.Sprintf("queue: failed to register consumer-unregister handler: %v", err))
	}
	if err := cluster.HandleFuncWithReply(queueDeliverMsg, r.handleDeliver); err != nil {
		panic(fmt.Sprintf("queue: failed to register deliver handler: %v", err))
	}
	if err := cluster.HandleFuncWithReply(queueReplyMsg, r.handleReply); err != nil {
		panic(fmt.Sprintf("queue: failed to register reply handler: %v", err))
	}
	if err := cluster.HandleFuncWithReply(queueHandoffMsg, r.handleHandoff); err != nil {
		panic(fmt.Sprintf("queue: failed to register handoff handler: %v", err))
	}

	r.stateHandler = cluster.HandleNodeStateChangeFunc(r.handleNodeStateChange)

	registries[cluster] = r
	return r
}

func (r *registry) registerQueue(name string, q *Queue) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.queues[name]; exists {
		panic(fmt.Sprintf("queue: queue %q already registered on this cluster", name))
	}
	r.queues[name] = q
}

// unregisterQueue removes a queue from the registry. If it's the last queue,
// the handlers are unregistered and the registry is removed.
//
// registriesMu is held across the whole teardown so a concurrent New cannot
// observe this registry via getOrCreateRegistry and then have its handlers
// unregistered out from under it.
func (r *registry) unregisterQueue(name string) {
	registriesMu.Lock()
	defer registriesMu.Unlock()

	r.mu.Lock()
	delete(r.queues, name)
	remaining := len(r.queues)
	r.mu.Unlock()

	if remaining > 0 {
		return
	}

	r.cluster.RemoveNodeStateChangeHandler(r.stateHandler)
	r.cluster.UnregisterMessageType(queuePublishMsg)
	r.cluster.UnregisterMessageType(queueAckMsg)
	r.cluster.UnregisterMessageType(queueNackMsg)
	r.cluster.UnregisterMessageType(queueReplyMsg)
	r.cluster.UnregisterMessageType(queueHandoffMsg)
	r.cluster.UnregisterMessageType(queueRegisterMsg)
	r.cluster.UnregisterMessageType(queueUnregisterMsg)
	r.cluster.UnregisterMessageType(queueDeliverMsg)

	delete(registries, r.cluster)
}

func (r *registry) getQueue(name string) *Queue {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.queues[name]
}

// --- Handlers ---

func (r *registry) handlePublish(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req publishRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	q := r.getQueue(req.QueueName)
	if q == nil {
		return &publishResponse{Accepted: false, Reason: reasonUnknownQueue}, nil
	}

	msg := &pendingMessage{
		messageID:     req.MessageID,
		payload:       req.Payload,
		replyTo:       req.ReplyTo,
		correlationID: req.CorrelationID,
	}

	if !q.coord.enqueue(msg) {
		return &publishResponse{Accepted: false, Reason: reasonQueueFull}, nil
	}

	return &publishResponse{Accepted: true}, nil
}

// handleRegister records (or refreshes) a remote consumer's registration.
func (r *registry) handleRegister(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req registerRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	q := r.getQueue(req.QueueName)
	if q == nil {
		return &registerResponse{OK: false}, nil
	}

	q.coord.registerConsumer(req.ConsumerID, req.Prefetch)
	return &registerResponse{OK: true}, nil
}

// handleUnregister withdraws a remote consumer's registration.
func (r *registry) handleUnregister(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req unregisterRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	q := r.getQueue(req.QueueName)
	if q == nil {
		return &unregisterResponse{OK: false}, nil
	}

	q.coord.unregisterConsumer(req.ConsumerID)
	return &unregisterResponse{OK: true}, nil
}

// handleDeliver receives a pushed message from a coordinator and places it in
// the local consumer's buffer. This returns as soon as the message is buffered
// — it does not wait for the handler to run, so the coordinator's dispatch path
// stays fast regardless of processing time.
func (r *registry) handleDeliver(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req deliverPush
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	q := r.getQueue(req.QueueName)
	if q == nil {
		return &deliverPushResponse{Accepted: false, Reason: reasonUnknownQueue}, nil
	}

	msg := &Message{
		queue:         q,
		DeliveryID:    req.DeliveryID,
		MessageID:     req.MessageID,
		Payload:       req.Payload,
		Attempt:       req.Attempt,
		replyTo:       req.ReplyTo,
		correlationID: req.CorrelationID,
	}

	if !q.deliverToLocalConsumer(msg) {
		return &deliverPushResponse{Accepted: false, Reason: reasonNoCapacity}, nil
	}

	return &deliverPushResponse{Accepted: true}, nil
}

// handleNack rejects a message so the coordinator re-queues it.
func (r *registry) handleNack(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req nackRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	q := r.getQueue(req.QueueName)
	if q == nil {
		return &nackResponse{OK: false}, nil
	}

	return &nackResponse{OK: q.coord.nack(req.DeliveryID)}, nil
}

func (r *registry) handleAck(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req ackRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	q := r.getQueue(req.QueueName)
	if q == nil {
		return &ackResponse{OK: false, Reason: "unknown queue"}, nil
	}

	ok := q.coord.ack(req.DeliveryID)
	if !ok {
		return &ackResponse{OK: false, Reason: "delivery not found"}, nil
	}

	return &ackResponse{OK: true}, nil
}

func (r *registry) handleReply(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req replyRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	q := r.getQueue(req.QueueName)
	if q == nil {
		return &replyResponse{OK: false}, nil
	}

	q.replies.deliver(req.CorrelationID, replyResult{
		Payload: req.Payload,
		Error:   req.Error,
	})

	return &replyResponse{OK: true}, nil
}

func (r *registry) handleHandoff(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req handoffRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	q := r.getQueue(req.QueueName)
	if q == nil {
		return &handoffResponse{Accepted: 0}, nil
	}

	accepted := q.coord.importEntries(req.Entries)
	return &handoffResponse{Accepted: accepted}, nil
}

func (r *registry) handleNodeStateChange(node *gossip.Node, prevState gossip.NodeState) {
	r.mu.RLock()
	queues := make([]*Queue, 0, len(r.queues))
	for _, q := range r.queues {
		queues = append(queues, q)
	}
	r.mu.RUnlock()

	currentState := node.GetObservedState()

	for _, q := range queues {
		// Re-queue messages from dead consumers
		if currentState == gossip.NodeDead {
			q.coord.releaseByConsumer(node.ID)
		}

		// Trigger handoff on any membership change
		q.requestHandoff()

		// The coordinator may have changed, so re-announce our consumer to it
		// immediately rather than waiting for the next heartbeat tick.
		if q.consumer.isRunning() {
			go q.registerConsumer()
		}
	}
}
