package queue

import (
	"sync"

	"github.com/paularlott/gossip"
)

// MessageHandler processes a delivered message. Return nil to ack the message.
// Return an error to nack it, which re-queues it for another consumer (or
// dead-letters it once MaxRetries is exhausted).
type MessageHandler func(msg *Message) error

// Message represents a delivered message given to a consumer's handler.
type Message struct {
	queue         *Queue
	DeliveryID    string
	MessageID     string
	Payload       []byte
	Attempt       int
	replyTo       gossip.NodeID
	correlationID string
}

// Reply sends a response back to the original caller (only meaningful if the
// publisher used Request). Safe to call even if no reply is expected — it's a
// no-op in that case.
func (m *Message) Reply(payload []byte) error {
	if m.correlationID == "" || m.replyTo == gossip.EmptyNodeID {
		return nil // no reply expected
	}
	return m.queue.sendReply(m.replyTo, m.correlationID, payload, "")
}

// ReplyError sends an error response back to the caller.
func (m *Message) ReplyError(errMsg string) error {
	if m.correlationID == "" || m.replyTo == gossip.EmptyNodeID {
		return nil
	}
	return m.queue.sendReply(m.replyTo, m.correlationID, nil, errMsg)
}

// consumerManager holds the local consumer handler and a bounded buffer of
// pushed messages awaiting processing.
//
// The coordinator never pushes more than `prefetch` messages to this node, so
// the buffer cannot grow without bound. Accepting a push is cheap (it just
// enqueues into the buffer), which keeps the coordinator's dispatch path fast
// regardless of how long the handler takes.
type consumerManager struct {
	mu       sync.Mutex
	handler  MessageHandler
	running  bool
	stopped  bool
	stopCh   chan struct{}
	inbox    chan *Message
	prefetch int
	wg       sync.WaitGroup
}

func newConsumerManager(prefetch int) *consumerManager {
	if prefetch <= 0 {
		prefetch = defaultPrefetch
	}
	return &consumerManager{
		stopCh:   make(chan struct{}),
		inbox:    make(chan *Message, prefetch),
		prefetch: prefetch,
	}
}

func (cm *consumerManager) setHandler(handler MessageHandler) {
	cm.mu.Lock()
	cm.handler = handler
	cm.mu.Unlock()
}

func (cm *consumerManager) getHandler() MessageHandler {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.handler
}

// offer places a pushed message into the local buffer. Returns false if the
// consumer is not running or the buffer is full (which means the coordinator
// exceeded our prefetch, so the message must go back to it).
func (cm *consumerManager) offer(msg *Message) bool {
	cm.mu.Lock()
	if !cm.running || cm.stopped {
		cm.mu.Unlock()
		return false
	}
	cm.mu.Unlock()

	select {
	case cm.inbox <- msg:
		return true
	default:
		return false
	}
}

func (cm *consumerManager) stop() {
	cm.mu.Lock()
	if cm.stopped {
		cm.mu.Unlock()
		return
	}
	cm.stopped = true
	cm.running = false
	close(cm.stopCh)
	cm.mu.Unlock()

	// Wait for in-flight handlers to finish
	cm.wg.Wait()
}

func (cm *consumerManager) isRunning() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.running
}

func (cm *consumerManager) isStopped() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.stopped
}

// markRunning flips the consumer to running. Returns false if it was already
// running or has been stopped.
func (cm *consumerManager) markRunning() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.running || cm.stopped {
		return false
	}
	cm.running = true
	return true
}
