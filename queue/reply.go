package queue

import "sync"

// replyMap manages in-flight request/reply correlations. When a producer uses
// Request(), it registers a correlation ID and waits for the result to arrive
// via a direct message from the worker.
type replyMap struct {
	mu       sync.Mutex
	channels map[string]chan replyResult
}

// replyResult holds the response from a worker.
type replyResult struct {
	Payload []byte
	Error   string
}

func newReplyMap() *replyMap {
	return &replyMap{
		channels: make(map[string]chan replyResult),
	}
}

// register creates a channel for the given correlation ID and returns it.
func (rm *replyMap) register(correlationID string) chan replyResult {
	ch := make(chan replyResult, 1)
	rm.mu.Lock()
	rm.channels[correlationID] = ch
	rm.mu.Unlock()
	return ch
}

// unregister removes the channel for a correlation ID.
//
// The channel is deliberately NOT closed. It is buffered(1) and only ever read
// by the Request call that created it, so dropping the map entry is sufficient
// for garbage collection. Closing it would race with a concurrent deliver()
// from a worker reply and panic with "send on closed channel", which would take
// down the whole node since packet dispatch has no recover().
func (rm *replyMap) unregister(correlationID string) {
	rm.mu.Lock()
	delete(rm.channels, correlationID)
	rm.mu.Unlock()
}

// deliver sends a result to the waiting caller. Returns false if no one is
// listening for this correlation ID.
//
// The send happens while holding the lock so that a concurrent unregister
// cannot remove the entry between the lookup and the send. The channel is
// buffered(1) and never closed, so the send can never block or panic.
func (rm *replyMap) deliver(correlationID string, result replyResult) bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	ch, exists := rm.channels[correlationID]
	if !exists {
		return false
	}

	select {
	case ch <- result:
		return true
	default:
		// Buffer already holds a result (duplicate reply) — drop it
		return false
	}
}
