package gossip

import (
	"sync/atomic"
)

// NodeStateChangeHandler and NodeMetadataChangeHandler are used to handle node state and metadata changes
type NodeStateChangeHandler func(*Node, NodeState)
type NodeMetadataChangeHandler func(*Node)

// eventHandlerFunc is a type constraint for the supported event handler types
type eventHandlerFunc interface {
	NodeStateChangeHandler | NodeMetadataChangeHandler
}

// EventHandlers manages a collection of handlers of a specific type
type eventHandlers[T eventHandlerFunc] struct {
	handlers atomic.Value // holds []T
}

// NewEventHandlers creates a new handler collection for the specified handler type
func NewEventHandlers[T eventHandlerFunc]() *eventHandlers[T] {
	registry := &eventHandlers[T]{}
	registry.handlers.Store(make([]T, 0))
	return registry
}

// Add registers a new handler function
func (eh *eventHandlers[T]) Add(handler T) {
	currentHandlers := eh.handlers.Load().([]T)

	// Create a new slice with the added handler
	newHandlers := make([]T, len(currentHandlers)+1)
	copy(newHandlers, currentHandlers)
	newHandlers[len(currentHandlers)] = handler

	eh.handlers.Store(newHandlers)
}

func notifyNodeStateChanged(handlers *eventHandlers[NodeStateChangeHandler], node *Node, prevState NodeState) {
	currentHandlers := handlers.handlers.Load().([]NodeStateChangeHandler)
	for _, handler := range currentHandlers {
		handler(node, prevState)
	}
}

func notifyNodeMetadataChanged(handlers *eventHandlers[NodeMetadataChangeHandler], node *Node) {
	currentHandlers := handlers.handlers.Load().([]NodeMetadataChangeHandler)
	for _, handler := range currentHandlers {
		handler(node)
	}
}
