package gossip

import (
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// NodeStateChangeHandler and NodeMetadataChangeHandler are used to handle node state and metadata changes
type NodeStateChangeHandler func(*Node, NodeState)
type NodeMetadataChangeHandler func(*Node)
type GossipHandler func()

// eventHandlerFunc is a type constraint for the supported event handler types
type eventHandlerFunc interface {
	NodeStateChangeHandler | NodeMetadataChangeHandler | GossipHandler
}

// HandlerID uniquely identifies a registered event handler
type HandlerID uuid.UUID

// EventHandlers manages a collection of handlers of a specific type
type eventHandlers[T eventHandlerFunc] struct {
	handlers atomic.Value // map[HandlerID]T
	mu       sync.Mutex
}

// NewEventHandlers creates a new handler collection for the specified handler type
func NewEventHandlers[T eventHandlerFunc]() *eventHandlers[T] {
	registry := &eventHandlers[T]{}
	registry.handlers.Store(make(map[HandlerID]T))
	return registry
}

// Add registers a handler and returns its ID
func (l *eventHandlers[T]) Add(handler T) HandlerID {
	// Generate a UUID and convert directly to HandlerID
	id := HandlerID(uuid.New())

	l.mu.Lock()
	defer l.mu.Unlock()

	// Get current map and create a new one with the added handler
	currentMap := l.handlers.Load().(map[HandlerID]T)
	newMap := make(map[HandlerID]T, len(currentMap)+1)

	// Copy existing handlers
	for k, v := range currentMap {
		newMap[k] = v
	}

	// Add new handler
	newMap[id] = handler

	// Store updated map
	l.handlers.Store(newMap)

	return id
}

// Remove unregisters a handler by its ID
func (l *eventHandlers[T]) Remove(id HandlerID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	currentMap := l.handlers.Load().(map[HandlerID]T)

	if _, exists := currentMap[id]; !exists {
		return false
	}

	// Create new map without the handler
	newMap := make(map[HandlerID]T, len(currentMap)-1)
	for k, v := range currentMap {
		if k != id {
			newMap[k] = v
		}
	}

	l.handlers.Store(newMap)
	return true
}

// GetHandlers returns all registered handlers
func (l *eventHandlers[T]) GetHandlers() []T {
	// This is a lock-free read operation
	currentMap := l.handlers.Load().(map[HandlerID]T)

	handlers := make([]T, 0, len(currentMap))
	for _, handler := range currentMap {
		handlers = append(handlers, handler)
	}

	return handlers
}
