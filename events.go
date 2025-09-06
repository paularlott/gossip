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

// HandlerID uniquely identifies a registered event handler
type HandlerID uuid.UUID

// EventHandlers manages a collection of handlers of a specific type
type EventHandlers[T any] struct {
	handlers atomic.Pointer[[]T] // Changed to slice for faster iteration
	mu       sync.RWMutex        // Changed to RWMutex for concurrent reads
	idMap    map[HandlerID]int   // Track handler positions for removal
}

// NewEventHandlers creates a new handler collection for any type
func NewEventHandlers[T any]() *EventHandlers[T] {
	registry := &EventHandlers[T]{
		idMap: make(map[HandlerID]int),
	}
	registry.handlers.Store(&[]T{})
	return registry
}

// Add registers a handler and returns its ID
func (l *EventHandlers[T]) Add(handler T) HandlerID {
	id := HandlerID(uuid.New())

	l.mu.Lock()
	defer l.mu.Unlock()

	// Get current slice and create new one
	currentSlice := l.handlers.Load()
	newSlice := make([]T, len(*currentSlice)+1)
	copy(newSlice, *currentSlice)
	newSlice[len(*currentSlice)] = handler

	// Update ID mapping
	l.idMap[id] = len(*currentSlice)

	// Store new slice
	l.handlers.Store(&newSlice)

	return id
}

// Remove unregisters a handler by its ID
func (l *EventHandlers[T]) Remove(id HandlerID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	currentSlice := l.handlers.Load()
	index, exists := l.idMap[id]
	if !exists || index >= len(*currentSlice) {
		return false
	}

	// Create new slice without the handler
	newSlice := make([]T, len(*currentSlice)-1)
	copy(newSlice[:index], (*currentSlice)[:index])
	copy(newSlice[index:], (*currentSlice)[index+1:])

	// Update ID mappings for shifted elements
	delete(l.idMap, id)
	for otherID, otherIndex := range l.idMap {
		if otherIndex > index {
			l.idMap[otherID] = otherIndex - 1
		}
	}

	l.handlers.Store(&newSlice)
	return true
}

// ForEach executes a function for each handler (optimized for iteration)
func (l *EventHandlers[T]) ForEach(fn func(T)) {
	// Lock-free iteration
	currentSlice := l.handlers.Load()
	if currentSlice == nil || len(*currentSlice) == 0 {
		return
	}

	for i := range *currentSlice {
		fn((*currentSlice)[i])
	}
}
