package leader

import (
	"sync/atomic"

	"github.com/paularlott/gossip"
)

type EventType int

const (
	LeaderElectedEvent EventType = iota
	LeaderLostEvent
	BecameLeaderEvent
	SteppedDownEvent
)

func (le EventType) String() string {
	switch le {
	case LeaderElectedEvent:
		return "Leader Elected"
	case LeaderLostEvent:
		return "Leader Lost"
	case BecameLeaderEvent:
		return "Became Leader"
	case SteppedDownEvent:
		return "Stepped Down"
	default:
		return "Unknown"
	}
}

type LeaderEventHandler func(EventType, gossip.NodeID)

type leaderEventHandlers struct {
	handlers atomic.Value
}

func newLeaderEventHandlers() *leaderEventHandlers {
	handlers := &leaderEventHandlers{}
	handlers.handlers.Store(make(map[EventType][]LeaderEventHandler))
	return handlers
}

func (h *leaderEventHandlers) add(eventType EventType, handler LeaderEventHandler) {
	currentHandlers := h.handlers.Load().(map[EventType][]LeaderEventHandler)

	// Create a copy of the handlers map
	newHandlers := make(map[EventType][]LeaderEventHandler)
	for t, handlers := range currentHandlers {
		newHandlers[t] = append([]LeaderEventHandler{}, handlers...)
	}

	// Add the new handler to the appropriate event type
	newHandlers[eventType] = append(newHandlers[eventType], handler)

	// Store the updated map
	h.handlers.Store(newHandlers)
}

func (h *leaderEventHandlers) dispatch(eventType EventType, leaderID gossip.NodeID) {
	handlers := h.handlers.Load().(map[EventType][]LeaderEventHandler)

	// Get handlers for this event type
	eventHandlers, ok := handlers[eventType]
	if !ok || len(eventHandlers) == 0 {
		return
	}

	// Call each handler
	for _, handler := range eventHandlers {
		go handler(eventType, leaderID)
	}
}
