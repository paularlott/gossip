package leader

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
	"github.com/paularlott/logger"
)

var testNodeID = gossip.NodeID(uuid.MustParse("01960f9b-72ca-7a51-9efa-47c12f42a138"))

func TestEventTypeString(t *testing.T) {
	for event, want := range map[EventType]string{
		LeaderElectedEvent: "Leader Elected",
		LeaderLostEvent:    "Leader Lost",
		BecameLeaderEvent:  "Became Leader",
		SteppedDownEvent:   "Stepped Down",
		EventType(999):     "Unknown",
	} {
		if got := event.String(); got != want {
			t.Errorf("EventType(%d) = %q, want %q", event, got, want)
		}
	}
}

func TestNoHandlers(t *testing.T) {
	handlers := newLeaderEventHandlers(logger.NewNullLogger())

	// Dispatching with nothing registered must not panic.
	handlers.dispatch(BecameLeaderEvent, testNodeID)
}

func TestLeaderEventHandlersConcurrentAddNoLoss(t *testing.T) {
	handlers := newLeaderEventHandlers(logger.NewNullLogger())

	var count atomic.Int64
	var wg sync.WaitGroup
	const numHandlers = 64

	for range numHandlers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handlers.add(BecameLeaderEvent, func(EventType, gossip.NodeID) {
				count.Add(1)
			})
		}()
	}
	wg.Wait()

	handlers.dispatch(BecameLeaderEvent, testNodeID)

	if got := count.Load(); got != numHandlers {
		t.Fatalf("expected %d handlers to run, got %d", numHandlers, got)
	}
}
