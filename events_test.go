package gossip

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Handlers run in the order they were added, each once per ForEach; removing one leaves the others in place, whichever
// it was.
func TestEventHandlersAddRemove(t *testing.T) {
	for _, removed := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("remove %d of 3", removed), func(t *testing.T) {
			handlers := NewEventHandlers[func(*[]int)]()
			ids := make([]HandlerID, 3)
			for i := range ids {
				ids[i] = handlers.Add(func(calls *[]int) { *calls = append(*calls, i) })
			}
			assert.NotEqual(t, ids[0], ids[1])
			assert.NotEqual(t, ids[1], ids[2])

			var calls []int
			handlers.ForEach(func(h func(*[]int)) { h(&calls) })
			assert.Equal(t, []int{0, 1, 2}, calls)

			require.True(t, handlers.Remove(ids[removed]))
			assert.False(t, handlers.Remove(ids[removed]), "already removed")
			calls = nil
			handlers.ForEach(func(h func(*[]int)) { h(&calls) })
			var want []int
			for i := range ids {
				if i != removed {
					want = append(want, i)
				}
			}
			assert.Equal(t, want, calls)

			for i, id := range ids {
				if i != removed {
					assert.True(t, handlers.Remove(id), "handler %d is still removable", i)
				}
			}
			calls = nil
			handlers.ForEach(func(h func(*[]int)) { h(&calls) })
			assert.Empty(t, calls)
		})
	}

	assert.False(t, NewEventHandlers[func()]().Remove(HandlerID(uuid.New())), "an id that was never issued")
}

func TestEventHandlersForEach(t *testing.T) {
	handlers := NewEventHandlers[func(int)]()

	var results []int
	var mu sync.Mutex

	handler1 := func(val int) {
		mu.Lock()
		results = append(results, val*2)
		mu.Unlock()
	}

	handler2 := func(val int) {
		mu.Lock()
		results = append(results, val*3)
		mu.Unlock()
	}

	handlers.Add(handler1)
	handlers.Add(handler2)

	handlers.ForEach(func(h func(int)) {
		h(5)
	})

	mu.Lock()
	defer mu.Unlock()

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Results should contain 10 and 15 (order may vary)
	found10, found15 := false, false
	for _, r := range results {
		if r == 10 {
			found10 = true
		} else if r == 15 {
			found15 = true
		}
	}

	if !found10 || !found15 {
		t.Fatalf("Expected results 10 and 15, got %v", results)
	}
}

func TestEventHandlersForEachEmpty(t *testing.T) {
	handlers := NewEventHandlers[func()]()

	called := false
	handlers.ForEach(func(h func()) {
		called = true
	})

	if called {
		t.Fatal("ForEach should not call function for empty handlers")
	}
}

func TestEventHandlersConcurrentAccess(t *testing.T) {
	handlers := NewEventHandlers[func()]()

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	var addedCount atomic.Int64

	// Concurrent adds
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				handler := func() {}
				handlers.Add(handler)
				addedCount.Add(1)
			}
		}()
	}

	// Concurrent ForEach calls
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				handlers.ForEach(func(h func()) {
					// Just iterate, don't call
				})
			}
		}()
	}

	wg.Wait()

	slice := handlers.handlers.Load()
	if int64(len(*slice)) != addedCount.Load() {
		t.Fatalf("Expected %d handlers, got %d", addedCount.Load(), len(*slice))
	}
}

func TestEventHandlersConcurrentAddRemove(t *testing.T) {
	handlers := NewEventHandlers[func()]()

	const numGoroutines = 5
	const numOperations = 50

	var wg sync.WaitGroup
	var ids sync.Map // thread-safe map to store handler IDs

	// Concurrent adds and removes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				handler := func() {}
				id := handlers.Add(handler)
				ids.Store(id, true)

				// Sometimes remove immediately
				if j%3 == 0 {
					handlers.Remove(id)
					ids.Delete(id)
				}
			}
		}(i)
	}

	wg.Wait()

	// Count remaining handlers
	remainingCount := 0
	ids.Range(func(key, value interface{}) bool {
		remainingCount++
		return true
	})

	slice := handlers.handlers.Load()
	if len(*slice) != remainingCount {
		t.Fatalf("Handler count mismatch: slice has %d, expected %d", len(*slice), remainingCount)
	}
}

func TestEventHandlersTypeSpecific(t *testing.T) {
	// Test with NodeStateChangeHandler
	stateHandlers := NewEventHandlers[NodeStateChangeHandler]()
	stateHandler := func(*Node, NodeState) {}
	stateID := stateHandlers.Add(stateHandler)

	// Test with NodeMetadataChangeHandler
	metadataHandlers := NewEventHandlers[NodeMetadataChangeHandler]()
	metadataHandler := func(*Node) {}
	metadataID := metadataHandlers.Add(metadataHandler)

	// Test with GossipHandler
	gossipHandlers := NewEventHandlers[GossipHandler]()
	gossipHandler := func() {}
	gossipID := gossipHandlers.Add(gossipHandler)

	// Verify all handlers are properly stored
	if len(*stateHandlers.handlers.Load()) != 1 {
		t.Fatal("State handler not added")
	}
	if len(*metadataHandlers.handlers.Load()) != 1 {
		t.Fatal("Metadata handler not added")
	}
	if len(*gossipHandlers.handlers.Load()) != 1 {
		t.Fatal("Gossip handler not added")
	}

	// Verify removal works for each type
	if !stateHandlers.Remove(stateID) {
		t.Fatal("Failed to remove state handler")
	}
	if !metadataHandlers.Remove(metadataID) {
		t.Fatal("Failed to remove metadata handler")
	}
	if !gossipHandlers.Remove(gossipID) {
		t.Fatal("Failed to remove gossip handler")
	}
}
