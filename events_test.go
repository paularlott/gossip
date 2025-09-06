package gossip

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
)

func TestNewEventHandlers(t *testing.T) {
	handlers := NewEventHandlers[NodeStateChangeHandler]()
	
	if handlers == nil {
		t.Fatal("NewEventHandlers returned nil")
	}
	
	if handlers.idMap == nil {
		t.Fatal("idMap not initialized")
	}
	
	if handlers.handlers.Load() == nil {
		t.Fatal("handlers slice not initialized")
	}
	
	if len(*handlers.handlers.Load()) != 0 {
		t.Fatal("handlers slice should be empty initially")
	}
}

func TestEventHandlersAdd(t *testing.T) {
	handlers := NewEventHandlers[NodeStateChangeHandler]()
	
	handler := func(*Node, NodeState) {}
	id := handlers.Add(handler)
	
	if id == HandlerID(uuid.Nil) {
		t.Fatal("Add should return non-nil ID")
	}
	
	slice := handlers.handlers.Load()
	if len(*slice) != 1 {
		t.Fatalf("Expected 1 handler, got %d", len(*slice))
	}
	
	if _, exists := handlers.idMap[id]; !exists {
		t.Fatal("Handler ID not found in idMap")
	}
}

func TestEventHandlersAddMultiple(t *testing.T) {
	handlers := NewEventHandlers[NodeStateChangeHandler]()
	
	handler1 := func(*Node, NodeState) {}
	handler2 := func(*Node, NodeState) {}
	
	id1 := handlers.Add(handler1)
	id2 := handlers.Add(handler2)
	
	if id1 == id2 {
		t.Fatal("Handler IDs should be unique")
	}
	
	slice := handlers.handlers.Load()
	if len(*slice) != 2 {
		t.Fatalf("Expected 2 handlers, got %d", len(*slice))
	}
	
	if handlers.idMap[id1] != 0 {
		t.Fatalf("Expected handler1 at index 0, got %d", handlers.idMap[id1])
	}
	
	if handlers.idMap[id2] != 1 {
		t.Fatalf("Expected handler2 at index 1, got %d", handlers.idMap[id2])
	}
}

func TestEventHandlersRemove(t *testing.T) {
	handlers := NewEventHandlers[NodeStateChangeHandler]()
	
	handler := func(*Node, NodeState) {}
	id := handlers.Add(handler)
	
	if !handlers.Remove(id) {
		t.Fatal("Remove should return true for existing handler")
	}
	
	slice := handlers.handlers.Load()
	if len(*slice) != 0 {
		t.Fatalf("Expected 0 handlers after removal, got %d", len(*slice))
	}
	
	if _, exists := handlers.idMap[id]; exists {
		t.Fatal("Handler ID should be removed from idMap")
	}
}

func TestEventHandlersRemoveNonExistent(t *testing.T) {
	handlers := NewEventHandlers[NodeStateChangeHandler]()
	
	fakeID := HandlerID(uuid.New())
	if handlers.Remove(fakeID) {
		t.Fatal("Remove should return false for non-existent handler")
	}
}

func TestEventHandlersRemoveMiddle(t *testing.T) {
	handlers := NewEventHandlers[NodeStateChangeHandler]()
	
	handler1 := func(*Node, NodeState) {}
	handler2 := func(*Node, NodeState) {}
	handler3 := func(*Node, NodeState) {}
	
	id1 := handlers.Add(handler1)
	id2 := handlers.Add(handler2)
	id3 := handlers.Add(handler3)
	
	// Remove middle handler
	if !handlers.Remove(id2) {
		t.Fatal("Remove should succeed")
	}
	
	slice := handlers.handlers.Load()
	if len(*slice) != 2 {
		t.Fatalf("Expected 2 handlers after removal, got %d", len(*slice))
	}
	
	// Check that indices are updated correctly
	if handlers.idMap[id1] != 0 {
		t.Fatalf("Expected handler1 at index 0, got %d", handlers.idMap[id1])
	}
	
	if handlers.idMap[id3] != 1 {
		t.Fatalf("Expected handler3 at index 1, got %d", handlers.idMap[id3])
	}
	
	if _, exists := handlers.idMap[id2]; exists {
		t.Fatal("Removed handler should not exist in idMap")
	}
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

// Benchmarks

func BenchmarkEventHandlersAdd(b *testing.B) {
	handlers := NewEventHandlers[func()]()
	handler := func() {}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handlers.Add(handler)
	}
}

func BenchmarkEventHandlersForEach(b *testing.B) {
	handlers := NewEventHandlers[func()]()
	
	// Pre-populate with handlers
	for i := 0; i < 100; i++ {
		handlers.Add(func() {})
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handlers.ForEach(func(h func()) {
			// Just iterate, don't call
		})
	}
}

func BenchmarkEventHandlersForEachWithCall(b *testing.B) {
	handlers := NewEventHandlers[func()]()
	
	// Pre-populate with handlers
	for i := 0; i < 100; i++ {
		handlers.Add(func() {})
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handlers.ForEach(func(h func()) {
			h()
		})
	}
}

func BenchmarkEventHandlersRemove(b *testing.B) {
	handlers := NewEventHandlers[func()]()
	
	// Pre-populate and collect IDs
	ids := make([]HandlerID, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = handlers.Add(func() {})
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handlers.Remove(ids[i])
	}
}

func BenchmarkEventHandlersConcurrentForEach(b *testing.B) {
	handlers := NewEventHandlers[func()]()
	
	// Pre-populate with handlers
	for i := 0; i < 1000; i++ {
		handlers.Add(func() {})
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			handlers.ForEach(func(h func()) {
				// Just iterate
			})
		}
	})
}

func BenchmarkEventHandlersConcurrentAdd(b *testing.B) {
	handlers := NewEventHandlers[func()]()
	handler := func() {}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			handlers.Add(handler)
		}
	})
}

func BenchmarkEventHandlersMixed(b *testing.B) {
	handlers := NewEventHandlers[func()]()
	handler := func() {}
	
	// Pre-populate
	var ids []HandlerID
	for i := 0; i < 100; i++ {
		ids = append(ids, handlers.Add(handler))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 3 {
		case 0:
			handlers.Add(handler)
		case 1:
			handlers.ForEach(func(h func()) {})
		case 2:
			if len(ids) > 0 {
				handlers.Remove(ids[i%len(ids)])
			}
		}
	}
}