package gossip

import (
	"testing"
)

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
