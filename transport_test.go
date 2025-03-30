package gossip

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNextMessageIDConcurrent(t *testing.T) {
	// Create a minimal config for testing
	config := &Config{
		BindAddr: "127.0.0.1:0", // Use port 0 to let the OS assign a free port
	}

	// Create a transport instance
	trans, err := newTransport(config)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}
	defer trans.stop()

	// Number of goroutines to spawn
	const numGoroutines = 100
	// Number of IDs each goroutine will generate
	const idsPerGoroutine = 1000

	// Channel to collect all message IDs
	allIDs := make(chan MessageID, numGoroutines*idsPerGoroutine)

	// WaitGroup to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Start goroutines that generate message IDs
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				id := trans.nextMessageID()
				allIDs <- id
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(allIDs)

	// Collect all generated IDs
	var ids []MessageID
	for id := range allIDs {
		ids = append(ids, id)
	}

	// Verify no duplicate IDs
	t.Run("No duplicate IDs", func(t *testing.T) {
		seen := make(map[string]bool)
		for _, id := range ids {
			// Create a unique string representation of the ID
			key := idKey(id)
			if seen[key] {
				t.Errorf("Duplicate ID found: %+v", id)
				return
			}
			seen[key] = true
		}
	})

	// Verify sequence numbers properly increment
	t.Run("Sequence numbers correctly assigned", func(t *testing.T) {
		// Group IDs by timestamp
		timestampGroups := make(map[int64][]uint16)
		for _, id := range ids {
			timestampGroups[id.Timestamp] = append(timestampGroups[id.Timestamp], id.Seq)
		}

		// For each timestamp, sequence numbers should be sequential without gaps
		for timestamp, sequences := range timestampGroups {
			// Sort the sequences for this timestamp
			seqNums := make([]uint16, len(sequences))
			copy(seqNums, sequences)
			if !validateSequence(seqNums) {
				t.Errorf("Sequence numbers for timestamp %d are not sequential: %v", timestamp, sequences)
			}
		}
	})

	// Verify timestamps are reasonable (all within test execution window)
	t.Run("Timestamps are reasonable", func(t *testing.T) {
		now := time.Now().UnixNano()
		testStartTime := now - int64(10*time.Second)

		for _, id := range ids {
			if id.Timestamp < testStartTime || id.Timestamp > now {
				t.Errorf("MessageID has unreasonable timestamp: %d (now: %d)", id.Timestamp, now)
			}
		}
	})

	// Test for sequence overflow (harder to trigger but can simulate)
	t.Run("Sequence overflow handled correctly", func(t *testing.T) {
		// Create a transport with a MessageID that has sequence number at max
		transport2 := &transport{
			config:        config,
			packetChannel: make(chan *incomingPacket, 10),
		}

		// Set initial message ID with sequence at max value
		initialID := &MessageID{
			Timestamp: time.Now().UnixNano(),
			Seq:       65535, // Max uint16
		}
		transport2.messageIdGen.Store(initialID)

		// Generate next ID, should handle overflow
		nextID := transport2.nextMessageID()

		// If timestamp is the same, sequence should wrap to 0
		if nextID.Timestamp == initialID.Timestamp {
			if nextID.Seq != 0 {
				t.Errorf("Sequence number did not wrap properly on overflow. Expected 0, got %d", nextID.Seq)
			}
		} else {
			// If timestamp changed, sequence should be 0
			if nextID.Seq != 0 {
				t.Errorf("Sequence number should be 0 for new timestamp, got %d", nextID.Seq)
			}
		}
	})
}

// Helper function to create a string key from a MessageID for uniqueness checking
func idKey(id MessageID) string {
	return fmt.Sprintf("%d-%d", id.Timestamp, id.Seq)
}

// Helper function to check if a sequence of uint16 values contains all
// consecutive numbers from 0 to len(seq)-1
func validateSequence(seq []uint16) bool {
	// Create a map to track seen numbers
	seen := make(map[uint16]bool)

	// Find min and max
	var min, max uint16 = 65535, 0
	for _, num := range seq {
		seen[num] = true
		if num < min {
			min = num
		}
		if num > max {
			max = num
		}
	}

	// Check if all numbers between min and max are present
	for i := min; i <= max; i++ {
		if !seen[i] {
			return false
		}
	}

	return true
}
