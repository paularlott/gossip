package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

const (
	// Message types
	KVFullSyncMsg gossip.MessageType = iota + gossip.UserMsg
	KVSyncMsg

	// Configuration
	defaultRetentionTime = 2 * time.Hour
)

// ValueVersion represents version information for a key-value pair
type ValueVersion struct {
	Timestamp int64  `msgpack:"ts" json:"ts"`   // Nanosecond timestamp
	Version   uint16 `msgpack:"ver" json:"ver"` // Version counter
}

// ValueState represents a value and its metadata
type ValueState struct {
	Value   interface{}  `msgpack:"val" json:"val"`
	Version ValueVersion `msgpack:"v" json:"v"`
	Deleted bool         `msgpack:"d" json:"d"`
}

// SyncPayload is the data structure sent during gossip sync
type SyncPayload struct {
	Entries map[string]ValueState `msgpack:"e" json:"e"`
}

// KVStore implements a distributed key-value store using gossip protocol
type KVStore struct {
	data          map[string]ValueState
	cluster       *gossip.Cluster
	mu            sync.RWMutex
	retentionTime time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	source        *rand.Rand
}

// NewKVStore creates a new KV store instance
func NewKVStore(cluster *gossip.Cluster) *KVStore {

	ctx, cancel := context.WithCancel(context.Background())

	store := &KVStore{
		data:          make(map[string]ValueState),
		cluster:       cluster,
		retentionTime: defaultRetentionTime,
		ctx:           ctx,
		cancel:        cancel,
		source:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// Register message handlers
	cluster.HandleFuncWithReply(KVFullSyncMsg, store.handleFullSync)
	cluster.HandleFunc(KVSyncMsg, store.handleSync)
	cluster.HandleGossipFunc(store.syncRandomSubset)

	// Start garbage collection goroutine
	go store.periodicGC()

	return store
}

// Stop terminates the periodic processes
func (kv *KVStore) Stop() {
	kv.cancel()
}

// Set stores a value with the given key
func (kv *KVStore) Set(key string, value interface{}) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	now := time.Now().UnixNano()
	var version uint16 = 0

	// Check if key exists to handle versioning
	if existingValue, exists := kv.data[key]; exists {
		// If timestamp is the same, increment version
		if existingValue.Version.Timestamp == now {
			version = existingValue.Version.Version + 1
		}
		// Otherwise version remains 0 for new timestamp
	}

	kv.data[key] = ValueState{
		Value: value,
		Version: ValueVersion{
			Timestamp: now,
			Version:   version,
		},
		Deleted: false,
	}

	// Broadcast the change to other nodes
	kv.cluster.Send(KVSyncMsg, SyncPayload{
		Entries: map[string]ValueState{
			key: kv.data[key],
		},
	})
}

// Get retrieves a value for the given key
func (kv *KVStore) Get(key string) (interface{}, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if entry, exists := kv.data[key]; exists && !entry.Deleted {
		return entry.Value, true
	}
	return nil, false
}

// Delete marks a key as deleted
func (kv *KVStore) Delete(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// If the key exists, mark it as deleted
	if existingValue, exists := kv.data[key]; exists {
		now := time.Now().UnixNano()
		var version uint16 = 0

		// If timestamp is the same, increment version
		if existingValue.Version.Timestamp == now {
			version = existingValue.Version.Version + 1
		}

		kv.data[key] = ValueState{
			Value: nil,
			Version: ValueVersion{
				Timestamp: now,
				Version:   version,
			},
			Deleted: true,
		}

		// Broadcast the change to other nodes
		kv.cluster.Send(KVSyncMsg, SyncPayload{
			Entries: map[string]ValueState{
				key: kv.data[key],
			},
		})
	}
}

// Keys returns a slice of all non-deleted keys
func (kv *KVStore) Keys() []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	var keys []string
	for k, v := range kv.data {
		if !v.Deleted {
			keys = append(keys, k)
		}
	}
	return keys
}

// syncRandomSubset selects a random subset of keys and gossips them
func (kv *KVStore) syncRandomSubset() {
	// Get the batch size early to avoid holding the lock while calculating it
	totalKeys := kv.getTotalKeyCount()
	if totalKeys == 0 {
		return // No data to sync
	}

	batchSize := kv.cluster.GetBatchSize(totalKeys)
	if batchSize == 0 {
		return // No keys to send in this batch
	}

	subset := make([]string, 0, batchSize)
	keysSeen := 0

	kv.mu.RLock()
	for key := range kv.data {
		keysSeen++

		if len(subset) < batchSize {
			subset = append(subset, key)
		} else {
			j := kv.source.Intn(keysSeen)
			if j < batchSize {
				subset[j] = key
			}
		}
	}
	kv.mu.RUnlock()

	if len(subset) == 0 {
		return // No keys to sync
	}

	// Create the sync payload
	payload := SyncPayload{
		Entries: make(map[string]ValueState, len(subset)),
	}

	kv.mu.RLock()
	for _, key := range subset {
		if entry, exists := kv.data[key]; exists {
			payload.Entries[key] = entry
		}
	}
	kv.mu.RUnlock()

	// Broadcast to the cluster
	if len(payload.Entries) > 0 {
		kv.cluster.Send(KVSyncMsg, payload)
	}
}

func (kv *KVStore) getTotalKeyCount() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}

// RequestFullSync requests a full data sync from random nodes
func (kv *KVStore) RequestFullSync() {
	nodes := kv.cluster.GetCandidates()

	// Try each node in order until we get a successful response
	for _, node := range nodes {
		var syncPayload SyncPayload
		err := kv.cluster.SendToWithResponse(node, KVFullSyncMsg, nil, &syncPayload)
		if err != nil {
			// Log error and try next node
			fmt.Printf("Failed to get sync from %s: %v\n", node.ID, err)
			continue
		}

		// If we got a response, merge the data
		if len(syncPayload.Entries) > 0 {
			kv.mergeEntries(syncPayload.Entries)
			return // Successfully got data, no need to continue
		}
	}
}

// handleFullSync handles a request for full synchronization and returns the full dataset
func (kv *KVStore) handleFullSync(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	payload := SyncPayload{
		Entries: make(map[string]ValueState, len(kv.data)),
	}

	// Copy all entries
	for k, v := range kv.data {
		payload.Entries[k] = v
	}

	// Return the full dataset directly as response
	return &payload, nil
}

// handleSync processes incoming sync messages
func (kv *KVStore) handleSync(sender *gossip.Node, packet *gossip.Packet) error {
	var payload SyncPayload
	if err := packet.Unmarshal(&payload); err != nil {
		return fmt.Errorf("failed to unmarshal sync payload: %w", err)
	}

	kv.mergeEntries(payload.Entries)
	return nil
}

// mergeEntries merges received entries with local data
func (kv *KVStore) mergeEntries(entries map[string]ValueState) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for key, incomingEntry := range entries {
		// Check if we have this key
		existingEntry, exists := kv.data[key]

		// Apply the update if:
		// 1. We don't have this key yet, or
		// 2. Incoming entry has a newer timestamp, or
		// 3. Timestamps match but incoming has higher version
		if !exists ||
			incomingEntry.Version.Timestamp > existingEntry.Version.Timestamp ||
			(incomingEntry.Version.Timestamp == existingEntry.Version.Timestamp &&
				incomingEntry.Version.Version > existingEntry.Version.Version) {

			// Update with a copy of the entry
			kv.data[key] = incomingEntry
		}
	}
}

// periodicGC runs garbage collection to remove old deleted entries
func (kv *KVStore) periodicGC() {
	ticker := time.NewTicker(kv.retentionTime / 10) // Run GC more frequently than the retention time
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			kv.garbageCollect()
		case <-kv.ctx.Done():
			return
		}
	}
}

// garbageCollect removes entries that have been deleted and older than retention time
func (kv *KVStore) garbageCollect() {
	now := time.Now()
	thresholdNano := now.Add(-kv.retentionTime).UnixNano()

	kv.mu.Lock()
	defer kv.mu.Unlock()

	for key, entry := range kv.data {
		if entry.Deleted && entry.Version.Timestamp < thresholdNano {
			delete(kv.data, key)
		}
	}
}

// Dump returns a copy of the current key-value state (for debugging)
func (kv *KVStore) Dump() map[string]interface{} {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	result := make(map[string]interface{})
	for k, v := range kv.data {
		if !v.Deleted {
			result[k] = v.Value
		}
	}
	return result
}

// DumpJSON returns a JSON representation of the KV store (for debugging)
func (kv *KVStore) DumpJSON() (string, error) {
	dump := kv.Dump()
	bytes, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
