package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

const (
	// Message types
	KVSyncMsg gossip.MessageType = iota + gossip.UserMsg
	KVFullSyncMsg

	// Configuration
	defaultSyncInterval  = 10 * time.Second
	defaultRetentionTime = 2 * time.Hour
)

// ValueState represents a value and its metadata
type ValueState struct {
	Value     interface{} `msgpack:"value" json:"value"`
	Timestamp time.Time   `msgpack:"timestamp" json:"timestamp"`
	Deleted   bool        `msgpack:"deleted" json:"deleted"`
	Version   uint64      `msgpack:"version" json:"version"`
}

// SyncPayload is the data structure sent during gossip sync
type SyncPayload struct {
	Entries map[string]ValueState `msgpack:"entries" json:"entries"`
}

// KVStore implements a distributed key-value store using gossip protocol
type KVStore struct {
	data          map[string]ValueState
	cluster       *gossip.Cluster
	nodeID        gossip.NodeID
	mu            sync.RWMutex
	syncInterval  time.Duration
	retentionTime time.Duration
	quit          chan struct{}
	version       uint64
}

// NewKVStore creates a new KV store instance
func NewKVStore(cluster *gossip.Cluster) *KVStore {
	store := &KVStore{
		data:          make(map[string]ValueState),
		cluster:       cluster,
		syncInterval:  defaultSyncInterval,
		retentionTime: defaultRetentionTime,
		quit:          make(chan struct{}),
	}

	// Register message handlers
	cluster.HandleFunc(KVSyncMsg, store.handleSync)
	cluster.HandleFunc(KVFullSyncMsg, store.handleFullSync)

	return store
}

// Start begins the periodic sync process and garbage collection
func (kv *KVStore) Start() {
	// Start sync goroutine
	go kv.periodicSync()

	// Start garbage collection goroutine
	go kv.periodicGC()
}

// Stop terminates the periodic processes
func (kv *KVStore) Stop() {
	close(kv.quit)
}

// Set stores a value with the given key
func (kv *KVStore) Set(key string, value interface{}) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.version++
	kv.data[key] = ValueState{
		Value:     value,
		Timestamp: time.Now(),
		Deleted:   false,
		Version:   kv.version,
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
	if _, exists := kv.data[key]; exists {
		kv.version++
		kv.data[key] = ValueState{
			Value:     nil,
			Timestamp: time.Now(),
			Deleted:   true,
			Version:   kv.version,
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

// periodicSync sends a subset of data to other nodes periodically
func (kv *KVStore) periodicSync() {
	ticker := time.NewTicker(kv.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			kv.syncRandomSubset()
		case <-kv.quit:
			return
		}
	}
}

// syncRandomSubset selects a random subset of keys and gossips them
func (kv *KVStore) syncRandomSubset() {
	kv.mu.RLock()

	// Quick check if we have data
	if len(kv.data) == 0 {
		kv.mu.RUnlock()
		return
	}

	// Select keys for this sync
	keys := make([]string, 0, len(kv.data))
	for k := range kv.data {
		keys = append(keys, k)
	}
	kv.mu.RUnlock()

	// Shuffle the keys
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	// Take a subset
	batchSize := kv.cluster.GetBatchSize(len(keys))
	subset := keys[:batchSize]

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
	kv.cluster.Send(KVSyncMsg, payload)
}

// RequestFullSync requests a full data sync from a random nodes
func (kv *KVStore) RequestFullSync() {
	nodes := kv.cluster.GetCandidates()

	// Send empty sync message which will trigger a full response
	for _, node := range nodes {
		kv.cluster.SendTo(node, KVFullSyncMsg, nil)
	}
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

// handleFullSync handles a request for full synchronization
func (kv *KVStore) handleFullSync(sender *gossip.Node, packet *gossip.Packet) error {
	kv.sendFullSyncToNode(sender)
	return nil
}

// sendFullSyncToNode sends the complete dataset to a specific node
func (kv *KVStore) sendFullSyncToNode(node *gossip.Node) {
	kv.mu.RLock()
	payload := SyncPayload{
		Entries: make(map[string]ValueState, len(kv.data)),
	}

	// Copy all entries
	for k, v := range kv.data {
		payload.Entries[k] = v
	}
	kv.mu.RUnlock()

	kv.cluster.SendTo(node, KVSyncMsg, payload)
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
			incomingEntry.Timestamp.After(existingEntry.Timestamp) ||
			(incomingEntry.Timestamp.Equal(existingEntry.Timestamp) && incomingEntry.Version > existingEntry.Version) {

			// Update with a copy of the entry
			kv.data[key] = incomingEntry

			// Update our version counter if needed
			if incomingEntry.Version > kv.version {
				kv.version = incomingEntry.Version
			}
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
		case <-kv.quit:
			return
		}
	}
}

// garbageCollect removes entries that have been deleted and older than retention time
func (kv *KVStore) garbageCollect() {
	now := time.Now()
	threshold := now.Add(-kv.retentionTime)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	for key, entry := range kv.data {
		if entry.Deleted && entry.Timestamp.Before(threshold) {
			delete(kv.data, key)
		}
	}
}

// SetSyncInterval changes the gossip synchronization interval
func (kv *KVStore) SetSyncInterval(d time.Duration) {
	kv.syncInterval = d
}

// SetRetentionTime changes the tombstone retention duration
func (kv *KVStore) SetRetentionTime(d time.Duration) {
	kv.retentionTime = d
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
