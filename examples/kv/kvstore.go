package main

import (
	"encoding/json"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/kv"
)

// KVStore is a thin, string-shaped wrapper around the kv package for this
// interactive example. Real callers use kv.NewStore directly: the store is
// scoped to a Membership (the whole cluster here, a NodeGroup for a zone),
// acknowledged writes are durable on WriteReplicas nodes, and reads are local.
type KVStore struct {
	store *kv.Store
}

// NewKVStore creates a cluster-scoped store with the default configuration.
func NewKVStore(cluster *gossip.Cluster) *KVStore {
	return &KVStore{
		store: kv.NewStore(cluster, kv.ClusterMembership{Cluster: cluster}, kv.DefaultConfig()),
	}
}

// Stop closes the store.
func (kv *KVStore) Stop() {
	kv.store.Close()
}

// Set stores a string value; the write is durable on WriteReplicas nodes
// before it returns.
func (kv *KVStore) Set(key, value string) error {
	return kv.store.Set(key, []byte(value), 0)
}

// Get retrieves a string value.
func (kv *KVStore) Get(key string) (string, bool) {
	v, ok := kv.store.Get(key)
	return string(v), ok
}

// Delete removes a key.
func (kv *KVStore) Delete(key string) error {
	return kv.store.Delete(key)
}

// Keys returns all live keys.
func (kv *KVStore) Keys() []string {
	return kv.store.Keys("")
}

// RequestFullSync runs one bidirectional full-sync exchange with the peers.
func (kv *KVStore) RequestFullSync() {
	kv.store.Sync()
}

// Dump returns the current key-value state (for debugging).
func (kv *KVStore) Dump() map[string]string {
	out := make(map[string]string)
	for _, k := range kv.store.Keys("") {
		if v, ok := kv.store.Get(k); ok {
			out[k] = string(v)
		}
	}
	return out
}

// DumpJSON returns a JSON representation of the store (for debugging).
func (kv *KVStore) DumpJSON() (string, error) {
	bytes, err := json.MarshalIndent(kv.Dump(), "", "  ")
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
