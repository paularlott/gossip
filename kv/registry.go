package kv

import (
	"fmt"
	"sync"

	"github.com/paularlott/gossip"
)

// registry is a per-cluster singleton. It registers the protocol handlers
// once and dispatches to the right store by name, so several named stores
// can share one set of message types.
type registry struct {
	mu      sync.RWMutex
	stores  map[string]*Store
	cluster *gossip.Cluster
}

var (
	registriesMu sync.Mutex
	kvRegistries = make(map[*gossip.Cluster]*registry)
)

func getOrCreateRegistry(cluster *gossip.Cluster) *registry {
	registriesMu.Lock()
	defer registriesMu.Unlock()

	if r, ok := kvRegistries[cluster]; ok {
		return r
	}

	r := &registry{
		stores:  make(map[string]*Store),
		cluster: cluster,
	}

	must := func(t gossip.MessageType, h gossip.ReplyHandler, what string) {
		if err := cluster.HandleFuncWithReply(t, h); err != nil {
			panic(fmt.Sprintf("kv: failed to register %s handler: %v", what, err))
		}
	}
	must(kvWritePushMsg, r.handleWritePush, "write push")
	must(kvFullSyncMsg, r.handleFullSync, "full sync")
	if err := cluster.HandleFunc(kvGossipMsg, r.handleGossip); err != nil {
		panic(fmt.Sprintf("kv: failed to register gossip handler: %v", err))
	}

	kvRegistries[cluster] = r
	return r
}

func (r *registry) registerStore(name string, s *Store) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.stores[name]; exists {
		panic(fmt.Sprintf("kv: store %q already registered on this cluster", name))
	}
	r.stores[name] = s
}

// unregisterStore removes a store. When the last one goes, the shared
// handlers are torn down too.
//
// registriesMu is held throughout so a concurrent NewStore cannot pick up
// this registry and then have its handlers removed underneath it.
func (r *registry) unregisterStore(name string) {
	registriesMu.Lock()
	defer registriesMu.Unlock()

	r.mu.Lock()
	delete(r.stores, name)
	remaining := len(r.stores)
	r.mu.Unlock()

	if remaining > 0 {
		return
	}

	r.cluster.UnregisterMessageType(kvWritePushMsg)
	r.cluster.UnregisterMessageType(kvGossipMsg)
	r.cluster.UnregisterMessageType(kvFullSyncMsg)

	delete(kvRegistries, r.cluster)
}

func (r *registry) getStore(name string) *Store {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stores[name]
}

// handleWritePush applies a durable write batch from a peer. Accepted by
// every member of the store's scope — the protocol is leaderless, so any
// member's push is merged by version rules.
func (r *registry) handleWritePush(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req writePush
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	s := r.getStore(req.StoreName)
	if s == nil {
		return &writeAck{Applied: false}, nil
	}

	s.adopt(req.Entries)
	return &writeAck{Applied: true}, nil
}

// handleGossip applies a fire-and-forget entry broadcast. Same merge rules
// as a push; no reply is expected.
func (r *registry) handleGossip(sender *gossip.Node, packet *gossip.Packet) error {
	var msg gossipBroadcast
	if err := packet.Unmarshal(&msg); err != nil {
		return err
	}

	if s := r.getStore(msg.StoreName); s != nil {
		s.adopt(msg.Entries)
	}
	return nil
}

// handleFullSync answers a catch-up exchange: the requester's snapshot is
// merged locally, and this node's snapshot is returned for the requester to
// merge — both sides converge on the union.
func (r *registry) handleFullSync(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req fullSyncRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	s := r.getStore(req.StoreName)
	if s == nil {
		return &fullSyncResponse{}, nil
	}

	s.adopt(req.Entries)
	return &fullSyncResponse{Entries: s.tbl.snapshot()}, nil
}
