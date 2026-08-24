package lock

import (
	"fmt"
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

// registry is a per-cluster singleton. It registers the protocol handlers once
// and dispatches to the right pool by name, so several named pools can share one
// set of message types.
type registry struct {
	mu      sync.RWMutex
	pools   map[string]*Pool
	cluster *gossip.Cluster
}

var (
	registriesMu sync.Mutex
	registries   = make(map[*gossip.Cluster]*registry)
)

func getOrCreateRegistry(cluster *gossip.Cluster) *registry {
	registriesMu.Lock()
	defer registriesMu.Unlock()

	if r, ok := registries[cluster]; ok {
		return r
	}

	r := &registry{
		pools:   make(map[string]*Pool),
		cluster: cluster,
	}

	must := func(t gossip.MessageType, h gossip.ReplyHandler, what string) {
		if err := cluster.HandleFuncWithReply(t, h); err != nil {
			panic(fmt.Sprintf("lock: failed to register %s handler: %v", what, err))
		}
	}
	must(lockAcquireMsg, r.handleAcquire, "acquire")
	must(lockReleaseMsg, r.handleRelease, "release")
	must(lockExtendMsg, r.handleExtend, "extend")
	must(lockQueryMsg, r.handleQuery, "query")
	must(lockReplicaPushMsg, r.handleReplicaPush, "replica push")
	must(lockStateQueryMsg, r.handleStateQuery, "state query")
	if err := cluster.HandleFunc(lockReplicaGossip, r.handleReplicaGossip); err != nil {
		panic(fmt.Sprintf("lock: failed to register replica gossip handler: %v", err))
	}

	registries[cluster] = r
	return r
}

func (r *registry) registerPool(name string, p *Pool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.pools[name]; exists {
		panic(fmt.Sprintf("lock: pool %q already registered on this cluster", name))
	}
	r.pools[name] = p
}

// unregisterPool removes a pool. When the last one goes, the shared handlers are
// torn down too.
//
// registriesMu is held throughout so a concurrent NewPool cannot pick up this
// registry and then have its handlers removed underneath it.
func (r *registry) unregisterPool(name string) {
	registriesMu.Lock()
	defer registriesMu.Unlock()

	r.mu.Lock()
	delete(r.pools, name)
	remaining := len(r.pools)
	r.mu.Unlock()

	if remaining > 0 {
		return
	}

	r.cluster.UnregisterMessageType(lockAcquireMsg)
	r.cluster.UnregisterMessageType(lockReleaseMsg)
	r.cluster.UnregisterMessageType(lockExtendMsg)
	r.cluster.UnregisterMessageType(lockQueryMsg)
	r.cluster.UnregisterMessageType(lockReplicaPushMsg)
	r.cluster.UnregisterMessageType(lockReplicaGossip)
	r.cluster.UnregisterMessageType(lockStateQueryMsg)

	delete(registries, r.cluster)
}

func (r *registry) getPool(name string) *Pool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pools[name]
}

// leaderPool resolves a pool by name and reports whether this node can serve
// requests for it right now. A node that is leader but still recovering replica
// state is reported separately: the truth about held locks is not yet known, so
// requests must be refused as transient rather than answered from an incomplete
// table.
func (r *registry) leaderPool(name string) (p *Pool, serving bool, recovering bool) {
	p = r.getPool(name)
	if p == nil || !p.leadership.IsLeader() {
		return nil, false, false
	}
	if !p.tbl.servingNow() {
		return p, false, true
	}
	return p, true, false
}

func (r *registry) handleAcquire(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req acquireRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	p, serving, recovering := r.leaderPool(req.PoolName)
	if p == nil {
		return &acquireResponse{Granted: false, Reason: reasonNotLeader}, nil
	}
	if recovering {
		return &acquireResponse{Granted: false, Reason: reasonRecovering}, nil
	}
	if !serving {
		return &acquireResponse{Granted: false, Reason: reasonNotLeader}, nil
	}

	ttl := time.Duration(req.TTLMs) * time.Millisecond
	if err := p.validateTTL(ttl); err != nil {
		return &acquireResponse{Granted: false, Reason: err.Error()}, nil
	}
	ent, granted, reason := p.tbl.tryAcquire(req.Key, req.RequesterID, ttl)
	if !granted {
		return &acquireResponse{Granted: false, Reason: reason}, nil
	}

	if err := p.replicateEntry(ent); err != nil {
		p.compensate(ent)
		return &acquireResponse{Granted: false, Reason: reasonWriteQuorum}, nil
	}

	return &acquireResponse{Granted: true, Token: ent.Token}, nil
}

func (r *registry) handleRelease(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req releaseRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	p, serving, recovering := r.leaderPool(req.PoolName)
	if p == nil {
		return &releaseResponse{Released: false, Reason: reasonNotLeader}, nil
	}
	if recovering {
		return &releaseResponse{Released: false, Reason: reasonRecovering}, nil
	}
	if !serving {
		return &releaseResponse{Released: false, Reason: reasonNotLeader}, nil
	}

	tomb, released, reason := p.tbl.release(req.Key, req.Token)
	if !released {
		return &releaseResponse{Released: false, Reason: reason}, nil
	}
	if !tomb.Token.IsZero() {
		if err := p.replicateEntry(tomb); err != nil {
			// The tombstone exists locally and wherever it did land; report
			// so the caller knows it was not made durable.
			return &releaseResponse{Released: false, Reason: reasonWriteQuorum}, nil
		}
	}
	return &releaseResponse{Released: true}, nil
}

func (r *registry) handleExtend(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req extendRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	p, serving, recovering := r.leaderPool(req.PoolName)
	if p == nil {
		return &extendResponse{Extended: false, Reason: reasonNotLeader}, nil
	}
	if recovering {
		return &extendResponse{Extended: false, Reason: reasonRecovering}, nil
	}
	if !serving {
		return &extendResponse{Extended: false, Reason: reasonNotLeader}, nil
	}

	ttl := time.Duration(req.TTLMs) * time.Millisecond
	if err := p.validateTTL(ttl); err != nil {
		return &extendResponse{Extended: false, Reason: err.Error()}, nil
	}
	ent, extended, reason := p.tbl.extend(req.Key, req.Token, ttl)
	if !extended {
		return &extendResponse{Extended: false, Reason: reason}, nil
	}

	if err := p.replicateEntry(ent); err != nil {
		return &extendResponse{Extended: false, Reason: reasonWriteQuorum}, nil
	}
	return &extendResponse{Extended: true}, nil
}

func (r *registry) handleQuery(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req queryRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	p, serving, recovering := r.leaderPool(req.PoolName)
	if p == nil {
		return &queryResponse{Held: false, Reason: reasonNotLeader}, nil
	}
	if recovering {
		return &queryResponse{Held: false, Reason: reasonRecovering}, nil
	}
	if !serving {
		return &queryResponse{Held: false, Reason: reasonNotLeader}, nil
	}

	held, owner, tok, ms, ready := p.tbl.query(req.Key)
	if !ready {
		return &queryResponse{Held: false, Reason: reasonRecovering}, nil
	}
	return &queryResponse{Held: held, Owner: owner, Token: tok, TTLMs: ms}, nil
}

// handleReplicaPush applies durable writes from the leader. Accepted whether
// or not this node considers itself leader — replicas hold state at all times.
func (r *registry) handleReplicaPush(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req replicaPush
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	p := r.getPool(req.PoolName)
	if p == nil {
		return &replicaAck{Applied: false}, nil
	}

	p.tbl.applyAll(req.Entries)
	return &replicaAck{Applied: true}, nil
}

// handleReplicaGossip applies a fire-and-forget entry broadcast. Same merge
// rules as a push; no reply is expected.
func (r *registry) handleReplicaGossip(sender *gossip.Node, packet *gossip.Packet) error {
	var msg replicaGossipBroadcast
	if err := packet.Unmarshal(&msg); err != nil {
		return err
	}

	if p := r.getPool(msg.PoolName); p != nil {
		p.tbl.applyAll(msg.Entries)
	}
	return nil
}

// handleStateQuery answers a newly elected leader's recovery query with this
// node's current replica view, tombstones included — they are what stop a
// released grant being resurrected by a node that never saw the release.
func (r *registry) handleStateQuery(sender *gossip.Node, packet *gossip.Packet) (interface{}, error) {
	var req stateQueryRequest
	if err := packet.Unmarshal(&req); err != nil {
		return nil, err
	}

	p := r.getPool(req.PoolName)
	if p == nil {
		return &stateQueryResponse{}, nil
	}

	return &stateQueryResponse{Entries: p.tbl.snapshot()}, nil
}
