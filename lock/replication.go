package lock

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

// stateQueryConcurrency caps the fan-out of recovery queries so a very large
// group does not open a connection per node at once.
const stateQueryConcurrency = 64

// replicationTargets returns the pool's peers as candidate replica targets,
// drawn from the election's candidate list — group-scoped for a group-scoped
// election. The local node is excluded — it is already one of the W replicas.
func (p *Pool) replicationTargets() []*gossip.Node {
	nodes := p.leadership.Candidates()

	self := p.cluster.LocalNode().ID
	out := make([]*gossip.Node, 0, len(nodes))
	for _, n := range nodes {
		if n == nil || n.ID == self {
			continue
		}
		if n.GetObservedState() != gossip.NodeAlive {
			continue
		}
		out = append(out, n)
	}
	return out
}

// replicateEntry makes a single mutation durable (see replicateBatch).
func (p *Pool) replicateEntry(ent replicaEntry) error {
	return p.replicateBatch([]replicaEntry{ent})
}

// replicateBatch makes a set of mutations durable: it pushes the entries to
// peers in parallel and requires WriteReplicas-1 of them to ack within the
// timeout, retrying once against replacement peers before giving up.
//
// When the group has no peers (single-node cluster) or WriteReplicas is 1, the
// leader's own copy is the whole replica set and the push is skipped. When the
// group is smaller than WriteReplicas the requirement degrades to what the
// group can provide — group size, never peer failures, lowers the bar.
func (p *Pool) replicateBatch(entries []replicaEntry) error {
	targets := p.replicationTargets()
	if len(entries) == 0 {
		return nil
	}
	if len(targets) == 0 || p.config.WriteReplicas <= 1 {
		// Nothing to push for durability; still spread best-effort so a
		// later-joining peer can pick the state up. Fire-and-forget.
		go p.gossipEntries(entries)
		return nil
	}

	rand.Shuffle(len(targets), func(i, j int) { targets[i], targets[j] = targets[j], targets[i] })

	need := p.config.WriteReplicas - 1
	if need > len(targets) {
		need = len(targets) // degrade: the group is smaller than W
	}
	// Push to one more than required when available, so a single failed ack
	// does not cost the round.
	pushTo := p.config.WriteReplicas
	if pushTo > len(targets) {
		pushTo = len(targets)
	}

	got := p.pushForAcks(targets[:pushTo], entries, p.config.ReplicationTimeout)
	if got >= need {
		go p.gossipEntries(entries)
		return nil
	}

	// One retry round against replacement peers.
	if rest := targets[pushTo:]; len(rest) > 0 {
		next := rest
		if len(next) > pushTo {
			next = next[:pushTo]
		}
		got += p.pushForAcks(next, entries, p.config.ReplicationTimeout)
	}
	if got >= need {
		go p.gossipEntries(entries)
		return nil
	}

	return fmt.Errorf("%w: need %d replica acks, got %d", ErrWriteQuorum, need, got)
}

// pushForAcks sends the entries to each target in parallel and counts acks that
// arrive within the deadline. Acks that arrive late are still applied on the
// peer — only this node's count misses them — so under-counting is always the
// conservative direction.
func (p *Pool) pushForAcks(targets []*gossip.Node, entries []replicaEntry, timeout time.Duration) int {
	type result struct{}
	acks := make(chan result, len(targets))

	for _, n := range targets {
		go func(n *gossip.Node) {
			req := &replicaPush{PoolName: p.config.Name, Entries: entries}
			var ack replicaAck
			if err := p.cluster.SendToWithResponse(n, lockReplicaPushMsg, req, &ack); err == nil && ack.Applied {
				select {
				case acks <- result{}:
				default:
				}
			}
		}(n)
	}

	deadline := time.After(timeout)
	count := 0
	for count < len(targets) {
		select {
		case <-acks:
			count++
		case <-deadline:
			return count
		case <-p.stopCh:
			return count
		}
	}
	return count
}

// gossipEntries spreads entries to the pool's candidates as fire-and-forget
// direct sends — scoped to the group for a group-scoped election, and to the
// alive nodes for a cluster-wide one. This is recovery-hit-rate insurance,
// never load-bearing: it raises steady-state replication beyond the W write
// replicas so a later recovery merge finds copies even when the original
// replica set has thinned. Lost deliveries are healed by the anti-entropy
// re-gossip.
func (p *Pool) gossipEntries(entries []replicaEntry) {
	if len(entries) == 0 {
		return
	}
	peers := p.replicationTargets()
	if len(peers) == 0 {
		return
	}

	for _, batch := range chunkEntries(entries, p.entriesPerPacket(len(entries))) {
		msg := &replicaGossipBroadcast{PoolName: p.config.Name, Entries: batch}
		for _, peer := range peers {
			if err := p.cluster.SendTo(peer, lockReplicaGossip, msg); err != nil {
				p.cluster.Logger().WithError(err).Debug("lock: replica gossip send failed")
				return
			}
		}
	}
}

// chunkEntries splits entries into payload-sized batches.
func chunkEntries(entries []replicaEntry, per int) [][]replicaEntry {
	if per <= 0 || len(entries) <= per {
		return [][]replicaEntry{entries}
	}
	var out [][]replicaEntry
	for i := 0; i < len(entries); i += per {
		end := i + per
		if end > len(entries) {
			end = len(entries)
		}
		out = append(out, entries[i:end])
	}
	return out
}

// entriesPerPacket is how many entries fit in one gossip payload, following
// the cluster's state-exchange sizing.
func (p *Pool) entriesPerPacket(total int) int {
	if per := p.cluster.CalcPayloadSize(total); per > 0 {
		return per
	}
	return total
}

// catchUp pulls the state of every candidate peer into the local replica
// store. Run when the pool starts or joins, so a late-arriving node becomes a
// useful replica immediately rather than waiting for the next leadership
// change. Returns true once any peer has answered.
func (p *Pool) catchUp() bool {
	targets := p.replicationTargets()
	if len(targets) == 0 {
		return false
	}

	deadline := time.NewTimer(p.config.RecoveryTimeout)
	defer deadline.Stop()

	answered := false
	sent := make(chan struct{}, len(targets))
	for _, n := range targets {
		go func(n *gossip.Node) {
			req := &stateQueryRequest{PoolName: p.config.Name}
			var resp stateQueryResponse
			if err := p.cluster.SendToWithResponse(n, lockStateQueryMsg, req, &resp); err != nil {
				return
			}
			p.tbl.applyAll(resp.Entries)
			sent <- struct{}{}
		}(n)
	}

	for i := 0; i < len(targets); i++ {
		select {
		case <-sent:
			answered = true
		case <-deadline.C:
			return answered
		case <-p.stopCh:
			return answered
		}
	}
	return answered
}

// regossip pushes one random, payload-sized batch of entries to each peer in
// rotation — the anti-entropy sweep. A fire-and-forget gossip delivery that
// was lost is healed on a later tick, and steady-state replication rises
// towards the whole candidate set over time.
func (p *Pool) regossip() {
	peers := p.replicationTargets()
	if len(peers) == 0 {
		return
	}
	snap := p.tbl.snapshot()
	if len(snap) == 0 {
		return
	}

	rand.Shuffle(len(snap), func(i, j int) { snap[i], snap[j] = snap[j], snap[i] })

	batches := chunkEntries(snap, p.entriesPerPacket(len(snap)))
	for _, batch := range batches {
		msg := &replicaGossipBroadcast{PoolName: p.config.Name, Entries: batch}
		if len(batches) == 1 {
			// The whole table fits one payload: sweep every peer, so small
			// groups fully reconcile each tick.
			for _, peer := range peers {
				_ = p.cluster.SendTo(peer, lockReplicaGossip, msg)
			}
			continue
		}
		// Large table: one random peer per batch per tick, so traffic spreads
		// and every peer converges over successive ticks.
		_ = p.cluster.SendTo(peers[rand.Intn(len(peers))], lockReplicaGossip, msg)
	}
}

// recoverState runs on a newly elected leader: it queries every live peer for
// its replica view, merges what arrives by the deadline, and opens the table
// for service. Responses that arrive after opening are still merged — token
// ordering makes late entries safe — so the scatter continues draining in the
// background.
func (p *Pool) recoverState(term uint64) {
	defer p.recoverWG.Done()

	targets := p.replicationTargets()
	batches := make(chan []replicaEntry, len(targets))

	// Bounded-concurrency scatter.
	sem := make(chan struct{}, stateQueryConcurrency)
	var scatterWG sync.WaitGroup
	for _, n := range targets {
		scatterWG.Add(1)
		go func(n *gossip.Node) {
			defer scatterWG.Done()
			select {
			case sem <- struct{}{}:
			case <-p.stopCh:
				return
			}
			defer func() { <-sem }()

			req := &stateQueryRequest{PoolName: p.config.Name}
			var resp stateQueryResponse
			if err := p.cluster.SendToWithResponse(n, lockStateQueryMsg, req, &resp); err != nil {
				return
			}
			select {
			case batches <- resp.Entries:
			case <-p.stopCh:
			}
		}(n)
	}
	go func() {
		scatterWG.Wait()
		close(batches)
	}()

	// Phase 1: merge until every peer has answered or the deadline passes.
	deadline := time.NewTimer(p.config.RecoveryTimeout)
	defer deadline.Stop()

	answered := 0
mergeLoop:
	for answered < len(targets) {
		select {
		case entries, ok := <-batches:
			if !ok {
				break mergeLoop
			}
			answered++
			p.tbl.applyAll(entries)
		case <-deadline.C:
			break mergeLoop
		case <-p.stopCh:
			return
		}
	}

	// The table may have assumed a newer term while we were merging; only the
	// holder of the current term may open.
	if p.tbl.currentTerm() == term {
		p.tbl.openForService()
	}

	// Phase 2: keep merging stragglers. Late entries cannot displace anything
	// the current term has granted — token ordering sees to that — but they
	// can rescue a grant the deadline missed.
	stragglers := time.NewTimer(p.config.RecoveryTimeout)
	defer stragglers.Stop()
	for {
		select {
		case entries, ok := <-batches:
			if !ok {
				return
			}
			p.tbl.applyAll(entries)
		case <-stragglers.C:
			return
		case <-p.stopCh:
			return
		}
	}
}
