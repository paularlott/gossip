package kv

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/paularlott/gossip"
)

// replicateBatch makes a set of writes durable: it pushes the entries to
// peers in parallel and requires needAcks() of them to ack within the
// timeout, retrying once against replacement peers before refusing.
//
// When the store needs no peer acks (W=1, or the water mark is down to the
// local node) the local copy is the whole replica set and the push is
// skipped — the batch is still spread best-effort so a later-joining peer
// picks the state up. When acks are needed but no peer is reachable the
// write is refused immediately: an empty target list is exactly the
// partitioned-node case the water mark exists to catch. When acks fall
// short the caller compensates (Set) or keeps the safe-direction writes
// (Delete) and reports ErrWriteQuorum.
func (s *Store) replicateBatch(entries []*Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.observeMembers()
	targets := s.targets()
	need := s.needAcks()

	if need <= 0 {
		if len(targets) > 0 {
			go s.gossipEntries(entries)
		}
		return nil
	}
	if len(targets) == 0 {
		return fmt.Errorf("%w: need %d replica acks, no peers reachable", ErrWriteQuorum, need)
	}

	rand.Shuffle(len(targets), func(i, j int) { targets[i], targets[j] = targets[j], targets[i] })

	// Push to one more than required when available, so a single failed ack
	// does not cost the round.
	pushTo := s.config.WriteReplicas
	if pushTo > len(targets) {
		pushTo = len(targets)
	}

	got := s.pushForAcks(targets[:pushTo], entries, s.config.ReplicationTimeout)
	if got >= need {
		go s.gossipEntries(entries)
		return nil
	}

	// One retry round against replacement peers.
	if rest := targets[pushTo:]; len(rest) > 0 {
		next := rest
		if len(next) > pushTo {
			next = next[:pushTo]
		}
		got += s.pushForAcks(next, entries, s.config.ReplicationTimeout)
	}
	if got >= need {
		go s.gossipEntries(entries)
		return nil
	}

	return fmt.Errorf("%w: need %d replica acks, got %d", ErrWriteQuorum, need, got)
}

// pushForAcks sends the entries to each target in parallel and counts acks
// that arrive within the deadline. Acks that arrive late are still applied on
// the peer — only this node's count misses them — so under-counting is always
// the conservative direction.
func (s *Store) pushForAcks(targets []*gossip.Node, entries []*Entry, timeout time.Duration) int {
	acks := make(chan struct{}, len(targets))

	for _, n := range targets {
		go func(n *gossip.Node) {
			req := &writePush{StoreName: s.config.Name, Entries: entries}
			var ack writeAck
			if err := s.cluster.SendToWithResponse(n, kvWritePushMsg, req, &ack); err == nil && ack.Applied {
				select {
				case acks <- struct{}{}:
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
		case <-s.stopCh:
			return count
		}
	}
	return count
}

// gossipEntries spreads entries to the store's peers as fire-and-forget
// direct sends — scoped to the membership, never a cluster-wide broadcast, so
// a group-scoped store never leaks entries outside its group. This raises
// steady-state replication beyond the W write replicas so a later full sync
// finds copies even when the original replica set has thinned. Lost
// deliveries are healed by the anti-entropy re-gossip.
func (s *Store) gossipEntries(entries []*Entry) {
	if len(entries) == 0 {
		return
	}
	peers := s.targets()
	if len(peers) == 0 {
		return
	}

	for _, batch := range chunkEntries(entries, s.entriesPerPacket(len(entries))) {
		msg := &gossipBroadcast{StoreName: s.config.Name, Entries: batch}
		for _, peer := range peers {
			if err := s.cluster.SendTo(peer, kvGossipMsg, msg); err != nil {
				s.cluster.Logger().WithError(err).Debug("kv: gossip send failed")
				return
			}
		}
	}
}

// chunkEntries splits entries into payload-sized batches.
func chunkEntries(entries []*Entry, per int) [][]*Entry {
	if per <= 0 || len(entries) <= per {
		return [][]*Entry{entries}
	}
	var out [][]*Entry
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
func (s *Store) entriesPerPacket(total int) int {
	if per := s.cluster.CalcPayloadSize(total); per > 0 {
		return per
	}
	return total
}

// catchUp pulls the state of every peer into the local table via a
// bidirectional full-sync exchange — the request carries our snapshot, the
// response carries theirs, both sides merge. Run on the gossip tick until
// some peer has answered. Returns true when the store is in sync: at least
// one peer answered, or there are no peers at all (a lone store is vacuously
// in sync).
func (s *Store) catchUp() bool {
	targets := s.targets()
	if len(targets) == 0 {
		return true
	}

	deadline := time.NewTimer(s.config.SyncTimeout)
	defer deadline.Stop()

	answered := make(chan struct{}, len(targets))
	for _, n := range targets {
		go func(n *gossip.Node) {
			req := &fullSyncRequest{StoreName: s.config.Name, Entries: s.tbl.snapshot()}
			var resp fullSyncResponse
			if err := s.cluster.SendToWithResponse(n, kvFullSyncMsg, req, &resp); err != nil {
				return
			}
			s.tbl.applyAll(resp.Entries)
			answered <- struct{}{}
		}(n)
	}

	for i := 0; i < len(targets); i++ {
		select {
		case <-answered:
			return true
		case <-deadline.C:
			return false
		case <-s.stopCh:
			return false
		}
	}
	return true
}

// regossip pushes one random, payload-sized batch of entries to each peer in
// rotation — the anti-entropy sweep. A fire-and-forget delivery that was lost
// is healed on a later tick, and steady-state replication rises towards the
// whole membership over time.
func (s *Store) regossip() {
	peers := s.targets()
	if len(peers) == 0 {
		return
	}
	snap := s.tbl.snapshot()
	if len(snap) == 0 {
		return
	}

	rand.Shuffle(len(snap), func(i, j int) { snap[i], snap[j] = snap[j], snap[i] })

	batches := chunkEntries(snap, s.entriesPerPacket(len(snap)))
	for _, batch := range batches {
		msg := &gossipBroadcast{StoreName: s.config.Name, Entries: batch}
		if len(batches) == 1 {
			// The whole table fits one payload: sweep every peer, so small
			// groups fully reconcile each tick.
			for _, peer := range peers {
				_ = s.cluster.SendTo(peer, kvGossipMsg, msg)
			}
			continue
		}
		// Large table: one random peer per batch per tick, so traffic spreads
		// and every peer converges over successive ticks.
		_ = s.cluster.SendTo(peers[rand.Intn(len(peers))], kvGossipMsg, msg)
	}
}
