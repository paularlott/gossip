package partition

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"hash/fnv"
	"slices"

	"github.com/paularlott/gossip"
)

// rendezvousScore is fnv64a(p as 8 bytes big-endian || node ID bytes).
func rendezvousScore(p int, id gossip.NodeID) uint64 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(p))
	h := fnv.New64a()
	h.Write(b[:])
	h.Write(id[:])
	return h.Sum64()
}

// rendezvousOrder returns the candidates ordered for p: by rendezvous score, highest first (ties: the higher node ID
// first), so that removing a candidate moves only the partitions it owned. The first is the rendezvous owner, rank 0.
func rendezvousOrder(p int, candidates []gossip.NodeID) []gossip.NodeID {
	order := slices.Clone(candidates)
	slices.SortFunc(order, func(a, b gossip.NodeID) int {
		if sa, sb := rendezvousScore(p, a), rendezvousScore(p, b); sa != sb {
			return cmp.Compare(sb, sa)
		}
		return cmpID(b, a)
	})
	return order
}

// rendezvousOwner returns the rendezvous owner of p, rank 0. There is none without candidates.
func rendezvousOwner(p int, candidates []gossip.NodeID) (gossip.NodeID, bool) {
	order := rendezvousOrder(p, candidates)
	if len(order) == 0 {
		return gossip.EmptyNodeID, false
	}
	return order[0], true
}

// rendezvousRank returns the rank of id among the candidates for p, or false if id is not a candidate.
func rendezvousRank(p int, id gossip.NodeID, candidates []gossip.NodeID) (int, bool) {
	rank := slices.Index(rendezvousOrder(p, candidates), id)
	return rank, rank >= 0
}

// cmpID compares node IDs byte-wise.
func cmpID(a, b gossip.NodeID) int { return bytes.Compare(a[:], b[:]) }

// compatible reports whether a message's size and cluster ID are this node's: anything else is ignored.
func (m *Manager) compatible(size int, cluster string) bool {
	return size == m.cfg.ClusterSize && cluster == m.cfg.ClusterID
}

// candidates returns the nodes that may own partitions: the alive nodes configured with this ClusterSize and ClusterID
// that have published partition.ready (ready and eligible), and this one if it is ready and eligible.
func (m *Manager) candidates() []gossip.NodeID {
	var out []gossip.NodeID
	if m.ready.Load() && m.eligible.Load() {
		out = append(out, m.self)
	}
	for _, pi := range m.net.peers() {
		if pi.Size == m.cfg.ClusterSize && pi.Cluster == m.cfg.ClusterID && pi.Ready {
			out = append(out, pi.ID)
		}
	}
	return out
}
