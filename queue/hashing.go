package queue

import (
	"hash/fnv"

	"github.com/paularlott/gossip"
)

// coordinatorFor returns the node responsible for coordinating the given queue.
// Uses rendezvous (highest random weight) hashing for deterministic assignment.
func coordinatorFor(name string, nodes []*gossip.Node) *gossip.Node {
	if len(nodes) == 0 {
		return nil
	}
	if len(nodes) == 1 {
		return nodes[0]
	}

	var best *gossip.Node
	var bestScore uint64

	for _, node := range nodes {
		score := rendezvousScore(name, node.ID)
		if best == nil || score > bestScore {
			best = node
			bestScore = score
		}
	}

	return best
}

func rendezvousScore(key string, nodeID gossip.NodeID) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	var buf [16]byte
	id := [16]byte(nodeID)
	copy(buf[:], id[:])
	h.Write(buf[:])
	return h.Sum64()
}
