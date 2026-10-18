package baseline

import (
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

// Package baseline provides the adaptive cluster-size tracker that quorum and
// write-bar calculations are measured against. It is shared by the leader
// package (election quorum) and the kv package (write water mark), so the
// growth and shrinkage rules — and their split-safety argument — live in
// exactly one place.
//
// Tracker maintains the cluster size that quorum is calculated against.
//
// The problem it solves: quorum derived purely from the currently observed count
// is unsafe, because two sides of a partition each compute a majority of their
// own view and both pass. A constant floor fixes that but has to be re-tuned
// whenever the cluster grows. The baseline gives an adaptive middle term that
// only ever moves for a defensible reason.
//
// # Growth
//
// The observed count must hold steady for stabilityPeriod before it is adopted.
// The delay matters twice over: it avoids latching a count sampled while
// membership is still converging, and it stops a node that appears for two
// seconds from ratcheting quorum upward for good.
//
// # Shrinkage
//
// This is the dangerous direction, because lowering quorum is what allows a
// minority to elect. The baseline therefore only shrinks on evidence that a node
// left *deliberately*:
//
//   - A graceful Leave() broadcasts its intent, and every node observes the
//     NodeLeaving transition. That is a positive assertion from outside the
//     failure domain, so it is safe to act on. A partitioned node cannot fake it,
//     because a partitioned node cannot broadcast at all.
//
//   - A crash or partition produces no such signal — only absence. The baseline
//     holds, keeping quorum conservative. This costs availability (a nine-node
//     cluster that loses one still requires five) and is reclaimed either by
//     draining nodes gracefully or by an operator calling Cluster.ForgetNode.
//
// A node that departs and later rejoins is un-marked, so rolling restarts do not
// accumulate and drive the baseline to zero.
//
// # Locality
//
// Everything here is derived from membership state the cluster already gossips,
// so nodes converge without any additional protocol. A node that misses a leave
// broadcast keeps the higher baseline, which is the strict direction and
// therefore safe; it re-converges through normal state exchange.
//
// The tracker deliberately keeps no copy of the eligible set: whether a
// departure is relevant to this election is decided by the caller, from the
// transition's previous state and the election's criteria (see
// wasEligibleForBaseline), which is both fresher and less state than a snapshot.
type Tracker struct {
	mu sync.Mutex

	stabilityPeriod time.Duration
	shrinkDwell     time.Duration
	autoShrink      bool
	cluster         *gossip.Cluster

	baseline int // adopted cluster size that quorum is measured against

	// floor pins the baseline from below: when positive the baseline never
	// drops under it, whatever the cause (graceful leave, Forget, dwell
	// shrink, Reset, a restart that observes fewer nodes). Set by operators
	// who know the cluster's intended size and want quorum fixed to it.
	floor int

	candidate     int       // observed count currently being timed for stability
	candidateFrom time.Time // when that count was first seen

	// departed holds nodes that announced a graceful leave (or were forgotten)
	// and are therefore discounted from the baseline. This is event memory, not
	// a duplicate of cluster state: the cluster records a node's current state,
	// while this records that a decrement has already been spent on it, so the
	// same departure can never count twice. Cleared per node if it returns.
	departed map[gossip.NodeID]struct{}
}

func New(stabilityPeriod, shrinkDwell time.Duration, autoShrink bool, cluster *gossip.Cluster) *Tracker {
	return &Tracker{
		stabilityPeriod: stabilityPeriod,
		shrinkDwell:     shrinkDwell,
		autoShrink:      autoShrink,
		cluster:         cluster,
		departed:        make(map[gossip.NodeID]struct{}),
	}
}

// SetFloor pins the baseline so it never drops below n. Zero or less removes
// the floor, restoring the purely adaptive behaviour. Growth above the floor
// is still adopted under the usual stability rule.
func (b *Tracker) SetFloor(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 {
		n = 0
	}
	b.floor = n
	if b.baseline > 0 && b.baseline < n {
		b.baseline = n
	}
}

// Floor returns the configured floor (0 when unset).
func (b *Tracker) Floor() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.floor
}

// minimum is the lowest value the baseline may take once seeded: the floor,
// and never less than one. Caller holds mu.
func (b *Tracker) minimum() int {
	if b.floor > 1 {
		return b.floor
	}
	return 1
}

// observe records the current eligible count and advances the stability timer.
// Called on every election check.
func (b *Tracker) Observe(nodes []*gossip.Node, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Un-mark anything that has returned, so it can depart again later.
	for _, n := range nodes {
		delete(b.departed, n.ID)
	}

	// Evict departed entries for nodes the cluster no longer knows about at all
	// (tombstone expired after NodeRetentionTime). These have already decremented
	// the baseline, and would never rejoin under the same ID, so keeping them
	// just leaks memory under high churn.
	if b.cluster != nil {
		for id := range b.departed {
			if b.cluster.GetNode(id) == nil {
				delete(b.departed, id)
			}
		}
	}

	count := len(nodes)

	// Seed immediately on first observation so a starting cluster is not held
	// back for a whole stability period before it can elect anything.
	if b.baseline == 0 {
		b.baseline = count
		if b.baseline < b.floor {
			b.baseline = b.floor
		}
		b.candidate = count
		b.candidateFrom = now
		return
	}

	if count != b.candidate {
		b.candidate = count
		b.candidateFrom = now
		return
	}

	steady := now.Sub(b.candidateFrom)

	// Growth: adopt a higher count once it has settled.
	if count > b.baseline && steady >= b.stabilityPeriod {
		b.baseline = count
		return
	}

	// Shrinkage: follow the observed count down, but only one node at a time and
	// only after a long dwell.
	//
	// Restricting the step to exactly one missing node is what keeps this
	// split-safe. Both sides of a partition could only shrink if each saw its own
	// baseline minus one, needing A = B = N-1 with A+B = N, which solves only at
	// N=2 where MinClusterSize already blocks both sides. Any larger shortfall is
	// left alone because it cannot be distinguished from a partition of that size.
	if b.autoShrink && count == b.baseline-1 && count >= b.floor && steady >= b.shrinkDwell {
		b.baseline = count
		// The count now equals the baseline, so this cannot immediately fire
		// again; a further step requires another node to go missing.
		b.candidateFrom = now
	}
}

// noteGracefulDeparture discounts a node that announced it is leaving.
//
// The caller must first establish that the departure is relevant to this
// election — see wasEligibleForBaseline on LeaderElection, which decides from
// the transition's previous state and the election's criteria rather than from
// any cached membership. Only counted once per node.
func (b *Tracker) NoteGracefulDeparture(id gossip.NodeID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, already := b.departed[id]; already {
		return false
	}

	b.departed[id] = struct{}{}

	if b.baseline > b.minimum() {
		b.baseline--
	}

	// Reset the stability window: the cluster has just changed shape, so the
	// current count should not immediately count as steady.
	b.candidate = -1

	return true
}

// forget discounts a node without requiring a graceful leave. Used when an
// operator has asserted externally that a node is gone for good. The caller is
// responsible for having established that the node is genuinely known to this
// cluster — the tracker itself will spend the decrement on any ID, once.
func (b *Tracker) Forget(id gossip.NodeID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, already := b.departed[id]; already {
		return false
	}

	b.departed[id] = struct{}{}
	if b.baseline > b.minimum() {
		b.baseline--
	}
	b.candidate = -1

	return true
}

// Size returns the current baseline. With a floor set it is never below the
// floor, even before the first observation.
func (b *Tracker) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.baseline < b.floor {
		return b.floor
	}
	return b.baseline
}

// departedCount reports how many nodes are currently discounted. Diagnostics.
func (b *Tracker) DepartedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.departed)
}

// Reset clears all state, restoring the tracker to its initial condition. The
// next observation re-seeds the baseline from what is visible then. The floor
// is configuration, not state, and survives a reset.
func (b *Tracker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.baseline = 0
	b.candidate = 0
	b.candidateFrom = time.Time{}
	b.departed = make(map[gossip.NodeID]struct{})
}
