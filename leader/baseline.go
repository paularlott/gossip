package leader

import (
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

// baselineTracker maintains the cluster size that quorum is calculated against.
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
type baselineTracker struct {
	mu sync.Mutex

	stabilityPeriod time.Duration
	shrinkDwell     time.Duration
	autoShrink      bool
	cluster         *gossip.Cluster

	baseline int // adopted cluster size that quorum is measured against

	candidate     int       // observed count currently being timed for stability
	candidateFrom time.Time // when that count was first seen

	// departed holds nodes that announced a graceful leave (or were forgotten)
	// and are therefore discounted from the baseline. This is event memory, not
	// a duplicate of cluster state: the cluster records a node's current state,
	// while this records that a decrement has already been spent on it, so the
	// same departure can never count twice. Cleared per node if it returns.
	departed map[gossip.NodeID]struct{}
}

func newBaselineTracker(stabilityPeriod, shrinkDwell time.Duration, autoShrink bool, cluster *gossip.Cluster) *baselineTracker {
	return &baselineTracker{
		stabilityPeriod: stabilityPeriod,
		shrinkDwell:     shrinkDwell,
		autoShrink:      autoShrink,
		cluster:         cluster,
		departed:        make(map[gossip.NodeID]struct{}),
	}
}

// observe records the current eligible count and advances the stability timer.
// Called on every election check.
func (b *baselineTracker) observe(nodes []*gossip.Node, now time.Time) {
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
	if b.autoShrink && count == b.baseline-1 && steady >= b.shrinkDwell {
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
func (b *baselineTracker) noteGracefulDeparture(id gossip.NodeID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, already := b.departed[id]; already {
		return false
	}

	b.departed[id] = struct{}{}

	if b.baseline > 1 {
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
func (b *baselineTracker) forget(id gossip.NodeID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, already := b.departed[id]; already {
		return false
	}

	b.departed[id] = struct{}{}
	if b.baseline > 1 {
		b.baseline--
	}
	b.candidate = -1

	return true
}

// size returns the current baseline.
func (b *baselineTracker) size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.baseline
}

// departedCount reports how many nodes are currently discounted. Diagnostics.
func (b *baselineTracker) departedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.departed)
}

// reset clears all state, restoring the tracker to its initial condition. The
// next observation re-seeds the baseline from what is visible then.
func (b *baselineTracker) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.baseline = 0
	b.candidate = 0
	b.candidateFrom = time.Time{}
	b.departed = make(map[gossip.NodeID]struct{})
}
