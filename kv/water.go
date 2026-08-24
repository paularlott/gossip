package kv

import (
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

// waterTracker maintains the group size that the store's write quorum is
// calculated against — the water mark feeding needAcks(). It is a port of the
// leader package's baselineTracker with the same rules, so the two packages
// reason identically about cluster growth and shrinkage.
//
// The problem it solves: quorum derived purely from the currently observed
// count is unsafe, because two sides of a partition each compute
// requirements from their own view and both write. A constant bar fixes that
// but never tolerates a legitimate scale-down. The water mark gives an
// adaptive middle term that only ever moves for a defensible reason.
//
// # Growth
//
// The observed count must hold steady for StabilityPeriod before it raises
// the mark. The delay avoids latching a count sampled mid-transition, and
// stops a node that appears for two seconds from ratcheting the write bar
// upward for good.
//
// # Shrinkage
//
// This is the dangerous direction, because lowering the bar is what allows a
// minority to accept writes. The mark therefore only shrinks on evidence
// that a node left deliberately:
//
//   - A graceful Leave() broadcasts its intent, and every member observes the
//     NodeLeaving transition. That is a positive assertion from outside the
//     failure domain, so it is safe to act on. A partitioned node cannot
//     fake it, because a partitioned node cannot broadcast at all.
//
//   - A crash or partition produces no such signal — only absence. The mark
//     holds through it, keeping the write bar conservative, with one
//     exception: auto-shrink follows the observed count down when exactly one
//     member is missing and it has stayed missing for ShrinkDwell. The
//     exactly-one restriction keeps it split-safe (both sides of a partition
//     can only each see mark-1 if the arithmetic solves at N=2, where a
//     two-node group with W=2 still refuses lone writes). Any larger loss is
//     indistinguishable from a partition of that size and leaves the mark
//     alone — shrinking past it needs graceful leaves or Store.Forget.
//
// A node that departs and later rejoins is un-marked, so rolling restarts do
// not accumulate and drive the mark to the floor.
type waterTracker struct {
	mu sync.Mutex

	stabilityPeriod time.Duration
	shrinkDwell     time.Duration
	autoShrink      bool
	cluster         *gossip.Cluster
	nowFn           func() time.Time

	water int // adopted group size the write bar is measured against

	candidate     int       // observed count currently being timed for stability
	candidateFrom time.Time // when that count was first seen

	// members holds every node ever observed in the store's scope; a
	// departure is only relevant to this store if the node was a member.
	members map[gossip.NodeID]struct{}

	// departed holds nodes that announced a graceful leave (or were
	// forgotten) and are therefore discounted from the mark. This is event
	// memory, not a duplicate of cluster state: it records that a decrement
	// has already been spent on a node, so the same departure can never
	// count twice. Cleared per node if it returns.
	departed map[gossip.NodeID]struct{}
}

func newWaterTracker(stabilityPeriod, shrinkDwell time.Duration, autoShrink bool, cluster *gossip.Cluster, nowFn func() time.Time) *waterTracker {
	if nowFn == nil {
		nowFn = time.Now
	}
	return &waterTracker{
		stabilityPeriod: stabilityPeriod,
		shrinkDwell:     shrinkDwell,
		autoShrink:      autoShrink,
		cluster:         cluster,
		nowFn:           nowFn,
		members:         make(map[gossip.NodeID]struct{}),
		departed:        make(map[gossip.NodeID]struct{}),
	}
}

// observe records the current member count and advances the stability timer.
// Called on every gossip tick and at construction.
func (w *waterTracker) observe(nodes []*gossip.Node, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Un-mark anything that has returned, so it can depart again later.
	for _, n := range nodes {
		if n == nil {
			continue
		}
		w.members[n.ID] = struct{}{}
		delete(w.departed, n.ID)
	}

	// Evict departed entries for nodes the cluster no longer knows about at
	// all (tombstone expired after NodeRetentionTime). These have already
	// decremented the mark, and would never rejoin under the same ID, so
	// keeping them just leaks memory under high churn.
	if w.cluster != nil {
		for id := range w.departed {
			if w.cluster.GetNode(id) == nil {
				delete(w.departed, id)
				delete(w.members, id)
			}
		}
	}

	count := len(nodes)

	// Seed immediately on first observation so a starting store is not held
	// to a bar above what it can see.
	if w.water == 0 {
		w.water = count
		w.candidate = count
		w.candidateFrom = now
		return
	}

	if count != w.candidate {
		w.candidate = count
		w.candidateFrom = now
		return
	}

	steady := now.Sub(w.candidateFrom)

	// Growth: adopt a higher count once it has settled.
	if count > w.water && steady >= w.stabilityPeriod {
		w.water = count
		return
	}

	// Shrinkage: follow the observed count down, but only one member at a
	// time and only after a long dwell. The count now equals the mark, so
	// this cannot immediately fire again; a further step requires another
	// member to go missing.
	if w.autoShrink && count == w.water-1 && steady >= w.shrinkDwell {
		w.water = count
		w.candidateFrom = now
	}
}

// noteGracefulDeparture discounts a member that announced it is leaving.
// Only counted once per node, and only for nodes this store has observed as
// members.
func (w *waterTracker) noteGracefulDeparture(id gossip.NodeID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, wasMember := w.members[id]; !wasMember {
		return false
	}
	if _, already := w.departed[id]; already {
		return false
	}

	w.departed[id] = struct{}{}
	if w.water > 1 {
		w.water--
	}
	// Reset the stability window: the group has just changed shape, so the
	// current count should not immediately count as steady.
	w.candidate = -1
	return true
}

// forget discounts a member without requiring a graceful leave — the
// operator's external assertion that it is gone for good. The caller is
// responsible for having established the node is genuinely known; the
// tracker spends the decrement on any known member, once.
func (w *waterTracker) forget(id gossip.NodeID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, wasMember := w.members[id]; !wasMember {
		return false
	}
	if _, already := w.departed[id]; already {
		return false
	}

	w.departed[id] = struct{}{}
	if w.water > 1 {
		w.water--
	}
	w.candidate = -1
	return true
}

// size returns the current mark.
func (w *waterTracker) size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.water
}

// wasMember reports whether the store has ever observed the node inside its
// scope.
func (w *waterTracker) wasMember(id gossip.NodeID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.members[id]
	return ok
}
