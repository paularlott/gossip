package leader

import (
	"time"

	"github.com/paularlott/gossip"
)

type Config struct {
	LeaderCheckInterval  time.Duration      // How often to check if we need to elect a leader
	LeaderTimeout        time.Duration      // How long a leader is considered valid without updates
	HeartbeatMessageType gossip.MessageType // Message type for heartbeat messages

	MetadataCriteria map[string]string // Optional, if given only nodes matching all criteria can be candidates

	// MinClusterSize is the minimum number of eligible nodes that must be
	// visible before a leader can be elected or kept. It is a hard floor on
	// quorum and, unlike a percentage, does not vary with what each node happens
	// to see — which is precisely what stops a minority partition electing its
	// own leader.
	//
	// # Choosing a value
	//
	// Set it to the majority of the SMALLEST cluster you will ever run:
	// MinClusterSize > N_min/2, i.e. N_min/2+1.
	//
	//	smallest is 3  -> 2
	//	smallest is 5  -> 3
	//	smallest is 7  -> 4
	//
	// The reasoning is short: for a split to produce two leaders, both sides
	// must reach the floor, which needs N >= 2*MinClusterSize. Keeping the floor
	// above half the cluster makes that impossible.
	//
	// You do NOT have to raise this as the cluster grows — the adaptive baseline
	// (see below) tracks growth automatically. It only needs to cover the
	// smallest size you will operate at.
	//
	// Setting it higher is allowed and simply trades availability for margin:
	// 4 of 5 tolerates one failure instead of two.
	//
	// # Leaving it unset
	//
	// At 0 there is no floor and quorum comes purely from observation. That
	// cannot tell "I am the only node" apart from "I am cut off from the others",
	// so an isolated node will elect itself. Acceptable when leadership is only
	// an optimisation; set the floor whenever leadership backs correctness, such
	// as distributed locks.
	MinClusterSize int

	// StabilityPeriod is how long the observed eligible count must hold steady
	// before it is trusted as the cluster's real size and used to raise quorum.
	//
	// It must comfortably exceed failure detection, or a count sampled
	// mid-transition gets latched. It also stops a transient spike — a node
	// appearing for a couple of seconds — from ratcheting quorum permanently.
	//
	// Zero means derive it as 2 x Cluster.DeadNodeTimeout(), which is the right
	// answer for default health timings and self-tunes if those are changed.
	StabilityPeriod time.Duration

	// ShrinkDwell is how long the cluster must sit at exactly one node below the
	// recorded baseline before the baseline follows it down.
	//
	// This lets a cluster shrink without operator involvement while remaining
	// split-safe, because the step only ever applies when exactly one node is
	// missing. For two sides of a partition to both shrink, each would have to
	// see its own baseline minus one, which requires A = B = N-1 with A+B = N —
	// solvable only at N=2, where MinClusterSize already blocks both sides.
	//
	// Any larger loss is left alone: a drop of three cannot be told apart from a
	// three-node partition, so the baseline holds and quorum stays conservative.
	// Shrinking past that point needs either graceful leaves or ForgetNode.
	//
	// The dwell must be long enough that a transient partition heals first, so it
	// is deliberately much longer than StabilityPeriod. Zero derives it as
	// 4 x Cluster.DeadNodeTimeout(). Set AutoShrinkDisabled to turn it off.
	ShrinkDwell time.Duration

	// AutoShrinkDisabled stops the baseline from ever following the observed
	// count downward on its own. With this set, shrinking requires a graceful
	// leave or an explicit ForgetNode.
	AutoShrinkDisabled bool

	// ExpectedClusterSize pins the quorum baseline to the cluster's intended
	// size. When positive:
	//
	//   - BaselineSize() never reports less than ExpectedClusterSize, from the
	//     moment the election is created — including on a node that has just
	//     restarted and can see only itself.
	//   - The baseline never drops below it: graceful leaves (and Stop, which
	//     leaves), ForgetNode, ResetQuorumBaseline and auto-shrink all stop at
	//     the pinned size.
	//   - QuorumSize() and WriteQuorum() are therefore at least
	//     ExpectedClusterSize/2+1.
	//
	// The baseline still grows above it if more eligible nodes are observed
	// stably, and can shrink back down to (but not below) it by the usual
	// rules. Resizing the cluster below the pinned size requires changing this
	// value and restarting.
	//
	// Use it when the cluster size is known up front and correctness depends
	// on quorum (replicated writes, locks): an adaptive baseline that has been
	// lowered by graceful leaves during a rolling restart or scale-down can let
	// a later minority reach "quorum". Zero (the default) keeps the purely
	// adaptive behaviour.
	ExpectedClusterSize int

	// ForgetMessageType carries ForgetNode broadcasts so a single call
	// propagates to the whole cluster.
	ForgetMessageType gossip.MessageType
}

func DefaultConfig() *Config {
	return &Config{
		LeaderCheckInterval:  1 * time.Second,
		LeaderTimeout:        3 * time.Second,
		HeartbeatMessageType: gossip.LeaderHeartbeatMsg,
		ForgetMessageType:    gossip.LeaderForgetMessage,
		MetadataCriteria:     nil,
	}
}
