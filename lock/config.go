package lock

import (
	"time"

	"github.com/paularlott/gossip"
)

// Message types for the lock protocol (reserved internal range).
const (
	lockAcquireMsg     gossip.MessageType = gossip.ReservedMsgsStart + 10
	lockReleaseMsg     gossip.MessageType = gossip.ReservedMsgsStart + 11
	lockExtendMsg      gossip.MessageType = gossip.ReservedMsgsStart + 12
	lockQueryMsg       gossip.MessageType = gossip.ReservedMsgsStart + 13
	lockReplicaPushMsg gossip.MessageType = gossip.ReservedMsgsStart + 14
	lockReplicaGossip  gossip.MessageType = gossip.ReservedMsgsStart + 15
	lockStateQueryMsg  gossip.MessageType = gossip.ReservedMsgsStart + 16
)

// Config holds configuration for a lock pool.
type Config struct {
	// Name identifies this pool. All nodes participating in the same logical
	// pool must use the same name. Multiple pools with different names can
	// coexist on one cluster. Default: "default".
	Name string

	// MinTTL is the shortest lock duration accepted. Default: 1s.
	MinTTL time.Duration

	// MaxTTL is the longest lock duration accepted. It bounds how long a lock
	// orphaned by a crash can stay wedged, and how long release tombstones
	// must be retained before garbage collection. Default: 30s.
	MaxTTL time.Duration

	// RetryInterval is the delay between attempts in the blocking Acquire.
	// Default: 50ms.
	RetryInterval time.Duration

	// WriteReplicas is W: the number of nodes — the leader included — that
	// must durably hold a lock mutation before it is acknowledged to the
	// caller. Default: 2 (the leader plus one peer).
	//
	// Every mutation (acquire, release, extend) is pushed to peers in
	// parallel and the operation fails closed if the required acks do not
	// arrive within ReplicationTimeout. The dial trades latency and
	// availability against durability at the moment of ack:
	//
	//   1 = leader only. No extra round trip; every acked grant dies with
	//       the leader. Only sensible when locks are advisory hints.
	//   2 = leader + 1 peer ack (default). Tolerates one crash at ack
	//       instant; the background gossip fan-out spreads entries further
	//       within seconds, raising steady-state replication to the whole
	//       group.
	//   3+ = one further parallel ack each. Tolerates W-1 simultaneous
	//       crashes at ack instant, at the cost of sensitivity to slow peers.
	//
	// When the group has fewer nodes than W the requirement degrades to what
	// is available — a single-node group runs with the leader's own copy as
	// the only replica, since refusing all operations would serve nobody.
	// Degrading applies to group size, never to peer failures: if enough
	// healthy peers exist and do not ack in time, the operation is refused.
	WriteReplicas int

	// ReplicationTimeout bounds one round of replica acks. An operation that
	// cannot collect its acks within this budget is retried once against
	// replacement peers and then refused. Default: 500ms.
	ReplicationTimeout time.Duration

	// RecoveryTimeout bounds how long a newly elected leader waits for
	// replica state to arrive before opening for service. Recovery normally
	// finishes early, as soon as every live peer has answered; the timeout
	// only caps the tail of stragglers. Default: 5s.
	RecoveryTimeout time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Name:               "default",
		MinTTL:             1 * time.Second,
		MaxTTL:             30 * time.Second,
		RetryInterval:      50 * time.Millisecond,
		WriteReplicas:      2,
		ReplicationTimeout: 500 * time.Millisecond,
		RecoveryTimeout:    5 * time.Second,
	}
}

// validate returns a normalised copy, applying defaults for zero-value fields.
func (c *Config) validate() *Config {
	defaults := DefaultConfig()
	if c == nil {
		return defaults
	}

	out := *c

	if out.Name == "" {
		out.Name = defaults.Name
	}
	if out.MinTTL <= 0 {
		out.MinTTL = defaults.MinTTL
	}
	if out.MaxTTL <= 0 {
		out.MaxTTL = defaults.MaxTTL
	}
	if out.MaxTTL < out.MinTTL {
		out.MaxTTL = out.MinTTL
	}
	if out.RetryInterval <= 0 {
		out.RetryInterval = defaults.RetryInterval
	}
	if out.WriteReplicas <= 0 {
		out.WriteReplicas = defaults.WriteReplicas
	}
	if out.ReplicationTimeout <= 0 {
		out.ReplicationTimeout = defaults.ReplicationTimeout
	}
	if out.RecoveryTimeout <= 0 {
		out.RecoveryTimeout = defaults.RecoveryTimeout
	}

	return &out
}
