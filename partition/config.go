package partition

import (
	"errors"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/logger"
)

// Message types for the partition protocol, allocated from the library's central reserved registry in packet.go
// (partition block: ReservedMsgsStart+30..+39).
const (
	partHeartbeatMsg  = gossip.PartHeartbeatMsg
	partPromiseMsg    = gossip.PartPromiseMsg
	partWriteMsg      = gossip.PartWriteMsg
	partChangeMsg     = gossip.PartChangeMsg
	partStateMsg      = gossip.PartStateMsg
	partCatchUpMsg    = gossip.PartCatchUpMsg
	partRelinquishMsg = gossip.PartRelinquishMsg
	partCommitMsg     = gossip.PartCommitMsg
	partClaimMsg      = gossip.PartClaimMsg
	partReadMsg       = gossip.PartReadMsg
)

// Node metadata keys the protocol publishes on the local node.
const (
	metaSize    = "partition.size"
	metaReady   = "partition.ready"
	metaCluster = "partition.cluster"
)

// Defaults for Config's zero values.
const (
	// DefaultPartitions is the default number of partitions.
	DefaultPartitions = 64
	// DefaultHeartbeatInterval is the default Config.HeartbeatInterval.
	DefaultHeartbeatInterval = 500 * time.Millisecond
	// DefaultWriteTimeout is the default Config.WriteTimeout.
	DefaultWriteTimeout = 5 * time.Second
	// DefaultMaxConcurrentChanges is the default Config.Tuning.MaxConcurrentChanges.
	DefaultMaxConcurrentChanges = 256
	// DefaultMaxConcurrentReads is the default Config.Tuning.MaxConcurrentReads.
	DefaultMaxConcurrentReads = 1024
	// DefaultMaxConcurrentRequests is the default Config.Tuning.MaxConcurrentRequests.
	DefaultMaxConcurrentRequests = 1024
	// DefaultTakeoverBackoffMax is the default Config.Tuning.TakeoverBackoffMax.
	DefaultTakeoverBackoffMax = 60 * time.Second
	// DefaultMaxChangesPerPartition is the default Config.Tuning.MaxChangesPerPartition.
	DefaultMaxChangesPerPartition = 32
	// DefaultMaxConcurrentCatchUps is the default Config.Tuning.MaxConcurrentCatchUps.
	DefaultMaxConcurrentCatchUps = 4
)

// Config configures a Manager. Every field but ClusterSize and Store has a default.
type Config struct {
	// Partitions is the number of partitions, 0 .. Partitions-1. Default: 64. Every node must use the same value.
	Partitions int

	// ClusterSize is the configured number of voting nodes (required, >= 1). The majority is ClusterSize/2+1; it is
	// never computed from the nodes that are visible. Nodes configured with a different size ignore each other.
	ClusterSize int

	// ClusterID names the data set the cluster holds (the application decides it once, when the data set is created,
	// e.g. at its first bootstrap or a restore, and every node of the cluster uses the same one). It travels in every
	// message, like ClusterSize: nodes with a different ClusterID ignore each other completely (no promises, writes,
	// heartbeats, state or catch-up), so two data sets can never be merged, however their nodes come to meet. Empty is
	// a valid ID of its own.
	ClusterID string

	// HeartbeatInterval is how often owners announce their partitions and the ownership loop runs. Default: 500ms.
	HeartbeatInterval time.Duration

	// OwnerTimeout is how long an owner may be silent before it is considered gone. Default: 3 x HeartbeatInterval.
	OwnerTimeout time.Duration

	// WriteTimeout bounds one Write, including waiting for a takeover. Default: 5s. It bounds one Read as well, and
	// separately one replication on the owner: a write that was begun runs on its own WriteTimeout from its start, so
	// that it is finished, or its owner steps down, whatever became of the caller.
	//
	// The cluster's transport must let a request wait for its reply longer than this (gossip Config.TCPDeadline at
	// least Manager.MinTransportDeadline()): a change sent to the owner whose reply the transport gives up on is reported
	// ErrUnknown while the owner may still commit it.
	WriteTimeout time.Duration

	// StaleAfter is CanRead's bound: how old the last sign of a live owner may be, and how long a follower may trail the
	// committed position its owner reported. Default: OwnerTimeout.
	StaleAfter time.Duration

	// ForceResync lets a node whose data is not intact (Store.Intact false) resync from the intact nodes that answer, at
	// least one, instead of from resyncQuorum(ClusterSize) of them; nodes that are resyncing themselves never count, so
	// it never completes from those alone. It is an operator's decision for when too few intact nodes are left to make
	// that quorum (in a cluster of three: one node down and another that lost its data): a write that only the
	// unreachable nodes held is then lost. Use it for one start, never as a setting.
	// It is also required to bootstrap empty nodes from a single seeded or restored node when Formed and SetFormed are
	// not set (they let a new data set form without it); the operator must ensure that it holds the intended data set.
	ForceResync bool

	// Tuning holds the advanced knobs. Every zero value means the default; most deployments leave it all zero.
	Tuning Tuning

	// Formed and SetFormed persist whether this node has formed: whether it has taken part in the protocol (given a
	// promise, which every takeover and every accepted write needs) or completed its startup synchronisation with intact
	// answers from a majority of the nodes, itself included. A new data set forms around its founder, the one node that
	// holds it (seeded or restored): an unformed node resyncing alongside other unformed nodes may copy it from intact
	// unformed nodes alone, which no node that has formed may (see syncOnce and DESIGN.md). Everywhere else the resync
	// quorum applies, and an intact answer from an unformed node does not count toward it.
	// SetFormed must be durable before it returns, and Formed must report it after a restart; SetFormed is called
	// before the first promise is stored. With either left nil every node counts as formed, and a new data set then
	// forms only with ForceResync on the empty nodes. A node that is not formed is reported "unformed" in its State
	// answers.
	Formed    func() bool
	SetFormed func() error

	// Store is the application's durable storage (required).
	Store Store

	// Logger receives the package's log lines. Default: the cluster's logger.
	Logger logger.Logger
}

// Tuning holds the advanced knobs of Config.Tuning: limits on concurrency and the details of how a node takes over
// ownership. Every zero (or negative) value means the default.
type Tuning struct {
	// BackoffMax bounds the random backoff in [0, BackoffMax) before a node proposes itself as owner. Default:
	// OwnerTimeout.
	BackoffMax time.Duration

	// RankDelay is how much longer each rank waits before it takes over a partition that has no owner: the nodes that
	// could own a partition are ranked by the rendezvous hash, and the node of rank r acts once the partition has been
	// ownerless for r x RankDelay (and after the random backoff). Rank 0 acts at once; a lost rank 0 is replaced by
	// rank 1 one RankDelay later, without waiting for the failure detector to drop it from the membership. Default:
	// 2 x HeartbeatInterval.
	RankDelay time.Duration

	// TakeoverBackoffMax caps the backoff after a takeover that did not complete: a node that fails to take over a
	// partition leaves it alone (neither takes over nor claims it) for OwnerTimeout, then twice that after the next
	// failure, and so on up to this value. A success resets it. Default: 60s.
	TakeoverBackoffMax time.Duration

	// MaxConcurrentChanges bounds the changes forwarded from other nodes that a node runs at once. Each runs in a
	// goroutine of the Manager while it waits for the followers; none holds one of the cluster's incoming workers
	// (the handlers reply later, see gossip.Cluster.HandleFuncWithDeferredReply), so the limit protects memory and the
	// Store, not the workers. A change that finds the limit reached is answered "busy" and its landing node retries
	// after a pause that starts at 1ms and doubles, with jitter, up to about 50ms (never past its deadline). Default:
	// 256.
	MaxConcurrentChanges int

	// MaxChangesPerPartition bounds the forwarded changes of one partition that a node, as its owner, holds at once
	// (running or waiting for the partition, which runs one at a time), so that one hot partition cannot take all of
	// MaxConcurrentChanges from the others. Beyond it a change is answered "busy", as above. Default: 32.
	MaxChangesPerPartition int

	// MaxConcurrentCatchUps bounds the catch-ups a node runs at once in the background (a follower that fell behind, the
	// startup synchronisation); the others wait for a slot. A takeover's own catch-up is not counted: it holds the
	// partition and must not wait for other partitions. Default: 4.
	MaxConcurrentCatchUps int

	// MaxConcurrentRequests bounds the other requests a node serves at once (promise, write, claim, state and
	// heartbeat), which share this one limit and each run in a goroutine of the Manager. Beyond it a request is refused
	// at once with its "not OK" reply (a promise or write is not acknowledged, a heartbeat is not counted as delivered,
	// a state request is not answered), never with an acknowledgement. Default: 1024.
	MaxConcurrentRequests int

	// MaxConcurrentReads bounds the reads forwarded from other nodes (Manager.Read) that a node serves at once as owner.
	// Each runs in a goroutine of the Manager while the owner confirms its leadership (reads of a partition share one
	// round); one that finds the limit reached is answered "busy" and its landing node retries as for a change.
	// Default: 1024.
	MaxConcurrentReads int

	// ReadyHoldDown is how long a node that has caught up waits before it declares itself ready and so becomes a
	// candidate for ownership. Default: 2 x OwnerTimeout.
	ReadyHoldDown time.Duration
}

// setDefault sets *v to d unless the caller set it (a zero or negative value counts as unset).
func setDefault[T int | time.Duration](v *T, d T) {
	if *v <= 0 {
		*v = d
	}
}

// withDefaults returns a copy of c with defaults applied, or an error if it is not usable.
func (c Config) withDefaults() (Config, error) {
	if c.Partitions < 0 {
		return c, errors.New("partition: Partitions must not be negative")
	}
	if c.ClusterSize < 1 {
		return c, errors.New("partition: ClusterSize is required and must be at least 1")
	}
	if c.Store == nil {
		return c, errors.New("partition: Store is required")
	}
	setDefault(&c.Partitions, DefaultPartitions)
	setDefault(&c.HeartbeatInterval, DefaultHeartbeatInterval)
	setDefault(&c.OwnerTimeout, 3*c.HeartbeatInterval)
	setDefault(&c.Tuning.BackoffMax, c.OwnerTimeout)
	setDefault(&c.Tuning.RankDelay, 2*c.HeartbeatInterval)
	setDefault(&c.Tuning.TakeoverBackoffMax, DefaultTakeoverBackoffMax)
	setDefault(&c.Tuning.MaxConcurrentChanges, DefaultMaxConcurrentChanges)
	setDefault(&c.Tuning.MaxConcurrentReads, DefaultMaxConcurrentReads)
	setDefault(&c.Tuning.MaxConcurrentRequests, DefaultMaxConcurrentRequests)
	setDefault(&c.Tuning.MaxChangesPerPartition, DefaultMaxChangesPerPartition)
	setDefault(&c.Tuning.MaxConcurrentCatchUps, DefaultMaxConcurrentCatchUps)
	setDefault(&c.Tuning.ReadyHoldDown, 2*c.OwnerTimeout)
	setDefault(&c.WriteTimeout, DefaultWriteTimeout)
	setDefault(&c.StaleAfter, c.OwnerTimeout)
	if c.Logger == nil {
		c.Logger = logger.NewNullLogger()
	}
	return c, nil
}
