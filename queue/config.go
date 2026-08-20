package queue

import (
	"time"

	"github.com/paularlott/gossip"
)

// Message types for the queue protocol (reserved internal range)
const (
	queuePublishMsg    gossip.MessageType = gossip.ReservedMsgsStart + 20
	queueAckMsg        gossip.MessageType = gossip.ReservedMsgsStart + 21
	queueNackMsg       gossip.MessageType = gossip.ReservedMsgsStart + 22
	queueReplyMsg      gossip.MessageType = gossip.ReservedMsgsStart + 23
	queueHandoffMsg    gossip.MessageType = gossip.ReservedMsgsStart + 24
	queueRegisterMsg   gossip.MessageType = gossip.ReservedMsgsStart + 25
	queueUnregisterMsg gossip.MessageType = gossip.ReservedMsgsStart + 26
	queueDeliverMsg    gossip.MessageType = gossip.ReservedMsgsStart + 27
)

const (
	// defaultPrefetch is how many messages a consumer will accept concurrently.
	defaultPrefetch = 16

	// coordinatorMaintenanceInterval is how often the coordinator checks for
	// expired inflight messages and stale consumer registrations.
	coordinatorMaintenanceInterval = 1 * time.Second

	// dispatchWorkers is the number of concurrent pushes the dispatcher will
	// have outstanding. Bounded so one slow or unreachable consumer cannot
	// stall delivery to the others.
	dispatchWorkers = 8
)

// Config holds configuration for a queue.
type Config struct {
	// Name identifies this queue. All nodes participating in the same queue
	// must use the same name. Multiple queues with different names can coexist
	// on a single cluster. Default: "default".
	Name string

	// NodeGroup optionally scopes the queue to a subset of nodes. Only nodes
	// in this group participate as coordinators and consumers. Any node in the
	// cluster can publish. Nil means the whole cluster participates.
	NodeGroup *gossip.NodeGroup

	// VisibilityTimeout is how long a delivered message remains inflight before
	// being redelivered to another consumer. Default: 30s.
	VisibilityTimeout time.Duration

	// MaxRetries is the maximum number of delivery attempts before a message
	// is discarded (or sent to the dead letter handler). Default: 3.
	MaxRetries int

	// MaxSize is the maximum number of pending messages in the queue. Publish
	// calls are rejected when this limit is reached. 0 means unlimited.
	MaxSize int

	// Prefetch is how many messages this node's consumer will accept
	// concurrently. The coordinator will not push more than this many
	// unacknowledged messages to it, which provides flow control.
	// Default: 16.
	Prefetch int

	// ConsumerHeartbeatInterval is how often a consumer re-announces itself to
	// the coordinator. This doubles as the mechanism for picking up a new
	// coordinator after a membership change. Consumers are considered stale
	// after three missed intervals. Default: 5s.
	ConsumerHeartbeatInterval time.Duration

	// DeadLetterHandler is called when a message exceeds MaxRetries. If nil,
	// the message is silently discarded. Panics in the handler are contained.
	DeadLetterHandler func(payload []byte, attempts int)
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Name:                      "default",
		VisibilityTimeout:         30 * time.Second,
		MaxRetries:                3,
		MaxSize:                   0,
		Prefetch:                  defaultPrefetch,
		ConsumerHeartbeatInterval: 5 * time.Second,
	}
}

// validate returns a normalised copy of the config, applying defaults for
// zero-value fields.
func (c *Config) validate() *Config {
	defaults := DefaultConfig()
	if c == nil {
		return defaults
	}

	out := *c

	if out.Name == "" {
		out.Name = defaults.Name
	}
	if out.VisibilityTimeout <= 0 {
		out.VisibilityTimeout = defaults.VisibilityTimeout
	}
	if out.MaxRetries <= 0 {
		out.MaxRetries = defaults.MaxRetries
	}
	if out.Prefetch <= 0 {
		out.Prefetch = defaults.Prefetch
	}
	if out.ConsumerHeartbeatInterval <= 0 {
		out.ConsumerHeartbeatInterval = defaults.ConsumerHeartbeatInterval
	}

	return &out
}
