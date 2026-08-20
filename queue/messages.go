package queue

import "github.com/paularlott/gossip"

// publishRequest is sent by a producer to the coordinator.
type publishRequest struct {
	QueueName     string        `msgpack:"q" json:"q"`
	MessageID     string        `msgpack:"id" json:"id"`
	Payload       []byte        `msgpack:"p" json:"p"`
	ReplyTo       gossip.NodeID `msgpack:"rt,omitempty" json:"rt,omitempty"` // Node to send reply to
	CorrelationID string        `msgpack:"cid,omitempty" json:"cid,omitempty"`
}

// publishResponse acknowledges receipt of a published message.
type publishResponse struct {
	Accepted bool   `msgpack:"a" json:"a"`
	Reason   string `msgpack:"r,omitempty" json:"r,omitempty"`
}

// registerRequest announces a consumer (and its prefetch capacity) to the
// coordinator. Re-sending it acts as a heartbeat.
type registerRequest struct {
	QueueName  string        `msgpack:"q" json:"q"`
	ConsumerID gossip.NodeID `msgpack:"cid" json:"cid"`
	Prefetch   int           `msgpack:"pf" json:"pf"`
}

// registerResponse confirms the registration was recorded.
type registerResponse struct {
	OK bool `msgpack:"ok" json:"ok"`
}

// unregisterRequest withdraws a consumer registration.
type unregisterRequest struct {
	QueueName  string        `msgpack:"q" json:"q"`
	ConsumerID gossip.NodeID `msgpack:"cid" json:"cid"`
}

// unregisterResponse confirms the withdrawal.
type unregisterResponse struct {
	OK bool `msgpack:"ok" json:"ok"`
}

// deliverPush is sent by the coordinator to push a message to a consumer.
type deliverPush struct {
	QueueName     string        `msgpack:"q" json:"q"`
	DeliveryID    string        `msgpack:"did" json:"did"`
	MessageID     string        `msgpack:"mid" json:"mid"`
	Payload       []byte        `msgpack:"p" json:"p"`
	ReplyTo       gossip.NodeID `msgpack:"rt,omitempty" json:"rt,omitempty"`
	CorrelationID string        `msgpack:"corr,omitempty" json:"corr,omitempty"`
	Attempt       int           `msgpack:"att" json:"att"`
}

// deliverPushResponse tells the coordinator whether the consumer took the
// message into its local buffer. This is answered immediately — it does not
// wait for the handler to run.
type deliverPushResponse struct {
	Accepted bool   `msgpack:"a" json:"a"`
	Reason   string `msgpack:"r,omitempty" json:"r,omitempty"`
}

// nackRequest is sent by a consumer to reject a message for redelivery.
type nackRequest struct {
	QueueName  string `msgpack:"q" json:"q"`
	DeliveryID string `msgpack:"did" json:"did"`
}

// nackResponse confirms the nack was processed.
type nackResponse struct {
	OK bool `msgpack:"ok" json:"ok"`
}

// ackRequest is sent by a consumer to acknowledge processing of a message.
type ackRequest struct {
	QueueName  string `msgpack:"q" json:"q"`
	DeliveryID string `msgpack:"did" json:"did"`
}

// ackResponse confirms the ack was processed.
type ackResponse struct {
	OK     bool   `msgpack:"ok" json:"ok"`
	Reason string `msgpack:"r,omitempty" json:"r,omitempty"`
}

// replyRequest is sent by a worker directly to the caller node.
type replyRequest struct {
	QueueName     string `msgpack:"q" json:"q"`
	CorrelationID string `msgpack:"cid" json:"cid"`
	Payload       []byte `msgpack:"p" json:"p"`
	Error         string `msgpack:"e,omitempty" json:"e,omitempty"`
}

// replyResponse acknowledges the reply was received.
type replyResponse struct {
	OK bool `msgpack:"ok" json:"ok"`
}

// handoffEntry represents a single message being transferred to a new coordinator.
type handoffEntry struct {
	MessageID     string        `msgpack:"mid" json:"mid"`
	Payload       []byte        `msgpack:"p" json:"p"`
	ReplyTo       gossip.NodeID `msgpack:"rt,omitempty" json:"rt,omitempty"`
	CorrelationID string        `msgpack:"cid,omitempty" json:"cid,omitempty"`
	Attempts      int           `msgpack:"att" json:"att"`
}

// handoffRequest is sent from an old coordinator to the new one.
type handoffRequest struct {
	QueueName string         `msgpack:"q" json:"q"`
	Entries   []handoffEntry `msgpack:"e" json:"e"`
}

// handoffResponse acknowledges receipt of transferred queue state.
type handoffResponse struct {
	Accepted int `msgpack:"a" json:"a"`
}
