package queue

import "github.com/paularlott/gossip"

// Test-only accessors so the external queue_test package can assert on wire
// traffic without exporting internals in the real API surface.

// ExportedRegisterMsgType returns the consumer-registration message type.
func ExportedRegisterMsgType() gossip.MessageType { return queueRegisterMsg }

// ExportedDeliverMsgType returns the push-delivery message type.
func ExportedDeliverMsgType() gossip.MessageType { return queueDeliverMsg }

// ExportedAckMsgType returns the ack message type.
func ExportedAckMsgType() gossip.MessageType { return queueAckMsg }
