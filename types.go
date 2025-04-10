package gossip

import (
	"github.com/google/uuid"
)

type NodeID uuid.UUID

func (n NodeID) String() string {
	return uuid.UUID(n).String()
}

// Indicates why we're selecting peers, allowing for purpose-specific optimizations
type peerSelectionPurpose int

const (
	purposeBroadcast     peerSelectionPurpose = iota // General message broadcast
	purposeStateExchange                             // Exchange node states
	purposeIndirectPing                              // Indirect ping requests
	purposeTTL                                       // TTL calculation
)

// ApplicationVersionCheck is a function that checks if an application version is compatible
type ApplicationVersionCheck func(version string) bool
