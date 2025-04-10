package gossip

import (
	"github.com/google/uuid"
)

type NodeID uuid.UUID

func (n NodeID) String() string {
	return uuid.UUID(n).String()
}

// ApplicationVersionCheck is a function that checks if an application version is compatible
type ApplicationVersionCheck func(version string) bool
