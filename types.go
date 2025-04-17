package gossip

import (
	"github.com/google/uuid"
)

type NodeID uuid.UUID

func (n NodeID) String() string {
	return uuid.UUID(n).String()
}

var EmptyNodeID = NodeID(uuid.Nil)

// ApplicationVersionCheck is a function that checks if an application version is compatible
type ApplicationVersionCheck func(version string) bool
