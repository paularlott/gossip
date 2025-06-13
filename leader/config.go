package leader

import (
	"time"

	"github.com/paularlott/gossip"
)

type Config struct {
	LeaderCheckInterval  time.Duration      // How often to check if we need to elect a leader
	LeaderTimeout        time.Duration      // How long a leader is considered valid without updates
	HeartbeatMessageType gossip.MessageType // Message type for heartbeat messages
	QuorumPercentage     int                // Percentage of nodes required for quorum (1-100)
	MetadataCriteria     map[string]string  // Optional, if given only nodes matching all criteria can be candidates
}

func DefaultConfig() *Config {
	return &Config{
		LeaderCheckInterval:  1 * time.Second,
		LeaderTimeout:        3 * time.Second,
		HeartbeatMessageType: gossip.ReservedMsgsStart + 1,
		QuorumPercentage:     51,
		MetadataCriteria:     nil,
	}
}
