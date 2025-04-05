package gossip

import (
	"context"
	"sync"
	"time"
)

type messageKey struct {
	nodeID    NodeID
	messageID MessageID
}

type historyShard struct {
	mutex   sync.RWMutex
	entries map[messageKey]int64
}

type messageHistory struct {
	config          *Config
	shardMask       uint32
	shards          []*historyShard
	shutdownContext context.Context
	cancelFunc      context.CancelFunc
}

func newMessageHistory(config *Config) *messageHistory {
	ctx, cancel := context.WithCancel(context.Background())

	mh := &messageHistory{
		config:          config,
		shardMask:       uint32(config.MsgHistoryShardCount - 1),
		shards:          make([]*historyShard, config.MsgHistoryShardCount),
		shutdownContext: ctx,
		cancelFunc:      cancel,
	}

	for i := range config.MsgHistoryShardCount {
		mh.shards[i] = &historyShard{
			mutex:   sync.RWMutex{},
			entries: make(map[messageKey]int64),
		}
	}

	mh.pruneHistory()

	return mh
}

func (mh *messageHistory) stop() {
	mh.cancelFunc()
}

func (mh *messageHistory) getShard(nodeID NodeID, messageID MessageID) *historyShard {
	// Extract bytes from UUID for better distribution
	// Assuming NodeID is a [16]byte or similar
	var hashBytes [4]byte

	// Mix bytes from different parts of the UUID for better distribution
	// This helps avoid clustering if UUIDs are sequential
	hashBytes[0] = nodeID[0] ^ nodeID[4] ^ nodeID[8] ^ nodeID[12]
	hashBytes[1] = nodeID[1] ^ nodeID[5] ^ nodeID[9] ^ nodeID[13]
	hashBytes[2] = nodeID[2] ^ nodeID[6] ^ nodeID[10] ^ nodeID[14]
	hashBytes[3] = nodeID[3] ^ nodeID[7] ^ nodeID[11] ^ nodeID[15]

	// Convert to uint32 - Little Endian
	hash := uint32(hashBytes[0]) |
		uint32(hashBytes[1])<<8 |
		uint32(hashBytes[2])<<16 |
		uint32(hashBytes[3])<<24

	// Incorporate messageID for better distribution - without truncation
	// Use XOR of high and low 32 bits to preserve entire int64 value
	messageIDLow := uint32(messageID.Timestamp & 0xFFFFFFFF)
	messageIDHigh := uint32(messageID.Timestamp >> 32)
	hash ^= messageIDLow ^ messageIDHigh

	return mh.shards[hash&mh.shardMask]
}

func (mh *messageHistory) recordMessage(nodeID NodeID, messageID MessageID) {
	shard := mh.getShard(nodeID, messageID)

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	key := messageKey{nodeID: nodeID, messageID: messageID}
	shard.entries[key] = time.Now().UnixNano()
}

func (mh *messageHistory) contains(nodeID NodeID, messageID MessageID) bool {
	shard := mh.getShard(nodeID, messageID)

	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	key := messageKey{nodeID: nodeID, messageID: messageID}
	_, exists := shard.entries[key]
	return exists
}

func (mh *messageHistory) pruneHistory() {
	go func() {
		pruneTimer := time.NewTicker(mh.config.MsgHistoryGCInterval)
		defer pruneTimer.Stop()

		for {
			select {
			case <-pruneTimer.C:
				cutoff := time.Now().Add(-mh.config.MsgHistoryMaxAge).UnixNano()

				for _, shard := range mh.shards {
					shard.mutex.Lock()
					for key, timestamp := range shard.entries {
						if timestamp < cutoff {
							delete(shard.entries, key)
						}
					}
					shard.mutex.Unlock()
				}

			case <-mh.shutdownContext.Done():
				return
			}
		}
	}()
}
