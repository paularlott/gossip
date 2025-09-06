package gossip

import (
	"context"
	"sync"
	"time"

	"github.com/paularlott/gossip/hlc"
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

func (mh *messageHistory) getShard(messageID MessageID) *historyShard {
	// Extract the time component by shifting right to remove counter bits
	timeComponent := uint64(messageID) >> hlc.CounterBits
	shardIndex := timeComponent & uint64(mh.shardMask)
	return mh.shards[shardIndex]
}

func (mh *messageHistory) recordMessage(nodeID NodeID, messageID MessageID) {
	shard := mh.getShard(messageID)

	shard.mutex.Lock()
	key := messageKey{nodeID: nodeID, messageID: messageID}
	shard.entries[key] = time.Now().UnixNano()
	shard.mutex.Unlock()
}

func (mh *messageHistory) contains(nodeID NodeID, messageID MessageID) bool {
	shard := mh.getShard(messageID)

	shard.mutex.RLock()
	key := messageKey{nodeID: nodeID, messageID: messageID}
	_, exists := shard.entries[key]
	shard.mutex.RUnlock()

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
