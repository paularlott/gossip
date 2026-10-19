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
	wg              sync.WaitGroup
	stopOnce        sync.Once
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
	mh.stopOnce.Do(func() {
		mh.cancelFunc()
		mh.wg.Wait()
	})
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
	mh.wg.Add(1)
	go func() {
		defer mh.wg.Done()
		pruneTimer := time.NewTicker(mh.config.MsgHistoryGCInterval)
		defer pruneTimer.Stop()

		for {
			select {
			case <-pruneTimer.C:
				now := time.Now()
				cutoff := now.Add(-mh.config.MsgHistoryMaxAge).UnixNano()

				for _, shard := range mh.shards {
					shard.mutex.Lock()
					for key, timestamp := range shard.entries {
						// With MaxPacketAge, a message is kept until its own time is that far gone: until then
						// a replay of it passes the age check, so the history must still catch it.
						if timestamp < cutoff && (mh.config.MaxPacketAge <= 0 || !withinAge(key.messageID, mh.config.MaxPacketAge, now)) {
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

// withinAge reports whether the time of message id is within maxAge of now, either way.
func withinAge(id MessageID, maxAge time.Duration, now time.Time) bool {
	d := now.Sub(hlc.Timestamp(id).Time())
	return d <= maxAge && d >= -maxAge
}
