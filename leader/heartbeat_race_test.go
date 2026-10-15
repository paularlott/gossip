package leader

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
)

// TestHeartbeatConcurrentWithLeaderLoss is a regression test for a data race: handleLeaderHeartbeat read
// isLeader/currentTerm after applyState released the lock, while handleNodeStateChange cleared the leader state
// under the lock. Run with -race.
func TestHeartbeatConcurrentWithLeaderLoss(t *testing.T) {
	cluster := newLeaderTestCluster(t, &countingTransport{})
	config := DefaultConfig()
	config.HeartbeatMessageType = gossip.ReservedMsgsStart + 53
	config.ForgetMessageType = gossip.ReservedMsgsStart + 54
	election := NewLeaderElection(cluster, config)
	election.Start()
	defer election.Stop()

	// A remote leader that is not alive: a zero-value node's observed state is NodeUnknown.
	remote := &gossip.Node{ID: gossip.NodeID(uuid.New())}
	jc := codec.NewJSONCodec()

	const rounds = 500
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range rounds {
			payload, err := jc.Marshal(heartbeatMessage{LeaderTime: time.Now(), Term: uint64(i + 1)})
			if err != nil {
				t.Error(err)
				return
			}
			p := gossip.NewPacket()
			p.SetCodec(jc)
			p.SetPayload(payload)
			if err := election.handleLeaderHeartbeat(remote, p); err != nil {
				t.Error(err)
			}
			p.Release()
		}
	}()
	go func() {
		defer wg.Done()
		for range rounds {
			election.handleNodeStateChange(remote, gossip.NodeAlive)
		}
	}()
	wg.Wait()

	if election.IsLeader() {
		t.Fatal("the local node must never become leader from a remote node's heartbeat")
	}
}
