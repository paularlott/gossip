package leader

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip"
)

func TestLeaderEventHandlers_AddAndDispatch(t *testing.T) {
	h := newLeaderEventHandlers()
	var called int32
	h.add(LeaderElectedEvent, func(et EventType, _ gossip.NodeID) {
		if et != LeaderElectedEvent {
			t.Fatalf("wrong event type")
		}
		atomic.AddInt32(&called, 1)
	})

	h.dispatch(LeaderElectedEvent, gossip.EmptyNodeID)
	// allow goroutine to run
	time.Sleep(10 * time.Millisecond)
	if atomic.LoadInt32(&called) != 1 {
		t.Fatalf("expected handler to be called once")
	}
}
