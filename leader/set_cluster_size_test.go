package leader

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/internal/baseline"
)

// SetClusterSize moves the pinned baseline and the minimum while the election runs (a deliberate resize), in both
// directions, without waiting for evidence of departures.
func TestSetClusterSize(t *testing.T) {
	le := &LeaderElection{config: &Config{}, baseline: baseline.New(stability, longDwell, false, nil)}
	le.SetClusterSize(5, 3)
	le.baseline.Observe(mkNodes(5), time.Now())
	if b, q := le.BaselineSize(), le.WriteQuorum(); b != 5 || q != 3 {
		t.Fatalf("5 nodes: baseline %d, write quorum %d", b, q)
	}
	// shrink by one: the baseline follows at once, though five nodes were seen
	le.SetClusterSize(4, 3)
	if b, q := le.BaselineSize(), le.WriteQuorum(); b != 4 || q != 3 {
		t.Fatalf("shrunk to 4: baseline %d, write quorum %d", b, q)
	}
	le.baseline.Observe(mkNodes(4), time.Now())
	le.SetClusterSize(3, 2)
	le.baseline.Observe(mkNodes(3), time.Now())
	if b, q := le.BaselineSize(), le.WriteQuorum(); b != 3 || q != 2 {
		t.Fatalf("shrunk to 3: baseline %d, write quorum %d", b, q)
	}
	// grow by one before the new node is seen: pinned at the new size
	le.SetClusterSize(4, 3)
	le.baseline.Observe(mkNodes(3), time.Now())
	if b, q := le.BaselineSize(), le.WriteQuorum(); b != 4 || q != 3 {
		t.Fatalf("grown to 4: baseline %d, write quorum %d", b, q)
	}
	if got := le.calculateQuorumForNodes(3); got != 3 {
		t.Fatalf("grown to 4, 3 visible: %d needed to lead", got)
	}
}
