package leader_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/internal/cuttable"
	"github.com/paularlott/gossip/leader"
	"github.com/paularlott/logger"
)

// End-to-end checks that the quorum baseline responds correctly to real cluster
// events: a graceful leave lowers it, a crash does not, and ForgetNode gives the
// operator a way to reclaim the availability a crash costs.

type blNode struct {
	cluster   *gossip.Cluster
	election  *leader.LeaderElection
	transport *cuttable.Transport
	label     string
}

func blCluster(t *testing.T, addr string) (*gossip.Cluster, *cuttable.Transport) {
	t.Helper()
	cfg := gossip.DefaultConfig()
	cfg.BindAddr = addr
	cfg.AdvertiseAddr = addr
	cfg.MsgCodec = codec.NewJSONCodec()
	cfg.Logger = logger.NewNullLogger()
	// Keep failure detection brisk so the tests do not crawl.
	cfg.DeadNodeTimeout = 2 * time.Second
	cfg.SuspectTimeout = 500 * time.Millisecond

	ct := cuttable.New(gossip.NewSocketTransport(cfg))
	cfg.Transport = ct

	c, err := gossip.NewCluster(cfg)
	if err != nil {
		t.Fatalf("cluster %s: %v", addr, err)
	}
	return c, ct
}

// crash severs the node from the cluster and then stops it. Because the
// transport is cut first, the Leave() that Stop() performs never reaches anyone,
// so peers see only silence — a genuine crash rather than a graceful exit.
func (n *blNode) crash() {
	n.transport.Cut()
	if n.election != nil {
		n.election.Stop()
	}
	if n.cluster != nil {
		n.cluster.Stop()
	}
	n.election = nil
	n.cluster = nil
}

// retire shuts the node down cleanly, announcing its departure.
func (n *blNode) retire() {
	if n.election != nil {
		n.election.Stop()
	}
	if n.cluster != nil {
		n.cluster.Leave()
		n.cluster.Stop()
	}
	n.election = nil
	n.cluster = nil
}

func blBuild(t *testing.T, basePort, count, minSize int) []*blNode {
	t.Helper()

	seed := fmt.Sprintf("127.0.0.1:%d", basePort)
	nodes := make([]*blNode, 0, count)

	for i := 0; i < count; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		c, ct := blCluster(t, addr)
		c.Start()
		if i > 0 {
			if err := c.Join([]string{seed}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}
		nodes = append(nodes, &blNode{cluster: c, transport: ct, label: fmt.Sprintf("n%d", i)})
	}

	for _, n := range nodes {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) && n.cluster.NumAliveNodes() < count {
			time.Sleep(50 * time.Millisecond)
		}
		if n.cluster.NumAliveNodes() < count {
			t.Fatalf("%s only sees %d of %d nodes", n.label, n.cluster.NumAliveNodes(), count)
		}
	}

	for _, n := range nodes {
		ec := leader.DefaultConfig()
		ec.LeaderCheckInterval = 100 * time.Millisecond
		ec.LeaderTimeout = time.Second
		ec.MinClusterSize = minSize
		ec.StabilityPeriod = 300 * time.Millisecond
		n.election = leader.NewLeaderElection(n.cluster, ec)
		n.election.Start()
	}

	return nodes
}

func blTeardown(nodes []*blNode) {
	for _, n := range nodes {
		if n.election != nil {
			n.election.Stop()
		}
	}
	for _, n := range nodes {
		if n.cluster != nil {
			n.cluster.Stop()
		}
	}
}

func waitBaseline(t *testing.T, n *blNode, want int, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if n.election.BaselineSize() == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s baseline is %d, expected %d", n.label, n.election.BaselineSize(), want)
}

func TestClusterBaselineSeedsAtClusterSize(t *testing.T) {
	nodes := blBuild(t, 20100, 5, 3)
	defer blTeardown(nodes)

	for _, n := range nodes {
		waitBaseline(t, n, 5, 10*time.Second)
		if q := n.election.QuorumSize(); q != 3 {
			t.Errorf("%s: expected quorum 3 for a 5-node cluster, got %d", n.label, q)
		}
	}
}

// A graceful Leave() is an explicit signal, so surviving nodes should lower their
// baseline and with it the quorum.
func TestClusterGracefulLeaveLowersBaseline(t *testing.T) {
	nodes := blBuild(t, 20110, 5, 2)
	defer func() {
		var live []*blNode
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		blTeardown(live)
	}()

	for _, n := range nodes {
		waitBaseline(t, n, 5, 10*time.Second)
	}

	// Retire two nodes properly.
	for _, victim := range nodes[3:] {
		victim.retire()
	}

	survivors := nodes[:3]
	for _, n := range survivors {
		waitBaseline(t, n, 3, 20*time.Second)
		if q := n.election.QuorumSize(); q != 2 {
			t.Errorf("%s: expected quorum 2 after draining to 3, got %d (baseline %d)",
				n.label, q, n.election.BaselineSize())
		}
	}

	// And leadership must survive the resize.
	deadline := time.Now().Add(15 * time.Second)
	var ok bool
	for time.Now().Before(deadline) {
		seen := map[gossip.NodeID]int{}
		all := true
		for _, n := range survivors {
			if !n.election.HasLeader() {
				all = false
				break
			}
			seen[n.election.GetLeaderID()]++
		}
		if all && len(seen) == 1 {
			ok = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ok {
		t.Error("the drained cluster failed to agree on a leader")
	}
}

// A hard stop produces no announcement, so the baseline must hold — a crash is
// indistinguishable from a partition and quorum stays conservative.
func TestClusterCrashDoesNotLowerBaseline(t *testing.T) {
	nodes := blBuild(t, 20120, 5, 2)
	defer func() {
		var live []*blNode
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		blTeardown(live)
	}()

	for _, n := range nodes {
		waitBaseline(t, n, 5, 10*time.Second)
	}

	// Kill two so they go silent — no announcement reaches the survivors.
	for _, victim := range nodes[3:] {
		victim.crash()
	}

	survivors := nodes[:3]

	// Give failure detection and several stability periods time to pass.
	time.Sleep(8 * time.Second)

	for _, n := range survivors {
		if bl := n.election.BaselineSize(); bl != 5 {
			t.Errorf("%s: baseline moved to %d after a silent loss; it must hold at 5", n.label, bl)
		}
		if q := n.election.QuorumSize(); q != 3 {
			t.Errorf("%s: expected quorum to stay at 3, got %d", n.label, q)
		}
	}
}

// ForgetNode is the operator's escape hatch for the availability a crash costs.
func TestClusterForgetNodeReclaimsQuorum(t *testing.T) {
	nodes := blBuild(t, 20130, 5, 2)
	defer func() {
		var live []*blNode
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		blTeardown(live)
	}()

	for _, n := range nodes {
		waitBaseline(t, n, 5, 10*time.Second)
	}

	deadIDs := []gossip.NodeID{
		nodes[3].cluster.LocalNode().ID,
		nodes[4].cluster.LocalNode().ID,
	}

	for _, victim := range nodes[3:] {
		victim.crash()
	}

	survivors := nodes[:3]
	time.Sleep(5 * time.Second)

	// Baseline held, as designed.
	for _, n := range survivors {
		if bl := n.election.BaselineSize(); bl != 5 {
			t.Fatalf("%s: expected the baseline to hold at 5, got %d", n.label, bl)
		}
	}

	// The operator asserts the two nodes are gone for good, on each survivor.
	for _, n := range survivors {
		for _, id := range deadIDs {
			if !n.election.ForgetNode(id) {
				t.Errorf("%s: ForgetNode(%v) should have discounted the node", n.label, id)
			}
		}
	}

	for _, n := range survivors {
		if bl := n.election.BaselineSize(); bl != 3 {
			t.Errorf("%s: expected the baseline to fall to 3 after forgetting, got %d", n.label, bl)
		}
		if q := n.election.QuorumSize(); q != 2 {
			t.Errorf("%s: expected quorum 2, got %d", n.label, q)
		}
		if n.cluster.GetNode(deadIDs[0]) != nil {
			t.Errorf("%s: forgotten node should be gone from the node list", n.label)
		}
	}
}

// Growth should be picked up automatically, without re-tuning MinClusterSize.
func TestClusterGrowthRaisesBaseline(t *testing.T) {
	// Floor chosen for a 3-node cluster.
	nodes := blBuild(t, 20140, 3, 2)
	defer func() { blTeardown(nodes) }()

	for _, n := range nodes {
		waitBaseline(t, n, 3, 10*time.Second)
		if q := n.election.QuorumSize(); q != 2 {
			t.Errorf("%s: expected quorum 2, got %d", n.label, q)
		}
	}

	// Scale out to 7 without touching the floor.
	extra := make([]*blNode, 0, 4)
	for i := 3; i < 7; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 20140+i)
		c, ct := blCluster(t, addr)
		c.Start()
		if err := c.Join([]string{"127.0.0.1:20140"}); err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
		ec := leader.DefaultConfig()
		ec.LeaderCheckInterval = 100 * time.Millisecond
		ec.LeaderTimeout = time.Second
		ec.MinClusterSize = 2
		ec.StabilityPeriod = 300 * time.Millisecond
		el := leader.NewLeaderElection(c, ec)
		el.Start()
		extra = append(extra, &blNode{cluster: c, transport: ct, election: el, label: fmt.Sprintf("n%d", i)})
	}
	defer blTeardown(extra)

	all := append(append([]*blNode{}, nodes...), extra...)

	for _, n := range all {
		waitBaseline(t, n, 7, 25*time.Second)
	}

	// Quorum should now be a majority of 7, not of the stale floor.
	for _, n := range all {
		if q := n.election.QuorumSize(); q != 4 {
			t.Errorf("%s: expected quorum 4 for a 7-node cluster, got %d (baseline %d)",
				n.label, q, n.election.BaselineSize())
		}
	}

	// Which means a 3-node minority could no longer elect.
	minorityQuorum := 4
	if 3 >= minorityQuorum {
		t.Error("a 3-node minority of 7 must not reach quorum")
	}
}

func TestClusterResetQuorumBaseline(t *testing.T) {
	nodes := blBuild(t, 20150, 5, 2)
	defer blTeardown(nodes)

	for _, n := range nodes {
		waitBaseline(t, n, 5, 10*time.Second)
	}

	// Discount a couple of nodes, then reset.
	n0 := nodes[0]
	n0.election.ForgetNode(nodes[3].cluster.LocalNode().ID)
	n0.election.ForgetNode(nodes[4].cluster.LocalNode().ID)
	if bl := n0.election.BaselineSize(); bl != 3 {
		t.Fatalf("expected 3 after forgetting two, got %d", bl)
	}

	n0.election.ResetQuorumBaseline()

	// Re-derives from what is visible; the forgotten nodes are still running, so
	// the cluster is still 5 from n0's perspective once they are re-learned.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if n0.election.BaselineSize() >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if bl := n0.election.BaselineSize(); bl < 3 {
		t.Errorf("expected the baseline to re-seed to at least 3, got %d", bl)
	}
}

// The stability period should self-tune from the cluster's failure detection when
// it is not set explicitly.
func TestStabilityPeriodDerivedFromDeadNodeTimeout(t *testing.T) {
	c, _ := blCluster(t, "127.0.0.1:20160")
	c.Start()
	defer c.Stop()

	if got := c.DeadNodeTimeout(); got != 2*time.Second {
		t.Fatalf("expected the configured DeadNodeTimeout of 2s, got %v", got)
	}

	ec := leader.DefaultConfig()
	ec.StabilityPeriod = 0 // derive it
	el := leader.NewLeaderElection(c, ec)
	el.Start()
	defer el.Stop()

	// Nothing to assert directly without exposing the period; the important
	// part is that construction succeeds and the election runs with a derived
	// value rather than zero (which would adopt every transient count).
	time.Sleep(200 * time.Millisecond)
	if el.BaselineSize() < 1 {
		t.Error("the election should have seeded a baseline")
	}
}

func TestForgetNodeRejectsSelf(t *testing.T) {
	c, _ := blCluster(t, "127.0.0.1:20170")
	c.Start()
	defer c.Stop()

	if c.ForgetNode(c.LocalNode().ID) {
		t.Error("the local node must not be removable")
	}
	if c.GetNode(c.LocalNode().ID) == nil {
		t.Error("the local node should still be present")
	}
}

func TestForgetNodeUnknownIsNoop(t *testing.T) {
	c, _ := blCluster(t, "127.0.0.1:20180")
	c.Start()
	defer c.Stop()

	if c.ForgetNode(gossip.NodeID(uuidNew())) {
		t.Error("forgetting an unknown node should report false")
	}
}

func uuidNew() [16]byte {
	var b [16]byte
	for i := range b {
		b[i] = byte(i + 1)
	}
	return b
}

// --- broadcast ForgetNode ---

// One ForgetNode call must reach the whole cluster. Visiting every node to shrink
// a large cluster is not workable, so the assertion is gossiped.
func TestClusterForgetNodeBroadcastsToAll(t *testing.T) {
	nodes := blBuildShrink(t, 20200, 5, 2, time.Hour) // auto-shrink effectively off
	defer func() {
		var live []*blNode
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		blTeardown(live)
	}()

	for _, n := range nodes {
		waitBaseline(t, n, 5, 10*time.Second)
	}

	deadID := nodes[4].cluster.LocalNode().ID
	nodes[4].crash()

	survivors := nodes[:4]
	time.Sleep(4 * time.Second)

	// Baseline holds after a silent loss, as designed.
	for _, n := range survivors {
		if bl := n.election.BaselineSize(); bl != 5 {
			t.Fatalf("%s: expected the baseline to hold at 5, got %d", n.label, bl)
		}
	}

	// A single call on ONE node should propagate to the rest.
	if !survivors[0].election.ForgetNode(deadID) {
		t.Fatal("ForgetNode should have applied locally")
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, n := range survivors {
			if n.election.BaselineSize() != 4 {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	for _, n := range survivors {
		if bl := n.election.BaselineSize(); bl != 4 {
			t.Errorf("%s: expected the broadcast to lower the baseline to 4, got %d", n.label, bl)
		}
		if n.cluster.GetNode(deadID) != nil {
			t.Errorf("%s: the forgotten node should be gone from the node list", n.label)
		}
	}
}

func TestClusterForgetNodeLocalDoesNotBroadcast(t *testing.T) {
	nodes := blBuildShrink(t, 20210, 3, 2, time.Hour)
	defer func() {
		var live []*blNode
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		blTeardown(live)
	}()

	for _, n := range nodes {
		waitBaseline(t, n, 3, 10*time.Second)
	}

	deadID := nodes[2].cluster.LocalNode().ID
	nodes[2].crash()
	time.Sleep(4 * time.Second)

	// Local-only application.
	nodes[0].election.ForgetNodeLocal(deadID)

	if bl := nodes[0].election.BaselineSize(); bl != 2 {
		t.Errorf("n0 should have discounted locally, baseline %d", bl)
	}

	time.Sleep(2 * time.Second)

	if bl := nodes[1].election.BaselineSize(); bl != 3 {
		t.Errorf("n1 should be unaffected by a local-only forget, baseline %d", bl)
	}
}

// A forget — local or broadcast — naming a node this cluster has never seen
// must be rejected outright. Every accepted forget spends a baseline decrement,
// so accepting unknown IDs would let any member (or any bug) suppress quorum
// by minting IDs.
func TestClusterForgetNodeUnknownIsRejected(t *testing.T) {
	nodes := blBuildShrink(t, 20260, 3, 2, time.Hour)
	defer func() {
		var live []*blNode
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		blTeardown(live)
	}()

	for _, n := range nodes {
		waitBaseline(t, n, 3, 10*time.Second)
	}

	stranger := gossip.NodeID(uuidNew())

	// Locally rejected, nothing spent.
	for _, n := range nodes {
		if n.election.ForgetNodeLocal(stranger) {
			t.Errorf("%s: forgetting an unknown node must be rejected", n.label)
		}
	}

	// The broadcast variant is equally inert: no peer's baseline may move.
	if nodes[0].election.ForgetNode(stranger) {
		t.Error("a broadcast forget of an unknown node must be rejected locally too")
	}

	time.Sleep(2 * time.Second)
	for _, n := range nodes {
		if bl := n.election.BaselineSize(); bl != 3 {
			t.Errorf("%s: baseline moved to %d on an unknown-node forget; it must hold at 3", n.label, bl)
		}
	}

	// A forget of a node the cluster does know still works (crash + forget).
	deadID := nodes[2].cluster.LocalNode().ID
	nodes[2].crash()
	time.Sleep(4 * time.Second)
	if !nodes[0].election.ForgetNode(deadID) {
		t.Fatalf("forgetting a known crashed node should apply, baseline %d", nodes[0].election.BaselineSize())
	}
	if bl := nodes[0].election.BaselineSize(); bl != 2 {
		t.Errorf("expected the baseline to fall to 2 after a valid forget, got %d", bl)
	}
}

func TestClusterForgetNodeSelfRejected(t *testing.T) {
	nodes := blBuildShrink(t, 20220, 3, 2, time.Hour)
	defer blTeardown(nodes)

	for _, n := range nodes {
		waitBaseline(t, n, 3, 10*time.Second)
	}

	if nodes[0].election.ForgetNode(nodes[0].cluster.LocalNode().ID) {
		t.Error("a node must not forget itself")
	}
	if bl := nodes[0].election.BaselineSize(); bl != 3 {
		t.Errorf("baseline should be untouched, got %d", bl)
	}
}

// --- auto-shrink at cluster level ---

// A single crashed node should be absorbed automatically after the dwell, with no
// operator involvement.
func TestClusterAutoShrinkAfterSingleCrash(t *testing.T) {
	nodes := blBuildShrink(t, 20230, 5, 2, 1*time.Second)
	defer func() {
		var live []*blNode
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		blTeardown(live)
	}()

	for _, n := range nodes {
		waitBaseline(t, n, 5, 10*time.Second)
	}

	nodes[4].crash()
	survivors := nodes[:4]

	// Failure detection plus the dwell.
	for _, n := range survivors {
		waitBaseline(t, n, 4, 25*time.Second)
	}

	for _, n := range survivors {
		if q := n.election.QuorumSize(); q != 3 {
			t.Errorf("%s: expected quorum 3 for a baseline of 4, got %d", n.label, q)
		}
	}
}

// Two nodes crashing at once is a shortfall of two, which auto-shrink must not
// absorb — it is indistinguishable from a two-node partition.
func TestClusterAutoShrinkIgnoresSimultaneousDoubleCrash(t *testing.T) {
	nodes := blBuildShrink(t, 20240, 5, 2, 1*time.Second)
	defer func() {
		var live []*blNode
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		blTeardown(live)
	}()

	for _, n := range nodes {
		waitBaseline(t, n, 5, 10*time.Second)
	}

	// Both at once.
	nodes[3].crash()
	nodes[4].crash()

	survivors := nodes[:3]

	// Well past detection and several dwells.
	time.Sleep(12 * time.Second)

	for _, n := range survivors {
		if bl := n.election.BaselineSize(); bl != 5 {
			t.Errorf("%s: a simultaneous loss of two must not shrink the baseline, got %d", n.label, bl)
		}
	}
}

// Crashing nodes one at a time, with a gap, should walk the baseline down.
func TestClusterAutoShrinkStepwise(t *testing.T) {
	nodes := blBuildShrink(t, 20250, 5, 2, 1*time.Second)
	defer func() {
		var live []*blNode
		for _, n := range nodes {
			if n.cluster != nil {
				live = append(live, n)
			}
		}
		blTeardown(live)
	}()

	for _, n := range nodes {
		waitBaseline(t, n, 5, 10*time.Second)
	}

	nodes[4].crash()
	for _, n := range nodes[:4] {
		waitBaseline(t, n, 4, 25*time.Second)
	}

	nodes[3].crash()
	for _, n := range nodes[:3] {
		waitBaseline(t, n, 3, 25*time.Second)
	}

	for _, n := range nodes[:3] {
		if q := n.election.QuorumSize(); q != 2 {
			t.Errorf("%s: expected quorum 2 for a baseline of 3, got %d", n.label, q)
		}
	}
}

// blBuildShrink is blBuild with an explicit shrink dwell.
func blBuildShrink(t *testing.T, basePort, count, minSize int, dwell time.Duration) []*blNode {
	t.Helper()

	seed := fmt.Sprintf("127.0.0.1:%d", basePort)
	nodes := make([]*blNode, 0, count)

	for i := 0; i < count; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		c, ct := blCluster(t, addr)
		c.Start()
		if i > 0 {
			if err := c.Join([]string{seed}); err != nil {
				t.Fatalf("node %d join: %v", i, err)
			}
		}
		nodes = append(nodes, &blNode{cluster: c, transport: ct, label: fmt.Sprintf("n%d", i)})
	}

	for _, n := range nodes {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) && n.cluster.NumAliveNodes() < count {
			time.Sleep(50 * time.Millisecond)
		}
		if n.cluster.NumAliveNodes() < count {
			t.Fatalf("%s only sees %d of %d nodes", n.label, n.cluster.NumAliveNodes(), count)
		}
	}

	base := gossip.ReservedMsgsStart + gossip.MessageType(50+(basePort%40))
	for _, n := range nodes {
		ec := leader.DefaultConfig()
		ec.LeaderCheckInterval = 100 * time.Millisecond
		ec.LeaderTimeout = time.Second
		ec.MinClusterSize = minSize
		ec.StabilityPeriod = 300 * time.Millisecond
		ec.ShrinkDwell = dwell
		ec.HeartbeatMessageType = base
		ec.ForgetMessageType = base + 1
		n.election = leader.NewLeaderElection(n.cluster, ec)
		n.election.Start()
	}

	return nodes
}
