package gossip

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pongFrom makes the cluster's mock transport answer pings with a pong in the given state.
func pongFrom(c *Cluster, state NodeState, addr string, metadata map[string]interface{}) {
	unitTransport(c).replyWith(c.config.MsgCodec, &pongMessage{
		NodeID: NodeID(uuid.New()), AdvertiseAddr: addr, MetadataTimestamp: hlc.Now(), Metadata: metadata, NodeState: state,
	})
}

// takeTasks takes the health checks waiting in the monitor's queue, without their timestamps.
func takeTasks(c *Cluster) []HealthCheckTask {
	var tasks []HealthCheckTask
	for {
		select {
		case task := <-c.healthMonitor.taskQueue:
			task.Timestamp = 0
			tasks = append(tasks, task)
		default:
			return tasks
		}
	}
}

func TestProcessHealthCheck(t *testing.T) {
	tests := []struct {
		name      string
		start     NodeState
		task      HealthCheckType
		answers   bool          // the node answers the ping
		deadAfter time.Duration // DeadNodeTimeout
		want      NodeState
	}{
		{"ping answered", NodeAlive, DirectPing, true, time.Hour, NodeAlive},
		{"ping unanswered", NodeAlive, DirectPing, false, time.Hour, NodeSuspect},
		{"suspect answers", NodeSuspect, SuspectRetry, true, time.Hour, NodeAlive},
		{"suspect silent, not yet dead", NodeSuspect, SuspectRetry, false, time.Hour, NodeSuspect},
		{"suspect silent past the timeout", NodeSuspect, SuspectRetry, false, -1, NodeDead},
		{"dead answers", NodeDead, DeadNodeRetry, true, time.Hour, NodeAlive},
		{"dead stays dead", NodeDead, DeadNodeRetry, false, time.Hour, NodeDead},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newUnitCluster(t, func(c *Config) { c.DeadNodeTimeout = tt.deadAfter })
			if tt.answers {
				pongFrom(c, NodeAlive, "127.0.0.1:8001", nil)
			}
			node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
			c.nodes.addOrUpdate(node)
			c.nodes.updateState(node.ID, tt.start)
			node.lastMessageTime = hlc.FromTime(time.Now().Add(-time.Hour))

			c.healthMonitor.processHealthCheck(HealthCheckTask{NodeID: node.ID, TaskType: tt.task, Timestamp: hlc.Now()})

			assert.Equal(t, tt.want, node.GetObservedState())
			sends := unitTransport(c).sends()
			require.Len(t, sends, 1)
			assert.Equal(t, pingMsg, sends[0].msgType)
			if tt.answers {
				assert.True(t, time.Since(node.getLastActivity().Time()) < time.Minute, "an answer counts as activity")
			}
		})
	}

	t.Run("unknown node", func(t *testing.T) {
		c := newUnitCluster(t)
		c.healthMonitor.processHealthCheck(HealthCheckTask{NodeID: NodeID(uuid.New()), TaskType: DirectPing})
		assert.Empty(t, unitTransport(c).sends())
	})
}

// A pong brings the node's new address and metadata with it.
func TestPingNodeAddressUpdate(t *testing.T) {
	c := newUnitCluster(t)
	pongFrom(c, NodeAlive, "127.0.0.1:9999", map[string]interface{}{"key": "val"})
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	c.nodes.addOrUpdate(node)

	require.True(t, c.healthMonitor.pingNode(node))

	assert.Equal(t, "127.0.0.1:9999", node.AdvertisedAddr())
	assert.Equal(t, "val", node.metadata.GetString("key"))
}

func TestHealthMonitorSchedulers(t *testing.T) {
	old := hlc.FromTime(time.Now().Add(-time.Hour))

	t.Run("scan pings alive nodes that have been silent", func(t *testing.T) {
		c := newUnitCluster(t)
		c.localNode.lastMessageTime = old
		silent := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
		silent.lastMessageTime = old
		chatty := newNode(NodeID(uuid.New()), "127.0.0.1:8002")
		suspect := newNode(NodeID(uuid.New()), "127.0.0.1:8003")
		suspect.observedState, suspect.lastMessageTime = NodeSuspect, old
		for _, n := range []*Node{silent, chatty, suspect} {
			c.nodes.addOrUpdate(n)
		}

		c.healthMonitor.scanNodes()

		assert.Equal(t, []HealthCheckTask{{NodeID: silent.ID, TaskType: DirectPing}}, takeTasks(c),
			"not the local node, a node that spoke recently, or a node already suspect")
	})

	t.Run("suspect nodes are retried", func(t *testing.T) {
		c := newUnitCluster(t)
		suspect := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
		suspect.observedState = NodeSuspect
		c.nodes.addOrUpdate(suspect)
		c.nodes.addOrUpdate(newNode(NodeID(uuid.New()), "127.0.0.1:8002"))

		c.healthMonitor.retrySuspectNodes()

		assert.Equal(t, []HealthCheckTask{{NodeID: suspect.ID, TaskType: SuspectRetry}}, takeTasks(c))
	})

	t.Run("dead nodes are retried within MaxDeadNodeRetryTime", func(t *testing.T) {
		c := newUnitCluster(t, func(c *Config) { c.MaxDeadNodeRetryTime = 10 * time.Minute })
		recent, ancient := agedNode(NodeDead, time.Minute), agedNode(NodeDead, time.Hour)
		c.nodes.addOrUpdate(recent)
		c.nodes.addOrUpdate(ancient)

		c.healthMonitor.retryDeadNodes()

		assert.Equal(t, []HealthCheckTask{{NodeID: recent.ID, TaskType: DeadNodeRetry}}, takeTasks(c))
	})

	t.Run("a full queue drops the task", func(t *testing.T) {
		c := newUnitCluster(t, func(c *Config) { c.HealthCheckQueueDepth = 1 })
		id := NodeID(uuid.New())
		c.healthMonitor.enqueueHealthCheck(id, DirectPing)
		c.healthMonitor.enqueueHealthCheck(id, SuspectRetry)

		assert.Equal(t, []HealthCheckTask{{NodeID: id, TaskType: DirectPing}}, takeTasks(c))
	})
}

// The running monitor takes a node that stops answering from alive through suspect to dead.
func TestHealthMonitorRuns(t *testing.T) {
	c := newUnitCluster(t, func(c *Config) {
		c.HealthCheckInterval = 10 * time.Millisecond
		c.SuspectTimeout = 10 * time.Millisecond
		c.SuspectRetryInterval = 10 * time.Millisecond
		c.DeadNodeTimeout = 50 * time.Millisecond
	})
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	c.nodes.addOrUpdate(node)

	c.Start()

	require.Eventually(t, func() bool { return node.GetObservedState() == NodeDead }, 5*time.Second, 10*time.Millisecond)
}
