package queue_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/queue"
	"github.com/paularlott/logger"
)

func createRealCluster(t *testing.T, bindAddr string) *gossip.Cluster {
	t.Helper()
	config := gossip.DefaultConfig()
	config.BindAddr = bindAddr
	config.AdvertiseAddr = bindAddr
	config.MsgCodec = codec.NewJSONCodec()
	config.Logger = logger.NewNullLogger()
	config.Transport = gossip.NewSocketTransport(config)
	cluster, err := gossip.NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster on %s: %v", bindAddr, err)
	}
	return cluster
}

func waitForClusterSize(t *testing.T, cluster *gossip.Cluster, expected int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cluster.NumAliveNodes() >= expected {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cluster did not reach size %d within %v (got %d)", expected, timeout, cluster.NumAliveNodes())
}

// --- Basic integration tests ---

func TestIntegrationPublishConsumeTwoNodes(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19300")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19301")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19300"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "tasks", VisibilityTimeout: 5 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "tasks", VisibilityTimeout: 5 * time.Second})
	defer q2.Close()

	// Publish from node1
	err := q1.Publish(context.Background(), []byte("job-1"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Consume from node2
	var received atomic.Value
	q2.Consume(func(msg *queue.Message) error {
		received.Store(string(msg.Payload))
		return nil
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if received.Load() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if received.Load() == nil {
		t.Fatal("consumer on node2 did not receive the message")
	}
	if received.Load().(string) != "job-1" {
		t.Errorf("expected 'job-1', got %q", received.Load())
	}
}

func TestIntegrationRequestReply(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19310")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19311")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19310"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "rpc", VisibilityTimeout: 5 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "rpc", VisibilityTimeout: 5 * time.Second})
	defer q2.Close()

	// Consumer on node2 — echoes the payload uppercased
	q2.Consume(func(msg *queue.Message) error {
		result := append(msg.Payload, []byte("-processed")...)
		msg.Reply(result)
		return nil
	})

	// Request from node1
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := q1.Request(ctx, []byte("hello"), 5*time.Second)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if string(result) != "hello-processed" {
		t.Errorf("expected 'hello-processed', got %q", string(result))
	}
}

func TestIntegrationMultipleMessages(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19320")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19321")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19320"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "multi", VisibilityTimeout: 5 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "multi", VisibilityTimeout: 5 * time.Second})
	defer q2.Close()

	// Publish 20 messages
	for i := 0; i < 20; i++ {
		err := q1.Publish(context.Background(), []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}

	// Consume from node2
	var count atomic.Int64
	q2.Consume(func(msg *queue.Message) error {
		count.Add(1)
		return nil
	})

	// Wait for all messages
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 20 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if count.Load() < 20 {
		t.Errorf("expected 20 messages consumed, got %d", count.Load())
	}
}

func TestIntegrationAtLeastOnceDelivery(t *testing.T) {
	// Consumer fails on first attempt, succeeds on second → message delivered at least once
	node1 := createRealCluster(t, "127.0.0.1:19330")
	node1.Start()
	defer node1.Stop()

	q := queue.New(node1, &queue.Config{
		Name:              "retry",
		VisibilityTimeout: 100 * time.Millisecond,
		MaxRetries:        5,
	})
	defer q.Close()

	q.Publish(context.Background(), []byte("retry-me"))

	var attempts atomic.Int64
	q.Consume(func(msg *queue.Message) error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("fail")
		}
		return nil
	})

	time.Sleep(2 * time.Second)

	if attempts.Load() < 3 {
		t.Errorf("expected at least 3 attempts, got %d", attempts.Load())
	}
}

// --- Cluster growth/shrink tests ---

func TestIntegrationHandoffOnNodeJoin(t *testing.T) {
	// Publish messages to single node, then add a second node.
	// If the queue coordinator moves, messages should be handed off.
	node1 := createRealCluster(t, "127.0.0.1:19340")
	node1.Start()
	defer node1.Stop()

	q1 := queue.New(node1, &queue.Config{Name: "handoff-join", VisibilityTimeout: 30 * time.Second})
	defer q1.Close()

	// Publish 10 messages (all local since single node)
	for i := 0; i < 10; i++ {
		q1.Publish(context.Background(), []byte(fmt.Sprintf("msg-%d", i)))
	}

	// Add node2
	node2 := createRealCluster(t, "127.0.0.1:19341")
	node2.Start()
	defer node2.Stop()

	q2 := queue.New(node2, &queue.Config{Name: "handoff-join", VisibilityTimeout: 30 * time.Second})
	defer q2.Close()

	node2.Join([]string{"127.0.0.1:19340"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)
	time.Sleep(500 * time.Millisecond)

	// Consume from either node — all 10 messages should be delivered
	var count atomic.Int64
	q2.Consume(func(msg *queue.Message) error {
		count.Add(1)
		return nil
	})
	q1.Consume(func(msg *queue.Message) error {
		count.Add(1)
		return nil
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 10 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if count.Load() < 10 {
		t.Errorf("expected 10 messages after node join, got %d", count.Load())
	}
}

func TestIntegrationHandoffOnNodeLeave(t *testing.T) {
	// 3 nodes with messages, gracefully remove one — all messages should survive
	node1 := createRealCluster(t, "127.0.0.1:19350")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19351")
	node2.Start()
	defer node2.Stop()

	node3 := createRealCluster(t, "127.0.0.1:19352")
	node3.Start()

	node2.Join([]string{"127.0.0.1:19350"})
	node3.Join([]string{"127.0.0.1:19350"})
	waitForClusterSize(t, node1, 3, 5*time.Second)
	waitForClusterSize(t, node2, 3, 5*time.Second)
	waitForClusterSize(t, node3, 3, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "leave-test", VisibilityTimeout: 30 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "leave-test", VisibilityTimeout: 30 * time.Second})
	defer q2.Close()

	q3 := queue.New(node3, &queue.Config{Name: "leave-test", VisibilityTimeout: 30 * time.Second})

	// Publish 15 messages
	for i := 0; i < 15; i++ {
		q1.Publish(context.Background(), []byte(fmt.Sprintf("leave-msg-%d", i)))
	}

	// Gracefully shut down node3 (pool closes first → drains)
	q3.Close()
	node3.Leave()
	node3.Stop()

	waitForClusterSize(t, node1, 2, 10*time.Second)
	waitForClusterSize(t, node2, 2, 10*time.Second)
	time.Sleep(500 * time.Millisecond)

	// Consume from remaining nodes — all 15 messages should be delivered
	var count atomic.Int64
	q1.Consume(func(msg *queue.Message) error {
		count.Add(1)
		return nil
	})
	q2.Consume(func(msg *queue.Message) error {
		count.Add(1)
		return nil
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 15 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if count.Load() < 15 {
		t.Errorf("expected 15 messages after node leave, got %d", count.Load())
	}
}

func TestIntegrationGrowCluster(t *testing.T) {
	// 2 nodes, publish messages, grow to 4 — all messages should be consumed
	node1 := createRealCluster(t, "127.0.0.1:19360")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19361")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19360"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "grow", VisibilityTimeout: 30 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "grow", VisibilityTimeout: 30 * time.Second})
	defer q2.Close()

	// Publish 20 messages
	for i := 0; i < 20; i++ {
		q1.Publish(context.Background(), []byte(fmt.Sprintf("grow-msg-%d", i)))
	}

	// Grow to 4
	node3 := createRealCluster(t, "127.0.0.1:19362")
	node3.Start()
	defer node3.Stop()
	q3 := queue.New(node3, &queue.Config{Name: "grow", VisibilityTimeout: 30 * time.Second})
	defer q3.Close()

	node4 := createRealCluster(t, "127.0.0.1:19363")
	node4.Start()
	defer node4.Stop()
	q4 := queue.New(node4, &queue.Config{Name: "grow", VisibilityTimeout: 30 * time.Second})
	defer q4.Close()

	node3.Join([]string{"127.0.0.1:19360"})
	node4.Join([]string{"127.0.0.1:19360"})
	waitForClusterSize(t, node1, 4, 5*time.Second)
	waitForClusterSize(t, node3, 4, 5*time.Second)
	waitForClusterSize(t, node4, 4, 5*time.Second)
	time.Sleep(1 * time.Second)

	// Consume from all nodes
	var count atomic.Int64
	handler := func(msg *queue.Message) error {
		count.Add(1)
		return nil
	}
	q1.Consume(handler)
	q2.Consume(handler)
	q3.Consume(handler)
	q4.Consume(handler)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 20 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if count.Load() < 20 {
		t.Errorf("expected 20 messages after cluster growth, got %d", count.Load())
	}
}

func TestIntegrationShrinkThenGrow(t *testing.T) {
	// 3 nodes, publish, shrink to 2, grow to 4, consume — all messages delivered
	node1 := createRealCluster(t, "127.0.0.1:19370")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19371")
	node2.Start()
	defer node2.Stop()

	node3 := createRealCluster(t, "127.0.0.1:19372")
	node3.Start()

	node2.Join([]string{"127.0.0.1:19370"})
	node3.Join([]string{"127.0.0.1:19370"})
	waitForClusterSize(t, node1, 3, 5*time.Second)
	waitForClusterSize(t, node2, 3, 5*time.Second)
	waitForClusterSize(t, node3, 3, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "sg", VisibilityTimeout: 30 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "sg", VisibilityTimeout: 30 * time.Second})
	defer q2.Close()

	q3 := queue.New(node3, &queue.Config{Name: "sg", VisibilityTimeout: 30 * time.Second})

	// Publish 15 messages
	for i := 0; i < 15; i++ {
		q1.Publish(context.Background(), []byte(fmt.Sprintf("sg-msg-%d", i)))
	}

	// Shrink: remove node3
	q3.Close()
	node3.Leave()
	node3.Stop()
	waitForClusterSize(t, node1, 2, 10*time.Second)
	time.Sleep(500 * time.Millisecond)

	// Grow: add node4
	node4 := createRealCluster(t, "127.0.0.1:19373")
	node4.Start()
	defer node4.Stop()
	q4 := queue.New(node4, &queue.Config{Name: "sg", VisibilityTimeout: 30 * time.Second})
	defer q4.Close()

	node4.Join([]string{"127.0.0.1:19370"})
	waitForClusterSize(t, node1, 3, 5*time.Second)
	waitForClusterSize(t, node4, 3, 5*time.Second)
	time.Sleep(500 * time.Millisecond)

	// Consume from all remaining nodes
	var count atomic.Int64
	handler := func(msg *queue.Message) error {
		count.Add(1)
		return nil
	}
	q1.Consume(handler)
	q2.Consume(handler)
	q4.Consume(handler)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 15 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if count.Load() < 15 {
		t.Errorf("expected 15 messages after shrink+grow, got %d", count.Load())
	}
}

// --- Deadlock / contention tests ---

func TestIntegrationNoDeadlockConcurrentPublishConsume(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19380")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19381")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19380"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "deadlock", VisibilityTimeout: 2 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "deadlock", VisibilityTimeout: 2 * time.Second})
	defer q2.Close()

	var consumed atomic.Int64
	q2.Consume(func(msg *queue.Message) error {
		consumed.Add(1)
		time.Sleep(5 * time.Millisecond) // simulate work
		return nil
	})

	// Publish concurrently from multiple goroutines — must not deadlock
	var wg sync.WaitGroup
	const numPublishers = 10
	const msgsPerPublisher = 20

	done := make(chan struct{})
	go func() {
		for i := 0; i < numPublishers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < msgsPerPublisher; j++ {
					q1.Publish(context.Background(), []byte(fmt.Sprintf("p%d-m%d", id, j)))
				}
			}(i)
		}
		wg.Wait()
		close(done)
	}()

	// If this takes more than 30 seconds, we've deadlocked
	select {
	case <-done:
		// good
	case <-time.After(30 * time.Second):
		t.Fatal("DEADLOCK: concurrent publish did not complete within 30 seconds")
	}

	// Wait for all messages to be consumed
	expected := int64(numPublishers * msgsPerPublisher)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if consumed.Load() >= expected {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if consumed.Load() < expected {
		t.Errorf("expected %d consumed, got %d (possible deadlock or message loss)", expected, consumed.Load())
	}
}

func TestIntegrationNoDeadlockRequestReply(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19390")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19391")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19390"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "rpc-deadlock", VisibilityTimeout: 5 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "rpc-deadlock", VisibilityTimeout: 5 * time.Second})
	defer q2.Close()

	q2.Consume(func(msg *queue.Message) error {
		msg.Reply(append(msg.Payload, []byte("-ok")...))
		return nil
	})

	// Concurrent requests — must not deadlock
	var wg sync.WaitGroup
	var errors atomic.Int64
	const numRequests = 20

	done := make(chan struct{})
	go func() {
		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				result, err := q1.Request(ctx, []byte(fmt.Sprintf("req-%d", id)), 10*time.Second)
				if err != nil {
					errors.Add(1)
					return
				}
				expected := fmt.Sprintf("req-%d-ok", id)
				if string(result) != expected {
					errors.Add(1)
				}
			}(i)
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// good
	case <-time.After(30 * time.Second):
		t.Fatal("DEADLOCK: concurrent request/reply did not complete within 30 seconds")
	}

	if errors.Load() > 0 {
		t.Errorf("%d request/reply errors (possible deadlock or routing issue)", errors.Load())
	}
}

func TestIntegrationMultipleQueues(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19400")
	node1.Start()
	defer node1.Stop()

	q1 := queue.New(node1, &queue.Config{Name: "alpha", VisibilityTimeout: 5 * time.Second})
	defer q1.Close()

	q2 := queue.New(node1, &queue.Config{Name: "beta", VisibilityTimeout: 5 * time.Second})
	defer q2.Close()

	q1.Publish(context.Background(), []byte("for-alpha"))
	q2.Publish(context.Background(), []byte("for-beta"))

	var alphaReceived, betaReceived atomic.Value
	q1.Consume(func(msg *queue.Message) error {
		alphaReceived.Store(string(msg.Payload))
		return nil
	})
	q2.Consume(func(msg *queue.Message) error {
		betaReceived.Store(string(msg.Payload))
		return nil
	})

	time.Sleep(500 * time.Millisecond)

	if alphaReceived.Load() == nil || alphaReceived.Load().(string) != "for-alpha" {
		t.Error("alpha queue should receive 'for-alpha'")
	}
	if betaReceived.Load() == nil || betaReceived.Load().(string) != "for-beta" {
		t.Error("beta queue should receive 'for-beta'")
	}
}

// --- NodeGroup-scoped tests ---

func TestIntegrationNodeGroupScoped(t *testing.T) {
	// 3 nodes: node1 and node2 have role=processor, node3 does not.
	// Queue scoped to role=processor — coordination on processor nodes only.
	node1 := createRealCluster(t, "127.0.0.1:19410")
	node1.Start()
	defer node1.Stop()
	node1.LocalMetadata().SetString("role", "processor")

	node2 := createRealCluster(t, "127.0.0.1:19411")
	node2.Start()
	defer node2.Stop()
	node2.LocalMetadata().SetString("role", "processor")

	node3 := createRealCluster(t, "127.0.0.1:19412")
	node3.Start()
	defer node3.Stop()
	// node3 has no role metadata

	node2.Join([]string{"127.0.0.1:19410"})
	node3.Join([]string{"127.0.0.1:19410"})
	waitForClusterSize(t, node1, 3, 5*time.Second)
	waitForClusterSize(t, node2, 3, 5*time.Second)
	waitForClusterSize(t, node3, 3, 5*time.Second)
	time.Sleep(1 * time.Second)

	// Create NodeGroup for processor nodes
	processorGroup1 := gossip.NewNodeGroup(node1, map[string]string{"role": "processor"}, nil)
	defer processorGroup1.Close()
	processorGroup2 := gossip.NewNodeGroup(node2, map[string]string{"role": "processor"}, nil)
	defer processorGroup2.Close()
	processorGroup3 := gossip.NewNodeGroup(node3, map[string]string{"role": "processor"}, nil)
	defer processorGroup3.Close()

	// Wait for group to populate
	time.Sleep(1 * time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "grouped", VisibilityTimeout: 5 * time.Second, NodeGroup: processorGroup1})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "grouped", VisibilityTimeout: 5 * time.Second, NodeGroup: processorGroup2})
	defer q2.Close()

	q3 := queue.New(node3, &queue.Config{Name: "grouped", VisibilityTimeout: 5 * time.Second, NodeGroup: processorGroup3})
	defer q3.Close()

	// Publish from node3 (non-processor) — should still work
	err := q3.Publish(context.Background(), []byte("from-non-processor"))
	if err != nil {
		t.Fatalf("publish from non-processor failed: %v", err)
	}

	// Consume on node2 (processor)
	var received atomic.Value
	q2.Consume(func(msg *queue.Message) error {
		received.Store(string(msg.Payload))
		return nil
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if received.Load() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if received.Load() == nil {
		t.Fatal("processor node should have received the message")
	}
	if received.Load().(string) != "from-non-processor" {
		t.Errorf("expected 'from-non-processor', got %q", received.Load())
	}
}

func TestIntegrationNodeGroupAddProcessor(t *testing.T) {
	// Start with 1 processor, publish messages, then add a 2nd processor.
	// Messages should be consumed regardless of coordinator shift.
	node1 := createRealCluster(t, "127.0.0.1:19420")
	node1.Start()
	defer node1.Stop()
	node1.LocalMetadata().SetString("role", "processor")

	group1 := gossip.NewNodeGroup(node1, map[string]string{"role": "processor"}, nil)
	defer group1.Close()

	q1 := queue.New(node1, &queue.Config{Name: "group-grow-q", VisibilityTimeout: 30 * time.Second, NodeGroup: group1})
	defer q1.Close()

	// Publish 10 messages
	for i := 0; i < 10; i++ {
		q1.Publish(context.Background(), []byte(fmt.Sprintf("msg-%d", i)))
	}

	// Add 2nd processor
	node2 := createRealCluster(t, "127.0.0.1:19421")
	node2.Start()
	defer node2.Stop()
	node2.LocalMetadata().SetString("role", "processor")

	group2 := gossip.NewNodeGroup(node2, map[string]string{"role": "processor"}, nil)
	defer group2.Close()

	node2.Join([]string{"127.0.0.1:19420"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)
	time.Sleep(1 * time.Second)

	q2 := queue.New(node2, &queue.Config{Name: "group-grow-q", VisibilityTimeout: 30 * time.Second, NodeGroup: group2})
	defer q2.Close()

	// Consume from both
	var count atomic.Int64
	handler := func(msg *queue.Message) error {
		count.Add(1)
		return nil
	}
	q1.Consume(handler)
	q2.Consume(handler)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 10 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if count.Load() < 10 {
		t.Errorf("expected 10 messages consumed after processor join, got %d", count.Load())
	}
}

func TestIntegrationNodeGroupRemoveProcessor(t *testing.T) {
	// 2 processor nodes with messages, gracefully remove one.
	// All messages should be consumed by the remaining processor.
	node1 := createRealCluster(t, "127.0.0.1:19430")
	node1.Start()
	defer node1.Stop()
	node1.LocalMetadata().SetString("role", "processor")

	node2 := createRealCluster(t, "127.0.0.1:19431")
	node2.Start()
	node2.LocalMetadata().SetString("role", "processor")

	node2.Join([]string{"127.0.0.1:19430"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)
	time.Sleep(1 * time.Second)

	group1 := gossip.NewNodeGroup(node1, map[string]string{"role": "processor"}, nil)
	defer group1.Close()
	group2 := gossip.NewNodeGroup(node2, map[string]string{"role": "processor"}, nil)
	defer group2.Close()

	q1 := queue.New(node1, &queue.Config{Name: "group-shrink-q", VisibilityTimeout: 30 * time.Second, NodeGroup: group1})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "group-shrink-q", VisibilityTimeout: 30 * time.Second, NodeGroup: group2})

	// Publish 10 messages
	for i := 0; i < 10; i++ {
		q1.Publish(context.Background(), []byte(fmt.Sprintf("shrink-msg-%d", i)))
	}

	// Gracefully remove node2
	q2.Close()
	node2.Leave()
	node2.Stop()

	waitForClusterSize(t, node1, 1, 10*time.Second)
	time.Sleep(1 * time.Second)

	// Consume from remaining node
	var count atomic.Int64
	q1.Consume(func(msg *queue.Message) error {
		count.Add(1)
		return nil
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 10 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if count.Load() < 10 {
		t.Errorf("expected 10 messages after processor removal, got %d", count.Load())
	}
}

// --- Push-model integration tests ---

// countingSocketTransport wraps the real socket transport and counts the queue
// messages that actually cross the wire, so we can assert that an idle queue is
// silent (i.e. we are not polling).
type countingSocketTransport struct {
	inner  gossip.Transport
	counts sync.Map // gossip.MessageType -> *atomic.Int64
}

func (t *countingSocketTransport) Name() string { return "counting-socket" }

func (t *countingSocketTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	return t.inner.Start(ctx, wg)
}

func (t *countingSocketTransport) PacketChannel() chan *gossip.Packet {
	return t.inner.PacketChannel()
}

func (t *countingSocketTransport) record(mt gossip.MessageType) {
	v, _ := t.counts.LoadOrStore(mt, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (t *countingSocketTransport) count(mt gossip.MessageType) int64 {
	v, ok := t.counts.Load(mt)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

func (t *countingSocketTransport) Send(tt gossip.TransportType, node *gossip.Node, packet *gossip.Packet) error {
	t.record(packet.MessageType)
	return t.inner.Send(tt, node, packet)
}

func (t *countingSocketTransport) SendWithReply(node *gossip.Node, packet *gossip.Packet) (*gossip.Packet, error) {
	t.record(packet.MessageType)
	return t.inner.SendWithReply(node, packet)
}

func createCountingCluster(t *testing.T, bindAddr string) (*gossip.Cluster, *countingSocketTransport) {
	t.Helper()
	config := gossip.DefaultConfig()
	config.BindAddr = bindAddr
	config.AdvertiseAddr = bindAddr
	config.MsgCodec = codec.NewJSONCodec()
	config.Logger = logger.NewNullLogger()

	ct := &countingSocketTransport{inner: gossip.NewSocketTransport(config)}
	config.Transport = ct

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster on %s: %v", bindAddr, err)
	}
	return cluster, ct
}

// An idle queue with an active consumer must not generate per-message traffic.
// Under the old pull model this produced ~20 requests/sec; now the only traffic
// is the consumer heartbeat.
func TestIntegrationIdleQueueGeneratesNoPollTraffic(t *testing.T) {
	node1, _ := createCountingCluster(t, "127.0.0.1:19440")
	node1.Start()
	defer node1.Stop()

	node2, ct2 := createCountingCluster(t, "127.0.0.1:19441")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19440"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	cfg := func() *queue.Config {
		return &queue.Config{
			Name:                      "idle-check",
			VisibilityTimeout:         30 * time.Second,
			ConsumerHeartbeatInterval: 500 * time.Millisecond,
		}
	}

	q1 := queue.New(node1, cfg())
	defer q1.Close()

	q2 := queue.New(node2, cfg())
	defer q2.Close()

	// node2 consumes; no messages will ever be published
	q2.Consume(func(msg *queue.Message) error { return nil })

	// Let it settle, then measure over a 2 second idle window
	time.Sleep(1 * time.Second)

	before := ct2.count(queue.ExportedRegisterMsgType())
	time.Sleep(2 * time.Second)
	after := ct2.count(queue.ExportedRegisterMsgType())

	heartbeats := after - before

	// With a 500ms heartbeat we expect ~4 over 2s. Allow generous slack, but
	// anything near the old poll rate (40 at 50ms) is a regression.
	if heartbeats > 12 {
		t.Errorf("idle queue sent %d registrations in 2s — expected only heartbeats (~4)", heartbeats)
	}
	t.Logf("idle traffic over 2s: %d heartbeat registrations (old poll model would be ~40)", heartbeats)
}

// Messages must be delivered promptly — a push should land in well under the
// old 50ms poll interval.
func TestIntegrationPushDeliveryLatency(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19450")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19451")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19450"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "latency", VisibilityTimeout: 30 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "latency", VisibilityTimeout: 30 * time.Second})
	defer q2.Close()

	received := make(chan time.Time, 1)
	q2.Consume(func(msg *queue.Message) error {
		select {
		case received <- time.Now():
		default:
		}
		return nil
	})

	// Give the consumer a moment to register
	time.Sleep(300 * time.Millisecond)

	sent := time.Now()
	if err := q1.Publish(context.Background(), []byte("latency-probe")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case at := <-received:
		latency := at.Sub(sent)
		t.Logf("push delivery latency: %v", latency)
		if latency > 2*time.Second {
			t.Errorf("delivery took %v — push should be much faster", latency)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message was never delivered")
	}
}

// Prefetch must bound how much work a single consumer holds at once.
func TestIntegrationPrefetchFlowControl(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19460")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19461")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19460"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	const prefetch = 2

	q1 := queue.New(node1, &queue.Config{Name: "flow", VisibilityTimeout: 30 * time.Second, Prefetch: prefetch})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "flow", VisibilityTimeout: 30 * time.Second, Prefetch: prefetch})
	defer q2.Close()

	var concurrent atomic.Int64
	var maxConcurrent atomic.Int64
	release := make(chan struct{})

	q2.Consume(func(msg *queue.Message) error {
		cur := concurrent.Add(1)
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}
		<-release // hold the message until the test lets go
		concurrent.Add(-1)
		return nil
	})

	time.Sleep(300 * time.Millisecond)

	// Publish far more than the prefetch limit
	for i := 0; i < 20; i++ {
		q1.Publish(context.Background(), []byte(fmt.Sprintf("m%d", i)))
	}

	// Let the coordinator push as much as it is willing to
	time.Sleep(1500 * time.Millisecond)

	observed := maxConcurrent.Load()
	close(release)

	// Only q2 consumes, so it should never hold more than its prefetch.
	// (q1 has no consumer registered.)
	if observed > prefetch {
		t.Errorf("consumer held %d messages concurrently, prefetch was %d", observed, prefetch)
	}
	t.Logf("max concurrent messages at consumer: %d (prefetch %d)", observed, prefetch)
}

// Work should spread across multiple consumers rather than piling onto one.
func TestIntegrationWorkSpreadsAcrossConsumers(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19470")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19471")
	node2.Start()
	defer node2.Stop()

	node3 := createRealCluster(t, "127.0.0.1:19472")
	node3.Start()
	defer node3.Stop()

	node2.Join([]string{"127.0.0.1:19470"})
	node3.Join([]string{"127.0.0.1:19470"})
	waitForClusterSize(t, node1, 3, 5*time.Second)
	waitForClusterSize(t, node2, 3, 5*time.Second)
	waitForClusterSize(t, node3, 3, 5*time.Second)

	newQ := func(c *gossip.Cluster) *queue.Config {
		return &queue.Config{Name: "spread", VisibilityTimeout: 30 * time.Second, Prefetch: 4}
	}

	q1 := queue.New(node1, newQ(node1))
	defer q1.Close()
	q2 := queue.New(node2, newQ(node2))
	defer q2.Close()
	q3 := queue.New(node3, newQ(node3))
	defer q3.Close()

	var c1, c2, c3 atomic.Int64
	q1.Consume(func(msg *queue.Message) error { c1.Add(1); return nil })
	q2.Consume(func(msg *queue.Message) error { c2.Add(1); return nil })
	q3.Consume(func(msg *queue.Message) error { c3.Add(1); return nil })

	// Let all three register with the coordinator
	time.Sleep(500 * time.Millisecond)

	const total = 60
	for i := 0; i < total; i++ {
		if err := q1.Publish(context.Background(), []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if c1.Load()+c2.Load()+c3.Load() >= total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got := c1.Load() + c2.Load() + c3.Load()
	if got < total {
		t.Fatalf("expected %d messages consumed, got %d (n1=%d n2=%d n3=%d)", total, got, c1.Load(), c2.Load(), c3.Load())
	}

	t.Logf("distribution: n1=%d n2=%d n3=%d", c1.Load(), c2.Load(), c3.Load())

	// Every consumer should have taken some share
	for i, c := range []int64{c1.Load(), c2.Load(), c3.Load()} {
		if c == 0 {
			t.Errorf("consumer %d received no messages — work is not spreading", i+1)
		}
	}
}

// A consumer joining after messages are already queued must start receiving.
func TestIntegrationLateConsumerDrainsBacklog(t *testing.T) {
	node1 := createRealCluster(t, "127.0.0.1:19480")
	node1.Start()
	defer node1.Stop()

	node2 := createRealCluster(t, "127.0.0.1:19481")
	node2.Start()
	defer node2.Stop()

	node2.Join([]string{"127.0.0.1:19480"})
	waitForClusterSize(t, node1, 2, 5*time.Second)
	waitForClusterSize(t, node2, 2, 5*time.Second)

	q1 := queue.New(node1, &queue.Config{Name: "backlog", VisibilityTimeout: 30 * time.Second})
	defer q1.Close()

	q2 := queue.New(node2, &queue.Config{Name: "backlog", VisibilityTimeout: 30 * time.Second})
	defer q2.Close()

	// Publish with nobody consuming
	const total = 15
	for i := 0; i < total; i++ {
		q1.Publish(context.Background(), []byte(fmt.Sprintf("m%d", i)))
	}

	time.Sleep(300 * time.Millisecond)

	// Now attach a consumer — registration should trigger dispatch of the backlog
	var count atomic.Int64
	q2.Consume(func(msg *queue.Message) error {
		count.Add(1)
		return nil
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if count.Load() < total {
		t.Errorf("expected late consumer to drain %d backlogged messages, got %d", total, count.Load())
	}
}
