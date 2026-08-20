package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/logger"
)

// --- Mock Transport ---

type mockTransport struct {
	ch chan *gossip.Packet
}

func newMockTransport() *mockTransport {
	return &mockTransport{ch: make(chan *gossip.Packet, 16)}
}

func (t *mockTransport) Name() string { return "mock" }

func (t *mockTransport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	return nil
}

func (t *mockTransport) PacketChannel() chan *gossip.Packet {
	return t.ch
}

func (t *mockTransport) Send(transportType gossip.TransportType, node *gossip.Node, packet *gossip.Packet) error {
	return nil
}

func (t *mockTransport) SendWithReply(node *gossip.Node, packet *gossip.Packet) (*gossip.Packet, error) {
	return nil, nil
}

func newTestCluster(t *testing.T) *gossip.Cluster {
	t.Helper()
	config := gossip.DefaultConfig()
	config.NodeID = uuid.New().String()
	config.Transport = newMockTransport()
	config.MsgCodec = codec.NewJSONCodec()
	config.Logger = logger.NewNullLogger()
	cluster, err := gossip.NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	return cluster
}

// --- Config Tests ---

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Name != "default" {
		t.Errorf("expected Name 'default', got %q", cfg.Name)
	}
	if cfg.VisibilityTimeout != 30*time.Second {
		t.Errorf("expected VisibilityTimeout 30s, got %v", cfg.VisibilityTimeout)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", cfg.MaxRetries)
	}
}

func TestConfigValidate(t *testing.T) {
	var nilCfg *Config
	v := nilCfg.validate()
	d := DefaultConfig()

	if v.Name != d.Name || v.VisibilityTimeout != d.VisibilityTimeout || v.MaxRetries != d.MaxRetries {
		t.Error("nil config should produce defaults")
	}

	cfg := &Config{VisibilityTimeout: -1, MaxRetries: -1}
	v = cfg.validate()
	if v.VisibilityTimeout != d.VisibilityTimeout {
		t.Errorf("negative VisibilityTimeout should default, got %v", v.VisibilityTimeout)
	}
	if v.MaxRetries != d.MaxRetries {
		t.Errorf("negative MaxRetries should default, got %d", v.MaxRetries)
	}
}

// testDequeue registers a consumer with generous prefetch and reserves the next
// pending message for it, mirroring what the dispatcher does. Test helper for
// exercising coordinator state without standing up a real dispatcher.
func testDequeue(qc *queueCoordinator, consumer gossip.NodeID) *inflightMessage {
	qc.registerConsumer(consumer, 1000)
	return qc.reserveForDispatch()
}

// --- Coordinator Tests ---

func TestCoordinatorEnqueueReserve(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3})
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())

	msg := &pendingMessage{messageID: "m1", payload: []byte("hello")}
	if !qc.enqueue(msg) {
		t.Fatal("enqueue should succeed")
	}

	if qc.pendingCount() != 1 {
		t.Errorf("expected 1 pending, got %d", qc.pendingCount())
	}

	inflight := testDequeue(qc, consumer)
	if inflight == nil {
		t.Fatal("reserve should return a message")
	}
	if inflight.messageID != "m1" {
		t.Errorf("expected messageID 'm1', got %q", inflight.messageID)
	}
	if inflight.attempts != 1 {
		t.Errorf("expected attempts 1, got %d", inflight.attempts)
	}
	if qc.pendingCount() != 0 {
		t.Error("pending should be 0 after reserve")
	}
	if qc.inflightCount() != 1 {
		t.Error("inflight should be 1 after reserve")
	}
}

func TestCoordinatorReserveEmpty(t *testing.T) {
	qc := newQueueCoordinator(DefaultConfig())
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())
	if testDequeue(qc, consumer) != nil {
		t.Error("reserve on empty queue should return nil")
	}
}

func TestCoordinatorAck(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3})
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())
	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("data")})

	inflight := testDequeue(qc, consumer)
	if !qc.ack(inflight.deliveryID) {
		t.Fatal("ack should succeed")
	}
	if qc.inflightCount() != 0 {
		t.Error("inflight should be 0 after ack")
	}
}

func TestCoordinatorAckUnknown(t *testing.T) {
	qc := newQueueCoordinator(DefaultConfig())
	defer qc.close()

	if qc.ack("nonexistent") {
		t.Error("ack of unknown deliveryID should return false")
	}
}

func TestCoordinatorMaxSize(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3, MaxSize: 2})
	defer qc.close()

	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("a")})
	qc.enqueue(&pendingMessage{messageID: "m2", payload: []byte("b")})

	if qc.enqueue(&pendingMessage{messageID: "m3", payload: []byte("c")}) {
		t.Error("enqueue should fail when queue is full")
	}
}

func TestCoordinatorRedelivery(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 10 * time.Millisecond, MaxRetries: 3})
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())
	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("data")})

	inflight := testDequeue(qc, consumer)
	if inflight == nil {
		t.Fatal("expected message")
	}

	// Wait for visibility timeout
	time.Sleep(50 * time.Millisecond)
	qc.redeliverExpired()

	// Message should be back in pending
	if qc.pendingCount() != 1 {
		t.Errorf("expected 1 pending after redelivery, got %d", qc.pendingCount())
	}
	if qc.inflightCount() != 0 {
		t.Errorf("expected 0 inflight after redelivery, got %d", qc.inflightCount())
	}
}

func TestCoordinatorMaxRetriesDeadLetter(t *testing.T) {
	var deadLettered atomic.Int64

	qc := newQueueCoordinator(&Config{
		VisibilityTimeout: 10 * time.Millisecond,
		MaxRetries:        2,
		DeadLetterHandler: func(payload []byte, attempts int) {
			deadLettered.Add(1)
		},
	})
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())
	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("data"), attempts: 0})

	// First delivery
	inflight := testDequeue(qc, consumer)
	if inflight == nil {
		t.Fatal("expected message")
	}
	time.Sleep(20 * time.Millisecond)
	qc.redeliverExpired()

	// Second delivery (attempt 2 = MaxRetries)
	inflight = testDequeue(qc, consumer)
	if inflight == nil {
		t.Fatal("expected redelivered message")
	}
	time.Sleep(20 * time.Millisecond)
	qc.redeliverExpired()

	// Should be dead-lettered now
	time.Sleep(20 * time.Millisecond)
	if deadLettered.Load() != 1 {
		t.Errorf("expected 1 dead-lettered message, got %d", deadLettered.Load())
	}
	if qc.pendingCount() != 0 {
		t.Errorf("expected 0 pending after dead-letter, got %d", qc.pendingCount())
	}
}

func TestCoordinatorReleaseByConsumer(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3})
	defer qc.close()

	consumer1 := gossip.NodeID(uuid.New())
	consumer2 := gossip.NodeID(uuid.New())

	// Prefetch of 1 each makes assignment deterministic: the first reserve fills
	// consumer1 to its limit, so the second must go to consumer2.
	qc.registerConsumer(consumer1, 1)
	qc.registerConsumer(consumer2, 1)

	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("a")})
	qc.enqueue(&pendingMessage{messageID: "m2", payload: []byte("b")})

	first := qc.reserveForDispatch()
	second := qc.reserveForDispatch()
	if first == nil || second == nil {
		t.Fatal("expected both messages to be reserved")
	}
	if first.consumerID == second.consumerID {
		t.Fatal("prefetch=1 should have spread the messages across both consumers")
	}

	qc.releaseByConsumer(first.consumerID)

	// The first message should be back in pending, the second still inflight
	if qc.pendingCount() != 1 {
		t.Errorf("expected 1 pending, got %d", qc.pendingCount())
	}
	if qc.inflightCount() != 1 {
		t.Errorf("expected 1 inflight, got %d", qc.inflightCount())
	}
}

func TestCoordinatorExportImport(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3})
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())
	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("a")})
	qc.enqueue(&pendingMessage{messageID: "m2", payload: []byte("b")})
	testDequeue(qc, consumer) // m1 goes inflight

	entries := qc.exportAll()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries exported, got %d", len(entries))
	}
	if qc.totalCount() != 0 {
		t.Error("coordinator should be empty after export")
	}

	// Import into a new coordinator
	qc2 := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3})
	defer qc2.close()

	accepted := qc2.importEntries(entries)
	if accepted != 2 {
		t.Errorf("expected 2 accepted, got %d", accepted)
	}
	if qc2.pendingCount() != 2 {
		t.Errorf("expected 2 pending after import, got %d", qc2.pendingCount())
	}
}

func TestCoordinatorFIFO(t *testing.T) {
	qc := newQueueCoordinator(DefaultConfig())
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())

	for i := 0; i < 5; i++ {
		qc.enqueue(&pendingMessage{messageID: string(rune('a' + i)), payload: []byte{byte(i)}})
	}

	// Reservation should be in FIFO order
	for i := 0; i < 5; i++ {
		msg := testDequeue(qc, consumer)
		if msg == nil {
			t.Fatalf("expected message %d", i)
		}
		if msg.payload[0] != byte(i) {
			t.Errorf("expected payload %d, got %d", i, msg.payload[0])
		}
	}
}

// --- Reply Map Tests ---

func TestReplyMapRegisterDeliver(t *testing.T) {
	rm := newReplyMap()
	ch := rm.register("corr-1")

	go func() {
		time.Sleep(10 * time.Millisecond)
		rm.deliver("corr-1", replyResult{Payload: []byte("answer")})
	}()

	result := <-ch
	if string(result.Payload) != "answer" {
		t.Errorf("expected 'answer', got %q", string(result.Payload))
	}
}

func TestReplyMapDeliverUnknown(t *testing.T) {
	rm := newReplyMap()
	if rm.deliver("nonexistent", replyResult{}) {
		t.Error("deliver to unknown correlationID should return false")
	}
}

func TestReplyMapUnregister(t *testing.T) {
	rm := newReplyMap()
	rm.register("corr-1")
	rm.unregister("corr-1")

	if rm.deliver("corr-1", replyResult{}) {
		t.Error("deliver after unregister should return false")
	}
}

// --- Queue (local coordinator path) ---

func TestQueuePublishConsume(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{VisibilityTimeout: 5 * time.Second})
	defer q.Close()

	// Publish
	err := q.Publish(context.Background(), []byte("task-1"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if q.PendingCount() != 1 {
		t.Errorf("expected 1 pending, got %d", q.PendingCount())
	}

	// Consume
	var received atomic.Value
	q.Consume(func(msg *Message) error {
		received.Store(string(msg.Payload))
		return nil
	})

	// Wait for consumer to process
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if received.Load() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if received.Load() == nil {
		t.Fatal("consumer did not receive the message")
	}
	if received.Load().(string) != "task-1" {
		t.Errorf("expected 'task-1', got %q", received.Load())
	}
}

func TestQueuePublishMultiple(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{VisibilityTimeout: 5 * time.Second})
	defer q.Close()

	for i := 0; i < 10; i++ {
		q.Publish(context.Background(), []byte("msg"))
	}

	if q.PendingCount() != 10 {
		t.Errorf("expected 10 pending, got %d", q.PendingCount())
	}
}

func TestQueueMaxSize(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{MaxSize: 3, VisibilityTimeout: 5 * time.Second})
	defer q.Close()

	for i := 0; i < 3; i++ {
		err := q.Publish(context.Background(), []byte("msg"))
		if err != nil {
			t.Fatalf("publish %d should succeed: %v", i, err)
		}
	}

	err := q.Publish(context.Background(), []byte("overflow"))
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestQueueClosed(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, nil)
	q.Close()

	err := q.Publish(context.Background(), []byte("msg"))
	if err != ErrQueueClosed {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}
}

func TestQueueCloseIdempotent(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, nil)
	q.Close()
	q.Close() // should not panic
}

func TestQueueConsumeAcks(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{VisibilityTimeout: 5 * time.Second})
	defer q.Close()

	q.Publish(context.Background(), []byte("ack-test"))

	var processed atomic.Int64
	q.Consume(func(msg *Message) error {
		processed.Add(1)
		return nil // ack
	})

	time.Sleep(200 * time.Millisecond)

	// Message should be processed and removed
	if q.PendingCount() != 0 {
		t.Errorf("expected 0 pending after ack, got %d", q.PendingCount())
	}
	if q.InflightCount() != 0 {
		t.Errorf("expected 0 inflight after ack, got %d", q.InflightCount())
	}
	if processed.Load() != 1 {
		t.Errorf("expected 1 processed, got %d", processed.Load())
	}
}

func TestQueueConsumeNackRedelivers(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{VisibilityTimeout: 100 * time.Millisecond, MaxRetries: 5})
	defer q.Close()

	q.Publish(context.Background(), []byte("nack-test"))

	var attempts atomic.Int64
	q.Consume(func(msg *Message) error {
		count := attempts.Add(1)
		if count < 3 {
			return fmt.Errorf("simulated failure") // nack
		}
		return nil // ack on 3rd attempt
	})

	// Wait for redeliveries
	time.Sleep(1 * time.Second)

	if attempts.Load() < 3 {
		t.Errorf("expected at least 3 attempts, got %d", attempts.Load())
	}
}

func TestMultipleQueuesSameCluster(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q1 := New(cluster, &Config{Name: "q1", VisibilityTimeout: 5 * time.Second})
	defer q1.Close()

	q2 := New(cluster, &Config{Name: "q2", VisibilityTimeout: 5 * time.Second})
	defer q2.Close()

	q1.Publish(context.Background(), []byte("for-q1"))
	q2.Publish(context.Background(), []byte("for-q2"))

	if q1.PendingCount() != 1 || q2.PendingCount() != 1 {
		t.Error("each queue should have 1 pending message independently")
	}
}

func TestMultipleQueuesDuplicateNamePanics(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{Name: "dup"})
	defer q.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate queue name")
		}
	}()

	_ = New(cluster, &Config{Name: "dup"})
}

// --- Regression tests for reviewed bugs ---

// Regression: deliver() must never panic when it races with unregister().
// Previously unregister() closed the channel, so a worker reply arriving just
// as a Request timed out would panic with "send on closed channel" and, since
// packet dispatch has no recover(), take down the whole node.
func TestReplyMapDeliverUnregisterRaceNoPanic(t *testing.T) {
	rm := newReplyMap()

	var wg sync.WaitGroup
	const iterations = 20000

	for i := 0; i < iterations; i++ {
		id := fmt.Sprintf("corr-%d", i)
		rm.register(id)

		wg.Add(2)
		go func() {
			defer wg.Done()
			rm.deliver(id, replyResult{Payload: []byte("x")})
		}()
		go func() {
			defer wg.Done()
			rm.unregister(id)
		}()
	}

	wg.Wait()
	// Reaching here without a panic is the assertion.
}

// Regression: the handoff worker must exit promptly when Close lands during the
// debounce window. Previously the drain loop received from a closed channel,
// which always succeeds, so the loop spun at 100% CPU forever.
func TestHandoffWorkerExitsWhenClosedDuringDebounce(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{Name: "spin-check"})

	// Signal a handoff so the worker consumes it and enters the debounce sleep.
	q.requestHandoff()
	time.Sleep(10 * time.Millisecond) // worker is now debouncing

	// Close during the debounce window. Close waits on handoffDone, so if the
	// worker were spinning this would block and the test would time out.
	done := make(chan struct{})
	go func() {
		q.Close()
		close(done)
	}()

	select {
	case <-done:
		// Worker exited cleanly
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return — handoff worker likely spinning or stuck")
	}
}

// Regression: requestHandoff must remain safe after Close (it must not panic on
// a closed channel). handoffReq is deliberately never closed.
func TestRequestHandoffAfterCloseIsSafe(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{Name: "post-close-signal"})
	q.Close()

	// Must not panic
	for i := 0; i < 100; i++ {
		q.requestHandoff()
	}
}

// Regression: a handoff whose destination accepts nothing (e.g. unknown queue
// name) must not silently drop the messages.
func TestHandoffShortAcceptReimportsMessages(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3})
	defer qc.close()

	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("a")})
	qc.enqueue(&pendingMessage{messageID: "m2", payload: []byte("b")})

	// Simulate the export half of a handoff
	entries := qc.exportAll()
	if len(entries) != 2 {
		t.Fatalf("expected 2 exported entries, got %d", len(entries))
	}
	if qc.totalCount() != 0 {
		t.Fatalf("coordinator should be empty after export, got %d", qc.totalCount())
	}

	// Simulate a destination that accepted nothing → caller must re-import
	accepted := 0
	if accepted < len(entries) {
		qc.importEntries(entries)
	}

	if qc.pendingCount() != 2 {
		t.Errorf("expected 2 messages re-imported after short accept, got %d", qc.pendingCount())
	}
}

// Regression: Consume after Close must not start an unstoppable goroutine.
func TestConsumeAfterCloseDoesNotStart(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{Name: "consume-after-close"})
	q.Close()

	var called atomic.Int64
	q.Consume(func(msg *Message) error {
		called.Add(1)
		return nil
	})

	if q.consumer.isRunning() {
		t.Error("consumer should not be running after Close")
	}
}

// A panicking DeadLetterHandler must not take down the process.
func TestDeadLetterHandlerPanicIsContained(t *testing.T) {
	var called atomic.Int64

	qc := newQueueCoordinator(&Config{
		VisibilityTimeout: 5 * time.Second,
		MaxRetries:        1,
		DeadLetterHandler: func(payload []byte, attempts int) {
			called.Add(1)
			panic("handler blew up")
		},
	})
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())
	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("data")})

	inflight := testDequeue(qc, consumer)
	if inflight == nil {
		t.Fatal("expected a message")
	}

	// Exceeds MaxRetries=1 → dead letter
	qc.nack(inflight.deliveryID)

	// Give the handler goroutine a chance to run and panic
	time.Sleep(200 * time.Millisecond)

	if called.Load() != 1 {
		t.Errorf("expected dead-letter handler to be called once, got %d", called.Load())
	}
	// Surviving to here means the panic was contained.
}

// Request must surface context cancellation distinctly from reply timeout.
func TestRequestContextCancellation(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{Name: "ctx-cancel"})
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// No consumer, so this waits until ctx is cancelled
	_, err := q.Request(ctx, []byte("no-one-home"), 30*time.Second)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// Publish must respect an already-cancelled context.
func TestPublishRespectsCancelledContext(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{Name: "ctx-precancelled"})
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := q.Publish(ctx, []byte("should-not-enqueue"))
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if q.PendingCount() != 0 {
		t.Errorf("expected nothing enqueued, got %d pending", q.PendingCount())
	}
}

// --- Push-model tests ---

func TestCoordinatorPrefetchLimitsDispatch(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3})
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())
	qc.registerConsumer(consumer, 2) // prefetch of 2

	for i := 0; i < 5; i++ {
		qc.enqueue(&pendingMessage{messageID: fmt.Sprintf("m%d", i), payload: []byte("x")})
	}

	// Only 2 should be reservable — prefetch is the cap
	first := qc.reserveForDispatch()
	second := qc.reserveForDispatch()
	third := qc.reserveForDispatch()

	if first == nil || second == nil {
		t.Fatal("expected two reservations within prefetch")
	}
	if third != nil {
		t.Fatal("third reservation should be blocked by prefetch limit")
	}

	// Acking frees capacity for exactly one more
	qc.ack(first.deliveryID)

	fourth := qc.reserveForDispatch()
	if fourth == nil {
		t.Fatal("expected a reservation after ack freed capacity")
	}
	if qc.reserveForDispatch() != nil {
		t.Fatal("should be back at the prefetch limit")
	}
}

func TestCoordinatorNoConsumersNoDispatch(t *testing.T) {
	qc := newQueueCoordinator(DefaultConfig())
	defer qc.close()

	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("x")})

	if qc.reserveForDispatch() != nil {
		t.Fatal("should not reserve when no consumers are registered")
	}
	if qc.pendingCount() != 1 {
		t.Errorf("message should stay pending, got %d", qc.pendingCount())
	}
}

func TestCoordinatorRoundRobinFairness(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3})
	defer qc.close()

	const numConsumers = 4
	consumers := make([]gossip.NodeID, numConsumers)
	for i := range consumers {
		consumers[i] = gossip.NodeID(uuid.New())
		qc.registerConsumer(consumers[i], 100)
	}

	const numMessages = 40
	for i := 0; i < numMessages; i++ {
		qc.enqueue(&pendingMessage{messageID: fmt.Sprintf("m%d", i), payload: []byte("x")})
	}

	counts := make(map[gossip.NodeID]int)
	for i := 0; i < numMessages; i++ {
		res := qc.reserveForDispatch()
		if res == nil {
			t.Fatalf("expected reservation %d", i)
		}
		counts[res.consumerID]++
	}

	// Round-robin should distribute evenly
	expected := numMessages / numConsumers
	for _, c := range consumers {
		if counts[c] != expected {
			t.Errorf("consumer got %d messages, expected even split of %d", counts[c], expected)
		}
	}
}

func TestCoordinatorReleaseReservationPreservesOrder(t *testing.T) {
	qc := newQueueCoordinator(&Config{VisibilityTimeout: 5 * time.Second, MaxRetries: 3})
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())
	qc.registerConsumer(consumer, 10)

	qc.enqueue(&pendingMessage{messageID: "first", payload: []byte("1")})
	qc.enqueue(&pendingMessage{messageID: "second", payload: []byte("2")})

	res := qc.reserveForDispatch()
	if res == nil || res.messageID != "first" {
		t.Fatal("expected to reserve 'first'")
	}

	// A failed push returns it to the front, not the back
	qc.releaseReservation(res.deliveryID)

	next := qc.reserveForDispatch()
	if next == nil || next.messageID != "first" {
		t.Fatalf("expected 'first' back at the head of the queue, got %v", next)
	}
	// And the failed attempt shouldn't have been counted against it
	if next.attempts != 1 {
		t.Errorf("expected attempts to be 1 after a failed push, got %d", next.attempts)
	}
}

func TestCoordinatorStaleConsumerExpiry(t *testing.T) {
	qc := newQueueCoordinator(&Config{
		VisibilityTimeout:         5 * time.Second,
		MaxRetries:                3,
		ConsumerHeartbeatInterval: 20 * time.Millisecond, // TTL = 60ms
	})
	defer qc.close()

	consumer := gossip.NodeID(uuid.New())
	qc.registerConsumer(consumer, 10)
	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("x")})

	res := qc.reserveForDispatch()
	if res == nil {
		t.Fatal("expected a reservation")
	}
	if qc.consumerCount() != 1 {
		t.Fatalf("expected 1 registered consumer, got %d", qc.consumerCount())
	}

	// Stop heartbeating and let the registration go stale
	time.Sleep(150 * time.Millisecond)
	qc.expireStaleConsumers()

	if qc.consumerCount() != 0 {
		t.Errorf("stale consumer should have been expired, got %d", qc.consumerCount())
	}
	if qc.pendingCount() != 1 {
		t.Errorf("expired consumer's message should be re-queued, got %d pending", qc.pendingCount())
	}
}

func TestCoordinatorSignalsDispatchOnEnqueue(t *testing.T) {
	qc := newQueueCoordinator(DefaultConfig())
	defer qc.close()

	// Drain any startup signal
	select {
	case <-qc.dispatchSignal:
	default:
	}

	qc.enqueue(&pendingMessage{messageID: "m1", payload: []byte("x")})

	select {
	case <-qc.dispatchSignal:
		// good — enqueue woke the dispatcher
	case <-time.After(time.Second):
		t.Fatal("enqueue did not signal the dispatcher")
	}
}

func TestConsumerManagerOfferRespectsPrefetch(t *testing.T) {
	cm := newConsumerManager(2)
	if !cm.markRunning() {
		t.Fatal("markRunning should succeed on a fresh manager")
	}

	mk := func(id string) *Message { return &Message{DeliveryID: id} }

	if !cm.offer(mk("a")) {
		t.Fatal("first offer should be accepted")
	}
	if !cm.offer(mk("b")) {
		t.Fatal("second offer should be accepted")
	}
	if cm.offer(mk("c")) {
		t.Fatal("third offer should be rejected — buffer is at prefetch")
	}
}

func TestConsumerManagerOfferRejectedWhenNotRunning(t *testing.T) {
	cm := newConsumerManager(4)

	// Not running yet
	if cm.offer(&Message{DeliveryID: "x"}) {
		t.Fatal("offer should be rejected before Consume starts")
	}

	cm.markRunning()
	if !cm.offer(&Message{DeliveryID: "y"}) {
		t.Fatal("offer should be accepted once running")
	}

	cm.stop()
	if cm.offer(&Message{DeliveryID: "z"}) {
		t.Fatal("offer should be rejected after stop")
	}
}

func TestConsumerManagerMarkRunningOnce(t *testing.T) {
	cm := newConsumerManager(4)

	if !cm.markRunning() {
		t.Fatal("first markRunning should succeed")
	}
	if cm.markRunning() {
		t.Fatal("second markRunning should report already-running")
	}

	cm.stop()
	if cm.markRunning() {
		t.Fatal("markRunning should fail after stop")
	}
}

// A panicking handler must nack the message rather than crash the node.
func TestHandlerPanicIsContainedAndNacks(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	q := New(cluster, &Config{
		Name:              "panic-handler",
		VisibilityTimeout: 10 * time.Second,
		MaxRetries:        10,
		Prefetch:          1,
	})
	defer q.Close()

	var attempts atomic.Int64
	q.Consume(func(msg *Message) error {
		n := attempts.Add(1)
		if n <= 2 {
			panic("handler exploded")
		}
		return nil
	})

	q.Publish(context.Background(), []byte("boom"))

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if attempts.Load() >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if attempts.Load() < 3 {
		t.Errorf("expected the message to be retried after panics, got %d attempts", attempts.Load())
	}
	// Surviving here means the panics were contained.
}
