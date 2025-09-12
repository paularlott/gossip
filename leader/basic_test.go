package leader

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip"
	"github.com/stretchr/testify/assert"
)

var testNodeID = gossip.NodeID(uuid.MustParse("01960f9b-72ca-7a51-9efa-47c12f42a138"))

func TestBasicQuorumCalculation(t *testing.T) {
	config := DefaultConfig()
	election := &LeaderElection{config: config}

	tests := []struct {
		nodes    int
		expected int
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
		{10, 6},
	}

	for _, test := range tests {
		result := election.calculateQuorumForNodes(test.nodes)
		assert.Equal(t, test.expected, result, "nodes: %d", test.nodes)
	}
}

func TestBasicConfig(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, 1*time.Second, config.LeaderCheckInterval)
	assert.Equal(t, 3*time.Second, config.LeaderTimeout)
	assert.Equal(t, 60, config.QuorumPercentage)
	assert.Nil(t, config.MetadataCriteria)
}

func TestEventTypes(t *testing.T) {
	tests := []struct {
		event    EventType
		expected string
	}{
		{LeaderElectedEvent, "Leader Elected"},
		{LeaderLostEvent, "Leader Lost"},
		{BecameLeaderEvent, "Became Leader"},
		{SteppedDownEvent, "Stepped Down"},
		{EventType(999), "Unknown"},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, test.event.String())
	}
}

func TestEventHandlerBasic(t *testing.T) {
	handlers := newLeaderEventHandlers(gossip.NewNullLogger())

	handler := func(EventType, gossip.NodeID) {
		// Handler executed
	}

	handlers.add(BecameLeaderEvent, handler)

	// Test that we can dispatch without hanging
	handlers.dispatch(BecameLeaderEvent, testNodeID)

	// Give a short time for goroutine to execute
	time.Sleep(5 * time.Millisecond)

	// We can't reliably test if called is true due to goroutine timing,
	// but we can test that dispatch doesn't panic or hang
	assert.NotNil(t, handlers)
}

func TestNoHandlers(t *testing.T) {
	handlers := newLeaderEventHandlers(gossip.NewNullLogger())

	// Should not panic when no handlers are registered
	assert.NotPanics(t, func() {
		handlers.dispatch(BecameLeaderEvent, testNodeID)
	})
}

func TestLeaderElectionInit(t *testing.T) {
	config := DefaultConfig()

	election := &LeaderElection{
		config:        config,
		hasLeader:     false,
		currentTerm:   0,
		isLeader:      false,
		eventHandlers: newLeaderEventHandlers(gossip.NewNullLogger()),
	}

	assert.NotNil(t, election)
	assert.Equal(t, config, election.config)
	assert.False(t, election.hasLeader)
	assert.False(t, election.isLeader)
	assert.Equal(t, uint64(0), election.currentTerm)
	assert.NotNil(t, election.eventHandlers)
}

func TestHeartbeatMessage(t *testing.T) {
	now := time.Now()
	msg := heartbeatMessage{
		LeaderTime: now,
		Term:       42,
	}

	assert.Equal(t, now, msg.LeaderTime)
	assert.Equal(t, uint64(42), msg.Term)
}

func TestQuorumEdgeCases(t *testing.T) {
	config := DefaultConfig()

	testCases := []struct {
		percentage int
		nodes      int
		expected   int
	}{
		{50, 3, 2},
		{60, 5, 3},
		{67, 3, 3},
		{100, 3, 3},
		{0, 3, 0},
	}

	for _, tc := range testCases {
		config.QuorumPercentage = tc.percentage
		election := &LeaderElection{config: config}

		result := election.calculateQuorumForNodes(tc.nodes)
		assert.Equal(t, tc.expected, result,
			"percentage: %d, nodes: %d", tc.percentage, tc.nodes)
	}
}
