package leader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQuorumCalculationSimple(t *testing.T) {
	config := DefaultConfig()

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

	// Create a minimal election instance just for testing quorum calculation
	election := &LeaderElection{
		config: config,
	}

	for _, test := range tests {
		result := election.calculateQuorumForNodes(test.nodes)
		assert.Equal(t, test.expected, result, "nodes: %d", test.nodes)
	}
}

func TestDefaultConfigSimple(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, 1*time.Second, config.LeaderCheckInterval)
	assert.Equal(t, 3*time.Second, config.LeaderTimeout)
	assert.Nil(t, config.MetadataCriteria)
}

func TestEventTypeStringSimple(t *testing.T) {
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
