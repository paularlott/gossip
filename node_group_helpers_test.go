package gossip

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateNodeGroupParams(t *testing.T) {
	c := newUnitCluster(t)
	assert.Panics(t, func() { validateNodeGroupParams(nil, map[string]string{"key": "value"}) }, "nil cluster")
	assert.Panics(t, func() { validateNodeGroupParams(c, map[string]string{}) }, "empty criteria")
	assert.Panics(t, func() { validateNodeGroupParams(c, nil) }, "nil criteria")
	assert.NotPanics(t, func() { validateNodeGroupParams(c, map[string]string{"key": "value"}) })
}

func TestNodeMatchesCriteria(t *testing.T) {
	c := newUnitCluster(t)
	node := clusterNode(c, NodeAlive, "zone", "us-west", "tags", "production,web,frontend", "empty", "")

	tests := []struct {
		name     string
		criteria map[string]string
		want     bool
	}{
		{"exact", map[string]string{"zone": "us-west"}, true},
		{"wrong value", map[string]string{"zone": "us-east"}, false},
		{"any value", map[string]string{"zone": MetadataAnyValue}, true},
		{"any value of a key that is missing", map[string]string{"missing": MetadataAnyValue}, false},
		{"contains", map[string]string{"tags": MetadataContainsPrefix + "web"}, true},
		{"does not contain", map[string]string{"tags": MetadataContainsPrefix + "api"}, false},
		{"empty value matches an empty value", map[string]string{"empty": ""}, true},
		{"empty value does not match another", map[string]string{"zone": ""}, false},
		{"missing key", map[string]string{"missing": "value"}, false},
		{"every key must match", map[string]string{"zone": "us-west", "tags": MetadataContainsPrefix + "api"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NodeMatchesCriteria(node, tt.criteria))
		})
	}
}
