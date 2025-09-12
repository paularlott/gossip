package gossip

import (
	"testing"

	"github.com/google/uuid"
)

func TestValidateNodeGroupParams(t *testing.T) {
	// Test nil cluster
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for nil cluster")
		}
	}()
	validateNodeGroupParams(nil, map[string]string{"key": "value"})
}

func TestValidateNodeGroupParamsEmptyCriteria(t *testing.T) {
	cluster := createTestCluster(t)
	defer cluster.Stop()

	// Test empty criteria
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for empty criteria")
		}
	}()
	validateNodeGroupParams(cluster, map[string]string{})
}

func TestNodeMatchesCriteriaHelper(t *testing.T) {
	node := newNode(NodeID(uuid.New()), "127.0.0.1:8001")
	node.metadata.SetString("zone", "us-west")
	node.metadata.SetString("tags", "production,web,frontend")

	// Test exact match
	criteria := map[string]string{"zone": "us-west"}
	if !nodeMatchesCriteria(node, criteria) {
		t.Error("Should match exact criteria")
	}

	// Test any value
	criteria = map[string]string{"zone": MetadataAnyValue}
	if !nodeMatchesCriteria(node, criteria) {
		t.Error("Should match any value criteria")
	}

	// Test contains
	criteria = map[string]string{"tags": MetadataContainsPrefix + "web"}
	if !nodeMatchesCriteria(node, criteria) {
		t.Error("Should match contains criteria")
	}

	// Test empty string (edge case fix)
	criteria = map[string]string{"empty": ""}
	node.metadata.SetString("empty", "")
	if !nodeMatchesCriteria(node, criteria) {
		t.Error("Should handle empty string criteria")
	}

	// Test missing metadata
	criteria = map[string]string{"missing": "value"}
	if nodeMatchesCriteria(node, criteria) {
		t.Error("Should not match missing metadata")
	}
}

func TestFnvHash(t *testing.T) {
	nodeID1 := NodeID(uuid.New())
	nodeID2 := NodeID(uuid.New())

	hash1 := fnvHash(nodeID1)
	hash2 := fnvHash(nodeID2)

	// Same input should produce same hash
	if fnvHash(nodeID1) != hash1 {
		t.Error("Hash should be deterministic")
	}

	// Different inputs should produce different hashes (very likely)
	if hash1 == hash2 {
		t.Error("Different inputs should produce different hashes")
	}
}