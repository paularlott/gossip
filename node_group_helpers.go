package gossip

import "strings"

// nodeMatchesCriteria checks if a node matches all the metadata criteria
func nodeMatchesCriteria(node *Node, metadataCriteria map[string]string) bool {
	for key, expectedValue := range metadataCriteria {
		if !node.Metadata.Exists(key) {
			return false
		}

		if len(expectedValue) > 0 && expectedValue[0] == MetadataContainsPrefix[0] {
			if !strings.Contains(node.Metadata.GetString(key), expectedValue[1:]) {
				return false
			}
		} else if expectedValue != MetadataAnyValue && expectedValue != node.Metadata.GetString(key) {
			return false
		}
	}
	return true
}

// fnvHash computes FNV-1a hash for better distribution
func fnvHash(nodeID NodeID) uint32 {
	idBytes := nodeID[:]
	hash := uint32(2166136261) // FNV offset basis

	for _, b := range idBytes {
		hash ^= uint32(b)
		hash *= 16777619 // FNV prime
	}

	return hash
}

// validateNodeGroupParams validates constructor parameters
func validateNodeGroupParams(cluster *Cluster, criteria map[string]string) {
	if cluster == nil {
		panic("gossip: cluster cannot be nil")
	}
	if len(criteria) == 0 {
		panic("gossip: criteria cannot be empty")
	}
}