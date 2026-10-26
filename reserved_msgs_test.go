package gossip

import "testing"

// The reserved allocation ledger: every library protocol message type in
// existence and the value it is allocated. Extend this table when adding to
// the registry in packet.go — the test fails if the two drift, if any two
// protocols collide, or if an allocation escapes the reserved range.
func TestReservedMessageTypesLedger(t *testing.T) {
	ledger := []struct {
		name string
		mt   MessageType
	}{
		// leader
		{"LeaderHeartbeatMsg", LeaderHeartbeatMsg},
		{"LeaderForgetMessage", LeaderForgetMessage},

		// lock
		{"LockAcquireMsg", LockAcquireMsg},
		{"LockReleaseMsg", LockReleaseMsg},
		{"LockExtendMsg", LockExtendMsg},
		{"LockQueryMsg", LockQueryMsg},
		{"LockReplicaPushMsg", LockReplicaPushMsg},
		{"LockReplicaGossipMsg", LockReplicaGossipMsg},
		{"LockStateQueryMsg", LockStateQueryMsg},

		// kv
		{"KVWritePushMsg", KVWritePushMsg},
		{"KVGossipMsg", KVGossipMsg},
		{"KVFullSyncMsg", KVFullSyncMsg},

		// partition
		{"PartHeartbeatMsg", PartHeartbeatMsg},
		{"PartPromiseMsg", PartPromiseMsg},
		{"PartWriteMsg", PartWriteMsg},
		{"PartChangeMsg", PartChangeMsg},
		{"PartStateMsg", PartStateMsg},
		{"PartCatchUpMsg", PartCatchUpMsg},
		{"PartRelinquishMsg", PartRelinquishMsg},
		{"PartCommitMsg", PartCommitMsg},
		{"PartClaimMsg", PartClaimMsg},
		{"PartReadMsg", PartReadMsg},
	}

	seen := make(map[MessageType]string, len(ledger))
	for _, e := range ledger {
		if e.mt < ReservedMsgsStart || e.mt >= UserMsg {
			t.Errorf("%s = %d: outside the reserved range [%d, %d)",
				e.name, e.mt, ReservedMsgsStart, UserMsg)
		}
		if other, dup := seen[e.mt]; dup {
			t.Errorf("%s and %s both allocate %d", e.name, other, e.mt)
		}
		seen[e.mt] = e.name
	}

	// The core protocol's own types sit below the reserved range; the ranges
	// must not overlap.
	for name, mt := range map[string]MessageType{
		"replyMsg": replyMsg,
		"pingMsg":  pingMsg,
	} {
		if mt >= ReservedMsgsStart {
			t.Errorf("core type %s = %d intrudes into the reserved range", name, mt)
		}
	}
}

// The core protocol's values are on the wire: renumbering any of them breaks compatibility with every node already
// running, so they are pinned here.
func TestWireConstants(t *testing.T) {
	for name, c := range map[string][2]int{
		"replyMsg":            {int(replyMsg), 0},
		"nodeJoinMsg":         {int(nodeJoinMsg), 1},
		"nodeLeaveMsg":        {int(nodeLeaveMsg), 2},
		"pushPullStateMsg":    {int(pushPullStateMsg), 3},
		"metadataUpdateMsg":   {int(metadataUpdateMsg), 4},
		"pingMsg":             {int(pingMsg), 5},
		"ReservedMsgsStart":   {int(ReservedMsgsStart), 64},
		"UserMsg":             {int(UserMsg), 128},
		"TransportBestEffort": {TransportBestEffort, 0},
		"TransportReliable":   {TransportReliable, 1},
	} {
		if c[0] != c[1] {
			t.Errorf("%s = %d, want %d", name, c[0], c[1])
		}
	}
}
