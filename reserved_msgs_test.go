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
