package gossip

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip/hlc"
)

func TestDataNodeGroup_AddRemoveUpdateAndClose(t *testing.T) {
	c := newTestCluster(t)
	defer c.Stop()

	// Create two nodes and set metadata
	n1 := newNode(NodeID{21}, "")
	n2 := newNode(NodeID{22}, "")
	c.nodes.addOrUpdate(n1)
	c.nodes.addOrUpdate(n2)
	c.nodes.updateState(n1.ID, NodeAlive)
	c.nodes.updateState(n2.ID, NodeAlive)

	n1.metadata.SetString("zone", "eu-1")
	n2.metadata.SetString("zone", "us-1")

	// Track callbacks
	var added, removed, updated atomic.Int32

	opts := &DataNodeGroupOptions[int]{
		OnNodeAdded:     func(node *Node, data *int) { added.Add(1) },
		OnNodeRemoved:   func(node *Node, data *int) { removed.Add(1) },
		OnNodeUpdated:   func(node *Node, data *int) { updated.Add(1) },
		DataInitializer: func(node *Node) *int { v := 0; return &v },
	}

	dng := NewDataNodeGroup[int](c, map[string]string{"zone": "eu-1"}, opts)

	// n1 matches initially, n2 does not
	if dng.Count() != 1 || !dng.Contains(n1.ID) || dng.Contains(n2.ID) {
		t.Fatalf("initial group contents unexpected")
	}
	if added.Load() == 0 {
		t.Fatalf("expected OnNodeAdded to be called for n1")
	}

	// Change n2 to match via remote-driven metadata update (triggers cluster notify)
	rs := []exchangeNodeState{{
		ID:                n2.ID,
		AdvertiseAddr:     n2.advertiseAddr,
		State:             n2.state,
		StateChangeTime:   n2.stateChangeTime,
		MetadataTimestamp: hlc.Now(),
		Metadata:          map[string]interface{}{"zone": "eu-1"},
	}}
	c.healthMonitor.combineRemoteNodeState(nil, rs)
	// Wait briefly for async handler
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if dng.Contains(n2.ID) && dng.Count() == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if dng.Count() != 2 || !dng.Contains(n2.ID) {
		t.Fatalf("expected n2 to be added after metadata change")
	}

	// Update metadata of an existing member (n1) without changing criteria to trigger OnNodeUpdated
	prevUpdated := updated.Load()
	n1.metadata.SetString("foo", "bar")
	// Manually notify since only local node auto-broadcasts metadata changes
	c.notifyMetadataChanged(n1)
	// Wait for async handler
	deadline2 := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline2) {
		if updated.Load() > prevUpdated {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if updated.Load() == prevUpdated {
		t.Fatalf("expected OnNodeUpdated to be called for metadata change")
	}

	// Move n1 to non-matching zone -> removed
	n1.metadata.SetString("zone", "us-2")
	// Manually notify since only local node auto-broadcasts metadata changes
	c.notifyMetadataChanged(n1)
	// Wait for async handler
	deadline3 := time.Now().Add(200 * time.Millisecond)
	removed1 := false
	for time.Now().Before(deadline3) {
		if !dng.Contains(n1.ID) {
			removed1 = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !removed1 {
		t.Fatalf("expected n1 to be removed after metadata no longer matches")
	}

	// Transition n2 to Dead -> removed via state handler
	prevRemoved := removed.Load()
	c.nodes.updateState(n2.ID, NodeDead)
	// Wait for async handler
	deadline4 := time.Now().Add(200 * time.Millisecond)
	removed2 := false
	for time.Now().Before(deadline4) {
		if !dng.Contains(n2.ID) && removed.Load() > prevRemoved {
			removed2 = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !removed2 {
		t.Fatalf("expected n2 to be removed on state change to dead")
	}

	// Close should unregister handlers (no panics, and handlers count reduces)
	prevStateHandlers := len(c.stateEventHandlers.GetHandlers())
	prevMetaHandlers := len(c.metadataChangeEventHandlers.GetHandlers())
	dng.Close()
	if len(c.stateEventHandlers.GetHandlers()) >= prevStateHandlers || len(c.metadataChangeEventHandlers.GetHandlers()) >= prevMetaHandlers {
		t.Fatalf("expected handlers to be unregistered on Close")
	}
}
