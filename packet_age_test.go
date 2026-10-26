package gossip

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/require"
)

const ageTestMsg MessageType = UserMsg + 61

// ageHistoryMaxAge is how long the ordinary message history keeps a message in these tests.
const ageHistoryMaxAge = 50 * time.Millisecond

// ageCluster is a cluster on a mock transport counting the test messages it accepts.
func ageCluster(t *testing.T, maxAge time.Duration) (*Cluster, *atomic.Int64) {
	t.Helper()
	c := newUnitCluster(t, func(cfg *Config) {
		cfg.MaxPacketAge = maxAge
		cfg.MsgHistoryMaxAge = ageHistoryMaxAge
		cfg.MsgHistoryGCInterval = ageHistoryMaxAge / 5
	})
	var n atomic.Int64
	require.NoError(t, c.HandleFunc(ageTestMsg, func(*Node, *Packet) error { n.Add(1); return nil }))
	return c, &n
}

// ageSender adds a known node to c and returns its id: packets from unknown nodes are dropped anyway.
func ageSender(c *Cluster) NodeID {
	id := NodeID(uuid.New())
	c.nodes.addOrUpdate(newNode(id, "127.0.0.1:1"))
	return id
}

func agePacket(c *Cluster, sender NodeID, id MessageID) *Packet {
	p := NewPacket()
	p.MessageType = ageTestMsg
	p.SenderID = sender
	p.MessageID = id
	p.TTL = 1
	p.SetCodec(c.config.MsgCodec)
	return p
}

// With MaxPacketAge, packets from too far in the past or future are refused; a packet replayed after it left the
// ordinary history is still caught, because the history keeps it until it would be refused by age.
func TestMaxPacketAge(t *testing.T) {
	const maxAge = 500 * time.Millisecond
	c, accepted := ageCluster(t, maxAge)
	sender := ageSender(c)
	now := time.Now()
	c.handleIncomingPacket(agePacket(c, sender, MessageID(hlc.FromTime(now.Add(-10*maxAge)))))
	c.handleIncomingPacket(agePacket(c, sender, MessageID(hlc.FromTime(now.Add(10*maxAge)))))
	require.Zero(t, accepted.Load(), "a packet outside MaxPacketAge was accepted")

	id := MessageID(hlc.Now())
	c.handleIncomingPacket(agePacket(c, sender, id))
	require.EqualValues(t, 1, accepted.Load())
	time.Sleep(3 * ageHistoryMaxAge) // past MsgHistoryMaxAge: the ordinary history would have let it go
	c.handleIncomingPacket(agePacket(c, sender, id))
	require.EqualValues(t, 1, accepted.Load(), "a replay inside MaxPacketAge was accepted")
	require.True(t, c.msgHistory.contains(sender, id))

	// once the message is older than MaxPacketAge it leaves the history, and a replay is refused by age instead
	require.Eventually(t, func() bool { return !c.msgHistory.contains(sender, id) }, 5*time.Second, 10*time.Millisecond)
	c.handleIncomingPacket(agePacket(c, sender, id))
	require.EqualValues(t, 1, accepted.Load(), "a replay older than MaxPacketAge was accepted")
}

// Without MaxPacketAge nothing changes: old packets are accepted and the history keeps MsgHistoryMaxAge only.
func TestMaxPacketAgeOff(t *testing.T) {
	c, accepted := ageCluster(t, 0)
	sender := ageSender(c)
	id := MessageID(hlc.FromTime(time.Now().Add(-time.Hour)))
	c.handleIncomingPacket(agePacket(c, sender, id))
	require.Eventually(t, func() bool { return accepted.Load() == 1 }, time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return !c.msgHistory.contains(sender, id) }, time.Second, 20*time.Millisecond)
}

func TestWithinAge(t *testing.T) {
	now := time.Now()
	for d, ok := range map[time.Duration]bool{0: true, time.Second: true, -time.Second: true,
		59 * time.Second: true, -59 * time.Second: true, 61 * time.Second: false, -61 * time.Second: false} {
		if withinAge(MessageID(hlc.FromTime(now.Add(d))), time.Minute, now) != ok {
			t.Errorf("%v from now: want %v", d, ok)
		}
	}
}
