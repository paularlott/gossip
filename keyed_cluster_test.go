package gossip

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/codec/hashicorp"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/codec/vmihailenco"
	"github.com/paularlott/gossip/encryption"
	"github.com/paularlott/gossip/encryption/aes"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
	"github.com/stretchr/testify/require"
)

const keyedTestMsg MessageType = UserMsg + 60

// keyedNode is a live node on its own port recording, per sender, how the test messages it received were encrypted.
type keyedNode struct {
	*Cluster
	mu    sync.Mutex
	got   map[NodeID][]bool // sender -> keyed flags of the test messages received
	count atomic.Int64
}

func (n *keyedNode) received(from *Cluster) []bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]bool(nil), n.got[from.LocalNode().ID]...)
}

// startKeyedNode starts a node encrypted with cipher and keys; cipher legacyCipher stands in for an older version.
func startKeyedNode(t *testing.T, cipher encryption.Cipher, current []byte, previous ...[]byte) *keyedNode {
	t.Helper()
	addr := getFreeTCPAddress(t)
	cfg := DefaultConfig()
	cfg.BindAddr, cfg.AdvertiseAddr = addr, addr
	cfg.Transport = NewSocketTransport(cfg)
	cfg.MsgCodec = shamaton.New()
	cfg.Logger = logger.NewNullLogger()
	cfg.Cipher = cipher
	cfg.EncryptionKey = current
	cfg.PreviousEncryptionKeys = previous
	c, err := NewCluster(cfg)
	require.NoError(t, err)
	n := &keyedNode{Cluster: c, got: map[NodeID][]bool{}}
	require.NoError(t, c.HandleFunc(keyedTestMsg, func(sender *Node, p *Packet) error {
		n.mu.Lock()
		n.got[p.SenderID] = append(n.got[p.SenderID], p.keyed)
		n.mu.Unlock()
		n.count.Add(1)
		return nil
	}))
	require.NoError(t, c.HandleStreamFunc(testStreamMsg, func(sender *Node, p *Packet, w io.Writer) error {
		_, err := w.Write([]byte("stream reply"))
		return err
	}))
	c.Start()
	t.Cleanup(c.Stop)
	return n
}

// meshed joins every node to the first and waits until each knows every other, with its key window.
func meshed(t *testing.T, nodes ...*keyedNode) {
	t.Helper()
	for _, n := range nodes[1:] {
		require.NoError(t, n.Join([]string{nodes[0].LocalNode().AdvertisedAddr()}))
	}
	require.Eventually(t, func() bool {
		for _, a := range nodes {
			for _, b := range nodes {
				if a == b {
					continue
				}
				nb := a.GetNode(b.LocalNode().ID)
				if nb == nil || nb.keyWindow.Load() != b.keyWindow() {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond, "nodes did not discover each other and their key windows")
}

// deliver sends a direct message from a to b, best effort and reliably, and waits for both to arrive.
func deliver(t *testing.T, a, b *keyedNode) []bool {
	t.Helper()
	before := len(b.received(a.Cluster))
	nb := a.GetNode(b.LocalNode().ID)
	require.NotNil(t, nb)
	var last time.Time
	require.Eventually(t, func() bool {
		if time.Since(last) > time.Second { // best effort may be lost: send again now and then
			a.SendTo(nb, keyedTestMsg, "best effort")
			a.SendToReliable(nb, keyedTestMsg, "reliable")
			last = time.Now()
		}
		return len(b.received(a.Cluster)) >= before+2
	}, 10*time.Second, 5*time.Millisecond, "message not delivered")
	return b.received(a.Cluster)[before:]
}

func allKeyed(flags []bool, want bool) bool {
	for _, f := range flags {
		if f != want {
			return false
		}
	}
	return len(flags) > 0
}

// streamFrom opens a stream from a to b and returns the reply.
func streamFrom(t *testing.T, a, b *keyedNode) string {
	t.Helper()
	r, err := a.OpenStream(context.Background(), a.GetNode(b.LocalNode().ID), testStreamMsg, streamReq{})
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(data)
}

// Nodes of this version find each other's key window and talk in the keyed format: messages both ways, best effort
// and reliable, replies and streams.
func TestKeyedFormatBetweenCurrentNodes(t *testing.T) {
	key := testKey(1, 32)
	a, b := startKeyedNode(t, aes.New(), key), startKeyedNode(t, aes.New(), key)
	meshed(t, a, b)
	require.NotZero(t, a.GetNode(b.LocalNode().ID).caps.Load()&capKeepsConnections, "connection reuse not advertised")
	require.True(t, allKeyed(deliver(t, a, b), true), "a -> b not keyed")
	require.True(t, allKeyed(deliver(t, b, a), true), "b -> a not keyed")
	require.Equal(t, "stream reply", streamFrom(t, a, b))
	var pong pongMessage // a request with a reply, over the keyed format
	require.NoError(t, a.sendToWithResponse(a.GetNode(b.LocalNode().ID), pingMsg,
		&pingMessage{SenderID: a.LocalNode().ID, AdvertiseAddr: a.LocalNode().AdvertisedAddr(), KeyWindow: a.keyWindow()}, &pong))
	require.Equal(t, b.LocalNode().ID, pong.NodeID)
}

// A cluster mixing this version with an older one keeps working through an upgrade: the older node is only ever
// sent the original format, and the current nodes keep the keyed format between themselves.
func TestMixedVersionCluster(t *testing.T) {
	key := testKey(2, 32)
	a, b := startKeyedNode(t, aes.New(), key), startKeyedNode(t, aes.New(), key)
	old := startKeyedNode(t, legacyCipher{aes.New()}, key)
	require.Zero(t, old.keyWindow())
	meshed(t, a, old, b) // the older node is the one the others join through

	require.True(t, allKeyed(deliver(t, a, b), true), "current nodes stopped using the keyed format")
	require.True(t, allKeyed(deliver(t, a, old), false), "an older node was sent the keyed format")
	require.True(t, allKeyed(deliver(t, old, a), false))
	require.True(t, allKeyed(deliver(t, b, old), false))
	require.Equal(t, "stream reply", streamFrom(t, a, old))
	require.Equal(t, "stream reply", streamFrom(t, old, b))

	// a broadcast from any node reaches every other, through relays of either version
	for _, from := range []*keyedNode{a, b, old} {
		counts := map[*keyedNode]int64{a: a.count.Load(), b: b.count.Load(), old: old.count.Load()}
		require.NoError(t, from.Send(keyedTestMsg, "broadcast"))
		require.Eventually(t, func() bool {
			for n, c := range counts {
				if n != from && n.count.Load() <= c {
					return false
				}
			}
			return true
		}, 10*time.Second, 50*time.Millisecond, "broadcast not delivered everywhere")
	}
}

// A node that goes back to an older version (advertising no key window) is sent the original format again.
func TestDowngradedNodeGetsOriginalFormat(t *testing.T) {
	key := testKey(3, 32)
	a, b := startKeyedNode(t, aes.New(), key), startKeyedNode(t, aes.New(), key)
	meshed(t, a, b)
	nb := a.GetNode(b.LocalNode().ID)
	require.True(t, a.transport.(*SocketTransport).keyedFor(nb))
	// b's next ping, as an older version sends it: no key window
	p, err := a.createPacketWithTarget(b.LocalNode().ID, nil, pingMsg, 1, &pingMessage{SenderID: b.LocalNode().ID, AdvertiseAddr: b.LocalNode().AdvertisedAddr()})
	require.NoError(t, err)
	_, err = a.handlePing(nb, p)
	p.Release()
	require.NoError(t, err)
	require.Zero(t, nb.keyWindow.Load())
	require.False(t, a.transport.(*SocketTransport).keyedFor(nb))
	require.True(t, allKeyed(deliver(t, a, b), false))
}

// The key of a running cluster is rotated in three steps, with traffic flowing throughout; a node that only has the
// old key afterwards is shut out.
func TestLiveKeyRotation(t *testing.T) {
	oldKey, newKey := testKey(4, 32), testKey(5, 32)
	nodes := []*keyedNode{startKeyedNode(t, aes.New(), oldKey), startKeyedNode(t, aes.New(), oldKey), startKeyedNode(t, aes.New(), oldKey)}
	meshed(t, nodes...)
	allTalk := func(step string) {
		t.Helper()
		for _, from := range nodes {
			for _, to := range nodes {
				if from != to {
					require.True(t, allKeyed(deliver(t, from, to), true), "%s: %s", step, "not keyed")
				}
			}
		}
	}
	allTalk("before")
	for _, n := range nodes { // step 1: the new key decrypts everywhere
		require.NoError(t, n.SetEncryptionKeys(oldKey, newKey))
	}
	allTalk("step 1")
	for i, n := range nodes { // step 2, node by node: the new key encrypts
		require.NoError(t, n.SetEncryptionKeys(newKey, oldKey))
		allTalk("step 2 after node " + string(rune('A'+i)))
	}
	for _, n := range nodes { // step 3: the old key goes
		require.NoError(t, n.SetEncryptionKeys(newKey))
	}
	allTalk("step 3")
	require.Equal(t, "stream reply", streamFrom(t, nodes[0], nodes[2]))

	// repeated rotations replace the keys rather than add to them
	current := newKey
	for i := byte(0); i < 20; i++ {
		next := testKey(100+i, 32)
		for _, n := range nodes {
			require.NoError(t, n.SetEncryptionKeys(current, next))
		}
		for _, n := range nodes {
			require.NoError(t, n.SetEncryptionKeys(next, current))
		}
		for _, n := range nodes {
			require.NoError(t, n.SetEncryptionKeys(next))
		}
		current = next
	}
	allTalk("after repeated rotations")
	for _, n := range nodes {
		k := n.socketTransport().keyring()
		require.Len(t, k.keys, 1)
		require.Equal(t, current, k.keys[0])
		k.mu.RLock()
		require.LessOrEqual(t, len(k.cache), 5)
		k.mu.RUnlock()
	}

	// a node with only the old key cannot take part (Join itself retries quietly in the background)
	stale := startKeyedNode(t, aes.New(), oldKey)
	stale.Join([]string{nodes[0].LocalNode().AdvertisedAddr()})
	require.Never(t, func() bool {
		return stale.GetNode(nodes[0].LocalNode().ID) != nil || nodes[0].GetNode(stale.LocalNode().ID) != nil
	}, 3*time.Second, 100*time.Millisecond, "a node with only the old key joined")
}

func TestSetEncryptionKeysValidation(t *testing.T) {
	n := startKeyedNode(t, aes.New(), testKey(1, 32))
	require.Error(t, n.SetEncryptionKeys(testKey(1, 5)))
	require.Error(t, n.SetEncryptionKeys(testKey(1, 32), testKey(2, 3)))

	cfg := DefaultConfig()
	cfg.BindAddr = getFreeTCPAddress(t)
	cfg.Transport = NewSocketTransport(cfg)
	cfg.Logger = logger.NewNullLogger()
	plain, err := NewCluster(cfg)
	require.NoError(t, err)
	require.Error(t, plain.SetEncryptionKeys(testKey(1, 32)), "turning encryption on in a running, unencrypted cluster")

	cfg2 := DefaultConfig()
	cfg2.BindAddr = getFreeTCPAddress(t)
	cfg2.Transport = NewSocketTransport(cfg2)
	cfg2.Logger = logger.NewNullLogger()
	cfg2.Cipher, cfg2.EncryptionKey, cfg2.PreviousEncryptionKeys = aes.New(), testKey(1, 32), [][]byte{testKey(2, 9)}
	_, err = NewCluster(cfg2)
	require.Error(t, err, "invalid previous key accepted at start")
}

// Every codec reads the new optional key-window field as absent in an older node's structs (they ignore it), and
// reads an older node's messages (without it) as zero.
func TestKeyWindowFieldCompatibility(t *testing.T) {
	type oldPing struct {
		SenderID      NodeID `msgpack:"sid" json:"sid"`
		AdvertiseAddr string `msgpack:"addr" json:"addr"`
	}
	type oldMetadataUpdate struct {
		MetadataTimestamp hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
		Metadata          map[string]interface{} `msgpack:"md" json:"md"`
		NodeState         NodeState              `msgpack:"state" json:"state"`
	}
	for name, c := range map[string]codec.Serializer{"shamaton": shamaton.New(), "vmihailenco": vmihailenco.New(),
		"hashicorp": hashicorp.New(), "json": codec.NewJSONCodec()} {
		id := NodeID(uuid.New())
		b, err := c.Marshal(&pingMessage{SenderID: id, AdvertiseAddr: "a:1", KeyWindow: 3600})
		require.NoError(t, err, name)
		var o oldPing
		require.NoError(t, c.Unmarshal(b, &o), name)
		require.Equal(t, "a:1", o.AdvertiseAddr, name)

		b, err = c.Marshal(&oldPing{SenderID: id, AdvertiseAddr: "a:1"})
		require.NoError(t, err, name)
		var n pingMessage
		require.NoError(t, c.Unmarshal(b, &n), name)
		require.Zero(t, n.KeyWindow, name)

		b, err = c.Marshal(&metadataUpdateMessage{NodeState: NodeAlive, Metadata: map[string]interface{}{"k": "v"}, KeyWindow: 60})
		require.NoError(t, err, name)
		var om oldMetadataUpdate
		require.NoError(t, c.Unmarshal(b, &om), name)
		require.Equal(t, "v", om.Metadata["k"], name)
	}
	// without a key window the field is left out entirely: an older node's messages are unchanged on the wire
	b, _ := shamaton.New().Marshal(&pingMessage{AdvertiseAddr: "a:1"})
	o, _ := shamaton.New().Marshal(&struct {
		SenderID      NodeID `msgpack:"sid"`
		AdvertiseAddr string `msgpack:"addr"`
	}{AdvertiseAddr: "a:1"})
	require.Equal(t, o, b)
}

// Random input never panics the decoder and is never accepted.
func TestKeyringOpenRejectsGarbage(t *testing.T) {
	k := mustKeyring(t, aes.New(), testKey(1, 32))
	for n := range 200 {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i*31 + n)
		}
		if _, _, err := k.open(nil, data); err == nil {
			t.Fatalf("garbage of %d bytes accepted", n)
		}
	}
}
