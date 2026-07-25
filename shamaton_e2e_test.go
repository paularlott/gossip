package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/compression/snappy"
	"github.com/paularlott/gossip/encryption/aes"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// richPayload exercises the field shapes that gossip serialises: msgpack-tagged
// structs, time.Time, pointers, slices, maps, and the same hlc.Timestamp /
// NodeID aliases used on the wire.
type richPayload struct {
	Name      string            `msgpack:"name" json:"name"`
	Port      uint16            `msgpack:"port" json:"port"`
	Tags      []string          `msgpack:"tags,omitempty" json:"tags,omitempty"`
	Meta      map[string]string `msgpack:"meta" json:"meta"`
	Timestamp hlc.Timestamp     `msgpack:"ts" json:"ts"`
	Opt       *string           `msgpack:"opt,omitempty" json:"opt,omitempty"`
}

// mkShamatonCluster creates a live cluster on a random port using the shamaton codec.
func mkShamatonCluster(t *testing.T, addr string, tags []string) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.BindAddr = addr
	cfg.AdvertiseAddr = addr
	if tags != nil {
		cfg.Tags = tags
	}
	cfg.Transport = NewSocketTransport(cfg)
	cfg.MsgCodec = shamaton.New()
	cfg.Logger = logger.NewNullLogger()
	c, err := NewCluster(cfg)
	require.NoError(t, err)
	c.Start()
	return c
}

// TestShamatonE2E_ClusterFormation verifies a two-node live cluster using the
// shamaton codec: join, mutual discovery, tag propagation.
func TestShamatonE2E_ClusterFormation(t *testing.T) {
	addr1 := getFreeTCPAddress(t)
	addr2 := getFreeTCPAddress(t)

	c1 := mkShamatonCluster(t, addr1, []string{"zone-a", "storage"})
	defer c1.Stop()
	c2 := mkShamatonCluster(t, addr2, []string{"zone-b", "compute"})
	defer c2.Stop()

	require.NoError(t, c2.Join([]string{addr1}))

	require.Eventually(t, func() bool {
		return c1.GetNode(c2.LocalNode().ID) != nil &&
			c2.GetNode(c1.LocalNode().ID) != nil
	}, 5*time.Second, 100*time.Millisecond, "nodes should discover each other")

	n2c1 := c1.GetNode(c2.LocalNode().ID)
	assert.Contains(t, n2c1.GetTags(), "zone-b")
	assert.Contains(t, n2c1.GetTags(), "compute")

	n1c2 := c2.GetNode(c1.LocalNode().ID)
	assert.Contains(t, n1c2.GetTags(), "zone-a")
	assert.Contains(t, n1c2.GetTags(), "storage")
}

// TestShamatonE2E_UserMessages verifies that a rich user-defined struct with
// msgpack tags round-trips correctly through the shamaton codec over the live
// socket transport (both reliable and best-effort).
func TestShamatonE2E_UserMessages(t *testing.T) {
	addr1 := getFreeTCPAddress(t)
	addr2 := getFreeTCPAddress(t)

	c1 := mkShamatonCluster(t, addr1, []string{"a"})
	defer c1.Stop()
	c2 := mkShamatonCluster(t, addr2, []string{"b"})
	defer c2.Stop()

	require.NoError(t, c2.Join([]string{addr1}))
	require.Eventually(t, func() bool {
		return c1.GetNode(c2.LocalNode().ID) != nil
	}, 5*time.Second, 100*time.Millisecond)

	const echoMsgType MessageType = 200

	received := make(chan richPayload, 10)
	optVal := "optional-field"
	require.NoError(t, c1.HandleFunc(echoMsgType, func(node *Node, packet *Packet) error {
		var rp richPayload
		if err := packet.Unmarshal(&rp); err != nil {
			return err
		}
		received <- rp
		return nil
	}))

	ts := hlc.Now()
	original := richPayload{
		Name:      "test-message",
		Port:      8080,
		Tags:      []string{"alpha", "beta"},
		Meta:      map[string]string{"region": "us-east", "role": "worker"},
		Timestamp: ts,
		Opt:       &optVal,
	}

	node1 := c2.GetNode(c1.LocalNode().ID)
	require.NotNil(t, node1)

	require.NoError(t, c2.SendToReliable(node1, echoMsgType, original))

	select {
	case got := <-received:
		assert.Equal(t, original.Name, got.Name)
		assert.Equal(t, original.Port, got.Port)
		assert.Equal(t, original.Tags, got.Tags)
		assert.Equal(t, original.Meta, got.Meta)
		assert.Equal(t, original.Timestamp, got.Timestamp)
		require.NotNil(t, got.Opt)
		assert.Equal(t, *original.Opt, *got.Opt)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for reliable user message")
	}

	require.NoError(t, c2.SendTo(node1, echoMsgType, original))

	select {
	case got := <-received:
		assert.Equal(t, original.Name, got.Name)
		assert.Equal(t, uint16(8080), got.Port)
		assert.Equal(t, original.Timestamp, got.Timestamp)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for best-effort user message")
	}
}

// TestShamatonE2E_MetadataPropagation verifies that node metadata (serialised
// as map[string]interface{} through the codec) propagates correctly with
// mixed-type values.
func TestShamatonE2E_MetadataPropagation(t *testing.T) {
	addr1 := getFreeTCPAddress(t)
	addr2 := getFreeTCPAddress(t)

	c1 := mkShamatonCluster(t, addr1, nil)
	defer c1.Stop()
	c2 := mkShamatonCluster(t, addr2, nil)
	defer c2.Stop()

	require.NoError(t, c2.Join([]string{addr1}))
	require.Eventually(t, func() bool {
		return c1.GetNode(c2.LocalNode().ID) != nil
	}, 5*time.Second, 100*time.Millisecond)

	c2.LocalMetadata().SetString("zone", "us-west-2")
	c2.LocalMetadata().SetString("version", "1.2.3")
	c2.LocalMetadata().SetInt("priority", 42)

	require.Eventually(t, func() bool {
		n := c1.GetNode(c2.LocalNode().ID)
		if n == nil {
			return false
		}
		return n.Metadata.GetString("zone") == "us-west-2" &&
			n.Metadata.GetString("version") == "1.2.3" &&
			n.Metadata.GetInt("priority") == 42
	}, 5*time.Second, 100*time.Millisecond, "all metadata should propagate to peer")

	n := c1.GetNode(c2.LocalNode().ID)
	assert.Equal(t, "us-west-2", n.Metadata.GetString("zone"))
	assert.Equal(t, "1.2.3", n.Metadata.GetString("version"))
	assert.Equal(t, 42, n.Metadata.GetInt("priority"))
}

// TestShamatonE2E_CompressionAndEncryption verifies that shamaton works
// end-to-end when compression and encryption are both enabled.
func TestShamatonE2E_CompressionAndEncryption(t *testing.T) {
	addr1 := getFreeTCPAddress(t)
	addr2 := getFreeTCPAddress(t)

	key := []byte("0123456789abcdef0123456789abcdef") // 32 bytes = AES-256

	mkSecCluster := func(addr string) *Cluster {
		t.Helper()
		cfg := DefaultConfig()
		cfg.BindAddr = addr
		cfg.AdvertiseAddr = addr
		cfg.Transport = NewSocketTransport(cfg)
		cfg.MsgCodec = shamaton.New()
		cfg.Compressor = snappy.New()
		cfg.CompressMinSize = 0
		cfg.Cipher = aes.New()
		cfg.EncryptionKey = key
		cfg.Logger = logger.NewNullLogger()
		c, err := NewCluster(cfg)
		require.NoError(t, err)
		c.Start()
		return c
	}

	c1 := mkSecCluster(addr1)
	defer c1.Stop()
	c2 := mkSecCluster(addr2)
	defer c2.Stop()

	require.NoError(t, c2.Join([]string{addr1}))
	require.Eventually(t, func() bool {
		return c1.GetNode(c2.LocalNode().ID) != nil &&
			c2.GetNode(c1.LocalNode().ID) != nil
	}, 5*time.Second, 100*time.Millisecond)

	const dataMsgType MessageType = 201

	received := make(chan string, 5)
	require.NoError(t, c1.HandleFunc(dataMsgType, func(node *Node, packet *Packet) error {
		var msg string
		if err := packet.Unmarshal(&msg); err != nil {
			return err
		}
		received <- msg
		return nil
	}))

	node1 := c2.GetNode(c1.LocalNode().ID)
	require.NotNil(t, node1)
	require.NoError(t, c2.SendToReliable(node1, dataMsgType, "encrypted-compressed-hello"))

	select {
	case got := <-received:
		assert.Equal(t, "encrypted-compressed-hello", got)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for encrypted+compressed message")
	}
}

// TestShamatonE2E_ReplyHandler verifies the request/reply pattern works with
// shamaton: a node sends a message and receives a typed reply.
func TestShamatonE2E_ReplyHandler(t *testing.T) {
	addr1 := getFreeTCPAddress(t)
	addr2 := getFreeTCPAddress(t)

	c1 := mkShamatonCluster(t, addr1, nil)
	defer c1.Stop()
	c2 := mkShamatonCluster(t, addr2, nil)
	defer c2.Stop()

	require.NoError(t, c2.Join([]string{addr1}))
	require.Eventually(t, func() bool {
		return c1.GetNode(c2.LocalNode().ID) != nil
	}, 5*time.Second, 100*time.Millisecond)

	const queryMsgType MessageType = 202

	type query struct {
		Key string `msgpack:"key" json:"key"`
	}
	type reply struct {
		Value string `msgpack:"val" json:"val"`
	}

	require.NoError(t, c1.HandleFuncWithResponse(queryMsgType, func(node *Node, packet *Packet) (interface{}, error) {
		var q query
		if err := packet.Unmarshal(&q); err != nil {
			return nil, err
		}
		return reply{Value: "echo:" + q.Key}, nil
	}))

	node1 := c2.GetNode(c1.LocalNode().ID)
	require.NotNil(t, node1)

	var resp reply
	require.NoError(t, c2.SendToWithResponse(node1, queryMsgType, query{Key: "hello"}, &resp))
	assert.Equal(t, "echo:hello", resp.Value)
}

// TestShamatonE2E_PacketHeaderRoundTrip verifies that the Packet header itself
// (the outermost serialised structure) round-trips correctly with shamaton,
// including NodeID (uuid), MessageID (hlc.Timestamp), and optional pointer fields.
func TestShamatonE2E_PacketHeaderRoundTrip(t *testing.T) {
	sr := shamaton.New()

	tag := "routing-tag"
	target := NodeID(uuid.New())
	p := Packet{
		MessageType:  UserMsg,
		SenderID:     NodeID(uuid.New()),
		TargetNodeID: &target,
		Tag:          &tag,
		MessageID:    MessageID(hlc.Now()),
		TTL:          5,
	}
	p.SetCodec(sr)
	p.SetPayload([]byte("payload"))

	data, err := sr.Marshal(&p)
	require.NoError(t, err)

	var decoded Packet
	require.NoError(t, sr.Unmarshal(data, &decoded))

	assert.Equal(t, p.MessageType, decoded.MessageType)
	assert.Equal(t, p.SenderID, decoded.SenderID)
	require.NotNil(t, decoded.TargetNodeID)
	assert.Equal(t, target, *decoded.TargetNodeID)
	require.NotNil(t, decoded.Tag)
	assert.Equal(t, tag, *decoded.Tag)
	assert.Equal(t, p.MessageID, decoded.MessageID)
	assert.Equal(t, p.TTL, decoded.TTL)
}

// TestShamatonE2E_LargePayload verifies that a large payload that forces TCP
// (reliable transport) works correctly with shamaton.
func TestShamatonE2E_LargePayload(t *testing.T) {
	addr1 := getFreeTCPAddress(t)
	addr2 := getFreeTCPAddress(t)

	c1 := mkShamatonCluster(t, addr1, nil)
	defer c1.Stop()
	c2 := mkShamatonCluster(t, addr2, nil)
	defer c2.Stop()

	require.NoError(t, c2.Join([]string{addr1}))
	require.Eventually(t, func() bool {
		return c1.GetNode(c2.LocalNode().ID) != nil
	}, 5*time.Second, 100*time.Millisecond)

	const bigMsgType MessageType = 203

	received := make(chan []string, 5)
	require.NoError(t, c1.HandleFunc(bigMsgType, func(node *Node, packet *Packet) error {
		var items []string
		if err := packet.Unmarshal(&items); err != nil {
			return err
		}
		received <- items
		return nil
	}))

	node1 := c2.GetNode(c1.LocalNode().ID)
	require.NotNil(t, node1)

	items := make([]string, 5000)
	for i := range items {
		items[i] = "item-" + uuid.New().String()
	}

	require.NoError(t, c2.SendToReliable(node1, bigMsgType, items))

	select {
	case got := <-received:
		assert.Len(t, got, 5000)
		assert.Equal(t, items[0], got[0])
		assert.Equal(t, items[4999], got[4999])
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for large payload")
	}
}

// TestShamatonE2E_Broadcast verifies broadcast message delivery works with
// shamaton in a three-node cluster.
func TestShamatonE2E_Broadcast(t *testing.T) {
	addr1 := getFreeTCPAddress(t)
	addr2 := getFreeTCPAddress(t)
	addr3 := getFreeTCPAddress(t)

	c1 := mkShamatonCluster(t, addr1, nil)
	defer c1.Stop()
	c2 := mkShamatonCluster(t, addr2, nil)
	defer c2.Stop()
	c3 := mkShamatonCluster(t, addr3, nil)
	defer c3.Stop()

	require.NoError(t, c2.Join([]string{addr1}))
	require.NoError(t, c3.Join([]string{addr1}))

	require.Eventually(t, func() bool {
		return c1.NumNodes() >= 3 && c2.NumNodes() >= 3 && c3.NumNodes() >= 3
	}, 10*time.Second, 200*time.Millisecond, "all nodes should see each other")

	const bcastMsgType MessageType = 204

	var mu sync.Mutex
	received := make(map[string]bool)

	registerHandler := func(c *Cluster) {
		require.NoError(t, c.HandleFunc(bcastMsgType, func(node *Node, packet *Packet) error {
			var msg string
			if err := packet.Unmarshal(&msg); err != nil {
				return err
			}
			mu.Lock()
			received[msg] = true
			mu.Unlock()
			return nil
		}))
	}
	registerHandler(c1)
	registerHandler(c2)
	registerHandler(c3)

	require.NoError(t, c1.Send(bcastMsgType, "broadcast-test"))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return received["broadcast-test"]
	}, 5*time.Second, 100*time.Millisecond, "broadcast should be received")
}
