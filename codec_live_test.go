package gossip

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/compression/snappy"
	"github.com/paularlott/gossip/encryption/aes"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// richPayload exercises the field shapes that gossip serialises: tagged structs, pointers, slices, maps, and the same
// hlc.Timestamp / NodeID aliases used on the wire.
type richPayload struct {
	Name      string            `msgpack:"name" json:"name"`
	Port      uint16            `msgpack:"port" json:"port"`
	Tags      []string          `msgpack:"tags,omitempty" json:"tags,omitempty"`
	Meta      map[string]string `msgpack:"meta" json:"meta"`
	Timestamp hlc.Timestamp     `msgpack:"ts" json:"ts"`
	Opt       *string           `msgpack:"opt,omitempty" json:"opt,omitempty"`
}

// A live pair of nodes per codec (and one with compression and encryption on): joining, tags, metadata of mixed types,
// user messages both ways of sending, request and reply, and a payload too big for UDP.
func TestCodecLive(t *testing.T) {
	setups := []struct {
		name string
		opts func(*Config)
	}{
		{"json", func(c *Config) { c.MsgCodec = codec.NewJSONCodec() }},
		{"shamaton", func(c *Config) { c.MsgCodec = shamaton.New() }},
		{"shamaton compressed and encrypted", func(c *Config) {
			c.MsgCodec = shamaton.New()
			c.Compressor, c.CompressMinSize = snappy.New(), 0
			c.Cipher, c.EncryptionKey = aes.New(), []byte("0123456789abcdef0123456789abcdef")
		}},
	}
	for _, setup := range setups {
		t.Run(setup.name, func(t *testing.T) {
			a := startLiveCluster(t, setup.opts, func(c *Config) { c.Tags = []string{"zone-a", "storage"} })
			b := startLiveCluster(t, setup.opts, func(c *Config) { c.Tags = []string{"zone-b", "compute"} })
			joinAndWait(t, a, b)
			aOnB, bOnA := b.GetNode(a.LocalNode().ID), a.GetNode(b.LocalNode().ID)

			t.Run("tags", func(t *testing.T) {
				assert.ElementsMatch(t, []string{"zone-b", "compute"}, bOnA.GetTags())
				assert.ElementsMatch(t, []string{"zone-a", "storage"}, aOnB.GetTags())
			})

			t.Run("metadata", func(t *testing.T) {
				b.LocalMetadata().SetString("zone", "us-west-2").SetString("version", "1.2.3").SetInt("priority", 42).SetBool("ok", true)
				require.Eventually(t, func() bool {
					return bOnA.Metadata.GetString("zone") == "us-west-2" && bOnA.Metadata.GetString("version") == "1.2.3" &&
						bOnA.Metadata.GetInt("priority") == 42 && bOnA.Metadata.GetBool("ok") // each set is its own update
				}, 5*time.Second, 10*time.Millisecond)
			})

			t.Run("user messages", func(t *testing.T) {
				const msgType = UserMsg + 70
				received := make(chan richPayload, 4)
				require.NoError(t, a.HandleFunc(msgType, func(_ *Node, p *Packet) error {
					var rp richPayload
					if err := p.Unmarshal(&rp); err != nil {
						return err
					}
					received <- rp
					return nil
				}))
				opt := "optional-field"
				sent := richPayload{Name: "test-message", Port: 8080, Tags: []string{"alpha", "beta"},
					Meta: map[string]string{"region": "us-east"}, Timestamp: hlc.Now(), Opt: &opt}

				for name, send := range map[string]func() error{
					"reliable":    func() error { return b.SendToReliable(aOnB, msgType, sent) },
					"best effort": func() error { return b.SendTo(aOnB, msgType, sent) },
				} {
					require.NoError(t, send(), name)
					select {
					case got := <-received:
						assert.Equal(t, sent, got, name)
					case <-time.After(5 * time.Second):
						t.Fatalf("%s: message not received", name)
					}
				}
			})

			t.Run("request and reply", func(t *testing.T) {
				const msgType = UserMsg + 71
				type query struct {
					Key string `msgpack:"key" json:"key"`
				}
				type reply struct {
					Value string `msgpack:"val" json:"val"`
				}
				require.NoError(t, a.HandleFuncWithResponse(msgType, func(_ *Node, p *Packet) (interface{}, error) {
					var q query
					if err := p.Unmarshal(&q); err != nil {
						return nil, err
					}
					return reply{Value: "echo:" + q.Key}, nil
				}))
				var got reply
				require.NoError(t, b.SendToWithResponse(aOnB, msgType, query{Key: "hello"}, &got))
				assert.Equal(t, "echo:hello", got.Value)
			})

			t.Run("large payload", func(t *testing.T) {
				const msgType = UserMsg + 72
				received := make(chan []string, 1)
				require.NoError(t, a.HandleFunc(msgType, func(_ *Node, p *Packet) error {
					var items []string
					if err := p.Unmarshal(&items); err != nil {
						return err
					}
					received <- items
					return nil
				}))
				items := make([]string, 5000)
				for i := range items {
					items[i] = "item-" + uuid.New().String()
				}
				require.NoError(t, b.SendToReliable(aOnB, msgType, items))
				select {
				case got := <-received:
					assert.Equal(t, items, got)
				case <-time.After(10 * time.Second):
					t.Fatal("large payload not received")
				}
			})
		})
	}
}
