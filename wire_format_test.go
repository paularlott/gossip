package gossip

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/compression/snappy"
	"github.com/paularlott/gossip/encryption/aes"
	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wireFormat is a transport's packet encoding, so that both transports run the same cases.
type wireFormat struct {
	encode func(p *Packet, reply bool) ([]byte, error)
	decode func(b []byte) (p *Packet, replyExpected bool, err error)
}

func socketWire(cfg *Config) wireFormat {
	st := NewSocketTransport(cfg)
	return wireFormat{encode: st.packetToBuffer, decode: st.packetFromBuffer}
}

func httpWire(cfg *Config) wireFormat {
	ht := NewHTTPTransport(cfg)
	return wireFormat{
		encode: ht.packetToBuffer,
		decode: func(b []byte) (*Packet, bool, error) {
			p, err := ht.packetFromBuffer(b)
			return p, err == nil && binary.LittleEndian.Uint16(b)&replyExpectedFlag != 0, err
		},
	}
}

// Packets survive the wire encoding of both transports with every codec, with and without the optional header fields,
// the reply flag, compression and (socket only: HTTP leaves encryption to TLS) encryption.
func TestWireFormat(t *testing.T) {
	compressible := bytes.Repeat([]byte("compressible-payload-"), 32)
	secret := []byte("a payload that must not be readable on the wire")
	target, tag := NodeID(uuid.New()), "route-tag"

	variants := []struct {
		name      string
		configure func(*Config)
		payload   []byte
		reply     bool
		optional  bool // target node and tag set
		socket    bool // socket transport only
		compress  bool // expected to be compressed on the wire
		encrypted bool
	}{
		{name: "plain", payload: []byte("plain payload")},
		{name: "reply expected", payload: []byte("flag"), reply: true},
		{name: "optional header fields", payload: []byte("with optionals"), optional: true},
		{name: "empty payload", payload: nil},
		{name: "compressed", configure: func(c *Config) { c.Compressor, c.CompressMinSize = snappy.New(), 32 }, payload: compressible, compress: true},
		{name: "below the compression threshold", configure: func(c *Config) { c.Compressor, c.CompressMinSize = snappy.New(), 1024 }, payload: []byte("tiny")},
		{name: "encrypted", configure: func(c *Config) { c.Cipher, c.EncryptionKey = aes.New(), bytes.Repeat([]byte("k"), 32) },
			payload: secret, socket: true, encrypted: true},
		{name: "compressed, encrypted, reply expected, optionals", configure: func(c *Config) {
			c.Compressor, c.CompressMinSize = snappy.New(), 32
			c.Cipher, c.EncryptionKey = aes.New(), bytes.Repeat([]byte("k"), 32)
		}, payload: compressible, reply: true, optional: true, socket: true, compress: true, encrypted: true},
	}

	for _, cd := range []codec.Serializer{codec.NewJSONCodec(), shamaton.New()} {
		for _, kind := range []string{"socket", "http"} {
			for _, v := range variants {
				if v.socket && kind == "http" {
					continue
				}
				t.Run(cd.Name()+"/"+kind+"/"+v.name, func(t *testing.T) {
					cfg := DefaultConfig()
					cfg.MsgCodec = cd
					if v.configure != nil {
						v.configure(cfg)
					}
					w := socketWire(cfg)
					if kind == "http" {
						w = httpWire(cfg)
					}

					p := NewPacket()
					defer p.Release()
					p.MessageType, p.SenderID, p.MessageID, p.TTL = UserMsg, NodeID(uuid.New()), MessageID(hlc.Now()), 5
					if v.optional {
						p.TargetNodeID, p.Tag = &target, &tag
					}
					p.SetCodec(cd)
					p.SetPayload(v.payload)

					buf, err := w.encode(p, v.reply)
					require.NoError(t, err)

					flags := binary.LittleEndian.Uint16(buf)
					assert.Equal(t, v.reply, flags&replyExpectedFlag != 0, "reply flag")
					assert.Equal(t, v.compress, flags&compressionFlag != 0, "compression flag")
					if v.compress {
						assert.Less(t, len(buf), len(v.payload), "a compressed packet is smaller than its payload")
					}
					if v.encrypted {
						assert.False(t, bytes.Contains(buf, v.payload), "the payload is readable on the wire")
					}

					got, reply, err := w.decode(buf)
					require.NoError(t, err)
					defer got.Release()
					assert.Equal(t, v.reply, reply, "reply expected")
					assert.Equal(t, p.MessageType, got.MessageType)
					assert.Equal(t, p.SenderID, got.SenderID)
					assert.Equal(t, p.MessageID, got.MessageID)
					assert.Equal(t, p.TTL, got.TTL)
					assert.Equal(t, p.TargetNodeID, got.TargetNodeID)
					assert.Equal(t, p.Tag, got.Tag)
					assert.True(t, bytes.Equal(v.payload, got.Payload()), "payload: got %d bytes, want %d", len(got.Payload()), len(v.payload))
				})
			}
		}
	}
}

// Input that is not a packet is refused, never decoded into something.
func TestWireFormatErrors(t *testing.T) {
	encrypted := func(c *Config) { c.Cipher, c.EncryptionKey = aes.New(), bytes.Repeat([]byte("k"), 32) }
	compressed := func(c *Config) { c.Compressor, c.CompressMinSize = snappy.New(), 1 }

	tests := []struct {
		name      string
		configure func(*Config)
		data      []byte
		socket    bool
		http      bool
	}{
		{name: "empty", data: nil, socket: true, http: true},
		{name: "one byte", data: []byte{0x00}, socket: true, http: true},
		{name: "header longer than the data", data: []byte{0xFF, 0x3F, 0x00}, socket: true, http: true},
		{name: "truncated header", data: []byte{0x10, 0x00, 0x81}, socket: true, http: true},
		{name: "garbage where the header should be", data: []byte{0x03, 0x00, '{', 'x', '}'}, socket: true, http: true},
		{name: "garbage that is not encrypted", configure: encrypted, data: append([]byte{0x10, 0x00}, make([]byte, 100)...), socket: true},
		{name: "compressed with no compressor to read it", configure: nil, http: true,
			data: func() []byte {
				cfg := DefaultConfig()
				cfg.MsgCodec = codec.NewJSONCodec()
				compressed(cfg)
				p := NewPacket()
				defer p.Release()
				p.SetCodec(cfg.MsgCodec)
				p.SetPayload(bytes.Repeat([]byte("x"), 200))
				b, err := httpWire(cfg).encode(p, false)
				if err != nil {
					panic(err)
				}
				return b
			}()},
	}
	for _, tt := range tests {
		for kind, run := range map[string]bool{"socket": tt.socket, "http": tt.http} {
			if !run {
				continue
			}
			t.Run(tt.name+"/"+kind, func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.MsgCodec = codec.NewJSONCodec()
				if tt.configure != nil {
					tt.configure(cfg)
				}
				w := socketWire(cfg)
				if kind == "http" {
					w = httpWire(cfg)
				}
				p, _, err := w.decode(tt.data)
				if err == nil {
					p.Release()
				}
				assert.Error(t, err)
			})
		}
	}
}

// Wire-level edge values that the codec must carry for the header: the zero node id and the largest message id.
func TestWireFormatHeaderEdges(t *testing.T) {
	for _, cd := range []codec.Serializer{codec.NewJSONCodec(), shamaton.New()} {
		t.Run(cd.Name(), func(t *testing.T) {
			for _, p := range []*Packet{
				{MessageType: UserMsg, SenderID: EmptyNodeID, MessageID: MessageID(hlc.Now()), TTL: 5},
				{MessageType: UserMsg, SenderID: NodeID(uuid.New()), MessageID: MessageID(^uint64(0)), TTL: 255},
			} {
				data, err := cd.Marshal(p)
				require.NoError(t, err)
				var got Packet
				require.NoError(t, cd.Unmarshal(data, &got))
				assert.Equal(t, p.SenderID, got.SenderID)
				assert.Equal(t, p.MessageID, got.MessageID)
				assert.Equal(t, p.TTL, got.TTL)
			}
		})
	}
}
