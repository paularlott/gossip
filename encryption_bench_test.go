package gossip

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/encryption/aes"
	"github.com/paularlott/gossip/hlc"
	"github.com/paularlott/logger"
)

func benchTransport() *SocketTransport {
	cfg := DefaultConfig()
	cfg.MsgCodec = shamaton.New()
	cfg.Logger = logger.NewNullLogger()
	cfg.Cipher = aes.New()
	cfg.EncryptionKey = []byte("0123456789abcdef0123456789abcdef")
	return NewSocketTransport(cfg)
}

func benchPacket(size int) *Packet {
	p := NewPacket()
	p.MessageType = UserMsg
	p.SenderID = NodeID(uuid.New())
	p.MessageID = MessageID(hlc.Now())
	p.TTL = 3
	p.SetCodec(shamaton.New())
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	p.SetPayload(payload)
	return p
}

// Typical gossip, the UDP limit, and a large reliable packet.
var benchSizes = []int{200, 1300, 65536}

func benchRoundTrip(b *testing.B, keyed bool) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			st := benchTransport()
			p := benchPacket(size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				buf, err := st.encode(p, false, false, keyed)
				if err != nil {
					b.Fatal(err)
				}
				q, _, err := st.packetFromBuffer(buf)
				if err != nil {
					b.Fatal(err)
				}
				q.Release()
			}
		})
	}
}

// Encrypted encode and decode in the original format (towards nodes that do not read the keyed one).
func BenchmarkEncryptedRoundTripOriginal(b *testing.B) { benchRoundTrip(b, false) }

// Encrypted encode and decode in the keyed format (between nodes of this version).
func BenchmarkEncryptedRoundTripKeyed(b *testing.B) { benchRoundTrip(b, true) }

// Decoding the original format on a node that reads the keyed one (from an older node during an upgrade): the keyed
// attempt comes first.
func BenchmarkDecodeOriginalOnKeyedNode(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			st := benchTransport()
			buf, _ := st.packetToBuffer(benchPacket(size), false)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				q, _, err := st.packetFromBuffer(buf)
				if err != nil {
					b.Fatal(err)
				}
				q.Release()
			}
		})
	}
}
