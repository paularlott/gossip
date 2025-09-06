package gossip

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/hlc"
)

type MessageType uint16
type MessageID hlc.Timestamp

const (
	// Message types
	replyMsg          MessageType = iota // Reply to a message
	nodeJoinMsg                          // Sent by peers when they are joining the network, as doing a push / pull state transfer
	pushPullStateMsg                     // Sent by peers when pushing / pulling state
	metadataUpdateMsg                    // Update the metadata of a node
	ReservedMsgsStart MessageType = 64   // Reserved for future use
	_                                    // skip to 128
	UserMsg           MessageType = 128  // User messages start here
)

var (
	packetPool = &sync.Pool{
		New: func() interface{} {
			return &Packet{}
		},
	}
)

// Packet holds the payload of a message being passed between nodes
type Packet struct {
	MessageType MessageType `msgpack:"mt" json:"mt"`
	SenderID    NodeID      `msgpack:"si" json:"si"`
	MessageID   MessageID   `msgpack:"mi" json:"mi"`
	TTL         uint8       `msgpack:"ttl" json:"ttl"`
	payload     []byte
	codec       codec.Serializer
	conn        net.Conn
	refCount    atomic.Int32
}

func NewPacket() *Packet {
	p := packetPool.Get().(*Packet)
	p.refCount.Add(1)
	return p
}

func (p *Packet) AddRef() *Packet {
	p.refCount.Add(1)
	return p
}

func (p *Packet) Release() {
	if p.refCount.Add(-1) == 0 {
		if p.conn != nil {
			p.conn.Close()
		}

		p.conn = nil
		p.payload = nil

		packetPool.Put(p)
	}
}

func (p *Packet) Unmarshal(v interface{}) error {
	return p.codec.Unmarshal(p.payload, v)
}

func (p *Packet) Codec() codec.Serializer {
	return p.codec
}

type joinMessage struct {
	ID                 NodeID                 `msgpack:"id" json:"id"`
	AdvertiseAddr      string                 `msgpack:"addr" json:"addr"`
	ProtocolVersion    uint16                 `msgpack:"pv" json:"pv"`
	ApplicationVersion string                 `msgpack:"av" json:"av"`
	MetadataTimestamp  hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata           map[string]interface{} `msgpack:"md" json:"md"`
}

type joinReplyMessage struct {
	Accepted           bool                   `msgpack:"acc" json:"acc"`
	RejectReason       string                 `msgpack:"rr" json:"rr"`
	ID                 NodeID                 `msgpack:"id" json:"id"`
	AdvertiseAddr      string                 `msgpack:"addr" json:"addr"`
	ProtocolVersion    uint16                 `msgpack:"pv" json:"pv"`
	ApplicationVersion string                 `msgpack:"av" json:"av"`
	MetadataTimestamp  hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata           map[string]interface{} `msgpack:"md" json:"md"`
}

type exchangeNodeState struct {
	ID                NodeID                 `msgpack:"id" json:"id"`
	AdvertiseAddr     string                 `msgpack:"addr" json:"addr"`
	State             NodeState              `msgpack:"s" json:"s"`
	StateChangeTime   hlc.Timestamp          `msgpack:"sct" json:"sct"`
	MetadataTimestamp hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata          map[string]interface{} `msgpack:"md" json:"md"`
}

type metadataUpdateMessage struct {
	MetadataTimestamp hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata          map[string]interface{} `msgpack:"md" json:"md"`
}
