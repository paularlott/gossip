package gossip

import (
	"fmt"
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
	nodeLeaveMsg                         // Sent by peers when they are leaving the network
	pushPullStateMsg                     // Sent by peers when pushing / pulling state
	metadataUpdateMsg                    // Update the metadata of a node
	pingMsg                              // Health check ping
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
	MessageType  MessageType `msgpack:"mt" json:"mt"`
	SenderID     NodeID      `msgpack:"si" json:"si"`
	TargetNodeID *NodeID     `msgpack:"ti,omitempty" json:"ti,omitempty"` // Optional: for direct messages
	MessageID    MessageID   `msgpack:"mi" json:"mi"`
	TTL          uint8       `msgpack:"ttl" json:"ttl"`
	payload      []byte
	codec        codec.Serializer

	// Connection for connection-based transports (TCP/WebSocket)
	conn net.Conn

	// Reply channel for async replies (works for all transports)
	replyChan chan<- *Packet

	refCount atomic.Int32
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
		p.replyChan = nil
		p.SenderID = EmptyNodeID
		p.TargetNodeID = nil

		packetPool.Put(p)
	}
}

func (p *Packet) Unmarshal(v interface{}) error {
	return p.codec.Unmarshal(p.payload, v)
}

func (p *Packet) Codec() codec.Serializer {
	return p.codec
}

// CanReply returns true if the packet can send a reply
func (p *Packet) CanReply() bool {
	return p.conn != nil || p.replyChan != nil
}

// SendReply sends a reply packet using the available reply mechanism
func (p *Packet) SendReply() error {
	if p.replyChan != nil {
		select {
		case p.replyChan <- p:
			return nil
		default:
			return fmt.Errorf("reply channel full or closed")
		}
	}
	return fmt.Errorf("no reply mechanism available")
}

// SetReplyChan sets the reply channel for this packet
func (p *Packet) SetReplyChan(ch chan<- *Packet) {
	p.replyChan = ch
}

// SetConn sets the connection for this packet
func (p *Packet) SetConn(conn net.Conn) {
	p.conn = conn
}

// Payload returns the packet payload
func (p *Packet) Payload() []byte {
	return p.payload
}

// SetPayload sets the packet payload
func (p *Packet) SetPayload(payload []byte) {
	p.payload = payload
}

// SetCodec sets the packet codec
func (p *Packet) SetCodec(codec codec.Serializer) {
	p.codec = codec
}

type joinMessage struct {
	ID                 NodeID                 `msgpack:"id" json:"id"`
	AdvertiseAddr      string                 `msgpack:"addr" json:"addr"`
	ProtocolVersion    uint16                 `msgpack:"pv" json:"pv"`
	ApplicationVersion string                 `msgpack:"av" json:"av"`
	State              NodeState              `msgpack:"s" json:"s"`
	MetadataTimestamp  hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata           map[string]interface{} `msgpack:"md" json:"md"`
}

type joinReplyMessage struct {
	Accepted          bool                   `msgpack:"acc" json:"acc"`
	RejectReason      string                 `msgpack:"rr" json:"rr"`
	NodeID            NodeID                 `msgpack:"id" json:"id"`
	AdvertiseAddr     string                 `msgpack:"addr" json:"addr"`
	MetadataTimestamp hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata          map[string]interface{} `msgpack:"md" json:"md"`
	Nodes             []joinNode             `msgpack:"nodes" json:"nodes"`
}

type joinNode struct {
	ID            NodeID `msgpack:"id" json:"id"`
	AdvertiseAddr string `msgpack:"addr" json:"addr"`
}

type exchangeNodeState struct {
	ID             NodeID        `msgpack:"id" json:"id"`
	AdvertiseAddr  string        `msgpack:"addr" json:"addr"`
	State          NodeState     `msgpack:"s" json:"s"`
	StateTimestamp hlc.Timestamp `msgpack:"sct" json:"sct"`
}

type metadataUpdateMessage struct {
	MetadataTimestamp hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata          map[string]interface{} `msgpack:"md" json:"md"`
	NodeState         NodeState              `msgpack:"state" json:"state"`
}

type pingMessage struct {
	SenderID      NodeID `msgpack:"sid" json:"sid"`
	AdvertiseAddr string `msgpack:"addr" json:"addr"`
}

type pongMessage struct {
	NodeID            NodeID                 `msgpack:"nid" json:"nid"`
	AdvertiseAddr     string                 `msgpack:"addr" json:"addr"`
	MetadataTimestamp hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata          map[string]interface{} `msgpack:"md" json:"md"`
	NodeState         NodeState              `msgpack:"state" json:"state"`
}
