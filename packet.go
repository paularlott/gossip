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

	// ReservedMsgsStart opens the library's internal message range, 64–127.
	// Applications must not use these types; user messages start at UserMsg
	// (128).
	//
	// Every subpackage protocol (leader, lock, kv, …) allocates its message
	// types from this range in the single registry below — one list, one
	// place, so allocations never collide. To add a new protocol: take the
	// next free block (leave a gap between blocks; next free is +30), add
	// the constants here, and extend the ledger test in
	// reserved_msgs_test.go. Values are wire-stable: never renumber an
	// allocated type.
	ReservedMsgsStart MessageType = 64

	// leader — leader election (the leader package's defaults; configurable
	// per election via its Config).
	LeaderHeartbeatMsg  MessageType = ReservedMsgsStart + 1
	LeaderForgetMessage MessageType = ReservedMsgsStart + 2

	// lock — distributed lock pool protocol.
	LockAcquireMsg       MessageType = ReservedMsgsStart + 10
	LockReleaseMsg       MessageType = ReservedMsgsStart + 11
	LockExtendMsg        MessageType = ReservedMsgsStart + 12
	LockQueryMsg         MessageType = ReservedMsgsStart + 13
	LockReplicaPushMsg   MessageType = ReservedMsgsStart + 14
	LockReplicaGossipMsg MessageType = ReservedMsgsStart + 15
	LockStateQueryMsg    MessageType = ReservedMsgsStart + 16

	// kv — replicated KV store protocol.
	KVWritePushMsg MessageType = ReservedMsgsStart + 20
	KVGossipMsg    MessageType = ReservedMsgsStart + 21
	KVFullSyncMsg  MessageType = ReservedMsgsStart + 22

	_                         // skip to 128
	UserMsg MessageType = 128 // User messages start here
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
	TargetNodeID *NodeID     `msgpack:"ti,omitempty" json:"ti,omitempty"`   // Optional: for direct messages
	Tag          *string     `msgpack:"tag,omitempty" json:"tag,omitempty"` // Optional: for tag-based routing
	MessageID    MessageID   `msgpack:"mi" json:"mi"`
	TTL          uint8       `msgpack:"ttl" json:"ttl"`
	payload      []byte
	codec        codec.Serializer

	// Connection for connection-based transports (TCP)
	conn net.Conn

	// Reply channel for async replies (works for all transports)
	replyChan chan<- *Packet

	// reply carries the reply to a request a transport is waiting on (setReply)
	reply *replyBox

	// keyed reports that the packet arrived in the keyed encryption format, so a reply goes back in it
	keyed bool

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
		p.reply = nil
		p.SenderID = EmptyNodeID
		p.TargetNodeID = nil
		p.Tag = nil
		p.keyed = false

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
	return p.conn != nil || p.replyChan != nil || p.reply != nil
}

// awaitingReply reports whether a requester is waiting for a reply to this
// packet. Only a reply channel can carry one: a connection on its own (a
// fire-and-forget packet delivered over TCP, which owns the connection only
// so it is closed on release) has no reply path, even though CanReply
// reports true for it.
func (p *Packet) awaitingReply() bool {
	return p.replyChan != nil || p.reply != nil
}

// SendReply sends a reply packet using the available reply mechanism
func (p *Packet) SendReply() (err error) {
	// Recover from panic if channel is closed
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("reply channel closed")
		}
	}()

	if p.reply != nil {
		return p.reply.send(p)
	}
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

// replyBox carries the one reply to a request from the dispatcher to the transport waiting for it. Its channel is
// never closed: the waiter gives up (timeout, shutdown, client gone) at any moment, possibly while the dispatcher is
// sending, and closing a channel another goroutine sends on is a data race (and a recovered panic when the send comes
// second). Instead send and close take the lock: a reply sent after close is refused (its sender releases it), and
// one sent before is released by close, so no reply keeps its reference once nobody is waiting.
type replyBox struct {
	mu     sync.Mutex
	closed bool
	ch     chan *Packet // buffered (1)
}

func newReplyBox() *replyBox { return &replyBox{ch: make(chan *Packet, 1)} }

// C is where the reply arrives.
func (b *replyBox) C() <-chan *Packet { return b.ch }

func (b *replyBox) send(p *Packet) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("reply no longer awaited")
	}
	select {
	case b.ch <- p:
		return nil
	default:
		return fmt.Errorf("reply already sent")
	}
}

// close stops the box taking replies and releases one that arrived but was not taken.
func (b *replyBox) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	select {
	case p := <-b.ch:
		if p != nil {
			p.Release()
		}
	default:
	}
}

// setReply makes b the packet's reply path (transports waiting for a reply to a request).
func (p *Packet) setReply(b *replyBox) {
	p.reply = b
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
	Tags               []string               `msgpack:"tags,omitempty" json:"tags,omitempty"`
	MetadataTimestamp  hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata           map[string]interface{} `msgpack:"md" json:"md"`
	KeyWindow          uint32                 `msgpack:"kw,omitempty" json:"kw,omitempty"` // encryption key window the sender reads (0: original format only)
	Caps               uint32                 `msgpack:"cp,omitempty" json:"cp,omitempty"` // capabilities of the sender (capKeepsConnections)
}

type joinReplyMessage struct {
	Accepted          bool                   `msgpack:"acc" json:"acc"`
	RejectReason      string                 `msgpack:"rr" json:"rr"`
	NodeID            NodeID                 `msgpack:"id" json:"id"`
	AdvertiseAddr     string                 `msgpack:"addr" json:"addr"`
	Tags              []string               `msgpack:"tags,omitempty" json:"tags,omitempty"`
	MetadataTimestamp hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata          map[string]interface{} `msgpack:"md" json:"md"`
	Nodes             []joinNode             `msgpack:"nodes" json:"nodes"`
	KeyWindow         uint32                 `msgpack:"kw,omitempty" json:"kw,omitempty"`
	Caps              uint32                 `msgpack:"cp,omitempty" json:"cp,omitempty"` // capabilities of the sender (capKeepsConnections)
}

type joinNode struct {
	ID            NodeID   `msgpack:"id" json:"id"`
	AdvertiseAddr string   `msgpack:"addr" json:"addr"`
	Tags          []string `msgpack:"tags,omitempty" json:"tags,omitempty"`
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
	KeyWindow         uint32                 `msgpack:"kw,omitempty" json:"kw,omitempty"`
	Caps              uint32                 `msgpack:"cp,omitempty" json:"cp,omitempty"` // capabilities of the sender (capKeepsConnections)
}

type pingMessage struct {
	SenderID      NodeID `msgpack:"sid" json:"sid"`
	AdvertiseAddr string `msgpack:"addr" json:"addr"`
	KeyWindow     uint32 `msgpack:"kw,omitempty" json:"kw,omitempty"`
	Caps          uint32 `msgpack:"cp,omitempty" json:"cp,omitempty"` // capabilities of the sender (capKeepsConnections)
}

type pongMessage struct {
	NodeID            NodeID                 `msgpack:"nid" json:"nid"`
	AdvertiseAddr     string                 `msgpack:"addr" json:"addr"`
	MetadataTimestamp hlc.Timestamp          `msgpack:"mdts" json:"mdts"`
	Metadata          map[string]interface{} `msgpack:"md" json:"md"`
	NodeState         NodeState              `msgpack:"state" json:"state"`
	KeyWindow         uint32                 `msgpack:"kw,omitempty" json:"kw,omitempty"`
	Caps              uint32                 `msgpack:"cp,omitempty" json:"cp,omitempty"` // capabilities of the sender (capKeepsConnections)
}
