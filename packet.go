package gossip

type MessageType uint16
type MessageID struct {
	Timestamp int64
	Seq       uint16
}

const (
	// Message types
	nilMsg              MessageType = iota // No operation message
	pingMsg                                // Sent to test if a peer is alive
	pingAckMsg                             // Sent in response to a ping message
	indirectPingMsg                        // Sent to test if a peer is alive, but not directly reachable
	indirectPingAckMsg                     // Sent in response to an indirect ping message
	nodeJoinMsg                            // Sent by peers when they are joining the network, as doing a push / pull state transfer
	nodeJoinAckMsg                         // Sent by the node that is handling join to acknowledge the joiner
	nodeJoiningMsg                         // Sent by the node that is handling join to announce the joiner to the cluster
	pushPullStateMsg                       // Sent by peers when pushing / pulling state
	pushPullStateAckMsg                    // Acknowledgement of the push state message
	aliveMsg                               // Sent to announce a node is alive
	suspicionMsg                           // Sent to announce a node is suspected to be dead
	leavingMsg                             // Sent to announce a node is leaving
	_                                      // skip to 128
	UserMsg             MessageType = 128  // User messages start here
)

// Packet holds the payload of a message being passed between nodes
type Packet struct {
	MessageType MessageType `msgpack:"message_type" json:"message_type"`
	SenderID    NodeID      `msgpack:"sender_id" json:"sender_id"`
	MessageID   MessageID   `msgpack:"message_id" json:"message_id"`
	TTL         uint8       `msgpack:"ttl" json:"ttl"`
	payload     []byte
	codec       MsgCodec
}

func (p *Packet) Unmarshal(v interface{}) error {
	return p.codec.Unmarshal(p.payload, v)
}

func (p *Packet) Codec() MsgCodec {
	return p.codec
}

type joinMessage struct {
	ID             NodeID `msgpack:"id" json:"id"`
	AdvertisedAddr string `msgpack:"advertised_addr" json:"advertised_addr"`
}

type exchangeNodeState struct {
	ID              NodeID    `msgpack:"id" json:"id"`
	AdvertisedAddr  string    `msgpack:"advertised_addr" json:"advertised_addr"`
	State           NodeState `msgpack:"state" json:"state"`
	StateChangeTime int64     `msgpack:"state_change_time" json:"state_change_time"`
}

type pingMessage struct {
	TargetID NodeID `msgpack:"target_id" json:"target_id"`
	Seq      uint32 `msgpack:"seq" json:"seq"`
	FromAddr string `msgpack:"from_addr" json:"from_addr"`
}

type indirectPingMessage struct {
	TargetID       NodeID `msgpack:"target_id" json:"target_id"`
	AdvertisedAddr string `msgpack:"advertised_addr" json:"advertised_addr"`
	Seq            uint32 `msgpack:"seq" json:"seq"`
	Ok             bool   `msgpack:"ok" json:"ok"`
	FromAddr       string `msgpack:"from_addr" json:"from_addr"`
}

type aliveMessage struct {
	NodeID         NodeID `msgpack:"node_id" json:"node_id"`
	AdvertisedAddr string `msgpack:"advertised_addr" json:"advertised_addr"`
}

type suspicionMessage struct {
	NodeID NodeID `msgpack:"node_id" json:"node_id"`
}

type leavingMessage struct {
	NodeID NodeID `msgpack:"node_id" json:"node_id"`
}
