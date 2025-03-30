package gossip

import (
	"github.com/shamaton/msgpack/v2"
)

type MessageType uint16
type MessageID struct {
	Timestamp int64
	Seq       uint16
}

const (
	// Message types
	pingMsg            MessageType = iota // Sent to test if a peer is alive
	pingAckMsg                            // Sent in response to a ping message
	indirectPingMsg                       // Sent to test if a peer is alive, but not directly reachable
	indirectPingAckMsg                    // Sent in response to an indirect ping message
	nodeJoinMsg                           // Sent by peers when they are joining the network, as doing a push / pull state transfer
	nodeJoinAckMsg                        // Sent by the node that is handling join to acknowledge the joiner
	/*
		 	nodeJoiningMsg                        // When the join message is forwarded to other peers it changes to this type, push only
			nodeLeaveMsg                          // Sent by a peer leaving the network
			//	peerAliveMsg                          // Sent by a peer to announce its presence
			//	peerSuspectMsg                         // Sent when a peer is suspected to be dead
			//	peerDeadMsg                            // Sent when a peer is marked as dead
	*/
	pushPullStateMsg    // Sent by peers when pushing / pulling state
	pushPullStateAckMsg // Acknowledgement of the push state message
/*
_                                     // skip to 128
userMsg             MessageType = 128 // User messages start here
*/
)

// Packet holds the payload of a message being passed between nodes
type Packet struct {
	MessageType MessageType `msgpack:"message_type"`
	SenderID    NodeID      `msgpack:"sender_id"`
	MessageID   MessageID   `msgpack:"message_id"`
	TTL         uint8       `msgpack:"ttl"`
	Payload     []byte      `msgpack:"-"`
}

func (p *Packet) Unmarshal(v interface{}) error {
	return msgpack.Unmarshal(p.Payload, v)
}

type joinMessage struct {
	ID             NodeID `msgpack:"id"`
	AdvertisedAddr string `msgpack:"advertised_addr"`
}

type pushPullState struct {
	ID              NodeID    `msgpack:"id"`
	AdvertisedAddr  string    `msgpack:"advertised_addr"`
	State           NodeState `msgpack:"state"`
	LastStateUpdate int64     `msgpack:"last_state_update"`
}

type pingMessage struct {
	TargetID NodeID `msgpack:"target_id"`
	Seq      uint32 `msgpack:"seq"`
}

type indirectPingMessage struct {
	TargetID       NodeID `msgpack:"target_id"`
	AdvertisedAddr string `msgpack:"advertised_addr"`
	Seq            uint32 `msgpack:"seq"`
	Ok             bool   `msgpack:"ok"`
}
