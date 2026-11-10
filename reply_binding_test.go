package gossip

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// A reply is taken only from the node asked and only when it answers this request (it carries the request's
// MessageID): a reply captured from an earlier exchange (sealed with the cluster key, so it decrypts) is refused.
func TestReplyBoundToItsRequest(t *testing.T) {
	c := newUnitCluster(t)
	peer := newNode(NodeID(uuid.New()), "127.0.0.1:1")
	var captured MessageID // the request id of the first exchange, which the replayed reply answers
	answer := func(edit func(req, reply *Packet)) {
		tr := unitTransport(c)
		tr.mu.Lock()
		tr.replyFn = func(node *Node, req *Packet) (*Packet, error) {
			reply := NewPacket()
			reply.SetCodec(c.config.MsgCodec)
			reply.MessageID = req.MessageID
			reply.makeReply(node.ID, []byte(`"pong"`))
			edit(req, reply)
			return reply, nil
		}
		tr.mu.Unlock()
	}
	ask := func() error {
		var out string
		return c.SendToWithResponse(peer, UserMsg+90, "ping", &out)
	}

	answer(func(req, _ *Packet) { captured = req.MessageID })
	require.NoError(t, ask(), "the reply to this request")

	answer(func(_, reply *Packet) { reply.MessageID = captured })
	require.Error(t, ask(), "a reply to an earlier request")

	answer(func(_, reply *Packet) { reply.SenderID = NodeID(uuid.New()) })
	require.Error(t, ask(), "a reply from another node")

	answer(func(_, reply *Packet) { reply.MessageType = UserMsg + 90 })
	require.Error(t, ask(), "a request, not a reply")
}
