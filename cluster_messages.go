package gossip

import (
	"context"
	"fmt"

	"github.com/paularlott/gossip/hlc"
)

func (c *Cluster) createPacketWithTarget(sender NodeID, target *NodeID, msgType MessageType, ttl uint8, payload interface{}) (*Packet, error) {
	return c.createPacketWithTargetAndTag(sender, target, nil, msgType, ttl, payload)
}

func (c *Cluster) createPacketWithTargetAndTag(sender NodeID, target *NodeID, tag *string, msgType MessageType, ttl uint8, payload interface{}) (*Packet, error) {
	packet := NewPacket()
	packet.MessageType = msgType
	packet.SenderID = sender
	packet.TargetNodeID = target
	packet.Tag = tag
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = ttl
	packet.codec = c.config.MsgCodec

	// Marshal the payload to a byte buffer
	var err error
	packet.payload, err = packet.codec.Marshal(payload)
	if err != nil {
		packet.Release()
		return nil, err
	}

	return packet, nil
}

func (c *Cluster) sendMessage(peers []*Node, transportType TransportType, ttl uint8, msgType MessageType, data interface{}) error {
	return c.sendMessageWithTargetAndTag(peers, nil, nil, transportType, ttl, msgType, data)
}

func (c *Cluster) sendMessageWithTarget(peers []*Node, target *NodeID, transportType TransportType, ttl uint8, msgType MessageType, data interface{}) error {
	return c.sendMessageWithTargetAndTag(peers, target, nil, transportType, ttl, msgType, data)
}

func (c *Cluster) sendMessageWithTag(peers []*Node, tag *string, transportType TransportType, ttl uint8, msgType MessageType, data interface{}) error {
	return c.sendMessageWithTargetAndTag(peers, nil, tag, transportType, ttl, msgType, data)
}

func (c *Cluster) sendMessageWithTargetAndTag(peers []*Node, target *NodeID, tag *string, transportType TransportType, ttl uint8, msgType MessageType, data interface{}) error {
	packet, err := c.createPacketWithTargetAndTag(c.localNode.ID, target, tag, msgType, ttl, data)
	if err != nil {
		return err
	}

	// broadcast will release the packet once it is sent
	c.enqueuePacketForBroadcast(packet, transportType, []NodeID{c.localNode.ID}, peers)
	return nil
}

func (c *Cluster) Send(msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage(nil, TransportBestEffort, c.getMaxTTL(), msgType, data)
}

func (c *Cluster) SendReliable(msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage(nil, TransportReliable, c.getMaxTTL(), msgType, data)
}

func (c *Cluster) SendTagged(tag string, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessageWithTag(nil, &tag, TransportBestEffort, c.getMaxTTL(), msgType, data)
}

func (c *Cluster) SendTaggedReliable(tag string, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessageWithTag(nil, &tag, TransportReliable, c.getMaxTTL(), msgType, data)
}

func (c *Cluster) SendTo(dstNode *Node, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessageWithTarget([]*Node{dstNode}, &dstNode.ID, TransportBestEffort, 1, msgType, data)
}

func (c *Cluster) SendToReliable(dstNode *Node, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessageWithTarget([]*Node{dstNode}, &dstNode.ID, TransportReliable, 1, msgType, data)
}

func (c *Cluster) SendToPeers(dstNodes []*Node, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage(dstNodes, TransportBestEffort, 1, msgType, data)
}

func (c *Cluster) SendToPeersReliable(dstNodes []*Node, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage(dstNodes, TransportReliable, 1, msgType, data)
}

// SendToWithResponse sends a message to the peer and waits for its response message, bounded by the transport's
// timeouts (TCPDialTimeout, TCPDeadline): SendToWithResponseContext with no deadline of the caller's own.
func (c *Cluster) SendToWithResponse(dstNode *Node, msgType MessageType, payload interface{}, responsePayload interface{}) error {
	return c.SendToWithResponseContext(context.Background(), dstNode, msgType, payload, responsePayload)
}

// SendToWithResponseContext sends a message to the peer and waits for its response message, also within ctx: its
// deadline or cancellation ends the exchange, the dial included, with ctx's error. With a transport that is not a
// ContextTransport the wait ends with ctx while the exchange runs on to the transport's own timeouts.
func (c *Cluster) SendToWithResponseContext(ctx context.Context, dstNode *Node, msgType MessageType, payload interface{}, responsePayload interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendToWithResponseContext(ctx, dstNode, msgType, payload, responsePayload)
}

// sendToWithResponse is SendToWithResponse for any message type (the cluster's own included).
func (c *Cluster) sendToWithResponse(dstNode *Node, msgType MessageType, payload interface{}, responsePayload interface{}) error {
	return c.sendToWithResponseContext(context.Background(), dstNode, msgType, payload, responsePayload)
}

func (c *Cluster) sendToWithResponseContext(ctx context.Context, dstNode *Node, msgType MessageType, payload interface{}, responsePayload interface{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	packet, err := c.createPacketWithTarget(c.localNode.ID, &dstNode.ID, msgType, 1, payload)
	if err != nil {
		return err
	}
	defer packet.Release()

	responsePacket, err := c.sendWithReply(ctx, dstNode, packet)
	if err != nil {
		if ctx.Err() == nil {
			dstNode.ClearAddress() // the peer failed, not the caller's deadline
		}
		return err
	}
	if responsePacket == nil {
		dstNode.ClearAddress()
		return fmt.Errorf("nil response from peer")
	}
	defer responsePacket.Release()

	// Unmarshal the response payload
	if responsePayload != nil {
		err = responsePacket.Unmarshal(responsePayload)
		if err != nil {
			dstNode.ClearAddress()
			return err
		}
	}

	return nil
}

// sendWithReply runs the transport's exchange within ctx: through ContextTransport when the transport is one,
// otherwise by waiting for it no longer than ctx (a reply that arrives after is released).
func (c *Cluster) sendWithReply(ctx context.Context, node *Node, packet *Packet) (*Packet, error) {
	if ct, ok := c.transport.(ContextTransport); ok {
		return ct.SendWithReplyContext(ctx, node, packet)
	}
	if ctx.Done() == nil {
		return c.transport.SendWithReply(node, packet)
	}
	type result struct {
		reply *Packet
		err   error
	}
	done := make(chan result, 1)
	own := packet.AddRef() // the exchange may outlive this call
	go func() {
		defer own.Release()
		reply, err := c.transport.SendWithReply(node, own)
		done <- result{reply, err}
	}()
	select {
	case r := <-done:
		return r.reply, r.err
	case <-ctx.Done():
		go func() {
			if r := <-done; r.reply != nil {
				r.reply.Release()
			}
		}()
		return nil, ctx.Err()
	}
}
