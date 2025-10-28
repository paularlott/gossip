package gossip

import (
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

// Send a message to the peer then accept a response message.
func (c *Cluster) sendToWithResponse(dstNode *Node, msgType MessageType, payload interface{}, responsePayload interface{}) error {
	packet, err := c.createPacketWithTarget(c.localNode.ID, &dstNode.ID, msgType, 1, payload)
	if err != nil {
		return err
	}
	defer packet.Release()

	responsePacket, err := c.transport.SendWithReply(dstNode, packet)
	if err != nil {
		dstNode.Address().Clear()
		return err
	}
	defer responsePacket.Release()

	// Unmarshal the response payload
	if responsePayload != nil {
		err = responsePacket.Unmarshal(responsePayload)
		if err != nil {
			dstNode.Address().Clear()
			return err
		}
	}

	return nil
}

// Send a message to the peer then accept a response message.
// Uses a TCP connection to send the packet and receive the response.
func (c *Cluster) SendToWithResponse(dstNode *Node, msgType MessageType, payload interface{}, responsePayload interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}

	return c.sendToWithResponse(dstNode, msgType, payload, responsePayload)
}
