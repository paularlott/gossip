package gossip

import (
	"fmt"

	"github.com/paularlott/gossip/hlc"
)

func (c *Cluster) createPacket(sender NodeID, msgType MessageType, ttl uint8, payload interface{}) (*Packet, error) {
	packet := NewPacket()
	packet.MessageType = msgType
	packet.SenderID = sender
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

func (c *Cluster) sendMessage(peers []*Node, transportType TransportType, msgType MessageType, data interface{}) error {
	packet, err := c.createPacket(c.localNode.ID, msgType, c.getMaxTTL(), data)
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
	return c.sendMessage(nil, TransportBestEffort, msgType, data)
}

func (c *Cluster) SendReliable(msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage(nil, TransportReliable, msgType, data)
}

func (c *Cluster) sendMessageExclude(peers []*Node, transportType TransportType, msgType MessageType, data interface{}, excludeNodes []NodeID) error {
	packet, err := c.createPacket(c.localNode.ID, msgType, c.getMaxTTL(), data)
	if err != nil {
		return err
	}

	// Ensure the local node is in the exclude list
	found := false
	for _, id := range excludeNodes {
		if id == c.localNode.ID {
			found = true
			break
		}
	}
	if !found {
		excludeNodes = append(excludeNodes, c.localNode.ID)
	}

	// broadcast will release the packet once it is sent
	c.enqueuePacketForBroadcast(packet, transportType, excludeNodes, peers)
	return nil
}

func (c *Cluster) SendExcluding(msgType MessageType, data interface{}, excludeNodes []NodeID) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessageExclude(nil, TransportBestEffort, msgType, data, excludeNodes)
}

func (c *Cluster) SendReliableExcluding(msgType MessageType, data interface{}, excludeNodes []NodeID) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessageExclude(nil, TransportReliable, msgType, data, excludeNodes)
}

// Internal function to send a message to a specific node.
func (c *Cluster) sendMessageTo(transportType TransportType, dstNode *Node, ttl uint8, msgType MessageType, data interface{}) error {
	packet, err := c.createPacket(c.localNode.ID, msgType, ttl, data)
	if err != nil {
		return err
	}
	defer packet.Release()

	return c.transport.Send(transportType, dstNode, packet)
}

func (c *Cluster) SendTo(dstNode *Node, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage([]*Node{dstNode}, TransportBestEffort, msgType, data)
}

func (c *Cluster) SendToReliable(dstNode *Node, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage([]*Node{dstNode}, TransportReliable, msgType, data)
}

func (c *Cluster) SendToPeers(dstNodes []*Node, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage(dstNodes, TransportBestEffort, msgType, data)
}

func (c *Cluster) SendToPeersReliable(dstNodes []*Node, msgType MessageType, data interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage(dstNodes, TransportReliable, msgType, data)
}

// Send a message to the peer then accept a response message.
func (c *Cluster) sendToWithResponse(dstNode *Node, msgType MessageType, payload interface{}, responsePayload interface{}) error {
	packet, err := c.createPacket(c.localNode.ID, msgType, 1, payload)
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
