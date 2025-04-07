package gossip

import (
	"fmt"
	"net"
	"time"
)

func (c *Cluster) nextMessageID() MessageID {
	// Get the current time once before entering the loop
	now := time.Now().UnixNano()

	for {
		current := c.messageIdGen.Load()

		// Create the next message ID (avoid allocating until needed)
		var nextSeq uint16
		var nextTimestamp int64

		// If timestamp is the same, increment sequence
		if current.Timestamp == now {
			nextTimestamp = now
			nextSeq = current.Seq + 1
			// Handle overflow
			if nextSeq == 0 {
				// In case of overflow, move to next nanosecond
				nextTimestamp = now + 1
			}
		} else if current.Timestamp < now {
			// If our timestamp is newer, start with sequence 0
			nextTimestamp = now
			nextSeq = 0
		} else {
			// Current timestamp is in the future (clock went backward)
			// Continue using the current timestamp but increment sequence
			nextTimestamp = current.Timestamp
			nextSeq = current.Seq + 1
		}

		// Create the next ID value (only allocate once we know the values)
		next := &MessageID{
			Timestamp: nextTimestamp,
			Seq:       nextSeq,
		}

		// Try to update atomically
		if c.messageIdGen.CompareAndSwap(current, next) {
			return *next
		}
	}
}

func (c *Cluster) createPacket(sender NodeID, msgType MessageType, ttl uint8, payload interface{}) (*Packet, error) {
	packet := &Packet{
		MessageType: msgType,
		SenderID:    sender,
		MessageID:   c.nextMessageID(),
		TTL:         ttl,
		codec:       c.config.MsgCodec,
	}

	// Marshal the payload to a byte buffer
	var err error
	packet.payload, err = packet.codec.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return packet, nil
}

func (c *Cluster) sendMessage(transportType TransportType, msgType MessageType, data interface{}) error {
	packet, err := c.createPacket(c.localNode.ID, msgType, uint8(c.getPeerSubsetSize(c.nodes.getLiveCount(), purposeTTL)), data)
	if err != nil {
		return err
	}

	c.enqueuePacketForBroadcast(packet, transportType, []NodeID{c.localNode.ID})
	return nil
}

func (c *Cluster) Send(msgType MessageType, data interface{}) error {
	if msgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage(TransportBestEffort, msgType, data)
}

func (c *Cluster) SendReliable(msgType MessageType, data interface{}) error {
	if msgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessage(TransportReliable, msgType, data)
}

// Internal function to send a message to a specific node.
func (c *Cluster) sendMessageTo(transportType TransportType, dstNode *Node, ttl uint8, msgType MessageType, data interface{}) error {
	packet, err := c.createPacket(c.localNode.ID, msgType, ttl, data)
	if err != nil {
		return err
	}

	return c.transport.SendPacket(transportType, dstNode, packet)
}

func (c *Cluster) SendTo(dstNode *Node, msgType MessageType, data interface{}) error {
	if msgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessageTo(TransportBestEffort, dstNode, uint8(c.getPeerSubsetSize(c.nodes.getLiveCount(), purposeTTL)), msgType, data)
}

func (c *Cluster) SendToReliable(dstNode *Node, msgType MessageType, data interface{}) error {
	if msgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}
	return c.sendMessageTo(TransportReliable, dstNode, uint8(c.getPeerSubsetSize(c.nodes.getLiveCount(), purposeTTL)), msgType, data)
}

// Send a message to the peer then accept a response message.
// Uses a TCP connection to send the packet and receive the response.
func (c *Cluster) sendToWithResponse(dstNode *Node, msgType MessageType, payload interface{}, responseMsgType MessageType, responsePayload interface{}) error {
	packet, err := c.createPacket(c.localNode.ID, msgType, 1, payload)
	if err != nil {
		return err
	}

	conn, err := c.transport.DialPeer(dstNode)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Write the packet to the connection
	err = c.transport.WritePacket(conn, packet)
	if err != nil {
		return err
	}

	responsePacket, err := c.transport.ReadPacket(conn)
	if err != nil {
		return err
	}

	// If the response message type doesn't match the expected type, return an error
	if responsePacket.MessageType != responseMsgType {
		return fmt.Errorf("unexpected response message type: got %d, want %d", responsePacket.MessageType, responseMsgType)
	}

	// Unmarshal the response payload
	err = responsePacket.Unmarshal(responsePayload)
	if err != nil {
		return err
	}

	return nil
}

// Send a message to the peer then accept a response message.
// Uses a TCP connection to send the packet and receive the response.
func (c *Cluster) SendToWithResponse(dstNode *Node, msgType MessageType, payload interface{}, responseMsgType MessageType, responsePayload interface{}) error {
	if msgType < UserMsg || responseMsgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}

	return c.sendToWithResponse(dstNode, msgType, payload, responseMsgType, responsePayload)
}

// Send a metadata update to the cluster.
func (c *Cluster) SendMetadataUpdate() error {
	updateMsg := &metadataUpdateMessage{
		MetadataTimestamp: c.localNode.metadata.GetTimestamp(),
		Metadata:          c.localNode.metadata.GetAll(),
	}

	packet, err := c.createPacket(c.localNode.ID, metadataUpdateMsg, uint8(c.getPeerSubsetSize(c.nodes.getLiveCount(), purposeTTL)), &updateMsg)
	if err != nil {
		return err
	}

	c.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{c.localNode.ID})
	return nil
}

func (c *Cluster) OpenStream(dstNode *Node, msgType MessageType, payload interface{}) (net.Conn, error) {
	if msgType < UserMsg {
		return nil, fmt.Errorf("invalid message type")
	}

	conn, err := c.transport.DialPeer(dstNode)
	if err != nil {
		return nil, err
	}

	err = c.WriteStream(conn, msgType, payload)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func (c *Cluster) WriteStream(conn net.Conn, msgType MessageType, payload interface{}) error {
	if msgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}

	packet, err := c.createPacket(c.localNode.ID, msgType, 1, payload)
	if err != nil {
		return err
	}

	// Write the packet to the connection
	return c.transport.WritePacket(conn, packet)
}

func (c *Cluster) ReadStream(conn net.Conn, msgType MessageType, payload interface{}) error {
	if msgType < UserMsg {
		return fmt.Errorf("invalid message type")
	}

	packet, err := c.transport.ReadPacket(conn)
	if err != nil {
		return err
	}

	// If the response message type doesn't match the expected type, return an error
	if packet.MessageType != msgType {
		return fmt.Errorf("unexpected response")
	}

	// Unmarshal the response payload
	return packet.Unmarshal(payload)
}
