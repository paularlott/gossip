package gossip

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// Helper function to create a Stream from a connection.
func (c *Cluster) wrapStream(conn net.Conn) (net.Conn, error) {
	return NewStream(conn, c.config), nil
}

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
	packet := NewPacket()
	packet.MessageType = msgType
	packet.SenderID = sender
	packet.MessageID = c.nextMessageID()
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

// Internal function to send a message to a specific node.
func (c *Cluster) sendMessageTo(transportType TransportType, dstNode *Node, ttl uint8, msgType MessageType, data interface{}) error {
	packet, err := c.createPacket(c.localNode.ID, msgType, ttl, data)
	if err != nil {
		return err
	}
	defer packet.Release()

	return c.transport.SendPacket(transportType, []*Node{dstNode}, packet)
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
// Uses a TCP connection to send the packet and receive the response.
func (c *Cluster) sendToWithResponse(dstNode *Node, msgType MessageType, payload interface{}, responsePayload interface{}) error {
	packet, err := c.createPacket(c.localNode.ID, msgType, 1, payload)
	if err != nil {
		return err
	}
	defer packet.Release()

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
	defer responsePacket.Release()

	// Unmarshal the response payload
	if responsePayload != nil {
		err = responsePacket.Unmarshal(responsePayload)
		if err != nil {
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

// Send a metadata update to the cluster.
func (c *Cluster) UpdateMetadata() error {
	updateMsg := &metadataUpdateMessage{
		MetadataTimestamp: c.localNode.metadata.GetTimestamp(),
		Metadata:          c.localNode.metadata.GetAll(),
	}

	packet, err := c.createPacket(c.localNode.ID, metadataUpdateMsg, c.getMaxTTL(), &updateMsg)
	if err != nil {
		return err
	}

	c.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{c.localNode.ID}, nil)

	// Trigger a metadata update event
	c.notifyMetadataChanged(c.localNode)
	return nil
}

func (c *Cluster) OpenStream(dstNode *Node, msgType MessageType, payload interface{}) (net.Conn, error) {
	if msgType < ReservedMsgsStart {
		return nil, fmt.Errorf("invalid message type")
	}

	conn, err := c.transport.DialPeer(dstNode)
	if err != nil {
		return nil, err
	}

	// Write a standard packet to the connection to initiate the stream
	packet, err := c.createPacket(c.localNode.ID, msgType, 1, payload)
	if err != nil {
		return nil, err
	}
	defer packet.Release()

	// Write the packet to the connection
	err = c.transport.WritePacket(conn, packet)
	if err != nil {
		conn.Close()
		return nil, err
	}

	// Wait for the server to ack the stream is open
	var ackMsg uint16
	err = binary.Read(conn, binary.BigEndian, &ackMsg)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read message type: %w", err)
	}

	if ackMsg != uint16(streamOpenAckMsg) {
		conn.Close()
		return nil, fmt.Errorf("unexpected message type: expected %d, got %d", streamOpenAckMsg, ackMsg)
	}

	// Wrap the connection in a stream
	return c.wrapStream(conn)
}

// WriteStreamMsg writes a message directly to the stream with a simple framing protocol:
// [2 bytes MessageType][4 bytes payload length][payload bytes]
func (c *Cluster) WriteStreamMsg(conn net.Conn, msgType MessageType, payload interface{}) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type for stream message: %d", msgType)
	}

	payloadBytes, err := c.config.MsgCodec.Marshal(payload)
	if err != nil {
		return err
	}

	// If the payload size +6 would exceed the maximum packet size, return an error
	if len(payloadBytes) > c.config.TCPMaxPacketSize-6 {
		return fmt.Errorf("payload size exceeds maximum packet size: %d bytes", len(payloadBytes)+6)
	}

	err = conn.SetWriteDeadline(time.Now().Add(c.config.TCPDeadline))
	if err != nil {
		return err
	}

	// Create a single buffer containing both header and payload
	combinedBuffer := make([]byte, 6+len(payloadBytes))

	// Write the message type and payload length to the buffer
	binary.BigEndian.PutUint16(combinedBuffer[0:2], uint16(msgType))
	binary.BigEndian.PutUint32(combinedBuffer[2:6], uint32(len(payloadBytes)))

	// Copy the payload into the buffer after the header
	copy(combinedBuffer[6:], payloadBytes)

	// Write everything in a single operation
	n, err := conn.Write(combinedBuffer)
	if err != nil {
		return err
	}
	if n != len(combinedBuffer) {
		return fmt.Errorf("failed to write all bytes: %d of %d bytes written", n, len(combinedBuffer))
	}

	return nil
}

// ReadStreamMsg reads a message from the stream and unmarshals it into the provided payload.
// It expects the message to have the specific msgType.
func (c *Cluster) ReadStreamMsg(conn net.Conn, expectedMsgType MessageType, payload interface{}) error {
	err := conn.SetReadDeadline(time.Now().Add(c.config.TCPDeadline))
	if err != nil {
		return err
	}

	// Read the message type (2 bytes) and payload length (4 bytes)
	header := make([]byte, 6)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		return err
	}

	// Unpack the header
	msgType := binary.BigEndian.Uint16(header[0:2])
	payloadLen := binary.BigEndian.Uint32(header[2:6])

	if payloadLen > uint32(c.config.TCPMaxPacketSize) {
		return fmt.Errorf("payload length too large: %d bytes", payloadLen)
	}

	payloadBytes := make([]byte, payloadLen)
	_, err = io.ReadFull(conn, payloadBytes)
	if err != nil {
		return err
	}

	if msgType != uint16(expectedMsgType) {
		return fmt.Errorf("unexpected message type: expected %d, got %d", expectedMsgType, msgType)
	}

	return c.config.MsgCodec.Unmarshal(payloadBytes, payload)
}
