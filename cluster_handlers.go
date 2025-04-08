package gossip

import (
	"fmt"
)

func (c *Cluster) registerSystemHandlers() {
	c.handlers.registerHandler(pingMsg, false, c.handlePing)
	c.handlers.registerHandler(pingAckMsg, false, c.handlePingAck)
	c.handlers.registerHandler(indirectPingMsg, false, c.handleIndirectPing)
	c.handlers.registerHandler(indirectPingAckMsg, false, c.handleIndirectPingAck)

	c.handlers.registerHandlerWithReply(nodeJoinMsg, c.handleJoin)
	c.handlers.registerHandler(nodeJoiningMsg, true, c.handleJoining)
	c.handlers.registerHandlerWithReply(pushPullStateMsg, c.handlePushPullState)
	c.handlers.registerHandler(metadataUpdateMsg, true, c.handleMetadataUpdate)

	c.handlers.registerHandler(aliveMsg, true, c.healthMonitor.handleAlive)
	c.handlers.registerHandler(suspicionMsg, true, c.healthMonitor.handleSuspicion)
	c.handlers.registerHandler(leavingMsg, true, c.healthMonitor.handleLeaving)
}

func (c *Cluster) handlePing(sender *Node, packet *Packet) error {
	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	ping := pingMessage{}
	if err := packet.Unmarshal(&ping); err != nil {
		return err
	}

	// Check if the ping is for us
	if ping.TargetID != c.localNode.ID {
		return nil
	}

	// Echo the ping back to the sender
	return c.sendMessageTo(TransportBestEffort, sender, 1, pingAckMsg, &ping)
}

func (c *Cluster) handlePingAck(sender *Node, packet *Packet) error {
	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	ping := pingMessage{}
	if err := packet.Unmarshal(&ping); err != nil {
		return err
	}

	c.healthMonitor.pingAckReceived(sender.ID, ping.Seq, true)
	return nil
}

func (c *Cluster) handleIndirectPing(sender *Node, packet *Packet) error {
	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	var err error

	ping := indirectPingMessage{}
	if err = packet.Unmarshal(&ping); err != nil {
		return err
	}

	// Create a temporary node for the target
	targetNode := newNode(ping.TargetID, ping.Address)
	ping.Ok, err = c.healthMonitor.pingNode(targetNode)

	// Respond to the sender with the ping acknowledgment
	return c.sendMessageTo(TransportBestEffort, sender, 1, indirectPingAckMsg, &ping)
}

func (c *Cluster) handleIndirectPingAck(sender *Node, packet *Packet) error {
	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	ping := indirectPingMessage{}
	if err := packet.Unmarshal(&ping); err != nil {
		return err
	}

	c.healthMonitor.pingAckReceived(sender.ID, ping.Seq, ping.Ok)
	return nil
}

func (c *Cluster) handleJoin(sender *Node, packet *Packet) (MessageType, interface{}, error) {
	var joinMsg joinMessage

	err := packet.Unmarshal(&joinMsg)
	if err != nil {
		return nilMsg, nil, err
	}

	// Check the protocol version and application version, reject if not compatible
	accepted := true
	if joinMsg.ProtocolVersion != c.localNode.ProtocolVersion || (c.config.ApplicationVersionCheck != nil && !c.config.ApplicationVersionCheck.CheckVersion(joinMsg.ApplicationVersion)) {
		accepted = false
	} else {
		// Check add the peer to our list of known peers unless it already exists
		node := newNode(joinMsg.ID, joinMsg.Address)
		node.ProtocolVersion = joinMsg.ProtocolVersion
		node.ApplicationVersion = joinMsg.ApplicationVersion

		if c.nodes.addOrUpdate(node) {
			node.metadata.update(joinMsg.Metadata, joinMsg.MetadataTimestamp, true)
		}

		// Gossip the node to our peers
		packet.MessageType = nodeJoiningMsg
		c.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{c.localNode.ID, packet.SenderID})
	}

	// Respond to the sender with our information
	selfJoinMsg := joinReplyMessage{
		Accepted:           accepted,
		ID:                 c.localNode.ID,
		Address:            c.localNode.address,
		MetadataTimestamp:  c.localNode.Metadata.GetTimestamp(),
		Metadata:           c.localNode.Metadata.GetAll(),
		ProtocolVersion:    c.localNode.ProtocolVersion,
		ApplicationVersion: c.localNode.ApplicationVersion,
	}

	return nodeJoinAckMsg, &selfJoinMsg, nil
}

func (c *Cluster) handleJoining(sender *Node, packet *Packet) error {
	var joinMsg joinMessage

	err := packet.Unmarshal(&joinMsg)
	if err != nil {
		return err
	}

	// Check the protocol version and application version, reject if not compatible
	if joinMsg.ProtocolVersion == c.localNode.ProtocolVersion && (c.config.ApplicationVersionCheck == nil || c.config.ApplicationVersionCheck.CheckVersion(joinMsg.ApplicationVersion)) {
		node := newNode(joinMsg.ID, joinMsg.Address)
		node.ProtocolVersion = joinMsg.ProtocolVersion
		node.ApplicationVersion = joinMsg.ApplicationVersion

		node.metadata.update(joinMsg.Metadata, joinMsg.MetadataTimestamp, true)
		c.nodes.addOrUpdate(node)
	}

	return nil
}

func (c *Cluster) handlePushPullState(sender *Node, packet *Packet) (MessageType, interface{}, error) {
	if sender == nil {
		return nilMsg, nil, fmt.Errorf("unknown sender")
	}

	var peerStates []exchangeNodeState
	err := packet.Unmarshal(&peerStates)
	if err != nil {
		return nilMsg, nil, err
	}

	nodes := c.nodes.getRandomNodes(c.getPeerSubsetSize(c.nodes.getTotalCount(), purposeStateExchange), []NodeID{}, nil)

	var localStates []exchangeNodeState
	for _, n := range nodes {
		localStates = append(localStates, exchangeNodeState{
			ID:                n.ID,
			Address:           n.address,
			State:             n.state,
			StateChangeTime:   n.stateChangeTime.UnixNano(),
			MetadataTimestamp: n.metadata.GetTimestamp(),
			Metadata:          n.metadata.GetAll(),
		})
	}

	go c.healthMonitor.combineRemoteNodeState(sender, peerStates)

	return pushPullStateAckMsg, &localStates, nil
}

func (c *Cluster) handleMetadataUpdate(sender *Node, packet *Packet) error {
	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	var metadataUpdate metadataUpdateMessage
	err := packet.Unmarshal(&metadataUpdate)
	if err != nil {
		return err
	}

	node := c.nodes.get(sender.ID)
	if node == nil {
		return fmt.Errorf("unknown sender")
	}

	if node.metadata.update(metadataUpdate.Metadata, metadataUpdate.MetadataTimestamp, false) {
		if c.eventListener != nil {
			c.eventListener.OnNodeMetadataChanged(node)
		}
	}

	return nil
}
