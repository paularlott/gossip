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

	c.handlers.registerHandler(aliveMsg, true, c.healthMonitor.handleAlive)
	c.handlers.registerHandler(suspicionMsg, true, c.healthMonitor.handleSuspicion)
	c.handlers.registerHandler(leavingMsg, true, c.healthMonitor.handleLeaving)
}

func (c *Cluster) handlePing(sender *Node, packet *Packet) error {
	ping := pingMessage{}
	if err := packet.Unmarshal(&ping); err != nil {
		return err
	}

	// Check if the ping is for us
	if ping.TargetID != c.localNode.ID {
		return nil
	}

	// If we don't know the sender then generate a temporary node and add it to our list of peers
	if sender == nil {
		sender = newNode(packet.SenderID, ping.FromAddr)
		c.nodes.addIfNotExists(sender)
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
	var err error

	ping := indirectPingMessage{}
	if err = packet.Unmarshal(&ping); err != nil {
		return err
	}

	// Create a temporary node for the target
	targetNode := newNode(ping.TargetID, ping.AdvertisedAddr)
	ping.Ok, err = c.healthMonitor.pingNode(targetNode)

	// If we don't know the sender then generate a temporary node and add it to our list of peers
	if sender == nil {
		sender = newNode(packet.SenderID, ping.FromAddr)
		c.nodes.addIfNotExists(sender)
	}

	// Respond to the sender with the ping acknowledgment
	err = c.sendMessageTo(TransportBestEffort, sender, 1, indirectPingAckMsg, &ping)
	if err != nil {
		return err
	}

	// If we got a good ping from the node then test if we know about it, if not we'll add it to our list of peers
	if ping.Ok && c.nodes.get(ping.TargetID) == nil {
		// Add the node to our list of peers
		c.nodes.addIfNotExists(targetNode)

		// Gossip the node to our peers
		join := joinMessage{
			ID:             ping.TargetID,
			AdvertisedAddr: ping.AdvertisedAddr,
		}
		packet, err := c.createPacket(c.localNode.ID, nodeJoiningMsg, 1, &join)
		if err != nil {
			return err
		}
		c.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{c.localNode.ID, ping.TargetID, packet.SenderID})
	}

	return nil
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

	// Check add the peer to our list of known peers unless it already exists
	node := newNode(joinMsg.ID, joinMsg.AdvertisedAddr)
	c.nodes.addOrUpdate(node)

	// Gossip the node to our peers
	packet.MessageType = nodeJoiningMsg
	c.enqueuePacketForBroadcast(packet, TransportBestEffort, []NodeID{c.localNode.ID, packet.SenderID})

	// Respond to the sender with our information
	selfJoinMsg := joinMessage{
		ID:             c.localNode.ID,
		AdvertisedAddr: c.localNode.advertisedAddr,
	}

	return nodeJoinAckMsg, &selfJoinMsg, nil
}

func (c *Cluster) handleJoining(sender *Node, packet *Packet) error {
	var joinMsg joinMessage

	err := packet.Unmarshal(&joinMsg)
	if err != nil {
		return err
	}

	node := newNode(joinMsg.ID, joinMsg.AdvertisedAddr)
	c.nodes.addOrUpdate(node)

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

	nodes := c.nodes.getRandomNodes(c.getPeerSubsetSize(c.nodes.getTotalCount(), purposeStateExchange), []NodeID{})

	var localStates []exchangeNodeState
	for _, n := range nodes {
		localStates = append(localStates, exchangeNodeState{
			ID:              n.ID,
			AdvertisedAddr:  n.advertisedAddr,
			State:           n.state,
			StateChangeTime: n.stateChangeTime.UnixNano(),
		})
	}

	go c.healthMonitor.combineRemoteNodeState(sender, peerStates)

	return pushPullStateAckMsg, &localStates, nil
}
