package gossip

import (
	"fmt"
	"math"
)

func (c *Cluster) registerSystemHandlers() {
	c.handlers.registerHandlerWithReply(nodeJoinMsg, c.handleJoin)
	c.handlers.registerHandler(nodeLeaveMsg, c.handleNodeLeave)
	c.handlers.registerHandlerWithReply(pushPullStateMsg, c.handlePushPullState)
	c.handlers.registerHandler(metadataUpdateMsg, c.handleMetadataUpdate)
	c.handlers.registerHandlerWithReply(pingMsg, c.handlePing)
}

func (c *Cluster) handleJoin(sender *Node, packet *Packet) (interface{}, error) {
	var node *Node
	var joinMsg joinMessage

	err := packet.Unmarshal(&joinMsg)
	if err != nil {
		return nil, err
	}

	c.logger.Trace("handleJoin", "sender_id", joinMsg.ID.String())

	// Check the protocol version and application version, reject if not compatible
	accepted := true
	rejectReason := ""
	if joinMsg.ID == c.localNode.ID {
		accepted = false
		rejectReason = "unable to join self"
	} else if joinMsg.ProtocolVersion != c.localNode.ProtocolVersion {
		accepted = false
		rejectReason = fmt.Sprintf("incompatible protocol version: %d != %d", joinMsg.ProtocolVersion, c.localNode.ProtocolVersion)
	} else if c.config.ApplicationVersionCheck != nil && !c.config.ApplicationVersionCheck(joinMsg.ApplicationVersion) {
		accepted = false
		rejectReason = fmt.Sprintf("incompatible application version: %s", joinMsg.ApplicationVersion)
	} else if sender == nil {
		// Check add the peer to our list of known peers unless it already exists
		node = newNode(joinMsg.ID, joinMsg.AdvertiseAddr)
		node.ProtocolVersion = joinMsg.ProtocolVersion
		node.ApplicationVersion = joinMsg.ApplicationVersion
		node.observedState = joinMsg.State

		node = c.nodes.addIfNotExists(node)
	} else {
		node = sender
		c.nodes.updateState(node.ID, joinMsg.State)
	}

	if node != nil {
		node.metadata.update(joinMsg.Metadata, joinMsg.MetadataTimestamp, false)
	}

	reply := &joinReplyMessage{
		Accepted:          accepted,
		RejectReason:      rejectReason,
		NodeID:            c.localNode.ID,
		AdvertiseAddr:     c.localNode.advertiseAddr,
		MetadataTimestamp: c.localNode.metadata.GetTimestamp(),
		Metadata:          c.localNode.metadata.GetAll(),
		Nodes:             []joinNode{},
	}

	if accepted {
		// Get a random selection of nodes
		nodes := c.nodes.getRandomNodesInStates(c.calculateJoinResponseSize(c.nodes.getAliveCount()), []NodeState{NodeAlive}, []NodeID{c.localNode.ID, joinMsg.ID})
		for _, n := range nodes {
			reply.Nodes = append(reply.Nodes, joinNode{
				ID:            n.ID,
				AdvertiseAddr: n.advertiseAddr,
			})
		}
	}

	return reply, nil
}

func (c *Cluster) calculateJoinResponseSize(totalAlive int) int {
	switch {
	case totalAlive <= 5:
		return totalAlive // Return all nodes in tiny clusters
	case totalAlive <= 20:
		return int(math.Ceil(float64(totalAlive) * 0.8)) // 80% of nodes
	case totalAlive <= 100:
		return 20 // Cap at 20 for medium clusters
	default:
		return 25 // Cap at 25 for large clusters
	}
}

func (c *Cluster) handleNodeLeave(sender *Node, packet *Packet) error {
	c.logger.Trace("handleNodeLeave", "sender_id", sender.ID.String())

	// Update the node's state to leaving
	if c.nodes.updateState(sender.ID, NodeLeaving) {
		c.logger.Debug("node is leaving the cluster", "nodeId", sender.ID.String())
	}

	return nil
}

func (c *Cluster) handlePushPullState(sender *Node, packet *Packet) (interface{}, error) {
	c.logger.Trace("handlePushPullState", "sender_id", sender.ID.String())

	var peerStates []exchangeNodeState
	err := packet.Unmarshal(&peerStates)
	if err != nil {
		return nil, err
	}

	c.combineStates(peerStates)

	// Get a random selection of nodes
	nodes := c.nodes.getRandomNodesInStates(
		c.CalcPayloadSize(c.nodes.getAliveCount()+c.nodes.getLeavingCount()+c.nodes.getSuspectCount()),
		[]NodeState{NodeAlive, NodeSuspect, NodeLeaving},
		[]NodeID{sender.ID},
	)

	var localStates []exchangeNodeState
	for _, n := range nodes {
		localStates = append(localStates, exchangeNodeState{
			ID:             n.ID,
			AdvertiseAddr:  n.advertiseAddr,
			State:          n.observedState,
			StateTimestamp: n.observedStateTime,
		})
	}

	return &localStates, nil
}

func (c *Cluster) handleMetadataUpdate(sender *Node, packet *Packet) error {
	var metadataUpdate metadataUpdateMessage
	err := packet.Unmarshal(&metadataUpdate)
	if err != nil {
		return err
	}

	if sender.metadata.update(metadataUpdate.Metadata, metadataUpdate.MetadataTimestamp, false) {
		c.nodes.notifyMetadataChanged(sender)
	}

	if metadataUpdate.NodeState != sender.observedState {
		c.logger.Debug("updating node state via metadata", "node_id", sender.ID.String(), "old_state", sender.observedState, "new_state", metadataUpdate.NodeState)
		c.nodes.updateState(sender.ID, metadataUpdate.NodeState)
	}

	return nil
}

func (c *Cluster) handlePing(sender *Node, packet *Packet) (interface{}, error) {
	var pingMsg pingMessage
	err := packet.Unmarshal(&pingMsg)
	if err != nil {
		return nil, err
	}

	c.logger.Trace("handlePing", "sender_id", pingMsg.SenderID.String())

	// If we don't know the sender, add them to our cluster view
	if sender == nil {
		req := &joinRequest{
			nodeAddr: pingMsg.AdvertiseAddr,
		}

		select {
		case c.joinQueue <- req:
			c.logger.Trace("added unknown node from ping", "sender_id", pingMsg.SenderID.String())
		default:
			c.logger.Warn("join queue full, dropping join request for node", "peer_id", pingMsg.SenderID.String(), "address", pingMsg.AdvertiseAddr)
		}
	}

	return &pongMessage{
		NodeID:            c.localNode.ID,
		AdvertiseAddr:     c.localNode.advertiseAddr,
		NodeState:         c.localNode.observedState,
		MetadataTimestamp: c.localNode.metadata.GetTimestamp(),
		Metadata:          c.localNode.metadata.GetAll(),
	}, nil
}
