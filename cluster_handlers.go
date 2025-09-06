package gossip

import (
	"fmt"
	"reflect"
)

func (c *Cluster) registerSystemHandlers() {
	c.handlers.registerHandlerWithReply(nodeJoinMsg, c.handleJoin)
	c.handlers.registerHandlerWithReply(pushPullStateMsg, c.handlePushPullState)
	c.handlers.registerHandler(metadataUpdateMsg, true, c.handleMetadataUpdate)
}

func (c *Cluster) handleJoin(sender *Node, packet *Packet) (interface{}, error) {
	var joinMsg joinMessage

	err := packet.Unmarshal(&joinMsg)
	if err != nil {
		return nil, err
	}

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
	} else {
		// Check add the peer to our list of known peers unless it already exists
		node := newNode(joinMsg.ID, joinMsg.AdvertiseAddr)
		node.ProtocolVersion = joinMsg.ProtocolVersion
		node.ApplicationVersion = joinMsg.ApplicationVersion

		if c.nodes.addOrUpdate(node) {
			node.metadata.update(joinMsg.Metadata, joinMsg.MetadataTimestamp, true)
		}
	}

	return &joinReplyMessage{
		Accepted:           accepted,
		RejectReason:       rejectReason,
		ID:                 c.localNode.ID,
		AdvertiseAddr:      c.localNode.advertiseAddr,
		MetadataTimestamp:  c.localNode.Metadata.GetTimestamp(),
		Metadata:           c.localNode.Metadata.GetAll(),
		ProtocolVersion:    c.localNode.ProtocolVersion,
		ApplicationVersion: c.localNode.ApplicationVersion,
	}, nil
}

func (c *Cluster) handlePushPullState(sender *Node, packet *Packet) (interface{}, error) {
	if sender == nil {
		return nil, fmt.Errorf("unknown sender")
	}

	var peerStates []exchangeNodeState
	err := packet.Unmarshal(&peerStates)
	if err != nil {
		return nil, err
	}

	// TODO Merge the remote states with our local states

	// Get a random selection of nodes
	nodes := c.nodes.getRandomNodesForGossip(
		c.CalcPayloadSize(c.nodes.getAliveCount()+c.nodes.getLeavingCount()+c.nodes.getSuspectCount()+c.nodes.getDeadCount()),
		[]NodeID{},
	)

	var localStates []exchangeNodeState
	for _, n := range nodes {
		localStates = append(localStates, exchangeNodeState{
			ID:                n.ID,
			AdvertiseAddr:     n.advertiseAddr,
			State:             n.state,
			StateChangeTime:   n.stateChangeTime,
			MetadataTimestamp: n.metadata.GetTimestamp(),
			Metadata:          n.metadata.GetAll(),
		})
	}

	return &localStates, nil
}

func (c *Cluster) handleMetadataUpdate(sender *Node, packet *Packet) error {
	if sender == nil {
		// Nothing to do as we don't know the node at this point
		return nil
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
		c.notifyMetadataChanged(node)
	} else {
		// If the timestamp is not newer but the data differs, accept the update when it
		// comes directly from the node itself (authoritative source). This heals cases
		// where clocks or prior incorrect timestamps prevented convergence.
		current := node.metadata.GetAll()
		if !reflect.DeepEqual(current, metadataUpdate.Metadata) {
			// Force-apply the update
			if node.metadata.update(metadataUpdate.Metadata, metadataUpdate.MetadataTimestamp, true) {
				c.notifyMetadataChanged(node)
			}
		}
	}

	return nil
}
