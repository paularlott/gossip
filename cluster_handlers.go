package gossip

import (
	"fmt"
	"net"
	"time"
)

func (c *Cluster) registerSystemHandlers() {
	c.handlers.registerHandler(pingMsg, false, c.handlePing)
	c.handlers.registerHandler(pingAckMsg, false, c.handlePingAck)
	c.handlers.registerHandler(indirectPingMsg, false, c.handleIndirectPing)
	c.handlers.registerHandler(indirectPingAckMsg, false, c.handleIndirectPingAck)

	c.handlers.registerConnHandler(nodeJoinMsg, c.handleJoin)
	c.handlers.registerConnHandler(pushPullStateMsg, c.handlePushPullState)
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
	return c.transport.sendMessage(TransportBestEffort, sender, c.localNode.ID, pingAckMsg, 1, &ping)
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

	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	ping := indirectPingMessage{}
	if err = packet.Unmarshal(&ping); err != nil {
		return err
	}

	// Create a temporary node for the target
	targetNode := newNode(ping.TargetID, ping.AdvertisedAddr)
	ping.Ok, err = c.healthMonitor.pingNode(targetNode)

	// Respond to the sender with the ping acknowledgment
	err = c.transport.sendMessage(TransportBestEffort, sender, c.localNode.ID, indirectPingAckMsg, 1, &ping)
	if err != nil {
		return err
	}

	// If we got a good ping from the node then test if we know about it, if not we'll add it to our list of peers
	if ping.Ok && c.nodes.get(ping.TargetID) == nil {
		// Add the node to our list of peers
		c.nodes.addIfNotExists(targetNode)

		// TODO if added then gossip the node to our peers?
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

func (c *Cluster) handleJoin(conn net.Conn, sender *Node, packet *Packet) error {
	var joinMsg joinMessage

	err := packet.Unmarshal(&joinMsg)
	if err != nil {
		return err
	}

	// Respond to the sender with our information
	selfJoinMsg := joinMessage{
		ID:             c.localNode.ID,
		AdvertisedAddr: c.localNode.advertisedAddr,
	}

	// Send the peer state back to the sender
	err = c.transport.writeMessage(conn, c.localNode.ID, nodeJoinAckMsg, &selfJoinMsg)
	if err != nil {
		return err
	}

	// Check add the peer to our list of known peers unless it already exists
	node := newNode(joinMsg.ID, joinMsg.AdvertisedAddr)
	c.nodes.addOrUpdate(node)

	// TODO Gossip the new node to our peers

	return nil
}

func (c *Cluster) handlePushPullState(conn net.Conn, sender *Node, packet *Packet) error {
	if sender == nil {
		return fmt.Errorf("unknown sender")
	}

	var peerStates []pushPullState

	err := packet.Unmarshal(&peerStates)
	if err != nil {
		return err
	}

	fmt.Println("Received push pull state from", sender.ID, "with", len(peerStates), "states")

	// TODO Finish this

	nodes := c.nodes.getRandomNodes(10, []NodeID{sender.ID})
	fmt.Println("Random nodes:", nodes)
	fmt.Println("to get ", c.nodes.getLiveCount(), c.getPeerSubsetSize(c.nodes.getLiveCount(), c.config.StatePushPullMultiplier), "nodes")

	var localStates []pushPullState
	for _, n := range nodes {
		localStates = append(localStates, pushPullState{
			ID:              n.ID,
			AdvertisedAddr:  n.advertisedAddr,
			State:           n.state,
			LastStateUpdate: time.Now().UnixNano(), // TODO this needs to be from the state
		})
	}

	err = c.transport.writeMessage(conn, c.localNode.ID, pushPullStateAckMsg, &localStates)
	if err != nil {
		return err
	}

	// TODO merge states

	return nil

	/*
		 	// Return our list of peers
			selfPeerStates := cluster.getPeerStateList()
			err = cluster.transport.WriteMessage(conn, cluster.localPeer.ID, pushPullStateAckMsg, &selfPeerStates)
			if err != nil {
				log.Error().Err(err).Msg("Failed to send push state message")
				return
			}

			// Combine the peer states with the current node's state
			cluster.combinePeerList(peerStates)
	*/
}
