package main

import (
	"fmt"

	"github.com/paularlott/gossip"
)

type GossipMessage struct {
	Message string `msgpack:"message" json:"message"`
}

type MyListener struct{}

func (l *MyListener) OnInit(cluster *gossip.Cluster) {
	fmt.Println("MyListener: Cluster init")

	cluster.LocalMetadata().SetString("location", "testing")
}

func (l *MyListener) OnNodeJoined(node *gossip.Node) {
	fmt.Printf("MyListener: Node %s joined\n", node.ID)
}

func (l *MyListener) OnNodeLeft(node *gossip.Node) {
	fmt.Printf("MyListener: Node %s left\n", node.ID)
}

func (l *MyListener) OnNodeDead(node *gossip.Node) {
	fmt.Printf("MyListener: Node %s is dead\n", node.ID)
}

func (l *MyListener) OnNodeStateChanged(node *gossip.Node, prevState gossip.NodeState) {
	fmt.Printf("MyListener: Node %s state changed from %s to %s\n", node.ID, prevState.String(), node.GetState().String())
}

func (l *MyListener) OnNodeMetadataChanged(node *gossip.Node) {
	fmt.Printf("MyListener: Node %s metadata changed\n", node.ID)
}
