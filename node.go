package gossip

import "time"

type NodeState uint8

const (
	nodeAlive NodeState = iota
	nodeLeaving
	nodeDead
	nodeSuspect
)

func (ns NodeState) String() string {
	switch ns {
	case nodeAlive:
		return "Alive"
	case nodeLeaving:
		return "Leaving"
	case nodeDead:
		return "Dead"
	case nodeSuspect:
		return "Suspect"
	default:
		return "Unknown"
	}
}

type Node struct {
	ID              NodeID
	advertisedAddr  string
	connectAddr     *Address
	stateChangeTime time.Time
	state           NodeState
}

func newNode(id NodeID, advertisedAddr string) *Node {
	return &Node{
		ID:              id,
		advertisedAddr:  advertisedAddr,
		connectAddr:     nil,
		stateChangeTime: time.Now(),
		state:           nodeAlive,
	}
}

func (node *Node) ResolveConnectAddr() (Address, error) {

	// If we've already resolved the address, return it
	if node.connectAddr != nil {
		return *node.connectAddr, nil
	}

	// Resolve the address
	addr, err := ResolveAddress(node.advertisedAddr)
	if err != nil {
		return Address{}, err
	}

	// Store the resolved address and update the last lookup time
	node.connectAddr = &addr

	return addr, nil
}

func (node *Node) GetState() NodeState {
	return node.state
}

func (node *Node) GetAdvertisedAddr() string {
	return node.advertisedAddr
}
