package gossip

import (
	"net"
	"net/http"

	"github.com/google/uuid"
)

type NodeID uuid.UUID

func (n NodeID) String() string {
	return uuid.UUID(n).String()
}

// Indicates why we're selecting peers, allowing for purpose-specific optimizations
type peerSelectionPurpose int

const (
	purposeBroadcast     peerSelectionPurpose = iota // General message broadcast
	purposeStateExchange                             // Exchange node states
	purposeIndirectPing                              // Indirect ping requests
	purposeTTL                                       // TTL calculation
)

// EventListener defines the interface for receiving events
type EventListener interface {
	OnInit(cluster *Cluster)
	OnNodeJoined(node *Node)
	OnNodeLeft(node *Node)
	OnNodeDead(node *Node)
	OnNodeStateChanged(node *Node, prevState NodeState)
	OnNodeMetadataChanged(node *Node)
}

// Used when a node joins to check if the application version is compatible
type ApplicationVersionCheck interface {
	CheckVersion(version string) bool
}

// Interface for decoupling the message serialization and deserialization
type MsgCodec interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

type CompressionCodec interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

// Provider defines an interface for WebSocket implementation providers
type WebsocketProvider interface {
	// DialWebsocket connects to a WebSocket server and returns a net.Conn adapter
	DialWebsocket(url string) (net.Conn, error)

	// UpgradeHTTPToWebsocket upgrades an HTTP connection to WebSocket
	UpgradeHTTPToWebsocket(w http.ResponseWriter, r *http.Request) (net.Conn, error)
}

// WSConn extends net.Conn with WebSocket-specific information
type WSConn interface {
	net.Conn
	IsSecure() bool
}
