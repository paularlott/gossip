package gossip

import (
	"time"

	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/compression"
	"github.com/paularlott/gossip/websocket"
)

type Config struct {
	NodeID   string // NodeID is the unique identifier for the node in the cluster, "" to generate a new one
	BindAddr string // BindAddr is the address and port to bind to
	// AdvertiseAddr is the address and port to advertise to other nodes, if this is given as a domain name
	// it will be resolved to an IP address and used as the advertised address
	// If this is prefixed with srv+ then a SRV record will be used to resolve the address to an IP and port
	// If not given the BindAddr will be used.
	AdvertiseAddr                 string
	ApplicationVersion            string                  // ApplicationVersion is the version of the application, used for compatibility checks
	DefaultPort                   int                     // DefaultPort is the default port to use for the node
	EncryptionKey                 string                  // Encryption key for the messages, must be either 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.
	Transport                     Transport               // Transport layer to communicate over UDP or TCP
	Logger                        Logger                  // Logger is the logger to use for logging messages
	MsgCodec                      codec.Serializer        // The codec to use for encoding and decoding messages
	Compressor                    compression.Codec       // The codec to use for compressing and decompressing messages, if not given messages will not be compressed
	CompressMinSize               int                     // The minimum size of a message before attempting to compress it
	WebsocketProvider             websocket.Provider      // The provider to use for WebSocket connections
	SocketTransportEnabled        bool                    // Whether to use the socket transport layer
	ApplicationVersionCheck       ApplicationVersionCheck // The application version check to use for checking compatibility with other nodes
	TCPDialTimeout                time.Duration           // TCPDialTimeout is the duration to wait for a TCP connection to be established
	TCPDeadline                   time.Duration           // TCPDeadline is the duration to wait for a TCP operation to complete
	UDPDeadline                   time.Duration           // UDPDeadline is the duration to wait for a UDP operation to complete
	UDPMaxPacketSize              int                     // UDPMaxSize is the maximum size of a UDP packet in bytes
	TCPMaxPacketSize              int                     // TCPMaxSize is the maximum size of a TCP packet in bytes
	MsgHistoryGCInterval          time.Duration           // MsgHistoryGCInterval is the duration between garbage collection operations
	MsgHistoryMaxAge              time.Duration           // MsgHistoryMaxAge is the maximum age of a message in the history
	MsgHistoryShardCount          int                     // MessageHistoryShardCount is the number of shards to use for storing message history, 16 for up to 50 nodes, 32 for up to 500 nodes and 64 for larger clusters.
	NodeShardCount                int                     // NodeShardCount is the number of shards to use for storing node information, 4 for up to 50 nodes, 16 for up to 500 nodes and 32 for larger clusters.
	NumSendWorkers                int                     // The number of workers to use for sending messages
	SendQueueSize                 int                     // SendQueueSize is the size of the send queue
	NumIncomingWorkers            int                     // The number of workers to use for processing incoming messages
	IncomingPacketQueueDepth      int                     // Depth of the queue for incoming packets
	HealthCheckInterval           time.Duration           // How often to perform health checks
	HealthCheckSampleSize         int                     // Number of random nodes to check each interval
	ActivityThresholdPercent      float64                 // Percentage of activity threshold for a node to be considered alive, multiplied with HealthCheckInterval
	SuspectThreshold              int                     // Number of consecutive failures before marking suspect
	SuspectTimeout                time.Duration           // How long a node can be suspect before final check
	DeadNodeTimeout               time.Duration           // How long to keep dead nodes before removal
	RefutationThreshold           int                     // Number of peers refuting suspicion to restore node
	EnableIndirectPings           bool                    // Whether to use indirect pings
	PingTimeout                   time.Duration           // Timeout for ping operations, should be less than HealthCheckInterval
	MaxParallelSuspectEvaluations int                     // Max number of parallel evaluations for suspect nodes
	StateSyncInterval             time.Duration           // How often to perform state synchronization with peers
	BroadcastMultiplier           float64                 // Scale of peer sampling for broadcast messages
	StateExchangeMultiplier       float64                 // Scale of peer sampling for state exchange messages
	IndirectPingMultiplier        float64                 // Scale of peer sampling for indirect ping messages
	TTLMultiplier                 float64                 // Multiplier for TTL, used to determine how many hops a message can take
	EventListener                 EventListener           // Event listener for cluster membership changes
}

func DefaultConfig() *Config {
	return &Config{
		BindAddr:                      "127.0.0.1:8000",
		AdvertiseAddr:                 "",
		DefaultPort:                   3500,
		CompressMinSize:               256,
		SocketTransportEnabled:        true,
		TCPDialTimeout:                5 * time.Second,
		TCPDeadline:                   5 * time.Second,
		UDPDeadline:                   5 * time.Second,
		UDPMaxPacketSize:              1400,
		TCPMaxPacketSize:              4194304, // 4MB
		MsgHistoryGCInterval:          1 * time.Second,
		MsgHistoryMaxAge:              30 * time.Second,
		MsgHistoryShardCount:          16,
		NodeShardCount:                4,
		NumSendWorkers:                8,
		SendQueueSize:                 512,
		NumIncomingWorkers:            8,
		IncomingPacketQueueDepth:      512,
		HealthCheckInterval:           1 * time.Second,
		HealthCheckSampleSize:         7,
		ActivityThresholdPercent:      0.5,
		SuspectThreshold:              3,
		SuspectTimeout:                15 * time.Second,
		DeadNodeTimeout:               1 * time.Minute,
		RefutationThreshold:           2,
		EnableIndirectPings:           true,
		PingTimeout:                   500 * time.Millisecond,
		MaxParallelSuspectEvaluations: 4,
		StateSyncInterval:             30 * time.Second,
		BroadcastMultiplier:           1,
		StateExchangeMultiplier:       0.8,
		IndirectPingMultiplier:        0.7,
		TTLMultiplier:                 1.0,
		EventListener:                 nil,
	}
}
