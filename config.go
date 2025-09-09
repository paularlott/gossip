package gossip

import (
	"time"

	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/compression"
	"github.com/paularlott/gossip/encryption"
)

type Config struct {
	NodeID             string // NodeID is the unique identifier for the node in the cluster, "" to generate a new one
	BindAddr           string // BindAddr is the address and port to bind to or the path for http transports
	AdvertiseAddr      string // Advertised address and port or URL for the node
	ApplicationVersion string // ApplicationVersion is the version of the application, used for compatibility checks

	EncryptionKey            []byte                  // Encryption key for the messages, must be either 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.
	Transport                Transport               // Transport layer to communicate over UDP or TCP
	Logger                   Logger                  // Logger is the logger to use for logging messages
	MsgCodec                 codec.Serializer        // The codec to use for encoding and decoding messages
	Compressor               compression.Compressor  // The compressor to use for compressing and decompressing messages, if not given messages will not be compressed
	CompressMinSize          int                     // The minimum size of a message before attempting to compress it
	BearerToken              string                  // Bearer token to use for authentication, if not given no authentication will be used
	Cipher                   encryption.Cipher       // The cipher to use for encrypting and decrypting messages
	ApplicationVersionCheck  ApplicationVersionCheck // The application version check to use for checking compatibility with other nodes
	GossipInterval           time.Duration           // How often to send gossip messages
	GossipMaxInterval        time.Duration           // Maximum interval between gossip messages
	MetadataGossipInterval   time.Duration           // Fast metadata gossip (~500ms)
	StateGossipInterval      time.Duration           // Medium state sync (~30-60s)
	TCPDialTimeout           time.Duration           // TCPDialTimeout is the duration to wait for a TCP connection to be established
	TCPDeadline              time.Duration           // TCPDeadline is the duration to wait for a TCP operation to complete
	UDPDeadline              time.Duration           // UDPDeadline is the duration to wait for a UDP operation to complete
	UDPMaxPacketSize         int                     // UDPMaxSize is the maximum size of a UDP packet in bytes
	TCPMaxPacketSize         int                     // TCPMaxSize is the maximum size of a TCP packet in bytes
	MsgHistoryGCInterval     time.Duration           // MsgHistoryGCInterval is the duration between garbage collection operations
	MsgHistoryMaxAge         time.Duration           // MsgHistoryMaxAge is the maximum age of a message in the history
	MsgHistoryShardCount     int                     // MessageHistoryShardCount is the number of shards to use for storing message history, 16 for up to 50 nodes, 32 for up to 500 nodes and 64 for larger clusters.
	NodeShardCount           int                     // NodeShardCount is the number of shards to use for storing node information, 4 for up to 50 nodes, 16 for up to 500 nodes and 32 for larger clusters.
	NumSendWorkers           int                     // The number of workers to use for sending messages
	SendQueueSize            int                     // SendQueueSize is the size of the send queue
	NumIncomingWorkers       int                     // The number of workers to use for processing incoming messages
	IncomingPacketQueueDepth int                     // Depth of the queue for incoming packets
	PingTimeout              time.Duration           // Timeout for ping operations, should be less than HealthCheckInterval
	FanOutMultiplier         float64                 // Scale of peer count for broadcast messages
	StateExchangeMultiplier  float64                 // Scale of peer sampling for state exchange messages
	TTLMultiplier            float64                 // Multiplier for TTL, used to determine how many hops a message can take
	ForceReliableTransport   bool                    // Force all messages to use reliable transport (TCP/WebSocket)
	Resolver                 Resolver                // DNS resolver to use for address resolution, if not set uses default resolver
	PreferIPv6               bool                    // Prefer IPv6 addresses when resolving hostnames (default false = prefer IPv4)
	NodeCleanupInterval      time.Duration           // How often to run node cleanup
	NodeRetentionTime        time.Duration           // How long to keep dead nodes before removal
	LeavingNodeTimeout       time.Duration           // How long to wait before moving leaving nodes to dead
	HealthCheckInterval      time.Duration           // How often to check node health (e.g., 2s)
	SuspectTimeout           time.Duration           // Time before marking node suspect (e.g., 1.5s)
	SuspectRetryInterval     time.Duration           // How often to retry suspect nodes (e.g., 1s)
	DeadNodeTimeout          time.Duration           // Time before marking suspect node as dead (e.g., 15s)
	DeadNodeRetryInterval    time.Duration           // How often to retry dead nodes
	MaxDeadNodeRetryTime     time.Duration           // Stop retrying dead nodes after this time
	HealthWorkerPoolSize     int                     // Number of workers for health checks (e.g., 4)
	HealthCheckQueueDepth    int                     // Queue depth for health check tasks (e.g., 256)
	JoinQueueSize            int                     // Queue depth for joining tasks (e.g., 100)
	NumJoinWorkers           int                     // Number of workers for joining tasks (e.g., 2 - 3)
}

func DefaultConfig() *Config {
	return &Config{
		BindAddr:      "127.0.0.1:8000",
		AdvertiseAddr: "",

		CompressMinSize:          256,
		BearerToken:              "",
		GossipInterval:           5 * time.Second,
		GossipMaxInterval:        20 * time.Second,
		MetadataGossipInterval:   500 * time.Millisecond,
		StateGossipInterval:      45 * time.Second,
		TCPDialTimeout:           2 * time.Second,
		TCPDeadline:              2 * time.Second,
		UDPDeadline:              2 * time.Second,
		UDPMaxPacketSize:         1400,
		TCPMaxPacketSize:         4194304, // 4MB
		MsgHistoryGCInterval:     1 * time.Second,
		MsgHistoryMaxAge:         30 * time.Second,
		MsgHistoryShardCount:     16,
		NodeShardCount:           4,
		NumSendWorkers:           8,
		SendQueueSize:            512,
		NumIncomingWorkers:       8,
		IncomingPacketQueueDepth: 512,
		PingTimeout:              500 * time.Millisecond,
		FanOutMultiplier:         1,
		StateExchangeMultiplier:  0.8,
		TTLMultiplier:            1.0,
		ForceReliableTransport:   false,
		PreferIPv6:               false,
		NodeCleanupInterval:      20 * time.Second,
		NodeRetentionTime:        1 * time.Hour,
		LeavingNodeTimeout:       30 * time.Second,
		HealthCheckInterval:      2 * time.Second,
		SuspectTimeout:           1500 * time.Millisecond,
		SuspectRetryInterval:     1 * time.Second,
		DeadNodeTimeout:          15 * time.Second,
		DeadNodeRetryInterval:    30 * time.Second, // Retry dead nodes every 30s
		MaxDeadNodeRetryTime:     10 * time.Minute, // Stop trying after 10 minutes
		HealthWorkerPoolSize:     4,
		HealthCheckQueueDepth:    256,
		JoinQueueSize:            100,
		NumJoinWorkers:           3,
	}
}
