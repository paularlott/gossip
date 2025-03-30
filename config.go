package gossip

import "time"

type Config struct {
	NodeID   string // NodeID is the unique identifier for the node in the cluster, "" to generate a new one
	BindAddr string // BindAddr is the address and port to bind to
	// AdvertiseAddr is the address and port to advertise to other nodes, if this is given as a domain name
	// it will be resolved to an IP address and used as the advertised address
	// If this is prefixed with srv+ then a SRV record will be used to resolve the address to an IP and port
	// If not given the BindAddr will be used.
	AdvertiseAddr                 string
	EncryptionKey                 string        // Encryption key for the messages, must be either 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.
	TCPDialTimeout                time.Duration // TCPDialTimeout is the duration to wait for a TCP connection to be established
	TCPDeadline                   time.Duration // TCPDeadline is the duration to wait for a TCP operation to complete
	UDPDeadline                   time.Duration // UDPDeadline is the duration to wait for a UDP operation to complete
	UDPMaxPacketSize              int           // UDPMaxSize is the maximum size of a UDP packet in bytes
	TCPMaxPacketSize              int           // TCPMaxSize is the maximum size of a TCP packet in bytes
	MsgHistoryGCInterval          time.Duration // MsgHistoryGCInterval is the duration between garbage collection operations
	MsgHistoryMaxAge              time.Duration // MsgHistoryMaxAge is the maximum age of a message in the history
	MsgHistoryShardCount          int           // MessageHistoryShardCount is the number of shards to use for storing message history, 16 for up to 50 nodes, 32 for up to 500 nodes and 64 for larger clusters.
	NodeShardCount                int           // NodeShardCount is the number of shards to use for storing node information, 4 for up to 50 nodes, 16 for up to 500 nodes and 32 for larger clusters.
	StatePushPullMultiplier       float64       // StatePushPullMultiplier is the multiplier for the number of states to push/pull at a time
	NumSendWorkers                int           // The number of workers to use for sending messages
	SendQueueSize                 int           // SendQueueSize is the size of the send queue
	HealthCheckInterval           time.Duration // How often to perform health checks
	HealthCheckSampleSize         int           // Number of random nodes to check each interval
	SuspectThreshold              int           // Number of consecutive failures before marking suspect
	SuspectTimeout                time.Duration // How long a node can be suspect before final check
	DeadNodeTimeout               time.Duration // How long to keep dead nodes before removal
	RefutationThreshold           int           // Number of peers refuting suspicion to restore node
	EnableIndirectPings           bool          // Whether to use indirect pings
	MaxNodesIndirectPing          int           // Max nodes to use for indirect pings
	PingTimeout                   time.Duration // Timeout for ping operations, should be less than HealthCheckInterval
	MaxParallelSuspectEvaluations int           // Max number of parallel evaluations for suspect nodes
	TTLMultiplier                 float64       // Multiplier for TTL, used to determine how many hops a message can take
}

// MergeDefault merges the default config with the given config to ensure all fields are set
func (c *Config) MergeDefault() *Config {
	defaultConfig := DefaultConfig()
	if c.BindAddr == "" {
		c.BindAddr = defaultConfig.BindAddr
	}
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.BindAddr
	}
	if c.TCPDialTimeout == 0 {
		c.TCPDialTimeout = defaultConfig.TCPDialTimeout
	}
	if c.TCPDeadline == 0 {
		c.TCPDeadline = defaultConfig.TCPDeadline
	}
	if c.UDPDeadline == 0 {
		c.UDPDeadline = defaultConfig.UDPDeadline
	}
	if c.UDPMaxPacketSize == 0 {
		c.UDPMaxPacketSize = defaultConfig.UDPMaxPacketSize
	}
	if c.TCPMaxPacketSize == 0 {
		c.TCPMaxPacketSize = defaultConfig.TCPMaxPacketSize
	}
	if c.MsgHistoryGCInterval == 0 {
		c.MsgHistoryGCInterval = defaultConfig.MsgHistoryGCInterval
	}
	if c.MsgHistoryMaxAge == 0 {
		c.MsgHistoryMaxAge = defaultConfig.MsgHistoryMaxAge
	}
	if c.MsgHistoryShardCount == 0 {
		c.MsgHistoryShardCount = defaultConfig.MsgHistoryShardCount
	}
	if c.NodeShardCount == 0 {
		c.NodeShardCount = defaultConfig.NodeShardCount
	}
	if c.StatePushPullMultiplier == 0 {
		c.StatePushPullMultiplier = defaultConfig.StatePushPullMultiplier
	}
	if c.NumSendWorkers == 0 {
		c.NumSendWorkers = defaultConfig.NumSendWorkers
	}
	if c.SendQueueSize == 0 {
		c.SendQueueSize = defaultConfig.SendQueueSize
	}
	if c.HealthCheckInterval == 0 {
		c.HealthCheckInterval = defaultConfig.HealthCheckInterval
	}
	if c.HealthCheckSampleSize == 0 {
		c.HealthCheckSampleSize = defaultConfig.HealthCheckSampleSize
	}
	if c.SuspectThreshold == 0 {
		c.SuspectThreshold = defaultConfig.SuspectThreshold
	}
	if c.SuspectTimeout == 0 {
		c.SuspectTimeout = defaultConfig.SuspectTimeout
	}
	if c.DeadNodeTimeout == 0 {
		c.DeadNodeTimeout = defaultConfig.DeadNodeTimeout
	}
	if c.RefutationThreshold == 0 {
		c.RefutationThreshold = defaultConfig.RefutationThreshold
	}
	if c.MaxNodesIndirectPing == 0 {
		c.MaxNodesIndirectPing = defaultConfig.MaxNodesIndirectPing
	}
	if c.PingTimeout == 0 {
		c.PingTimeout = defaultConfig.PingTimeout
	}
	if c.MaxParallelSuspectEvaluations == 0 {
		c.MaxParallelSuspectEvaluations = defaultConfig.MaxParallelSuspectEvaluations
	}
	if c.TTLMultiplier == 0 {
		c.TTLMultiplier = defaultConfig.TTLMultiplier
	}

	return c
}

func DefaultConfig() *Config {
	return &Config{
		BindAddr:                      "127.0.0.1:8000",
		AdvertiseAddr:                 "",
		TCPDialTimeout:                5 * time.Second,
		TCPDeadline:                   5 * time.Second,
		UDPDeadline:                   5 * time.Second,
		UDPMaxPacketSize:              1400,
		TCPMaxPacketSize:              4194304, // 4MB
		MsgHistoryGCInterval:          2 * time.Second,
		MsgHistoryMaxAge:              30 * time.Second,
		MsgHistoryShardCount:          16,
		NodeShardCount:                4,
		StatePushPullMultiplier:       2.5,
		NumSendWorkers:                4,
		SendQueueSize:                 128,
		HealthCheckInterval:           1 * time.Second,
		HealthCheckSampleSize:         7,
		SuspectThreshold:              3,
		SuspectTimeout:                15 * time.Second,
		DeadNodeTimeout:               1 * time.Minute,
		RefutationThreshold:           2,
		EnableIndirectPings:           true,
		MaxNodesIndirectPing:          3,
		PingTimeout:                   500 * time.Millisecond,
		MaxParallelSuspectEvaluations: 4,
		TTLMultiplier:                 2,
	}
}
