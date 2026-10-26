// Command partition is a small partitioned key-value server built on the gossip/partition package: every key lives in
// one partition, each partition has one owner at a time, and a write is committed once a majority of the configured
// cluster holds it. Its data is kept on disk, so a node can be killed and restarted. See README.md.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/paularlott/gossip/examples/common"
)

func main() {
	bind := flag.String("bind", "127.0.0.1:7001", "Address for gossip traffic (TCP and UDP); also what the other nodes use to reach this one")
	httpAddr := flag.String("http", "127.0.0.1:8001", "Address of the HTTP API")
	peersArg := flag.String("peers", "", "Comma separated gossip addresses of the other nodes, e.g. 127.0.0.1:7002,127.0.0.1:7003 (this node's own address may be included)")
	clusterSize := flag.Int("cluster-size", 3, "The configured number of nodes; the same on every node, and never changed while running")
	partitions := flag.Int("partitions", 16, "The number of partitions; the same on every node")
	dataDir := flag.String("data", "", "Data directory (required)")
	newCluster := flag.Bool("new-cluster", false, "This is one of the first nodes of a brand new cluster: an empty data directory is trusted. Leave it off for a node that joins an existing cluster")
	resync := flag.Bool("resync", false, "The data was lost, restored from a backup or is otherwise suspect: catch up from a majority of the others before taking part")
	nodeID := flag.String("node-id", "", "Node ID (a UUID); give each node a fixed one so that a restarted node is the same node (default: a new one every start)")
	heartbeat := flag.Duration("heartbeat", 500*time.Millisecond, "Partition heartbeat interval; the owner timeout is three times this")
	logLevel := flag.String("log-level", "info", "Log level: trace, debug, info, warn, error")
	flag.Parse()

	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "-data is required")
		flag.Usage()
		os.Exit(2)
	}
	common.Configure(*logLevel, "console", os.Stderr)

	var peers []string
	if *peersArg != "" {
		peers = strings.Split(*peersArg, ",")
	}
	srv, err := newServer(serverConfig{
		NodeID:      *nodeID,
		Bind:        *bind,
		HTTPAddr:    *httpAddr,
		Peers:       peers,
		ClusterSize: *clusterSize,
		Partitions:  *partitions,
		DataDir:     *dataDir,
		NewCluster:  *newCluster,
		Resync:      *resync,
		Heartbeat:   *heartbeat,
		Logger:      common.GetLogger(),
	})
	if err != nil {
		common.WithError(err).Fatal("Failed to start")
	}
	common.Info("Node started", "id", srv.cluster.LocalNode().ID.String(), "gossip", *bind, "http", srv.httpURL(),
		"intact", srv.store.Intact())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	srv.Close()
}
