package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec"
)

func main() {
	// Parse command line arguments
	bindAddr := flag.String("bind", "127.0.0.1:8000", "Address to bind to")
	peersStr := flag.String("peers", "", "Comma-separated list of peer addresses")
	flag.Parse()

	var peers []string
	if *peersStr != "" {
		peers = strings.Split(*peersStr, ",")
	}

	// Create configuration
	config := gossip.DefaultConfig()
	config.BindAddr = *bindAddr
	config.MsgCodec = codec.NewJsonCodec()
	config.Transport = gossip.NewSocketTransport(config)
	
	// Configure health monitoring with faster intervals for demo
	config.HealthCheckInterval = 2 * time.Second
	config.SuspectTimeout = 1 * time.Second
	config.SuspectRetryInterval = 500 * time.Millisecond
	config.DeadNodeTimeout = 5 * time.Second

	// Create and start the cluster
	cluster, err := gossip.NewCluster(config)
	if err != nil {
		log.Fatalf("Failed to create cluster: %v", err)
	}

	cluster.Start()
	defer cluster.Stop()

	// Register node state change handler to monitor health events
	cluster.HandleNodeStateChangeFunc(func(node *gossip.Node, newState gossip.NodeState) {
		fmt.Printf("[%s] Node %s: -> %s\n", 
			time.Now().Format("15:04:05"), 
			node.ID.String()[:8], 
			newState.String())
	})

	fmt.Printf("Health monitoring demo started on %s\n", config.BindAddr)
	fmt.Printf("Node ID: %s\n", cluster.LocalNode().ID.String())
	fmt.Printf("Health check interval: %v\n", config.HealthCheckInterval)
	fmt.Printf("Suspect timeout: %v\n", config.SuspectTimeout)

	// Join existing cluster if peers provided
	if len(peers) > 0 {
		fmt.Printf("Joining cluster via peers: %v\n", peers)
		err = cluster.Join(peers)
		if err != nil {
			fmt.Printf("Warning: %v\n", err)
		}
	}

	// Print cluster status periodically
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				alive := cluster.NumAliveNodes()
				suspect := cluster.NumSuspectNodes()
				dead := cluster.NumDeadNodes()
				
				fmt.Printf("[%s] Cluster status: %d alive, %d suspect, %d dead\n",
					time.Now().Format("15:04:05"), alive, suspect, dead)
					
				if alive > 1 || suspect > 0 || dead > 0 {
					fmt.Printf("  Nodes:\n")
					for _, node := range cluster.Nodes() {
						if node.ID != cluster.LocalNode().ID {
							fmt.Printf("    %s: %s\n", node.ID.String()[:8], node.GetObservedState().String())
						}
					}
				}
			}
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
}