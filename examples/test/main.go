package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/paularlott/gossip"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type MyListener struct{}

func (l *MyListener) OnInit(cluster *gossip.Cluster) {
	fmt.Println("MyListener: Cluster init")
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

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC822})
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	port := flag.Int("port", 8000, "Port to listen on")
	peersArg := flag.String("peers", "", "Comma separated list of peers to connect to, e.g. 127.0.0.1:8001,127.0.0.1:8002")
	flag.Parse()

	// Parse peers
	var peers []string
	if *peersArg != "" {
		peers = strings.Split(*peersArg, ",")
	}

	config := gossip.DefaultConfig()
	config.BindAddr = fmt.Sprintf("127.0.0.1:%d", *port)
	config.AdvertiseAddr = ""
	config.EncryptionKey = "1234567890123456"
	config.EventListener = &MyListener{}

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create cluster")
	}
	defer func() {
		cluster.Leave()
		cluster.Stop()
	}()

	err = cluster.Join(peers)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to join cluster")
	}

	// Handle CLI input
	fmt.Printf("Cluster started local node ID %s\n\n", cluster.GetLocalNode().ID.String())
	go handleCLIInput(cluster)

	// Wait for termination signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fmt.Println("Shutting down...")
}

func handleCLIInput(c *gossip.Cluster) {
	// Simple command line interface to set and get values
	fmt.Println("Enter commands: set key value, get key, or peers. Type Ctl+C to exit.")
	for {
		var cmd, key, value string
		fmt.Print("> ")
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		parts := strings.SplitN(input, " ", 3)
		cmd = strings.ToLower(parts[0])

		if len(parts) > 1 {
			key = parts[1]
		}

		if len(parts) > 2 {
			value = parts[2]
		}

		fmt.Println("Command:", cmd)
		fmt.Println("Key:", key)
		fmt.Println("Value:", value)

		switch cmd {
		/* 		case "SET":
		   			if key == "" || value == "" {
		   				fmt.Println("Usage: SET key value")
		   				continue
		   			}
		   			n.Set(key, value)
		   			fmt.Println("Value set")

		   		case "GET":
		   			if key == "" {
		   				fmt.Println("Usage: GET key")
		   				continue
		   			}
		   			value, exists := n.Get(key)
		   			if !exists {
		   				fmt.Println("Key not found")
		   			} else {
		   				fmt.Println(value)
		   			} */

		case "peers":
			// TODO Show if connected or not
			peers := c.GetAllNodes()
			for _, p := range peers {
				// Get the last state update in local time
				//lastSeen := time.Unix(0, p.LastStateUpdate*int64(time.Millisecond))
				//fmt.Printf("Peer ID: %s, State: %s, Date: %s, Direct: %t\n", p.ID, p.State.String(), lastSeen.Format(time.RFC3339), p.IsDirect)
				fmt.Printf("Node ID: %s, Advertised: %s, State: %s\n", p.ID, p.GetAdvertisedAddr(), p.GetState().String())
			}

		default:
			fmt.Println("Unknown command")
		}
	}
}
