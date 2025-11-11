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
	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/compression"
	"github.com/paularlott/gossip/encryption"
	"github.com/paularlott/gossip/examples/common"
)

const (
	TaggedMsgType gossip.MessageType = gossip.UserMsg + 1
)

type TaggedMessage struct {
	From    string `msgpack:"from" json:"from"`
	Message string `msgpack:"msg" json:"msg"`
	Tag     string `msgpack:"tag" json:"tag"`
}

func main() {
	// Configure logging
	common.Configure("info", "console", os.Stderr)

	port := flag.Int("port", 0, "Port to listen on")
	peersArg := flag.String("peers", "", "Comma separated list of peers to connect to, e.g. 127.0.0.1:8001,127.0.0.1:8002")
	nodeID := flag.String("node-id", "", "Node ID to use (optional, will be generated if not provided)")
	tagsArg := flag.String("tags", "", "Comma separated list of tags for this node, e.g. taga,tagb")
	flag.Parse()

	// Parse peers
	var peers []string
	if *peersArg != "" {
		peers = strings.Split(*peersArg, ",")
	}

	// Parse tags
	var tags []string
	if *tagsArg != "" {
		tags = strings.Split(*tagsArg, ",")
	}

	// Create the advertise address
	advertiseAddr := fmt.Sprintf("127.0.0.1:%d", *port)
	bindAddr := fmt.Sprintf("127.0.0.1:%d", *port)

	// Build configuration
	config := gossip.DefaultConfig()
	config.NodeID = *nodeID
	config.BindAddr = bindAddr
	config.AdvertiseAddr = advertiseAddr
	config.Tags = tags
	config.BearerToken = "my-secret-token"
	config.EncryptionKey = []byte("1234567890123456")
	config.Cipher = encryption.NewAESEncryptor()
	config.Logger = common.GetLogger()
	config.MsgCodec = codec.NewShamatonMsgpackCodec()
	config.Compressor = compression.NewSnappyCompressor()
	config.Transport = gossip.NewSocketTransport(config)
	config.ApplicationVersion = "0.0.1"

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		common.WithError(err).Fatal("Failed to create cluster")
	}

	// Register message handler for tagged messages
	err = cluster.HandleFunc(TaggedMsgType, func(sender *gossip.Node, packet *gossip.Packet) error {
		var msg TaggedMessage
		if err := packet.Unmarshal(&msg); err != nil {
			return err
		}

		fmt.Printf("\n[RECEIVED] From: %s | Tag: %s | Message: %s\n", msg.From, msg.Tag, msg.Message)
		fmt.Print("> ")
		return nil
	})
	if err != nil {
		common.WithError(err).Fatal("Failed to register message handler")
	}

	cluster.Start()
	defer cluster.Stop()

	// Join the cluster
	err = cluster.Join(peers)
	if err != nil {
		common.WithError(err).Fatal("Failed to join cluster")
	}

	// Print node info
	fmt.Printf("\n=== Node Started ===\n")
	fmt.Printf("Node ID: %s\n", cluster.LocalNode().ID.String())
	fmt.Printf("Address: %s\n", advertiseAddr)
	fmt.Printf("Tags: %v\n", tags)
	fmt.Printf("====================\n\n")

	// Start interactive CLI
	go func() {
		time.Sleep(2 * time.Second) // Give cluster time to stabilize

		fmt.Println("Commands:")
		fmt.Println("  send <tag> <message>  - Send a tagged message")
		fmt.Println("  nodes                 - List all nodes")
		fmt.Println("  nodes <tag>           - List nodes with specific tag")
		fmt.Println("  quit                  - Exit")
		fmt.Print("> ")

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			input := strings.TrimSpace(scanner.Text())

			if input == "" {
				fmt.Print("> ")
				continue
			}

			switch {
			case strings.HasPrefix(input, "send "):
				parts := strings.SplitN(strings.TrimPrefix(input, "send "), " ", 2)
				if len(parts) < 2 {
					fmt.Println("Usage: send <tag> <message>")
					fmt.Print("> ")
					continue
				}

				tag := parts[0]
				message := parts[1]

				msg := TaggedMessage{
					From:    cluster.LocalNode().ID.String()[:8],
					Message: message,
					Tag:     tag,
				}

				err := cluster.SendTagged(tag, TaggedMsgType, msg)
				if err != nil {
					fmt.Printf("Error sending message: %v\n", err)
				} else {
					fmt.Printf("Sent tagged message to nodes with tag '%s'\n", tag)
				}
				fmt.Print("> ")

			case input == "nodes":
				nodes := cluster.AliveNodes()
				fmt.Printf("\nAll Alive Nodes (%d):\n", len(nodes))
				for _, node := range nodes {
					isLocal := ""
					if node.ID == cluster.LocalNode().ID {
						isLocal = " (local)"
					}
					fmt.Printf("  - %s: %s | Tags: %v%s\n",
						node.ID.String()[:8],
						node.AdvertisedAddr(),
						node.GetTags(),
						isLocal)
				}
				fmt.Println()
				fmt.Print("> ")

			case strings.HasPrefix(input, "nodes "):
				tag := strings.TrimPrefix(input, "nodes ")
				nodes := cluster.GetNodesByTag(tag)
				fmt.Printf("\nNodes with tag '%s' (%d):\n", tag, len(nodes))
				for _, node := range nodes {
					isLocal := ""
					if node.ID == cluster.LocalNode().ID {
						isLocal = " (local)"
					}
					fmt.Printf("  - %s: %s | Tags: %v%s\n",
						node.ID.String()[:8],
						node.AdvertisedAddr(),
						node.GetTags(),
						isLocal)
				}
				fmt.Println()
				fmt.Print("> ")

			case input == "quit":
				fmt.Println("Exiting...")
				os.Exit(0)

			default:
				fmt.Println("Unknown command. Type 'send <tag> <message>', 'nodes', 'nodes <tag>', or 'quit'")
				fmt.Print("> ")
			}
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
}
