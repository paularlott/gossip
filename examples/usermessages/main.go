package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
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

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	// Define a custom message type for user messages
	GossipMsg gossip.MessageType = gossip.UserMsg + iota // User message
)

type GossipMessage struct {
	Content string `msgpack:"content" json:"content"`
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC822})
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	port := flag.Int("port", 0, "Port to listen on")
	webPort := flag.Int("web-port", 0, "Web port")
	peersArg := flag.String("peers", "", "Comma separated list of peers to connect to, e.g. 127.0.0.1:8001,127.0.0.1:8002")
	nodeID := flag.String("node-id", "", "Node ID to use (optional, will be generated if not provided)")
	flag.Parse()

	// Parse peers
	var peers []string
	if *peersArg != "" {
		peers = strings.Split(*peersArg, ",")
	}

	// Create the advertise address
	advertiseAddr := ""
	bindAddr := ""
	if *port > 0 {
		advertiseAddr = fmt.Sprintf("127.0.0.1:%d", *port)
		bindAddr = fmt.Sprintf("127.0.0.1:%d", *port)
	} else if *webPort > 0 {
		advertiseAddr = fmt.Sprintf("http://127.0.0.1:%d", *webPort)
		bindAddr = "/"
	}

	// Build configuration
	config := gossip.DefaultConfig()
	config.NodeID = *nodeID
	config.BindAddr = bindAddr
	config.AdvertiseAddr = advertiseAddr
	config.EncryptionKey = []byte("1234567890123456")
	config.Cipher = encryption.NewAESEncryptor()
	config.Logger = common.NewZerologLogger(log.Logger)
	config.MsgCodec = codec.NewShamatonMsgpackCodec()
	config.Compressor = compression.NewSnappyCompressor()

	var httpTransport *gossip.HTTPTransport
	if *webPort > 0 {
		httpTransport = gossip.NewHTTPTransport(config)
		config.Transport = httpTransport
	} else {
		config.Transport = gossip.NewSocketTransport(config)
	}

	config.ApplicationVersion = "0.0.1"
	config.ApplicationVersionCheck = func(version string) bool {
		fmt.Println("Checking application version:", version)
		return true
	}

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create cluster")
	}
	cluster.Start()
	defer cluster.Stop()

	// Register a handler for the gossip message
	cluster.HandleFunc(GossipMsg, func(sender *gossip.Node, packet *gossip.Packet) error {
		var msg GossipMessage
		if err := packet.Unmarshal(&msg); err != nil {
			return err
		}

		fmt.Printf("Received message from %s: %s\n", sender.ID, msg.Content)
		return nil
	})

	// Join the cluster
	err = cluster.Join(peers)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to join cluster")
	}

	// Handle CLI input
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "gossip",
		HelpText: "gossip <message>         - Send a message to the cluster",
		Handler: func(c *gossip.Cluster, args []string) {
			if len(args) < 2 {
				fmt.Println("Usage: gossip <message>")
				return
			}

			msg := GossipMessage{Content: strings.Join(args[1:], " ")}
			cluster.Send(GossipMsg, &msg)
		},
	})
	go common.HandleCLIInput(cluster)

	// If web port is specified then start a web server to handle websocket traffic
	var httpServer *http.Server
	if *webPort > 0 {
		http.HandleFunc("/", httpTransport.HandleGossipRequest)
		httpServer = &http.Server{Addr: fmt.Sprintf(":%d", *webPort)}
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatal().Err(err).Msg("Failed to start web server")
			}
		}()
	}

	// Wait for termination signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	// Shutdown the HTTP server if it's running
	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to gracefully shutdown HTTP server")
		}
	}
}
