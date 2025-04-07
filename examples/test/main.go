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
	"github.com/paularlott/gossip/websocket"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	GossipMsg gossip.MessageType = gossip.UserMsg + iota // User message
)

type AppVersionCheck struct{}

func (av *AppVersionCheck) CheckVersion(version string) bool {
	fmt.Println("Checking application version:", version)
	return true
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

	// Create the bind address
	bindAddr := ""
	if *port > 0 {
		bindAddr = fmt.Sprintf("127.0.0.1:%d", *port)
	}
	if *webPort > 0 {
		if bindAddr != "" {
			bindAddr += "|"
		}
		bindAddr += fmt.Sprintf("ws://127.0.0.1:%d", *webPort)
	}

	// Build configuration
	config := gossip.DefaultConfig()
	config.NodeID = *nodeID
	config.BindAddr = bindAddr
	config.AdvertiseAddr = ""
	config.EncryptionKey = "1234567890123456"
	config.EventListener = &MyListener{}
	config.Logger = NewZerologLogger(log.Logger)
	config.MsgCodec = codec.NewShamatonMsgpackCodec()
	config.WebsocketProvider = websocket.NewCoderProvider(5*time.Second, true, "")

	config.Compressor = compression.NewSnappyCompressor()

	config.ApplicationVersion = "0.0.1"
	config.ApplicationVersionCheck = &AppVersionCheck{}

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create cluster")
	}
	defer cluster.Shutdown()

	// Join the cluster
	err = cluster.Join(peers)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to join cluster")
	}

	// Add a handler for a custom message which can be sent by typing "gossip <message>" in the CLI
	cluster.HandleFunc(GossipMsg, func(sender *gossip.Node, packet *gossip.Packet) error {
		var msg GossipMessage
		if err := packet.Unmarshal(&msg); err != nil {
			return err
		}

		fmt.Printf("Received GossipMsg message from %s: %s\n", sender.ID, msg.Message)
		return nil
	})

	// Handle CLI input
	go handleCLIInput(cluster)

	// If web port is specified then start a web server to handle websocket traffic
	var httpServer *http.Server
	if *webPort > 0 {
		http.HandleFunc("/", cluster.WebsocketHandler)
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
