package main

import (
	"context"
	"flag"
	"fmt"
	"net"
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
	"github.com/paularlott/gossip/websocket"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	// Define a custom message type for user messages
	StartStreamMsg gossip.MessageType = gossip.UserMsg + iota // User message
	StreamDataBlock
)

type StreamStartMessage struct {
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
	if *port > 0 {
		advertiseAddr = fmt.Sprintf("127.0.0.1:%d", *port)
	} else if *webPort > 0 {
		advertiseAddr = fmt.Sprintf("ws://127.0.0.1:%d", *webPort)
	}

	// Build configuration
	config := gossip.DefaultConfig()
	config.NodeID = *nodeID
	config.BindAddr = fmt.Sprintf("127.0.0.1:%d", *port)
	config.AdvertiseAddr = advertiseAddr
	config.EncryptionKey = []byte("1234567890123456")
	config.Cipher = encryption.NewAESEncryptor()
	config.Logger = common.NewZerologLogger(log.Logger)
	config.MsgCodec = codec.NewShamatonMsgpackCodec()
	config.Compressor = compression.NewSnappyCompressor()
	config.Cipher = encryption.NewAESEncryptor()

	if *port == 0 {
		config.SocketTransportEnabled = false
	}
	if *webPort > 0 {
		config.WebsocketProvider = websocket.NewCoderProvider(5*time.Second, true, "")
		config.AllowInsecureWebsockets = true
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
	cluster.HandleStreamFunc(StartStreamMsg, func(sender *gossip.Node, packet *gossip.Packet, conn net.Conn) {
		var msg StreamStartMessage
		if err := packet.Unmarshal(&msg); err != nil {
			return
		}

		fmt.Printf("Received message from %s: %s\n", sender.ID, msg.Content)

		var d string
		err := cluster.ReadStreamMsg(conn, StreamDataBlock, &d)
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			return
		}
		fmt.Printf("Received message: %s\n", d)

		msgStr := fmt.Sprintf("Pong: %s", d)
		cluster.WriteStreamMsg(conn, StreamDataBlock, &msgStr)

		// Note: conn is closed automatically when the function returns
	})

	// Join the cluster
	err = cluster.Join(peers)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to join cluster")
	}

	// Handle CLI input
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "stream",
		HelpText: "stream <nodeid> <message>         - Send a message to the cluster",
		Handler: func(c *gossip.Cluster, args []string) {
			if len(args) < 3 {
				fmt.Println("Usage: stream <nodeid> <message>")
				return
			}

			dstNode := cluster.GetNodeByIDString(args[1])
			if dstNode == nil {
				fmt.Println("Error: Node not found")
				return
			}

			msg := StreamStartMessage{Content: "Example Stream Start Message"}
			conn, err := cluster.OpenStream(dstNode, StartStreamMsg, &msg)
			if err != nil {
				fmt.Println("Failed to open stream:", err)
				return
			}
			defer conn.Close()

			err = cluster.WriteStreamMsg(conn, StreamDataBlock, strings.Join(args[2:], " "))
			if err != nil {
				fmt.Println("Error writing to stream:", err)
				return
			}

			var d string
			err = cluster.ReadStreamMsg(conn, StreamDataBlock, &d)
			if err != nil {
				fmt.Println("Error reading from connection:", err)
				return
			}
			fmt.Printf("Cmd Received message: %s\n", d)
		},
	})
	go common.HandleCLIInput(cluster)

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
