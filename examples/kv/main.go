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

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create cluster")
	}
	cluster.Start()
	defer cluster.Stop()

	// Create KV store
	store := NewKVStore(cluster)
	defer store.Stop()

	// Join the cluster
	err = cluster.Join(peers)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to join cluster")
	}

	// Trigger a full sync
	go store.RequestFullSync()

	// Handle CLI input
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "set",
		HelpText: "set <key> <value>        - Set a value in the store",
		Handler: func(c *gossip.Cluster, args []string) {
			if len(args) < 3 {
				fmt.Println("Usage: set <key> <value>")
				return
			}

			store.Set(args[1], strings.Join(args[2:], " "))
		},
	})
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "get",
		HelpText: "get <key>                - Get a value from the store",
		Handler: func(c *gossip.Cluster, args []string) {
			if len(args) < 2 {
				fmt.Println("Usage: get <key>")
				return
			}

			value, ok := store.Get(args[1])
			if !ok {
				fmt.Println("Error getting value: key not found")
				return
			}
			fmt.Println("Value:", value)
		},
	})
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "delete",
		HelpText: "delete <key>             - Delete a value from the store",
		Handler: func(c *gossip.Cluster, args []string) {
			if len(args) < 2 {
				fmt.Println("Usage: delete <key>")
				return
			}

			store.Delete(args[1])
			fmt.Println("Key deleted:", args[1])
		},
	})
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "keys",
		HelpText: "keys                     - Get all keys from the store",
		Handler: func(c *gossip.Cluster, args []string) {
			// Get all keys
			keys := store.Keys()
			fmt.Printf("Current keys: %v\n", keys)

			// Dump the current state as JSON
			json, _ := store.DumpJSON()
			fmt.Printf("Current state:\n%s\n", json)
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
