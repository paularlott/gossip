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
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/compression/snappy"
	"github.com/paularlott/gossip/encryption/aes"
	"github.com/paularlott/gossip/examples/common"
	"github.com/paularlott/gossip/queue"
)

var q *queue.Queue

func main() {
	common.Configure("debug", "console", os.Stderr)

	port := flag.Int("port", 0, "Port to listen on")
	webPort := flag.Int("web-port", 0, "Web port")
	peersArg := flag.String("peers", "", "Comma separated list of peers to connect to")
	nodeID := flag.String("node-id", "", "Node ID to use (optional)")
	consumer := flag.Bool("consume", false, "Act as a consumer (process messages)")
	prefetch := flag.Int("prefetch", 4, "Max messages this consumer processes concurrently")
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
	config.Cipher = aes.New()
	config.Logger = common.GetLogger()
	config.MsgCodec = shamaton.New()
	config.Compressor = snappy.New()

	var httpTransport *gossip.HTTPTransport
	if *webPort > 0 {
		httpTransport = gossip.NewHTTPTransport(config)
		config.Transport = httpTransport
	} else {
		config.Transport = gossip.NewSocketTransport(config)
	}

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		common.WithError(err).Fatal("Failed to create cluster")
	}

	// Create the queue
	q = queue.New(cluster, &queue.Config{
		Name:              "tasks",
		VisibilityTimeout: 30 * time.Second,
		MaxRetries:        3,
		Prefetch:          *prefetch,
	})
	defer q.Close()

	// If --consume flag, start processing messages
	if *consumer {
		q.Consume(func(msg *queue.Message) error {
			common.Info("Processing message",
				"id", msg.MessageID,
				"payload", string(msg.Payload),
				"attempt", msg.Attempt,
			)

			// If the message expects a reply, send one
			result := fmt.Sprintf("processed: %s", string(msg.Payload))
			msg.Reply([]byte(result))

			return nil // ack
		})
		common.Info("Consumer started, waiting for messages...")
	}

	// Register CLI commands
	common.Commands = append(common.Commands,
		common.Command{
			Cmd:      "publish",
			HelpText: "  publish <message>           - Publish a message to the queue",
			Handler:  handlePublishCommand,
		},
		common.Command{
			Cmd:      "request",
			HelpText: "  request <message>           - Publish and wait for reply (10s timeout)",
			Handler:  handleRequestCommand,
		},
		common.Command{
			Cmd:      "status",
			HelpText: "  status                      - Show queue status",
			Handler:  handleStatusCommand,
		},
	)

	cluster.Start()

	// Join the cluster
	if len(peers) > 0 {
		err = cluster.Join(peers)
		if err != nil {
			common.WithError(err).Fatal("Failed to join cluster")
		}
	}

	// If web port is specified, start a web server
	var httpServer *http.Server
	if *webPort > 0 {
		http.HandleFunc("/", httpTransport.HandleGossipRequest)
		httpServer = &http.Server{Addr: fmt.Sprintf(":%d", *webPort)}
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				common.WithError(err).Error("Failed to start web server")
			}
		}()
	}

	// Handle CLI input
	go common.HandleCLIInput(cluster)

	// Wait for termination signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpServer.Shutdown(ctx)
	}
}

func handlePublishCommand(c *gossip.Cluster, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: publish <message>")
		return
	}

	payload := strings.Join(args[1:], " ")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := q.Publish(ctx, []byte(payload))
	if err != nil {
		fmt.Printf("Publish failed: %v\n", err)
		return
	}

	fmt.Printf("Published: %s\n", payload)
}

func handleRequestCommand(c *gossip.Cluster, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: request <message>")
		return
	}

	payload := strings.Join(args[1:], " ")
	fmt.Printf("Sending request: %s (waiting up to 10s)...\n", payload)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := q.Request(ctx, []byte(payload), 10*time.Second)
	if err != nil {
		fmt.Printf("Request failed: %v\n", err)
		return
	}

	fmt.Printf("Reply: %s\n", string(result))
}

func handleStatusCommand(c *gossip.Cluster, args []string) {
	fmt.Printf("Queue: %s\n", q.Name())
	fmt.Printf("  Pending (local coordinator):   %d\n", q.PendingCount())
	fmt.Printf("  Inflight (local coordinator):  %d\n", q.InflightCount())
	fmt.Printf("  Consumers (local coordinator): %d\n", q.ConsumerCount())
}
