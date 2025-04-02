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

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ZerologLogger implements the Logger interface using zerolog
type ZerologLogger struct {
	zl zerolog.Logger
}

func NewZerologLogger(zl zerolog.Logger) *ZerologLogger {
	return &ZerologLogger{zl: zl}
}

func (l *ZerologLogger) Debugf(format string, args ...interface{}) {
	l.zl.Debug().Msgf(format, args...)
}
func (l *ZerologLogger) Infof(format string, args ...interface{}) {
	l.zl.Info().Msgf(format, args...)
}
func (l *ZerologLogger) Warnf(format string, args ...interface{}) {
	l.zl.Warn().Msgf(format, args...)
}
func (l *ZerologLogger) Errorf(format string, args ...interface{}) {
	l.zl.Error().Msgf(format, args...)
}
func (l *ZerologLogger) Field(key string, value interface{}) gossip.Logger {
	return &ZerologLogger{
		zl: l.zl.With().Interface(key, value).Logger(),
	}
}
func (l *ZerologLogger) Err(err error) gossip.Logger {
	return &ZerologLogger{
		zl: l.zl.With().Err(err).Logger(),
	}
}

const (
	GossipMsg gossip.MessageType = gossip.UserMsg + iota // User message
)

type GossipMessage struct {
	Message string `msgpack:"message" json:"message"`
}

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
	config.Logger = NewZerologLogger(log.Logger)
	config.MsgCodec = codec.NewShamatonMsgpackCodec()

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create cluster")
	}
	defer func() {
		cluster.Leave()
		cluster.Shutdown()
	}()

	err = cluster.Join(peers)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to join cluster")
	}

	cluster.HandleFunc(GossipMsg, func(sender *gossip.Node, packet *gossip.Packet) error {
		var msg GossipMessage
		if err := packet.Unmarshal(&msg); err != nil {
			return err
		}

		fmt.Printf("Received GossipMsg message from %s: %s\n", sender.ID, msg.Message)
		return nil
	})

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
	fmt.Println("Enter 'help' to show available commands. Press Ctrl+C to exit.")

	reader := bufio.NewReader(os.Stdin)

	for {
		// Display prompt and read input
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		// Parse the command
		args := parseCommand(strings.TrimSpace(input))
		if len(args) == 0 {
			continue // Empty input
		}

		cmd := strings.ToLower(args[0])

		// Process the command
		switch cmd {
		case "gossip":
			handleGossipCommand(c, args)

		case "peers":
			handlePeersCommand(c)

		case "help":
			displayHelp()

		// Commented out for future implementation
		/*
		   case "set":
		       handleSetCommand(c, args)

		   case "get":
		       handleGetCommand(c, args)
		*/

		case "":
			// Do nothing for empty input

		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

// parseCommand splits the input into command arguments
func parseCommand(input string) []string {
	// Split by spaces, but respect quoted strings
	var args []string
	inQuotes := false
	current := ""

	for _, char := range input {
		switch char {
		case '"':
			inQuotes = !inQuotes
		case ' ':
			if !inQuotes {
				if current != "" {
					args = append(args, current)
					current = ""
				}
			} else {
				current += string(char)
			}
		default:
			current += string(char)
		}
	}

	if current != "" {
		args = append(args, current)
	}

	return args
}

// handleGossipCommand processes the gossip command
func handleGossipCommand(c *gossip.Cluster, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: gossip <message>")
		return
	}

	// Combine all remaining arguments as the message
	messageText := strings.Join(args[1:], " ")

	msg := GossipMessage{Message: messageText}
	err := c.Send(GossipMsg, msg)
	if err != nil {
		fmt.Printf("Error sending message: %v\n", err)
	} else {
		fmt.Println("Message sent")
	}
}

// handlePeersCommand displays all peers in the cluster
func handlePeersCommand(c *gossip.Cluster) {
	peers := c.GetAllNodes()
	if len(peers) == 0 {
		fmt.Println("No peers in the cluster")
		return
	}

	fmt.Println("Cluster peers:")
	fmt.Println("-----------------------------------")
	for _, p := range peers {
		fmt.Printf("Node ID: %s\n", p.ID)
		fmt.Printf("  Address: %s\n", p.GetAdvertisedAddr())
		fmt.Printf("  State: %s\n", p.GetState().String())
		fmt.Println("-----------------------------------")
	}
}

// displayHelp shows available commands
func displayHelp() {
	fmt.Println("Available commands:")
	fmt.Println("-----------------------------------")
	fmt.Println("  gossip <message>  - Send a message to the cluster")
	fmt.Println("  peers             - Show all peers in the cluster")
	fmt.Println("  help              - Show this help message")
	fmt.Println("  (Ctrl+C to exit)")
	fmt.Println("-----------------------------------")

	// Commented out for future implementation
	/*
	   fmt.Println("  set <key> <value> - Set a value in the distributed store")
	   fmt.Println("  get <key>         - Get a value from the distributed store")
	*/
}

// Commented out for future implementation
/*
func handleSetCommand(c *gossip.Cluster, args []string) {
    if len(args) < 3 {
        fmt.Println("Usage: set <key> <value>")
        return
    }

    key := args[1]
    value := strings.Join(args[2:], " ")

    // TODO: Implement set functionality
    // c.Set(key, value)

    fmt.Println("Value set successfully")
}

func handleGetCommand(c *gossip.Cluster, args []string) {
    if len(args) < 2 {
        fmt.Println("Usage: get <key>")
        return
    }

    key := args[1]

    // TODO: Implement get functionality
    // value, exists := c.Get(key)
    // if !exists {
    //     fmt.Println("Key not found")
    // } else {
    //     fmt.Println(value)
    // }
}
*/
