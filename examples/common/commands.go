package common

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/paularlott/gossip"

	"github.com/google/uuid"
)

type CommandHandler func(c *gossip.Cluster, args []string)
type Command struct {
	Cmd      string
	HelpText string
	Handler  CommandHandler
}

var Commands = []Command{}

func HandleCLIInput(c *gossip.Cluster) {
	fmt.Printf("Cluster started local node ID %s\n\n", c.GetLocalNode().ID.String())
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
		case "peers":
			handlePeersCommand(c)

		case "help":
			displayHelp()

		case "set-meta":
			handleSetmetaCommand(c, args)

		case "get-meta":
			handleGetmetaCommand(c, args)

		case "show-meta":
			handleShowmetaCommand(c, args)

		case "whoami":
			fmt.Printf("Local node ID: %s (%s)\n", c.GetLocalNode().ID.String(), c.GetLocalNode().GetAddress().String())

		case "":
			// Do nothing for empty input

		default:
			// Look through the registered commands
			found := false
			for _, command := range Commands {
				if command.Cmd == cmd {
					command.Handler(c, args)
					found = true
					break
				}
			}

			if !found {
				fmt.Println("Unknown command. Type 'help' for available commands.")
			}
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

// handlePeersCommand displays all peers in the cluster
func handlePeersCommand(c *gossip.Cluster) {
	peers := c.GetAllNodes()
	if len(peers) == 0 {
		fmt.Println("No peers in the cluster")
		return
	}

	data := [][]string{}
	data = append(data, []string{"ID", "Address", "State", "Metadata"})
	for _, p := range peers {
		meta := p.Metadata.GetAllAsString()
		metaKV := []string{}
		for k, v := range meta {
			metaKV = append(metaKV, fmt.Sprintf("%s = %s", k, v))
		}

		data = append(data, []string{p.ID.String(), p.GetAddress().String(), p.GetState().String(), strings.Join(metaKV, ", ")})
	}

	PrintTable(data)
}

func handleSetmetaCommand(c *gossip.Cluster, args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: setmeta <key> <value>")
		return
	}

	key := args[1]
	value := strings.Join(args[2:], " ")

	c.LocalMetadata().SetString(key, value)
	fmt.Printf("Metadata set: %s = %s\n", key, value)

	c.SendMetadataUpdate()
}

func handleGetmetaCommand(c *gossip.Cluster, args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: getmeta <node_id> <key>")
		return
	}

	u, err := uuid.Parse(args[1])
	if err != nil {
		return
	}

	nodeID := gossip.NodeID(u)
	key := args[2]

	node := c.GetNodeByID(nodeID)
	if node == nil {
		fmt.Printf("Node %s not found\n", nodeID)
		return
	}

	value := node.Metadata.GetString(key)
	if !node.Metadata.Exists(key) {
		fmt.Printf("Key %s not found in node %s metadata\n", key, nodeID)
	} else {
		fmt.Printf("Node %s metadata: %s = %s\n", nodeID, key, value)
	}
}

func handleShowmetaCommand(c *gossip.Cluster, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: show-meta <node_id>")
		return
	}

	u, err := uuid.Parse(args[1])
	if err != nil {
		return
	}

	nodeID := gossip.NodeID(u)
	node := c.GetNodeByID(nodeID)
	if node == nil {
		fmt.Printf("Node %s not found\n", nodeID)
		return
	}
	meta := node.Metadata.GetAllAsString()

	data := [][]string{}
	data = append(data, []string{"Key", "Value"})
	for k, v := range meta {
		data = append(data, []string{k, v})
	}

	PrintTable(data)
}

// displayHelp shows available commands
func displayHelp() {
	fmt.Println("Available commands:")
	fmt.Println("-----------------------------------")
	fmt.Println("  whoami									  - Show local node ID")
	fmt.Println("  peers                    - Show all peers in the cluster")
	fmt.Println("  set-meta <key> <value>   - Set metadata for the local node")
	fmt.Println("  get-meta <node_id> <key> - Get metadata for the node")
	fmt.Println("  show-meta <node_id>      - Show all metadata for the node")

	for _, command := range Commands {
		fmt.Printf("  %s\n", command.HelpText)
	}

	fmt.Println("  help                     - Show this help message")
	fmt.Println("  (Ctrl+C to exit)")
	fmt.Println("-----------------------------------")
}

func PrintTable(table [][]string) {
	// Find the maximum width of each column
	maxWidths := make([]int, len(table[0]))
	for _, row := range table {
		for i, cell := range row {
			if len(cell) > maxWidths[i] {
				maxWidths[i] = len(cell)
			}
		}
	}

	// Print each row
	for _, row := range table {
		for i, cell := range row {
			// Pad the columns as necessary
			fmt.Printf("%-*s  ", maxWidths[i], cell)
		}
		fmt.Println()
	}
}
