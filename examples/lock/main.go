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
	"github.com/paularlott/gossip/leader"
	"github.com/paularlott/gossip/lock"
)

var pool *lock.Pool

func main() {
	common.Configure("debug", "console", os.Stderr)

	port := flag.Int("port", 0, "Port to listen on")
	webPort := flag.Int("web-port", 0, "Web port")
	peersArg := flag.String("peers", "", "Comma separated list of peers")
	nodeID := flag.String("node-id", "", "Node ID (optional)")
	minNodes := flag.Int("min-nodes", 2, "MinClusterSize for quorum")
	writeReplicas := flag.Int("w", 2, "WriteReplicas (W): nodes that must hold a mutation before ack")
	flag.Parse()

	var peers []string
	if *peersArg != "" {
		peers = strings.Split(*peersArg, ",")
	}

	advertiseAddr := ""
	bindAddr := ""
	if *port > 0 {
		advertiseAddr = fmt.Sprintf("127.0.0.1:%d", *port)
		bindAddr = fmt.Sprintf("127.0.0.1:%d", *port)
	} else if *webPort > 0 {
		advertiseAddr = fmt.Sprintf("http://127.0.0.1:%d", *webPort)
		bindAddr = "/"
	}

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

	// Leader election
	ec := leader.DefaultConfig()
	ec.MinClusterSize = *minNodes
	election := leader.NewLeaderElection(cluster, ec)

	election.HandleEventFunc(leader.BecameLeaderEvent, func(et leader.EventType, id gossip.NodeID) {
		common.Warn("became leader")
	})
	election.HandleEventFunc(leader.LeaderLostEvent, func(et leader.EventType, id gossip.NodeID) {
		common.Warn("leader lost", "was", id.String())
	})

	election.Start()
	defer election.Stop()

	// Lock pool
	pool = lock.NewPool(cluster, election, &lock.Config{
		MinTTL:        1 * time.Second,
		MaxTTL:        30 * time.Second,
		WriteReplicas: *writeReplicas,
	})
	defer pool.Close()

	common.Commands = append(common.Commands,
		common.Command{Cmd: "lock", HelpText: "  lock <key> <ttl_seconds>    - Acquire a lock (blocks 10s)", Handler: handleLock},
		common.Command{Cmd: "trylock", HelpText: "  trylock <key> <ttl_seconds> - Non-blocking acquire", Handler: handleTryLock},
		common.Command{Cmd: "unlock", HelpText: "  unlock <key>                - Release a lock", Handler: handleUnlock},
		common.Command{Cmd: "extend", HelpText: "  extend <key> <ttl_seconds>  - Extend a lock's TTL", Handler: handleExtend},
		common.Command{Cmd: "query", HelpText: "  query <key>                 - Query lock status", Handler: handleQuery},
		common.Command{Cmd: "locks", HelpText: "  locks                       - Show locally held locks", Handler: handleLocks},
		common.Command{Cmd: "leader", HelpText: "  leader                      - Show current leader", Handler: handleLeader},
	)

	cluster.Start()

	if len(peers) > 0 {
		if err := cluster.Join(peers); err != nil {
			common.WithError(err).Fatal("Failed to join cluster")
		}
	}

	var httpServer *http.Server
	if *webPort > 0 {
		http.HandleFunc("/", httpTransport.HandleGossipRequest)
		httpServer = &http.Server{Addr: fmt.Sprintf(":%d", *webPort)}
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				common.WithError(err).Error("HTTP server failed")
			}
		}()
	}

	go common.HandleCLIInput(cluster)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpServer.Shutdown(ctx)
	}
}

var heldLocks = make(map[string]*lock.Lock)

func handleLock(c *gossip.Cluster, args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: lock <key> <ttl_seconds>")
		return
	}
	key := args[1]
	ttl, _ := time.ParseDuration(args[2] + "s")
	if ttl == 0 {
		fmt.Println("Invalid TTL")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lk, err := pool.Acquire(ctx, key, ttl)
	if err != nil {
		fmt.Printf("Failed: %v\n", err)
		return
	}
	heldLocks[key] = lk
	fmt.Printf("Acquired: key=%s token=%s\n", key, lk.Token())
}

func handleTryLock(c *gossip.Cluster, args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: trylock <key> <ttl_seconds>")
		return
	}
	key := args[1]
	ttl, _ := time.ParseDuration(args[2] + "s")
	if ttl == 0 {
		fmt.Println("Invalid TTL")
		return
	}

	lk, err := pool.TryAcquire(key, ttl)
	if err != nil {
		fmt.Printf("Not acquired: %v\n", err)
		return
	}
	heldLocks[key] = lk
	fmt.Printf("Acquired: key=%s token=%s\n", key, lk.Token())
}

func handleUnlock(c *gossip.Cluster, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: unlock <key>")
		return
	}
	key := args[1]
	lk, ok := heldLocks[key]
	if !ok {
		fmt.Printf("No local lock for '%s'\n", key)
		return
	}
	if err := lk.Release(); err != nil {
		fmt.Printf("Release failed: %v\n", err)
		return
	}
	delete(heldLocks, key)
	fmt.Printf("Released: %s\n", key)
}

func handleExtend(c *gossip.Cluster, args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: extend <key> <ttl_seconds>")
		return
	}
	key := args[1]
	ttl, _ := time.ParseDuration(args[2] + "s")
	lk, ok := heldLocks[key]
	if !ok {
		fmt.Printf("No local lock for '%s'\n", key)
		return
	}
	if err := lk.Extend(ttl); err != nil {
		fmt.Printf("Extend failed: %v\n", err)
		return
	}
	fmt.Printf("Extended: %s ttl=%v\n", key, ttl)
}

func handleQuery(c *gossip.Cluster, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: query <key>")
		return
	}
	held, owner, token, remaining, err := pool.Query(args[1])
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}
	if !held {
		fmt.Printf("'%s' is not held\n", args[1])
	} else {
		fmt.Printf("'%s': owner=%s token=%s remaining=%v\n", args[1], owner, token, remaining.Round(time.Millisecond))
	}
}

func handleLocks(c *gossip.Cluster, args []string) {
	if len(heldLocks) == 0 {
		fmt.Println("No locally held locks")
		return
	}
	data := [][]string{{"Key", "Token"}}
	for k, lk := range heldLocks {
		data = append(data, []string{k, lk.Token().String()})
	}
	common.PrintTable(data)
}

func handleLeader(c *gossip.Cluster, args []string) {
	fmt.Printf("Leader: %v | W=%d | known locks: %d\n",
		func() string {
			if pool.IsLeader() {
				return "this node"
			}
			return "elsewhere"
		}(),
		pool.WriteReplicas(),
		pool.ReplicaCount())
}
