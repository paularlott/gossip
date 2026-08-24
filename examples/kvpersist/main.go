// Command kvpersist demonstrates the kv package's optional persistence: a
// store whose snapshots are dumped to a human-readable JSON file, reloaded
// on start, and kept fresh by the debounced snapshot triggers.
//
// Run two or three instances with -peers pointing at each other, set and
// delete some keys, watch kv-snapshot.json appear after a few writes (or
// force it with the "snapshot" command), then restart everything — the
// store comes back from the file and catches up from its peers.
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/compression/snappy"
	"github.com/paularlott/gossip/encryption/aes"
	"github.com/paularlott/gossip/examples/common"
	"github.com/paularlott/gossip/kv"
)

// --- a Persister that dumps to JSON ---

// jsonEntry is the readable mirror of kv.Entry: byte slices and UUIDs become
// hex strings so the file can be read (and hand-edited, if you like living
// dangerously) by a human.
//
// Every field of kv.Entry must survive the round trip — see the Persister
// contract — so the mirror keeps them all, even the tombstone timestamps.
type jsonEntry struct {
	Key         string `json:"key"`
	Version     uint64 `json:"version"`
	Origin      string `json:"origin"`
	Tombstone   bool   `json:"tombstone,omitempty"`
	ExpiresAtMs int64  `json:"expires_at_ms,omitempty"`
	DeletedAtMs int64  `json:"deleted_at_ms,omitempty"`
	Value       string `json:"value,omitempty"` // hex
}

type jsonFile struct {
	Store   string      `json:"store"`
	SavedAt time.Time   `json:"saved_at"`
	Entries []jsonEntry `json:"entries"`
}

// jsonPersister implements kv.Persister by writing the snapshot as pretty
// JSON. The encoding is entirely ours — the store hands over the entries
// and does not care how they reach the disk. Save is called on its own
// goroutine by the store, so a little internal locking is all the safety
// needed. Writes are atomic: temp file, then rename.
type jsonPersister struct {
	path string
	mu   sync.Mutex
}

func (p *jsonPersister) Save(snap *kv.StoreSnapshot) error {
	file := jsonFile{Store: snap.Store, SavedAt: time.Now().UTC()}
	for _, e := range snap.Entries {
		file.Entries = append(file.Entries, jsonEntry{
			Key:         e.Key,
			Version:     e.Version,
			Origin:      hex.EncodeToString(e.Origin[:]),
			Tombstone:   e.Tombstone,
			ExpiresAtMs: e.ExpiresAtMs,
			DeletedAtMs: e.DeletedAtMs,
			Value:       hex.EncodeToString(e.Value),
		})
	}

	data, err := json.MarshalIndent(&file, "", "  ")
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return atomicWrite(p.path, data)
}

func (p *jsonPersister) Load() (*kv.StoreSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.path)
	if os.IsNotExist(err) {
		return nil, nil // first run: nothing to restore
	}
	if err != nil {
		return nil, err
	}

	var file jsonFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", p.path, err)
	}

	entries := make([]*kv.Entry, 0, len(file.Entries))
	for _, je := range file.Entries {
		e := &kv.Entry{
			Key:         je.Key,
			Version:     je.Version,
			Tombstone:   je.Tombstone,
			ExpiresAtMs: je.ExpiresAtMs,
			DeletedAtMs: je.DeletedAtMs,
		}
		if je.Origin != "" {
			origin, err := hex.DecodeString(je.Origin)
			if err != nil || len(origin) != len(e.Origin) {
				return nil, fmt.Errorf("entry %q: bad origin", je.Key)
			}
			copy(e.Origin[:], origin)
		}
		if je.Value != "" {
			val, err := hex.DecodeString(je.Value)
			if err != nil {
				return nil, fmt.Errorf("entry %q: bad value", je.Key)
			}
			e.Value = val
		}
		entries = append(entries, e)
	}
	return &kv.StoreSnapshot{Store: file.Store, Entries: entries}, nil
}

// atomicWrite writes via a temp file in the same directory, then renames —
// a crash mid-save can never leave a truncated snapshot behind.
func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func main() {
	common.Configure("debug", "console", os.Stderr)

	port := flag.Int("port", 0, "Port to listen on")
	peersArg := flag.String("peers", "", "Comma separated list of peers, e.g. 127.0.0.1:8001,127.0.0.1:8002")
	snapPath := flag.String("file", "kv-snapshot.json", "Path of the JSON snapshot file")
	storeName := flag.String("store", "demo", "Store name")
	nodeID := flag.String("node-id", "", "Node ID to use (optional)")
	flag.Parse()

	var peers []string
	if *peersArg != "" {
		peers = strings.Split(*peersArg, ",")
	}

	config := gossip.DefaultConfig()
	config.NodeID = *nodeID
	config.BindAddr = fmt.Sprintf("127.0.0.1:%d", *port)
	config.AdvertiseAddr = fmt.Sprintf("127.0.0.1:%d", *port)
	config.EncryptionKey = []byte("1234567890123456")
	config.Cipher = aes.New()
	config.Logger = common.GetLogger()
	config.MsgCodec = shamaton.New()
	config.Compressor = snappy.New()
	config.Transport = gossip.NewSocketTransport(config)

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		common.WithError(err).Error("Failed to create cluster")
		os.Exit(1)
	}
	cluster.Start()
	defer cluster.Stop()

	if err := cluster.Join(peers); err != nil {
		common.WithError(err).Error("Failed to join cluster")
	}

	persister := &jsonPersister{path: *snapPath}
	store := kv.NewStore(cluster, kv.ClusterMembership{Cluster: cluster}, &kv.Config{
		Name:             *storeName,
		WriteReplicas:    2,
		Persister:        persister,
		SnapshotWrites:   5,               // snapshot after five changes...
		SnapshotInterval: 5 * time.Second, // ...or five seconds of unsaved changes
	})
	defer store.Close()

	fmt.Printf("kv store %q with JSON persistence at %s\n", *storeName, *snapPath)
	if restored := store.Len(); restored > 0 {
		fmt.Printf("restored %d keys from the snapshot\n", restored)
	} else {
		fmt.Println("no snapshot found; starting empty")
	}

	common.Commands = append(common.Commands, common.Command{
		Cmd:      "set",
		HelpText: "set <key> <value>        - Set a value (durable on W nodes)",
		Handler: func(c *gossip.Cluster, args []string) {
			if len(args) < 3 {
				fmt.Println("Usage: set <key> <value>")
				return
			}
			if err := store.Set(args[1], []byte(strings.Join(args[2:], " ")), 0); err != nil {
				fmt.Println("Error setting value:", err)
				return
			}
			fmt.Println("Key set:", args[1])
		},
	})
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "get",
		HelpText: "get <key>                - Get a value",
		Handler: func(c *gossip.Cluster, args []string) {
			if len(args) < 2 {
				fmt.Println("Usage: get <key>")
				return
			}
			if value, ok := store.Get(args[1]); ok {
				fmt.Println("Value:", string(value))
				return
			}
			fmt.Println("Error getting value: key not found")
		},
	})
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "delete",
		HelpText: "delete <key>             - Delete a key (tombstone)",
		Handler: func(c *gossip.Cluster, args []string) {
			if len(args) < 2 {
				fmt.Println("Usage: delete <key>")
				return
			}
			if err := store.Delete(args[1]); err != nil {
				fmt.Println("Error deleting key:", err)
				return
			}
			fmt.Println("Key deleted:", args[1])
		},
	})
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "keys",
		HelpText: "keys                     - List live keys",
		Handler: func(c *gossip.Cluster, args []string) {
			fmt.Printf("Current keys: %v\n", store.Keys(""))
		},
	})
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "snapshot",
		HelpText: "snapshot                 - Force an immediate JSON snapshot",
		Handler: func(c *gossip.Cluster, args []string) {
			if err := store.Snapshot(); err != nil {
				fmt.Println("Error saving snapshot:", err)
				return
			}
			fmt.Println("Snapshot written to", *snapPath)
		},
	})
	common.Commands = append(common.Commands, common.Command{
		Cmd:      "file",
		HelpText: "file                     - Print the JSON snapshot file",
		Handler: func(c *gossip.Cluster, args []string) {
			data, err := os.ReadFile(*snapPath)
			if os.IsNotExist(err) {
				fmt.Println("No snapshot yet; try 'snapshot' first")
				return
			}
			if err != nil {
				fmt.Println("Error reading snapshot:", err)
				return
			}
			fmt.Println(string(data))
		},
	})
	go common.HandleCLIInput(cluster)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
