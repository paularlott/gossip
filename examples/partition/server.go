package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/partition"
	"github.com/paularlott/logger"
)

// maxValueSize bounds a value, so one request cannot make a partition's snapshot (and every replication of it) huge.
const maxValueSize = 256 << 10

// serverConfig is everything a node of the example is started with.
type serverConfig struct {
	NodeID      string   // optional: a stable node ID (default: a new one every start)
	Bind        string   // gossip address, host:port (TCP and UDP); also advertised to the peers
	HTTPAddr    string   // address of the HTTP API, host:port ("127.0.0.1:0": pick a port)
	Peers       []string // gossip addresses of other nodes to join
	ClusterSize int      // the configured number of nodes, the same on every node
	Partitions  int      // the number of partitions, the same on every node
	DataDir     string
	NewCluster  bool // one of the first nodes of a brand new cluster: an empty data directory is fine
	Resync      bool // the data is lost, restored or suspect: catch up from the others first
	Heartbeat   time.Duration
	Logger      logger.Logger
}

// server is one node: a gossip cluster, the partition manager over a fileStore, and the HTTP API.
type server struct {
	cfg     serverConfig
	cluster *gossip.Cluster
	store   *fileStore
	mgr     *partition.Manager
	http    *http.Server
	ln      net.Listener

	stop     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// newServer starts a node.
func newServer(cfg serverConfig) (*server, error) {
	if cfg.Logger == nil {
		cfg.Logger = logger.NewNullLogger()
	}
	store, err := openFileStore(cfg.DataDir, cfg.Partitions, cfg.NewCluster, cfg.Resync)
	if err != nil {
		return nil, fmt.Errorf("opening the store: %w", err)
	}

	gc := gossip.DefaultConfig()
	gc.NodeID = cfg.NodeID
	gc.BindAddr = cfg.Bind
	gc.AdvertiseAddr = cfg.Bind
	gc.Logger = cfg.Logger
	// Failure detection a little brisker than the library's default, so that a killed node is noticed (and its
	// partitions taken over) within a few seconds.
	gc.SuspectTimeout = 500 * time.Millisecond
	gc.DeadNodeTimeout = 2 * time.Second
	gc.GossipInterval = 250 * time.Millisecond
	// A forwarded write may take up to the Manager's WriteTimeout (5s by default) on its owner: the transport must wait
	// longer than that for the answer, or the write is reported ErrUnknown while it still commits (checked below).
	gc.TCPDeadline = 10 * time.Second
	gc.Transport = gossip.NewSocketTransport(gc) // catch-up uses streams: the socket and HTTP transports have them
	cluster, err := gossip.NewCluster(gc)
	if err != nil {
		return nil, fmt.Errorf("creating the cluster: %w", err)
	}

	mgr, err := partition.New(cluster, partition.Config{
		Partitions:        cfg.Partitions,
		ClusterSize:       cfg.ClusterSize,
		HeartbeatInterval: cfg.Heartbeat,
		Store:             store,
		Logger:            cfg.Logger,
	})
	if err != nil {
		return nil, err
	}
	if gc.TCPDeadline < mgr.MinTransportDeadline() {
		return nil, fmt.Errorf("the gossip TCPDeadline (%s) must be at least %s", gc.TCPDeadline, mgr.MinTransportDeadline())
	}

	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", cfg.HTTPAddr, err)
	}
	s := &server{cfg: cfg, cluster: cluster, store: store, mgr: mgr, ln: ln, stop: make(chan struct{})}

	// When the resync is over the node's data is the cluster's again, so the data directory can be trusted: from then on a
	// restart needs no -resync. (Registered before Start; a node that started intact never has a resync to finish.)
	mgr.HandleResyncedFunc(func() {
		if err := store.MarkIntact(); err != nil {
			cfg.Logger.Error("could not mark the data directory intact", "error", err)
		}
	})

	cluster.Start()
	if err := mgr.Start(); err != nil {
		cluster.Stop()
		ln.Close()
		return nil, err
	}
	if len(cfg.Peers) > 0 {
		// A failed join is not fatal: the node keeps trying to find the others through gossip once one answers.
		if err := cluster.Join(cfg.Peers); err != nil {
			cfg.Logger.Warn("could not join the peers (yet)", "error", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key...}", s.handlePut)
	mux.HandleFunc("GET /kv/{key...}", s.handleGet)
	mux.HandleFunc("DELETE /kv/{key...}", s.handleDelete)
	mux.HandleFunc("GET /status", s.handleStatus)
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	s.goroutine(func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			cfg.Logger.Error("http server failed", "error", err)
		}
	})

	return s, nil
}

func (s *server) goroutine(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
}

// httpURL is the base URL of the node's HTTP API.
func (s *server) httpURL() string { return "http://" + s.ln.Addr().String() }

// Close stops the node: the HTTP API, the partition manager (which hands its partitions back), the cluster, and flushes
// the store (a Commit does not wait for the disk).
func (s *server) Close() {
	s.stopOnce.Do(func() {
		close(s.stop)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.http.Shutdown(ctx)
		s.mgr.Stop()
		s.cluster.Stop()
		s.wg.Wait()
		if err := s.store.Sync(); err != nil {
			s.cfg.Logger.Error("could not flush the store", "error", err)
		}
	})
}

// --- HTTP API ---

// target resolves the key of a request to its partition.
func (s *server) target(r *http.Request) (key string, p int, err error) {
	key = r.PathValue("key")
	if key == "" {
		return "", 0, errors.New("empty key")
	}
	return key, partition.PartitionOf(key, s.cfg.Partitions), nil
}

// expectation reads the optional precondition of a write: If-Match: "<version>" or If-None-Match: * (must not exist).
func expectation(r *http.Request) (*uint64, error) {
	if r.Header.Get("If-None-Match") == "*" {
		zero := uint64(0)
		return &zero, nil
	}
	m := strings.Trim(strings.TrimSpace(r.Header.Get("If-Match")), `"`)
	if m == "" {
		return nil, nil
	}
	v, err := strconv.ParseUint(m, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("If-Match must be a version number: %v", err)
	}
	return &v, nil
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request) {
	key, p, err := s.target(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	expect, err := expectation(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	val, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueSize))
	if err != nil {
		http.Error(w, "value too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	s.write(w, r, p, change{Op: "set", Key: key, Val: val, Expect: expect})
}

func (s *server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key, p, err := s.target(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	expect, err := expectation(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.write(w, r, p, change{Op: "delete", Key: key, Expect: expect})
}

// write sends the change to the partition's owner (this node, or another one: Manager.Write finds it) and maps the
// outcome to a status code.
func (s *server) write(w http.ResponseWriter, r *http.Request, p int, c change) {
	raw, err := json.Marshal(c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Partition", strconv.Itoa(p))
	res, err := s.mgr.Write(r.Context(), p, raw)
	var refused *partition.RefusedError
	switch {
	case err == nil:
		// The new version of the key, which the owner's Prepare decided and the package carried back to this node.
		if len(res.Reply) > 0 {
			w.Header().Set("ETag", `"`+string(res.Reply)+`"`)
		}
		// Hold the answer until this node has applied the write too, so that a GET on this node right afterwards sees it.
		// The write is committed either way: a node that is slow to hear of it only answers a little sooner than it could
		// have read.
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		s.mgr.WaitApplied(ctx, p, res.Pos)
		cancel()
		w.WriteHeader(http.StatusNoContent)
	case errors.As(err, &refused):
		switch refused.Code {
		case refusalNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		case refusalConflict:
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	case errors.Is(err, partition.ErrUnknown):
		// Sent, but whether it was committed is not known: do not retry blindly, read the key first.
		http.Error(w, "outcome unknown: the change may or may not have been applied", http.StatusGatewayTimeout)
	case errors.Is(err, partition.ErrUnavailable):
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no owner or no majority reachable; nothing was written", http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleGet is a local read by default: it is answered from this node's copy if the Manager says that copy is recent
// enough (CanRead), and with 503 otherwise. A node other than the owner can be a heartbeat behind, so a read right after a
// write may not see it yet; the owner's copy is current. With ?consistent=1 it is a linearizable read instead (see
// getConsistent).
func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	key, p, err := s.target(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("X-Partition", strconv.Itoa(p))
	if r.URL.Query().Get("consistent") == "1" {
		s.getConsistent(w, r, p, key)
		return
	}
	if !s.mgr.CanRead(p) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "this node cannot serve reads of the partition right now", http.StatusServiceUnavailable)
		return
	}
	val, ver, ok := s.store.get(p, key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeValue(w, val, ver)
}

// getConsistent is GET ?consistent=1: the key as of at least every write committed before the request arrived, on any node.
// Manager.Read sends it to the partition's owner, which confirms with a majority that it still owns the partition before it
// answers (Store.Read), so it costs a round trip to a majority, plus the hop to the owner when this node is not it; a
// plain GET costs nothing. 503 means no owner or no majority could be reached in time (nothing is served from a copy that
// may be behind), as for a write.
func (s *server) getConsistent(w http.ResponseWriter, r *http.Request, p int, key string) {
	raw, err := s.mgr.Read(r.Context(), p, []byte(key))
	var refused *partition.RefusedError
	switch {
	case err == nil:
		var ans readAnswer
		if err := json.Unmarshal(raw, &ans); err != nil {
			http.Error(w, "undecodable answer: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeValue(w, ans.Val, ans.Ver)
	case errors.As(err, &refused) && refused.Code == refusalNotFound:
		http.Error(w, "not found", http.StatusNotFound)
	case errors.As(err, &refused):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, partition.ErrUnavailable):
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no owner or no majority reachable; nothing was served", http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// writeValue answers a GET with the value and its version as the ETag.
func writeValue(w http.ResponseWriter, val []byte, ver uint64) {
	w.Header().Set("ETag", `"`+strconv.FormatUint(ver, 10)+`"`)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(val)
}

// partStatus is the /status entry of one partition.
type partStatus struct {
	Partition int    `json:"partition"`
	Owner     string `json:"owner,omitempty"`
	IsOwner   bool   `json:"isOwner"`
	CanRead   bool   `json:"canRead"`
	Epoch     uint64 `json:"epoch"`     // the epoch counter this node has promised
	Committed uint64 `json:"committed"` // the sequence of the last write applied here
	Pending   bool   `json:"pending"`
	Keys      int    `json:"keys"`
}

type statusResp struct {
	Node       string       `json:"node"`
	Ready      bool         `json:"ready"`
	Resync     bool         `json:"resync"` // started without intact data and still catching up
	Owned      []int        `json:"owned"`
	Partitions []partStatus `json:"partitions,omitempty"`
}

func (s *server) status() statusResp {
	id := s.cluster.LocalNode().ID
	out := statusResp{Node: id.String(), Ready: s.mgr.Ready(), Resync: s.mgr.Resyncing(), Owned: []int{}}
	for p := 0; p < s.cfg.Partitions; p++ {
		st, _ := s.store.State(p)
		ps := partStatus{Partition: p, IsOwner: s.mgr.IsOwner(p), CanRead: s.mgr.CanRead(p),
			Epoch: st.Promised.Counter, Committed: st.Committed.Seq, Pending: st.Pending != nil, Keys: s.store.keys(p)}
		if o, ok := s.mgr.Owner(p); ok {
			ps.Owner = o.String()
		}
		if ps.IsOwner {
			out.Owned = append(out.Owned, p)
		}
		out.Partitions = append(out.Partitions, ps)
	}
	return out
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	st := s.status()
	if r.URL.Query().Has("brief") {
		st.Partitions = nil // GET /status?brief: just the node, readiness and the partitions it owns
	}
	enc.Encode(st)
}
