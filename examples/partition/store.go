package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/paularlott/gossip/partition"
)

// fileStore is a durable partition.Store for the example, built on the standard library only.
//
// Layout, one directory per partition under the data directory:
//
//	<dir>/intact           marker: this data directory's state can be trusted (see Intact)
//	<dir>/p03/state.json   the protocol state that changes on every message: Promised and Pending
//	<dir>/p03/data.json    the committed data: Committed and every record (tombstones included)
//
// Every file is replaced atomically: the new content is written to a temporary file in the same directory, fsynced,
// renamed over the old file, and the directory is fsynced. A crash leaves the old file or the new one, never a torn
// one; temporary files left behind are removed when the store opens.
//
// The design choice is a snapshot per partition: a Commit applies the write's records and the new committed position to
// the partition's data in one atomic step (a Commit cannot be half done), and data.json is rewritten with the result.
// That costs time proportional to the partition's size per write, which is fine for a small example (and the number of
// partitions bounds it); an append-only log with a periodic snapshot is the upgrade when partitions get big, and it would
// not change anything outside this file. The whole store is also kept in memory, so State, Prepare, Export and reads
// never touch the disk.
//
// Commit and Import need not be durable before they return (the Store contract of the partition package: a write is safe
// once it is stored as pending on a majority, and a commit lost in a crash is redone from that), so they only update
// memory and leave the rewrite of data.json to a background flush, which still replaces the file atomically (temporary
// file, fsync, rename), so a crash leaves the state from before or after a flush, never a torn one, and a burst of commits
// costs one rewrite. What the contract adds is an ordering: SetPromised and SetPending flush the committed data before
// they write state.json, so that a crash never keeps a pending write (or a promise) while forgetting the commit before
// it. Sync does it on a graceful stop.
//
// A pending write is cleared lazily: Commit does not touch state.json, and State hides a pending write whose
// sequence is not above the committed one (the protocol's invariant is Pending.Seq == Committed.Seq+1, so a stored
// pending write at or below Committed is by definition decided). That saves a second fsync per commit and cannot be
// torn between the two files.
//
// Export answers "changed after seq" from each record's own Seq, the sequence of the write that last wrote it
// (Seq, not the full position: a sequence number is the same write on every node, but the epoch label of its
// position can differ between nodes). Deletes are tombstones, records with Del set, kept so that a delete reaches a
// node that catches up. This store never purges them, so Export never returns partition.ErrPurged and the cost is
// that a deleted key stays on disk forever.
type fileStore struct {
	dir    string
	intact bool // decided when the store was opened; see Intact
	parts  []*filePart
}

// filePart is one partition, all of it in memory, guarded by mu.
type filePart struct {
	mu   sync.RWMutex
	dir  string
	st   partition.State   // Pending is the stored one, possibly stale: see pending
	data map[string]record // replaced, never changed, by Commit and Import, so a snapshot of it can be read without mu
	gen  uint64            // bumped by every Commit and Import: the version of data and st.Committed

	// diskMu serialises the rewrites of data.json (lock order: mu, then diskMu); onDisk is the gen it holds.
	diskMu   sync.Mutex
	onDisk   atomic.Uint64
	flushing atomic.Bool // a background flush is running
}

// record is one key's committed state. Ver counts the writes of the key, tombstones included, so it never repeats;
// Seq is the sequence of the partition write that last wrote the key.
type record struct {
	Val []byte `json:"val,omitempty"`
	Ver uint64 `json:"ver"`
	Seq uint64 `json:"seq"`
	Del bool   `json:"del,omitempty"`
}

// live is the version a compare-and-swap is checked against: 0 for a key that is absent or deleted.
func (r record) live() uint64 {
	if r.Del {
		return 0
	}
	return r.Ver
}

type stateFile struct {
	Promised partition.Epoch  `json:"promised"`
	Pending  *partition.Write `json:"pending,omitempty"`
}

type dataFile struct {
	Committed partition.Pos     `json:"committed"`
	Records   map[string]record `json:"records"`
}

// The change a client asks for (the argument of Manager.Write) and the write it becomes (Store.Prepare's result).
type change struct {
	Op     string  `json:"op"` // "set" or "delete"
	Key    string  `json:"key"`
	Val    []byte  `json:"val,omitempty"`
	Expect *uint64 `json:"expect,omitempty"` // the live version the key must have (0: must not exist); nil: any
}

type changeKeyed struct {
	Key string `json:"key"`
	record
}

// The codes Prepare refuses with (partition.Refuse); the HTTP layer maps them to status codes.
const (
	refusalConflict = "conflict"
	refusalNotFound = "not-found"
)

// openFileStore opens (creating it if needed) the store in dir with the given number of partitions.
//
// Whether its state can be trusted (Intact) is decided here: a data directory that carries the intact marker is
// trusted unless resync is set; a new directory is trusted only if newCluster says this is one of the first nodes of
// a brand new cluster. Anything else (the directory was lost or never held this node's data, or the operator asked
// for a resync) is not intact, and the node catches up from a majority of the others before it promises or stores
// anything. The marker is removed for a resync and written (MarkIntact) once the node is ready, so a crash half way
// through a resync leaves the node needing one again.
func openFileStore(dir string, partitions int, newCluster, resync bool) (*fileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &fileStore{dir: dir, parts: make([]*filePart, partitions)}
	marker := filepath.Join(dir, "intact")
	_, err := os.Stat(marker)
	have := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	switch {
	case resync:
		if err := os.Remove(marker); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if err := syncDir(dir); err != nil {
			return nil, err
		}
	case have:
		s.intact = true
	case newCluster:
		if err := s.MarkIntact(); err != nil {
			return nil, err
		}
		s.intact = true
	}
	for p := range s.parts {
		fp, err := loadPart(filepath.Join(dir, fmt.Sprintf("p%02d", p)))
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", p, err)
		}
		s.parts[p] = fp
	}
	return s, nil
}

// MarkIntact records that this directory's state can be trusted from now on (the node finished its resync).
func (s *fileStore) MarkIntact() error {
	return writeFileAtomic(filepath.Join(s.dir, "intact"), []byte("1\n"))
}

func loadPart(dir string) (*filePart, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// A crash between creating a temporary file and renaming it leaves it behind; it is not part of the state.
	if tmps, err := filepath.Glob(filepath.Join(dir, "*.tmp")); err == nil {
		for _, t := range tmps {
			os.Remove(t)
		}
	}
	fp := &filePart{dir: dir, data: map[string]record{}}
	var sf stateFile
	if err := readJSON(filepath.Join(dir, "state.json"), &sf); err != nil {
		return nil, err
	}
	fp.st.Promised, fp.st.Pending = sf.Promised, sf.Pending
	var df dataFile
	if err := readJSON(filepath.Join(dir, "data.json"), &df); err != nil {
		return nil, err
	}
	fp.st.Committed = df.Committed
	if df.Records != nil {
		fp.data = df.Records
	}
	return fp, nil
}

// readJSON decodes the file into v; a missing file leaves v alone.
func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// writeFileAtomic replaces path with data: write a temporary file, fsync it, rename it over path, fsync the directory.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

// syncDir makes a rename or removal in dir durable. Windows cannot fsync a directory.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// saveState makes the committed data durable and then replaces state.json: the order the Store contract asks of a
// durable SetPromised and SetPending. The caller holds mu.
func (fp *filePart) saveState(promised partition.Epoch, pending *partition.Write) error {
	if err := fp.persist(fp.gen, fp.st.Committed, fp.data); err != nil {
		return err
	}
	b, err := json.Marshal(stateFile{Promised: promised, Pending: pending})
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(fp.dir, "state.json"), b)
}

// persist replaces data.json with the data of generation gen unless it already holds that or a later one. The arguments
// are a snapshot (data is never changed in place), so it needs no lock but diskMu.
func (fp *filePart) persist(gen uint64, committed partition.Pos, data map[string]record) error {
	fp.diskMu.Lock()
	defer fp.diskMu.Unlock()
	if gen <= fp.onDisk.Load() {
		return nil
	}
	b, err := json.Marshal(dataFile{Committed: committed, Records: data})
	if err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(fp.dir, "data.json"), b); err != nil {
		return err
	}
	fp.onDisk.Store(gen)
	return nil
}

// flush makes everything committed or imported so far durable.
func (fp *filePart) flush() error {
	fp.mu.RLock()
	gen, committed, data := fp.gen, fp.st.Committed, fp.data
	fp.mu.RUnlock()
	return fp.persist(gen, committed, data)
}

// flushSoon starts a background flush unless one is running. It repeats while commits keep arriving, so a burst of
// them costs few rewrites; it gives up on an error, which leaves the data dirty for the next SetPending or SetPromised
// to retry (and report).
func (fp *filePart) flushSoon() {
	if !fp.flushing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		for {
			err := fp.flush()
			fp.flushing.Store(false)
			fp.mu.RLock()
			dirty := fp.gen > fp.onDisk.Load()
			fp.mu.RUnlock()
			if err != nil || !dirty || !fp.flushing.CompareAndSwap(false, true) {
				return
			}
		}
	}()
}

// Sync makes everything committed so far durable. A server calls it when it stops.
func (s *fileStore) Sync() error {
	for _, fp := range s.parts {
		if err := fp.flush(); err != nil {
			return err
		}
	}
	return nil
}

// pending returns the stored pending write unless it is decided already (its sequence is not above the committed one).
func (fp *filePart) pending() *partition.Write {
	if p := fp.st.Pending; p != nil && p.Pos.Seq > fp.st.Committed.Seq {
		return p
	}
	return nil
}

func copyWrite(w *partition.Write) *partition.Write {
	if w == nil {
		return nil
	}
	c := *w
	c.Data = bytes.Clone(w.Data)
	return &c
}

func (s *fileStore) part(p int) (*filePart, error) {
	if p < 0 || p >= len(s.parts) {
		return nil, fmt.Errorf("partition %d out of range", p)
	}
	return s.parts[p], nil
}

// State implements partition.Store.
func (s *fileStore) State(p int) (partition.State, error) {
	fp, err := s.part(p)
	if err != nil {
		return partition.State{}, err
	}
	fp.mu.RLock()
	defer fp.mu.RUnlock()
	return partition.State{Promised: fp.st.Promised, Committed: fp.st.Committed, Pending: copyWrite(fp.pending())}, nil
}

// SetPromised implements partition.Store.
func (s *fileStore) SetPromised(p int, e partition.Epoch) error {
	fp, err := s.part(p)
	if err != nil {
		return err
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if err := fp.saveState(e, fp.pending()); err != nil {
		return err
	}
	fp.st.Promised = e
	return nil
}

// SetPending implements partition.Store.
func (s *fileStore) SetPending(p int, w *partition.Write) error {
	fp, err := s.part(p)
	if err != nil {
		return err
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	w = copyWrite(w)
	if err := fp.saveState(fp.st.Promised, w); err != nil {
		return err
	}
	fp.st.Pending = w
	return nil
}

// Commit implements partition.Store: the write's records and the new committed position replace the partition's data in
// one step. It does not wait for the disk (see the top of the file): data.json is rewritten by a background flush.
func (s *fileStore) Commit(w *partition.Write) error {
	fp, err := s.part(w.Partition)
	if err != nil {
		return err
	}
	var recs []changeKeyed
	if err := json.Unmarshal(w.Data, &recs); err != nil {
		return fmt.Errorf("commit: undecodable write data: %w", err)
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if w.Prev != fp.st.Committed {
		return fmt.Errorf("commit of %s with prev %s, committed is %s", w.Pos, w.Prev, fp.st.Committed)
	}
	next := make(map[string]record, len(fp.data)+len(recs))
	for k, r := range fp.data {
		next[k] = r
	}
	for _, kr := range recs {
		kr.Seq = w.Pos.Seq
		next[kr.Key] = kr.record
	}
	fp.data, fp.st.Committed = next, w.Pos
	fp.gen++
	fp.flushSoon()
	return nil
}

// Prepare implements partition.Store: it checks the change against the committed data and returns the write that
// makes it. The version numbers are decided here, on the owner, so every node applies the same bytes. The reply is the
// key's new version for a set (the caller gets it as the ETag), and nothing for a delete. A refusal carries a code
// (partition.Refuse) that crosses the network with it.
func (s *fileStore) Prepare(p int, raw []byte) (data, reply []byte, err error) {
	fp, err := s.part(p)
	if err != nil {
		return nil, nil, err
	}
	var c change
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, nil, fmt.Errorf("undecodable change: %w", err)
	}
	fp.mu.RLock()
	cur, exists := fp.data[c.Key]
	fp.mu.RUnlock()
	if c.Expect != nil && cur.live() != *c.Expect {
		return nil, nil, partition.Refuse(refusalConflict,
			fmt.Sprintf("key %q is at version %d, expected %d", c.Key, cur.live(), *c.Expect))
	}
	next := record{Ver: cur.Ver + 1} // Ver of a tombstone counts on, so a recreated key never repeats a version
	switch c.Op {
	case "set":
		next.Val = c.Val
		reply = []byte(strconv.FormatUint(next.Ver, 10))
	case "delete":
		if !exists || cur.Del {
			return nil, nil, partition.Refuse(refusalNotFound, fmt.Sprintf("key %q does not exist", c.Key))
		}
		next.Del = true
	default:
		return nil, nil, fmt.Errorf("unknown operation %q", c.Op)
	}
	data, err = json.Marshal([]changeKeyed{{Key: c.Key, record: next}})
	return data, reply, err
}

// readAnswer is what Read answers for a key: its value and version.
type readAnswer struct {
	Val []byte `json:"val"`
	Ver uint64 `json:"ver"`
}

// Read implements partition.Store: a lookup of one key (req is its name) in the committed data, answered as a readAnswer.
// A key that is absent or deleted is a refusal with the code not-found, which reaches the caller of Manager.Read as a
// *partition.RefusedError, like the refusal of a delete of a missing key. It does not see a pending write.
func (s *fileStore) Read(p int, req []byte) ([]byte, error) {
	val, ver, ok := s.get(p, string(req))
	if !ok {
		return nil, partition.Refuse(refusalNotFound, fmt.Sprintf("key %q does not exist", req))
	}
	return json.Marshal(readAnswer{Val: val, Ver: ver})
}

// exportBody is what Export streams and Import reads: one JSON document, so a stream that is cut short cannot be
// mistaken for a complete one.
type exportBody struct {
	Records map[string]record `json:"records"`
}

// Export implements partition.Store: the records written after sequence after, as one consistent snapshot.
func (s *fileStore) Export(p int, after uint64) (partition.Pos, io.ReadCloser, error) {
	fp, err := s.part(p)
	if err != nil {
		return partition.Pos{}, nil, err
	}
	fp.mu.RLock()
	defer fp.mu.RUnlock()
	out := exportBody{Records: map[string]record{}}
	for k, r := range fp.data {
		if r.Seq > after {
			out.Records[k] = r
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		return partition.Pos{}, nil, err
	}
	return fp.st.Committed, io.NopCloser(bytes.NewReader(b)), nil
}

// Import implements partition.Store.
func (s *fileStore) Import(p int, r io.Reader, upTo partition.Pos, full bool) error {
	fp, err := s.part(p)
	if err != nil {
		return err
	}
	var in exportBody
	if err := json.NewDecoder(r).Decode(&in); err != nil {
		return fmt.Errorf("import: %w", err) // a stream cut short fails here and nothing is applied
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if upTo.Seq <= fp.st.Committed.Seq {
		return fmt.Errorf("import up to %s, committed is %s", upTo, fp.st.Committed)
	}
	next := map[string]record{}
	if !full {
		for k, r := range fp.data {
			next[k] = r
		}
	}
	for k, r := range in.Records {
		if old, ok := next[k]; !ok || r.Seq >= old.Seq { // the snapshot never has less than this node had
			next[k] = r
		}
	}
	fp.data, fp.st.Committed = next, upTo
	fp.gen++
	fp.flushSoon()
	return nil
}

// Intact implements partition.Store; see openFileStore.
func (s *fileStore) Intact() bool { return s.intact }

// get is a local read of key: its value and version, false if absent or deleted.
func (s *fileStore) get(p int, key string) (val []byte, ver uint64, ok bool) {
	fp, err := s.part(p)
	if err != nil {
		return nil, 0, false
	}
	fp.mu.RLock()
	defer fp.mu.RUnlock()
	r, found := fp.data[key]
	if !found || r.Del {
		return nil, 0, false
	}
	return bytes.Clone(r.Val), r.Ver, true
}

// keys is the number of live keys in p.
func (s *fileStore) keys(p int) int {
	fp, err := s.part(p)
	if err != nil {
		return 0
	}
	fp.mu.RLock()
	defer fp.mu.RUnlock()
	n := 0
	for _, r := range fp.data {
		if !r.Del {
			n++
		}
	}
	return n
}

var _ partition.Store = (*fileStore)(nil)
