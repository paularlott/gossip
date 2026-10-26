package partition

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/paularlott/gossip"
)

// memStore is an in-memory Store for tests, with fault hooks. Committed data is a list of (Seq, Data) entries; an
// export is the entries after a sequence, as JSON.
type memStore struct {
	mu     sync.Mutex
	parts  map[int]*memPart
	intact bool

	// failNext makes the next call of the named method (State, SetPromised, SetPending, Commit, Prepare, Export,
	// Import) return the error, once.
	failNext map[string]error
	// purgedBefore: Export with 0 < after < purgedBefore returns ErrPurged.
	purgedBefore uint64
	// onSetPromised runs at the start of SetPromised, before it takes effect; it may block.
	onSetPromised func(p int, e Epoch)
	// onSetPending runs at the start of SetPending of a write (not of a clear), before it takes effect; it may block.
	onSetPending func(p int, w *Write)
	// calls records the mutating calls in order, as "Method(p)".
	calls []string
	// exports counts Export calls (a catch-up reads the serving node's store this way).
	exports int

	// syncCost is how long SetPromised and SetPending take before they take effect, and commitCost how long Commit and
	// Import take: the fsync a durable Store pays for the first two, and the one a Store that made Commit durable would
	// pay. They are separate so that a benchmark can show what a Commit that need not be durable saves. (Set before use.)
	syncCost, commitCost time.Duration

	// undo is the state of a partition before the last Commit or Import, which a crash that loses that call (a Commit need
	// not be durable) brings back; see loseLastCommit. A SetPromised or SetPending makes it durable, as the Store contract
	// requires of them, and clears it.
	undo *memUndo
}

// memUndo is what loseLastCommit restores: the partition's state, and its committed entries (the first n of them: they
// only ever grow, except in a full import, which keeps a copy in saved).
type memUndo struct {
	p     int
	st    State
	n     int
	saved []memEntry
}

type memPart struct {
	st      State
	entries []memEntry
}

type memEntry struct {
	Seq  uint64 `json:"s"`
	Data []byte `json:"d"`
}

func newMemStore() *memStore {
	return &memStore{parts: map[int]*memPart{}, intact: true, failNext: map[string]error{}}
}

func (s *memStore) part(p int) *memPart {
	mp := s.parts[p]
	if mp == nil {
		mp = &memPart{}
		s.parts[p] = mp
	}
	return mp
}

// hook returns the armed failure for method, if any, and records mutating calls. The caller holds s.mu.
func (s *memStore) hook(method string, p int, mutating bool) error {
	if mutating {
		s.calls = append(s.calls, fmt.Sprintf("%s(%d)", method, p))
	}
	if err, ok := s.failNext[method]; ok {
		delete(s.failNext, method)
		return err
	}
	return nil
}

func (s *memStore) failOnce(method string, err error) {
	s.mu.Lock()
	s.failNext[method] = err
	s.mu.Unlock()
}

func copyWrite(w *Write) *Write {
	if w == nil {
		return nil
	}
	c := *w
	c.Data = append([]byte(nil), w.Data...)
	return &c
}

func (s *memStore) State(p int) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.hook("State", p, false); err != nil {
		return State{}, err
	}
	st := s.part(p).st
	st.Pending = copyWrite(st.Pending)
	return st, nil
}

// pay sleeps for d, outside the store's lock.
func pay(d time.Duration) {
	if d > 0 {
		time.Sleep(d)
	}
}

// snapshot records p's state for undo; full says the call about to run replaces the committed entries.
func (s *memStore) snapshot(p int, full bool) *memUndo {
	mp := s.part(p)
	u := &memUndo{p: p, st: mp.st, n: len(mp.entries)}
	u.st.Pending = copyWrite(mp.st.Pending)
	if full {
		u.saved = append([]memEntry(nil), mp.entries...)
	}
	return u
}

// loseLastCommit is a crash that loses the last Commit or Import (neither is durable): the partition goes back to what it
// was before, a node that holds the write as pending again. It reports whether there was one to lose.
func (s *memStore) loseLastCommit() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.undo == nil {
		return false
	}
	u, mp := s.undo, s.part(s.undo.p)
	mp.st = u.st
	if u.saved != nil {
		mp.entries = u.saved
	} else {
		mp.entries = mp.entries[:u.n]
	}
	s.undo = nil
	return true
}

func (s *memStore) SetPromised(p int, e Epoch) error {
	if s.onSetPromised != nil {
		s.onSetPromised(p, e)
	}
	pay(s.syncCost)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.hook("SetPromised", p, true); err != nil {
		return err
	}
	s.undo = nil
	s.part(p).st.Promised = e
	return nil
}

func (s *memStore) SetPending(p int, w *Write) error {
	if s.onSetPending != nil && w != nil {
		s.onSetPending(p, copyWrite(w))
	}
	pay(s.syncCost)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.hook("SetPending", p, true); err != nil {
		return err
	}
	s.undo = nil
	s.part(p).st.Pending = copyWrite(w)
	return nil
}

func (s *memStore) Commit(w *Write) error {
	pay(s.commitCost)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.hook("Commit", w.Partition, true); err != nil {
		return err
	}
	mp := s.part(w.Partition)
	if w.Prev != mp.st.Committed {
		return fmt.Errorf("memStore: commit of %s with prev %s, committed is %s", w.Pos, w.Prev, mp.st.Committed)
	}
	s.undo = s.snapshot(w.Partition, false)
	mp.entries = append(mp.entries, memEntry{Seq: w.Pos.Seq, Data: append([]byte(nil), w.Data...)})
	mp.st.Committed = w.Pos
	if mp.st.Pending != nil && mp.st.Pending.Pos == w.Pos {
		mp.st.Pending = nil
	}
	return nil
}

// Prepare refuses a change that starts with "refuse" (wrapping ErrRefused, no code) or "conflict" (Refuse with the code
// "conflict"), fails one that starts with "fail", and otherwise makes the change itself the write's data and "ok:" + change
// its reply.
func (s *memStore) Prepare(p int, change []byte) ([]byte, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.hook("Prepare", p, false); err != nil {
		return nil, nil, err
	}
	switch {
	case strings.HasPrefix(string(change), "refuse"):
		return nil, nil, fmt.Errorf("memStore: %w: %s", ErrRefused, change)
	case strings.HasPrefix(string(change), "conflict"):
		return nil, nil, Refuse("conflict", fmt.Sprintf("memStore: %s", change))
	case strings.HasPrefix(string(change), "fail"):
		return nil, nil, errors.New("memStore: prepare failed")
	}
	return append([]byte(nil), change...), append([]byte("ok:"), change...), nil
}

// Read answers a request from the committed data: "seq" is the committed sequence number, "data" (or anything else) the
// committed entries' data joined by ",". A request that starts with "refuse" is refused (wrapping ErrRefused, no code), one
// that starts with "conflict" is refused with the code "conflict", and one that starts with "fail" fails. Read changes
// nothing, and does not see the pending write.
func (s *memStore) Read(p int, req []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.hook("Read", p, false); err != nil {
		return nil, err
	}
	switch {
	case strings.HasPrefix(string(req), "refuse"):
		return nil, fmt.Errorf("memStore: %w: %s", ErrRefused, req)
	case strings.HasPrefix(string(req), "conflict"):
		return nil, Refuse("conflict", fmt.Sprintf("memStore: %s", req))
	case strings.HasPrefix(string(req), "fail"):
		return nil, errors.New("memStore: read failed")
	}
	mp := s.part(p)
	if string(req) == "seq" {
		return []byte(fmt.Sprint(mp.st.Committed.Seq)), nil
	}
	var out []string
	for _, e := range mp.entries {
		out = append(out, string(e.Data))
	}
	return []byte(strings.Join(out, ",")), nil
}

func (s *memStore) Export(p int, after uint64) (Pos, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exports++
	if err := s.hook("Export", p, false); err != nil {
		return Pos{}, nil, err
	}
	if after > 0 && after < s.purgedBefore {
		return Pos{}, nil, ErrPurged
	}
	mp := s.part(p)
	var out []memEntry
	for _, e := range mp.entries {
		if e.Seq > after {
			out = append(out, e)
		}
	}
	b, _ := json.Marshal(out)
	return mp.st.Committed, io.NopCloser(bytes.NewReader(b)), nil
}

func (s *memStore) Import(p int, r io.Reader, upTo Pos, full bool) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	pay(s.commitCost)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.hook("Import", p, true); err != nil {
		return err
	}
	mp := s.part(p)
	if upTo.Seq <= mp.st.Committed.Seq {
		return fmt.Errorf("memStore: import up to %s, committed is %s", upTo, mp.st.Committed)
	}
	var in []memEntry
	if err := json.Unmarshal(body, &in); err != nil {
		return err
	}
	s.undo = s.snapshot(p, full)
	if full {
		mp.entries = nil
	}
	for _, e := range in {
		if len(mp.entries) == 0 || e.Seq > mp.entries[len(mp.entries)-1].Seq {
			mp.entries = append(mp.entries, e)
		}
	}
	mp.st.Committed = upTo
	if mp.st.Pending != nil && mp.st.Pending.Pos.Seq <= upTo.Seq {
		mp.st.Pending = nil
	}
	return nil
}

func (s *memStore) Intact() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.intact
}

// --- test helpers ---

// state is State for tests that cannot fail.
func (s *memStore) state(p int) State {
	st, err := s.State(p)
	if err != nil {
		panic(err)
	}
	return st
}

// data returns the committed data of p in sequence order.
func (s *memStore) data(p int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, e := range s.part(p).entries {
		out = append(out, string(e.Data))
	}
	return out
}

// seedCommitted gives p committed writes 1..n at epoch e, with data "d<seq>".
func (s *memStore) seedCommitted(p, n int, e Epoch) {
	for seq := 1; seq <= n; seq++ {
		st := s.state(p)
		w := &Write{Partition: p, Pos: Pos{Epoch: e, Seq: uint64(seq)}, Prev: st.Committed, Data: []byte(fmt.Sprintf("d%d", seq))}
		if err := s.Commit(w); err != nil {
			panic(err)
		}
	}
}

// seedPending sets p's pending write.
func (s *memStore) seedPending(p int, w Write) {
	w.Partition = p
	if err := s.SetPending(p, &w); err != nil {
		panic(err)
	}
}

// exportCount is the number of Export calls so far.
func (s *memStore) exportCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exports
}

func (s *memStore) callLog() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// checkInvariant reports a violation of Pending == nil || Pending.Seq == Committed.Seq+1 for every partition.
func (s *memStore) checkInvariant() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p, mp := range s.parts {
		if pe := mp.st.Pending; pe != nil && pe.Pos.Seq != mp.st.Committed.Seq+1 {
			return fmt.Errorf("partition %d: pending %s with committed %s", p, pe.Pos, mp.st.Committed)
		}
	}
	return nil
}

// nid makes a node ID from a number.
func nid(n int) gossip.NodeID {
	var id gossip.NodeID
	id[0], id[15] = byte(n*37), byte(n)
	return id
}

// seedCommittedAt commits one more write at seq (which must follow the committed one), with data "d<seq>".
func (s *memStore) seedCommittedAt(p, seq int, e Epoch) {
	st := s.state(p)
	w := &Write{Partition: p, Pos: Pos{Epoch: e, Seq: uint64(seq)}, Prev: st.Committed, Data: []byte(fmt.Sprintf("d%d", seq))}
	if err := s.Commit(w); err != nil {
		panic(err)
	}
}
