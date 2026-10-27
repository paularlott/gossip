package partition

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"testing"
)

func TestCatchUpStreamFraming(t *testing.T) {
	fc := newFakeCluster(t, 2, 3, nil)
	fc.attach()
	src := fc.nodes[0]
	e1 := ep(1, 1)
	src.store.seedCommitted(1, 4, e1)

	var buf bytes.Buffer
	if err := src.m.serveCatchUp(&catchUpReq{Size: 3, P: 1, After: 2}, &buf); err != nil {
		t.Fatal(err)
	}
	hdr, err := readCatchUpHeader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Err != "" || hdr.UpTo != (Pos{Epoch: e1, Seq: 4}) {
		t.Fatalf("header %+v", hdr)
	}
	// What follows the header is exactly the Store's export: the changes after 2.
	body, _ := io.ReadAll(&buf)
	_, r, _ := src.store.Export(1, 2)
	want, _ := io.ReadAll(r)
	if !bytes.Equal(body, want) {
		t.Errorf("body %q, want %q", body, want)
	}

	// Errors travel in the header, with no body.
	for name, tc := range map[string]struct {
		req  catchUpReq
		want string
	}{
		"size":      {catchUpReq{Size: 4, P: 1}, ErrSizeMismatch.Error()},
		"partition": {catchUpReq{Size: 3, P: 40}, "out of range"},
	} {
		buf.Reset()
		if err := src.m.serveCatchUp(&tc.req, &buf); err != nil {
			t.Fatal(err)
		}
		hdr, err := readCatchUpHeader(&buf)
		if err != nil || hdr.Err == "" || !bytes.Contains([]byte(hdr.Err), []byte(tc.want)) || buf.Len() != 0 {
			t.Errorf("%s: header %+v, %v, %d bytes left", name, hdr, err, buf.Len())
		}
	}
	src.store.purgedBefore = 3
	buf.Reset()
	src.m.serveCatchUp(&catchUpReq{Size: 3, P: 1, After: 1}, &buf)
	if hdr, _ := readCatchUpHeader(&buf); hdr.Err != ErrPurged.Error() {
		t.Errorf("purged export: header %+v", hdr)
	}
	// Full asks for everything whatever After says.
	buf.Reset()
	src.m.serveCatchUp(&catchUpReq{Size: 3, P: 1, After: 1, Full: true}, &buf)
	if hdr, _ := readCatchUpHeader(&buf); hdr.Err != "" {
		t.Errorf("full export: header %+v", hdr)
	}
}

func TestReadCatchUpHeaderRejectsGarbage(t *testing.T) {
	huge := make([]byte, 4)
	binary.BigEndian.PutUint32(huge, maxHeaderFrame+1)
	for name, in := range map[string][]byte{
		"empty":     nil,
		"short":     {0, 0},
		"zero":      {0, 0, 0, 0},
		"too large": huge,
		"truncated": {0, 0, 0, 9, '{'},
		"not json":  {0, 0, 0, 3, 'a', 'b', 'c'},
	} {
		if _, err := readCatchUpHeader(bytes.NewReader(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestCatchUpIncrementalAndFull(t *testing.T) {
	e1 := ep(1, 2)
	fc := newFakeCluster(t, 2, 3, nil)
	fc.attach()
	f, src := fc.nodes[0], fc.nodes[1]
	src.store.seedCommitted(1, 6, e1)
	f.store.seedCommitted(1, 2, e1)

	// Incremental: only what follows.
	if err := f.m.catchUp(t.Context(), 1, src.id, false); err != nil {
		t.Fatal(err)
	}
	if d := f.store.data(1); !slices.Equal(d, src.store.data(1)) {
		t.Fatalf("data %v", d)
	}
	if st := f.store.state(1); st.Committed != (Pos{Epoch: e1, Seq: 6}) {
		t.Fatalf("%+v", st)
	}

	// A source that purged the history needed answers ErrPurged, and the follower asks again for all of it.
	for i := 7; i <= 9; i++ {
		src.store.seedCommittedAt(1, i, e1)
	}
	src.store.purgedBefore = 8
	g := fc.nodes[0]
	g.store = newMemStore()
	g.m.store = g.store
	g.store.seedCommitted(1, 3, e1)
	if err := g.m.catchUp(t.Context(), 1, src.id, false); err != nil {
		t.Fatal(err)
	}
	if d := g.store.data(1); !slices.Equal(d, src.store.data(1)) {
		t.Fatalf("after a full transfer: %v, want %v", d, src.store.data(1))
	}
}

func TestCatchUpDiscardsABodyThatIsNotAhead(t *testing.T) {
	e1 := ep(1, 2)
	fc := newFakeCluster(t, 2, 3, nil)
	fc.attach()
	f, src := fc.nodes[0], fc.nodes[1]
	src.store.seedCommitted(1, 3, e1)
	f.store.seedCommitted(1, 3, e1)
	before := len(f.store.callLog())
	if err := f.m.catchUp(t.Context(), 1, src.id, false); err != nil {
		t.Fatal(err)
	}
	if len(f.store.callLog()) != before {
		t.Error("imported a body that was not ahead")
	}
}

func TestCatchUpFailures(t *testing.T) {
	e1 := ep(1, 2)
	fc := newFakeCluster(t, 2, 3, nil)
	fc.attach()
	f, src := fc.nodes[0], fc.nodes[1]
	src.store.seedCommitted(1, 3, e1)

	src.store.failOnce("Export", errors.New("disk"))
	if err := f.m.catchUp(t.Context(), 1, src.id, false); err == nil {
		t.Error("a source's failure was not reported")
	}
	f.store.failOnce("Import", errors.New("disk"))
	if err := f.m.catchUp(t.Context(), 1, src.id, false); err == nil {
		t.Error("an import failure was not reported")
	}
	if st := f.store.state(1); st.Committed.Seq != 0 {
		t.Errorf("a failed import moved committed: %+v", st)
	}
	src.down.Store(true)
	if err := f.m.catchUp(t.Context(), 1, src.id, false); err == nil {
		t.Error("an unreachable source was not reported")
	}
	src.down.Store(false)
	if err := f.m.catchUp(t.Context(), 1, src.id, false); err != nil {
		t.Fatal(err)
	}
}

func TestCatchUpAsyncRunsOnePerPartition(t *testing.T) {
	e1 := ep(1, 2)
	fc := newFakeCluster(t, 2, 3, nil)
	fc.attach()
	f, src := fc.nodes[0], fc.nodes[1]
	src.store.seedCommitted(1, 3, e1)

	f.m.catchingUp(1, true) // one is "in flight"
	f.m.catchUpAsync(1, src.id)
	f.m.wg.Wait()
	if st := f.store.state(1); st.Committed.Seq != 0 {
		t.Fatal("a second catch-up ran while one was in flight")
	}
	f.m.catchingUp(1, false)
	f.m.catchUpAsync(1, src.id)
	f.m.wg.Wait()
	if st := f.store.state(1); st.Committed.Seq != 3 {
		t.Fatalf("%+v", st)
	}
	if f.m.parts[1].catchingUp.Load() {
		t.Error("the in-flight flag was not cleared")
	}
}

// A partition whose full copy was cut short (Incomplete) asks for everything at once: the changes after its committed
// position would not restore the records the copy removed.
func TestCatchUpOfAnIncompletePartitionCopiesItWhole(t *testing.T) {
	e1 := ep(1, 2)
	fc := newFakeCluster(t, 2, 3, nil)
	fc.attach()
	f, src := fc.nodes[0], fc.nodes[1]
	src.store.seedCommitted(1, 6, e1)
	f.store.seedCommitted(1, 3, e1)
	f.store.seedIncomplete(1)
	if err := f.m.catchUp(t.Context(), 1, src.id, false); err != nil {
		t.Fatal(err)
	}
	if d := f.store.data(1); !slices.Equal(d, src.store.data(1)) {
		t.Fatalf("data %v, want %v", d, src.store.data(1))
	}
	if st := f.store.state(1); st.Incomplete || st.Committed != (Pos{Epoch: e1, Seq: 6}) {
		t.Fatalf("%+v", st)
	}
}
