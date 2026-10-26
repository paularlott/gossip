package partition

import "io"

// Store is the durable side of the protocol, implemented by the application.
//
// Concurrency. The Manager serialises everything that changes one partition's state, but Store methods for different
// partitions run concurrently, and State and Export may be called while a change to the same partition is in progress,
// so a Store must be safe for concurrent use.
//
// Durability. The contract is simple: Commit, Import, SetPromised and SetPending are all durable (fsync, or a database
// transaction commit) before they return, and Commit and Import are also atomic: a crash leaves the state from before
// the call or from after it, never something between. A promise or a stored write that a crash can forget lets a node
// accept what it should refuse, or count toward a majority for a write it no longer holds; a lost commit or import is
// the same loss of a write this node acknowledged.
//
// Optional optimisation: deferred durability of Commit and Import. A write's safety comes from its being stored as
// pending on a majority; if a crash loses a Commit the node restarts with Committed at the write before and the write
// still pending, which every rule of the protocol already handles (a follower commits it when the owner's committed
// position says so, a new owner settles it). So a Store may let Commit and Import return before they are durable, and
// then a write waits for the slower of the owner's durable SetPending and the followers' (all of them run at the same
// time), and for no durable Commit at all. A Store may do this only if it guarantees the ordering rule: a crash must
// never keep a later SetPending or SetPromised while forgetting an earlier Commit or Import (the pending slot would then
// not follow Committed, and the write that was committed would be gone from this node), so a durable SetPending or
// SetPromised must also make everything committed or imported before it durable. A store that keeps committed data in
// one file and the protocol state in another has to flush the first before it writes the second. A Store that cannot
// guarantee this must make Commit and Import durable.
//
// Positions. A sequence number may carry different epoch labels on different nodes for the same write (an owner commits
// write s, loses ownership before the others hear of it, and the next owner re-proposes the same data as s under its own
// epoch). Anything a Store keys by position must key by Seq, and Export's "changed after" is by Seq. Commit is called
// with w.Prev rewritten to this node's own committed position.
type Store interface {
	// State returns the durable state of partition p. It is called often (several times per write, for catch-ups and
	// state requests) and sometimes without the partition's mutex, so it must be cheap (serve it from memory) and safe to
	// call while the partition changes: Committed and Promised only move forward, so a value that is a moment stale is a
	// lower bound. It must reflect every SetPromised, SetPending, Commit and Import that has returned.
	//
	// The result is read-only for the package: it never changes the State or the Write that Pending points to (it
	// copies a write before changing it), so a Store may hand out the pending write it keeps without copying it. The
	// application must not change a Write it has handed out either, nor one the package has passed to it.
	State(p int) (State, error)

	// SetPromised durably records Promised for p (fsync before returning). With deferred durability of Commit and Import
	// it also makes durable everything committed or imported before it.
	SetPromised(p int, e Epoch) error

	// SetPending durably records (fsync before returning) or, with nil, clears the pending write of p. With deferred
	// durability of Commit and Import it also makes durable everything committed or imported before it. It is the
	// acknowledgement of a write: once it returns, this node counts toward the write's majority.
	SetPending(p int, w *Write) error

	// Commit applies w's Data to p's visible data and sets Committed = w.Pos, atomically and durably (deferred durability
	// is allowed only under the ordering rule; see Durability above). If Pending equals w it is cleared in the same step. Commit is only ever called with
	// w.Prev equal to this node's own Committed (not necessarily the position the owner labelled it with).
	Commit(w *Write) error

	// Prepare runs on the owner, serialised per partition: it checks change against p's committed data and returns the
	// Data of the write that makes it, and a reply for the caller of Manager.Write (a new version, a generated id; nil is
	// fine), which reaches the caller in Result.Reply. A refusal (expected version moved, uniqueness, ...) is an error
	// made with Refuse, or any error wrapping ErrRefused, which the package returns to the caller as a *RefusedError;
	// any other error is a failure. Prepare must not change anything: its result may never be committed.
	Prepare(p int, change []byte) (data, reply []byte, err error)

	// Read answers req from p's committed data and must not change anything. It runs on the owner for Manager.Read,
	// after the owner has confirmed that it still owns p, and without the partition's mutex: it may run while a write is
	// being committed or a catch-up imported, so, like State and Export, it must see the committed data as it was before
	// or after such a call, never between (a write is visible only once its Commit has returned). Pending writes are not
	// part of the committed data and Read must not see them. A refusal (an unknown key, a malformed request) is an error
	// made with Refuse, or any error wrapping ErrRefused, which the package returns to the caller as a *RefusedError; any
	// other error is a failure and the caller is told ErrUnavailable. The answer travels to the caller of Manager.Read in
	// one message, like a change.
	Read(p int, req []byte) ([]byte, error)

	// Export returns the committed data of p changed after sequence after (after = 0: all of it), as one consistent
	// snapshot, and the Pos it reflects (its Committed at the time). The caller closes the reader. It returns
	// ErrPurged if after is older than the oldest point it can export changes from (the application purged history
	// it needs). The format must be self-delimiting, so that Import can tell a stream cut short from a complete one.
	Export(p int, after uint64) (Pos, io.ReadCloser, error)

	// Import applies what Export returned and sets Committed = upTo, durably (as for Commit, deferred durability is allowed only under the ordering rule). With
	// full, p's data is replaced (records absent from the stream are removed). A Pending with Seq <= upTo.Seq is
	// cleared. It is only called with upTo.Seq > Committed.Seq. Committed moves only once the whole stream has been
	// applied: a stream that ends before its end marker must make Import fail without moving Committed. A large import
	// may apply the stream's records in batches as they arrive; then a failed or interrupted import may leave some of
	// them applied with Committed where it was. That is safe: they are committed data of the source, never anything
	// uncommitted, and the next catch-up (which every takeover does before it writes) applies the rest and moves
	// Committed. A Store that can apply the whole stream in one transaction should.
	Import(p int, r io.Reader, upTo Pos, full bool) error

	// Intact reports whether this node's state can be trusted: false for a node whose data was lost or restored, which
	// must catch up from enough of the other nodes that every majority includes one (size - majority + 1) before it
	// promises or stores anything (resync mode). It is read
	// once, at Start; Manager.HandleResyncedFunc tells the application when the resync has finished.
	Intact() bool
}
