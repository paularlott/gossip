package partition

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"math"

	"github.com/paularlott/gossip"
)

var (
	// ErrRefused means the application's Store refused the change (the version it expected has moved, a uniqueness
	// rule, ...) and nothing was written. Errors returned by Manager.Write wrap it.
	ErrRefused = errors.New("partition: change refused")

	// ErrUnknown means the change was sent but its outcome is not known: it timed out, or the owner was lost after
	// replication began. It may or may not have been committed.
	ErrUnknown = errors.New("partition: outcome unknown")

	// ErrUnavailable means no owner could be reached, no majority was available or this node was not ready within
	// the write timeout. Nothing was replicated.
	ErrUnavailable = errors.New("partition: unavailable")

	// ErrPurged is returned by Store.Export when the history it would need to export changes after the given
	// sequence has been purged by the application; the caller then asks for a full export.
	ErrPurged = errors.New("partition: history purged")

	// ErrExhausted means an epoch counter or a sequence number would have reached math.MaxUint64. Counters never
	// wrap: the operation is refused and nothing is done.
	ErrExhausted = errors.New("partition: counter exhausted")

	// ErrSizeMismatch means a peer is configured with a different ClusterSize.
	ErrSizeMismatch = errors.New("partition: cluster size mismatch")
)

// RefusedError is the error a Store refuses a change with, and the error Manager.Write returns for a refusal, on the
// owner and on any other node alike: errors.As(err, &re) finds it, errors.Is(err, ErrRefused) is true, and Code
// survives the trip from the owner to the node that called Write. A Store refuses with Refuse; an error that merely wraps
// ErrRefused is refused too, with Code "" and its text as Msg.
type RefusedError struct {
	// Code is the application's machine-readable reason ("conflict", "not-found", ...); "" if it gave none.
	Code string
	// Msg is the human-readable reason.
	Msg string
	// Pos is the owner's committed position when it refused (set by the package; a Store leaves it zero): the state the
	// refusal was decided against. After a refusal for a stale expectation (a compare-and-swap whose version moved), an
	// application that re-reads its local copy first waits for it to reach Pos (Manager.WaitApplied(ctx, p, Pos)), so that
	// it does not read the same stale state again.
	Pos Pos
}

// Refuse returns the error a Store's Prepare returns to refuse a change: a *RefusedError with the given code and message.
func Refuse(code, msg string) error { return &RefusedError{Code: code, Msg: msg} }

// Error returns Msg, or the text of ErrRefused if there is none.
func (e *RefusedError) Error() string {
	if e.Msg == "" {
		return ErrRefused.Error()
	}
	return e.Msg
}

// Is makes a RefusedError match ErrRefused.
func (e *RefusedError) Is(target error) bool { return target == ErrRefused }

// refusal returns err as a *RefusedError: the one it is or wraps, or one that carries its text.
func refusal(err error) *RefusedError {
	var re *RefusedError
	if errors.As(err, &re) {
		return re
	}
	return &RefusedError{Msg: err.Error()}
}

// Result is what a successful Manager.Write returns.
type Result struct {
	// Pos is the position of the committed write. Manager.WaitApplied(ctx, p, Pos) waits until this node has applied it.
	Pos Pos
	// Reply is what the owner's Store.Prepare returned for the caller (a new version, a generated id), carried back
	// unchanged to the node that called Write.
	Reply []byte
}

// Epoch identifies one tenure of ownership of a partition. Epochs are ordered by Counter and then by Node (byte-wise
// compare of the ID); the zero Epoch is the lowest.
type Epoch struct {
	// Counter is the tenure number, always higher than any counter the proposer had seen for the partition.
	Counter uint64 `msgpack:"c" json:"c"`
	// Node is the node that proposed the epoch, which breaks ties between concurrent proposals.
	Node gossip.NodeID `msgpack:"n" json:"n"`
}

// Compare returns -1, 0 or 1 as e is lower than, equal to or higher than o.
func (e Epoch) Compare(o Epoch) int {
	switch {
	case e.Counter < o.Counter:
		return -1
	case e.Counter > o.Counter:
		return 1
	}
	return bytes.Compare(e.Node[:], o.Node[:])
}

// Less reports whether e is lower than o.
func (e Epoch) Less(o Epoch) bool { return e.Compare(o) < 0 }

// IsZero reports whether e is the zero Epoch.
func (e Epoch) IsZero() bool { return e == Epoch{} }

// String returns a short form for logs.
func (e Epoch) String() string { return fmt.Sprintf("%d@%.8s", e.Counter, e.Node.String()) }

// Pos is the position of a committed write. Seq is per partition, starts at 1 for the first write, never resets (not
// across epochs either) and is the order of writes. Two writes are the same write if and only if their Pos are
// equal.
type Pos struct {
	// Epoch is the epoch of the owner that wrote it.
	Epoch Epoch `msgpack:"e" json:"e"`
	// Seq is the write's sequence number in the partition.
	Seq uint64 `msgpack:"s" json:"s"`
}

// IsZero reports whether p is the zero Pos (no write).
func (p Pos) IsZero() bool { return p == Pos{} }

// String returns a short form for logs.
func (p Pos) String() string { return fmt.Sprintf("%d/%s", p.Seq, p.Epoch) }

// Write is one write of a partition: Data, produced by the application on the owner (Store.Prepare), at position Pos,
// following the write at Prev (the zero Pos for Seq 1).
type Write struct {
	// Partition is the partition the write belongs to.
	Partition int `msgpack:"p" json:"p"`
	// Pos is the write's position.
	Pos Pos `msgpack:"pos" json:"pos"`
	// Prev is the position of the write before it.
	Prev Pos `msgpack:"prev" json:"prev"`
	// Data is the opaque bytes the application's Store applies on commit.
	Data []byte `msgpack:"d,omitempty" json:"d,omitempty"`
}

// State is the durable state of one partition, kept by the application's Store.
//
// The package maintains the invariant Pending == nil || Pending.Pos.Seq == Committed.Seq+1.
type State struct {
	// Promised is the highest epoch promised.
	Promised Epoch
	// Committed is the last committed write, applied to the visible data.
	Committed Pos
	// Pending is the at most one stored write that is not known to be committed; nil if none.
	Pending *Write
}

// majority is the number of nodes that make a majority of a cluster of size nodes. It is computed from the
// configured size, never from the nodes that are visible.
func majority(size int) int { return size/2 + 1 }

// resyncQuorum is how many of the other nodes a node that lost its data must hear from before it trusts what it has
// caught up: enough that every majority (which every committed write is stored on) includes one of them, whatever
// nodes the lost data belonged to. Of the size-1 other nodes at most size-majority hold none of a committed write, so
// size-majority+1 answers always include a holder: 2 of 3, 1 of 2, 2 of 4, 3 of 5.
func resyncQuorum(size int) int { return size - majority(size) + 1 }

// increment returns n+1, or ErrExhausted if that would reach math.MaxUint64: an epoch counter or a sequence number never
// wraps.
func increment(n uint64) (uint64, error) {
	if n >= math.MaxUint64-1 {
		return 0, ErrExhausted
	}
	return n + 1, nil
}

// PartitionOf maps key to a partition in [1, partitions-1] (1 + fnv64a(key) % (partitions-1)); partition 0 is left for
// the application's global data. With a single partition it returns 0.
func PartitionOf(key string, partitions int) int {
	if partitions <= 1 {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(key))
	return 1 + int(h.Sum64()%uint64(partitions-1))
}
