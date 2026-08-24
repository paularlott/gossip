package kv

import "errors"

var (
	// ErrStoreClosed means the store has been shut down.
	ErrStoreClosed = errors.New("kv: store closed")

	// ErrKeyEmpty means the key was the empty string, which no entry may use.
	ErrKeyEmpty = errors.New("kv: key must not be empty")

	// ErrValueTooLarge means the value exceeded the configured MaxValueSize.
	// The write was rejected before any replication was attempted.
	ErrValueTooLarge = errors.New("kv: value too large")

	// ErrTTLOutOfRange means the requested TTL was negative or exceeded the
	// configured MaxTTL.
	ErrTTLOutOfRange = errors.New("kv: TTL out of range")

	// ErrTooManyKeys means the store is at its configured MaxKeys cap and the
	// write would have introduced a new key.
	ErrTooManyKeys = errors.New("kv: key cap reached")

	// ErrWriteQuorum means a Set could not be made durable on the configured
	// number of nodes. The write has not taken effect: any local copy has
	// been compensated with a tombstone, and a value half-landed on a peer is
	// dominated by that tombstone. Deletes are different in kind — a delete
	// that falls short of quorum stands and converges via anti-entropy, so
	// the error means "not confirmed durable", never "undone".
	ErrWriteQuorum = errors.New("kv: write not durable")
)
