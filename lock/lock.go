package lock

import (
	"errors"
	"time"
)

// Reason strings sent over the wire when the leader refuses a request.
const (
	reasonNotLeader   = "not leader"
	reasonRecovering  = "recovering state"
	reasonWriteQuorum = "write not durable"
)

var (
	// ErrLockNotAcquired means the key is currently held by another node.
	ErrLockNotAcquired = errors.New("lock: not acquired")

	// ErrTTLOutOfRange means the requested TTL falls outside [MinTTL, MaxTTL].
	ErrTTLOutOfRange = errors.New("lock: TTL out of range")

	// ErrNoLeader means no leader is currently available to serve the request.
	// This is transient: blocking Acquire retries through it.
	ErrNoLeader = errors.New("lock: no leader available")

	// ErrWarmingUp means a newly elected leader is still letting locks issued by
	// its predecessor expire. Also transient.
	ErrWarmingUp = errors.New("lock: leader warming up")

	// ErrPoolClosed means the pool has been shut down.
	ErrPoolClosed = errors.New("lock: pool closed")

	// ErrWriteQuorum means a mutation could not be made durable on the
	// configured number of replicas, and has therefore not taken effect.
	// Transient: a blocking Acquire retries through it.
	ErrWriteQuorum = errors.New("lock: write not durable")
)

// Lock is a held distributed lock.
//
// Callers should Release when done. TTL expiry is the safety net if the holder
// crashes, so a lock is never held indefinitely.
type Lock struct {
	pool  *Pool
	key   string
	token Token
}

// Release gives up the lock. Safe to call more than once; later calls are no-ops.
// Also safe on a nil Lock, so `lk, _ := pool.TryAcquire(...); defer lk.Release()`
// cannot panic when the acquire failed.
//
// An error means the leader would not honour the release — typically because the
// lock had already expired and been taken by somebody else, in which case this
// holder was no longer the owner anyway.
func (l *Lock) Release() error {
	if l == nil || l.pool == nil {
		return nil
	}
	pool := l.pool
	l.pool = nil
	return pool.release(l.key, l.token)
}

// Extend refreshes the lock's TTL. ttl is required and must be within the pool's
// configured bounds.
func (l *Lock) Extend(ttl time.Duration) error {
	if l == nil || l.pool == nil {
		return ErrLockNotAcquired
	}
	return l.pool.extend(l.key, l.token, ttl)
}

// Key returns the locked key.
func (l *Lock) Key() string {
	if l == nil {
		return ""
	}
	return l.key
}

// Token returns the fencing token for this acquisition.
//
// Tokens are ordered pairs of (election term, counter) and form a strict total
// order across leaders. Pass this to whatever resource the lock protects and
// have it reject anything carrying a token below the highest it has seen; that
// way a stale holder cannot act even if it still believes it holds the lock.
func (l *Lock) Token() Token {
	if l == nil {
		return Token{}
	}
	return l.token
}
