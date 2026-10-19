package gossip

import (
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/paularlott/gossip/encryption"
)

// keyring holds the cluster's encryption keys: the first encrypts, every key decrypts, so a key can be rotated
// without stopping the cluster (Cluster.SetEncryptionKeys).
//
// Two formats are in use. The original ("legacy") one is the configured cipher's output under the configured key:
// nonce|ciphertext. The keyed one, used with an AEADCipher towards nodes that advertise the same key window, adds a
// single byte in front:
//
//	window parity (2 bits) | key hint (6 bits), then nonce|ciphertext
//
// The traffic key is derived with HKDF from the configured key for each time window (EncryptionKeyWindow), so no key
// encrypts more than a window's traffic, which keeps every key far inside the safe number of random nonces for
// AES-GCM. The parity picks the window among the receiver's previous, current and next one (clocks may differ by
// less than a window); the hint picks the configured key (1 in 64 keys may share a hint, then each is tried). The
// frame's flags and this byte are authenticated as additional data.
//
// A receiver tries the keyed format first and falls back to the legacy one, so nodes of both versions interoperate
// during an upgrade; a sender uses the keyed format only towards nodes known to read it.
type keyring struct {
	cipher encryption.Cipher
	aead   encryption.AEADCipher // nil: the cipher has no AEAD, so only the legacy format is used
	keys   [][]byte
	hints  []byte
	window int64            // seconds
	now    func() time.Time // the clock (tests replace it)

	mu    sync.RWMutex
	cache map[aeadKey]cipher.AEAD // traffic keys by configured key and window
}

type aeadKey struct {
	key    int
	window int64
}

// DefaultEncryptionKeyWindow is how long one traffic key is used when Config.EncryptionKeyWindow is not set.
const DefaultEncryptionKeyWindow = time.Hour

var errNoKey = errors.New("packet not encrypted with a known key")

func newKeyring(c encryption.Cipher, window time.Duration, current []byte, previous [][]byte) (*keyring, error) {
	if c == nil {
		return nil, errors.New("no cipher")
	}
	if window <= 0 {
		window = DefaultEncryptionKeyWindow
	}
	if window < time.Minute {
		return nil, errors.New("encryption key window must be at least a minute")
	}
	k := &keyring{cipher: c, window: int64(window / time.Second), now: time.Now, cache: map[aeadKey]cipher.AEAD{}}
	if a, ok := c.(encryption.AEADCipher); ok {
		k.aead = a
	}
	for _, key := range append([][]byte{current}, previous...) {
		if l := len(key); l != 16 && l != 24 && l != 32 {
			return nil, errors.New("invalid encryption key length: must be 16, 24, or 32 bytes")
		}
		k.keys = append(k.keys, key)
		k.hints = append(k.hints, keyHint(key))
	}
	return k, nil
}

// keyHint is the 6-bit hint naming a configured key in the keyed format.
func keyHint(key []byte) byte {
	h := sha256.Sum256(append([]byte("gossip key hint:"), key...))
	return h[0] & 0x3f
}

// windowSeconds is the key window this node advertises: 0 when it cannot use the keyed format.
func (k *keyring) windowSeconds() uint32 {
	if k == nil || k.aead == nil {
		return 0
	}
	return uint32(k.window)
}

func (k *keyring) currentWindow() int64 { return k.now().Unix() / k.window }

// traffic returns the AEAD for configured key i in window w.
func (k *keyring) traffic(i int, w int64) (cipher.AEAD, error) {
	k.mu.RLock()
	a, ok := k.cache[aeadKey{i, w}]
	k.mu.RUnlock()
	if ok {
		return a, nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if a, ok := k.cache[aeadKey{i, w}]; ok {
		return a, nil
	}
	tk, err := hkdf.Key(sha256.New, k.keys[i], []byte("gossip traffic key"), "window "+strconv.FormatInt(w, 10), len(k.keys[i]))
	if err != nil {
		return nil, err
	}
	a, err = k.aead.NewAEAD(tk)
	if err != nil {
		return nil, err
	}
	for ck := range k.cache { // only the windows around now are ever needed
		if ck.window < w-2 || ck.window > w+2 {
			delete(k.cache, ck)
		}
	}
	k.cache[aeadKey{i, w}] = a
	return a, nil
}

// seal encrypts plain: in the keyed format when keyed (and possible), else in the legacy one. aad is authenticated
// in the keyed format only.
func (k *keyring) seal(keyed bool, aad, plain []byte) ([]byte, error) {
	if !keyed || k.aead == nil {
		return k.cipher.Encrypt(k.keys[0], plain)
	}
	w := k.currentWindow()
	a, err := k.traffic(0, w)
	if err != nil {
		return nil, err
	}
	b := byte(w&3)<<6 | k.hints[0]
	ns := a.NonceSize()
	out := make([]byte, 1+ns, 1+ns+len(plain)+a.Overhead())
	out[0] = b
	if _, err := rand.Read(out[1 : 1+ns]); err != nil {
		return nil, err
	}
	return a.Seal(out, out[1:1+ns], plain, keyedAAD(aad, b)), nil
}

// open decrypts data in either format, reporting whether it was keyed.
func (k *keyring) open(aad, data []byte) ([]byte, bool, error) {
	if k.aead != nil && len(data) > 1 {
		b := data[0]
		cur := k.currentWindow()
		for _, w := range [3]int64{cur, cur - 1, cur + 1} {
			if byte(w&3) != b>>6 {
				continue
			}
			for i, h := range k.hints {
				if h != b&0x3f {
					continue
				}
				a, err := k.traffic(i, w)
				if err != nil {
					continue
				}
				ns := a.NonceSize()
				if len(data) < 1+ns+a.Overhead() {
					break
				}
				if plain, err := a.Open(nil, data[1:1+ns], data[1+ns:], keyedAAD(aad, b)); err == nil {
					return plain, true, nil
				}
			}
			break // the parity names exactly one window
		}
	}
	for _, key := range k.keys {
		if plain, err := k.cipher.Decrypt(key, data); err == nil {
			return plain, false, nil
		}
	}
	return nil, false, errNoKey
}

// keyedAAD is the additional data of a keyed frame: what the caller authenticates, then the key byte.
func keyedAAD(aad []byte, b byte) []byte {
	out := make([]byte, 0, len(aad)+1)
	return append(append(out, aad...), b)
}
