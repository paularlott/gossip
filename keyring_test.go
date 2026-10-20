package gossip

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	"github.com/paularlott/gossip/encryption"
	"github.com/paularlott/gossip/encryption/aes"
)

// legacyCipher is AES without NewAEAD: it behaves exactly as the library did before the keyed format (it neither
// sends nor reads it), so it stands in for a node of an older version.
type legacyCipher struct{ c *aes.Encryptor }

func (l legacyCipher) Name() string                          { return l.c.Name() }
func (l legacyCipher) Encrypt(key, d []byte) ([]byte, error) { return l.c.Encrypt(key, d) }
func (l legacyCipher) Decrypt(key, d []byte) ([]byte, error) { return l.c.Decrypt(key, d) }

var _ encryption.Cipher = legacyCipher{}

func testKey(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }

func mustKeyring(t *testing.T, c encryption.Cipher, current []byte, previous ...[]byte) *keyring {
	t.Helper()
	k, err := newKeyring(c, time.Hour, current, previous)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// at fixes a keyring's clock.
func at(k *keyring, t time.Time) *keyring {
	k.now = func() time.Time { return t }
	return k
}

func TestKeyringRoundTrips(t *testing.T) {
	for _, n := range []int{16, 24, 32} {
		k := mustKeyring(t, aes.New(), testKey(1, n))
		aad := []byte{0x12, 0x34}
		plain := []byte("payload")
		for _, keyed := range []bool{true, false} {
			sealed, err := k.seal(keyed, aad, plain)
			if err != nil {
				t.Fatal(err)
			}
			got, wasKeyed, err := k.open(aad, sealed)
			if err != nil || !bytes.Equal(got, plain) || wasKeyed != keyed {
				t.Fatalf("AES-%d keyed=%v: %q %v %v", n*8, keyed, got, wasKeyed, err)
			}
		}
	}
}

// The keyed format costs exactly one byte on the wire over the original.
func TestKeyedFormatAddsOneByte(t *testing.T) {
	k := mustKeyring(t, aes.New(), testKey(1, 32))
	plain := make([]byte, 500)
	keyed, _ := k.seal(true, nil, plain)
	legacy, _ := k.seal(false, nil, plain)
	if len(keyed) != len(legacy)+1 {
		t.Fatalf("keyed %d bytes, original %d", len(keyed), len(legacy))
	}
}

// The original format (as older nodes send it) still opens, and what this keyring sends in it opens with the plain
// cipher (as older nodes read it).
func TestKeyringInteroperatesWithOriginalFormat(t *testing.T) {
	key := testKey(3, 32)
	k := mustKeyring(t, aes.New(), key)
	old, _ := aes.New().Encrypt(key, []byte("from an older node"))
	if got, keyed, err := k.open([]byte{1, 2}, old); err != nil || keyed || string(got) != "from an older node" {
		t.Fatalf("original format: %q %v %v", got, keyed, err)
	}
	sealed, _ := k.seal(false, []byte{1, 2}, []byte("to an older node"))
	if got, err := aes.New().Decrypt(key, sealed); err != nil || string(got) != "to an older node" {
		t.Fatalf("older node cannot read: %q %v", got, err)
	}
	// and an older node cannot read the keyed format (so it is never sent to one)
	keyedMsg, _ := k.seal(true, nil, []byte("x"))
	if _, err := aes.New().Decrypt(key, keyedMsg); err == nil {
		t.Fatal("keyed format readable as the original")
	}
}

// The flags and the key byte are authenticated; any change is refused.
func TestKeyedFormatAuthenticatesHeader(t *testing.T) {
	k := mustKeyring(t, aes.New(), testKey(1, 32))
	aad := []byte{0x40, 0x10}
	sealed, _ := k.seal(true, aad, []byte("payload"))
	if _, _, err := k.open([]byte{0x60, 0x10}, sealed); err == nil {
		t.Fatal("changed flags accepted")
	}
	for bit := range 8 {
		tampered := bytes.Clone(sealed)
		tampered[0] ^= 1 << bit
		if _, _, err := k.open(aad, tampered); err == nil {
			t.Fatalf("changed key byte (bit %d) accepted", bit)
		}
	}
	tampered := bytes.Clone(sealed)
	tampered[len(tampered)-1] ^= 1
	if _, _, err := k.open(aad, tampered); err == nil {
		t.Fatal("changed ciphertext accepted")
	}
	if _, _, err := k.open(aad, sealed[:5]); err == nil {
		t.Fatal("truncated frame accepted")
	}
}

// Traffic keys change with the window; a receiver accepts the window before and after its own (clocks differ by
// less than a window), but not two away.
func TestKeyWindows(t *testing.T) {
	key := testKey(5, 32)
	base := time.Unix(1_800_000_000, 0).Truncate(time.Hour)
	sender := at(mustKeyring(t, aes.New(), key), base.Add(30*time.Minute))
	sealed, _ := sender.seal(true, nil, []byte("x"))
	next, _ := at(mustKeyring(t, aes.New(), key), base.Add(90*time.Minute)).seal(true, nil, []byte("x"))
	if sealed[0]>>6 == next[0]>>6 {
		t.Fatal("window parity did not change between windows")
	}
	for offset, ok := range map[time.Duration]bool{
		0: true, 59 * time.Minute: true, -29 * time.Minute: true, // same window
		61 * time.Minute: true, -31 * time.Minute: true, // next and previous window
		121 * time.Minute: false, -91 * time.Minute: false, // two windows away
	} {
		r := at(mustKeyring(t, aes.New(), key), base.Add(30*time.Minute+offset))
		got, keyed, err := r.open(nil, sealed)
		if ok != (err == nil && keyed && string(got) == "x") {
			t.Errorf("receiver %v from the sender: opened=%v (%v)", offset, err == nil, err)
		}
	}
	// two windows' keys differ
	a, _ := sender.traffic(0, 10)
	b, _ := sender.traffic(0, 11)
	nonce := make([]byte, a.NonceSize())
	if bytes.Equal(a.Seal(nil, nonce, []byte("same"), nil), b.Seal(nil, nonce, []byte("same"), nil)) {
		t.Fatal("windows share a traffic key")
	}
}

// A rotated keyring opens what the old key sent, in both formats, and sends with the new key; once the old key is
// removed, its traffic no longer opens.
func TestKeyringRotation(t *testing.T) {
	oldKey, newKey := testKey(1, 32), testKey(2, 32)
	old := mustKeyring(t, aes.New(), oldKey)
	rotated := mustKeyring(t, aes.New(), newKey, oldKey)
	for _, keyed := range []bool{true, false} {
		sealed, _ := old.seal(keyed, nil, []byte("old"))
		if got, _, err := rotated.open(nil, sealed); err != nil || string(got) != "old" {
			t.Fatalf("rotated keyring cannot open old key (keyed=%v): %v", keyed, err)
		}
		fresh, _ := rotated.seal(keyed, nil, []byte("new"))
		if _, _, err := old.open(nil, fresh); err == nil {
			t.Fatalf("new traffic opened with the old key alone (keyed=%v)", keyed)
		}
		// a node that added the new key as previous (rotation step 1) reads it
		step1 := mustKeyring(t, aes.New(), oldKey, newKey)
		if got, _, err := step1.open(nil, fresh); err != nil || string(got) != "new" {
			t.Fatalf("step-1 node cannot read the new key (keyed=%v): %v", keyed, err)
		}
		done := mustKeyring(t, aes.New(), newKey)
		if _, _, err := done.open(nil, sealed); err == nil {
			t.Fatalf("removed key still opens (keyed=%v)", keyed)
		}
	}
}

// Two keys whose hints collide are both tried.
func TestKeyHintCollision(t *testing.T) {
	a := testKey(9, 32)
	var b []byte
	for {
		b = make([]byte, 32)
		rand.Read(b)
		if keyHint(b) == keyHint(a) && !bytes.Equal(a, b) {
			break
		}
	}
	sealed, _ := mustKeyring(t, aes.New(), b).seal(true, nil, []byte("b"))
	if got, keyed, err := mustKeyring(t, aes.New(), a, b).open(nil, sealed); err != nil || !keyed || string(got) != "b" {
		t.Fatalf("colliding hint: %q %v %v", got, keyed, err)
	}
}

// A cipher without an AEAD keeps the original format and advertises no key window.
func TestKeyringWithoutAEAD(t *testing.T) {
	k := mustKeyring(t, legacyCipher{aes.New()}, testKey(1, 32))
	if k.windowSeconds() != 0 {
		t.Fatal("advertises a key window without an AEAD")
	}
	sealed, _ := k.seal(true, nil, []byte("x"))
	if got, err := aes.New().Decrypt(testKey(1, 32), sealed); err != nil || string(got) != "x" {
		t.Fatal("did not fall back to the original format")
	}
	if mustKeyring(t, aes.New(), testKey(1, 32)).windowSeconds() != 3600 {
		t.Fatal("AES keyring advertises the wrong window")
	}
}

func TestKeyringValidation(t *testing.T) {
	for name, err := range map[string]error{
		"short key": func() error { _, e := newKeyring(aes.New(), time.Hour, testKey(1, 10), nil); return e }(),
		"short previous": func() error {
			_, e := newKeyring(aes.New(), time.Hour, testKey(1, 32), [][]byte{testKey(1, 7)})
			return e
		}(),
		"tiny window": func() error { _, e := newKeyring(aes.New(), time.Second, testKey(1, 32), nil); return e }(),
		"no cipher":   func() error { _, e := newKeyring(nil, time.Hour, testKey(1, 32), nil); return e }(),
	} {
		if err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if k, err := newKeyring(aes.New(), 0, testKey(1, 32), nil); err != nil || k.window != 3600 {
		t.Fatalf("zero window should default to an hour: %v", err)
	}
}

// Only the windows around now stay cached.
func TestKeyringCacheBounded(t *testing.T) {
	k := mustKeyring(t, aes.New(), testKey(1, 32))
	for w := int64(0); w < 50; w++ {
		k.traffic(0, w)
	}
	if n := len(k.cache); n > 5 {
		t.Fatalf("%d traffic keys cached", n)
	}
}

// Traffic over many windows, under every configured key, keeps at most the windows around now for each key: the
// cache does not grow as the traffic keys rotate.
func TestKeyringCacheBoundedOverTime(t *testing.T) {
	keys := [][]byte{testKey(1, 32), testKey(2, 32), testKey(3, 32)}
	base := time.Unix(1_800_000_000, 0).Truncate(time.Hour)
	recv := mustKeyring(t, aes.New(), keys[0], keys[1:]...)
	senders := make([]*keyring, len(keys))
	for i, key := range keys {
		senders[i] = mustKeyring(t, aes.New(), key)
	}
	for h := 0; h < 500; h++ {
		now := base.Add(time.Duration(h)*time.Hour + 30*time.Minute)
		at(recv, now)
		for _, s := range senders {
			for _, skew := range []time.Duration{-time.Hour, 0, time.Hour} { // the previous, current and next window
				sealed, _ := at(s, now.Add(skew)).seal(true, nil, []byte("x"))
				if _, keyed, err := recv.open(nil, sealed); err != nil || !keyed {
					t.Fatalf("hour %d skew %v: %v", h, skew, err)
				}
			}
		}
		if _, err := recv.seal(true, nil, []byte("x")); err != nil {
			t.Fatal(err)
		}
		recv.mu.RLock()
		n := len(recv.cache)
		recv.mu.RUnlock()
		if n > 5*len(keys) {
			t.Fatalf("hour %d: %d traffic keys cached for %d keys", h, n, len(keys))
		}
	}
}
