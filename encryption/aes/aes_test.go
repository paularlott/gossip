package aes

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	e := New()
	assert.Equal(t, "aes-gcm", e.Name())
	data := []byte("This is secret data that needs encryption")

	for _, size := range []int{16, 24, 32} {
		key := bytes.Repeat([]byte("k"), size)
		enc, err := e.Encrypt(key, data)
		require.NoError(t, err, "%d byte key", size)
		assert.False(t, bytes.Contains(enc, data), "the data is not readable")
		got, err := e.Decrypt(key, enc)
		require.NoError(t, err)
		assert.Equal(t, data, got)

		again, err := e.Encrypt(key, data)
		require.NoError(t, err)
		assert.NotEqual(t, enc, again, "every encryption has its own nonce")
	}
}

func TestRefusesWhatItCannotRead(t *testing.T) {
	e := New()
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := e.Encrypt(key, []byte("data"))
	require.NoError(t, err)

	_, err = e.Decrypt(bytes.Repeat([]byte("x"), 32), enc)
	assert.Error(t, err, "the wrong key")

	tampered := append([]byte(nil), enc...)
	tampered[len(tampered)-1] ^= 1
	_, err = e.Decrypt(key, tampered)
	assert.Error(t, err, "a changed ciphertext")

	_, err = e.Decrypt(key, enc[:5])
	assert.Error(t, err, "shorter than a nonce")

	_, err = e.Encrypt([]byte("short"), []byte("data"))
	assert.Error(t, err, "a key of the wrong size")
	_, err = e.Decrypt([]byte("short"), enc)
	assert.Error(t, err)
	_, err = e.NewAEAD([]byte("short"))
	assert.Error(t, err)

	aead, err := e.NewAEAD(key)
	require.NoError(t, err)
	assert.NotNil(t, aead)
}
