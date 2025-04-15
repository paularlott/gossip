package encryption

import (
	"crypto/rand"
	"errors"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
)

// ChaCha20Encryptor implements XChaCha20-Poly1305 encryption/decryption
type ChaCha20Encryptor struct{}

// NewChaCha20Encryptor creates a new ChaCha20Encryptor
func NewChaCha20Encryptor() *ChaCha20Encryptor {
	return &ChaCha20Encryptor{}
}

// Encrypt encrypts data using XChaCha20-Poly1305 with a random nonce
func (c *ChaCha20Encryptor) Encrypt(key []byte, data []byte) ([]byte, error) {
	// Validate key length - XChaCha20-Poly1305 requires a 32-byte key
	if len(key) != chacha20poly1305.KeySize {
		return nil, errors.New("xchacha20poly1305: key must be 32 bytes")
	}

	// Create XChaCha20-Poly1305 AEAD cipher
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	// Create nonce (24 bytes for XChaCha20)
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Encrypt and authenticate the data
	ciphertext := aead.Seal(nil, nonce, data, nil)

	// Prepend nonce to the ciphertext for decryption
	result := make([]byte, len(nonce)+len(ciphertext))
	copy(result, nonce)
	copy(result[len(nonce):], ciphertext)

	return result, nil
}

// Decrypt decrypts data using XChaCha20-Poly1305
func (c *ChaCha20Encryptor) Decrypt(key []byte, data []byte) ([]byte, error) {
	// Validate key length
	if len(key) != chacha20poly1305.KeySize {
		return nil, errors.New("xchacha20poly1305: key must be 32 bytes")
	}

	// Create XChaCha20-Poly1305 AEAD cipher
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	// Ensure data is long enough to contain nonce and ciphertext
	nonceSize := aead.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	// Extract nonce and ciphertext
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]

	// Decrypt and verify authentication
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// Ensure ChaCha20Encryptor implements the Cipher interface
var _ Cipher = (*ChaCha20Encryptor)(nil)
