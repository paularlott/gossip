package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

// AESEncryptor implements AES encryption/decryption
type AESEncryptor struct{}

func NewAESEncryptor() *AESEncryptor {
	return &AESEncryptor{}
}

// Encrypt encrypts data using AES-GCM with a random nonce
func (e *AESEncryptor) Encrypt(key []byte, data []byte) ([]byte, error) {
	// Create cipher block
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Create nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Seal will encrypt and authenticate the data
	ciphertext := gcm.Seal(nil, nonce, data, nil)

	// Prepend nonce to the ciphertext
	result := make([]byte, len(nonce)+len(ciphertext))
	copy(result, nonce)
	copy(result[len(nonce):], ciphertext)

	return result, nil
}

// Decrypt decrypts data using AES-GCM
func (e *AESEncryptor) Decrypt(key []byte, data []byte) ([]byte, error) {
	// Create cipher block
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Ensure data is long enough to contain nonce and ciphertext
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	// Extract nonce and ciphertext
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]

	// Decrypt and authenticate
	return gcm.Open(nil, nonce, ciphertext, nil)
}

// Ensure AESEncryptor implements the Cipher interface
var _ Cipher = (*AESEncryptor)(nil)
