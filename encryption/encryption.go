package encryption

import "crypto/cipher"

// Cipher decouples message encryption from the implementation.
// Concrete implementations live in sub-packages:
//
//   - encryption/aes
type Cipher interface {
	Name() string
	Encrypt(key, data []byte) ([]byte, error)
	Decrypt(key, encryptedData []byte) ([]byte, error)
}

// AEADCipher is a Cipher that can also give an AEAD for a key. With one, the
// cluster uses its keyed format: traffic keys derived from the configured key
// for each time window, the frame header authenticated, and keys that can be
// rotated without a restart (see Config.EncryptionKeyWindow). A Cipher without
// it keeps the original format.
type AEADCipher interface {
	Cipher
	NewAEAD(key []byte) (cipher.AEAD, error)
}
