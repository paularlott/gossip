package encryption

// Cipher decouples message encryption from the implementation.
// Concrete implementations live in sub-packages:
//
//   - encryption/aes
type Cipher interface {
	Name() string
	Encrypt(key, data []byte) ([]byte, error)
	Decrypt(key, encryptedData []byte) ([]byte, error)
}
