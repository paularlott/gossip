package encryption

// Interface for decoupling the message encryption and decryption
type Crypter interface {
	Encrypt(key, data []byte) ([]byte, error)
	Decrypt(key, encryptedData []byte) ([]byte, error)
}
