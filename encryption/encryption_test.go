package encryption

import (
	"crypto/rand"
	"fmt"
	"testing"
)

// go test -bench=. -benchmem
// go test -bench=. -benchmem -benchtime=5s -count=3

// generateRandomBytes creates random bytes for testing
func generateRandomBytes(n int) ([]byte, error) {
	bytes := make([]byte, n)
	_, err := rand.Read(bytes)
	return bytes, err
}

// benchmarkEncryption benchmarks the encryption speed for a given cipher and data size
func benchmarkEncryption(b *testing.B, cipher Cipher, keySize, dataSize int) {
	// Generate random key of the specified size
	key, err := generateRandomBytes(keySize)
	if err != nil {
		b.Fatal(err)
	}

	// Generate random data
	data, err := generateRandomBytes(dataSize)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cipher.Encrypt(key, data)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(dataSize)) // For B/s measurement
}

// benchmarkDecryption benchmarks the decryption speed for a given cipher and data size
func benchmarkDecryption(b *testing.B, cipher Cipher, keySize, dataSize int) {
	// Generate random key of the specified size
	key, err := generateRandomBytes(keySize)
	if err != nil {
		b.Fatal(err)
	}

	// Generate random data and encrypt it
	data, err := generateRandomBytes(dataSize)
	if err != nil {
		b.Fatal(err)
	}
	encryptedData, err := cipher.Encrypt(key, data)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cipher.Decrypt(key, encryptedData)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(dataSize)) // For B/s measurement
}

// AES-GCM Encryption Benchmarks with 32-byte key (AES-256)
func BenchmarkAES_Encrypt_32B(b *testing.B) {
	benchmarkEncryption(b, NewAESEncryptor(), 32, 32)
}

func BenchmarkAES_Encrypt_1KB(b *testing.B) {
	benchmarkEncryption(b, NewAESEncryptor(), 32, 1024)
}

func BenchmarkAES_Encrypt_64KB(b *testing.B) {
	benchmarkEncryption(b, NewAESEncryptor(), 32, 64*1024)
}

func BenchmarkAES_Encrypt_1MB(b *testing.B) {
	benchmarkEncryption(b, NewAESEncryptor(), 32, 1024*1024)
}

// AES-GCM Decryption Benchmarks with 32-byte key (AES-256)
func BenchmarkAES_Decrypt_32B(b *testing.B) {
	benchmarkDecryption(b, NewAESEncryptor(), 32, 32)
}

func BenchmarkAES_Decrypt_1KB(b *testing.B) {
	benchmarkDecryption(b, NewAESEncryptor(), 32, 1024)
}

func BenchmarkAES_Decrypt_64KB(b *testing.B) {
	benchmarkDecryption(b, NewAESEncryptor(), 32, 64*1024)
}

func BenchmarkAES_Decrypt_1MB(b *testing.B) {
	benchmarkDecryption(b, NewAESEncryptor(), 32, 1024*1024)
}

// ChaCha20-Poly1305 Encryption Benchmarks with 32-byte key
func BenchmarkChaCha20_Encrypt_32B(b *testing.B) {
	benchmarkEncryption(b, NewChaCha20Encryptor(), 32, 32)
}

func BenchmarkChaCha20_Encrypt_1KB(b *testing.B) {
	benchmarkEncryption(b, NewChaCha20Encryptor(), 32, 1024)
}

func BenchmarkChaCha20_Encrypt_64KB(b *testing.B) {
	benchmarkEncryption(b, NewChaCha20Encryptor(), 32, 64*1024)
}

func BenchmarkChaCha20_Encrypt_1MB(b *testing.B) {
	benchmarkEncryption(b, NewChaCha20Encryptor(), 32, 1024*1024)
}

// ChaCha20-Poly1305 Decryption Benchmarks with 32-byte key
func BenchmarkChaCha20_Decrypt_32B(b *testing.B) {
	benchmarkDecryption(b, NewChaCha20Encryptor(), 32, 32)
}

func BenchmarkChaCha20_Decrypt_1KB(b *testing.B) {
	benchmarkDecryption(b, NewChaCha20Encryptor(), 32, 1024)
}

func BenchmarkChaCha20_Decrypt_64KB(b *testing.B) {
	benchmarkDecryption(b, NewChaCha20Encryptor(), 32, 64*1024)
}

func BenchmarkChaCha20_Decrypt_1MB(b *testing.B) {
	benchmarkDecryption(b, NewChaCha20Encryptor(), 32, 1024*1024)
}

// Additional benchmarks with different AES key sizes
func BenchmarkAES128_Encrypt_1MB(b *testing.B) {
	benchmarkEncryption(b, NewAESEncryptor(), 16, 1024*1024) // AES-128
}

func BenchmarkAES192_Encrypt_1MB(b *testing.B) {
	benchmarkEncryption(b, NewAESEncryptor(), 24, 1024*1024) // AES-192
}

// Comparing encryption+decryption round trips
func BenchmarkAES_RoundTrip_64KB(b *testing.B) {
	cipher := NewAESEncryptor()
	key, _ := generateRandomBytes(32)
	data, _ := generateRandomBytes(64 * 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encrypted, err := cipher.Encrypt(key, data)
		if err != nil {
			b.Fatal(err)
		}
		_, err = cipher.Decrypt(key, encrypted)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(64 * 1024))
}

func BenchmarkChaCha20_RoundTrip_64KB(b *testing.B) {
	cipher := NewChaCha20Encryptor()
	key, _ := generateRandomBytes(32)
	data, _ := generateRandomBytes(64 * 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encrypted, err := cipher.Encrypt(key, data)
		if err != nil {
			b.Fatal(err)
		}
		_, err = cipher.Decrypt(key, encrypted)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(64 * 1024))
}

// Example test that verifies both ciphers work correctly
func TestCipherCorrectness(t *testing.T) {
	testCases := []struct {
		name   string
		cipher Cipher
		keyLen int
	}{
		{"AES-128", NewAESEncryptor(), 16},
		{"AES-192", NewAESEncryptor(), 24},
		{"AES-256", NewAESEncryptor(), 32},
		{"ChaCha20", NewChaCha20Encryptor(), 32},
	}

	testSizes := []int{32, 1024, 64 * 1024}

	for _, tc := range testCases {
		for _, size := range testSizes {
			t.Run(fmt.Sprintf("%s_%dBytes", tc.name, size), func(t *testing.T) {
				key, _ := generateRandomBytes(tc.keyLen)
				original, _ := generateRandomBytes(size)

				encrypted, err := tc.cipher.Encrypt(key, original)
				if err != nil {
					t.Fatalf("Encryption error: %v", err)
				}

				decrypted, err := tc.cipher.Decrypt(key, encrypted)
				if err != nil {
					t.Fatalf("Decryption error: %v", err)
				}

				// Verify the roundtrip worked
				if len(decrypted) != len(original) {
					t.Errorf("Expected decrypted length %d, got %d", len(original), len(decrypted))
				}

				// Check all bytes match
				match := true
				for i := range original {
					if original[i] != decrypted[i] {
						match = false
						break
					}
				}
				if !match {
					t.Error("Decrypted data doesn't match original")
				}

				// Also verify that the encrypted data is different from the original
				if len(encrypted) <= len(original) {
					t.Error("Encrypted data should be longer than original (needs nonce + tag)")
				}
			})
		}
	}
}
