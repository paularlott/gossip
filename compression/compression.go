package compression

// Compressor decouples message compression from the implementation.
// Concrete implementations live in sub-packages:
//
//   - compression/snappy
type Compressor interface {
	Name() string
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}
