package compression

type Compressor interface {
	Name() string
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}
