package compression

import (
	"github.com/klauspost/compress/snappy"
)

type SnappyCompressor struct{}

func NewSnappyCompressor() *SnappyCompressor {
	return &SnappyCompressor{}
}

func (s *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (s *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

// Ensure SnappyCompressor implements the Codec interface
var _ Codec = (*SnappyCompressor)(nil)
