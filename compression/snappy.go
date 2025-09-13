//go:build !skip_compression_snappy

package compression

import (
	"github.com/klauspost/compress/snappy"
)

type SnappyCompressor struct{}

func NewSnappyCompressor() *SnappyCompressor {
	return &SnappyCompressor{}
}

func (s *SnappyCompressor) Name() string {
	return "snappy"
}

func (s *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (s *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}
