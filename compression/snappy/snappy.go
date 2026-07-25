package snappy

import (
	"github.com/klauspost/compress/snappy"
	"github.com/paularlott/gossip/compression"
)

type SnappyCompressor struct{}

func New() *SnappyCompressor {
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

var _ compression.Compressor = (*SnappyCompressor)(nil)
