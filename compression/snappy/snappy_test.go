package snappy

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	c := New()
	assert.Equal(t, "snappy", c.Name())

	for name, data := range map[string][]byte{
		"repetitive": bytes.Repeat([]byte("This is a test string that should compress well. "), 20),
		"empty":      {},
		"one byte":   {7},
	} {
		t.Run(name, func(t *testing.T) {
			compressed, err := c.Compress(data)
			require.NoError(t, err)
			got, err := c.Decompress(compressed)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(data, got))
			if name == "repetitive" {
				assert.Less(t, len(compressed), len(data))
			}
		})
	}

	_, err := c.Decompress([]byte("this is not snappy"))
	assert.Error(t, err)
}
