package shamaton

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type message struct {
	ID    [16]byte          `msgpack:"id"`
	Addr  string            `msgpack:"addr"`
	Tags  []string          `msgpack:"tags,omitempty"`
	Meta  map[string]any    `msgpack:"md"`
	Stamp uint64            `msgpack:"ts"`
	Extra map[string]string `msgpack:"extra,omitempty"`
}

func TestName(t *testing.T) {
	assert.Equal(t, "shamaton-msgpack", New().Name())
}

// Time values in metadata come back as time.Time in UTC, and strings as strings.
func TestTimeAndMixedTypesInMetadata(t *testing.T) {
	c := New()
	when := time.Date(2025, 6, 15, 12, 30, 45, 0, time.FixedZone("x", 3600))
	in := map[string]any{"created": when, "name": "test-node", "ok": true, "n": int64(42), "big": uint64(1<<64 - 1), "f": 3.14159}

	data, err := c.Marshal(in)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, c.Unmarshal(data, &out))

	got, ok := out["created"].(time.Time)
	require.True(t, ok, "created is %T", out["created"])
	assert.True(t, when.Equal(got), "the same instant")
	assert.Equal(t, time.UTC, got.Location())
	assert.Equal(t, "test-node", out["name"], "a string is not []byte")
	assert.Equal(t, true, out["ok"])
	assert.EqualValues(t, 42, out["n"])
	assert.EqualValues(t, uint64(1<<64-1), out["big"])
	assert.InDelta(t, 3.14159, out["f"], 1e-9)
}

func TestEdgeValuesAndLargeValues(t *testing.T) {
	c := New()
	msgs := make([]message, 200)
	for i := range msgs {
		msgs[i] = message{ID: [16]byte{byte(i)}, Addr: "127.0.0.1:" + strconv.Itoa(8000+i), Stamp: uint64(i), Meta: map[string]any{}}
	}
	msgs[0].ID = [16]byte{}
	msgs[1].Stamp = ^uint64(0)

	data, err := c.Marshal(&msgs)
	require.NoError(t, err)
	var out []message
	require.NoError(t, c.Unmarshal(data, &out))
	require.Len(t, out, 200)
	assert.Equal(t, [16]byte{}, out[0].ID, "the zero id")
	assert.Equal(t, ^uint64(0), out[1].Stamp, "the largest timestamp")
	assert.Equal(t, msgs[199].Addr, out[199].Addr)
}

// An empty slice and a nil one are both left out of the encoding and decode as empty.
func TestNilAndEmptySlices(t *testing.T) {
	c := New()
	for name, tags := range map[string][]string{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			data, err := c.Marshal(message{Addr: "a", Tags: tags, Meta: map[string]any{}})
			require.NoError(t, err)
			var out message
			require.NoError(t, c.Unmarshal(data, &out))
			assert.Empty(t, out.Tags)
		})
	}
}

func TestTypeMismatchIsAnError(t *testing.T) {
	c := New()
	data, err := c.Marshal("hello-world")
	require.NoError(t, err)
	var m message
	assert.Error(t, c.Unmarshal(data, &m))
}
