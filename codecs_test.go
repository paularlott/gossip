package gossip

import (
	"testing"

	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/codec/hashicorp"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/codec/vmihailenco"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every codec carries the same message shapes: what the gossip messages are made of.
func TestCodecRoundTrip(t *testing.T) {
	type sample struct {
		Name   string            `json:"name" msgpack:"name" codec:"name"`
		Count  int64             `json:"count" msgpack:"count" codec:"count"`
		Active bool              `json:"active" msgpack:"active" codec:"active"`
		Tags   []string          `json:"tags" msgpack:"tags" codec:"tags"`
		Meta   map[string]string `json:"meta" msgpack:"meta" codec:"meta"`
		Raw    []byte            `json:"raw" msgpack:"raw" codec:"raw"`
	}
	want := sample{Name: "node-1", Count: 1 << 40, Active: true, Tags: []string{"web", "api"},
		Meta: map[string]string{"zone": "us-west"}, Raw: []byte{1, 2, 3}}

	codecs := []codec.Serializer{codec.NewJSONCodec(), shamaton.New(), vmihailenco.New(), hashicorp.New()}
	names := map[string]bool{}
	for _, c := range codecs {
		t.Run(c.Name(), func(t *testing.T) {
			assert.NotEmpty(t, c.Name())
			assert.False(t, names[c.Name()], "codec names are unique")
			names[c.Name()] = true

			data, err := c.Marshal(want)
			require.NoError(t, err)
			var got sample
			require.NoError(t, c.Unmarshal(data, &got))
			assert.Equal(t, want, got)

			data, err = c.Marshal(map[string]interface{}{"zone": "eu-west", "ok": true})
			require.NoError(t, err)
			var generic map[string]interface{}
			require.NoError(t, c.Unmarshal(data, &generic))
			assert.Equal(t, "eu-west", generic["zone"], "strings stay strings")
			assert.Equal(t, true, generic["ok"])

			assert.Error(t, c.Unmarshal([]byte("not a message"), &got), "garbage")
		})
	}
}
