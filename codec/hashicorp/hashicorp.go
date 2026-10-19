package hashicorp

import (
	"github.com/hashicorp/go-msgpack/v2/codec"
	gossipcodec "github.com/paularlott/gossip/codec"
)

type MsgpackCodec struct {
	handle *codec.MsgpackHandle
}

func New() *MsgpackCodec {
	h := new(codec.MsgpackHandle)
	// Strings inside interface{} values (node metadata, user maps) decode as strings, as with the other codecs,
	// rather than as []byte, which Metadata.GetString and similar readers do not recognise. Decoding only: what is
	// sent is unchanged.
	h.RawToString = true
	return &MsgpackCodec{handle: h}
}

func (c *MsgpackCodec) Name() string {
	return "hashicorp-msgpack"
}

func (c *MsgpackCodec) Marshal(v interface{}) ([]byte, error) {
	var data []byte

	enc := codec.NewEncoderBytes(&data, c.handle)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return data, nil
}

func (c *MsgpackCodec) Unmarshal(data []byte, v interface{}) error {
	dec := codec.NewDecoderBytes(data, c.handle)
	return dec.Decode(v)
}

var _ gossipcodec.Serializer = (*MsgpackCodec)(nil)
