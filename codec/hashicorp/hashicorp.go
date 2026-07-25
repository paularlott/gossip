package hashicorp

import (
	"github.com/hashicorp/go-msgpack/v2/codec"
	gossipcodec "github.com/paularlott/gossip/codec"
)

type MsgpackCodec struct {
	handle *codec.MsgpackHandle
}

func New() *MsgpackCodec {
	return &MsgpackCodec{
		handle: new(codec.MsgpackHandle),
	}
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
