package shamaton

import (
	"github.com/paularlott/gossip/codec"
	msgpack "github.com/shamaton/msgpack/v3"
)

type MsgpackCodec struct{}

func New() *MsgpackCodec {
	return &MsgpackCodec{}
}

func (c *MsgpackCodec) Name() string {
	return "shamaton-msgpack"
}

func (c *MsgpackCodec) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (c *MsgpackCodec) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

var _ codec.Serializer = (*MsgpackCodec)(nil)
