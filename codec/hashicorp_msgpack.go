//go:build !skip_codec_hashicorp_msgpack

package codec

import msgpack "github.com/hashicorp/go-msgpack/v2/codec"

type HashicorpMsgpackCodec struct {
	handle *msgpack.MsgpackHandle
}

func NewHashicorpMsgpackCodec() *HashicorpMsgpackCodec {
	return &HashicorpMsgpackCodec{
		handle: new(msgpack.MsgpackHandle),
	}
}

func (mp *HashicorpMsgpackCodec) Marshal(v interface{}) ([]byte, error) {
	var data []byte

	enc := msgpack.NewEncoderBytes(&data, mp.handle)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return data, nil
}

func (mp *HashicorpMsgpackCodec) Unmarshal(data []byte, v interface{}) error {
	dec := msgpack.NewDecoderBytes(data, mp.handle)
	return dec.Decode(v)
}
