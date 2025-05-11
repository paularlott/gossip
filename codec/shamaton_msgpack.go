//go:build !skip_codec_shamaton_msgpack

package codec

import shamaton "github.com/shamaton/msgpack/v2"

type ShamatonMsgpackCodec struct{}

func NewShamatonMsgpackCodec() *ShamatonMsgpackCodec {
	return &ShamatonMsgpackCodec{}
}

func (mp *ShamatonMsgpackCodec) Marshal(v interface{}) ([]byte, error) {
	return shamaton.Marshal(v)
}

func (mp *ShamatonMsgpackCodec) Unmarshal(data []byte, v interface{}) error {
	return shamaton.Unmarshal(data, v)
}
