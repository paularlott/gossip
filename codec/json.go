//go:build !skip_codec_json

package codec

import "encoding/json"

type JsonCodec struct{}

func NewJsonCodec() *JsonCodec {
	return &JsonCodec{}
}

func (mp *JsonCodec) Marshal(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (mp *JsonCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
