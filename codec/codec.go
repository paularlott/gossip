package codec

import "encoding/json"

// Serializer decouples message serialization from the implementation.
// Concrete implementations live in sub-packages:
//
//   - codec/shamaton     (recommended — fastest)
//   - codec/vmihailenco
//   - codec/hashicorp
//
// JSON is included here because it has no external dependencies.
type Serializer interface {
	Name() string
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

// JSONCodec serializes using encoding/json.
type JSONCodec struct{}

func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

func (c *JSONCodec) Name() string {
	return "json"
}

func (c *JSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (c *JSONCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
