package codec

// Interface for decoupling the message serialization and deserialization
type Serializer interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}
