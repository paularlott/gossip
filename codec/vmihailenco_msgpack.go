package codec

import vmihailenco "github.com/vmihailenco/msgpack/v5"

type VmihailencoMsgpackCodec struct{}

func NewVmihailencoMsgpackCodec() *VmihailencoMsgpackCodec {
	return &VmihailencoMsgpackCodec{}
}

func (mp *VmihailencoMsgpackCodec) Marshal(v interface{}) ([]byte, error) {
	return vmihailenco.Marshal(v)
}

func (mp *VmihailencoMsgpackCodec) Unmarshal(data []byte, v interface{}) error {
	return vmihailenco.Unmarshal(data, v)
}
