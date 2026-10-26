package partition

import (
	"reflect"
	"testing"

	"github.com/paularlott/gossip/codec"
	"github.com/paularlott/gossip/codec/hashicorp"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/codec/vmihailenco"
)

// Every message must survive each of the library's codecs.
func TestMessagesRoundTripEveryCodec(t *testing.T) {
	w := Write{Partition: 3, Pos: Pos{Epoch: ep(4, 2), Seq: 9}, Prev: Pos{Epoch: ep(3, 1), Seq: 8}, Data: []byte{0, 1, 2, 255}}
	pos := Pos{Epoch: ep(7, 3), Seq: 1 << 40}
	msgs := []interface{}{
		&heartbeatMsg{Size: 3, Cluster: "prod-a", From: nid(2), Entries: []heartbeatEntry{{P: 1, Epoch: ep(2, 2), Committed: pos}, {P: 5, Epoch: ep(9, 2)}}},
		&heartbeatResp{OK: true, Fenced: []heartbeatFence{{P: 2, Promised: ep(9, 1)}}},
		&heartbeatResp{SizeMismatch: true},
		&heartbeatResp{OK: true, Accepted: []int{0, 3, 63}},
		&readReq{Size: 3, Cluster: "prod-a", Deadline: 1_700_000_000_123_456_789, P: 5, Req: []byte{0, 1, 255}},
		&readReq{Size: 3, P: 5},
		&promiseReq{Size: 3, Cluster: "prod-a", P: 7, Epoch: ep(5, 1)},
		&promiseResp{OK: true, Promised: ep(5, 1), Committed: pos, Pending: &w},
		&promiseResp{Promised: ep(6, 2)},
		&writeReq{Size: 3, Cluster: "prod-a", Write: w, OwnerCommitted: pos},
		&writeResp{OK: true, Promised: ep(1, 1), Committed: pos},
		&writeResp{NeedCatchUp: true, Committed: pos},
		&changeReq{Size: 3, Cluster: "prod-a", Deadline: 1_700_000_000_123_456_789, P: 2, Change: []byte("change")},
		&changeResp{Status: statusRefused, RefusedMsg: "no", Owner: nid(3), Epoch: ep(2, 3)},
		&changeResp{Status: statusRefused, RefusedCode: "conflict", RefusedMsg: "version moved"},
		&changeResp{Status: statusDone, Pos: pos, Reply: []byte{0, 7, 255}},
		&changeResp{Status: statusBusy},
		&changeResp{Status: statusDone},
		&stateReq{Size: 3, Cluster: "prod-a"},
		&stateResp{Committed: []Pos{pos, {}}, Promised: []Epoch{ep(1, 1), {}}},
		&stateResp{Committed: []Pos{pos, {}}, Promised: []Epoch{ep(1, 1), {}}, Pending: []*Write{nil, &w}},
		&stateResp{Committed: []Pos{pos, {}}, Promised: []Epoch{ep(1, 1), {}}, Intact: true},
		&stateResp{Committed: []Pos{pos, {}}, Promised: []Epoch{ep(1, 1), {}}, Intact: true, Unformed: true},
		&catchUpReq{Size: 3, Cluster: "prod-a", P: 1, After: 77, Full: true},
		&relinquishMsg{Size: 3, Cluster: "prod-a", P: 4, Epoch: ep(3, 3)},
		&commitMsg{Size: 3, Cluster: "prod-a", P: 4, Epoch: ep(3, 3), Committed: pos},
		&claimReq{Size: 3, Cluster: "prod-a", P: 9},
		&claimResp{OK: true},
		&claimResp{SizeMismatch: true},
	}
	codecs := []codec.Serializer{shamaton.New(), vmihailenco.New(), hashicorp.New(), codec.NewJSONCodec()}
	for _, c := range codecs {
		for _, in := range msgs {
			b, err := c.Marshal(in)
			if err != nil {
				t.Fatalf("%s: marshal %T: %v", c.Name(), in, err)
			}
			out := reflect.New(reflect.TypeOf(in).Elem()).Interface()
			if err := c.Unmarshal(b, out); err != nil {
				t.Fatalf("%s: unmarshal %T: %v", c.Name(), in, err)
			}
			if !reflect.DeepEqual(in, out) {
				t.Errorf("%s: %T changed in transit:\n in  %+v\n out %+v", c.Name(), in, in, out)
			}
		}
	}
}
