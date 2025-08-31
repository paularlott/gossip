package codec

import "testing"

func TestJsonCodec_RoundTrip(t *testing.T) {
	c := NewJsonCodec()
	in := map[string]interface{}{"a": 1, "b": "x"}
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := map[string]interface{}{}
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out) != 2 || out["b"].(string) != "x" {
		t.Fatalf("unexpected round-trip: %#v", out)
	}
}
