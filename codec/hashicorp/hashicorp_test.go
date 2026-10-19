package hashicorp

import "testing"

// Strings inside interface{} values come back as strings, as with the other codecs (node metadata relies on it).
func TestStringsInInterfacesStayStrings(t *testing.T) {
	c := New()
	b, err := c.Marshal(map[string]interface{}{"zone": "eu-west", "n": 3})
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]interface{}
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if s, ok := out["zone"].(string); !ok || s != "eu-west" {
		t.Fatalf("zone decoded as %T %v", out["zone"], out["zone"])
	}
	// []byte fields of a typed struct are unaffected
	type rec struct {
		Data []byte `codec:"d"`
	}
	b, _ = c.Marshal(rec{Data: []byte{1, 2, 3}})
	var r rec
	if err := c.Unmarshal(b, &r); err != nil || len(r.Data) != 3 || r.Data[2] != 3 {
		t.Fatalf("typed []byte: %v %v", r.Data, err)
	}
}
