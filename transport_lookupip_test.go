package gossip

import (
	"net"
	"testing"
)

func TestLookupIP_PortParsing(t *testing.T) {
	cfg := DefaultConfig()
	tr := newTestTransport(t, cfg)

	// numeric only => port
	addrs, err := tr.lookupIP("8081", 3500)
	if err != nil {
		t.Fatalf("lookupIP numeric: %v", err)
	}
	if len(addrs) != 1 || addrs[0].Port != 8081 || addrs[0].IP.String() != "127.0.0.1" {
		t.Fatalf("unexpected numeric parsing: %v", addrs)
	}

	// missing port => default
	addrs, err = tr.lookupIP("127.0.0.1", 9999)
	if err != nil {
		t.Fatalf("lookupIP default: %v", err)
	}
	if len(addrs) != 1 || addrs[0].IP.String() != "127.0.0.1" || addrs[0].Port != 9999 {
		t.Fatalf("unexpected default port handling: %v", addrs)
	}

	// with port => keep
	addrs, err = tr.lookupIP("127.0.0.1:4321", 9999)
	if err != nil {
		t.Fatalf("lookupIP host:port: %v", err)
	}
	if len(addrs) != 1 || addrs[0].Port != 4321 {
		t.Fatalf("unexpected explicit port handling: %v", addrs)
	}
}

type fixedResolver struct{ ips []string }

func (r *fixedResolver) LookupIP(host string) ([]string, error)           { return r.ips, nil }
func (r *fixedResolver) LookupSRV(service string) ([]*net.TCPAddr, error) { return nil, nil }

func TestLookupIP_Hostname_OrderPref(t *testing.T) {
	// Hostname resolves to both v4 and v6
	res := &fixedResolver{ips: []string{"2001:db8::2", "192.0.2.5"}}

	cfg := DefaultConfig()
	cfg.Resolver = res
	tr := newTestTransport(t, cfg)

	// Default prefer IPv4
	addrs, err := tr.lookupIP("example.test:1234", 3500)
	if err != nil {
		t.Fatalf("lookupIP: %v", err)
	}
	if len(addrs) != 2 || addrs[0].IP.To4() == nil || addrs[0].Port != 1234 || addrs[1].IP.To4() != nil {
		t.Fatalf("unexpected v4-first ordering: %v", addrs)
	}

	// Prefer IPv6
	cfg2 := DefaultConfig()
	cfg2.Resolver = res
	cfg2.PreferIPv6 = true
	tr2 := newTestTransport(t, cfg2)
	addrs, err = tr2.lookupIP("example.test:1234", 3500)
	if err != nil {
		t.Fatalf("lookupIP v6: %v", err)
	}
	if len(addrs) != 2 || addrs[0].IP.To4() != nil || addrs[1].IP.To4() == nil {
		t.Fatalf("unexpected v6-first ordering: %v", addrs)
	}
}
