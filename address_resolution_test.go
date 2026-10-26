package gossip

import (
	"fmt"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tcpAddr(ip string, port int) *net.TCPAddr { return &net.TCPAddr{IP: net.ParseIP(ip), Port: port} }

// ipPorts formats resolved addresses as "ip:port", in order.
func ipPorts(addrs []Address) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, fmt.Sprintf("%s:%d", a.IP, a.Port))
	}
	return out
}

// An advertise address is a host:port (the host an IP, a name for the resolver, or empty for loopback), a bare port,
// or srv+service for the resolver's SRV records; IPv4 addresses are tried before IPv6 ones unless PreferIPv6.
func TestResolveAddress(t *testing.T) {
	mixed := []string{"192.168.1.1", "fd00::1"}
	tests := []struct {
		name       string
		address    string
		resolver   *mockResolver
		preferIPv6 bool
		want       []string // nil: an error
	}{
		{name: "ip and port", address: "127.0.0.1:8080", want: []string{"127.0.0.1:8080"}},
		{name: "port only", address: "8080", want: []string{"127.0.0.1:8080"}},
		{name: "empty host", address: ":9000", want: []string{"127.0.0.1:9000"}},
		{name: "no port", address: "127.0.0.1"},
		{name: "invalid port", address: "127.0.0.1:notaport"},
		{name: "port out of range", address: "127.0.0.1:70000"},
		{name: "name resolved, IPv4 first", address: "myhost:7946", resolver: &mockResolver{ips: mixed},
			want: []string{"192.168.1.1:7946", "fd00::1:7946"}},
		{name: "name resolved, IPv6 first", address: "myhost:7946", resolver: &mockResolver{ips: mixed}, preferIPv6: true,
			want: []string{"fd00::1:7946", "192.168.1.1:7946"}},
		{name: "name not resolved", address: "unknownhost:7946", resolver: &mockResolver{ipErr: fmt.Errorf("no such host")}},
		{name: "srv", address: "srv+myservice.consul", resolver: &mockResolver{srvAddrs: []*net.TCPAddr{
			tcpAddr("fd00::1", 7947), tcpAddr("10.0.0.1", 7946), tcpAddr("10.0.0.2", 7946)}},
			want: []string{"10.0.0.1:7946", "10.0.0.2:7946", "fd00::1:7947"}},
		{name: "srv, IPv6 first", address: "srv+myservice.consul", preferIPv6: true, resolver: &mockResolver{srvAddrs: []*net.TCPAddr{
			tcpAddr("10.0.0.1", 7946), tcpAddr("::1", 7947)}}, want: []string{"::1:7947", "10.0.0.1:7946"}},
		{name: "srv lookup fails", address: "srv+myservice.consul", resolver: &mockResolver{srvErr: fmt.Errorf("SRV lookup failed")}},
		{name: "srv has no records", address: "srv+myservice.consul", resolver: &mockResolver{srvAddrs: []*net.TCPAddr{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.PreferIPv6 = tt.preferIPv6
			if tt.resolver != nil {
				cfg.Resolver = tt.resolver
			}
			addrs, err := NewSocketTransport(cfg).resolveAddress(tt.address)
			if tt.want == nil {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, ipPorts(addrs))
			if tt.resolver != nil && len(tt.resolver.srvQueries) > 0 {
				assert.Equal(t, []string{"myservice.consul."}, tt.resolver.srvQueries, "queried as an absolute name")
			}
		})
	}

	t.Run("srv without resolving to addresses gives ports only", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Resolver = &mockResolver{srvAddrs: []*net.TCPAddr{{Port: 7946}, {Port: 7947}}}
		addrs, err := NewSocketTransport(cfg).lookupSRV("myservice.consul", false)
		require.NoError(t, err)
		require.Len(t, addrs, 2)
		assert.Nil(t, addrs[0].IP)
		assert.Equal(t, 7946, addrs[0].Port)
		assert.Equal(t, 7947, addrs[1].Port)
	})
}

func TestParseBindAddress(t *testing.T) {
	tests := []struct {
		addr string
		ip   string
		port int
		err  bool
	}{
		{addr: ":8080", port: 8080},
		{addr: "192.168.1.1:9000", ip: "192.168.1.1", port: 9000},
		{addr: "not[valid:address", err: true},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			got, err := NewSocketTransport(DefaultConfig()).parseBindAddress(tt.addr)
			if tt.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.port, got.Port)
			if tt.ip != "" {
				assert.True(t, got.IP.Equal(net.ParseIP(tt.ip)), "ip %s", got.IP)
			}
		})
	}
}

// A node's address is resolved once, from its advertise address, and kept until it is cleared.
func TestEnsureNodeAddressResolved(t *testing.T) {
	srv := &mockResolver{srvAddrs: []*net.TCPAddr{tcpAddr("10.0.0.1", 443)}}

	t.Run("socket", func(t *testing.T) {
		tests := []struct {
			name      string
			advertise string
			resolved  *Address // already resolved
			resolver  *mockResolver
			want      string // "" with wantErr
			wantErr   bool
		}{
			{name: "ip and port", advertise: "127.0.0.1:8080", want: "127.0.0.1:8080"},
			{name: "nothing to resolve", advertise: "", wantErr: true},
			{name: "already resolved", advertise: "127.0.0.1:8080", resolved: &Address{IP: net.ParseIP("10.9.9.9"), Port: 1}, want: "10.9.9.9:1"},
			{name: "srv takes the first record", advertise: "srv+svc.consul", resolver: srv, want: "10.0.0.1:443"},
			{name: "unresolvable", advertise: "nowhere:1", resolver: &mockResolver{ipErr: fmt.Errorf("no such host")}, wantErr: true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cfg := DefaultConfig()
				if tt.resolver != nil {
					cfg.Resolver = tt.resolver
				}
				node := newNode(NodeID(uuid.New()), tt.advertise)
				if tt.resolved != nil {
					node.SetAddress(*tt.resolved)
				}
				err := NewSocketTransport(cfg).ensureNodeAddressResolved(node)
				if tt.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.want, ipPorts([]Address{node.GetAddress()})[0])
			})
		}
	})

	t.Run("http", func(t *testing.T) {
		tests := []struct {
			name      string
			advertise string
			bindPath  string
			resolved  string // an address URL already set
			resolver  *mockResolver
			want      string
			wantErr   bool
		}{
			{name: "http url takes the path", advertise: "http://example.com:8080", bindPath: "/gossip", want: "http://example.com:8080/gossip"},
			{name: "https url", advertise: "https://myhost:8443", bindPath: "/gossip", want: "https://myhost:8443/gossip"},
			{name: "no path configured", advertise: "http://example.com:8080", want: "http://example.com:8080/"},
			{name: "already resolved", advertise: "https://myhost:8443", resolved: "https://myhost:8443/other", want: "https://myhost:8443/other"},
			{name: "nothing to resolve", advertise: "", wantErr: true},
			{name: "no scheme is https", advertise: "myhost:8443", bindPath: "/gossip", want: "https://myhost:8443"},
			{name: "srv takes the first record's port", advertise: "srv+https://svc.consul", bindPath: "/gossip", resolver: srv,
				want: "https://svc.consul:443/gossip"},
			{name: "srv lookup fails", advertise: "srv+https://svc.consul", resolver: &mockResolver{srvErr: fmt.Errorf("SRV failed")}, wantErr: true},
			{name: "srv has no records", advertise: "srv+https://svc.consul", resolver: &mockResolver{srvAddrs: []*net.TCPAddr{}}, wantErr: true},
			{name: "malformed srv url", advertise: "srv+://[bad url\\", wantErr: true},
			{name: "malformed url", advertise: "://[bad url\\", wantErr: true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.BindAddr = tt.bindPath
				cfg.MsgCodec = codec.NewJSONCodec()
				if tt.resolver != nil {
					cfg.Resolver = tt.resolver
				}
				node := newNode(NodeID(uuid.New()), tt.advertise)
				if tt.resolved != "" {
					node.SetAddress(Address{URL: tt.resolved})
				}
				err := NewHTTPTransport(cfg).ensureNodeAddressResolved(node)
				if tt.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.want, node.GetAddress().URL)
			})
		}
	})
}

func TestDefaultResolver(t *testing.T) {
	r := NewDefaultResolver()
	ips, err := r.LookupIP("localhost")
	require.NoError(t, err)
	assert.NotEmpty(t, ips)

	_, err = r.LookupSRV("")
	assert.Error(t, err, "an empty service")
}
