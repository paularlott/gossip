package gossip

import (
	"fmt"
	"net"
)

// DefaultResolver implements the Resolver interface using Go's standard net package
type DefaultResolver struct{}

// NewDefaultResolver creates a new DefaultResolver
func NewDefaultResolver() *DefaultResolver {
	return &DefaultResolver{}
}

// LookupIP takes a hostname and converts it to a list of IP addresses
func (r *DefaultResolver) LookupIP(host string) ([]string, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve hostname: %w", err)
	}

	result := make([]string, len(ips))
	for i, ip := range ips {
		result[i] = ip.String()
	}
	return result, nil
}

// LookupSRV takes a service name and returns a list of service records as TCPAddr that match the service name
func (r *DefaultResolver) LookupSRV(service string) ([]*net.TCPAddr, error) {
	// Guard empty service to avoid indexing panic
	if len(service) == 0 {
		return nil, fmt.Errorf("empty service")
	}
	// Make sure the service ends with a dot so net.LookupSRV treats it as absolute
	if service[len(service)-1] != '.' {
		service += "."
	}

	// Per net.LookupSRV docs, empty service/proto means lookup SRV for name as-is.
	_, addrs, err := net.LookupSRV("", "", service)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup SRV record: %w", err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no SRV records found for service")
	}

	// net.LookupSRV already returns results sorted by priority and randomized by weight.
	// Build a flat list of TCPAddrs using all resolved IPs for each valid SRV target.
	var result []*net.TCPAddr
	for _, srv := range addrs {
		// Skip invalid/no-service entries
		if srv.Port == 0 || srv.Target == "." {
			continue
		}
		ips, err := r.LookupIP(srv.Target)
		if err != nil || len(ips) == 0 {
			continue
		}
		for _, ipStr := range ips {
			if ip := net.ParseIP(ipStr); ip != nil {
				result = append(result, &net.TCPAddr{IP: ip, Port: int(srv.Port)})
			}
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no SRV records with resolvable IPs")
	}
	return result, nil
}
