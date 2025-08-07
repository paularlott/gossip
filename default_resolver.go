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
	// Make sure the service ends with a dot
	if service[len(service)-1] != '.' {
		service += "."
	}

	_, addrs, err := net.LookupSRV("", "", service)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup SRV record: %w", err)
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("no SRV records found for service")
	}

	result := make([]*net.TCPAddr, len(addrs))
	for i, srv := range addrs {
		result[i] = &net.TCPAddr{
			IP:   net.ParseIP(srv.Target),
			Port: int(srv.Port),
		}
		// If IP parsing fails, try to resolve the hostname
		if result[i].IP == nil {
			ips, err := r.LookupIP(srv.Target)
			if err != nil || len(ips) == 0 {
				continue
			}
			result[i].IP = net.ParseIP(ips[0])
		}
	}
	return result, nil
}
