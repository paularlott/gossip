package gossip

import "net"

// Resolver defines the interface for DNS resolution
type Resolver interface {
	// LookupIP takes a hostname and converts it to a list of IP addresses
	LookupIP(host string) ([]string, error)

	// LookupSRV takes a service name and returns a list of service records as TCPAddr that match the service name
	LookupSRV(service string) ([]*net.TCPAddr, error)
}
