package gossip

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Address struct {
	IP   net.IP
	Port uint16
}

// ResolveAddress converts a string to an Address struct
//
// The string can be in the form of:
//   - ip:port
//   - hostname:port
//   - hostname
//   - ip
//   - srv+service-name
func ResolveAddress(addressStr string) (Address, error) {
	// Check if this is an SRV record
	if strings.HasPrefix(addressStr, "srv+") {
		serviceName := addressStr[4:] // Remove the "srv+" prefix

		// Look up the SRV record
		_, addrs, err := net.LookupSRV("", "", serviceName)
		if err != nil {
			return Address{}, fmt.Errorf("failed to lookup SRV record: %w", err)
		}

		if len(addrs) == 0 {
			return Address{}, fmt.Errorf("no SRV records found for service: %s", serviceName)
		}

		// Use the first SRV record
		srv := addrs[0]

		// Resolve the target hostname to an IP
		ips, err := net.LookupIP(srv.Target)
		if err != nil {
			return Address{}, fmt.Errorf("failed to resolve SRV target hostname: %w", err)
		}

		if len(ips) == 0 {
			return Address{}, fmt.Errorf("no IP addresses found for SRV target: %s", srv.Target)
		}

		// Use the first IP and the port from the SRV record
		return Address{
			IP:   ips[0],
			Port: uint16(srv.Port),
		}, nil
	}

	// If the address string contains only numbers then assume it's a port and prefix with :
	if _, err := strconv.Atoi(addressStr); err == nil {
		addressStr = ":" + addressStr
	}

	// Split the address into host and port
	host, portStr, err := net.SplitHostPort(addressStr)
	if err != nil {
		// No port specified, use the host as is and default port
		host = addressStr
		portStr = ""
	}

	// If host is empty then use loopback address
	if host == "" {
		host = "127.0.0.1"
	}

	// Parse port if provided, otherwise use default
	var port uint16
	if portStr != "" {
		portVal, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return Address{}, fmt.Errorf("invalid port: %w", err)
		}
		port = uint16(portVal)
	} else {
		return Address{}, fmt.Errorf("port not specified")
	}

	// Resolve the IP address
	var ip net.IP
	if net.ParseIP(host) != nil {
		// Host is already an IP address
		ip = net.ParseIP(host)
	} else {
		// Host is a hostname, resolve it
		ips, err := net.LookupIP(host)
		if err != nil {
			return Address{}, fmt.Errorf("failed to resolve hostname: %w", err)
		}
		if len(ips) == 0 {
			return Address{}, fmt.Errorf("no IP addresses found for host: %s", host)
		}
		// Use the first IP address
		ip = ips[0]
	}

	return Address{IP: ip, Port: port}, nil
}
