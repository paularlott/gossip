package gossip_test

import (
	"fmt"

	"github.com/paularlott/gossip"
)

// TCP-only operation. Setting ForceReliableTransport makes the socket
// transport send every message — gossip, health probes and state exchange
// alike — over TCP and skip binding the UDP listener entirely, so only the
// TCP port needs to be reachable. Use it where UDP is blocked or unreliable
// (some container platforms, load-balanced or NAT'd networks, overlay
// networks). Setting ListenFunc (e.g. for tsnet) enables it automatically.
//
// The trade-off is a TCP round trip (pooled where possible, see TCPPoolSize)
// for traffic that would otherwise be a single datagram; every node in the
// cluster should use the same setting, since a TCP-only node does not listen
// for UDP.
func Example_tcpOnly() {
	config := gossip.DefaultConfig()
	config.BindAddr = "0.0.0.0:3500"
	config.ForceReliableTransport = true // TCP only: no UDP listener, no UDP sends

	cluster, err := gossip.NewCluster(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	cluster.Start()
	defer cluster.Stop()

	// Join, send and handle messages exactly as with the default transport.
	_ = cluster.Join([]string{"10.0.0.2:3500"})
}
