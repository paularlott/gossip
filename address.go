package gossip

import (
	"fmt"
	"net"
)

type Address struct {
	IP   net.IP
	Port int
}

func (a Address) String() string {
	return fmt.Sprintf("%s:%d", a.IP.String(), a.Port)
}
