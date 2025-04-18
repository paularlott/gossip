package gossip

import (
	"fmt"
	"net"
)

type Address struct {
	IP   net.IP `msgpack:"ip,omitempty" json:"ip,omitempty"`
	Port int    `msgpack:"port,omitempty" json:"port,omitempty"`
	URL  string `msgpack:"url,omitempty" json:"url,omitempty"`
}

func (a Address) String() string {
	output := ""

	if a.Port > 0 {
		output = fmt.Sprintf("%s:%d", a.IP.String(), a.Port)
	}

	if a.URL != "" {
		if output != "" {
			output += ", "
		}

		output += a.URL
	}

	return output
}
