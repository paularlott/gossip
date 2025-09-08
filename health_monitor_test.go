package gossip

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/codec"
)

func TestHealthMonitorBasic(t *testing.T) {
	config1 := DefaultConfig()
	config1.BindAddr = "127.0.0.1:0"
	config1.MsgCodec = codec.NewJsonCodec()
	config1.Transport = NewSocketTransport(config1)
	config1.HealthCheckInterval = 100 * time.Millisecond
	config1.SuspectTimeout = 50 * time.Millisecond

	cluster1, err := NewCluster(config1)
	if err != nil {
		t.Fatal(err)
	}
	cluster1.Start()
	defer cluster1.Stop()

	// Wait for health monitor to start
	time.Sleep(200 * time.Millisecond)

	// Verify health monitor is running
	if cluster1.healthMonitor == nil {
		t.Fatal("Health monitor not initialized")
	}
}