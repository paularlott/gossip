package gossip

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/hlc"
)

// Regression test for a use-after-release on the dispatch error path:
// handleIncomingPacket used to read packet.MessageType for its log line after
// dispatch had already released the packet back to the pool, racing with any
// goroutine that took the same packet from the pool and initialised it. Local
// metadata churn makes that reuse immediate (every SetString mints and
// broadcasts a packet), so it is the background load here. Meaningful under
// -race; without it the test only checks the path does not panic or leak.
func TestDispatchErrorPathDoesNotTouchReleasedPacket(t *testing.T) {
	c := newUnitCluster(t)
	c.Start()

	const failingMsg = UserMsg + 7
	if err := c.HandleFunc(failingMsg, func(*Node, *Packet) error {
		return errors.New("handler failed")
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	const replyFailingMsg = UserMsg + 8
	if err := c.HandleFuncWithReply(replyFailingMsg, func(*Node, *Packet) (interface{}, error) {
		return nil, errors.New("reply handler failed")
	}); err != nil {
		t.Fatalf("register reply: %v", err)
	}

	// A known sender, so packets are dispatched rather than dropped.
	sender := newNode(NodeID(uuid.New()), "127.0.0.1:1")
	c.nodes.addIfNotExists(sender)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Metadata churn: each SetString creates and broadcasts a pooled packet.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				c.LocalMetadata().SetString(fmt.Sprintf("k%d", g), fmt.Sprint(i))
			}
		}(g)
	}

	// Inbound packets whose handlers fail, so dispatch returns an error after
	// releasing the packet.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				p := NewPacket()
				if i%2 == 0 {
					p.MessageType = failingMsg
				} else {
					p.MessageType = replyFailingMsg
				}
				p.SenderID = sender.ID
				p.MessageID = MessageID(hlc.Now())
				p.TTL = uint8(i % 2) // exercise both the forward and no-forward paths
				p.codec = c.config.MsgCodec
				p.payload = []byte(`{}`)
				c.handleIncomingPacket(p)
			}
		}(g)
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// A packet for a message type with no handler must still be released, so a
// connection-owning packet does not hold its connection open.
func TestUnhandledPacketIsReleased(t *testing.T) {
	c := newUnitCluster(t)
	sender := newNode(NodeID(uuid.New()), "127.0.0.1:1")
	c.nodes.addIfNotExists(sender)

	p := NewPacket()
	p.MessageType = UserMsg + 99 // nothing registered
	p.SenderID = sender.ID
	p.MessageID = MessageID(hlc.Now())
	p.TTL = 0
	p.codec = c.config.MsgCodec
	conn := &closeRecorder{}
	p.SetConn(conn)

	c.handleIncomingPacket(p)
	if !conn.closed {
		t.Fatal("unhandled packet was not released (its connection is still open)")
	}
}

// closeRecorder is a net.Conn that only records Close.
type closeRecorder struct {
	net.Conn
	closed bool
}

func (c *closeRecorder) Close() error {
	c.closed = true
	return nil
}
