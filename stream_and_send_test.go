package gossip

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip/codec"
)

// loopbackTransport simulates both broadcast send and direct dial/write/read paths using net.Pipe.
type loopbackTransport struct {
	cfg     *Config
	cluster *Cluster
	onSend  atomic.Value // func(*Packet)
	pktCh   chan *Packet
	chMu    sync.Mutex
}

func (t *loopbackTransport) PacketChannel() chan *Packet {
	t.chMu.Lock()
	defer t.chMu.Unlock()
	if t.pktCh == nil {
		t.pktCh = make(chan *Packet)
	}
	return t.pktCh
}
func (t *loopbackTransport) WebsocketHandler(_ context.Context, _ http.ResponseWriter, _ *http.Request) {
}
func (t *loopbackTransport) ResolveAddress(_ string) ([]Address, error) { return []Address{{}}, nil }

// DialPeer returns a net.Pipe connection and starts a goroutine that decodes a single packet
// and dispatches it to the cluster's handler (reply or stream).
func (t *loopbackTransport) DialPeer(_ *Node) (net.Conn, error) {
	client, server := net.Pipe()

	go func() {
		// Read length prefix
		br := bufio.NewReader(server)
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(br, lenBuf); err != nil {
			_ = server.Close()
			return
		}
		n := binary.BigEndian.Uint32(lenBuf)
		raw := make([]byte, n)
		if _, err := io.ReadFull(br, raw); err != nil {
			_ = server.Close()
			return
		}

		// Decode packet (replicates transport.packetFromBuffer for unencrypted/uncompressed)
		if len(raw) < 2 {
			_ = server.Close()
			return
		}
		flags := binary.BigEndian.Uint16(raw[:2])
		headerSize := int(flags & 0x7FFF)
		if len(raw)-2 < headerSize {
			_ = server.Close()
			return
		}

		pkt := NewPacket()
		if err := t.cfg.MsgCodec.Unmarshal(raw[2:2+headerSize], &pkt); err != nil {
			_ = server.Close()
			return
		}
		pkt.codec = t.cfg.MsgCodec
		pkt.payload = raw[2+headerSize:]
		pkt.conn = server

		// Dispatch directly to handler (bypassing accept loop)
		if h := t.cluster.handlers.getHandler(pkt.MessageType); h != nil {
			_ = h.dispatch(t.cluster, nil, pkt)
		} else {
			// No handler; just release
			pkt.Release()
			_ = server.Close()
		}
	}()

	return client, nil
}

func (t *loopbackTransport) WritePacket(conn net.Conn, packet *Packet) error {
	// Build raw packet: [4-byte length][2-byte header size+flags][header][payload]
	header, err := t.cfg.MsgCodec.Marshal(packet)
	if err != nil {
		return err
	}
	headerSize := uint16(len(header))
	// No compression/encryption in tests
	// Compose body
	body := make([]byte, 2+len(header)+len(packet.payload))
	binary.BigEndian.PutUint16(body[:2], headerSize)
	copy(body[2:2+len(header)], header)
	copy(body[2+len(header):], packet.payload)

	// Prefix with length
	total := uint32(len(body))
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, total)
	if _, err := conn.Write(lenBuf); err != nil {
		return err
	}
	if _, err := conn.Write(body); err != nil {
		return err
	}
	return nil
}

func (t *loopbackTransport) ReadPacket(conn net.Conn) (*Packet, error) {
	br := bufio.NewReader(conn)
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(br, lenBuf); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf)
	raw := make([]byte, n)
	if _, err := io.ReadFull(br, raw); err != nil {
		return nil, err
	}

	if len(raw) < 2 {
		return nil, io.ErrUnexpectedEOF
	}
	flags := binary.BigEndian.Uint16(raw[:2])
	headerSize := int(flags & 0x7FFF)
	if len(raw)-2 < headerSize {
		return nil, io.ErrUnexpectedEOF
	}

	pkt := NewPacket()
	if err := t.cfg.MsgCodec.Unmarshal(raw[2:2+headerSize], &pkt); err != nil {
		return nil, err
	}
	pkt.codec = t.cfg.MsgCodec
	pkt.payload = raw[2+headerSize:]
	pkt.conn = conn
	return pkt, nil
}

func (t *loopbackTransport) SendPacket(_ TransportType, _ []*Node, packet *Packet) error {
	if v := t.onSend.Load(); v != nil {
		v.(func(*Packet))(packet)
	}
	return nil
}

// --- Tests ---

func newLoopbackCluster(t *testing.T) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SocketTransportEnabled = false
	cfg.WebsocketProvider = nil
	cfg.Transport = nil // filled after cluster constructed so we can set cluster backref
	cfg.MsgCodec = codec.NewJsonCodec()
	cfg.HealthCheckInterval = 200 * time.Millisecond
	cfg.PingTimeout = 50 * time.Millisecond
	cfg.StateSyncInterval = 200 * time.Millisecond
	cfg.GossipInterval = 200 * time.Millisecond

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}

	lb := &loopbackTransport{cfg: cfg, cluster: c}
	c.transport = lb
	c.Start()
	return c
}

func TestOpenStream_ReadWrite(t *testing.T) {
	c := newLoopbackCluster(t)
	defer c.Stop()

	// Register stream handler
	streamType := UserMsg + 1
	dataType := UserMsg + 2
	replyType := UserMsg + 3

	c.HandleStreamFunc(streamType, func(_ *Node, pkt *Packet, conn net.Conn) {
		// Read a message, then reply
		var in struct{ Msg string }
		if err := c.ReadStreamMsg(conn, dataType, &in); err != nil {
			t.Errorf("server ReadStreamMsg: %v", err)
			return
		}
		if in.Msg != "hello" {
			t.Errorf("unexpected server in: %#v", in)
		}
		if err := c.WriteStreamMsg(conn, replyType, struct{ Msg string }{Msg: "world"}); err != nil {
			t.Errorf("server WriteStreamMsg: %v", err)
		}
		_ = conn.Close()
		_ = pkt // not used
	})

	// Create a remote node record so GetNode lookups work if needed
	remote := newNode(NodeID{7}, "")
	c.nodes.addOrUpdate(remote)

	// Open stream
	conn, err := c.OpenStream(remote, streamType, struct{ A int }{A: 1})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer conn.Close()

	// Send data, then read reply
	if err := c.WriteStreamMsg(conn, dataType, struct{ Msg string }{Msg: "hello"}); err != nil {
		t.Fatalf("client WriteStreamMsg: %v", err)
	}
	var out struct{ Msg string }
	if err := c.ReadStreamMsg(conn, replyType, &out); err != nil {
		t.Fatalf("client ReadStreamMsg: %v", err)
	}
	if out.Msg != "world" {
		t.Fatalf("unexpected client out: %#v", out)
	}
}

func TestClusterSendHelpers_BroadcastAndDirect(t *testing.T) {
	c := newLoopbackCluster(t)
	defer c.Stop()

	// Track send invocations
	lb := c.transport.(*loopbackTransport)
	sent := make(map[MessageType]int)
	var mu sync.Mutex
	lb.onSend.Store(func(pkt *Packet) {
		mu.Lock()
		sent[pkt.MessageType]++
		mu.Unlock()
	})

	// Peers for broadcast subset
	p1 := newNode(NodeID{11}, "")
	p2 := newNode(NodeID{12}, "")
	c.nodes.addOrUpdate(p1)
	c.nodes.addOrUpdate(p2)

	// Send helpers
	must := func(err error) {
		if err != nil {
			t.Fatalf("send error: %v", err)
		}
	}
	must(c.Send(UserMsg+10, struct{ X int }{1}))
	must(c.SendReliable(UserMsg+11, "r"))
	must(c.SendExcluding(UserMsg+12, 123, nil))
	must(c.SendReliableExcluding(UserMsg+13, []byte("x"), []NodeID{p1.ID}))
	must(c.SendTo(p1, UserMsg+14, "to"))
	must(c.SendToReliable(p1, UserMsg+15, "tor"))
	must(c.SendToPeers([]*Node{p1, p2}, UserMsg+16, 9))
	must(c.SendToPeersReliable([]*Node{p1, p2}, UserMsg+17, 10))

	// Allow async broadcast worker to flush
	time.Sleep(150 * time.Millisecond)

	for _, mt := range []MessageType{UserMsg + 10, UserMsg + 11, UserMsg + 12, UserMsg + 13, UserMsg + 14, UserMsg + 15, UserMsg + 16, UserMsg + 17} {
		mu.Lock()
		count := sent[mt]
		mu.Unlock()
		if count == 0 {
			t.Fatalf("expected send for message type %d", mt)
		}
	}
}

func TestSendToWithResponse_SimpleRoundTrip(t *testing.T) {
	c := newLoopbackCluster(t)
	defer c.Stop()

	// Register a request/response handler
	reqType := UserMsg + 20
	type req struct{ Q string }
	type resp struct{ A string }
	if err := c.HandleFuncWithReply(reqType, func(_ *Node, pkt *Packet) (interface{}, error) {
		var r req
		_ = pkt.Unmarshal(&r)
		return &resp{A: r.Q + "!"}, nil
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	// Add a remote
	remote := newNode(NodeID{8}, "")
	c.nodes.addOrUpdate(remote)

	// Do request/response
	var out resp
	if err := c.SendToWithResponse(remote, reqType, &req{Q: "ping"}, &out); err != nil {
		t.Fatalf("SendToWithResponse: %v", err)
	}
	if out.A != "ping!" {
		t.Fatalf("unexpected response: %#v", out)
	}
}

func TestSendToWithResponse_RoundTrip(t *testing.T) {
	c := newLoopbackCluster(t)
	defer c.Stop()

	// Register a reply handler
	rpcType := UserMsg + 21
	c.HandleFuncWithReply(rpcType, func(_ *Node, pkt *Packet) (interface{}, error) {
		var in struct{ A int }
		_ = pkt.Unmarshal(&in)
		return struct{ B int }{B: in.A + 1}, nil
	})

	// Destination node
	dst := newNode(NodeID{22}, "")
	c.nodes.addOrUpdate(dst)

	// Request/Response
	var resp struct{ B int }
	if err := c.SendToWithResponse(dst, rpcType, struct{ A int }{A: 41}, &resp); err != nil {
		t.Fatalf("SendToWithResponse: %v", err)
	}
	if resp.B != 42 {
		t.Fatalf("unexpected response: %#v", resp)
	}
}
