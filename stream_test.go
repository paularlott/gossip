package gossip

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec/shamaton"
	"github.com/paularlott/gossip/encryption/aes"
	"github.com/paularlott/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testStreamMsg MessageType = UserMsg + 50
	testStreamErr MessageType = UserMsg + 51
)

type streamReq struct {
	Size int `msgpack:"size" json:"size"`
}

// mkStreamCluster starts a socket cluster serving test streams: testStreamMsg
// replies with req.Size bytes of a deterministic pattern, testStreamErr
// writes some data then fails.
func mkStreamCluster(t *testing.T, addr string, encrypt bool) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.BindAddr = addr
	cfg.AdvertiseAddr = addr
	cfg.Transport = NewSocketTransport(cfg)
	cfg.MsgCodec = shamaton.New()
	cfg.Logger = logger.NewNullLogger()
	if encrypt {
		cfg.EncryptionKey = []byte("0123456789abcdef0123456789abcdef")
		cfg.Cipher = aes.New()
	}
	c, err := NewCluster(cfg)
	require.NoError(t, err)
	require.NoError(t, c.HandleStreamFunc(testStreamMsg, func(sender *Node, packet *Packet, w io.Writer) error {
		var req streamReq
		if err := packet.Unmarshal(&req); err != nil {
			return err
		}
		_, err := io.Copy(w, io.LimitReader(&patternReader{}, int64(req.Size)))
		return err
	}))
	require.NoError(t, c.HandleStreamFunc(testStreamErr, func(sender *Node, packet *Packet, w io.Writer) error {
		w.Write(bytes.Repeat([]byte("x"), 100000))
		return errors.New("disk on fire")
	}))
	c.Start()
	return c
}

// patternReader yields an endless, position-dependent byte pattern.
type patternReader struct{ off int }

func (p *patternReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte((p.off + i) * 7)
	}
	p.off += len(b)
	return len(b), nil
}

func patternSum(n int) [32]byte {
	data, _ := io.ReadAll(io.LimitReader(&patternReader{}, int64(n)))
	return sha256.Sum256(data)
}

func joinedPair(t *testing.T, encrypt bool) (*Cluster, *Cluster) {
	t.Helper()
	addr1, addr2 := getFreeTCPAddress(t), getFreeTCPAddress(t)
	c1 := mkStreamCluster(t, addr1, encrypt)
	t.Cleanup(c1.Stop)
	c2 := mkStreamCluster(t, addr2, encrypt)
	t.Cleanup(c2.Stop)
	require.NoError(t, c2.Join([]string{addr1}))
	require.Eventually(t, func() bool {
		return c1.GetNode(c2.LocalNode().ID) != nil && c2.GetNode(c1.LocalNode().ID) != nil
	}, 5*time.Second, 50*time.Millisecond)
	return c1, c2
}

func TestStream_SocketLargeReply(t *testing.T) {
	for _, encrypt := range []bool{false, true} {
		c1, c2 := joinedPair(t, encrypt)
		// Far beyond the packet size limit.
		const size = 24*1024*1024 + 123
		r, err := c2.OpenStream(context.Background(), c2.GetNode(c1.LocalNode().ID), testStreamMsg, &streamReq{Size: size})
		require.NoError(t, err)
		data, err := io.ReadAll(r)
		require.NoError(t, err, "encrypt=%v", encrypt)
		require.NoError(t, r.Close())
		assert.Equal(t, size, len(data))
		assert.Equal(t, patternSum(size), sha256.Sum256(data), "encrypt=%v", encrypt)
	}
}

func TestStream_EmptyReply(t *testing.T) {
	c1, c2 := joinedPair(t, false)
	r, err := c2.OpenStream(context.Background(), c2.GetNode(c1.LocalNode().ID), testStreamMsg, &streamReq{Size: 0})
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Empty(t, data)
	r.Close()
}

func TestStream_HandlerError(t *testing.T) {
	c1, c2 := joinedPair(t, true)
	r, err := c2.OpenStream(context.Background(), c2.GetNode(c1.LocalNode().ID), testStreamErr, &streamReq{})
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk on fire")
	assert.Greater(t, len(data), 0, "data written before the error is delivered")
}

func TestStream_NoHandler(t *testing.T) {
	c1, c2 := joinedPair(t, false)
	r, err := c2.OpenStream(context.Background(), c2.GetNode(c1.LocalNode().ID), UserMsg+99, &streamReq{})
	require.NoError(t, err)
	defer r.Close()
	_, err = io.ReadAll(r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no stream handler")
}

func TestStream_UnknownSenderRefused(t *testing.T) {
	addr1 := getFreeTCPAddress(t)
	c1 := mkStreamCluster(t, addr1, false)
	defer c1.Stop()
	c2 := mkStreamCluster(t, getFreeTCPAddress(t), false)
	defer c2.Stop()
	// c2 never joins: c1 does not know it.
	target := newNode(c1.LocalNode().ID, addr1)
	r, err := c2.OpenStream(context.Background(), target, testStreamMsg, &streamReq{Size: 10})
	require.NoError(t, err)
	defer r.Close()
	_, err = io.ReadAll(r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown node")
}

func TestStream_ContextCancelAborts(t *testing.T) {
	c1, c2 := joinedPair(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	r, err := c2.OpenStream(ctx, c2.GetNode(c1.LocalNode().ID), testStreamMsg, &streamReq{Size: 1 << 30})
	require.NoError(t, err)
	defer r.Close()
	buf := make([]byte, 1024)
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)
	cancel()
	_, err = io.Copy(io.Discard, r)
	assert.Error(t, err, "a cancelled stream must not read to the end")
}

func TestStream_FramesDetectTruncation(t *testing.T) {
	var buf bytes.Buffer
	fw := newFrameWriter(context.Background(), &buf, nil, nil)
	fw.Write(bytes.Repeat([]byte("a"), streamFrameSize*2+10))
	fw.finish(nil)
	whole := buf.Bytes()

	// Complete: reads to EOF.
	data, err := io.ReadAll(newFrameReader(bytes.NewReader(whole), io.NopCloser(nil), nil, nil))
	require.NoError(t, err)
	assert.Len(t, data, streamFrameSize*2+10)

	// Cut before the end frame: an unexpected EOF, never a clean end.
	_, err = io.ReadAll(newFrameReader(bytes.NewReader(whole[:len(whole)-4]), io.NopCloser(nil), nil, nil))
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), "got %v", err)

	// A corrupt length is refused rather than allocated.
	bad := []byte{0xfe, 0xff, 0xff, 0x7f}
	_, err = io.ReadAll(newFrameReader(bytes.NewReader(bad), io.NopCloser(nil), nil, nil))
	assert.Error(t, err)
}

func TestStream_HTTPTransport(t *testing.T) {
	serverCfg := DefaultConfig()
	serverCfg.MsgCodec = shamaton.New()
	serverCfg.BearerToken = "secret"
	server := NewHTTPTransport(serverCfg)
	server.SetStreamHandler(func(packet *Packet, w io.Writer) error {
		defer packet.Release()
		var req streamReq
		if err := packet.Unmarshal(&req); err != nil {
			return err
		}
		if req.Size < 0 {
			return errors.New("negative size")
		}
		_, err := io.Copy(w, io.LimitReader(&patternReader{}, int64(req.Size)))
		return err
	})
	srv := httptest.NewServer(http.HandlerFunc(server.HandleGossipRequest))
	defer srv.Close()

	clientCfg := DefaultConfig()
	clientCfg.MsgCodec = shamaton.New()
	clientCfg.BearerToken = "secret"
	client := NewHTTPTransport(clientCfg)

	open := func(size int) (io.ReadCloser, error) {
		p := NewPacket()
		defer p.Release()
		p.MessageType = testStreamMsg
		p.SetCodec(clientCfg.MsgCodec)
		payload, _ := clientCfg.MsgCodec.Marshal(&streamReq{Size: size})
		p.SetPayload(payload)
		return client.OpenStream(context.Background(), newNode(NodeID(uuid.New()), srv.URL), p)
	}

	const size = 9*1024*1024 + 7
	r, err := open(size)
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	r.Close()
	assert.Equal(t, patternSum(size), sha256.Sum256(data))

	r, err = open(-1)
	require.NoError(t, err)
	_, err = io.ReadAll(r)
	r.Close()
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "negative size"), err.Error())

	// Without the bearer token the stream is refused.
	clientCfg.BearerToken = ""
	_, err = open(10)
	assert.Error(t, err)
}

func TestStream_UnsupportedTransport(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MsgCodec = shamaton.New()
	cfg.Transport = &noStreamTransport{}
	c, err := NewCluster(cfg)
	require.NoError(t, err)
	_, err = c.OpenStream(context.Background(), newNode(NodeID(uuid.New()), "127.0.0.1:1"), testStreamMsg, &streamReq{})
	assert.True(t, errors.Is(err, ErrStreamsUnsupported), "got %v", err)
}

// noStreamTransport is a transport without stream support.
type noStreamTransport struct{}

func (noStreamTransport) Name() string                                  { return "none" }
func (noStreamTransport) Start(context.Context, *sync.WaitGroup) error  { return nil }
func (noStreamTransport) PacketChannel() chan *Packet                   { return nil }
func (noStreamTransport) Send(TransportType, *Node, *Packet) error      { return nil }
func (noStreamTransport) SendWithReply(*Node, *Packet) (*Packet, error) { return nil, nil }

// Many streams at once between the same pair, each with its own reply.
func TestStream_Concurrent(t *testing.T) {
	c1, c2 := joinedPair(t, true)
	node := c2.GetNode(c1.LocalNode().ID)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			size := 100000 + i*37
			r, err := c2.OpenStream(context.Background(), node, testStreamMsg, &streamReq{Size: size})
			if err != nil {
				errs <- err
				return
			}
			defer r.Close()
			data, err := io.ReadAll(r)
			if err != nil {
				errs <- err
				return
			}
			if sha256.Sum256(data) != patternSum(size) {
				errs <- fmt.Errorf("stream %d: content differs", i)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// A stream cut by the serving node shutting down ends in an error, never a
// clean (and short) end.
func TestStream_ServerStopsMidStream(t *testing.T) {
	c1, c2 := joinedPair(t, false)
	r, err := c2.OpenStream(context.Background(), c2.GetNode(c1.LocalNode().ID), testStreamMsg, &streamReq{Size: 1 << 30})
	require.NoError(t, err)
	defer r.Close()
	buf := make([]byte, 1<<20)
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)
	c1.Stop()
	_, err = io.Copy(io.Discard, r)
	assert.Error(t, err)
	assert.False(t, errors.Is(err, io.EOF))
}

// Packets from nodes running the previous release (14 bit header size, no
// stream flag) still decode: the stream flag took a bit header sizes never
// reach.
func TestStream_OldPacketsStillDecode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MsgCodec = shamaton.New()
	st := NewSocketTransport(cfg)
	p := NewPacket()
	p.MessageType = UserMsg + 1
	p.SetCodec(cfg.MsgCodec)
	payload, _ := cfg.MsgCodec.Marshal(&streamReq{Size: 42})
	p.SetPayload(payload)
	raw, err := st.packetToBuffer(p, true)
	require.NoError(t, err)
	flags := binary.LittleEndian.Uint16(raw[:2])
	assert.Zero(t, flags&streamFlag, "a normal packet never carries the stream flag")
	assert.Less(t, int(flags&0x3FFF), 0x2000, "headers stay below the bit the stream flag uses")

	back, replyExpected, err := st.packetFromBuffer(raw)
	require.NoError(t, err)
	assert.True(t, replyExpected)
	var req streamReq
	require.NoError(t, back.Unmarshal(&req))
	assert.Equal(t, 42, req.Size)
}

// Streams over HTTPS, with the server's write timeout shorter than the
// transfer.
func TestStream_HTTPSAndWriteTimeout(t *testing.T) {
	serverCfg := DefaultConfig()
	serverCfg.MsgCodec = shamaton.New()
	server := NewHTTPTransport(serverCfg)
	server.SetStreamHandler(func(packet *Packet, w io.Writer) error {
		defer packet.Release()
		for i := 0; i < 8; i++ {
			if _, err := w.Write(bytes.Repeat([]byte{byte(i)}, 256*1024)); err != nil {
				return err
			}
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(server.HandleGossipRequest))
	srv.Config.WriteTimeout = 100 * time.Millisecond
	srv.StartTLS()
	defer srv.Close()

	clientCfg := DefaultConfig()
	clientCfg.MsgCodec = shamaton.New()
	clientCfg.InsecureSkipVerify = true
	client := NewHTTPTransport(clientCfg)
	p := NewPacket()
	defer p.Release()
	p.MessageType = testStreamMsg
	p.SetCodec(clientCfg.MsgCodec)
	r, err := client.OpenStream(context.Background(), newNode(NodeID(uuid.New()), srv.URL), p)
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.NoError(t, err, "a stream outlasting the write timeout must still complete")
	assert.Len(t, data, 8*256*1024)
}

// The frame reader never panics or over-allocates on arbitrary input.
func FuzzFrameReader(f *testing.F) {
	var buf bytes.Buffer
	fw := newFrameWriter(context.Background(), &buf, nil, nil)
	fw.Write([]byte("hello"))
	fw.finish(nil)
	f.Add(buf.Bytes())
	buf.Reset()
	fw = newFrameWriter(context.Background(), &buf, nil, nil)
	fw.finish(errors.New("boom"))
	f.Add(buf.Bytes())
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0x10, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		io.Copy(io.Discard, newFrameReader(bytes.NewReader(data), io.NopCloser(nil), nil, nil))
	})
}

func BenchmarkStreamThroughput(b *testing.B) {
	for _, encrypt := range []bool{false, true} {
		b.Run(fmt.Sprintf("encrypt=%v", encrypt), func(b *testing.B) {
			t := &testing.T{}
			c1, c2 := joinedPair(t, encrypt)
			node := c2.GetNode(c1.LocalNode().ID)
			const size = 64 << 20
			b.SetBytes(size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := c2.OpenStream(context.Background(), node, testStreamMsg, &streamReq{Size: size})
				if err != nil {
					b.Fatal(err)
				}
				io.Copy(io.Discard, r)
				r.Close()
			}
		})
	}
}
