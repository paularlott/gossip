package gossip

import (
	"runtime"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacketRefCounting(t *testing.T) {
	p := NewPacket()
	require.EqualValues(t, 1, refs(p), "a new packet has one reference")

	assert.Same(t, p, p.AddRef(), "AddRef returns the packet")
	assert.EqualValues(t, 2, refs(p))

	extra := []*Packet{p.AddRef(), p.AddRef(), p.AddRef()}
	assert.EqualValues(t, 5, refs(p))
	for _, e := range extra {
		e.Release()
	}
	assert.EqualValues(t, 2, refs(p))

	p.Release()
	assert.EqualValues(t, 1, refs(p), "still held")
	p.Release()
	assert.EqualValues(t, 0, refs(p), "the last release gives the packet up")
}

// Whichever of many holders releases last, the count comes back to the owner's one.
func TestPacketRefCountingConcurrent(t *testing.T) {
	p := NewPacket()
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ref := p.AddRef()
			runtime.Gosched()
			ref.Release()
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, refs(p))
	p.Release()
}

// Packets taken from and given back to the pool by many goroutines at once.
func TestPacketPoolConcurrentUse(t *testing.T) {
	var wg sync.WaitGroup
	for w := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 2000 {
				p := NewPacket()
				if (w+i)%3 == 0 {
					for range 3 {
						p.AddRef().Release()
					}
				}
				p.Release()
			}
		}()
	}
	wg.Wait()
}

// The last release clears what the packet carried, closes the connection it owned, and leaves the packet unable to reply.
func TestPacketReleaseResets(t *testing.T) {
	conn := &mockConn{}
	p := NewPacket()
	p.SenderID = NodeID(uuid.New())
	target, tag := NodeID(uuid.New()), "web"
	p.TargetNodeID, p.Tag = &target, &tag
	p.SetPayload([]byte("payload"))
	p.SetConn(conn)
	p.keyed, p.reliable = true, true
	require.True(t, p.CanReply(), "a packet with a connection can reply")

	held := p.AddRef()
	p.Release()
	assert.False(t, conn.closed, "the connection stays open while the packet is held")
	assert.NotNil(t, held.Payload())

	held.Release() // the last reference: p and held are the same packet, now given up
	assert.Equal(t, EmptyNodeID, p.SenderID)
	assert.Nil(t, p.TargetNodeID)
	assert.Nil(t, p.Tag)
	assert.Nil(t, p.Payload())
	assert.False(t, p.keyed)
	assert.False(t, p.reliable)
	assert.False(t, p.CanReply())
	assert.True(t, conn.closed, "the connection is closed with the packet")

	ch := make(chan *Packet, 1)
	q := NewPacket()
	q.SetReplyChan(ch)
	require.True(t, q.CanReply())
	q.Release()
	assert.False(t, q.CanReply(), "the reply channel is dropped")
}

func TestPacketPayloadAndCodec(t *testing.T) {
	p := NewPacket()
	defer p.Release()
	c := codec.NewJSONCodec()
	p.SetCodec(c)
	assert.Same(t, c, p.Codec())

	p.SetPayload([]byte(`{"key":"value"}`))
	assert.Equal(t, `{"key":"value"}`, string(p.Payload()))
	var got map[string]interface{}
	require.NoError(t, p.Unmarshal(&got))
	assert.Equal(t, "value", got["key"])
}

func TestPacketSendReply(t *testing.T) {
	t.Run("no way to reply", func(t *testing.T) {
		p := NewPacket()
		defer p.Release()
		assert.False(t, p.CanReply())
		assert.Error(t, p.SendReply())
	})

	t.Run("reply channel", func(t *testing.T) {
		ch := make(chan *Packet, 1)
		reply := NewPacket()
		reply.SetReplyChan(ch)
		require.True(t, reply.CanReply())

		require.NoError(t, reply.SendReply())
		select {
		case got := <-ch:
			assert.Same(t, reply, got, "the packet itself is the reply")
			got.Release()
		default:
			t.Fatal("no reply on the channel")
		}
	})

	t.Run("reply channel nobody is reading", func(t *testing.T) {
		p := NewPacket()
		defer p.Release()
		p.SetReplyChan(make(chan *Packet)) // unbuffered
		assert.Error(t, p.SendReply())
	})
}
