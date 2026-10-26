package gossip

import (
	"testing"

	"github.com/google/uuid"
	"github.com/paularlott/gossip/codec"
)

func BenchmarkHandlerRegistryRegister(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}
}

func BenchmarkHandlerRegistryGetHandler(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	// Pre-populate with handlers
	for i := 0; i < 1000; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgType := MessageType(int(UserMsg) + (i % 1000))
		hr.getHandler(msgType)
	}
}

func BenchmarkHandlerRegistryUnregister(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	// Pre-populate with handlers
	for i := 0; i < b.N; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.unregister(msgType)
	}
}

func BenchmarkMsgHandlerDispatch(b *testing.B) {
	mh := &msgHandler{
		handler: func(*Node, *Packet) error { return nil },
	}

	node := &Node{ID: NodeID(uuid.New())}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packet := NewPacket()
		mh.dispatch(nil, node, packet)
	}
}

func BenchmarkMsgHandlerDispatchReply(b *testing.B) {
	mh := &msgHandler{
		replyHandler: func(*Node, *Packet) (interface{}, error) {
			return "reply", nil
		},
	}

	config := DefaultConfig()
	config.MsgCodec = codec.NewJSONCodec()
	config.Transport = &mockTransport{}

	cluster, err := NewCluster(config)
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}

	node := &Node{ID: NodeID(uuid.New())}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packet := NewPacket()
		packet.conn = &mockConn{}
		packet.codec = config.MsgCodec
		mh.dispatch(cluster, node, packet)
	}
}

func BenchmarkHandlerRegistryConcurrentRead(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	// Pre-populate with handlers
	for i := 0; i < 1000; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msgType := MessageType(int(UserMsg) + (b.N % 1000))
			hr.getHandler(msgType)
		}
	})
}

func BenchmarkHandlerRegistryConcurrentWrite(b *testing.B) {
	handler := func(*Node, *Packet) error { return nil }

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		hr := newHandlerRegistry()
		i := 0
		for pb.Next() {
			msgType := MessageType(int(UserMsg) + i)
			hr.registerHandler(msgType, handler)
			i++
		}
	})
}

func BenchmarkHandlerRegistryMixed(b *testing.B) {
	hr := newHandlerRegistry()
	handler := func(*Node, *Packet) error { return nil }

	// Pre-populate
	for i := 0; i < 100; i++ {
		msgType := MessageType(int(UserMsg) + i)
		hr.registerHandler(msgType, handler)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 3 {
		case 0:
			msgType := MessageType(int(UserMsg) + 1000 + i)
			hr.registerHandler(msgType, handler)
		case 1:
			msgType := MessageType(int(UserMsg) + (i % 100))
			hr.getHandler(msgType)
		case 2:
			msgType := MessageType(int(UserMsg) + (i % 100))
			hr.unregister(msgType)
		}
	}
}
