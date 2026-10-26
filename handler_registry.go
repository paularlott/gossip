package gossip

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/paularlott/gossip/hlc"
)

// Message handler for fire and forget messages.
//
// The packet is pooled and is only valid until the handler returns: it is
// released (and may be reused for another message) immediately afterwards.
// Unmarshal what you need inside the handler; do not retain the packet or
// its Payload() slice, or use them from a goroutine started by the handler.
type Handler func(*Node, *Packet) error

// Message handler for request / reply messages, must return reply data.
// The same packet lifetime rule as Handler applies.
type ReplyHandler func(*Node, *Packet) (interface{}, error)

// Replier answers one request of a DeferredReplyHandler. Reply may be called once, from any goroutine, at any time;
// later calls do nothing. Every request must be answered: call Reply on every path, with a nil payload (or an error)
// when there is nothing to send, because the Replier holds the request packet until Reply is called. A Replier that is
// never used keeps the packet until the garbage collector reclaims it.
//
// Reply(payload, nil) with a non-nil payload sends payload to the requester, marshalled with the request's codec, as
// the return value of a ReplyHandler would be. Reply(_, err) with an error, or with a nil payload, sends nothing: the
// requester waits until its own timeout, as it does when a ReplyHandler returns an error or a nil reply. A reply to a
// requester that has gone (it timed out, its connection closed) is dropped. A request that is not waiting for a reply
// (a message sent without SendToWithResponse) is never answered; Reply only releases it.
type Replier interface {
	Reply(payload interface{}, err error)
}

// DeferredReplyHandler handles a request and answers it through r, now or later from any goroutine. It runs on one of
// the cluster's incoming workers like any handler and should return quickly: the worker is free again when it returns,
// however long the reply takes.
//
// Decode the request (packet.Unmarshal) before returning. The Replier holds a reference to the packet so it stays
// valid until Reply is called, but Reply turns the packet into the reply, so a read of the packet by the handler's
// goroutine must not run concurrently with it. The same applies to the packet's Payload() slice.
type DeferredReplyHandler func(node *Node, packet *Packet, r Replier)

type msgHandler struct {
	handler         Handler
	replyHandler    ReplyHandler
	deferredHandler DeferredReplyHandler
	streamHandler   StreamHandler
}

// deferredReplier is the Replier dispatch hands to a DeferredReplyHandler. It owns one reference to the packet and
// gives it up exactly once: to the reply box when the reply is sent (the transport releases it after writing it), by
// Release on every other path.
type deferredReplier struct {
	cluster *Cluster
	packet  *Packet
	msgType MessageType // for the log, taken before the packet becomes the reply
	used    atomic.Bool
}

func (r *deferredReplier) Reply(payload interface{}, err error) {
	if !r.used.CompareAndSwap(false, true) {
		return
	}
	c, packet := r.cluster, r.packet
	r.packet = nil

	if err != nil {
		if c != nil {
			c.logger.Warn("error dispatching packet", "error", err, "message_type", r.msgType)
		}
		packet.Release()
		return
	}

	// Only a packet with a waiting requester is turned into the reply (see dispatch).
	if payload == nil || c == nil || !packet.awaitingReply() {
		packet.Release()
		return
	}

	data, err := packet.codec.Marshal(payload)
	if err != nil {
		c.logger.Warn("error dispatching packet", "error", err, "message_type", r.msgType)
		packet.Release()
		return
	}
	packet.MessageType = replyMsg
	packet.SenderID = c.localNode.ID
	packet.MessageID = MessageID(hlc.Now())
	packet.TTL = 1
	packet.payload = data

	// On success the reply box owns the reference; it was refused when the requester stopped waiting.
	if err := packet.SendReply(); err != nil {
		c.logger.Debug("reply not delivered", "error", err, "message_type", r.msgType)
		packet.Release()
	}
}

// Dispatch invokes the appropriate handler based on the packet's message type.
//
// dispatch takes ownership of the caller's reference: the packet is released
// before dispatch returns, on every path including errors. The caller must not
// read or write the packet afterwards — capture anything it needs (e.g. the
// message type for logging) before calling.
func (mh *msgHandler) dispatch(c *Cluster, node *Node, packet *Packet) error {
	if packet == nil {
		return fmt.Errorf("packet is nil")
	}

	// Ensure the packet is released after processing
	defer packet.Release()

	if mh.replyHandler != nil {
		replyData, err := mh.replyHandler(node, packet)
		if err != nil {
			return err
		}

		// Only a packet with a waiting requester is turned into the reply. A
		// fire-and-forget packet may at this moment be shared with the
		// broadcast queue for forwarding, so it must never be mutated.
		if replyData != nil && c != nil && packet.awaitingReply() {
			// Update the packet with the reply data
			packet.AddRef()
			packet.MessageType = replyMsg
			packet.SenderID = c.localNode.ID
			packet.MessageID = MessageID(hlc.Now())
			packet.TTL = 1
			packet.payload, err = packet.codec.Marshal(replyData)
			if err != nil {
				packet.Release()
				return err
			}

			if err := packet.SendReply(); err != nil {
				packet.Release()
				return err
			}
			return nil
		}
		return nil
	} else if mh.deferredHandler != nil {
		// The replier takes its own reference; dispatch releases its own as for every handler.
		mh.deferredHandler(node, packet, &deferredReplier{cluster: c, packet: packet.AddRef(), msgType: packet.MessageType})
		return nil
	} else if mh.handler != nil {
		return mh.handler(node, packet)
	}

	return fmt.Errorf("no handler registered")
}

type handlerRegistry struct {
	handlers atomic.Pointer[map[MessageType]msgHandler]
	mu       sync.Mutex
}

func newHandlerRegistry() *handlerRegistry {
	registry := &handlerRegistry{}
	initialMap := make(map[MessageType]msgHandler)
	registry.handlers.Store(&initialMap)
	return registry
}

func (hr *handlerRegistry) register(t MessageType, h msgHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	currentHandlers := hr.handlers.Load()
	if currentHandlers == nil {
		currentHandlers = &map[MessageType]msgHandler{}
	}

	// Check for existing handler
	if _, ok := (*currentHandlers)[t]; ok {
		panic(fmt.Sprintf("Handler already registered for message type: %d", t))
	}

	// Create new map with extra capacity for future additions
	newHandlers := make(map[MessageType]msgHandler, len(*currentHandlers)+4)
	for k, v := range *currentHandlers {
		newHandlers[k] = v
	}
	newHandlers[t] = h

	hr.handlers.Store(&newHandlers)
}

func (hr *handlerRegistry) unregister(msgType MessageType) bool {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	currentHandlers := hr.handlers.Load()
	if currentHandlers == nil {
		return false
	}

	if _, ok := (*currentHandlers)[msgType]; !ok {
		return false // No handler registered for this message type
	}

	// Create new map without the removed handler
	newHandlers := make(map[MessageType]msgHandler, len(*currentHandlers)-1)
	for k, v := range *currentHandlers {
		if k != msgType {
			newHandlers[k] = v
		}
	}

	hr.handlers.Store(&newHandlers)
	return true
}

func (hr *handlerRegistry) registerHandler(msgType MessageType, handler Handler) {
	hr.register(msgType, msgHandler{
		handler: handler,
	})
}

func (hr *handlerRegistry) registerHandlerWithReply(msgType MessageType, handler ReplyHandler) {
	hr.register(msgType, msgHandler{
		replyHandler: handler,
	})
}

func (hr *handlerRegistry) registerHandlerWithDeferredReply(msgType MessageType, handler DeferredReplyHandler) {
	hr.register(msgType, msgHandler{
		deferredHandler: handler,
	})
}

func (hr *handlerRegistry) getHandler(msgType MessageType) *msgHandler {
	currentHandlers := hr.handlers.Load()
	if currentHandlers == nil {
		return nil
	}

	if handler, ok := (*currentHandlers)[msgType]; ok {
		return &handler
	}
	return nil
}
