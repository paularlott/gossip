package gossip

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Message handler for fire and forget messages
type Handler func(*Node, *Packet) error

// Message handler for request / reply messages, must return reply data
type ReplyHandler func(*Node, *Packet) (interface{}, error)

type msgHandler struct {
	forward      bool
	handler      Handler
	replyHandler ReplyHandler
}

// Dispatch invokes the appropriate handler based on the packet's message type
func (mh *msgHandler) dispatch(c *Cluster, node *Node, packet *Packet) error {
	if packet == nil {
		return fmt.Errorf("packet is nil")
	}

	// Ensure the packet is released and connection closed after processing
	defer packet.Release()

	if packet.conn != nil && mh.replyHandler != nil {
		replyData, err := mh.replyHandler(node, packet)
		if err != nil {
			return err
		}

		if replyData != nil && c != nil {
			replyPacket, err := c.createPacket(c.localNode.ID, replyMsg, 1, replyData)
			if err != nil {
				return err
			}
			defer replyPacket.Release()

			return c.transport.WritePacket(packet.conn, replyPacket)
		}
		return nil
	} else if mh.handler != nil {
		return mh.handler(node, packet)
	}

	return fmt.Errorf("no handler registered")
}

type handlerRegistry struct {
	handlers atomic.Value
	mu       sync.Mutex
}

func newHandlerRegistry() *handlerRegistry {
	registry := &handlerRegistry{}
	registry.handlers.Store(make(map[MessageType]msgHandler))
	return registry
}

func (hr *handlerRegistry) register(t MessageType, h msgHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	currentHandlers := hr.handlers.Load().(map[MessageType]msgHandler)

	newHandlers := make(map[MessageType]msgHandler, len(currentHandlers))
	for k, v := range currentHandlers {
		newHandlers[k] = v
	}

	if _, ok := newHandlers[t]; ok {
		panic(fmt.Sprintf("Handler already registered for message type: %d", t))
	}

	newHandlers[t] = h
	hr.handlers.Store(newHandlers)
}

func (hr *handlerRegistry) unregister(msgType MessageType) bool {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	currentHandlers := hr.handlers.Load().(map[MessageType]msgHandler)

	if _, ok := currentHandlers[msgType]; !ok {
		return false // No handler registered for this message type
	}

	newHandlers := make(map[MessageType]msgHandler, len(currentHandlers)-1)
	for k, v := range currentHandlers {
		if k != msgType {
			newHandlers[k] = v
		}
	}

	hr.handlers.Store(newHandlers)
	return true
}

func (hr *handlerRegistry) registerHandler(msgType MessageType, forward bool, handler Handler) {
	hr.register(msgType, msgHandler{
		forward: forward,
		handler: handler,
	})
}

func (hr *handlerRegistry) registerHandlerWithReply(msgType MessageType, handler ReplyHandler) {
	hr.register(msgType, msgHandler{
		forward:      false,
		replyHandler: handler,
	})
}

func (hr *handlerRegistry) getHandler(msgType MessageType) *msgHandler {
	currentHandlers := hr.handlers.Load().(map[MessageType]msgHandler)
	if handler, ok := currentHandlers[msgType]; ok {
		return &handler
	}
	return nil
}
