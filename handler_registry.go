package gossip

import (
	"fmt"
	"net"
	"sync/atomic"
)

type Handler func(*Node, *Packet) error
type ReplyHandler func(*Node, *Packet) (MessageType, interface{}, error)
type StreamHandler func(*Node, *Packet, net.Conn)

type msgHandler struct {
	forward       bool
	handler       Handler
	replyHandler  ReplyHandler
	streamHandler StreamHandler
}

// Dispatch invokes the appropriate handler based on the packet's message type
func (mh *msgHandler) dispatch(c *Cluster, node *Node, packet *Packet) error {

	if packet.conn != nil && mh.streamHandler != nil {
		// Start stream handlers in their own go routine as they could run for a while
		go func() {
			defer packet.Release()
			mh.streamHandler(node, packet, packet.conn)
		}()
		return nil
	}

	// Ensure the packet is released and connection closed after processing
	defer packet.Release()

	if packet.conn != nil && mh.replyHandler != nil {
		replyType, replyData, err := mh.replyHandler(node, packet)
		if err != nil {
			return err
		}

		if replyType != nilMsg && c != nil {
			replyPacket, err := c.createPacket(c.localNode.ID, replyType, 1, replyData)
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
}

func newHandlerRegistry() *handlerRegistry {
	registry := &handlerRegistry{}
	registry.handlers.Store(make(map[MessageType]msgHandler))
	return registry
}

func (hr *handlerRegistry) register(t MessageType, h msgHandler) {
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

func (hr *handlerRegistry) registerStreamHandler(msgType MessageType, handler StreamHandler) {
	hr.register(msgType, msgHandler{
		forward:       false,
		streamHandler: handler,
	})
}

func (hr *handlerRegistry) getHandler(msgType MessageType) *msgHandler {
	currentHandlers := hr.handlers.Load().(map[MessageType]msgHandler)
	if handler, ok := currentHandlers[msgType]; ok {
		return &handler
	}
	return nil
}
