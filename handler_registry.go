package gossip

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
)

type Handler func(*Node, *Packet) error
type ReplyHandler func(*Node, *Packet) (MessageType, interface{}, error)

type msgHandler struct {
	forward      bool
	handler      Handler
	replyHandler ReplyHandler
}

// Dispatch invokes the appropriate handler based on the packet's message type
func (mh *msgHandler) dispatch(conn net.Conn, localNode *Node, transport *transport, node *Node, packet *Packet) error {
	if conn != nil && mh.replyHandler != nil {
		replyType, replyData, err := mh.replyHandler(node, packet)
		if err != nil {
			return err
		}

		if replyType != nilMsg && transport != nil {
			return transport.writeMessage(conn, localNode.ID, replyType, replyData)
		}
		return nil
	} else if mh.handler != nil {
		return mh.handler(node, packet)
	}

	return fmt.Errorf("no handler registered")
}

type handlerRegistry struct {
	registerMutex sync.Mutex
	handlers      atomic.Value
}

func newHandlerRegistry() *handlerRegistry {
	registry := &handlerRegistry{
		registerMutex: sync.Mutex{},
	}
	registry.handlers.Store(make(map[MessageType]msgHandler))
	return registry
}

func (hr *handlerRegistry) register(t MessageType, h msgHandler) {
	hr.registerMutex.Lock()
	defer hr.registerMutex.Unlock()

	currentHandlers := hr.handlers.Load().(map[MessageType]msgHandler)

	newHandlers := make(map[MessageType]msgHandler, len(currentHandlers))
	for k, v := range currentHandlers {
		newHandlers[k] = v
	}

	if _, ok := newHandlers[t]; ok {
		log.Fatal().Msgf("Handler already registered for message type: %d", t)
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

func (hr *handlerRegistry) getHandler(msgType MessageType) *msgHandler {
	currentHandlers := hr.handlers.Load().(map[MessageType]msgHandler)
	if handler, ok := currentHandlers[msgType]; ok {
		return &handler
	}
	return nil
}
