package gossip

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/paularlott/gossip/hlc"
)

// Message handler for fire and forget messages
type Handler func(*Node, *Packet) error

// Message handler for request / reply messages, must return reply data
type ReplyHandler func(*Node, *Packet) (interface{}, error)

type msgHandler struct {
	handler      Handler
	replyHandler ReplyHandler
}

// Dispatch invokes the appropriate handler based on the packet's message type
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

		if replyData != nil && c != nil && packet.replyChan != nil {
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

			return packet.SendReply()
		}
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
