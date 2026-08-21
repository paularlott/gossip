// Package cuttable provides a transport wrapper for tests that need to sever a
// node from its cluster without the node announcing anything.
//
// This matters because Cluster.Stop() calls Leave() internally, which broadcasts
// a departure. A test that merely calls Stop() is exercising a *graceful* exit,
// not a crash. To simulate a genuine crash — process killed, cable pulled — the
// node must go silent while still believing it is running, which is what Cut()
// provides: every send fails and nothing further is delivered inbound, so any
// subsequent Leave() broadcast is silently dropped and peers observe only
// silence, exactly as they would for a crashed process.
package cuttable

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/paularlott/gossip"
)

// ErrTransportCut is returned by sends attempted after Cut.
var ErrTransportCut = errors.New("transport cut (simulated crash)")

// Transport wraps a real transport and can be severed at will.
type Transport struct {
	inner gossip.Transport

	cutOff atomic.Bool

	mu     sync.Mutex
	closed bool
	ch     chan *gossip.Packet
}

// New wraps inner in a cuttable transport.
func New(inner gossip.Transport) *Transport {
	return &Transport{inner: inner}
}

func (t *Transport) Name() string { return "cuttable" }

func (t *Transport) Start(ctx context.Context, wg *sync.WaitGroup) error {
	if err := t.inner.Start(ctx, wg); err != nil {
		return err
	}

	t.mu.Lock()
	t.ch = make(chan *gossip.Packet, 256)
	t.mu.Unlock()

	// Relay inbound packets until cut.
	wg.Add(1)
	go func() {
		defer wg.Done()
		src := t.inner.PacketChannel()
		for {
			select {
			case <-ctx.Done():
				return
			case p, ok := <-src:
				if !ok {
					return
				}
				if t.cutOff.Load() {
					p.Release()
					continue
				}
				t.mu.Lock()
				closed := t.closed
				dst := t.ch
				t.mu.Unlock()
				if closed {
					p.Release()
					return
				}
				select {
				case dst <- p:
				case <-ctx.Done():
					p.Release()
					return
				}
			}
		}
	}()

	return nil
}

func (t *Transport) PacketChannel() chan *gossip.Packet {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ch == nil {
		t.ch = make(chan *gossip.Packet, 256)
	}
	return t.ch
}

func (t *Transport) Send(tt gossip.TransportType, node *gossip.Node, packet *gossip.Packet) error {
	if t.cutOff.Load() {
		return ErrTransportCut
	}
	return t.inner.Send(tt, node, packet)
}

func (t *Transport) SendWithReply(node *gossip.Node, packet *gossip.Packet) (*gossip.Packet, error) {
	if t.cutOff.Load() {
		return nil, ErrTransportCut
	}
	return t.inner.SendWithReply(node, packet)
}

// Cut severs the node: outbound sends fail and inbound packets are discarded.
func (t *Transport) Cut() {
	t.cutOff.Store(true)
}

// Uncut restores the node. Packets flow again; the node was alive and
// believing it was running throughout, exactly like a healed partition.
func (t *Transport) Uncut() {
	t.cutOff.Store(false)
}
