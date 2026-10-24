package gossip

import (
	"sync"
	"testing"
)

// A waiter that gives up (timeout, shutdown) closes its reply box while the dispatcher may be sending the reply. That
// is not a data race, and the reply is released exactly once whichever side comes first: by the sender when the box
// refused it, by close when it was already in the box.
func TestReplyBoxCloseRacesSend(t *testing.T) {
	for range 2000 {
		b := newReplyBox()
		p := NewPacket()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if b.send(p) != nil {
				p.Release() // refused: the sender keeps its reference and drops it, as dispatch does
			}
		}()
		go func() {
			defer wg.Done()
			b.close()
		}()
		wg.Wait()
		if n := p.refCount.Load(); n != 0 {
			t.Fatalf("reply holds %d references after the race, want 0", n)
		}
	}
}

// A reply sent before the waiter reads it is delivered; once the box is closed it refuses replies.
func TestReplyBoxDeliversThenRefuses(t *testing.T) {
	b := newReplyBox()
	p := NewPacket()
	p.setReply(b)
	if err := p.SendReply(); err != nil {
		t.Fatal(err)
	}
	if got := <-b.C(); got != p {
		t.Fatal("wrong reply")
	}
	p.Release()
	b.close()
	q := NewPacket()
	q.setReply(b)
	if err := q.SendReply(); err == nil {
		t.Fatal("a closed box took a reply")
	}
	q.Release()
}
