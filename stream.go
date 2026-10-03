package gossip

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// Streams carry a reply of any size directly from one node to another. The
// caller sends a request as a packet with OpenStream; the node's
// StreamHandler writes the reply, which the caller reads as an io.Reader.
// Bulk data such as files or state transfers is therefore not bound by the
// packet size limit or a single deadline, and is never gossiped.
//
// On the wire the reply is a sequence of frames, each a 4 byte length
// followed by that many bytes (encrypted with the cluster key on the socket
// transport, as packets are). A zero length frame ends the stream cleanly,
// so a reader can tell a complete reply from a broken connection; an error
// frame carries the handler's error to the caller.

// StreamHandler serves a stream: it reads the request from the packet and
// writes the reply to w. Returning an error ends the stream with that error
// at the caller, even after part of the reply has been written.
type StreamHandler func(sender *Node, packet *Packet, w io.Writer) error

var (
	// ErrStreamsUnsupported is returned by OpenStream when the transport
	// cannot carry streams.
	ErrStreamsUnsupported = errors.New("transport does not support streams")

	errNoStreamHandler = errors.New("no stream handler registered")
)

// streamTransport is implemented by transports that carry streams.
type streamTransport interface {
	// OpenStream sends the request packet to node and returns the reply.
	OpenStream(ctx context.Context, node *Node, packet *Packet) (io.ReadCloser, error)
	// SetStreamHandler installs the function serving incoming streams.
	SetStreamHandler(handler func(packet *Packet, w io.Writer) error)
}

const (
	// streamFrameSize is the most reply data carried by one frame.
	streamFrameSize = 64 * 1024
	// streamFrameMax bounds a frame's length on the wire, leaving room for
	// encryption overhead, so a corrupt length cannot cause a huge allocation.
	streamFrameMax = streamFrameSize + 4096
	// streamErrorFrame marks a frame carrying an error message.
	streamErrorFrame = 0xFFFFFFFF
)

// HandleStreamFunc registers the handler serving streams of msgType.
func (c *Cluster) HandleStreamFunc(msgType MessageType, handler StreamHandler) error {
	if msgType < ReservedMsgsStart {
		return fmt.Errorf("invalid message type")
	}
	c.handlers.register(msgType, msgHandler{streamHandler: handler})
	return nil
}

// OpenStream sends payload to node as a request of msgType and returns the
// reply written by the node's stream handler. The caller must close it.
// Reading returns io.EOF at the end of a complete reply, the handler's error
// if it failed, and io.ErrUnexpectedEOF if the connection broke. Cancelling
// ctx aborts the stream.
func (c *Cluster) OpenStream(ctx context.Context, node *Node, msgType MessageType, payload interface{}) (io.ReadCloser, error) {
	if msgType < ReservedMsgsStart {
		return nil, fmt.Errorf("invalid message type")
	}
	st, ok := c.transport.(streamTransport)
	if !ok {
		return nil, ErrStreamsUnsupported
	}
	packet, err := c.createPacketWithTarget(c.localNode.ID, &node.ID, msgType, 1, payload)
	if err != nil {
		return nil, err
	}
	defer packet.Release()
	return st.OpenStream(ctx, node, packet)
}

// serveStream dispatches an incoming stream to its handler. Like packets,
// streams are only accepted from known nodes and only when addressed here.
func (c *Cluster) serveStream(packet *Packet, w io.Writer) error {
	defer packet.Release()

	if packet.TargetNodeID != nil && *packet.TargetNodeID != c.localNode.ID {
		return fmt.Errorf("stream addressed to another node")
	}
	h := c.handlers.getHandler(packet.MessageType)
	if h == nil || h.streamHandler == nil {
		return errNoStreamHandler
	}
	sender := c.nodes.get(packet.SenderID)
	if sender == nil {
		return fmt.Errorf("stream from unknown node")
	}
	sender.updateLastActivity()
	return h.streamHandler(sender, packet, w)
}

// setStreamFlag marks an encoded packet as a stream request; the flags are
// the first two bytes and are never encrypted.
func setStreamFlag(raw []byte) {
	flags := binary.LittleEndian.Uint16(raw[:2])
	binary.LittleEndian.PutUint16(raw[:2], flags|streamFlag)
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

// frameWriter writes a stream reply as frames.
type frameWriter struct {
	ctx      context.Context // ends the stream when done, e.g. on shutdown
	w        io.Writer
	buf      []byte
	seal     func([]byte) ([]byte, error) // encrypts a frame, nil for none
	deadline func()                       // arms the write deadline, may be nil
	flush    func()                       // pushes data to the peer, may be nil
	err      error
}

func newFrameWriter(ctx context.Context, w io.Writer, seal func([]byte) ([]byte, error), deadline func()) *frameWriter {
	fw := &frameWriter{ctx: ctx, w: w, buf: make([]byte, 0, streamFrameSize), seal: seal, deadline: deadline}
	if f, ok := w.(http.Flusher); ok {
		fw.flush = f.Flush
	}
	return fw
}

func (fw *frameWriter) Write(p []byte) (int, error) {
	if fw.err != nil {
		return 0, fw.err
	}
	n := 0
	for len(p) > 0 {
		take := min(len(p), streamFrameSize-len(fw.buf))
		fw.buf = append(fw.buf, p[:take]...)
		p = p[take:]
		n += take
		if len(fw.buf) == streamFrameSize {
			if err := fw.writeFrame(fw.buf); err != nil {
				return n, err
			}
			fw.buf = fw.buf[:0]
		}
	}
	return n, nil
}

func (fw *frameWriter) writeFrame(data []byte) error {
	if fw.seal != nil {
		sealed, err := fw.seal(data)
		if err != nil {
			fw.err = err
			return err
		}
		data = sealed
	}
	return fw.writeRaw(uint32(len(data)), data)
}

func (fw *frameWriter) writeRaw(length uint32, data []byte) error {
	if err := fw.ctx.Err(); err != nil {
		fw.err = err
		return err
	}
	if fw.deadline != nil {
		fw.deadline()
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], length)
	if _, err := fw.w.Write(hdr[:]); err != nil {
		fw.err = err
		return err
	}
	if len(data) > 0 {
		if _, err := fw.w.Write(data); err != nil {
			fw.err = err
			return err
		}
	}
	if fw.flush != nil {
		fw.flush()
	}
	return nil
}

// finish sends any buffered data and ends the stream, with handlerErr when
// the handler failed.
func (fw *frameWriter) finish(handlerErr error) {
	if fw.err != nil {
		return
	}
	if handlerErr != nil {
		msg := []byte(handlerErr.Error())
		if fw.seal != nil {
			sealed, err := fw.seal(msg)
			if err != nil {
				return
			}
			msg = sealed
		}
		if fw.writeRaw(streamErrorFrame, nil) == nil {
			fw.writeRaw(uint32(len(msg)), msg)
		}
		return
	}
	if len(fw.buf) > 0 {
		if fw.writeFrame(fw.buf) != nil {
			return
		}
	}
	fw.writeRaw(0, nil)
}

// frameReader reads a stream reply written by a frameWriter.
type frameReader struct {
	r        *bufio.Reader
	open     func([]byte) ([]byte, error) // decrypts a frame, nil for none
	deadline func()                       // arms the read deadline, may be nil
	cur      []byte
	err      error

	closeOnce sync.Once
	closer    io.Closer
	stop      func() bool // stops the context watcher, may be nil
}

func newFrameReader(r io.Reader, closer io.Closer, open func([]byte) ([]byte, error), deadline func()) *frameReader {
	return &frameReader{r: bufio.NewReaderSize(r, 32*1024), closer: closer, open: open, deadline: deadline}
}

func (fr *frameReader) Read(p []byte) (int, error) {
	for len(fr.cur) == 0 {
		if fr.err != nil {
			return 0, fr.err
		}
		fr.err = fr.next()
	}
	n := copy(p, fr.cur)
	fr.cur = fr.cur[n:]
	return n, nil
}

// next reads the following frame into cur, returning io.EOF at the end.
func (fr *frameReader) next() error {
	length, err := fr.readLength()
	if err != nil {
		return err
	}
	switch length {
	case 0:
		return io.EOF
	case streamErrorFrame:
		n, err := fr.readLength()
		if err != nil {
			return err
		}
		msg, err := fr.readBody(n)
		if err != nil {
			return err
		}
		return fmt.Errorf("stream failed on peer: %s", msg)
	}
	data, err := fr.readBody(length)
	if err != nil {
		return err
	}
	fr.cur = data
	return nil
}

func (fr *frameReader) readLength() (uint32, error) {
	if fr.deadline != nil {
		fr.deadline()
	}
	var hdr [4]byte
	if _, err := io.ReadFull(fr.r, hdr[:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return binary.LittleEndian.Uint32(hdr[:]), nil
}

func (fr *frameReader) readBody(length uint32) ([]byte, error) {
	if length > streamFrameMax {
		return nil, fmt.Errorf("stream frame too large: %d bytes", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(fr.r, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if fr.open != nil {
		return fr.open(data)
	}
	return data, nil
}

func (fr *frameReader) Close() error {
	var err error
	fr.closeOnce.Do(func() {
		if fr.stop != nil {
			fr.stop()
		}
		err = fr.closer.Close()
	})
	return err
}

// closeWith makes cancelling ctx close the stream.
func (fr *frameReader) closeWith(ctx context.Context) {
	fr.stop = context.AfterFunc(ctx, func() { fr.closer.Close() })
}
