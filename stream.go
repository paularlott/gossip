package gossip

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Flag bit for compressed data (most significant bit)
	flagCompressed uint32 = 1 << 31
	// Maximum size mask (31 bits)
	maxSizeMask uint32 = 0x7FFFFFFF
)

var (
	ErrPacketTooLarge = errors.New("packet exceeds maximum allowed size")
)

// Stream wraps a net.Conn with compression and encryption support.
// It implements the net.Conn interface for compatibility with existing code.
type Stream struct {
	conn   net.Conn
	config *Config

	// Read state
	readBuf []byte
	readPos int
	readMu  sync.Mutex // Protects read operations

	// Write state
	writeMu sync.Mutex // Protects write operations

	// Connection state
	closed    atomic.Bool // Atomic flag to check closed state without locks
	closeOnce sync.Once
}

// NewStream creates a new stream wrapping the given connection
func NewStream(conn net.Conn, config *Config) net.Conn {

	// If there's no compression or encryption, just return the connection
	if config.Compressor == nil && config.Cipher == nil {
		return conn
	}

	// Compression or encryption enabled so we're going to need to wrap the connection
	return &Stream{
		conn:    conn,
		config:  config,
		readBuf: nil,
		readPos: 0,
	}
}

// Read reads data from the connection.
// It implements the io.Reader interface.
func (s *Stream) Read(b []byte) (n int, err error) {
	if s.closed.Load() {
		return 0, net.ErrClosed
	}

	s.readMu.Lock()
	defer s.readMu.Unlock()

	// If we have data in the buffer, return it
	if s.readBuf != nil && s.readPos < len(s.readBuf) {
		n = copy(b, s.readBuf[s.readPos:])
		s.readPos += n

		// If we've read all the data, clear the buffer
		if s.readPos >= len(s.readBuf) {
			s.readBuf = nil
			s.readPos = 0
		}

		return n, nil
	}

	// No data in buffer, read a new frame
	err = s.conn.SetReadDeadline(time.Now().Add(s.config.TCPDeadline))
	if err != nil {
		return 0, err
	}

	// Read the frame header (4 bytes)
	header := make([]byte, 4)
	_, err = io.ReadFull(s.conn, header)
	if err != nil {
		return 0, err
	}

	// Parse the header
	flagsAndSize := binary.BigEndian.Uint32(header)
	isCompressed := (flagsAndSize & flagCompressed) != 0
	dataSize := flagsAndSize & maxSizeMask

	// Check if the packet is too large
	if dataSize > uint32(s.config.StreamMaxPacketSize) {
		return 0, ErrPacketTooLarge
	}

	// Read the frame data
	frameData := make([]byte, dataSize)
	_, err = io.ReadFull(s.conn, frameData)
	if err != nil {
		return 0, err
	}

	// Process the data
	var processedData []byte

	// Decrypt if needed
	if s.config.Cipher != nil {
		frameData, err = s.config.Cipher.Decrypt(s.config.EncryptionKey, frameData)
		if err != nil {
			return 0, fmt.Errorf("failed to decrypt data: %w", err)
		}
	}

	// Decompress if needed
	if isCompressed {
		if s.config.Compressor == nil {
			return 0, errors.New("received compressed data but no decompressor configured")
		}

		processedData, err = s.config.Compressor.Decompress(frameData)
		if err != nil {
			return 0, fmt.Errorf("failed to decompress data: %w", err)
		}
	} else {
		processedData = frameData
	}

	// If the processed data fits in the provided buffer, copy it directly
	if len(processedData) <= len(b) {
		return copy(b, processedData), nil
	}

	// Otherwise, store the data in our buffer and copy what we can
	s.readBuf = processedData
	s.readPos = copy(b, processedData)
	return s.readPos, nil
}

// Write writes data to the connection.
// It implements the io.Writer interface.
func (s *Stream) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	if s.closed.Load() {
		return 0, net.ErrClosed
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// Check if the data is too large
	if len(b) > s.config.StreamMaxPacketSize {
		return 0, ErrPacketTooLarge
	}

	// Process the data
	var processedData []byte
	var isCompressed bool

	// Try compression if enabled and data meets minimum size threshold
	if s.config.Compressor != nil && len(b) >= s.config.CompressMinSize {
		compressed, err := s.config.Compressor.Compress(b)
		if err != nil {
			return 0, fmt.Errorf("failed to compress data: %w", err)
		}

		// Only use compression if it actually reduced the size
		if len(compressed) < len(b) {
			processedData = compressed
			isCompressed = true
		} else {
			processedData = b
			isCompressed = false
		}
	} else {
		processedData = b
		isCompressed = false
	}

	// Encrypt if enabled
	if s.config.Cipher != nil {
		processedData, err = s.config.Cipher.Encrypt(s.config.EncryptionKey, processedData)
		if err != nil {
			return 0, fmt.Errorf("failed to encrypt data: %w", err)
		}
	}

	// Set write deadline
	err = s.conn.SetWriteDeadline(time.Now().Add(s.config.TCPDeadline))
	if err != nil {
		return 0, err
	}

	// Prepare the header
	var flagsAndSize uint32 = uint32(len(processedData)) & maxSizeMask
	if isCompressed {
		flagsAndSize |= flagCompressed
	}

	// Write the header
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, flagsAndSize)

	_, err = s.conn.Write(header)
	if err != nil {
		return 0, err
	}

	// Write the data
	_, err = s.conn.Write(processedData)
	if err != nil {
		return 0, err
	}

	// Return the original data length - this is what the caller expects
	return len(b), nil
}

// Close closes the connection.
// It implements the io.Closer interface.
func (s *Stream) Close() error {
	var err error

	s.closeOnce.Do(func() {
		// Set closed flag atomically
		s.closed.Store(true)

		// Close the underlying connection
		err = s.conn.Close()
	})

	return err
}

// LocalAddr returns the local network address.
func (s *Stream) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (s *Stream) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines.
func (s *Stream) SetDeadline(t time.Time) error {
	return s.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline.
func (s *Stream) SetReadDeadline(t time.Time) error {
	return s.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (s *Stream) SetWriteDeadline(t time.Time) error {
	return s.conn.SetWriteDeadline(t)
}

// Make sure Stream implements net.Conn
var _ net.Conn = (*Stream)(nil)
