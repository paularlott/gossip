package gossip

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/gossip/compression"
)

// go test -bench=BenchmarkStream -benchmem

// mockEncryptor implements a simple encryption algorithm for testing
type mockEncryptor struct{}

func (m *mockEncryptor) Encrypt(key, data []byte) ([]byte, error) {
	// Simple "encryption": add prefix "E:" and suffix ":E"
	return append(append([]byte("E:"), data...), ':', 'E'), nil
}

func (m *mockEncryptor) Decrypt(key, data []byte) ([]byte, error) {
	if len(data) <= 2 || !bytes.HasPrefix(data, []byte("E:")) || !bytes.HasSuffix(data, []byte(":E")) {
		return data, nil // Not encrypted data
	}
	// Remove the prefix and suffix
	return data[2 : len(data)-2], nil
}

// streamMockConn implements net.Conn for testing
type streamMockConn struct {
	readBuf       *bytes.Buffer
	writeBuf      *bytes.Buffer
	closed        bool
	mu            sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func newstreamMockConn() *streamMockConn {
	return &streamMockConn{
		readBuf:  new(bytes.Buffer),
		writeBuf: new(bytes.Buffer),
	}
}

func (c *streamMockConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	if !c.readDeadline.IsZero() && time.Now().After(c.readDeadline) {
		return 0, os.ErrDeadlineExceeded
	}
	return c.readBuf.Read(b)
}

func (c *streamMockConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	if !c.writeDeadline.IsZero() && time.Now().After(c.writeDeadline) {
		return 0, os.ErrDeadlineExceeded
	}
	return c.writeBuf.Write(b)
}

func (c *streamMockConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *streamMockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000}
}
func (c *streamMockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}
}

func (c *streamMockConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

func (c *streamMockConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = t
	return nil
}

func (c *streamMockConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = t
	return nil
}

// simulateReadWrite copies data from write buffer to read buffer, simulating a network round trip
func (c *streamMockConn) simulateReadWrite() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readBuf.Write(c.writeBuf.Bytes())
	c.writeBuf.Reset()
}

// getWrittenData returns what was written and resets the write buffer
func (c *streamMockConn) getWrittenData() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	data := c.writeBuf.Bytes()
	c.writeBuf.Reset()
	return data
}

// prepareDataToRead adds data to the read buffer
func (c *streamMockConn) prepareDataToRead(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readBuf.Write(data)
}

// Basic test configuration
func getTestConfig() *Config {
	return &Config{
		TCPDeadline:         5 * time.Second,
		CompressMinSize:     20, // Set small for testing
		StreamMaxPacketSize: 1024 * 1024,
	}
}

func TestStream_BasicReadWrite(t *testing.T) {
	streamMockConn := newstreamMockConn()
	config := getTestConfig()
	stream := NewStream(streamMockConn, config)

	// Write data to stream
	data := []byte("hello, world")
	n, err := stream.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d bytes, expected %d", n, len(data))
	}

	// Verify written data follows our protocol format
	written := streamMockConn.getWrittenData()
	if len(written) < 4 {
		t.Fatalf("Written data too short: %d bytes", len(written))
	}

	// Verify header
	headerSize := binary.BigEndian.Uint32(written[:4])
	if (headerSize & flagCompressed) != 0 {
		t.Error("Compression flag should not be set for small data")
	}

	// Now simulate read by copying to read buffer
	streamMockConn.prepareDataToRead(written)

	// Read from stream
	readBuf := make([]byte, 100)
	n, err = stream.Read(readBuf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if n != len(data) {
		t.Errorf("Read returned %d bytes, expected %d", n, len(data))
	}
	if !bytes.Equal(readBuf[:n], data) {
		t.Errorf("Read data %q, expected %q", readBuf[:n], data)
	}
}

func TestStream_WithCompression(t *testing.T) {
	streamMockConn := newstreamMockConn()
	config := getTestConfig()
	config.Compressor = compression.NewSnappyCompressor() // We have to use a real compressor or the compressed data is larger than the original and so no compression is used
	config.CompressMinSize = 2                            // Set small for testing
	stream := NewStream(streamMockConn, config)

	// Use data large enough to trigger compression
	data := []byte(strings.Repeat("please compress this data ", 5)) // Need enough data to make compression work
	n, err := stream.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d bytes, expected %d", n, len(data))
	}

	// Get written data and check compression flag
	written := streamMockConn.getWrittenData()
	if len(written) < 4 {
		t.Fatalf("Written data too short: %d bytes", len(written))
	}

	// Verify header
	headerVal := binary.BigEndian.Uint32(written[:4])
	if (headerVal & flagCompressed) == 0 {
		t.Error("Compression flag should be set")
	}

	// Simulate read
	streamMockConn.prepareDataToRead(written)

	// Read back
	readBuf := make([]byte, 1000)
	n, err = stream.Read(readBuf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if n != len(data) {
		t.Errorf("Read returned %d bytes, expected %d", n, len(data))
	}
	if !bytes.Equal(readBuf[:n], data) {
		t.Errorf("Decompressed data doesn't match original")
	}
}

func TestStream_WithEncryption(t *testing.T) {
	streamMockConn := newstreamMockConn()
	config := getTestConfig()
	config.Cipher = &mockEncryptor{}
	config.EncryptionKey = []byte("12345678901234567890123456789012")
	stream := NewStream(streamMockConn, config)

	// Write data
	data := []byte("encrypt this message")
	_, err := stream.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	// Read the written data
	written := streamMockConn.getWrittenData()

	// Prepare for read
	streamMockConn.prepareDataToRead(written)

	// Read back
	readBuf := make([]byte, 100)
	n, err := stream.Read(readBuf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if !bytes.Equal(readBuf[:n], data) {
		t.Errorf("Decrypted data doesn't match original")
	}
}

func TestStream_WithCompressionAndEncryption(t *testing.T) {
	streamMockConn := newstreamMockConn()
	config := getTestConfig()
	config.Compressor = compression.NewSnappyCompressor()
	config.Cipher = &mockEncryptor{}
	config.EncryptionKey = []byte("12345678901234567890123456789012")
	stream := NewStream(streamMockConn, config)

	// Use data large enough to trigger compression
	data := []byte(strings.Repeat("compress and encrypt me ", 5))
	_, err := stream.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	// Prepare for read
	written := streamMockConn.getWrittenData()
	streamMockConn.prepareDataToRead(written)

	// Read back
	readBuf := make([]byte, 1000)
	n, err := stream.Read(readBuf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if !bytes.Equal(readBuf[:n], data) {
		t.Errorf("Processed data doesn't match original")
	}
}

func TestStream_LargeData(t *testing.T) {
	streamMockConn := newstreamMockConn()
	config := getTestConfig()
	stream := NewStream(streamMockConn, config)

	// Generate large data (100KB)
	data := make([]byte, 100*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write data
	_, err := stream.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	// Prepare for read
	written := streamMockConn.getWrittenData()
	streamMockConn.prepareDataToRead(written)

	// Read back in small chunks to test buffering
	readBuf := make([]byte, 16*1024) // Use smaller buffer than data
	totalRead := 0
	var readData []byte

	for totalRead < len(data) {
		n, err := stream.Read(readBuf)
		if err != nil && err != io.EOF {
			t.Fatalf("Read error: %v", err)
		}
		readData = append(readData, readBuf[:n]...)
		totalRead += n
		if err == io.EOF {
			break
		}
	}

	if !bytes.Equal(readData, data) {
		t.Error("Large data read doesn't match original")
	}
}

func TestStream_Close(t *testing.T) {
	streamMockConn := newstreamMockConn()
	stream := NewStream(streamMockConn, getTestConfig())

	// Close the stream
	err := stream.Close()
	if err != nil {
		t.Errorf("Close error: %v", err)
	}

	// Verify underlying connection is closed
	if !streamMockConn.closed {
		t.Error("Underlying connection not closed")
	}

	// Try to write after close
	_, err = stream.Write([]byte("test"))
	if err != net.ErrClosed {
		t.Errorf("Expected ErrClosed on write after close, got: %v", err)
	}

	// Try to read after close
	_, err = stream.Read(make([]byte, 10))
	if err != net.ErrClosed {
		t.Errorf("Expected ErrClosed on read after close, got: %v", err)
	}

	// Call Close again - should be no-op
	err = stream.Close()
	if err != nil {
		t.Errorf("Second Close error: %v", err)
	}
}

func TestStream_NetworkAddresses(t *testing.T) {
	streamMockConn := newstreamMockConn()
	stream := NewStream(streamMockConn, getTestConfig())

	local := stream.LocalAddr()
	if local.String() != "127.0.0.1:9000" {
		t.Errorf("Unexpected LocalAddr: %s", local.String())
	}

	remote := stream.RemoteAddr()
	if remote.String() != "127.0.0.1:9001" {
		t.Errorf("Unexpected RemoteAddr: %s", remote.String())
	}
}

func TestStream_Deadlines(t *testing.T) {
	streamMockConn := newstreamMockConn()
	stream := NewStream(streamMockConn, getTestConfig())

	// Set deadlines
	deadline := time.Now().Add(time.Second)
	err := stream.SetDeadline(deadline)
	if err != nil {
		t.Errorf("SetDeadline error: %v", err)
	}

	err = stream.SetReadDeadline(deadline)
	if err != nil {
		t.Errorf("SetReadDeadline error: %v", err)
	}

	err = stream.SetWriteDeadline(deadline)
	if err != nil {
		t.Errorf("SetWriteDeadline error: %v", err)
	}

	// Verify deadlines were set on underlying connection
	if !streamMockConn.readDeadline.Equal(deadline) {
		t.Error("Read deadline not propagated to underlying connection")
	}
	if !streamMockConn.writeDeadline.Equal(deadline) {
		t.Error("Write deadline not propagated to underlying connection")
	}
}

func TestStream_BufferedReads(t *testing.T) {
	streamMockConn := newstreamMockConn()
	stream := NewStream(streamMockConn, getTestConfig())

	// Write data to stream
	data := []byte("this is a test of buffered reads")
	_, err := stream.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	// Prepare for read
	written := streamMockConn.getWrittenData()
	streamMockConn.prepareDataToRead(written)

	// Read a small part first
	smallBuf := make([]byte, 10)
	n, err := stream.Read(smallBuf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if n != 10 {
		t.Errorf("Expected to read 10 bytes, got %d", n)
	}
	if !bytes.Equal(smallBuf, data[:10]) {
		t.Errorf("First part doesn't match: got %q, expected %q", smallBuf, data[:10])
	}

	// Read the rest
	restBuf := make([]byte, 100)
	n, err = stream.Read(restBuf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if n != len(data)-10 {
		t.Errorf("Expected to read %d bytes, got %d", len(data)-10, n)
	}
	if !bytes.Equal(restBuf[:n], data[10:]) {
		t.Errorf("Second part doesn't match")
	}
}

func TestStream_ZeroLengthWrite(t *testing.T) {
	streamMockConn := newstreamMockConn()
	stream := NewStream(streamMockConn, getTestConfig())

	// Write empty data
	n, err := stream.Write([]byte{})
	if err != nil {
		t.Errorf("Zero-length write error: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes written, got %d", n)
	}

	// Verify nothing was actually written to underlying conn
	written := streamMockConn.getWrittenData()
	if len(written) > 0 {
		t.Errorf("Expected no data written, got %d bytes", len(written))
	}
}

// BenchmarkStream_Write_Small tests writing small payloads
func BenchmarkStream_Write_Small(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	stream := NewStream(mockConn, config)

	data := []byte("small payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stream.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		mockConn.writeBuf.Reset() // Clear buffer between writes
	}
	b.SetBytes(int64(len(data)))
}

// BenchmarkStream_Write_Medium tests writing medium-sized payloads
func BenchmarkStream_Write_Medium(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	stream := NewStream(mockConn, config)

	// Create a 4KB payload
	data := make([]byte, 4*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stream.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		mockConn.writeBuf.Reset() // Clear buffer between writes
	}
	b.SetBytes(int64(len(data)))
}

// BenchmarkStream_Write_Large tests writing large payloads
func BenchmarkStream_Write_Large(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	stream := NewStream(mockConn, config)

	// Create a 1MB payload
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stream.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		mockConn.writeBuf.Reset() // Clear buffer between writes
	}
	b.SetBytes(int64(len(data)))
}

// BenchmarkStream_Read_Small tests reading small payloads
func BenchmarkStream_Read_Small(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	stream := NewStream(mockConn, config)

	data := []byte("small payload")

	// Prepare the framed data
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	frameData := append(header, data...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mockConn.prepareDataToRead(frameData)
		buf := make([]byte, len(data))
		_, err := stream.Read(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(data)))
}

// BenchmarkStream_Read_Medium tests reading medium-sized payloads
func BenchmarkStream_Read_Medium(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	stream := NewStream(mockConn, config)

	// Create a 4KB payload
	data := make([]byte, 4*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Prepare the framed data
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	frameData := append(header, data...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mockConn.prepareDataToRead(frameData)
		buf := make([]byte, len(data))
		_, err := stream.Read(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(data)))
}

// BenchmarkStream_Read_Large tests reading large payloads
func BenchmarkStream_Read_Large(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	stream := NewStream(mockConn, config)

	// Create a 1MB payload
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Prepare the framed data
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	frameData := append(header, data...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mockConn.prepareDataToRead(frameData)
		buf := make([]byte, len(data))
		_, err := stream.Read(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(data)))
}

// BenchmarkStream_Compression_Write tests writing with compression
func BenchmarkStream_Compression_Write(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	config.Compressor = compression.NewSnappyCompressor()
	config.CompressMinSize = 10 // Set small to ensure compression is used
	stream := NewStream(mockConn, config)

	// Create compressible data (repeating pattern)
	data := bytes.Repeat([]byte("abcdefghijklmnopqrstuvwxyz"), 1024) // ~26KB of repeating data

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stream.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		mockConn.writeBuf.Reset()
	}
	b.SetBytes(int64(len(data)))
}

// BenchmarkStream_Encryption_Write tests writing with encryption
func BenchmarkStream_Encryption_Write(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	config.Cipher = &mockEncryptor{}
	config.EncryptionKey = []byte("test-encryption-key-32bytes-long!!")
	stream := NewStream(mockConn, config)

	// Create 64KB of data
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stream.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		mockConn.writeBuf.Reset()
	}
	b.SetBytes(int64(len(data)))
}

// BenchmarkStream_CompressionAndEncryption_Write tests writing with both compression and encryption
func BenchmarkStream_CompressionAndEncryption_Write(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	config.Compressor = compression.NewSnappyCompressor()
	config.CompressMinSize = 10
	config.Cipher = &mockEncryptor{}
	config.EncryptionKey = []byte("test-encryption-key-32bytes-long!!")
	stream := NewStream(mockConn, config)

	// Create compressible data
	data := bytes.Repeat([]byte("abcdefghijklmnopqrstuvwxyz"), 1024) // ~26KB of repeating data

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stream.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		mockConn.writeBuf.Reset()
	}
	b.SetBytes(int64(len(data)))
}

// BenchmarkStream_RoundTrip tests a complete write+read cycle
func BenchmarkStream_RoundTrip(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	stream := NewStream(mockConn, config)

	// Create 4KB of data
	data := make([]byte, 4*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	readBuf := make([]byte, len(data))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Write data
		_, err := stream.Write(data)
		if err != nil {
			b.Fatal(err)
		}

		// Simulate network transfer
		mockConn.simulateReadWrite()

		// Read data back
		_, err = stream.Read(readBuf)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(data) * 2)) // Count both read and write
}

// BenchmarkStream_RoundTrip_WithCompression tests a complete write+read cycle with compression
func BenchmarkStream_RoundTrip_WithCompression(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	config.Compressor = compression.NewSnappyCompressor()
	config.CompressMinSize = 10
	stream := NewStream(mockConn, config)

	// Create compressible data
	data := bytes.Repeat([]byte("abcdefghijklmnopqrstuvwxyz"), 160) // ~4KB of repeating data
	readBuf := make([]byte, len(data))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Write data
		_, err := stream.Write(data)
		if err != nil {
			b.Fatal(err)
		}

		// Simulate network transfer
		mockConn.simulateReadWrite()

		// Read data back
		_, err = stream.Read(readBuf)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(data) * 2))
}

// BenchmarkStream_RoundTrip_WithEncryption tests a complete write+read cycle with encryption
func BenchmarkStream_RoundTrip_WithEncryption(b *testing.B) {
	mockConn := newstreamMockConn()
	config := getTestConfig()
	config.Cipher = &mockEncryptor{}
	config.EncryptionKey = []byte("test-encryption-key-32bytes-long!!")
	stream := NewStream(mockConn, config)

	// Create 4KB of data
	data := make([]byte, 4*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	readBuf := make([]byte, len(data))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Write data
		_, err := stream.Write(data)
		if err != nil {
			b.Fatal(err)
		}

		// Simulate network transfer
		mockConn.simulateReadWrite()

		// Read data back
		_, err = stream.Read(readBuf)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(data) * 2))
}
