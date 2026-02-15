package chunkwriter

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/riegeli-go/internal/block"
	"github.com/google/riegeli-go/internal/chunk"
)

func TestWriteFileSignature(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}
	if w.Pos() != 64 {
		t.Errorf("Pos = %d, want 64", w.Pos())
	}

	data := buf.Bytes()
	if len(data) != 64 {
		t.Fatalf("wrote %d bytes, want 64", len(data))
	}

	// Verify block header (first 24 bytes).
	bh := block.Parse(data[:24])
	if !bh.Valid() {
		t.Error("block header hash invalid")
	}
	if bh.PreviousChunk() != 0 {
		t.Errorf("PreviousChunk = %d, want 0", bh.PreviousChunk())
	}
	if bh.NextChunk() != 64 {
		t.Errorf("NextChunk = %d, want 64", bh.NextChunk())
	}

	// Verify chunk header (bytes 24-63).
	ch := chunk.ParseHeader(data[24:64])
	if !ch.ValidHeader() {
		t.Error("chunk header hash invalid")
	}
	if ch.Type() != chunk.FileSignature {
		t.Errorf("Type = %c, want %c", ch.Type(), chunk.FileSignature)
	}
	if ch.NumRecords() != 0 {
		t.Errorf("NumRecords = %d, want 0", ch.NumRecords())
	}
	if ch.DataSize() != 0 {
		t.Errorf("DataSize = %d, want 0", ch.DataSize())
	}
}

func TestWriteFileSignatureKnownBytes(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	expected := []byte{
		0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, // block header hash
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk = 0
		0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk = 64
		0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, // chunk header hash
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size = 0
		0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, // data_hash
		0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type='s' | num=0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size = 0
	}

	data := buf.Bytes()
	if !bytes.Equal(data[:64], expected) {
		t.Errorf("file signature bytes mismatch")
		t.Logf("Got:  %x", data[:64])
		t.Logf("Want: %x", expected)
	}
}

func TestWriteFileSignatureNotAtZero(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)
	// Advance the writer position artificially.
	w.WriteChunk(&chunk.Chunk{
		Header: chunk.NewHeader(nil, chunk.FileSignature, 0, 0),
	})
	err := w.WriteFileSignature()
	if err == nil {
		t.Error("WriteFileSignature at non-zero should fail")
	}
}

func TestWriteChunkSmall(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)

	// Write file signature first.
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Write a small data chunk.
	data := []byte("test data for chunk")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Position should be past the chunk.
	if w.Pos() <= 64 {
		t.Errorf("Pos = %d, should be > 64", w.Pos())
	}
}

func TestWriteChunkSpanningBlocks(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Write a chunk large enough to span at least one block boundary.
	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Verify block headers exist at 64 KiB boundaries.
	output := buf.Bytes()
	numBlocks := w.Pos() / block.BlockSize
	for i := uint64(0); i <= numBlocks; i++ {
		pos := i * block.BlockSize
		if pos+block.HeaderSize > uint64(len(output)) {
			break
		}
		bh := block.Parse(output[pos : pos+block.HeaderSize])
		if !bh.Valid() {
			t.Errorf("block header at pos %d is invalid", pos)
		}
	}
}

func TestFlushWithFlusher(t *testing.T) {
	var inner bytes.Buffer
	fb := &flushBuffer{Buffer: &inner}
	w := New(fb)
	w.WriteFileSignature()
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !fb.flushed {
		t.Error("Flush should have called inner Flush")
	}
}

func TestFlushWithoutFlusher(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)
	// Flushing a plain bytes.Buffer should be a no-op.
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

type flushBuffer struct {
	*bytes.Buffer
	flushed bool
}

func (f *flushBuffer) Flush() error {
	f.flushed = true
	return nil
}

func TestWriteMultipleChunks(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}
	if w.Pos() != 64 {
		t.Errorf("pos after sig = %d, want 64", w.Pos())
	}

	// Write several chunks and verify positions advance.
	for i := 0; i < 5; i++ {
		prevPos := w.Pos()
		data := bytes.Repeat([]byte{byte('A' + i)}, 100+i*50)
		c := chunk.Chunk{
			Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
			Data:   data,
		}
		if err := w.WriteChunk(&c); err != nil {
			t.Fatalf("WriteChunk[%d]: %v", i, err)
		}
		if w.Pos() <= prevPos {
			t.Errorf("chunk[%d]: pos %d did not advance past %d", i, w.Pos(), prevPos)
		}
	}
}

func TestWriteChunkZeroData(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// A chunk with no data (like file signature or metadata).
	c := chunk.Chunk{
		Header: chunk.NewHeader(nil, chunk.Padding, 0, 0),
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
}

func TestCloseWriter(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWriteChunkWithPadding(t *testing.T) {
	// Test the padding loop in WriteChunk. Padding is needed when:
	//   RoundUpToPossibleChunkBoundary(chunkBegin + numRecords)
	//     > AddWithOverhead(chunkBegin, headerSize + dataSize)
	// This happens with many records but small data.
	var buf bytes.Buffer
	w := New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Create a chunk with many records (100) but tiny data (10 bytes).
	// chunkBegin = 64, numRecords = 100
	// RoundUpToPossibleChunkBoundary(64 + 100) = RoundUpToPossibleChunkBoundary(164) = 164 (already valid)
	// AddWithOverhead(64, 40 + 10) = AddWithOverhead(64, 50) = 114
	// So chunkEnd = max(114, 164) = 164
	// The chunk data is only headerSize(40) + dataSize(10) = 50 bytes,
	// but the chunk must span to position 164, requiring padding.
	data := []byte("0123456789")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 100, 10),
		Data:   data,
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Position should be at chunkEnd (164).
	if w.Pos() != 164 {
		t.Errorf("Pos = %d, want 164", w.Pos())
	}
}

func TestBlockHeadersAtBoundaries(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Write a large chunk that spans 3+ blocks.
	data := make([]byte, 200000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Check every 64KiB boundary has a valid block header.
	output := buf.Bytes()
	for pos := uint64(0); pos+block.HeaderSize <= uint64(len(output)); pos += block.BlockSize {
		bh := block.Parse(output[pos : pos+block.HeaderSize])
		if !bh.Valid() {
			t.Errorf("invalid block header at position %d", pos)
		}

		// At position 0, previous_chunk should be 0.
		if pos == 0 {
			if bh.PreviousChunk() != 0 {
				t.Errorf("block[0] PreviousChunk = %d, want 0", bh.PreviousChunk())
			}
		}
	}
}

// errWriter always returns an error on Write.
type errWriter struct{}

func (e *errWriter) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("write error")
}

// shortWriter writes fewer bytes than requested without returning an error.
type shortWriter struct{ buf bytes.Buffer }

func (s *shortWriter) Write(p []byte) (int, error) {
	if len(p) > 1 {
		n, _ := s.buf.Write(p[:1])
		return n, nil
	}
	return s.buf.Write(p)
}

// failAfterWriter fails after a given number of bytes have been written.
type failAfterWriter struct {
	buf     bytes.Buffer
	limit   int
	written int
}

func (f *failAfterWriter) Write(p []byte) (int, error) {
	if f.written >= f.limit {
		return 0, fmt.Errorf("write limit exceeded")
	}
	allowed := f.limit - f.written
	if allowed > len(p) {
		allowed = len(p)
	}
	n, err := f.buf.Write(p[:allowed])
	f.written += n
	if n < len(p) && err == nil {
		return n, nil
	}
	return n, err
}

func TestWriteChunkWithErrWriter(t *testing.T) {
	ew := &errWriter{}
	w := New(ew)

	// WriteFileSignature should fail because writeDataWithBlockHeaders
	// will fail on the first writeRaw call, producing a "writing chunk header" error.
	err := w.WriteFileSignature()
	if err == nil {
		t.Fatal("WriteFileSignature with errWriter should fail")
	}
	if !strings.Contains(err.Error(), "writing chunk header") {
		t.Errorf("error = %q, want it to contain 'writing chunk header'", err)
	}

	// WriteChunk should also fail.
	c := chunk.Chunk{
		Header: chunk.NewHeader(nil, chunk.Padding, 0, 0),
	}
	err = w.WriteChunk(&c)
	if err == nil {
		t.Fatal("WriteChunk with errWriter should fail")
	}
}

func TestWriteRawShortWrite(t *testing.T) {
	sw := &shortWriter{}
	w := New(sw)

	// Writing the file signature requires writing 24 bytes (block header)
	// as the first writeRaw call. The shortWriter will write only 1 byte
	// and return no error, which should trigger io.ErrShortWrite.
	err := w.WriteFileSignature()
	if err == nil {
		t.Fatal("WriteFileSignature with shortWriter should fail")
	}
	if !strings.Contains(err.Error(), io.ErrShortWrite.Error()) {
		t.Errorf("error = %q, want it to contain %q", err, io.ErrShortWrite.Error())
	}
}

func TestWriteChunkPaddingCrossingBlockBoundary(t *testing.T) {
	// This test exercises the block boundary check inside the padding loop
	// of WriteChunk (lines 50-70 of chunkwriter.go). We need the padding
	// to cross a 64 KiB block boundary.
	//
	// Strategy: After writing the file signature (pos=64), write a chunk
	// with numRecords large enough that chunkEnd extends past the next
	// block boundary (65536), while the actual header+data is small.
	// This forces the padding loop to cross the 65536 boundary.
	var buf bytes.Buffer
	w := New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}
	// pos = 64.

	// chunkBegin = 64, numRecords = 131073, dataSize = 0.
	// AddWithOverhead(64, 40+0) = 104 (no block boundary overhead for 40 bytes)
	// RoundUpToPossibleChunkBoundary(64 + 131073) = RoundUpToPossibleChunkBoundary(131137) = 131137
	// chunkEnd = max(104, 131137) = 131137
	// After writing the 40-byte header, pos = 104. Padding goes from 104 to 131137,
	// crossing block boundaries at 65536 and 131072.
	c := chunk.Chunk{
		Header: chunk.NewHeader(nil, chunk.Simple, 131073, 0),
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	if w.Pos() != 131137 {
		t.Errorf("Pos = %d, want 131137", w.Pos())
	}

	// Verify block headers at boundaries 0, 65536, and 131072 are valid.
	output := buf.Bytes()
	for _, bpos := range []uint64{0, 65536, 131072} {
		if bpos+block.HeaderSize > uint64(len(output)) {
			t.Errorf("output too short for block header at %d", bpos)
			continue
		}
		bh := block.Parse(output[bpos : bpos+block.HeaderSize])
		if !bh.Valid() {
			t.Errorf("block header at position %d is invalid", bpos)
		}
	}
}

func TestWriteDataCrossingBlockBoundaryError(t *testing.T) {
	// This test triggers the error path inside writeDataWithBlockHeaders
	// when writeRaw fails mid-way through writing data that crosses a
	// block boundary.
	//
	// The file signature writes 64 bytes (24 block header + 40 chunk header).
	// We allow 64 bytes to succeed (file signature), then set up a chunk
	// whose data write will fail partway through.
	fw := &failAfterWriter{limit: 80}
	w := New(fw)

	// File signature writes 64 bytes; this succeeds.
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Now write a chunk. The chunk header is 40 bytes. The failAfterWriter
	// has 16 bytes remaining (80 - 64 = 16), so it will fail partway through
	// writing the 40-byte chunk header.
	data := []byte("some data")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	err := w.WriteChunk(&c)
	if err == nil {
		t.Fatal("WriteChunk with failAfterWriter should fail")
	}
	if !strings.Contains(err.Error(), "writing chunk header") {
		t.Errorf("error = %q, want it to contain 'writing chunk header'", err)
	}
}
