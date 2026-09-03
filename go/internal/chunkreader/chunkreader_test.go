package chunkreader

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/riegeli-go/internal/block"
	"github.com/google/riegeli-go/internal/chunk"
	"github.com/google/riegeli-go/internal/chunkwriter"
)

func TestReadFileSignature(t *testing.T) {
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	r := New(bytes.NewReader(buf.Bytes()))
	c, pos, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if pos != 0 {
		t.Errorf("chunkBegin = %d, want 0", pos)
	}
	if c.Header.Type() != chunk.FileSignature {
		t.Errorf("Type = %c, want %c", c.Header.Type(), chunk.FileSignature)
	}
	if c.Header.DataSize() != 0 {
		t.Errorf("DataSize = %d, want 0", c.Header.DataSize())
	}

	// Next read should be EOF.
	_, _, err = r.ReadChunk()
	if err != io.EOF {
		t.Errorf("second ReadChunk: got %v, want io.EOF", err)
	}
}

func TestReadMultipleChunks(t *testing.T) {
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	for i := 0; i < 2; i++ {
		data := []byte("test chunk data")
		c := chunk.Chunk{
			Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
			Data:   data,
		}
		if err := w.WriteChunk(&c); err != nil {
			t.Fatalf("WriteChunk[%d]: %v", i, err)
		}
	}

	r := New(bytes.NewReader(buf.Bytes()))
	count := 0
	for {
		_, _, err := r.ReadChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadChunk[%d]: %v", count, err)
		}
		count++
	}
	if count != 3 {
		t.Errorf("read %d chunks, want 3", count)
	}
}

func TestReadChunkSpanningBlocks(t *testing.T) {
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

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

	r := New(bytes.NewReader(buf.Bytes()))
	_, _, err := r.ReadChunk() // skip signature
	if err != nil {
		t.Fatalf("ReadChunk (signature): %v", err)
	}

	readChunk, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (data): %v", err)
	}
	if !bytes.Equal(readChunk.Data, data) {
		t.Errorf("data mismatch: got %d bytes, want %d", len(readChunk.Data), len(data))
	}
}

func TestSeekToChunkContaining(t *testing.T) {
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	chunkPositions := []uint64{w.Pos()}
	for i := 0; i < 3; i++ {
		data := bytes.Repeat([]byte{byte(i)}, 200)
		c := chunk.Chunk{
			Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
			Data:   data,
		}
		if err := w.WriteChunk(&c); err != nil {
			t.Fatalf("WriteChunk[%d]: %v", i, err)
		}
		chunkPositions = append(chunkPositions, w.Pos())
	}

	reader := bytes.NewReader(buf.Bytes())
	r := New(reader)
	if err := r.SeekToChunkContaining(0); err != nil {
		t.Fatalf("SeekToChunkContaining(0): %v", err)
	}
	if r.Pos() != 0 {
		t.Errorf("Pos after seek(0) = %d, want 0", r.Pos())
	}

	c, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk after seek: %v", err)
	}
	if c.Header.Type() != chunk.FileSignature {
		t.Errorf("after seek(0), Type = %c, want %c", c.Header.Type(), chunk.FileSignature)
	}
}

func TestReaderRequiresSeeker(t *testing.T) {
	data := make([]byte, 100)
	r := New(bytes.NewBuffer(data))
	err := r.SeekToChunkContaining(0)
	if err == nil {
		t.Error("SeekToChunkContaining should fail without io.ReadSeeker")
	}
}

func TestSeekToSecondChunk(t *testing.T) {
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	data1 := make([]byte, 100000)
	for i := range data1 {
		data1[i] = byte(i % 256)
	}
	c1 := chunk.Chunk{
		Header: chunk.NewHeader(data1, chunk.Simple, 1, uint64(len(data1))),
		Data:   data1,
	}
	if err := w.WriteChunk(&c1); err != nil {
		t.Fatalf("WriteChunk[0]: %v", err)
	}

	secondPos := w.Pos()
	data2 := bytes.Repeat([]byte{0xBB}, 200)
	c2 := chunk.Chunk{
		Header: chunk.NewHeader(data2, chunk.Simple, 1, uint64(len(data2))),
		Data:   data2,
	}
	if err := w.WriteChunk(&c2); err != nil {
		t.Fatalf("WriteChunk[1]: %v", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	r := New(reader)
	if err := r.SeekToChunkContaining(secondPos); err != nil {
		t.Fatalf("SeekToChunkContaining(%d): %v", secondPos, err)
	}

	c, pos, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if pos != secondPos {
		t.Errorf("chunk pos = %d, want %d", pos, secondPos)
	}
	if c.Header.NumRecords() != 1 {
		t.Errorf("NumRecords = %d, want 1", c.Header.NumRecords())
	}
}

func TestReadWriteRoundTripMultiBlock(t *testing.T) {
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	type chunkInfo struct {
		pos  uint64
		data []byte
	}
	var chunks []chunkInfo
	for i := 0; i < 10; i++ {
		pos := w.Pos()
		size := 1000 + i*5000
		data := make([]byte, size)
		for j := range data {
			data[j] = byte((i + j) % 256)
		}
		c := chunk.Chunk{
			Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
			Data:   data,
		}
		if err := w.WriteChunk(&c); err != nil {
			t.Fatalf("WriteChunk[%d]: %v", i, err)
		}
		chunks = append(chunks, chunkInfo{pos: pos, data: data})
	}

	r := New(bytes.NewReader(buf.Bytes()))

	c, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}
	if c.Header.Type() != chunk.FileSignature {
		t.Fatalf("first chunk type = %c, want 's'", c.Header.Type())
	}

	for i, expected := range chunks {
		c, pos, err := r.ReadChunk()
		if err != nil {
			t.Fatalf("ReadChunk[%d]: %v", i, err)
		}
		if pos != expected.pos {
			t.Errorf("chunk[%d] pos = %d, want %d", i, pos, expected.pos)
		}
		if !bytes.Equal(c.Data, expected.data) {
			t.Errorf("chunk[%d] data mismatch (len %d vs %d)", i, len(c.Data), len(expected.data))
		}
	}
}

func TestPosTracking(t *testing.T) {
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	r := New(bytes.NewReader(buf.Bytes()))
	if r.Pos() != 0 {
		t.Errorf("initial pos = %d, want 0", r.Pos())
	}

	_, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if r.Pos() != 64 {
		t.Errorf("pos after reading file signature = %d, want 64", r.Pos())
	}
}

func TestSeekAndReadSequence(t *testing.T) {
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)

	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	largeData := make([]byte, 200000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	largeChunkPos := w.Pos()
	c := chunk.Chunk{
		Header: chunk.NewHeader(largeData, chunk.Simple, 1, uint64(len(largeData))),
		Data:   largeData,
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk (large): %v", err)
	}

	largeData2 := make([]byte, 100000)
	for i := range largeData2 {
		largeData2[i] = byte((i + 42) % 256)
	}
	secondChunkPos := w.Pos()
	c2 := chunk.Chunk{
		Header: chunk.NewHeader(largeData2, chunk.Simple, 1, uint64(len(largeData2))),
		Data:   largeData2,
	}
	if err := w.WriteChunk(&c2); err != nil {
		t.Fatalf("WriteChunk (second): %v", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	r := New(reader)
	if err := r.SeekToChunkContaining(0); err != nil {
		t.Fatalf("SeekToChunkContaining(0): %v", err)
	}
	sig, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}
	if sig.Header.Type() != chunk.FileSignature {
		t.Errorf("expected file signature, got %c", sig.Header.Type())
	}

	if err := r.SeekToChunkContaining(secondChunkPos); err != nil {
		t.Fatalf("SeekToChunkContaining(%d): %v", secondChunkPos, err)
	}
	readSecond, pos, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (second): %v", err)
	}
	if pos != secondChunkPos {
		t.Errorf("second chunk pos = %d, want %d", pos, secondChunkPos)
	}
	if !bytes.Equal(readSecond.Data, largeData2) {
		t.Errorf("second chunk data mismatch")
	}

	if err := r.SeekToChunkContaining(largeChunkPos); err != nil {
		t.Fatalf("SeekToChunkContaining(%d): %v", largeChunkPos, err)
	}
	readLarge, pos, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (large): %v", err)
	}
	if pos != largeChunkPos {
		t.Errorf("large chunk pos = %d, want %d", pos, largeChunkPos)
	}
	if !bytes.Equal(readLarge.Data, largeData) {
		t.Errorf("large chunk data mismatch")
	}
}

// nonSeekReader wraps a bytes.Reader without exposing Seek.
type nonSeekReader struct {
	r *bytes.Reader
}

func (n *nonSeekReader) Read(p []byte) (int, error) { return n.r.Read(p) }

func TestReadWithNonSeekableReader(t *testing.T) {
	// This tests the skipBytes fallback path (non-seeker).
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	data := []byte("hello from non-seekable")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Wrap in non-seekable reader so skipBytes uses the fallback read-and-discard path.
	r := New(&nonSeekReader{r: bytes.NewReader(buf.Bytes())})

	// Read file signature - this requires skipping the block header via the fallback path.
	sigChunk, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}
	if sigChunk.Header.Type() != chunk.FileSignature {
		t.Errorf("Type = %c, want 's'", sigChunk.Header.Type())
	}

	// Read data chunk.
	dataChunk, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (data): %v", err)
	}
	if !bytes.Equal(dataChunk.Data, data) {
		t.Errorf("data = %q, want %q", dataChunk.Data, data)
	}
}

func TestReadWithNonSeekableReaderLargeChunk(t *testing.T) {
	// Test the skipBytes fallback with chunks large enough to need >4096 byte skips.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Use many records but small data so the chunk has padding that needs to be skipped.
	data := bytes.Repeat([]byte{0xAA}, 50)
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 10000, 50),
		Data:   data,
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Write another chunk after the padded one.
	data2 := []byte("after padding")
	c2 := chunk.Chunk{
		Header: chunk.NewHeader(data2, chunk.Simple, 1, uint64(len(data2))),
		Data:   data2,
	}
	if err := w.WriteChunk(&c2); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	r := New(&nonSeekReader{r: bytes.NewReader(buf.Bytes())})

	// Skip signature.
	_, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}

	// Read the padded chunk - this exercises skipBytes fallback for padding.
	_, _, err = r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (padded): %v", err)
	}

	// Read the chunk after the padding.
	dataChunk, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (after): %v", err)
	}
	if !bytes.Equal(dataChunk.Data, data2) {
		t.Errorf("data = %q, want %q", dataChunk.Data, data2)
	}
}

func TestReadChunkExcessiveDataSize(t *testing.T) {
	// Craft a chunk header with a huge data_size to trigger the maxDataSize check.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Manually construct a fake chunk header with huge data_size.
	// We just need 40 bytes after the file signature (position 64+).
	raw := buf.Bytes()
	// Extend the buffer past the signature to contain a fake chunk header.
	fakeHeader := make([]byte, 40)
	binary.LittleEndian.PutUint64(fakeHeader[8:16], 2<<30) // data_size = 2 GiB (> maxDataSize)
	fakeHeader[24] = byte(chunk.Simple)                     // chunk type
	combined := make([]byte, len(raw)+len(fakeHeader))
	copy(combined, raw)
	copy(combined[len(raw):], fakeHeader)

	r := New(bytes.NewReader(combined))
	// Skip signature.
	_, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}
	// The next chunk should fail due to excessive data_size.
	_, _, err = r.ReadChunk()
	if err == nil {
		t.Error("ReadChunk with excessive data_size should fail")
	}
}

func TestSeekToMiddleOfSpanningChunk(t *testing.T) {
	// Tests the prevChunk != 0 seek branch: seeking to a position
	// within a chunk that spans a block boundary.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Write a large chunk that spans the 64K boundary.
	largeData := make([]byte, 100000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	chunkPos := w.Pos() // 64
	c := chunk.Chunk{
		Header: chunk.NewHeader(largeData, chunk.Simple, 1, uint64(len(largeData))),
		Data:   largeData,
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Seek to a position in the middle of the large chunk (e.g., 70000),
	// which is past the first block boundary (65536).
	reader := bytes.NewReader(buf.Bytes())
	r := New(reader)
	err := r.SeekToChunkContaining(70000)
	if err != nil {
		t.Fatalf("SeekToChunkContaining(70000): %v", err)
	}

	// The reader should have seeked back to the start of the large chunk.
	if r.Pos() != chunkPos {
		t.Errorf("Pos = %d, want %d", r.Pos(), chunkPos)
	}

	// Verify we can read the chunk correctly.
	readC, pos, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if pos != chunkPos {
		t.Errorf("chunk pos = %d, want %d", pos, chunkPos)
	}
	if !bytes.Equal(readC.Data, largeData) {
		t.Errorf("data mismatch (len %d vs %d)", len(readC.Data), len(largeData))
	}
}

func TestSeekPastSpanningChunk(t *testing.T) {
	// Tests seeking to a position past a spanning chunk (prevChunk != 0 and pos >= blockBound+nextChunk).
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Write a chunk that spans the 64K boundary.
	data1 := make([]byte, 80000)
	for i := range data1 {
		data1[i] = byte(i % 256)
	}
	c1 := chunk.Chunk{
		Header: chunk.NewHeader(data1, chunk.Simple, 1, uint64(len(data1))),
		Data:   data1,
	}
	if err := w.WriteChunk(&c1); err != nil {
		t.Fatalf("WriteChunk[0]: %v", err)
	}

	secondPos := w.Pos()
	data2 := bytes.Repeat([]byte{0xCC}, 500)
	c2 := chunk.Chunk{
		Header: chunk.NewHeader(data2, chunk.Simple, 1, uint64(len(data2))),
		Data:   data2,
	}
	if err := w.WriteChunk(&c2); err != nil {
		t.Fatalf("WriteChunk[1]: %v", err)
	}

	// Seek to secondPos. The block boundary at 65536 has prevChunk != 0
	// (since the first data chunk spans it). Since secondPos >= blockBound+nextChunk,
	// we should land at secondPos.
	reader := bytes.NewReader(buf.Bytes())
	r := New(reader)

	// Only seek here if secondPos's block boundary is the one at 65536.
	blockBound := block.RoundDownToBlockBoundary(secondPos)
	if blockBound > 0 {
		// This should exercise the prevChunk != 0, pos >= blockBound+nextChunk path.
		if err := r.SeekToChunkContaining(secondPos); err != nil {
			t.Fatalf("SeekToChunkContaining(%d): %v", secondPos, err)
		}

		readC, pos, err := r.ReadChunk()
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		if pos != secondPos {
			t.Errorf("chunk pos = %d, want %d", pos, secondPos)
		}
		if !bytes.Equal(readC.Data, data2) {
			t.Errorf("data mismatch")
		}
	}
}

func TestReadChunkWithPadding(t *testing.T) {
	// Tests the padding-skip path in ReadChunk: chunkEnd > pos after data read.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	if err := w.WriteFileSignature(); err != nil {
		t.Fatalf("WriteFileSignature: %v", err)
	}

	// Many records, small data -> padding needed.
	data := []byte("x")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 200, 1),
		Data:   data,
	}
	if err := w.WriteChunk(&c); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Write another chunk after.
	data2 := []byte("after")
	c2 := chunk.Chunk{
		Header: chunk.NewHeader(data2, chunk.Simple, 1, uint64(len(data2))),
		Data:   data2,
	}
	if err := w.WriteChunk(&c2); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	r := New(bytes.NewReader(buf.Bytes()))
	_, _, err := r.ReadChunk() // sig
	if err != nil {
		t.Fatalf("sig: %v", err)
	}
	_, _, err = r.ReadChunk() // padded chunk
	if err != nil {
		t.Fatalf("padded chunk: %v", err)
	}
	readC2, _, err := r.ReadChunk() // chunk after padding
	if err != nil {
		t.Fatalf("after chunk: %v", err)
	}
	if !bytes.Equal(readC2.Data, data2) {
		t.Errorf("data = %q, want %q", readC2.Data, data2)
	}
}

// errAfterNReader returns data up to a limit, then returns an injected error.
// It does NOT implement io.Seeker.
type errAfterNReader struct {
	data  []byte
	pos   int
	limit int
}

func (r *errAfterNReader) Read(p []byte) (int, error) {
	if r.pos >= r.limit {
		return 0, fmt.Errorf("injected read error")
	}
	n := copy(p, r.data[r.pos:])
	if n > r.limit-r.pos {
		n = r.limit - r.pos
	}
	r.pos += n
	if r.pos >= r.limit {
		return n, fmt.Errorf("injected read error")
	}
	return n, nil
}

// errSeekReader wraps a bytes.Reader and returns errors on Seek/Read after a threshold.
type errSeekReader struct {
	*bytes.Reader
	seekErr  error // if non-nil, Seek always returns this
	readErr  error // if non-nil, Read always returns this
	failRead bool  // if true, Read returns readErr
}

func (r *errSeekReader) Seek(offset int64, whence int) (int64, error) {
	if r.seekErr != nil {
		return 0, r.seekErr
	}
	return r.Reader.Seek(offset, whence)
}

func (r *errSeekReader) Read(p []byte) (int, error) {
	if r.failRead {
		return 0, r.readErr
	}
	return r.Reader.Read(p)
}

// buildTestFile creates a file with a signature and one data chunk, returning
// the raw bytes and the position where the data chunk begins.
func buildTestFile(dataSize int) ([]byte, uint64) {
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	_ = w.WriteChunk(&c)
	return buf.Bytes(), 64 // file signature ends at position 64
}

func TestReadChunkHeaderReadError(t *testing.T) {
	// Build a file: signature (ends at 64) + data chunk.
	// At position 64 there is a block header (24 bytes) then the data chunk header (40 bytes).
	// Set the error limit so the block header at 64 reads fine (positions 0-87 are ok)
	// but the chunk header read at 88 fails partway through.
	fileData, _ := buildTestFile(100)

	// The file signature occupies positions 0-63 (block header 0-23, chunk header 24-63).
	// After reading the signature, pos=64. The next ReadChunk:
	//   - skipBytes(24) for block header at 64 -> reads positions 64-87
	//   - readFull for chunk header at 88 -> reads positions 88-127
	// Set limit to 100 so the chunk header read at 88 fails after reading 12 bytes.
	r := New(&errAfterNReader{data: fileData, limit: 100})
	// First ReadChunk reads file signature (positions 0-63) - should succeed.
	_, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}

	// Second ReadChunk should fail reading chunk header.
	_, _, err = r.ReadChunk()
	if err == nil {
		t.Fatal("expected error reading chunk header, got nil")
	}
	if !strings.Contains(err.Error(), "reading chunk header") {
		t.Errorf("error = %q, want it to contain 'reading chunk header'", err)
	}
}

func TestReadChunkDataReadError(t *testing.T) {
	// Build a file with a data chunk that has data.
	// Set limit so the chunk header reads fine but data read fails.
	fileData, _ := buildTestFile(100)

	// File layout:
	// 0-23: block header
	// 24-63: signature chunk header (40 bytes)
	// 64-87: block header at boundary
	// 88-127: data chunk header (40 bytes)
	// 128+: data chunk data (100 bytes)
	// Set limit to 140 so data read fails after 12 bytes.
	r := New(&errAfterNReader{data: fileData, limit: 140})
	_, _, err := r.ReadChunk() // sig
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}

	_, _, err = r.ReadChunk()
	if err == nil {
		t.Fatal("expected error reading chunk data, got nil")
	}
	if !strings.Contains(err.Error(), "reading chunk data") {
		t.Errorf("error = %q, want it to contain 'reading chunk data'", err)
	}
}

func TestReadChunkPaddingSkipError(t *testing.T) {
	// Test the non-EOF error path during padding skip.
	// Write a chunk with many records (creates padding) using a non-seekable reader
	// that returns an error during the padding skip.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	// Many records, small data -> padding needed.
	data := []byte("x")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 200, 1),
		Data:   data,
	}
	_ = w.WriteChunk(&c)

	fileData := buf.Bytes()

	// After reading file signature (pos=64), read the padded chunk:
	// Block header at 64 (24 bytes) + chunk header at 88 (40 bytes) + 1 byte data at 128 = pos 129.
	// ChunkEnd will be > 129 due to the 200 records. We need padding skip to fail.
	// Compute where padding starts: after block header (24) + chunk header (40) + data (1) = pos 129.
	// chunkEnd = max(AddWithOverhead(64, 40+1), RoundUpToPossibleChunkBoundary(64+200))
	//          = max(AddWithOverhead(64, 41), RoundUpToPossibleChunkBoundary(264))
	//          = max(64+41+24, 264) = max(129, 264) = 264
	// So padding skip needs to skip from 129 to 264 = 135 bytes.
	// Set the limit so this fails.
	// File sig reads 0-63 (64 bytes), then padded chunk reads 64-128 (65 bytes) = 129 total bytes read.
	// Set limit to 135 so there's 6 bytes left, then the padding skip fails partway.
	r := New(&errAfterNReader{data: fileData, limit: 135})
	_, _, err := r.ReadChunk() // sig
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}

	_, _, err = r.ReadChunk() // padded chunk - should fail during padding skip
	if err == nil {
		t.Fatal("expected error skipping padding, got nil")
	}
	if !strings.Contains(err.Error(), "skipping padding") {
		t.Errorf("error = %q, want it to contain 'skipping padding'", err)
	}
}

func TestReadSkipBlockHeaderError(t *testing.T) {
	// Test error in readSkippingBlockHeaders when skipBytes fails at a block boundary.
	// Build a file with a chunk that starts at pos 64 (block boundary).
	// Use a non-seekable errAfterNReader that fails during the skipBytes(24) for the block header.
	fileData, _ := buildTestFile(100)

	// After reading sig (64 bytes), next ReadChunk tries to read chunk header at pos 64.
	// pos 64 is a block boundary, so skipBytes(24) is called first (reads 64-87).
	// Set limit to 70 so the skipBytes call fails partway through the block header.
	r := New(&errAfterNReader{data: fileData, limit: 70})
	_, _, err := r.ReadChunk() // sig succeeds (reads 0-63)
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}

	// The next ReadChunk should fail trying to skip the block header at position 64.
	_, _, err = r.ReadChunk()
	if err == nil {
		t.Fatal("expected error skipping block header, got nil")
	}
}

func TestSeekBlockHeaderReadError(t *testing.T) {
	// Test the block header read error path in SeekToChunkContaining.
	// Use an errSeekReader that allows Seek but fails on Read.
	fileData, _ := buildTestFile(100)

	esr := &errSeekReader{
		Reader:   bytes.NewReader(fileData),
		readErr:  fmt.Errorf("injected read error"),
		failRead: false,
	}
	r := New(esr)

	// Seek to position 100 (in the first block after boundary 0).
	// This will:
	// 1. Seek to blockBound=0
	// 2. blockBound==0 && pos!=0, so it tries to read block header
	// Enable read failure before the ReadFull for the block header.
	esr.failRead = true
	err := r.SeekToChunkContaining(100)
	if err == nil {
		t.Fatal("expected error reading block header for seek, got nil")
	}
	if !strings.Contains(err.Error(), "reading block header for seek") {
		t.Errorf("error = %q, want it to contain 'reading block header for seek'", err)
	}
}

func TestSeekInvalidBlockHeader(t *testing.T) {
	// Test the invalid block header path in SeekToChunkContaining.
	// Build a valid file, then corrupt the block header at position 0.
	fileData, _ := buildTestFile(100)
	corruptData := make([]byte, len(fileData))
	copy(corruptData, fileData)

	// Corrupt the block header at position 0 (bytes 0-23).
	// Zero out the hash to make it invalid.
	for i := 0; i < 8; i++ {
		corruptData[i] = 0xFF
	}

	r := New(bytes.NewReader(corruptData))
	err := r.SeekToChunkContaining(32) // pos=32, blockBound=0, not (0,0)
	if err == nil {
		t.Fatal("expected invalid block header error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid block header") {
		t.Errorf("error = %q, want it to contain 'invalid block header'", err)
	}
}

func TestSeekPrevChunkZero(t *testing.T) {
	// Test the prevChunk==0 branch in SeekToChunkContaining.
	// At block boundary 0, the block header has prevChunk=0, nextChunk=64
	// (the file signature chunk at position 0 with chunkEnd=64).
	// Seeking to pos=32: blockBound=0, it's not (0,0) so we read the block header.
	// prevChunk=0, nextChunk=64, pos=32 < 0+64=64 -> enters prevChunk==0 branch.
	// Should seek to blockBound=0 and be able to read the signature.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	// Write a data chunk so the file has content past the signature.
	data := []byte("test data for seek")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	_ = w.WriteChunk(&c)

	reader := bytes.NewReader(buf.Bytes())
	r := New(reader)

	// Seek to position 32 (within the first block, within the signature chunk).
	err := r.SeekToChunkContaining(32)
	if err != nil {
		t.Fatalf("SeekToChunkContaining(32): %v", err)
	}

	// The reader should be at position 0 (blockBound).
	if r.Pos() != 0 {
		t.Errorf("Pos = %d, want 0", r.Pos())
	}

	// Should be able to read the file signature chunk.
	ch, pos, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk after seek: %v", err)
	}
	if pos != 0 {
		t.Errorf("chunk pos = %d, want 0", pos)
	}
	if ch.Header.Type() != chunk.FileSignature {
		t.Errorf("Type = %c, want %c", ch.Header.Type(), chunk.FileSignature)
	}
}

func TestSeekPrevChunkZeroPastNextChunk(t *testing.T) {
	// Test prevChunk==0 && pos >= blockBound+nextChunk branch.
	// At block boundary 0: prevChunk=0, nextChunk=64.
	// Seeking to pos=64: 64 >= 0+64 -> enters the first branch.
	// Should seek to nextChunkPos=64.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	data := []byte("test data for seek past")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	_ = w.WriteChunk(&c)

	reader := bytes.NewReader(buf.Bytes())
	r := New(reader)

	err := r.SeekToChunkContaining(64)
	if err != nil {
		t.Fatalf("SeekToChunkContaining(64): %v", err)
	}

	// Should be at the data chunk position (64).
	if r.Pos() != 64 {
		t.Errorf("Pos = %d, want 64", r.Pos())
	}

	// Should be able to read the data chunk.
	ch, pos, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk after seek: %v", err)
	}
	if pos != 64 {
		t.Errorf("chunk pos = %d, want 64", pos)
	}
	if ch.Header.Type() != chunk.Simple {
		t.Errorf("Type = %c, want %c", ch.Header.Type(), chunk.Simple)
	}
}

func TestSeekInitialSeekError(t *testing.T) {
	// Test the initial Seek error path in SeekToChunkContaining (line 148-150).
	fileData, _ := buildTestFile(100)
	esr := &errSeekReader{
		Reader:  bytes.NewReader(fileData),
		seekErr: fmt.Errorf("injected seek error"),
	}
	r := New(esr)
	err := r.SeekToChunkContaining(100)
	if err == nil {
		t.Fatal("expected seek error, got nil")
	}
	if !strings.Contains(err.Error(), "injected seek error") {
		t.Errorf("error = %q, want it to contain 'injected seek error'", err)
	}
}

// seekAfterNSeeker wraps a bytes.Reader and fails on the Nth Seek call.
type seekAfterNSeeker struct {
	*bytes.Reader
	seekCount    int
	failAfterN   int
	seekErr      error
}

func (s *seekAfterNSeeker) Seek(offset int64, whence int) (int64, error) {
	s.seekCount++
	if s.seekCount > s.failAfterN {
		return 0, s.seekErr
	}
	return s.Reader.Seek(offset, whence)
}

func TestSeekSecondSeekError(t *testing.T) {
	// Test seek errors in the branch-specific Seek calls in SeekToChunkContaining.
	// The first Seek (to blockBound) succeeds, the block header read succeeds,
	// but the second Seek (within a branch) fails.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	// Write a large chunk spanning the 64K boundary.
	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	_ = w.WriteChunk(&c)

	fileData := buf.Bytes()

	// Test: seek to 70000 (within the spanning chunk, past block boundary 65536).
	// blockBound=65536, prevChunk!=0, pos < blockBound+nextChunk => seeks back to chunkStart.
	// First Seek (to blockBound 65536) succeeds, second Seek (to chunkStart) should fail.
	s := &seekAfterNSeeker{
		Reader:     bytes.NewReader(fileData),
		failAfterN: 1,
		seekErr:    fmt.Errorf("injected second seek error"),
	}
	r := New(s)
	err := r.SeekToChunkContaining(70000)
	if err == nil {
		t.Fatal("expected seek error in branch, got nil")
	}
	if !strings.Contains(err.Error(), "injected second seek error") {
		t.Errorf("error = %q, want it to contain 'injected second seek error'", err)
	}
}

func TestSeekPrevChunkZeroSecondSeekError(t *testing.T) {
	// Test the seek error within the prevChunk==0 branch (line 183-185).
	// Seek to pos=32 with blockBound=0: first Seek to 0 succeeds, read block header succeeds,
	// then Seek back to 0 (in prevChunk==0 branch) should fail.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	data := []byte("test data")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	_ = w.WriteChunk(&c)

	fileData := buf.Bytes()

	s := &seekAfterNSeeker{
		Reader:     bytes.NewReader(fileData),
		failAfterN: 1, // first Seek (to blockBound=0) succeeds, second fails
		seekErr:    fmt.Errorf("injected prevchunk0 seek error"),
	}
	r := New(s)
	err := r.SeekToChunkContaining(32)
	if err == nil {
		t.Fatal("expected seek error in prevChunk==0 branch, got nil")
	}
	if !strings.Contains(err.Error(), "injected prevchunk0 seek error") {
		t.Errorf("error = %q, want it to contain 'injected prevchunk0 seek error'", err)
	}
}

func TestSeekPrevChunkZeroPastNextChunkSeekError(t *testing.T) {
	// Test the seek error in the prevChunk==0 && pos>=blockBound+nextChunk branch (line 177-179).
	// Seek to pos=64 with blockBound=0: prevChunk=0, nextChunk=64, pos=64 >= 0+64.
	// First Seek (to blockBound=0) succeeds, second Seek (to nextChunkPos=64) should fail.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	data := []byte("test data")
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	_ = w.WriteChunk(&c)

	fileData := buf.Bytes()

	s := &seekAfterNSeeker{
		Reader:     bytes.NewReader(fileData),
		failAfterN: 1,
		seekErr:    fmt.Errorf("injected nextchunk seek error"),
	}
	r := New(s)
	err := r.SeekToChunkContaining(64)
	if err == nil {
		t.Fatal("expected seek error, got nil")
	}
	if !strings.Contains(err.Error(), "injected nextchunk seek error") {
		t.Errorf("error = %q, want it to contain 'injected nextchunk seek error'", err)
	}
}

func TestSeekElseBranchSeekError(t *testing.T) {
	// Test the seek error in the else branch (pos >= blockBound+nextChunk, prevChunk != 0).
	// Need: a block boundary where prevChunk!=0 and seek to a pos >= blockBound+nextChunk.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	// Write a chunk spanning 64K boundary.
	data1 := make([]byte, 80000)
	for i := range data1 {
		data1[i] = byte(i % 256)
	}
	c1 := chunk.Chunk{
		Header: chunk.NewHeader(data1, chunk.Simple, 1, uint64(len(data1))),
		Data:   data1,
	}
	_ = w.WriteChunk(&c1)

	// Write another chunk after.
	secondPos := w.Pos()
	data2 := bytes.Repeat([]byte{0xDD}, 200)
	c2 := chunk.Chunk{
		Header: chunk.NewHeader(data2, chunk.Simple, 1, uint64(len(data2))),
		Data:   data2,
	}
	_ = w.WriteChunk(&c2)

	fileData := buf.Bytes()

	// Seek to secondPos. blockBound for secondPos is 65536 (if secondPos > 65536).
	// At 65536, prevChunk != 0 (first chunk spans it), nextChunk points past first chunk.
	// If secondPos >= blockBound+nextChunk -> else branch.
	blockBound := block.RoundDownToBlockBoundary(secondPos)
	if blockBound > 0 {
		s := &seekAfterNSeeker{
			Reader:     bytes.NewReader(fileData),
			failAfterN: 1,
			seekErr:    fmt.Errorf("injected else branch seek error"),
		}
		r := New(s)
		err := r.SeekToChunkContaining(secondPos)
		if err == nil {
			t.Fatal("expected seek error, got nil")
		}
		if !strings.Contains(err.Error(), "injected else branch seek error") {
			t.Errorf("error = %q, want it to contain 'injected else branch seek error'", err)
		}
	}
}

func TestSkipBytesSeekError(t *testing.T) {
	// Test the Seek error path in skipBytes (line 113-115).
	// When the source is a ReadSeeker and Seek fails during skipBytes.
	fileData, _ := buildTestFile(100)

	// The reader position starts at 0. When we call ReadChunk, the first thing it does
	// is try to read the chunk header. At pos=0, it's a block boundary, so skipBytes(24)
	// is called. Since our reader is a ReadSeeker, skipBytes uses Seek.
	// We need the Seek in skipBytes to fail.
	s := &seekAfterNSeeker{
		Reader:     bytes.NewReader(fileData),
		failAfterN: 0, // fail on the very first Seek
		seekErr:    fmt.Errorf("injected skipBytes seek error"),
	}
	r := New(s)
	_, _, err := r.ReadChunk()
	if err == nil {
		t.Fatal("expected error from skipBytes seek, got nil")
	}
}

func TestSkipBytesNonSeekerReadError(t *testing.T) {
	// Test the read error path in skipBytes for non-seeker (line 129-131).
	// Use a reader that returns an error when trying to read during skipBytes.
	// The first read at pos=0 is a block boundary -> skipBytes(24) -> reads 24 bytes.
	// Use errAfterNReader with limit=10 so the skip read fails.
	fileData, _ := buildTestFile(100)
	r := New(&errAfterNReader{data: fileData, limit: 10})
	_, _, err := r.ReadChunk()
	if err == nil {
		t.Fatal("expected error from non-seeker skipBytes read, got nil")
	}
}

func TestRemainingEqualsBlockSize(t *testing.T) {
	// Test the remaining==0 -> remaining=block.BlockSize path in readSkippingBlockHeaders (line 91-93).
	// This happens when pos is at a block boundary + HeaderSize, and after the block header
	// skip, pos moves to boundary + HeaderSize. Then RemainingInBlock returns
	// BlockSize - HeaderSize = 65512 which is NOT 0. So that doesn't work.
	//
	// Actually, remaining==0 happens when pos % BlockSize == 0, i.e., pos is exactly
	// at a block boundary. But at a block boundary, IsBlockBoundary returns true and
	// skipBytes is called FIRST, moving pos past the header. So the remaining==0 check
	// at line 91 would only trigger if after the block header skip, the new pos is
	// AGAIN at a block boundary. That would require HeaderSize == BlockSize which is false.
	//
	// Wait: the remaining==0 case at line 91 handles the case where we're NOT at
	// a block boundary (IsBlockBoundary returned false at line 83), but RemainingInBlock
	// returns 0. RemainingInBlock returns 0 only when pos % BlockSize == 0, which IS
	// a block boundary. So this is a defensive check that should logically not be reached
	// via normal flow. But we can still try to exercise it.
	//
	// Actually, re-reading: IsBlockBoundary checks pos%BlockSize==0.
	// RemainingInBlock returns (-pos)%BlockSize. When pos%BlockSize==0, this is 0.
	// So if IsBlockBoundary is true, we skip the header. After skipping, pos is
	// pos+24, and RemainingInBlock(pos+24) = BlockSize-24 = 65512 != 0.
	// So the remaining==0 path can't be reached through normal code flow.
	//
	// This is dead code / defensive code. We skip covering it.
	// Instead, let's add another test that increases statement coverage elsewhere.

	// Write a file where a chunk's data spans exactly from one block boundary's header end
	// to the next block boundary. This ensures readSkippingBlockHeaders processes
	// chunks that exactly fill up blocks.
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	// Write data that will span multiple complete blocks.
	data := make([]byte, 200000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	_ = w.WriteChunk(&c)

	r := New(bytes.NewReader(buf.Bytes()))
	// Read signature
	_, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (sig): %v", err)
	}
	// Read the large chunk - exercises multi-block readSkippingBlockHeaders
	readC, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk (data): %v", err)
	}
	if !bytes.Equal(readC.Data, data) {
		t.Errorf("data mismatch (len %d vs %d)", len(readC.Data), len(data))
	}
}

func TestSeekToMiddleOfSpanningChunkSeekError(t *testing.T) {
	// Test the seek error in prevChunk != 0, pos < blockBound+nextChunk branch (line 190-192).
	var buf bytes.Buffer
	w := chunkwriter.New(&buf)
	_ = w.WriteFileSignature()

	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	c := chunk.Chunk{
		Header: chunk.NewHeader(data, chunk.Simple, 1, uint64(len(data))),
		Data:   data,
	}
	_ = w.WriteChunk(&c)

	fileData := buf.Bytes()

	// Seek to 70000: blockBound=65536, prevChunk!=0, pos < blockBound+nextChunk.
	// First Seek to blockBound succeeds, then Seek to chunkStart fails.
	s := &seekAfterNSeeker{
		Reader:     bytes.NewReader(fileData),
		failAfterN: 1,
		seekErr:    fmt.Errorf("injected spanning seek error"),
	}
	r := New(s)
	err := r.SeekToChunkContaining(70000)
	if err == nil {
		t.Fatal("expected seek error, got nil")
	}
	if !strings.Contains(err.Error(), "injected spanning seek error") {
		t.Errorf("error = %q, want it to contain 'injected spanning seek error'", err)
	}
}

