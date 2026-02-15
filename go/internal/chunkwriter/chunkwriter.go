// Package chunkwriter writes chunks to a Riegeli file, inserting block headers
// at 64 KiB boundaries.
package chunkwriter

import (
	"fmt"
	"io"

	"github.com/google/riegeli-go/internal/block"
	"github.com/google/riegeli-go/internal/chunk"
)

// Writer writes chunks to a Riegeli file stream.
type Writer struct {
	dest io.Writer
	pos  uint64 // current file position (physical)
}

// New creates a new chunk writer wrapping dest.
func New(dest io.Writer) *Writer {
	return &Writer{dest: dest}
}

// Pos returns the current file position.
func (w *Writer) Pos() uint64 {
	return w.pos
}

// WriteChunk writes a complete chunk (header + data) to the output, inserting
// block headers at 64 KiB boundaries as needed.
func (w *Writer) WriteChunk(c *chunk.Chunk) error {
	// chunk_begin is the logical position (may be a block boundary).
	chunkBegin := w.pos

	// Pre-compute chunk end so block headers written during data output
	// have the correct next_chunk value.
	chunkEnd := block.ChunkEnd(chunk.HeaderSize, c.Header.DataSize(), c.Header.NumRecords(), chunkBegin)

	// Write chunk header bytes, inserting block headers at boundaries.
	if err := w.writeDataWithBlockHeaders(c.Header.Bytes(), chunkBegin, chunkEnd); err != nil {
		return fmt.Errorf("chunkwriter: writing chunk header: %w", err)
	}

	// Write chunk data bytes.
	if len(c.Data) > 0 {
		if err := w.writeDataWithBlockHeaders(c.Data, chunkBegin, chunkEnd); err != nil {
			return fmt.Errorf("chunkwriter: writing chunk data: %w", err)
		}
	}
	for w.pos < chunkEnd {
		if block.IsBlockBoundary(w.pos) {
			if err := w.writeBlockHeaderAt(chunkBegin, chunkEnd); err != nil {
				return err
			}
			continue
		}
		remaining := chunkEnd - w.pos
		nextBound := block.RemainingInBlock(w.pos)
		if nextBound == 0 {
			nextBound = block.BlockSize
		}
		toWrite := remaining
		if toWrite > nextBound {
			toWrite = nextBound
		}
		zeros := make([]byte, toWrite)
		if err := w.writeRaw(zeros); err != nil {
			return fmt.Errorf("chunkwriter: writing padding: %w", err)
		}
	}

	return nil
}

// writeDataWithBlockHeaders writes data bytes, inserting block headers at
// 64 KiB boundaries as needed.
func (w *Writer) writeDataWithBlockHeaders(data []byte, chunkBegin, chunkEnd uint64) error {
	for len(data) > 0 {
		if block.IsBlockBoundary(w.pos) {
			if err := w.writeBlockHeaderAt(chunkBegin, chunkEnd); err != nil {
				return err
			}
		}

		remaining := block.RemainingInBlock(w.pos)
		if remaining == 0 {
			remaining = block.BlockSize
		}
		toWrite := uint64(len(data))
		if toWrite > remaining {
			toWrite = remaining
		}
		if err := w.writeRaw(data[:toWrite]); err != nil {
			return err
		}
		data = data[toWrite:]
	}
	return nil
}

// writeBlockHeaderAt writes a 24-byte block header at the current position.
func (w *Writer) writeBlockHeaderAt(chunkBegin, chunkEnd uint64) error {
	previousChunk := w.pos - chunkBegin
	var nextChunk uint64
	if chunkEnd > w.pos {
		nextChunk = chunkEnd - w.pos
	}
	bh := block.NewHeader(previousChunk, nextChunk)
	return w.writeRaw(bh.Bytes())
}

// writeRaw writes data directly to the output.
func (w *Writer) writeRaw(data []byte) error {
	n, err := w.dest.Write(data)
	w.pos += uint64(n)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// WriteFileSignature writes the initial file signature chunk at position 0.
func (w *Writer) WriteFileSignature() error {
	if w.pos != 0 {
		return fmt.Errorf("chunkwriter: file signature must be at position 0, currently at %d", w.pos)
	}

	sig := chunk.FileSignatureChunk()
	return w.WriteChunk(&sig)
}

// Flush flushes the underlying writer if it supports flushing.
func (w *Writer) Flush() error {
	if f, ok := w.dest.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

// Close finalizes the writer.
func (w *Writer) Close() error {
	return nil
}
