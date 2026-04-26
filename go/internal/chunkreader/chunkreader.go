// Package chunkreader reads chunks from a Riegeli file, transparently
// skipping block headers at 64 KiB boundaries.
package chunkreader

import (
	"fmt"
	"io"

	"github.com/google/riegeli-go/internal/block"
	"github.com/google/riegeli-go/internal/chunk"
)

// Reader reads chunks from a Riegeli file stream.
type Reader struct {
	src     io.Reader
	pos     uint64 // current file position (physical)
	skipBuf []byte // reusable buffer for skipBytes on non-seekable readers
}

// New creates a new chunk reader wrapping src.
func New(src io.Reader) *Reader {
	return &Reader{src: src}
}

// Pos returns the current file position.
func (r *Reader) Pos() uint64 {
	return r.pos
}

// ReadChunk reads the next chunk from the stream.
// Returns the chunk and the logical chunk begin position.
// Returns io.EOF at end of file.
func (r *Reader) ReadChunk() (*chunk.Chunk, uint64, error) {
	// Record chunk begin BEFORE skipping any block header.
	// In the Riegeli format, chunk_begin is the logical position which may
	// be a block boundary. The block header at that boundary is overhead.
	chunkBegin := r.pos

	// Read chunk header (40 bytes), skipping any intervening block headers.
	headerBuf := make([]byte, chunk.HeaderSize)
	if err := r.readSkippingBlockHeaders(headerBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, 0, io.EOF
		}
		return nil, 0, fmt.Errorf("chunkreader: reading chunk header: %w", err)
	}

	hdr := chunk.ParseHeader(headerBuf)

	// Read chunk data.
	dataSize := hdr.DataSize()
	// Sanity check: limit data_size to prevent OOM on corrupt headers.
	const maxDataSize = 1 << 30 // 1 GiB
	if dataSize > maxDataSize {
		return nil, 0, fmt.Errorf("chunkreader: data_size %d exceeds maximum %d", dataSize, maxDataSize)
	}
	data := make([]byte, dataSize)
	if dataSize > 0 {
		if err := r.readSkippingBlockHeaders(data); err != nil {
			return nil, 0, fmt.Errorf("chunkreader: reading chunk data: %w", err)
		}
	}

	// Advance past padding to next chunk boundary.
	chunkEnd := block.ChunkEnd(chunk.HeaderSize, hdr.DataSize(), hdr.NumRecords(), chunkBegin)
	if r.pos < chunkEnd {
		skip := chunkEnd - r.pos
		if err := r.skipBytes(skip); err != nil {
			// EOF after data is OK for the last chunk.
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				return nil, 0, fmt.Errorf("chunkreader: skipping padding: %w", err)
			}
		}
	}

	return &chunk.Chunk{Header: hdr, Data: data}, chunkBegin, nil
}

// readSkippingBlockHeaders reads exactly len(buf) bytes of data, transparently
// skipping 24-byte block headers at 64 KiB boundaries.
func (r *Reader) readSkippingBlockHeaders(buf []byte) error {
	for len(buf) > 0 {
		// If we're at a block boundary, skip the block header.
		if block.IsBlockBoundary(r.pos) {
			if err := r.skipBytes(block.HeaderSize); err != nil {
				return err
			}
		}

		// Read up to the next block boundary.
		remaining := block.RemainingInBlock(r.pos)
		if remaining == 0 {
			remaining = block.BlockSize
		}
		toRead := uint64(len(buf))
		if toRead > remaining {
			toRead = remaining
		}

		n, err := io.ReadFull(r.src, buf[:toRead])
		r.pos += uint64(n)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

// skipBytes discards n bytes from the underlying reader.
func (r *Reader) skipBytes(n uint64) error {
	if s, ok := r.src.(io.Seeker); ok {
		_, err := s.Seek(int64(n), io.SeekCurrent)
		if err != nil {
			return err
		}
		r.pos += n
		return nil
	}
	// Fall back to reading and discarding.
	if r.skipBuf == nil {
		r.skipBuf = make([]byte, 4096)
	}
	for n > 0 {
		toRead := n
		if toRead > uint64(len(r.skipBuf)) {
			toRead = uint64(len(r.skipBuf))
		}
		nn, err := r.src.Read(r.skipBuf[:toRead])
		r.pos += uint64(nn)
		n -= uint64(nn)
		if err != nil {
			return err
		}
	}
	return nil
}

// SeekToChunkContaining seeks to the chunk boundary containing or preceding pos.
// Requires the underlying reader to implement io.ReadSeeker.
func (r *Reader) SeekToChunkContaining(pos uint64) error {
	seeker, ok := r.src.(io.ReadSeeker)
	if !ok {
		return fmt.Errorf("chunkreader: seeking requires io.ReadSeeker")
	}

	// Find the block boundary at or before pos.
	blockBound := block.RoundDownToBlockBoundary(pos)

	// Read the block header at that boundary.
	if _, err := seeker.Seek(int64(blockBound), io.SeekStart); err != nil {
		return err
	}
	r.pos = blockBound

	if blockBound == 0 && pos == 0 {
		// Position 0 is the start of the file signature chunk.
		return nil
	}

	// Read the block header at the boundary.
	headerBuf := make([]byte, block.HeaderSize)
	if _, err := io.ReadFull(r.src, headerBuf); err != nil {
		return fmt.Errorf("chunkreader: reading block header for seek: %w", err)
	}
	r.pos += block.HeaderSize

	bh := block.Parse(headerBuf)
	if !bh.Valid() {
		return fmt.Errorf("chunkreader: invalid block header at %d", blockBound)
	}

	// Determine the chunk start from the block header.
	prevChunk := bh.PreviousChunk()
	nextChunk := bh.NextChunk()

	seekTo := func(target uint64) error {
		if _, err := seeker.Seek(int64(target), io.SeekStart); err != nil {
			return err
		}
		r.pos = target
		return nil
	}

	switch {
	case prevChunk == 0 && pos >= blockBound+nextChunk:
		return seekTo(blockBound + nextChunk)
	case prevChunk == 0:
		return seekTo(blockBound)
	case pos < blockBound+nextChunk:
		return seekTo(blockBound - prevChunk)
	default:
		return seekTo(blockBound + nextChunk)
	}
}
