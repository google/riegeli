package riegeli

import (
	"fmt"
	"io"

	"github.com/google/riegeli-go/internal/chunk"
	"github.com/google/riegeli-go/internal/chunkreader"
	"github.com/google/riegeli-go/internal/simple"
	"github.com/google/riegeli-go/internal/transpose"
)

// RecordReader reads records from a Riegeli/records file.
type RecordReader struct {
	cr   *chunkreader.Reader
	opts readerOptions

	// Current chunk state.
	records    [][]byte // decoded records from current chunk
	recordIdx  int      // index into records
	chunkBegin uint64   // file position of current chunk

	signatureVerified bool
	closed            bool
	err               error // sticky error
}

// NewRecordReader creates a new RecordReader reading from src.
func NewRecordReader(src io.Reader, opts ...ReaderOption) (*RecordReader, error) {
	o := defaultReaderOptions()
	for _, opt := range opts {
		opt(&o)
	}

	r := &RecordReader{
		cr:   chunkreader.New(src),
		opts: o,
	}

	// Read and verify file signature.
	if err := r.readFileSignature(); err != nil {
		return nil, err
	}

	return r, nil
}

// readFileSignature reads and verifies the file signature chunk.
func (r *RecordReader) readFileSignature() error {
	c, _, err := r.cr.ReadChunk()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSignature, err)
	}

	if c.Header.Type() != chunk.FileSignature {
		return fmt.Errorf("%w: first chunk type is '%c', expected 's'", ErrInvalidSignature, c.Header.Type())
	}

	if r.opts.verifyHashes {
		if !c.Header.ValidHeader() {
			return fmt.Errorf("%w: file signature header hash mismatch", ErrCorrupted)
		}
	}

	r.signatureVerified = true
	return nil
}

// ReadRecord reads the next record from the file.
// Returns io.EOF when all records have been read.
func (r *RecordReader) ReadRecord() ([]byte, error) {
	if r.closed {
		return nil, fmt.Errorf("riegeli: reader is closed")
	}
	if r.err != nil {
		return nil, r.err
	}

	for {
		// Return records from current chunk.
		if r.recordIdx < len(r.records) {
			rec := r.records[r.recordIdx]
			r.recordIdx++
			return rec, nil
		}

		// Read next chunk.
		if err := r.readNextDataChunk(); err != nil {
			return nil, err
		}
	}
}

// readNextDataChunk reads the next data chunk (simple or transposed),
// skipping metadata and padding chunks.
func (r *RecordReader) readNextDataChunk() error {
	for {
		c, chunkBegin, err := r.cr.ReadChunk()
		if err != nil {
			if err == io.EOF {
				r.err = io.EOF
				return io.EOF
			}
			r.err = err
			return err
		}

		// Verify hashes.
		if r.opts.verifyHashes {
			if !c.Header.ValidHeader() {
				return r.handleCorruption(chunkBegin, fmt.Errorf("%w: chunk header hash mismatch at %d", ErrCorrupted, chunkBegin))
			}
			if len(c.Data) > 0 && !c.Header.ValidData(c.Data) {
				return r.handleCorruption(chunkBegin, fmt.Errorf("%w: chunk data hash mismatch at %d", ErrCorrupted, chunkBegin))
			}
		}

		switch c.Header.Type() {
		case chunk.Simple:
			records, err := simple.Decode(c.Data, c.Header.NumRecords(), c.Header.DecodedDataSize())
			if err != nil {
				return r.handleCorruption(chunkBegin, fmt.Errorf("%w: simple chunk decode at %d: %v", ErrCorrupted, chunkBegin, err))
			}
			r.records = records
			r.recordIdx = 0
			r.chunkBegin = chunkBegin
			return nil

		case chunk.Transposed:
			records, err := transpose.Decode(c.Data, c.Header.NumRecords(), c.Header.DecodedDataSize())
			if err != nil {
				return r.handleCorruption(chunkBegin, fmt.Errorf("%w: transposed chunk decode at %d: %v", ErrCorrupted, chunkBegin, err))
			}
			r.records = records
			r.recordIdx = 0
			r.chunkBegin = chunkBegin
			return nil

		case chunk.FileMetadata, chunk.Padding, chunk.FileSignature:
			// Skip metadata, padding, and duplicate signatures.
			continue

		default:
			return r.handleCorruption(chunkBegin, fmt.Errorf("%w: chunk type '%c' at %d", ErrUnsupportedChunkType, c.Header.Type(), chunkBegin))
		}
	}
}

// handleCorruption handles a corrupted chunk. If recovery is configured,
// it notifies the callback and tries to continue. Otherwise, returns the error.
func (r *RecordReader) handleCorruption(chunkBegin uint64, err error) error {
	if r.opts.recovery != nil {
		r.opts.recovery(SkippedRegion{Begin: chunkBegin, End: r.cr.Pos()})
		return nil
	}
	r.err = err
	return err
}

// Pos returns the position of the next record to be read.
func (r *RecordReader) Pos() RecordPosition {
	return RecordPosition{
		ChunkBegin:  r.chunkBegin,
		RecordIndex: uint64(r.recordIdx),
	}
}

// Seek seeks to the given record position.
// Requires the underlying reader to implement io.ReadSeeker.
func (r *RecordReader) Seek(pos RecordPosition) error {
	if err := r.cr.SeekToChunkContaining(pos.ChunkBegin); err != nil {
		return fmt.Errorf("riegeli: seek: %w", err)
	}

	// Read the chunk at the seek position.
	r.records = nil
	r.recordIdx = 0
	r.err = nil

	if err := r.readNextDataChunk(); err != nil {
		return err
	}

	// Skip to the requested record index.
	if pos.RecordIndex > uint64(len(r.records)) {
		return fmt.Errorf("riegeli: record index %d out of range (chunk has %d records)", pos.RecordIndex, len(r.records))
	}
	r.recordIdx = int(pos.RecordIndex)
	return nil
}

// SeekNumeric seeks to a position specified as a single numeric value.
// This is equivalent to Seek(RecordPosition{ChunkBegin: pos, RecordIndex: 0})
// when pos is a chunk boundary.
func (r *RecordReader) SeekNumeric(pos uint64) error {
	return r.Seek(RecordPosition{ChunkBegin: pos})
}

// Close closes the reader.
func (r *RecordReader) Close() error {
	r.closed = true
	return nil
}
