package riegeli

import (
	"fmt"
	"io"

	"github.com/google/riegeli-go/internal/chunk"
	"github.com/google/riegeli-go/internal/chunkwriter"
	"github.com/google/riegeli-go/internal/simple"
)

// RecordWriter writes records to a Riegeli/records file.
type RecordWriter struct {
	cw   *chunkwriter.Writer
	opts writerOptions

	pending         [][]byte // records buffered but not yet flushed
	pendingSize     int      // total byte size of pending records
	signatureWritten bool
	closed          bool
}

// NewRecordWriter creates a new RecordWriter writing to dest.
func NewRecordWriter(dest io.Writer, opts ...WriterOption) (*RecordWriter, error) {
	o := defaultWriterOptions()
	for _, opt := range opts {
		opt(&o)
	}

	w := &RecordWriter{
		cw:   chunkwriter.New(dest),
		opts: o,
	}

	// Write file signature.
	if err := w.cw.WriteFileSignature(); err != nil {
		return nil, fmt.Errorf("riegeli: writing file signature: %w", err)
	}
	w.signatureWritten = true

	return w, nil
}

// WriteRecord writes a single record.
func (w *RecordWriter) WriteRecord(record []byte) error {
	if w.closed {
		return fmt.Errorf("riegeli: writer is closed")
	}

	// Make a copy of the record.
	rec := make([]byte, len(record))
	copy(rec, record)
	w.pending = append(w.pending, rec)
	w.pendingSize += len(rec)

	// Flush if we've accumulated enough data.
	if w.pendingSize >= w.opts.chunkSize {
		return w.Flush()
	}
	return nil
}

// Flush writes all buffered records as a chunk.
func (w *RecordWriter) Flush() error {
	if len(w.pending) == 0 {
		return nil
	}

	// Encode records into a simple chunk.
	data, numRecords, decodedDataSize, err := simple.Encode(
		w.pending,
		w.opts.compressionType,
		w.opts.compressionLevel,
	)
	if err != nil {
		return fmt.Errorf("riegeli: encoding chunk: %w", err)
	}

	// Create chunk header.
	hdr := chunk.NewHeader(data, chunk.Simple, numRecords, decodedDataSize)
	c := &chunk.Chunk{Header: hdr, Data: data}

	if err := w.cw.WriteChunk(c); err != nil {
		return fmt.Errorf("riegeli: writing chunk: %w", err)
	}

	w.pending = w.pending[:0]
	w.pendingSize = 0
	return nil
}

// Pos returns the current write position.
func (w *RecordWriter) Pos() RecordPosition {
	return RecordPosition{
		ChunkBegin:  w.cw.Pos(),
		RecordIndex: uint64(len(w.pending)),
	}
}

// Close flushes any remaining records and finalizes the file.
func (w *RecordWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Flush remaining records.
	if err := w.Flush(); err != nil {
		return err
	}

	// Flush the underlying writer (e.g., bufio.Writer) if it supports flushing.
	if err := w.cw.Flush(); err != nil {
		return err
	}

	return w.cw.Close()
}
