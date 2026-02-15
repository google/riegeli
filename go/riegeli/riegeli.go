// Package riegeli provides reading and writing of Riegeli/records files.
//
// Riegeli is a high-performance binary record file format designed for
// sequential and random-access reading of Protocol Buffer records, with
// support for compression and columnar encoding (transposition).
//
// # Basic Usage
//
//	// Writing
//	w, err := riegeli.NewRecordWriter(file)
//	w.WriteRecord([]byte("hello"))
//	w.Close()
//
//	// Reading
//	r, err := riegeli.NewRecordReader(file)
//	for {
//	    record, err := r.ReadRecord()
//	    if err == io.EOF { break }
//	}
//	r.Close()
package riegeli

import (
	"github.com/google/riegeli-go/internal/chunk"
)

// CompressionType specifies the compression algorithm used for writing.
type CompressionType = chunk.CompressionType

const (
	// NoCompression writes records without compression.
	NoCompression = chunk.NoCompression
	// BrotliCompression uses Brotli compression.
	BrotliCompression = chunk.BrotliCompression
	// ZstdCompression uses Zstandard compression.
	ZstdCompression = chunk.ZstdCompression
	// SnappyCompression uses Snappy compression.
	SnappyCompression = chunk.SnappyCompression
)
