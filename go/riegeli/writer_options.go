package riegeli

import "github.com/google/riegeli-go/internal/chunk"

// WriterOption configures a RecordWriter.
type WriterOption func(*writerOptions)

type writerOptions struct {
	compressionType  chunk.CompressionType
	compressionLevel int
	transpose        bool
	chunkSize        int
}

func defaultWriterOptions() writerOptions {
	return writerOptions{
		compressionType:  chunk.NoCompression,
		compressionLevel: -1, // use default
		transpose:        false,
		chunkSize:        1 << 20, // 1 MiB
	}
}

// WithCompression sets the compression algorithm.
func WithCompression(ct CompressionType) WriterOption {
	return func(o *writerOptions) {
		o.compressionType = ct
	}
}

// WithCompressionLevel sets the compression level.
// The meaning depends on the compression algorithm.
func WithCompressionLevel(level int) WriterOption {
	return func(o *writerOptions) {
		o.compressionLevel = level
	}
}

// WithTranspose enables Protocol Buffer transposition for better compression.
func WithTranspose(enable bool) WriterOption {
	return func(o *writerOptions) {
		o.transpose = enable
	}
}

// WithChunkSize sets the target chunk size in bytes before compression.
func WithChunkSize(size int) WriterOption {
	return func(o *writerOptions) {
		o.chunkSize = size
	}
}
