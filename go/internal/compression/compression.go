// Package compression provides Riegeli compression and decompression.
//
// The compressed format for non-None types is:
//
//	<decompressed_size: varint64><compressed_data: bytes>
package compression

import (
	"fmt"

	"github.com/google/riegeli-go/internal/chunk"
	"github.com/google/riegeli-go/internal/varint"
)

// MaxDecompressedSize is the maximum allowed decompressed size (1 GiB).
// This prevents allocation bombs from malicious size fields.
const MaxDecompressedSize = 1 << 30

// Decompress decompresses data according to the given compression type.
// For non-None types, the data starts with a varint64 decompressed_size
// followed by the compressed bytes.
func Decompress(ct chunk.CompressionType, data []byte) ([]byte, error) {
	if ct == chunk.NoCompression {
		return data, nil
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("compression: empty compressed data")
	}

	// Read decompressed size prefix.
	decompressedSize, n, err := varint.Uvarint64(data)
	if err != nil {
		return nil, fmt.Errorf("compression: reading decompressed size: %w", err)
	}
	if decompressedSize > MaxDecompressedSize {
		return nil, fmt.Errorf("compression: decompressed size %d exceeds maximum %d", decompressedSize, MaxDecompressedSize)
	}
	compressed := data[n:]

	var decompressed []byte
	switch ct {
	case chunk.BrotliCompression:
		decompressed, err = decompressBrotli(compressed, decompressedSize)
	case chunk.ZstdCompression:
		decompressed, err = decompressZstd(compressed, decompressedSize)
	case chunk.SnappyCompression:
		decompressed, err = decompressSnappy(compressed, decompressedSize)
	default:
		return nil, fmt.Errorf("compression: unknown compression type: %d", ct)
	}
	if err != nil {
		return nil, err
	}
	if uint64(len(decompressed)) != decompressedSize {
		return nil, fmt.Errorf("compression: decompressed size mismatch: got %d, want %d", len(decompressed), decompressedSize)
	}
	return decompressed, nil
}

// Compress compresses data according to the given compression type and level.
// For non-None types, the output is prefixed with varint64 decompressed_size.
func Compress(ct chunk.CompressionType, data []byte, level int) ([]byte, error) {
	if ct == chunk.NoCompression {
		return data, nil
	}

	var compressed []byte
	var err error
	switch ct {
	case chunk.BrotliCompression:
		compressed, err = compressBrotli(data, level)
	case chunk.ZstdCompression:
		compressed, err = compressZstd(data, level)
	case chunk.SnappyCompression:
		compressed, err = compressSnappy(data)
	default:
		return nil, fmt.Errorf("compression: unknown compression type: %d", ct)
	}
	if err != nil {
		return nil, err
	}

	// Prefix with decompressed size.
	var buf [varint.MaxLenVarint64]byte
	n := varint.PutUvarint64(buf[:], uint64(len(data)))
	result := make([]byte, n+len(compressed))
	copy(result, buf[:n])
	copy(result[n:], compressed)
	return result, nil
}

// DecompressedSize returns the decompressed size from compressed data.
// For NoCompression, returns len(data).
func DecompressedSize(ct chunk.CompressionType, data []byte) (uint64, error) {
	if ct == chunk.NoCompression {
		return uint64(len(data)), nil
	}
	if len(data) == 0 {
		return 0, fmt.Errorf("compression: empty compressed data")
	}
	size, _, err := varint.Uvarint64(data)
	if err != nil {
		return 0, fmt.Errorf("compression: reading decompressed size: %w", err)
	}
	return size, nil
}

