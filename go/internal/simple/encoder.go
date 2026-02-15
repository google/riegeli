package simple

import (
	"fmt"

	"github.com/google/riegeli-go/internal/chunk"
	"github.com/google/riegeli-go/internal/compression"
	"github.com/google/riegeli-go/internal/varint"
)

// Encode encodes records into simple chunk data.
// Returns the chunk data, the number of records, and the decoded data size.
func Encode(records [][]byte, ct chunk.CompressionType, level int) ([]byte, uint64, uint64, error) {
	numRecords := uint64(len(records))

	// Build sizes buffer.
	var decodedDataSize uint64
	sizesLen := 0
	for _, r := range records {
		sizesLen += varint.LenUvarint64(uint64(len(r)))
		decodedDataSize += uint64(len(r))
	}

	sizesBuf := make([]byte, sizesLen)
	off := 0
	for _, r := range records {
		off += varint.PutUvarint64(sizesBuf[off:], uint64(len(r)))
	}

	// Build values buffer (concatenation of all records).
	valuesBuf := make([]byte, decodedDataSize)
	off = 0
	for _, r := range records {
		copy(valuesBuf[off:], r)
		off += len(r)
	}

	// Compress sizes.
	sizesCompressed, err := compression.Compress(ct, sizesBuf, level)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("simple: compressing sizes: %w", err)
	}

	// Compress values.
	valuesCompressed, err := compression.Compress(ct, valuesBuf, level)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("simple: compressing values: %w", err)
	}

	// Build chunk data:
	// compression_type (1 byte) + sizes_size (varint64) + sizes_data + values_data
	sizesSizeLen := varint.LenUvarint64(uint64(len(sizesCompressed)))
	totalLen := 1 + sizesSizeLen + len(sizesCompressed) + len(valuesCompressed)
	result := make([]byte, totalLen)

	result[0] = byte(ct)
	n := varint.PutUvarint64(result[1:], uint64(len(sizesCompressed)))
	copy(result[1+n:], sizesCompressed)
	copy(result[1+n+len(sizesCompressed):], valuesCompressed)

	return result, numRecords, decodedDataSize, nil
}
