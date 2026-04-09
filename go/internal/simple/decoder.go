// Package simple provides encoding and decoding of simple ('r') chunks.
//
// A simple chunk data section has this format:
//
//	compression_type (1 byte)
//	sizes_size (varint64) - total compressed size of sizes section (including decompressed_size prefix if compressed)
//	sizes_data (sizes_size bytes) - [varint64(decompressed_size)] compressed record sizes
//	values_data (remaining bytes) - [varint64(decompressed_size)] compressed record values
//
// The sizes section contains num_records varint64 values, each the byte length
// of the corresponding record. The values section is the concatenation of all
// record byte contents.
package simple

import (
	"fmt"

	"github.com/google/riegeli-go/internal/chunk"
	"github.com/google/riegeli-go/internal/compression"
	"github.com/google/riegeli-go/internal/varint"
)

const (
	maxDecodedDataSize = 1 << 30  // 1 GiB
	maxNumRecords      = 10 << 20 // ~10 million; limits [][]byte overhead to ~240 MiB
)

// Decode decodes a simple chunk into individual records.
// data is the chunk data (after the 40-byte chunk header).
// numRecords and decodedDataSize come from the chunk header.
func Decode(data []byte, numRecords, decodedDataSize uint64) ([][]byte, error) {
	if numRecords > maxNumRecords {
		return nil, fmt.Errorf("simple: num_records %d exceeds maximum %d", numRecords, maxNumRecords)
	}
	if decodedDataSize > maxDecodedDataSize {
		return nil, fmt.Errorf("simple: decoded_data_size %d exceeds maximum %d", decodedDataSize, maxDecodedDataSize)
	}
	if len(data) == 0 {
		if numRecords == 0 && decodedDataSize == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("simple: empty data for non-empty chunk")
	}

	// Read compression type.
	ct := chunk.CompressionType(data[0])
	data = data[1:]

	// Read sizes_size.
	sizesSize, n, err := varint.Uvarint64(data)
	if err != nil {
		return nil, fmt.Errorf("simple: reading sizes_size: %w", err)
	}
	data = data[n:]

	if uint64(len(data)) < sizesSize {
		return nil, fmt.Errorf("simple: data too short for sizes section: have %d, need %d", len(data), sizesSize)
	}

	// Decompress sizes.
	sizesCompressed := data[:sizesSize]
	valuesCompressed := data[sizesSize:]

	sizesRaw, err := compression.Decompress(ct, sizesCompressed)
	if err != nil {
		return nil, fmt.Errorf("simple: decompressing sizes: %w", err)
	}

	// Parse record sizes.
	limits := make([]uint64, 0, numRecords)
	var cumulative uint64
	remaining := sizesRaw
	for i := uint64(0); i < numRecords; i++ {
		sz, nn, err := varint.Uvarint64(remaining)
		if err != nil {
			return nil, fmt.Errorf("simple: reading record size %d: %w", i, err)
		}
		remaining = remaining[nn:]
		cumulative += sz
		if cumulative > decodedDataSize {
			return nil, fmt.Errorf("simple: cumulative size %d exceeds decoded_data_size %d", cumulative, decodedDataSize)
		}
		limits = append(limits, cumulative)
	}
	if len(remaining) != 0 {
		return nil, fmt.Errorf("simple: %d extra bytes in sizes section", len(remaining))
	}
	if cumulative != decodedDataSize {
		return nil, fmt.Errorf("simple: total size %d != decoded_data_size %d", cumulative, decodedDataSize)
	}

	// Decompress values.
	valuesRaw, err := compression.Decompress(ct, valuesCompressed)
	if err != nil {
		return nil, fmt.Errorf("simple: decompressing values: %w", err)
	}
	if uint64(len(valuesRaw)) != decodedDataSize {
		return nil, fmt.Errorf("simple: decompressed values size %d != decoded_data_size %d", len(valuesRaw), decodedDataSize)
	}

	// Split values into records.
	records := make([][]byte, numRecords)
	var prev uint64
	for i, limit := range limits {
		records[i] = valuesRaw[prev:limit]
		prev = limit
	}
	return records, nil
}
