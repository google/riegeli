package compression

import (
	"fmt"

	"github.com/klauspost/compress/snappy"
)

func decompressSnappy(data []byte, decompressedSize uint64) ([]byte, error) {
	// Validate snappy's declared length matches our capped decompressedSize
	// before allocating, to prevent allocation bombs via the snappy header.
	dLen, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, fmt.Errorf("snappy: decoded len: %w", err)
	}
	if uint64(dLen) != decompressedSize {
		return nil, fmt.Errorf("snappy: snappy length %d != riegeli decompressed_size %d", dLen, decompressedSize)
	}
	out := make([]byte, decompressedSize)
	out, err = snappy.Decode(out, data)
	if err != nil {
		return nil, fmt.Errorf("snappy: decompress: %w", err)
	}
	return out, nil
}

func compressSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}
