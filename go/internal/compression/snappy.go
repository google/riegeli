package compression

import (
	"fmt"

	"github.com/klauspost/compress/snappy"
)

func decompressSnappy(data []byte, decompressedSize uint64) ([]byte, error) {
	out, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("snappy: decompress: %w", err)
	}
	return out, nil
}

func compressSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}
