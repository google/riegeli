package compression

import (
	"bytes"
	"fmt"

	"github.com/andybalholm/brotli"
)

func decompressBrotli(data []byte, decompressedSize uint64) ([]byte, error) {
	r := brotli.NewReader(bytes.NewReader(data))
	out := make([]byte, decompressedSize)
	n, err := readFull(r, out)
	if err != nil {
		return nil, fmt.Errorf("brotli: decompress: %w", err)
	}
	if uint64(n) != decompressedSize {
		return nil, fmt.Errorf("brotli: short read: got %d, want %d", n, decompressedSize)
	}
	return out, nil
}

func compressBrotli(data []byte, level int) ([]byte, error) {
	if level < 0 {
		level = brotli.DefaultCompression
	}
	var buf bytes.Buffer
	w := brotli.NewWriterLevel(&buf, level)
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("brotli: compress: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("brotli: close: %w", err)
	}
	return buf.Bytes(), nil
}
