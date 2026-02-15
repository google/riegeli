package compression

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

func decompressZstd(data []byte, decompressedSize uint64) ([]byte, error) {
	dec, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("zstd: new reader: %w", err)
	}
	defer dec.Close()
	out := make([]byte, decompressedSize)
	n, err := readFull(dec, out)
	if err != nil {
		return nil, fmt.Errorf("zstd: decompress: %w", err)
	}
	if uint64(n) != decompressedSize {
		return nil, fmt.Errorf("zstd: short read: got %d, want %d", n, decompressedSize)
	}
	return out, nil
}

func compressZstd(data []byte, level int) ([]byte, error) {
	el := zstd.SpeedDefault
	if level > 0 {
		el = zstd.EncoderLevel(level)
	}
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(el))
	if err != nil {
		return nil, fmt.Errorf("zstd: new writer: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("zstd: compress: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("zstd: close: %w", err)
	}
	return buf.Bytes(), nil
}
