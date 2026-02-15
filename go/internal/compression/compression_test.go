package compression

import (
	"bytes"
	"strings"
	"testing"

	"github.com/google/riegeli-go/internal/chunk"
	"github.com/google/riegeli-go/internal/varint"
)

func TestRoundTripNone(t *testing.T) {
	data := []byte("Hello, Riegeli!")
	compressed, err := Compress(chunk.NoCompression, data, 0)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	// NoCompression should return data unchanged.
	if !bytes.Equal(compressed, data) {
		t.Errorf("NoCompression should be identity")
	}

	decompressed, err := Decompress(chunk.NoCompression, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Errorf("round-trip failed")
	}
}

func TestRoundTripBrotli(t *testing.T) {
	testRoundTrip(t, chunk.BrotliCompression, 6)
}

func TestRoundTripZstd(t *testing.T) {
	testRoundTrip(t, chunk.ZstdCompression, 3)
}

func TestRoundTripSnappy(t *testing.T) {
	testRoundTrip(t, chunk.SnappyCompression, 0)
}

func testRoundTrip(t *testing.T, ct chunk.CompressionType, level int) {
	t.Helper()

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"small", []byte("Hello")},
		{"medium", []byte(strings.Repeat("abcdef", 1000))},
		{"binary", func() []byte {
			b := make([]byte, 10000)
			for i := range b {
				b[i] = byte(i % 256)
			}
			return b
		}()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := Compress(ct, tc.data, level)
			if err != nil {
				t.Fatalf("Compress: %v", err)
			}

			// For non-None compression, verify the decompressed_size prefix.
			if ct != chunk.NoCompression && len(tc.data) > 0 {
				size, err := DecompressedSize(ct, compressed)
				if err != nil {
					t.Fatalf("DecompressedSize: %v", err)
				}
				if size != uint64(len(tc.data)) {
					t.Errorf("DecompressedSize = %d, want %d", size, len(tc.data))
				}
			}

			decompressed, err := Decompress(ct, compressed)
			if err != nil {
				t.Fatalf("Decompress: %v", err)
			}
			if !bytes.Equal(decompressed, tc.data) {
				t.Errorf("round-trip failed: got %d bytes, want %d", len(decompressed), len(tc.data))
			}
		})
	}
}

func TestCompressedSmallerThanOriginal(t *testing.T) {
	// Highly compressible data should get smaller.
	data := []byte(strings.Repeat("a", 100000))

	types := []struct {
		name string
		ct   chunk.CompressionType
	}{
		{"brotli", chunk.BrotliCompression},
		{"zstd", chunk.ZstdCompression},
		{"snappy", chunk.SnappyCompression},
	}

	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := Compress(tc.ct, data, 0)
			if err != nil {
				t.Fatalf("Compress: %v", err)
			}
			if len(compressed) >= len(data) {
				t.Errorf("compressed size %d >= original size %d", len(compressed), len(data))
			}
		})
	}
}

func TestDecompressedSizeNone(t *testing.T) {
	data := []byte("test data")
	size, err := DecompressedSize(chunk.NoCompression, data)
	if err != nil {
		t.Fatalf("DecompressedSize: %v", err)
	}
	if size != uint64(len(data)) {
		t.Errorf("DecompressedSize = %d, want %d", size, len(data))
	}
}

func TestDecompressEmptyError(t *testing.T) {
	types := []chunk.CompressionType{
		chunk.BrotliCompression,
		chunk.ZstdCompression,
		chunk.SnappyCompression,
	}
	for _, ct := range types {
		_, err := Decompress(ct, nil)
		if err == nil {
			t.Errorf("Decompress(type=%d, nil) should fail", ct)
		}
	}
}

func TestUnknownCompressionType(t *testing.T) {
	_, err := Compress(chunk.CompressionType(0xFF), []byte("data"), 0)
	if err == nil {
		t.Error("Compress with unknown type should fail")
	}
	_, err = Decompress(chunk.CompressionType(0xFF), []byte{0x01, 0x00})
	if err == nil {
		t.Error("Decompress with unknown type should fail")
	}
}

func TestDecompressedSizeCompressed(t *testing.T) {
	data := []byte("Hello, world! This is a test of the decompressed size function.")
	types := []struct {
		name string
		ct   chunk.CompressionType
	}{
		{"brotli", chunk.BrotliCompression},
		{"zstd", chunk.ZstdCompression},
		{"snappy", chunk.SnappyCompression},
	}
	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := Compress(tc.ct, data, 0)
			if err != nil {
				t.Fatalf("Compress: %v", err)
			}
			size, err := DecompressedSize(tc.ct, compressed)
			if err != nil {
				t.Fatalf("DecompressedSize: %v", err)
			}
			if size != uint64(len(data)) {
				t.Errorf("DecompressedSize = %d, want %d", size, len(data))
			}
		})
	}
}

func TestDecompressedSizeEmptyError(t *testing.T) {
	_, err := DecompressedSize(chunk.BrotliCompression, nil)
	if err == nil {
		t.Error("DecompressedSize with empty data should fail")
	}
}

func TestDecompressedSizeInvalidVarint(t *testing.T) {
	// Create data with an overflowing varint (10 continuation bytes + 1).
	badData := make([]byte, 12)
	for i := 0; i < 10; i++ {
		badData[i] = 0x80
	}
	badData[10] = 0x02
	badData[11] = 0x00

	_, err := DecompressedSize(chunk.BrotliCompression, badData)
	if err == nil {
		t.Fatal("DecompressedSize with invalid varint should fail")
	}
	if !strings.Contains(err.Error(), "decompressed size") {
		t.Fatalf("expected 'decompressed size' error, got: %v", err)
	}
	t.Logf("got expected error: %v", err)
}

func TestCompressEmptyData(t *testing.T) {
	types := []struct {
		name string
		ct   chunk.CompressionType
	}{
		{"brotli", chunk.BrotliCompression},
		{"zstd", chunk.ZstdCompression},
		{"snappy", chunk.SnappyCompression},
	}
	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := Compress(tc.ct, nil, 0)
			if err != nil {
				t.Fatalf("Compress(nil): %v", err)
			}
			decompressed, err := Decompress(tc.ct, compressed)
			if err != nil {
				t.Fatalf("Decompress: %v", err)
			}
			if len(decompressed) != 0 {
				t.Errorf("decompressed len = %d, want 0", len(decompressed))
			}
		})
	}
}

func TestCompressDefaultLevel(t *testing.T) {
	data := []byte(strings.Repeat("test data for compression level testing ", 100))
	types := []struct {
		name string
		ct   chunk.CompressionType
	}{
		{"brotli", chunk.BrotliCompression},
		{"zstd", chunk.ZstdCompression},
	}
	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			// Level -1 means default.
			compressed, err := Compress(tc.ct, data, -1)
			if err != nil {
				t.Fatalf("Compress with level -1: %v", err)
			}
			decompressed, err := Decompress(tc.ct, compressed)
			if err != nil {
				t.Fatalf("Decompress: %v", err)
			}
			if !bytes.Equal(decompressed, data) {
				t.Error("round-trip failed with default level")
			}
		})
	}
}

func TestDecompressCorruptedBrotli(t *testing.T) {
	data := []byte(strings.Repeat("hello brotli corruption test ", 100))
	compressed, err := Compress(chunk.BrotliCompression, data, 6)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	// Skip past the varint prefix and heavily corrupt the compressed bytes.
	_, n, err := varint.Uvarint64(compressed)
	if err != nil {
		t.Fatalf("Uvarint64: %v", err)
	}
	// Corrupt multiple bytes throughout the compressed payload.
	for i := n; i < len(compressed); i++ {
		compressed[i] ^= 0xFF
	}
	_, err = Decompress(chunk.BrotliCompression, compressed)
	if err == nil {
		t.Fatal("Decompress of corrupted brotli data should fail")
	}
	t.Logf("got expected error: %v", err)
}

func TestDecompressCorruptedZstd(t *testing.T) {
	data := []byte(strings.Repeat("hello zstd corruption test ", 100))
	compressed, err := Compress(chunk.ZstdCompression, data, 3)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	// Skip past the varint prefix and corrupt the compressed bytes.
	_, n, err := varint.Uvarint64(compressed)
	if err != nil {
		t.Fatalf("Uvarint64: %v", err)
	}
	if n+2 < len(compressed) {
		compressed[n+2] ^= 0xFF
	}
	_, err = Decompress(chunk.ZstdCompression, compressed)
	if err == nil {
		t.Fatal("Decompress of corrupted zstd data should fail")
	}
	t.Logf("got expected error: %v", err)
}

func TestDecompressCorruptedSnappy(t *testing.T) {
	data := []byte(strings.Repeat("hello snappy corruption test ", 100))
	compressed, err := Compress(chunk.SnappyCompression, data, 0)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	// Skip past the varint prefix and corrupt the compressed bytes.
	_, n, err := varint.Uvarint64(compressed)
	if err != nil {
		t.Fatalf("Uvarint64: %v", err)
	}
	if n+2 < len(compressed) {
		compressed[n+2] ^= 0xFF
	}
	_, err = Decompress(chunk.SnappyCompression, compressed)
	if err == nil {
		t.Fatal("Decompress of corrupted snappy data should fail")
	}
	t.Logf("got expected error: %v", err)
}

func TestDecompressSizeMismatch(t *testing.T) {
	// Use snappy because snappy.Decode returns actual decompressed data
	// regardless of the declared size, which triggers the size mismatch
	// check in Decompress().
	data := []byte("size mismatch test data")
	compressed, err := Compress(chunk.SnappyCompression, data, 0)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	// Extract the compressed payload (after varint prefix).
	_, n, err := varint.Uvarint64(compressed)
	if err != nil {
		t.Fatalf("Uvarint64: %v", err)
	}
	payload := compressed[n:]

	// Build new data with a wrong decompressed_size (original + 100).
	// Snappy will decompress to len(data) bytes, but the prefix says
	// len(data)+100, causing the mismatch check to fire.
	var newPrefix [varint.MaxLenVarint64]byte
	nn := varint.PutUvarint64(newPrefix[:], uint64(len(data))+100)
	newData := make([]byte, nn+len(payload))
	copy(newData, newPrefix[:nn])
	copy(newData[nn:], payload)

	_, err = Decompress(chunk.SnappyCompression, newData)
	if err == nil {
		t.Fatal("Decompress with wrong decompressed_size should fail")
	}
	if !strings.Contains(err.Error(), "decompressed size mismatch") {
		t.Fatalf("expected 'decompressed size mismatch' error, got: %v", err)
	}
	t.Logf("got expected error: %v", err)
}

func TestDecompressInvalidVarint(t *testing.T) {
	// Create data that starts with an invalid varint: 10 bytes all with MSB set.
	badData := make([]byte, 12)
	for i := 0; i < 10; i++ {
		badData[i] = 0x80
	}
	badData[10] = 0x01
	badData[11] = 0x00

	_, err := Decompress(chunk.BrotliCompression, badData)
	if err == nil {
		t.Fatal("Decompress with invalid varint should fail")
	}
	if !strings.Contains(err.Error(), "decompressed size") {
		t.Fatalf("expected 'decompressed size' error, got: %v", err)
	}
	t.Logf("got expected error: %v", err)
}

func TestDecompressBrotliShortRead(t *testing.T) {
	// Compress a small amount of data with brotli.
	data := []byte("short")
	compressed, err := Compress(chunk.BrotliCompression, data, 6)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	// Extract the compressed payload.
	_, n, err := varint.Uvarint64(compressed)
	if err != nil {
		t.Fatalf("Uvarint64: %v", err)
	}
	payload := compressed[n:]

	// Declare a much larger decompressed_size to trigger short read.
	var newPrefix [varint.MaxLenVarint64]byte
	nn := varint.PutUvarint64(newPrefix[:], 100000)
	newData := make([]byte, nn+len(payload))
	copy(newData, newPrefix[:nn])
	copy(newData[nn:], payload)

	_, err = Decompress(chunk.BrotliCompression, newData)
	if err == nil {
		t.Fatal("Decompress with inflated decompressed_size should fail")
	}
	t.Logf("got expected error: %v", err)
}

func TestDecompressZstdShortRead(t *testing.T) {
	// Compress a small amount of data with zstd.
	data := []byte("short")
	compressed, err := Compress(chunk.ZstdCompression, data, 3)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	// Extract the compressed payload.
	_, n, err := varint.Uvarint64(compressed)
	if err != nil {
		t.Fatalf("Uvarint64: %v", err)
	}
	payload := compressed[n:]

	// Declare a much larger decompressed_size to trigger short read.
	var newPrefix [varint.MaxLenVarint64]byte
	nn := varint.PutUvarint64(newPrefix[:], 100000)
	newData := make([]byte, nn+len(payload))
	copy(newData, newPrefix[:nn])
	copy(newData[nn:], payload)

	_, err = Decompress(chunk.ZstdCompression, newData)
	if err == nil {
		t.Fatal("Decompress with inflated decompressed_size should fail")
	}
	t.Logf("got expected error: %v", err)
}
