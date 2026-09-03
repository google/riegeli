package simple

import (
	"bytes"
	"strings"
	"testing"

	"github.com/google/riegeli-go/internal/chunk"
)

func TestEncodeDecodeEmpty(t *testing.T) {
	data, numRecs, decodedSize, err := Encode(nil, chunk.NoCompression, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if numRecs != 0 {
		t.Errorf("numRecords = %d, want 0", numRecs)
	}
	if decodedSize != 0 {
		t.Errorf("decodedDataSize = %d, want 0", decodedSize)
	}

	records, err := Decode(data, 0, 0)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("got %d records, want 0", len(records))
	}
}

func TestEncodeDecodeSingle(t *testing.T) {
	input := [][]byte{[]byte("Hello")}
	data, numRecs, decodedSize, err := Encode(input, chunk.NoCompression, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if numRecs != 1 {
		t.Errorf("numRecords = %d, want 1", numRecs)
	}
	if decodedSize != 5 {
		t.Errorf("decodedDataSize = %d, want 5", decodedSize)
	}

	records, err := Decode(data, numRecs, decodedSize)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if string(records[0]) != "Hello" {
		t.Errorf("record[0] = %q, want %q", records[0], "Hello")
	}
}

func TestEncodeDecodeMultiple(t *testing.T) {
	input := [][]byte{
		[]byte("a"),
		[]byte("bc"),
		[]byte("def"),
	}
	data, numRecs, decodedSize, err := Encode(input, chunk.NoCompression, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if numRecs != 3 {
		t.Errorf("numRecords = %d, want 3", numRecs)
	}
	if decodedSize != 6 {
		t.Errorf("decodedDataSize = %d, want 6", decodedSize)
	}

	records, err := Decode(data, numRecs, decodedSize)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3", len(records))
	}
	for i, want := range input {
		if !bytes.Equal(records[i], want) {
			t.Errorf("record[%d] = %q, want %q", i, records[i], want)
		}
	}
}

func TestEncodeDecodeEmptyRecords(t *testing.T) {
	input := [][]byte{nil, nil, nil, []byte(""), nil}
	data, numRecs, decodedSize, err := Encode(input, chunk.NoCompression, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if numRecs != 5 {
		t.Errorf("numRecords = %d, want 5", numRecs)
	}
	if decodedSize != 0 {
		t.Errorf("decodedDataSize = %d, want 0", decodedSize)
	}

	records, err := Decode(data, numRecs, decodedSize)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 5 {
		t.Fatalf("got %d records, want 5", len(records))
	}
	for i, r := range records {
		if len(r) != 0 {
			t.Errorf("record[%d] has length %d, want 0", i, len(r))
		}
	}
}

func TestEncodeDecodeWithCompression(t *testing.T) {
	compressions := []struct {
		name  string
		ct    chunk.CompressionType
		level int
	}{
		{"none", chunk.NoCompression, 0},
		{"brotli", chunk.BrotliCompression, 6},
		{"zstd", chunk.ZstdCompression, 3},
		{"snappy", chunk.SnappyCompression, 0},
	}

	input := make([][]byte, 20)
	for i := range input {
		input[i] = []byte(strings.Repeat("record_", 100) + string(rune('A'+i%26)))
	}

	for _, tc := range compressions {
		t.Run(tc.name, func(t *testing.T) {
			data, numRecs, decodedSize, err := Encode(input, tc.ct, tc.level)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			records, err := Decode(data, numRecs, decodedSize)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if len(records) != len(input) {
				t.Fatalf("got %d records, want %d", len(records), len(input))
			}
			for i, want := range input {
				if !bytes.Equal(records[i], want) {
					t.Errorf("record[%d]: got %d bytes, want %d", i, len(records[i]), len(want))
				}
			}
		})
	}
}

func TestEncodeLargeRecords(t *testing.T) {
	// Records large enough to exercise the varint encoding for sizes.
	input := make([][]byte, 3)
	for i := range input {
		input[i] = make([]byte, 50000+i*10000)
		for j := range input[i] {
			input[i][j] = byte((i*50000 + j) % 256)
		}
	}

	data, numRecs, decodedSize, err := Encode(input, chunk.NoCompression, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	records, err := Decode(data, numRecs, decodedSize)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != len(input) {
		t.Fatalf("got %d records, want %d", len(records), len(input))
	}
	for i, want := range input {
		if !bytes.Equal(records[i], want) {
			t.Errorf("record[%d]: data mismatch (len %d vs %d)", i, len(records[i]), len(want))
		}
	}
}

func TestDecodeWrongNumRecords(t *testing.T) {
	input := [][]byte{[]byte("a"), []byte("b")}
	data, _, decodedSize, err := Encode(input, chunk.NoCompression, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Decode with wrong numRecords should fail.
	_, err = Decode(data, 3, decodedSize)
	if err == nil {
		t.Error("Decode with wrong numRecords should fail")
	}
}

func TestDecodeWrongDecodedDataSize(t *testing.T) {
	input := [][]byte{[]byte("abc")}
	data, numRecs, _, err := Encode(input, chunk.NoCompression, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Wrong decodedDataSize.
	_, err = Decode(data, numRecs, 999)
	if err == nil {
		t.Error("Decode with wrong decodedDataSize should fail")
	}
}

func TestDecodeEmptyDataNonZeroRecords(t *testing.T) {
	_, err := Decode(nil, 1, 0)
	if err == nil {
		t.Error("Decode with empty data and numRecords>0 should fail")
	}
}

func TestChunkDataFormat(t *testing.T) {
	// Verify the encoded format matches expectations.
	input := [][]byte{[]byte("Hello")}
	data, _, _, err := Encode(input, chunk.NoCompression, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// First byte: compression type (0 = none)
	if data[0] != 0 {
		t.Errorf("compression type = %d, want 0", data[0])
	}
	// Second byte: sizes_size varint (should be 1 for a single small record)
	// For "Hello" (5 bytes), sizes section = varint(5) = [0x05], so sizes_size = 1
	if data[1] != 1 {
		t.Errorf("sizes_size = %d, want 1", data[1])
	}
	// Third byte: the sizes section: varint(5) = 0x05
	if data[2] != 5 {
		t.Errorf("size[0] = %d, want 5", data[2])
	}
	// Remaining: "Hello"
	if string(data[3:]) != "Hello" {
		t.Errorf("values = %q, want %q", data[3:], "Hello")
	}
}

func TestDecodeNilDataZeroRecords(t *testing.T) {
	records, err := Decode(nil, 0, 0)
	if err != nil {
		t.Fatalf("Decode(nil, 0, 0) returned error: %v", err)
	}
	if records != nil {
		t.Errorf("Decode(nil, 0, 0) = %v, want nil", records)
	}
}

func TestDecodeTruncatedSizesSize(t *testing.T) {
	// compression_type=0, then 0x80 which is an incomplete varint (high bit set, no following byte).
	data := []byte{0x00, 0x80}
	_, err := Decode(data, 1, 5)
	if err == nil {
		t.Fatal("expected error for truncated sizes_size varint, got nil")
	}
	if !strings.Contains(err.Error(), "reading sizes_size") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "reading sizes_size")
	}
}

func TestDecodeDataTooShortForSizes(t *testing.T) {
	// compression_type=0, sizes_size=10 (varint 0x0A), but only 1 byte follows.
	data := []byte{0x00, 0x0A, 0x01}
	_, err := Decode(data, 1, 1)
	if err == nil {
		t.Fatal("expected error for data too short for sizes section, got nil")
	}
	if !strings.Contains(err.Error(), "data too short for sizes section") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "data too short for sizes section")
	}
}

func TestDecodeCumulativeSizeExceedsDecoded(t *testing.T) {
	// Encode two records "abc" and "def" (total decodedDataSize = 6).
	input := [][]byte{[]byte("abc"), []byte("def")}
	data, numRecs, _, err := Encode(input, chunk.NoCompression, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Decode with decodedDataSize=4, which is less than the cumulative 6.
	_, err = Decode(data, numRecs, 4)
	if err == nil {
		t.Fatal("expected error for cumulative size exceeding decoded_data_size, got nil")
	}
	if !strings.Contains(err.Error(), "cumulative size") || !strings.Contains(err.Error(), "exceeds decoded_data_size") {
		t.Errorf("error = %q, want it to contain 'cumulative size ... exceeds decoded_data_size'", err.Error())
	}
}

func TestDecodeExtraBytesInSizes(t *testing.T) {
	// Manually construct: compression_type=0, sizes_size=2, two size bytes [0x05, 0x00],
	// then 5 bytes of values "Hello".
	// With numRecords=1, only the first varint (0x05=5) is consumed, leaving 0x00 as extra.
	data := []byte{0x00, 0x02, 0x05, 0x00, 'H', 'e', 'l', 'l', 'o'}
	_, err := Decode(data, 1, 5)
	if err == nil {
		t.Fatal("expected error for extra bytes in sizes section, got nil")
	}
	if !strings.Contains(err.Error(), "extra bytes in sizes section") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "extra bytes in sizes section")
	}
}

func TestDecodeValuesSizeMismatch(t *testing.T) {
	// Encode ["Hello"] normally (decodedDataSize=5), then decode with decodedDataSize=10.
	// The sizes will sum to 5, but we claim decodedDataSize=10, so total size != decoded_data_size.
	// We need sizes that sum to decodedDataSize, so we manually construct the data.
	// compression_type=0, sizes_size=1, size varint=10, then 5 bytes "Hello".
	// cumulative=10 == decodedDataSize=10 ✓, but decompressed values length=5 != 10.
	data := []byte{0x00, 0x01, 0x0A, 'H', 'e', 'l', 'l', 'o'}
	_, err := Decode(data, 1, 10)
	if err == nil {
		t.Fatal("expected error for values size mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "decompressed values size") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "decompressed values size")
	}
}
