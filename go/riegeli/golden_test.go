package riegeli_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/riegeli-go/riegeli"
)

const goldenDir = "../testdata/golden"

func readAllRecords(t *testing.T, path string) [][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open %s: %v", path, err)
	}
	defer f.Close()

	r, err := riegeli.NewRecordReader(f)
	if err != nil {
		t.Fatalf("NewRecordReader %s: %v", path, err)
	}
	defer r.Close()

	var records [][]byte
	for {
		rec, err := r.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadRecord %s (after %d records): %v", path, len(records), err)
		}
		cp := make([]byte, len(rec))
		copy(cp, rec)
		records = append(records, cp)
	}
	return records
}

func TestGoldenEmpty(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "empty.riegeli"))
	if len(records) != 0 {
		t.Errorf("empty.riegeli: got %d records, want 0", len(records))
	}
}

func TestGoldenSingleString(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "single_string_uncompressed.riegeli"))
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if string(records[0]) != "Hello" {
		t.Errorf("record[0] = %q, want %q", records[0], "Hello")
	}
}

func TestGoldenThreeStrings(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "three_strings_uncompressed.riegeli"))
	want := []string{"a", "bc", "def"}
	if len(records) != len(want) {
		t.Fatalf("got %d records, want %d", len(records), len(want))
	}
	for i, w := range want {
		if string(records[i]) != w {
			t.Errorf("record[%d] = %q, want %q", i, records[i], w)
		}
	}
}

func TestGoldenMultiRecordsUncompressed(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "multi_records_uncompressed.riegeli"))
	if len(records) != 23 {
		t.Fatalf("got %d records, want 23", len(records))
	}
	for i, rec := range records {
		want := strings.Repeat("record_data_", 100) + string(rune('A'+i%26))
		if string(rec) != want {
			t.Errorf("record[%d]: got len %d, want len %d", i, len(rec), len(want))
		}
	}
}

func TestGoldenMultiRecordsBrotli(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "multi_records_brotli.riegeli"))
	if len(records) != 23 {
		t.Fatalf("got %d records, want 23", len(records))
	}
	for i, rec := range records {
		want := strings.Repeat("record_data_", 100) + string(rune('A'+i%26))
		if string(rec) != want {
			t.Errorf("record[%d]: got len %d, want len %d", i, len(rec), len(want))
		}
	}
}

func TestGoldenMultiRecordsZstd(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "multi_records_zstd.riegeli"))
	if len(records) != 23 {
		t.Fatalf("got %d records, want 23", len(records))
	}
	for i, rec := range records {
		want := strings.Repeat("record_data_", 100) + string(rune('A'+i%26))
		if string(rec) != want {
			t.Errorf("record[%d]: got len %d, want len %d", i, len(rec), len(want))
		}
	}
}

func TestGoldenMultiRecordsSnappy(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "multi_records_snappy.riegeli"))
	if len(records) != 23 {
		t.Fatalf("got %d records, want 23", len(records))
	}
	for i, rec := range records {
		want := strings.Repeat("record_data_", 100) + string(rune('A'+i%26))
		if string(rec) != want {
			t.Errorf("record[%d]: got len %d, want len %d", i, len(rec), len(want))
		}
	}
}

func TestGoldenBlockSpanning(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "block_spanning.riegeli"))
	if len(records) != 5 {
		t.Fatalf("got %d records, want 5", len(records))
	}
	for i, rec := range records {
		want := make([]byte, 50000)
		for j := range want {
			want[j] = byte((i*50000 + j) % 256)
		}
		if !bytes.Equal(rec, want) {
			t.Errorf("record[%d]: data mismatch (len %d vs %d)", i, len(rec), len(want))
		}
	}
}

func TestGoldenEmptyRecords(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "empty_records.riegeli"))
	if len(records) != 10 {
		t.Fatalf("got %d records, want 10", len(records))
	}
	for i, rec := range records {
		if len(rec) != 0 {
			t.Errorf("record[%d]: got len %d, want 0", i, len(rec))
		}
	}
}

func TestGoldenLargeRecord(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "large_record.riegeli"))
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := make([]byte, 1<<20)
	for i := range want {
		want[i] = byte(i % 256)
	}
	if !bytes.Equal(records[0], want) {
		t.Errorf("large record data mismatch (len %d vs %d)", len(records[0]), len(want))
	}
}

func TestGoldenManyRecords(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "many_records.riegeli"))
	if len(records) != 1000 {
		t.Fatalf("got %d records, want 1000", len(records))
	}
	for i, rec := range records {
		want := strings.Repeat("x", i%100)
		if string(rec) != want {
			t.Errorf("record[%d]: got len %d, want len %d", i, len(rec), len(want))
		}
	}
}

func TestGoldenTransposedNonProtoBrotli(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "transposed_nonproto_brotli.riegeli"))
	want := []string{"alpha", "beta", "gamma", "delta"}
	if len(records) != len(want) {
		t.Fatalf("got %d records, want %d", len(records), len(want))
	}
	for i, w := range want {
		if string(records[i]) != w {
			t.Errorf("record[%d] = %q, want %q", i, records[i], w)
		}
	}
}

func TestGoldenTransposedNonProtoZstd(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "transposed_nonproto_zstd.riegeli"))
	want := []string{"one", "two", "three"}
	if len(records) != len(want) {
		t.Fatalf("got %d records, want %d", len(records), len(want))
	}
	for i, w := range want {
		if string(records[i]) != w {
			t.Errorf("record[%d] = %q, want %q", i, records[i], w)
		}
	}
}

func TestGoldenTransposedNonProtoUncompressed(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "transposed_nonproto_uncompressed.riegeli"))
	want := []string{"Hello", "World", "from", "transposed", "chunk"}
	if len(records) != len(want) {
		t.Fatalf("got %d records, want %d", len(records), len(want))
	}
	for i, w := range want {
		if string(records[i]) != w {
			t.Errorf("record[%d] = %q, want %q", i, records[i], w)
		}
	}
}

func TestGoldenTransposedMultiBrotli(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "transposed_multi_brotli.riegeli"))
	if len(records) != 23 {
		t.Fatalf("got %d records, want 23", len(records))
	}
	for i, rec := range records {
		want := strings.Repeat("record_data_", 100) + string(rune('A'+i%26))
		if string(rec) != want {
			t.Errorf("record[%d]: got len %d, want len %d", i, len(rec), len(want))
		}
	}
}

func TestGoldenTransposedProtoBrotli(t *testing.T) {
	records := readAllRecords(t, filepath.Join(goldenDir, "transposed_proto_brotli.riegeli"))
	if len(records) != 10 {
		t.Fatalf("got %d records, want 10", len(records))
	}
	// Verify each record is a valid proto encoding of {id=i, payload="payload_i"}.
	for i, rec := range records {
		// Build expected proto bytes.
		payload := "payload_" + string(rune('0'+i))
		var expected []byte
		expected = append(expected, 0x08, byte(i))                   // field 1: varint
		expected = append(expected, 0x12, byte(len(payload)))         // field 2: length-delimited
		expected = append(expected, []byte(payload)...)
		if !bytes.Equal(rec, expected) {
			t.Errorf("record[%d]: got %x, want %x", i, rec, expected)
		}
	}
}

func TestGoldenFileSignatureMatch(t *testing.T) {
	// Verify the first 64 bytes of the empty golden file match what our Go
	// writer produces.
	data, err := os.ReadFile(filepath.Join(goldenDir, "empty.riegeli"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) != 64 {
		t.Fatalf("empty.riegeli is %d bytes, want 64", len(data))
	}

	// Write the same with Go writer.
	var buf bytes.Buffer
	w, err := riegeli.NewRecordWriter(&buf)
	if err != nil {
		t.Fatalf("NewRecordWriter: %v", err)
	}
	w.Close()

	goData := buf.Bytes()
	if len(goData) < 64 {
		t.Fatalf("Go writer produced %d bytes, want >= 64", len(goData))
	}

	// The file signatures should be byte-identical.
	if !bytes.Equal(data[:64], goData[:64]) {
		t.Errorf("file signature mismatch between C++ and Go")
		t.Logf("C++: %x", data[:64])
		t.Logf("Go:  %x", goData[:64])
	}
}
