package chunk

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/google/riegeli-go/internal/hashutil"
)

func TestChunkHeaderSize(t *testing.T) {
	if HeaderSize != 40 {
		t.Errorf("HeaderSize = %d, want 40", HeaderSize)
	}
}

func TestNewHeaderAndParse(t *testing.T) {
	data := []byte("hello world")
	h := NewHeader(data, Simple, 1, uint64(len(data)))

	if h.Type() != Simple {
		t.Errorf("Type = %c, want %c", h.Type(), Simple)
	}
	if h.NumRecords() != 1 {
		t.Errorf("NumRecords = %d, want 1", h.NumRecords())
	}
	if h.DataSize() != uint64(len(data)) {
		t.Errorf("DataSize = %d, want %d", h.DataSize(), len(data))
	}
	if h.DecodedDataSize() != uint64(len(data)) {
		t.Errorf("DecodedDataSize = %d, want %d", h.DecodedDataSize(), len(data))
	}
	if !h.ValidHeader() {
		t.Error("header hash mismatch")
	}
	if !h.ValidData(data) {
		t.Error("data hash mismatch")
	}
	if h.ValidData([]byte("wrong data")) {
		t.Error("data hash should not match wrong data")
	}

	// Parse round-trip.
	h2 := ParseHeader(h.Bytes())
	if h2.Type() != h.Type() || h2.NumRecords() != h.NumRecords() ||
		h2.DataSize() != h.DataSize() || h2.DecodedDataSize() != h.DecodedDataSize() {
		t.Error("parsed header fields don't match original")
	}
	if !bytes.Equal(h2.Bytes(), h.Bytes()) {
		t.Error("parsed header bytes don't match original")
	}
}

func TestChunkTypeNumRecordsPacking(t *testing.T) {
	// The packed field is: uint64(chunk_type) | (num_records << 8)
	tests := []struct {
		ct         ChunkType
		numRecords uint64
	}{
		{Simple, 0},
		{Simple, 1},
		{Simple, 255},
		{Simple, 1000},
		{Transposed, 42},
		{FileSignature, 0},
		{Padding, 0},
	}
	for _, tc := range tests {
		h := NewHeader(nil, tc.ct, tc.numRecords, 0)
		if h.Type() != tc.ct {
			t.Errorf("Type = %c, want %c (numRecords=%d)", h.Type(), tc.ct, tc.numRecords)
		}
		if h.NumRecords() != tc.numRecords {
			t.Errorf("NumRecords = %d, want %d (type=%c)", h.NumRecords(), tc.numRecords, tc.ct)
		}
	}
}

func TestFileSignatureChunk(t *testing.T) {
	sig := FileSignatureChunk()

	if sig.Header.Type() != FileSignature {
		t.Errorf("Type = %c, want %c", sig.Header.Type(), FileSignature)
	}
	if sig.Header.NumRecords() != 0 {
		t.Errorf("NumRecords = %d, want 0", sig.Header.NumRecords())
	}
	if sig.Header.DataSize() != 0 {
		t.Errorf("DataSize = %d, want 0", sig.Header.DataSize())
	}
	if sig.Header.DecodedDataSize() != 0 {
		t.Errorf("DecodedDataSize = %d, want 0", sig.Header.DecodedDataSize())
	}
	if !sig.Header.ValidHeader() {
		t.Error("file signature header hash invalid")
	}
	if len(sig.Data) != 0 {
		t.Errorf("Data length = %d, want 0", len(sig.Data))
	}

	// Verify known hash values from format spec.
	// header_hash should be Hash(bytes 8-39).
	headerHash := sig.Header.StoredHeaderHash()
	computedHash := hashutil.Hash(sig.Header.Bytes()[8:40])
	if headerHash != computedHash {
		t.Errorf("header_hash = 0x%016x, computed = 0x%016x", headerHash, computedHash)
	}

	// data_hash should be Hash(nil) = 0x72c3b1e9c0139fe1
	if sig.Header.DataHash() != 0x72c3b1e9c0139fe1 {
		t.Errorf("data_hash = 0x%016x, want 0x72c3b1e9c0139fe1", sig.Header.DataHash())
	}

	// chunk_type | num_records packed: 's' | (0 << 8) = 0x73
	packed := binary.LittleEndian.Uint64(sig.Header.Bytes()[24:32])
	if packed != 0x73 {
		t.Errorf("packed type|num = 0x%x, want 0x73", packed)
	}
}

func TestPaddingChunk(t *testing.T) {
	p := PaddingChunk(100)
	if p.Header.Type() != Padding {
		t.Errorf("Type = %c, want %c", p.Header.Type(), Padding)
	}
	if p.Header.NumRecords() != 0 {
		t.Errorf("NumRecords = %d, want 0", p.Header.NumRecords())
	}
	if p.Header.DataSize() != 100 {
		t.Errorf("DataSize = %d, want 100", p.Header.DataSize())
	}
	if len(p.Data) != 100 {
		t.Errorf("Data length = %d, want 100", len(p.Data))
	}
	// Data should be all zeros.
	for i, b := range p.Data {
		if b != 0 {
			t.Errorf("Data[%d] = %d, want 0", i, b)
			break
		}
	}
	if !p.Header.ValidHeader() {
		t.Error("padding chunk header hash invalid")
	}
	if !p.Header.ValidData(p.Data) {
		t.Error("padding chunk data hash invalid")
	}
}

func TestHeaderCorruption(t *testing.T) {
	h := NewHeader([]byte("test"), Simple, 1, 4)
	if !h.ValidHeader() {
		t.Fatal("header should be valid before corruption")
	}

	// Corrupt a byte in the header payload.
	corrupted := make([]byte, HeaderSize)
	copy(corrupted, h.Bytes())
	corrupted[10] ^= 0xFF

	h2 := ParseHeader(corrupted)
	if h2.ValidHeader() {
		t.Error("corrupted header should not be valid")
	}
}
