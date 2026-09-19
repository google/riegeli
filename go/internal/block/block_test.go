package block

import (
	"testing"
)

func TestConstants(t *testing.T) {
	if BlockSize != 65536 {
		t.Errorf("BlockSize = %d, want 65536", BlockSize)
	}
	if HeaderSize != 24 {
		t.Errorf("HeaderSize = %d, want 24", HeaderSize)
	}
	if UsableBlockSize != 65512 {
		t.Errorf("UsableBlockSize = %d, want 65512", UsableBlockSize)
	}
}

func TestIsBlockBoundary(t *testing.T) {
	tests := []struct {
		pos  uint64
		want bool
	}{
		{0, true},
		{1, false},
		{23, false},
		{24, false},
		{100, false},
		{BlockSize, true},
		{BlockSize * 2, true},
	}
	for _, tt := range tests {
		if got := IsBlockBoundary(tt.pos); got != tt.want {
			t.Errorf("IsBlockBoundary(%d) = %v, want %v", tt.pos, got, tt.want)
		}
	}
}

func TestRemainingInBlock(t *testing.T) {
	tests := []struct {
		pos  uint64
		want uint64
	}{
		{0, 0},
		{1, BlockSize - 1},
		{24, BlockSize - 24},
		{BlockSize, 0},
		{BlockSize + 1, BlockSize - 1},
	}
	for _, tt := range tests {
		if got := RemainingInBlock(tt.pos); got != tt.want {
			t.Errorf("RemainingInBlock(%d) = %d, want %d", tt.pos, got, tt.want)
		}
	}
}

func TestIsPossibleChunkBoundary(t *testing.T) {
	tests := []struct {
		pos  uint64
		want bool
	}{
		{0, true},         // block boundary, remaining=0 < UsableBlockSize
		{1, false},        // inside block header
		{23, false},       // last byte of block header
		{24, false},       // immediately after block header (remaining=65512, not < 65512)
		{25, true},        // first valid chunk boundary in first block
		{64, true},        // pos 64
		{BlockSize, true}, // next block boundary
	}
	for _, tt := range tests {
		if got := IsPossibleChunkBoundary(tt.pos); got != tt.want {
			t.Errorf("IsPossibleChunkBoundary(%d) = %v, want %v", tt.pos, got, tt.want)
		}
	}
}

func TestRoundUpToPossibleChunkBoundary(t *testing.T) {
	tests := []struct {
		pos  uint64
		want uint64
	}{
		{0, 0},
		{1, 25},  // inside block header, rounds up past it
		{24, 25}, // immediately after block header
		{25, 25}, // already a valid chunk boundary
		{64, 64},
	}
	for _, tt := range tests {
		if got := RoundUpToPossibleChunkBoundary(tt.pos); got != tt.want {
			t.Errorf("RoundUpToPossibleChunkBoundary(%d) = %d, want %d", tt.pos, got, tt.want)
		}
	}
}

func TestAddWithOverhead(t *testing.T) {
	// File signature: chunk_begin=0, length=40 (chunk header only, no data)
	// One block header at pos 0, so overhead = 1*24 = 24.
	// Result = 0 + 40 + 24 = 64.
	if got := AddWithOverhead(0, 40); got != 64 {
		t.Errorf("AddWithOverhead(0, 40) = %d, want 64", got)
	}

	// Small chunk after file signature: chunk_begin=64, length=48
	// 64 is a valid chunk boundary. Next block boundary is at 65536.
	// (48 + (64 + 65511) % 65536) / 65512 = (48 + 39) / 65512 = 0
	// Result = 64 + 48 + 0 = 112.
	if got := AddWithOverhead(64, 48); got != 112 {
		t.Errorf("AddWithOverhead(64, 48) = %d, want 112", got)
	}
}

func TestDistanceWithoutOverheadInverse(t *testing.T) {
	// DistanceWithoutOverhead should be the inverse of AddWithOverhead.
	cases := []struct {
		chunkBegin uint64
		length     uint64
	}{
		{0, 0},
		{0, 40},
		{0, 65536},
		{64, 48},
		{64, 100},
		{64, 65536},
		{64, 131072},
	}
	for _, tc := range cases {
		end := AddWithOverhead(tc.chunkBegin, tc.length)
		got := DistanceWithoutOverhead(tc.chunkBegin, end)
		if got != tc.length {
			t.Errorf("DistanceWithoutOverhead(%d, AddWithOverhead(%d, %d)=%d) = %d, want %d",
				tc.chunkBegin, tc.chunkBegin, tc.length, end, got, tc.length)
		}
	}
}

func TestChunkEnd(t *testing.T) {
	// File signature: chunk_begin=0, headerSize=40, dataSize=0, numRecords=0
	// max(AddWithOverhead(0, 40+0), RoundUpToPossibleChunkBoundary(0+0))
	// = max(64, 0) = 64
	if got := ChunkEnd(40, 0, 0, 0); got != 64 {
		t.Errorf("ChunkEnd(40, 0, 0, 0) = %d, want 64", got)
	}
}

func TestBlockHeader(t *testing.T) {
	bh := NewHeader(0, 64)
	if !bh.Valid() {
		t.Error("NewHeader produced invalid block header")
	}
	if bh.PreviousChunk() != 0 {
		t.Errorf("PreviousChunk = %d, want 0", bh.PreviousChunk())
	}
	if bh.NextChunk() != 64 {
		t.Errorf("NextChunk = %d, want 64", bh.NextChunk())
	}

	// Verify round-trip through Parse.
	bh2 := Parse(bh.Bytes())
	if !bh2.Valid() {
		t.Error("Parsed block header is invalid")
	}
	if bh2.PreviousChunk() != 0 || bh2.NextChunk() != 64 {
		t.Error("Parsed block header has wrong values")
	}
}

func TestRemainingInBlockHeader(t *testing.T) {
	tests := []struct {
		pos  uint64
		want uint64
	}{
		{0, 24},  // at block boundary, full header remaining
		{1, 23},  // 1 byte into header
		{23, 1},  // last byte of header
		{24, 0},  // past header
		{100, 0}, // well past header
	}
	for _, tt := range tests {
		if got := RemainingInBlockHeader(tt.pos); got != tt.want {
			t.Errorf("RemainingInBlockHeader(%d) = %d, want %d", tt.pos, got, tt.want)
		}
	}
}
