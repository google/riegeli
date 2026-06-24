package riegeli

import "fmt"

// RecordPosition identifies a specific record within a Riegeli file.
type RecordPosition struct {
	// ChunkBegin is the file position where the chunk containing this record starts.
	ChunkBegin uint64
	// RecordIndex is the zero-based index of the record within its chunk.
	RecordIndex uint64
}

// Numeric returns the numeric position (ChunkBegin + RecordIndex).
func (p RecordPosition) Numeric() uint64 {
	return p.ChunkBegin + p.RecordIndex
}

// String returns a human-readable representation.
func (p RecordPosition) String() string {
	return fmt.Sprintf("%d/%d", p.ChunkBegin, p.RecordIndex)
}
