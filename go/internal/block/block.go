// Package block provides block header parsing and arithmetic for the Riegeli
// file format. Block headers appear every 64 KiB in a Riegeli file.
package block

import (
	"encoding/binary"

	"github.com/google/riegeli-go/internal/hashutil"
)

const (
	// BlockSize is the distance between consecutive block boundaries (64 KiB).
	BlockSize uint64 = 1 << 16 // 65536

	// HeaderSize is the size of a block header in bytes.
	HeaderSize = 24

	// UsableBlockSize is the number of usable bytes per block (excluding the header).
	UsableBlockSize = BlockSize - HeaderSize // 65512
)

// Header represents a 24-byte block header.
type Header struct {
	data [HeaderSize]byte
}

// NewHeader creates a new block header with the given previous/next chunk fields.
func NewHeader(previousChunk, nextChunk uint64) Header {
	var h Header
	binary.LittleEndian.PutUint64(h.data[8:16], previousChunk)
	binary.LittleEndian.PutUint64(h.data[16:24], nextChunk)
	hash := hashutil.Hash(h.data[8:24])
	binary.LittleEndian.PutUint64(h.data[0:8], hash)
	return h
}

// Parse parses a block header from 24 bytes.
func Parse(data []byte) Header {
	var h Header
	copy(h.data[:], data[:HeaderSize])
	return h
}

// Bytes returns the raw 24-byte header.
func (h *Header) Bytes() []byte { return h.data[:] }

// StoredHeaderHash returns the stored hash of bytes 8-23.
func (h *Header) StoredHeaderHash() uint64 {
	return binary.LittleEndian.Uint64(h.data[0:8])
}

// ComputedHeaderHash computes the hash of bytes 8-23.
func (h *Header) ComputedHeaderHash() uint64 {
	return hashutil.Hash(h.data[8:24])
}

// Valid returns true if the stored hash matches the computed hash.
func (h *Header) Valid() bool {
	return h.StoredHeaderHash() == h.ComputedHeaderHash()
}

// PreviousChunk returns the distance from this block boundary back to the
// beginning of the chunk that contains the boundary.
func (h *Header) PreviousChunk() uint64 {
	return binary.LittleEndian.Uint64(h.data[8:16])
}

// NextChunk returns the distance from this block boundary forward to the
// beginning of the next chunk (or end of file).
func (h *Header) NextChunk() uint64 {
	return binary.LittleEndian.Uint64(h.data[16:24])
}

// IsBlockBoundary returns true if pos is a block boundary (immediately before a block header).
func IsBlockBoundary(pos uint64) bool {
	return pos%BlockSize == 0
}

// RemainingInBlock returns how many bytes remain until the end of the block.
// Returns 0 at a block boundary.
func RemainingInBlock(pos uint64) uint64 {
	return (-pos) % BlockSize
}

// IsPossibleChunkBoundary returns true if pos is a valid chunk boundary
// (not inside or immediately after a block header).
func IsPossibleChunkBoundary(pos uint64) bool {
	return RemainingInBlock(pos) < UsableBlockSize
}

// RoundUpToPossibleChunkBoundary returns the nearest possible chunk boundary
// at or after pos.
func RoundUpToPossibleChunkBoundary(pos uint64) uint64 {
	r := RemainingInBlock(pos)
	if r > UsableBlockSize-1 {
		return pos + r - (UsableBlockSize - 1)
	}
	return pos
}

// RoundDownToBlockBoundary returns the nearest block boundary at or before pos.
func RoundDownToBlockBoundary(pos uint64) uint64 {
	return pos - pos%BlockSize
}

// RemainingInBlockHeader returns the number of bytes remaining in the block header
// if pos is within a block header, otherwise 0.
func RemainingInBlockHeader(pos uint64) uint64 {
	offset := pos % BlockSize
	if HeaderSize > offset {
		return HeaderSize - offset
	}
	return 0
}

// AddWithOverhead returns the position after length bytes starting at chunkBegin,
// accounting for intervening block headers.
func AddWithOverhead(chunkBegin, length uint64) uint64 {
	numOverheadBlocks := (length + (chunkBegin+UsableBlockSize-1)%BlockSize) / UsableBlockSize
	return chunkBegin + length + numOverheadBlocks*HeaderSize
}

// DistanceWithoutOverhead returns the data length between chunkBegin and pos,
// subtracting intervening block headers.
func DistanceWithoutOverhead(chunkBegin, pos uint64) uint64 {
	numOverheadBlocks := pos/BlockSize - chunkBegin/BlockSize

	adjustedPos := pos - min64(pos%BlockSize, HeaderSize)
	adjustedBegin := chunkBegin - min64(chunkBegin%BlockSize, HeaderSize)
	return adjustedPos - adjustedBegin - numOverheadBlocks*HeaderSize
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// ChunkEnd returns the position after a chunk which begins at chunkBegin.
// The chunk header data_size and num_records are given separately to avoid
// a dependency on the chunk package.
func ChunkEnd(chunkHeaderSize, dataSize, numRecords, chunkBegin uint64) uint64 {
	return max64(
		AddWithOverhead(chunkBegin, chunkHeaderSize+dataSize),
		RoundUpToPossibleChunkBoundary(chunkBegin+numRecords),
	)
}
