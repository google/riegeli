// Package chunk provides chunk header parsing and construction for the Riegeli
// file format. A chunk is the fundamental data unit containing records.
package chunk

import (
	"encoding/binary"

	"github.com/google/riegeli-go/internal/hashutil"
)

const (
	// HeaderSize is the size of a chunk header in bytes (5 * uint64 = 40 bytes).
	HeaderSize = 40

	// MaxNumRecords is the maximum number of records in a chunk.
	MaxNumRecords = (1 << 56) - 1
)

// ChunkType represents the type of a chunk.
type ChunkType byte

const (
	FileSignature ChunkType = 's'
	FileMetadata  ChunkType = 'm'
	Padding       ChunkType = 'p'
	Simple        ChunkType = 'r'
	Transposed    ChunkType = 't'
)

// CompressionType represents the compression algorithm used.
type CompressionType byte

const (
	NoCompression      CompressionType = 0
	BrotliCompression  CompressionType = 'b'
	ZstdCompression    CompressionType = 'z'
	SnappyCompression  CompressionType = 's'
)

// Header represents a 40-byte chunk header.
type Header struct {
	data [HeaderSize]byte
}

// ParseHeader parses a chunk header from 40 bytes.
func ParseHeader(data []byte) Header {
	var h Header
	copy(h.data[:], data[:HeaderSize])
	return h
}

// NewHeader creates a chunk header from its components.
func NewHeader(chunkData []byte, ct ChunkType, numRecords, decodedDataSize uint64) Header {
	var h Header
	binary.LittleEndian.PutUint64(h.data[8:16], uint64(len(chunkData)))
	binary.LittleEndian.PutUint64(h.data[16:24], hashutil.Hash(chunkData))
	binary.LittleEndian.PutUint64(h.data[24:32], uint64(ct)|(numRecords<<8))
	binary.LittleEndian.PutUint64(h.data[32:40], decodedDataSize)
	// Set header hash (hash of bytes 8-39).
	hash := hashutil.Hash(h.data[8:40])
	binary.LittleEndian.PutUint64(h.data[0:8], hash)
	return h
}

// Bytes returns the raw 40-byte header.
func (h *Header) Bytes() []byte { return h.data[:] }

// StoredHeaderHash returns the stored hash of bytes 8-39.
func (h *Header) StoredHeaderHash() uint64 {
	return binary.LittleEndian.Uint64(h.data[0:8])
}

// ComputedHeaderHash computes the hash of bytes 8-39.
func (h *Header) ComputedHeaderHash() uint64 {
	return hashutil.Hash(h.data[8:40])
}

// ValidHeader returns true if the stored header hash matches the computed one.
func (h *Header) ValidHeader() bool {
	return h.StoredHeaderHash() == h.ComputedHeaderHash()
}

// DataSize returns the size of the chunk data in bytes.
func (h *Header) DataSize() uint64 {
	return binary.LittleEndian.Uint64(h.data[8:16])
}

// DataHash returns the stored HighwayHash of the chunk data.
func (h *Header) DataHash() uint64 {
	return binary.LittleEndian.Uint64(h.data[16:24])
}

// Type returns the chunk type.
func (h *Header) Type() ChunkType {
	return ChunkType(h.data[24])
}

// NumRecords returns the number of records in the chunk.
func (h *Header) NumRecords() uint64 {
	return binary.LittleEndian.Uint64(h.data[24:32]) >> 8
}

// DecodedDataSize returns the decoded (decompressed) data size.
func (h *Header) DecodedDataSize() uint64 {
	return binary.LittleEndian.Uint64(h.data[32:40])
}

// ValidData returns true if the stored data hash matches the computed hash.
func (h *Header) ValidData(data []byte) bool {
	return h.DataHash() == hashutil.Hash(data)
}

// Chunk represents a chunk with its header and data.
type Chunk struct {
	Header Header
	Data   []byte
}

// FileSignatureChunk returns the chunk that must appear at the beginning of
// every Riegeli file. This is a deterministic, fixed-content chunk.
func FileSignatureChunk() Chunk {
	return Chunk{
		Header: NewHeader(nil, FileSignature, 0, 0),
		Data:   nil,
	}
}

// PaddingChunk creates a padding chunk. The data size should be set to
// fill the desired space.
func PaddingChunk(dataSize int) Chunk {
	data := make([]byte, dataSize)
	return Chunk{
		Header: NewHeader(data, Padding, 0, 0),
		Data:   data,
	}
}
