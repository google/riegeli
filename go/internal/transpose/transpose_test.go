package transpose

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

func TestConstants(t *testing.T) {
	// Verify constants match C++ transpose_internal.h.
	if MessageIDNoOp != 0 {
		t.Errorf("MessageIDNoOp = %d, want 0", MessageIDNoOp)
	}
	if MessageIDNonProto != 1 {
		t.Errorf("MessageIDNonProto = %d, want 1", MessageIDNonProto)
	}
	if MessageIDStartOfSubmessage != 2 {
		t.Errorf("MessageIDStartOfSubmessage = %d, want 2", MessageIDStartOfSubmessage)
	}
	if MessageIDStartOfMessage != 3 {
		t.Errorf("MessageIDStartOfMessage = %d, want 3", MessageIDStartOfMessage)
	}
	if MessageIDRoot != 4 {
		t.Errorf("MessageIDRoot = %d, want 4", MessageIDRoot)
	}

	if SubtypeVarint1 != 0 {
		t.Errorf("SubtypeVarint1 = %d, want 0", SubtypeVarint1)
	}
	if SubtypeVarintMax != 9 {
		t.Errorf("SubtypeVarintMax = %d, want 9", SubtypeVarintMax)
	}
	if SubtypeVarintInline0 != 10 {
		t.Errorf("SubtypeVarintInline0 = %d, want 10", SubtypeVarintInline0)
	}
	if SubtypeVarintInlineMax != 137 {
		t.Errorf("SubtypeVarintInlineMax = %d, want 137", SubtypeVarintInlineMax)
	}
	if MaxTransition != 63 {
		t.Errorf("MaxTransition = %d, want 63", MaxTransition)
	}
}

func TestGetTagWireType(t *testing.T) {
	tests := []struct {
		tag  uint32
		want uint32
	}{
		{0x08, WireVarint},         // field 1, varint
		{0x10, WireVarint},         // field 2, varint
		{0x0D, WireFixed32},        // field 1, fixed32
		{0x09, WireFixed64},        // field 1, fixed64
		{0x12, WireLengthDelimited}, // field 2, length-delimited
		{0x1B, WireStartGroup},     // field 3, start group
		{0x1C, WireEndGroup},       // field 3, end group
	}
	for _, tc := range tests {
		got := GetTagWireType(tc.tag)
		if got != tc.want {
			t.Errorf("GetTagWireType(0x%x) = %d, want %d", tc.tag, got, tc.want)
		}
	}
}

func TestGetTagFieldNumber(t *testing.T) {
	tests := []struct {
		tag  uint32
		want int
	}{
		{0x08, 1}, // field 1
		{0x10, 2}, // field 2
		{0x18, 3}, // field 3
		{0x12, 2}, // field 2, length-delimited
	}
	for _, tc := range tests {
		got := GetTagFieldNumber(tc.tag)
		if got != tc.want {
			t.Errorf("GetTagFieldNumber(0x%x) = %d, want %d", tc.tag, got, tc.want)
		}
	}
}

func TestMakeTag(t *testing.T) {
	tests := []struct {
		field int
		wt    uint32
		want  uint32
	}{
		{1, WireVarint, 0x08},
		{1, WireFixed32, 0x0D},
		{1, WireFixed64, 0x09},
		{2, WireLengthDelimited, 0x12},
		{3, WireStartGroup, 0x1B},
	}
	for _, tc := range tests {
		got := MakeTag(tc.field, tc.wt)
		if got != tc.want {
			t.Errorf("MakeTag(%d, %d) = 0x%x, want 0x%x", tc.field, tc.wt, got, tc.want)
		}
	}
}

func TestHasSubtype(t *testing.T) {
	tests := []struct {
		tag  uint32
		want bool
	}{
		{0x08, true},  // varint
		{0x0D, false}, // fixed32
		{0x09, false}, // fixed64
		{0x12, false}, // length-delimited
		{0x1B, false}, // start group
		{0x1C, false}, // end group
	}
	for _, tc := range tests {
		got := HasSubtype(tc.tag)
		if got != tc.want {
			t.Errorf("HasSubtype(0x%x) = %v, want %v", tc.tag, got, tc.want)
		}
	}
}

func TestHasDataBuffer(t *testing.T) {
	tests := []struct {
		tag     uint32
		subtype uint8
		want    bool
	}{
		// Varint with buffer-backed subtype.
		{0x08, SubtypeVarint1, true},
		{0x08, SubtypeVarintMax, true},
		// Varint with inline subtype.
		{0x08, SubtypeVarintInline0, false},
		{0x08, SubtypeVarintInlineMax, false},
		// Fixed32 and Fixed64 always have buffers.
		{0x0D, 0, true},
		{0x09, 0, true},
		// Length-delimited string.
		{0x12, SubtypeLengthDelimitedString, true},
		// Length-delimited submessage.
		{0x12, SubtypeLengthDelimitedStartOfSubmessage, false},
		{0x12, SubtypeLengthDelimitedEndOfSubmessage, false},
		// Groups have no buffers.
		{0x1B, 0, false},
		{0x1C, 0, false},
	}
	for _, tc := range tests {
		got := HasDataBuffer(tc.tag, tc.subtype)
		if got != tc.want {
			t.Errorf("HasDataBuffer(0x%x, %d) = %v, want %v", tc.tag, tc.subtype, got, tc.want)
		}
	}
}

func TestValidTag(t *testing.T) {
	tests := []struct {
		tag  uint32
		want bool
	}{
		{0, false},    // field 0, varint - invalid
		{7, false},    // field 0, wire 7 - invalid wire type
		{8, true},     // field 1, varint
		{0x08, true},  // field 1, varint
		{0x10, true},  // field 2, varint
		{0x0D, true},  // field 1, fixed32
		{0x12, true},  // field 2, length-delimited
	}
	for _, tc := range tests {
		got := ValidTag(tc.tag)
		if got != tc.want {
			t.Errorf("ValidTag(0x%x) = %v, want %v", tc.tag, got, tc.want)
		}
	}
}

func TestDecodeEmpty(t *testing.T) {
	// Empty data with 0 records should succeed.
	records, err := Decode(nil, 0, 0)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("got %d records, want 0", len(records))
	}
}

func TestDecodeEmptyDataNonZeroRecords(t *testing.T) {
	_, err := Decode(nil, 1, 0)
	if err == nil {
		t.Error("Decode with empty data and numRecords>0 should fail")
	}
}

func TestDecodeGoldenNonProto(t *testing.T) {
	// Read a transposed non-proto golden file and decode it.
	// The golden file at ../testdata/golden/transposed_nonproto_uncompressed.riegeli
	// contains records: "Hello", "World", "from", "transposed", "chunk"
	data, err := os.ReadFile("../../testdata/golden/transposed_nonproto_uncompressed.riegeli")
	if err != nil {
		t.Skipf("golden file not available: %v", err)
	}

	// Find the transposed chunk by scanning the file.
	// File structure: 24-byte block header + 40-byte file signature chunk = 64 bytes
	// Then the data chunk starts at position 64.
	if len(data) < 65 {
		t.Fatalf("golden file too short: %d bytes", len(data))
	}

	// Parse the chunk header at position 24 (after block header) - that's the file signature.
	// The actual data chunk header starts at position 64.
	chunkHeaderData := data[64 : 64+40]
	chdr := parseChunkHeader(chunkHeaderData)

	// Read chunk data (after the 40-byte chunk header).
	chunkData := data[64+40 : 64+40+int(chdr.dataSize)]

	records, err := Decode(chunkData, chdr.numRecords, chdr.decodedDataSize)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

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

func TestDecodeGoldenNonProtoBrotli(t *testing.T) {
	data, err := os.ReadFile("../../testdata/golden/transposed_nonproto_brotli.riegeli")
	if err != nil {
		t.Skipf("golden file not available: %v", err)
	}

	if len(data) < 65 {
		t.Fatalf("golden file too short: %d bytes", len(data))
	}

	chunkHeaderData := data[64 : 64+40]
	chdr := parseChunkHeader(chunkHeaderData)
	chunkData := data[64+40 : 64+40+int(chdr.dataSize)]

	records, err := Decode(chunkData, chdr.numRecords, chdr.decodedDataSize)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

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

func TestDecodeGoldenProto(t *testing.T) {
	data, err := os.ReadFile("../../testdata/golden/transposed_proto_brotli.riegeli")
	if err != nil {
		t.Skipf("golden file not available: %v", err)
	}

	if len(data) < 65 {
		t.Fatalf("golden file too short: %d bytes", len(data))
	}

	chunkHeaderData := data[64 : 64+40]
	chdr := parseChunkHeader(chunkHeaderData)
	chunkData := data[64+40 : 64+40+int(chdr.dataSize)]

	records, err := Decode(chunkData, chdr.numRecords, chdr.decodedDataSize)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if len(records) != 10 {
		t.Fatalf("got %d records, want 10", len(records))
	}

	// Verify each record is a valid proto encoding of {id=i, payload="payload_i"}.
	for i, rec := range records {
		payload := "payload_" + string(rune('0'+i))
		var expected []byte
		expected = append(expected, 0x08, byte(i))
		expected = append(expected, 0x12, byte(len(payload)))
		expected = append(expected, []byte(payload)...)
		if !bytes.Equal(rec, expected) {
			t.Errorf("record[%d]: got %x, want %x", i, rec, expected)
		}
	}
}

// parseChunkHeader extracts fields from a 40-byte chunk header for testing.
type chunkHeaderInfo struct {
	dataSize        uint64
	numRecords      uint64
	decodedDataSize uint64
	chunkType       byte
}

func parseChunkHeader(data []byte) chunkHeaderInfo {
	_ = data[39] // bounds check
	dataSize := binary.LittleEndian.Uint64(data[8:16])
	packed := binary.LittleEndian.Uint64(data[24:32])
	return chunkHeaderInfo{
		dataSize:        dataSize,
		numRecords:      packed >> 8,
		decodedDataSize: binary.LittleEndian.Uint64(data[32:40]),
		chunkType:       byte(packed & 0xFF),
	}
}

// putVarint32 appends a varint-encoded uint32 to buf.
func putVarint32(buf []byte, v uint32) []byte {
	var tmp [5]byte
	n := binary.PutUvarint(tmp[:], uint64(v))
	return append(buf, tmp[:n]...)
}

// putVarint64 appends a varint-encoded uint64 to buf.
func putVarint64(buf []byte, v uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

// buildTransposedChunk builds a synthetic transposed chunk data section.
// All data is uncompressed (NoCompression).
type transposedChunkBuilder struct {
	numBuckets    uint32
	numBuffers    uint32
	bucketLengths []uint64
	bufferLengths []uint64
	smSize        uint32
	tags          []uint32
	nextNodes     []uint32
	subtypeBytes  []byte
	bufferIndices []uint32
	firstNode     uint32
	bucketData    [][]byte
	transitions   []byte
}

func (b *transposedChunkBuilder) build() []byte {
	// Build header.
	var header []byte
	header = putVarint32(header, b.numBuckets)
	header = putVarint32(header, b.numBuffers)
	for _, bl := range b.bucketLengths {
		header = putVarint64(header, bl)
	}
	for _, bl := range b.bufferLengths {
		header = putVarint64(header, bl)
	}
	header = putVarint32(header, b.smSize)
	for _, t := range b.tags {
		header = putVarint32(header, t)
	}
	for _, nn := range b.nextNodes {
		header = putVarint32(header, nn)
	}
	header = append(header, b.subtypeBytes...)
	for _, bi := range b.bufferIndices {
		header = putVarint32(header, bi)
	}
	header = putVarint32(header, b.firstNode)

	// Build chunk data: compression_type + header_length + header + buckets + transitions
	var data []byte
	data = append(data, 0x00) // NoCompression
	data = putVarint64(data, uint64(len(header)))
	data = append(data, header...)
	for _, bd := range b.bucketData {
		data = append(data, bd...)
	}
	data = append(data, b.transitions...)
	return data
}

// TestDecodeSyntheticFixed32 tests decoding a transposed chunk with a Fixed32 field.
// Proto record: field 2 (fixed32) = 42, field 1 (varint inline) = 5
// Wire format: 0x08 0x05 0x15 0x2A 0x00 0x00 0x00 (7 bytes)
func TestDecodeSyntheticFixed32(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    1,
		bucketLengths: []uint64{4},
		bufferLengths: []uint64{4},
		smSize:        3,
		// State 0: tag=0x15 (field 2, fixed32), next→1
		// State 1: tag=0x08 (field 1, varint inline=5), next→2
		// State 2: MessageIDStartOfMessage (tag=3), next→0
		tags:      []uint32{0x15, 0x08, 3},
		nextNodes: []uint32{1, 2, 0},
		// Only state 1 (varint) has subtype: SubtypeVarintInline0+5 = 15
		subtypeBytes:  []byte{15},
		bufferIndices: []uint32{0}, // state 0 (fixed32) has buffer idx 0
		firstNode:     0,
		bucketData:    [][]byte{{0x2A, 0x00, 0x00, 0x00}}, // 42 in LE
		transitions:   []byte{0x00, 0x00},                  // 2 transition bytes
	}
	data := b.build()

	records, err := Decode(data, 1, 7)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := []byte{0x08, 0x05, 0x15, 0x2A, 0x00, 0x00, 0x00}
	if !bytes.Equal(records[0], want) {
		t.Errorf("record[0] = %x, want %x", records[0], want)
	}
}

// TestDecodeSyntheticFixed64 tests decoding with a Fixed64 field.
// Proto record: field 1 (fixed64) = 0x0102030405060708
// Wire format: 0x09 0x08 0x07 0x06 0x05 0x04 0x03 0x02 0x01 (9 bytes)
func TestDecodeSyntheticFixed64(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    1,
		bucketLengths: []uint64{8},
		bufferLengths: []uint64{8},
		smSize:        2,
		// State 0: tag=0x09 (field 1, fixed64), next→1
		// State 1: MessageIDStartOfMessage (tag=3), next→0
		tags:          []uint32{0x09, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil, // no varint tags
		bufferIndices: []uint32{0},
		firstNode:     0,
		bucketData:    [][]byte{{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}},
		transitions:   []byte{0x00},
	}
	data := b.build()

	records, err := Decode(data, 1, 9)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := []byte{0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}
	if !bytes.Equal(records[0], want) {
		t.Errorf("record[0] = %x, want %x", records[0], want)
	}
}

// TestDecodeSyntheticMultiByteVarint tests varint with continuation bits.
// Proto record: field 1 (varint) = 300 → 0x08 0xAC 0x02 (3 bytes)
// Buffer stores bytes stripped of continuation bits: [0x2C, 0x02]
func TestDecodeSyntheticMultiByteVarint(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    1,
		bucketLengths: []uint64{2},
		bufferLengths: []uint64{2},
		smSize:        2,
		// State 0: tag=0x08 (field 1, varint), SubtypeVarint2=1, buffer=0, next→1
		// State 1: MessageIDStartOfMessage (tag=3), next→0
		tags:      []uint32{0x08, 3},
		nextNodes: []uint32{1, 0},
		// Subtype for state 0: SubtypeVarint1+1 = 1 (2-byte varint)
		subtypeBytes:  []byte{1},
		bufferIndices: []uint32{0},
		firstNode:     0,
		// Buffer: varint bytes without continuation bits
		// 300 = 0b100101100 → 2 bytes: 0xAC=0b10101100, 0x02=0b00000010
		// Strip MSB: 0x2C, 0x02
		bucketData:  [][]byte{{0x2C, 0x02}},
		transitions: []byte{0x00},
	}
	data := b.build()

	records, err := Decode(data, 1, 3)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := []byte{0x08, 0xAC, 0x02}
	if !bytes.Equal(records[0], want) {
		t.Errorf("record[0] = %x, want %x", records[0], want)
	}
}

// TestDecodeSyntheticString tests decoding a string/bytes field.
// Proto record: field 2 (bytes) = "Hi" → 0x12 0x02 0x48 0x69 (4 bytes)
func TestDecodeSyntheticString(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    1,
		bucketLengths: []uint64{3},
		bufferLengths: []uint64{3},
		smSize:        2,
		// State 0: tag=0x12 (field 2, length-delimited), SubtypeLengthDelimitedString=0 (implicit), buffer=0, next→1
		// State 1: MessageIDStartOfMessage (tag=3), next→0
		tags:          []uint32{0x12, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil, // length-delimited doesn't contribute to subtypes
		bufferIndices: []uint32{0},
		firstNode:     0,
		// Buffer: varint(length=2) + "Hi"
		bucketData:  [][]byte{{0x02, 0x48, 0x69}},
		transitions: []byte{0x00},
	}
	data := b.build()

	records, err := Decode(data, 1, 4)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := []byte{0x12, 0x02, 0x48, 0x69}
	if !bytes.Equal(records[0], want) {
		t.Errorf("record[0] = %x, want %x", records[0], want)
	}
}

// TestDecodeSyntheticSubmessage tests decoding a submessage.
// Proto record: field 2 { field 1: 42 } → 0x12 0x02 0x08 0x2A (4 bytes)
// Transposed: EndOfSubmessage(tag=0x16, wire=6) → inner field → StartOfSubmessage → StartOfMessage
func TestDecodeSyntheticSubmessage(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		bucketLengths: nil,
		bufferLengths: nil,
		smSize:        4,
		// State 0: tag=0x16 (field 2, wire 6=submessage end), next→1
		// State 1: tag=0x08 (field 1, varint inline 42), next→2
		// State 2: MessageIDStartOfSubmessage (tag=2), next→3
		// State 3: MessageIDStartOfMessage (tag=3), next→0
		tags:      []uint32{0x16, 0x08, 2, 3},
		nextNodes: []uint32{1, 2, 3, 0},
		// Subtypes: tag=0x08 is varint → SubtypeVarintInline0+42 = 52
		subtypeBytes:  []byte{52},
		bufferIndices: nil, // no data buffers
		firstNode:     0,
		bucketData:    nil,
		transitions:   []byte{0x00, 0x00, 0x00},
	}
	data := b.build()

	records, err := Decode(data, 1, 4)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	// Proto: field 2 (tag=0x12) length=2 field 1 (tag=0x08) value=42
	want := []byte{0x12, 0x02, 0x08, 0x2A}
	if !bytes.Equal(records[0], want) {
		t.Errorf("record[0] = %x, want %x", records[0], want)
	}
}

// TestDecodeSyntheticGroups tests decoding start/end group tags.
// Proto record: start_group(field 3) inner_varint(field 1, 5) end_group(field 3)
// Wire format: 0x1B 0x08 0x05 0x1C (4 bytes)
func TestDecodeSyntheticGroups(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		bucketLengths: nil,
		bufferLengths: nil,
		smSize:        4,
		// Written backward: EndGroup, inner varint, StartGroup, StartOfMessage
		// State 0: tag=0x1C (field 3, end group), next→1
		// State 1: tag=0x08 (field 1, varint inline 5), next→2
		// State 2: tag=0x1B (field 3, start group), next→3
		// State 3: MessageIDStartOfMessage (tag=3), next→0
		tags:      []uint32{0x1C, 0x08, 0x1B, 3},
		nextNodes: []uint32{1, 2, 3, 0},
		// Subtypes: tag=0x08 is varint → SubtypeVarintInline0+5 = 15
		subtypeBytes:  []byte{15},
		bufferIndices: nil,
		firstNode:     0,
		bucketData:    nil,
		transitions:   []byte{0x00, 0x00, 0x00},
	}
	data := b.build()

	records, err := Decode(data, 1, 4)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := []byte{0x1B, 0x08, 0x05, 0x1C}
	if !bytes.Equal(records[0], want) {
		t.Errorf("record[0] = %x, want %x", records[0], want)
	}
}

// TestDecodeSyntheticImplicitTransitions tests implicit transitions.
// 2 records of {field 1, varint inline 5}: each = [0x08 0x05] (2 bytes)
// Uses implicit next_node values (>= smSize).
func TestDecodeSyntheticImplicitTransitions(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		bucketLengths: nil,
		bufferLengths: nil,
		smSize:        2,
		// State 0: tag=0x08 (varint inline 5), nextNode=3 (>=2, so implicit, actual=1)
		// State 1: MessageIDStartOfMessage (tag=3), nextNode=0 (not implicit)
		tags:      []uint32{0x08, 3},
		nextNodes: []uint32{3, 0}, // state 0: 3 >= 2 → implicit
		// Subtypes: tag=0x08 is varint → 15 (SubtypeVarintInline0+5)
		subtypeBytes:  []byte{15},
		bufferIndices: nil,
		firstNode:     0,
		bucketData:    nil,
		// 1 transition byte for the second record, then EOF terminates
		transitions: []byte{0x00},
	}
	data := b.build()

	records, err := Decode(data, 2, 4)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	want := []byte{0x08, 0x05}
	for i, r := range records {
		if !bytes.Equal(r, want) {
			t.Errorf("record[%d] = %x, want %x", i, r, want)
		}
	}
}

// TestDecodeSyntheticInlineVarint tests inline varint with value 0.
// Record: field 1 (varint) = 0 → [0x08, 0x00] (2 bytes)
func TestDecodeSyntheticInlineVarintZero(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		bucketLengths: nil,
		bufferLengths: nil,
		smSize:        2,
		tags:          []uint32{0x08, 3},
		nextNodes:     []uint32{1, 0},
		// SubtypeVarintInline0 + 0 = 10
		subtypeBytes:  []byte{10},
		bufferIndices: nil,
		firstNode:     0,
		bucketData:    nil,
		transitions:   []byte{0x00},
	}
	data := b.build()

	records, err := Decode(data, 1, 2)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := []byte{0x08, 0x00}
	if !bytes.Equal(records[0], want) {
		t.Errorf("record[0] = %x, want %x", records[0], want)
	}
}

// TestDecodeSyntheticMultipleFields tests a record with multiple field types.
// Record: field 1 varint=5, field 2 string="AB", field 3 fixed32=1
// Wire: 0x08 0x05 0x12 0x02 0x41 0x42 0x1D 0x01 0x00 0x00 0x00 (11 bytes)
func TestDecodeSyntheticMultipleFields(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    2,
		bucketLengths: []uint64{7}, // string buffer(3) + fixed32 buffer(4) = 7
		bufferLengths: []uint64{3, 4},
		smSize:        4,
		// Backward writing order: fixed32, string, varint, StartOfMessage
		// State 0: tag=0x1D (field 3, fixed32), buffer=1, next→1
		// State 1: tag=0x12 (field 2, length-delimited string), buffer=0, next→2
		// State 2: tag=0x08 (field 1, varint inline 5), next→3
		// State 3: MessageIDStartOfMessage (tag=3), next→0
		tags:      []uint32{0x1D, 0x12, 0x08, 3},
		nextNodes: []uint32{1, 2, 3, 0},
		// Subtypes: only state 2 (varint) → 15 (inline 5)
		subtypeBytes: []byte{15},
		// Buffer indices: state 0 (fixed32)=1, state 1 (string)=0
		bufferIndices: []uint32{1, 0},
		firstNode:     0,
		// Bucket: string buffer (varint(2) + "AB") + fixed32 buffer (0x01 0x00 0x00 0x00)
		bucketData:  [][]byte{{0x02, 0x41, 0x42, 0x01, 0x00, 0x00, 0x00}},
		transitions: []byte{0x00, 0x00, 0x00},
	}
	data := b.build()

	records, err := Decode(data, 1, 11)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := []byte{0x08, 0x05, 0x12, 0x02, 0x41, 0x42, 0x1D, 0x01, 0x00, 0x00, 0x00}
	if !bytes.Equal(records[0], want) {
		t.Errorf("record[0] = %x, want %x", records[0], want)
	}
}

// TestDecodeByteReaderEdgeCases tests the byteReader helper.
func TestDecodeByteReaderEdgeCases(t *testing.T) {
	// Read from empty reader.
	r := newByteReader(nil)
	_, err := r.ReadByte()
	if err == nil {
		t.Error("ReadByte on empty should fail")
	}
	rem := r.Remaining()
	if rem != nil {
		t.Errorf("Remaining on empty = %v, want nil", rem)
	}

	// Read with short read.
	r = newByteReader([]byte{0x01, 0x02})
	buf := make([]byte, 5)
	n, err := r.Read(buf)
	if err == nil {
		t.Error("Read past end should fail")
	}
	if n != 2 {
		t.Errorf("Read n = %d, want 2", n)
	}

	// Read exactly available.
	r = newByteReader([]byte{0x01, 0x02})
	buf = make([]byte, 2)
	n, err = r.Read(buf)
	if err != nil {
		t.Fatalf("Read exact: %v", err)
	}
	if n != 2 || buf[0] != 0x01 || buf[1] != 0x02 {
		t.Errorf("Read = %d %v, want 2 [01 02]", n, buf)
	}

	// Remaining after partial read.
	r = newByteReader([]byte{0x01, 0x02, 0x03})
	r.ReadByte()
	rem = r.Remaining()
	if !bytes.Equal(rem, []byte{0x02, 0x03}) {
		t.Errorf("Remaining = %v, want [02 03]", rem)
	}

	// Read from exhausted reader.
	r = newByteReader([]byte{0x01})
	r.ReadByte()
	_, err = r.Read(make([]byte, 1))
	if err == nil {
		t.Error("Read from exhausted reader should fail")
	}
}

// TestDecodeReadUvarint32Overflow tests varint32 overflow in readUvarint32FromReader.
func TestDecodeReadUvarint32Overflow(t *testing.T) {
	// Encode a value > 0xFFFFFFFF as a varint.
	var buf [10]byte
	n := binary.PutUvarint(buf[:], 0x1_0000_0000) // 2^32
	r := bytes.NewReader(buf[:n])
	_, err := readUvarint32FromReader(r)
	if err == nil {
		t.Error("readUvarint32FromReader with overflow should fail")
	}
}

// TestDecodeGoldenNonProtoZstd tests decoding from zstd-compressed transposed file.
func TestDecodeGoldenNonProtoZstd(t *testing.T) {
	data, err := os.ReadFile("../../testdata/golden/transposed_nonproto_zstd.riegeli")
	if err != nil {
		t.Skipf("golden file not available: %v", err)
	}
	if len(data) < 65 {
		t.Fatalf("golden file too short: %d bytes", len(data))
	}

	chunkHeaderData := data[64 : 64+40]
	chdr := parseChunkHeader(chunkHeaderData)
	chunkData := data[64+40 : 64+40+int(chdr.dataSize)]

	records, err := Decode(chunkData, chdr.numRecords, chdr.decodedDataSize)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) == 0 {
		t.Error("expected at least one record")
	}
}

// TestDecodeTruncatedData tests various truncation error paths.
func TestDecodeTruncatedData(t *testing.T) {
	// Single byte: only compression type, no header size.
	_, err := Decode([]byte{0x00}, 1, 1)
	if err == nil {
		t.Error("should fail on truncated header size")
	}

	// Compression type + header_size=5 but no header data.
	_, err = Decode([]byte{0x00, 0x05}, 1, 1)
	if err == nil {
		t.Error("should fail on truncated header data")
	}

	// Valid header but truncated: header says 1 bucket of length 100 but no bucket data.
	var header []byte
	header = putVarint32(header, 1)  // numBuckets=1
	header = putVarint32(header, 0)  // numBuffers=0
	header = putVarint64(header, 100) // bucket_lengths[0]=100
	var data []byte
	data = append(data, 0x00) // NoCompression
	data = putVarint64(data, uint64(len(header)))
	data = append(data, header...)
	// No bucket data follows.
	_, err = Decode(data, 1, 1)
	if err == nil {
		t.Error("should fail on truncated bucket data")
	}
}

// TestDecodeFirstNodeOutOfRange tests first_node >= state_machine_size.
func TestDecodeFirstNodeOutOfRange(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x08, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  []byte{15},
		bufferIndices: nil,
		firstNode:     5, // >= smSize=2 → error
		transitions:   nil,
	}
	data := b.build()
	_, err := Decode(data, 1, 2)
	if err == nil {
		t.Error("should fail with first_node out of range")
	}
}

// TestDecodeWrongNumRecords tests "too many records" error.
func TestDecodeWrongNumRecordsTooFew(t *testing.T) {
	// Build a valid chunk for 2 records but declare numRecords=1.
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x08, 3},
		nextNodes:     []uint32{3, 0}, // implicit
		subtypeBytes:  []byte{15},
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00},
	}
	data := b.build()
	// Chunk produces 2 records of [0x08 0x05] each = 4 bytes.
	// But we declare numRecords=1, decodedDataSize=4.
	// The decoder will hit "too many records" on the second MessageIDStartOfMessage.
	_, err := Decode(data, 1, 4)
	if err == nil {
		t.Error("should fail with too many records")
	}
}

// TestDecodeBytesRemainingInDest tests "bytes remaining in dest" error.
func TestDecodeBytesRemainingInDest(t *testing.T) {
	// Build valid chunk for 1 record of 2 bytes but declare decodedDataSize=10.
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x08, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  []byte{15},
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00},
	}
	data := b.build()
	// Record is 2 bytes but we say 10. destPos will be 8, not 0.
	_, err := Decode(data, 1, 10)
	if err == nil {
		t.Error("should fail with bytes remaining")
	}
}

// TestDecodeWrongRecordCount tests "got N records, expected M" error.
func TestDecodeWrongRecordCount(t *testing.T) {
	// Build valid chunk for 1 record but declare numRecords=2.
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x08, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  []byte{15},
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00},
	}
	data := b.build()
	// Only 1 record produced but numRecords=2 → "got 1 records, expected 2"
	_, err := Decode(data, 2, 2)
	if err == nil {
		t.Error("should fail with wrong record count")
	}
}

// TestDecodeNodeIndexOutOfRange tests transition leading to out-of-range node.
func TestDecodeNodeIndexOutOfRange(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x08, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  []byte{15},
		bufferIndices: nil,
		firstNode:     0,
		// Transition with large offset: offset=63, numIters=0 → tb = 63<<2 = 252
		// nodeIdx = 1 + 63 = 64, which >= smSize=2 → error
		transitions: []byte{252},
	}
	data := b.build()
	_, err := Decode(data, 1, 2)
	if err == nil {
		t.Error("should fail with node index out of range")
	}
}

// TestDecodeNextNodeTooLarge tests next_node double-overflow check.
func TestDecodeNextNodeTooLarge(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x08, 3},
		// nextNode[0] = 6: implicit (6>=2), actual = 6-2 = 4, but 4>=2 → error
		nextNodes:     []uint32{6, 0},
		subtypeBytes:  []byte{15},
		bufferIndices: nil,
		firstNode:     0,
		transitions:   nil,
	}
	data := b.build()
	_, err := Decode(data, 1, 2)
	if err == nil {
		t.Error("should fail with next_node too large")
	}
}

// TestDecodeHasDataBufferDefault tests the default case in HasDataBuffer.
func TestHasDataBufferDefault(t *testing.T) {
	// Wire type 7 (invalid) should return false.
	tag := uint32((1 << 3) | 7) // field 1, wire type 7
	got := HasDataBuffer(tag, 0)
	if got != false {
		t.Errorf("HasDataBuffer(wire7) = %v, want false", got)
	}
}

// TestDecodeTruncatedHeader tests various header truncation points.
func TestDecodeTruncatedHeader(t *testing.T) {
	// Header with num_buckets=1 but truncated before bucket_length.
	var header []byte
	header = putVarint32(header, 1) // numBuckets=1
	header = putVarint32(header, 0) // numBuffers=0
	// Missing: bucket_lengths[0]

	var data []byte
	data = append(data, 0x00) // NoCompression
	data = putVarint64(data, uint64(len(header)))
	data = append(data, header...)

	_, err := Decode(data, 1, 1)
	if err == nil {
		t.Error("should fail: truncated bucket length")
	}

	// Header with state_machine_size but truncated before tags.
	header = nil
	header = putVarint32(header, 0) // numBuckets=0
	header = putVarint32(header, 0) // numBuffers=0
	header = putVarint32(header, 2) // stateMachineSize=2
	// Missing tags

	data = nil
	data = append(data, 0x00)
	data = putVarint64(data, uint64(len(header)))
	data = append(data, header...)

	_, err = Decode(data, 1, 1)
	if err == nil {
		t.Error("should fail: truncated tags")
	}

	// Header with tags but truncated before next_nodes.
	header = nil
	header = putVarint32(header, 0)    // numBuckets=0
	header = putVarint32(header, 0)    // numBuffers=0
	header = putVarint32(header, 1)    // stateMachineSize=1
	header = putVarint32(header, 0x08) // tags[0]=varint field 1
	// Missing next_nodes

	data = nil
	data = append(data, 0x00)
	data = putVarint64(data, uint64(len(header)))
	data = append(data, header...)

	_, err = Decode(data, 1, 1)
	if err == nil {
		t.Error("should fail: truncated next_nodes")
	}

	// Header with tags+next_nodes but truncated before subtypes.
	header = nil
	header = putVarint32(header, 0)    // numBuckets=0
	header = putVarint32(header, 0)    // numBuffers=0
	header = putVarint32(header, 1)    // stateMachineSize=1
	header = putVarint32(header, 0x08) // tags[0]=varint field 1 (needs subtype)
	header = putVarint32(header, 0)    // next_nodes[0]=0
	// Missing subtypes (1 byte expected for the varint tag)

	data = nil
	data = append(data, 0x00)
	data = putVarint64(data, uint64(len(header)))
	data = append(data, header...)

	_, err = Decode(data, 1, 1)
	if err == nil {
		t.Error("should fail: truncated subtypes")
	}

	// Header complete but truncated before first_node.
	header = nil
	header = putVarint32(header, 0)    // numBuckets=0
	header = putVarint32(header, 0)    // numBuffers=0
	header = putVarint32(header, 1)    // stateMachineSize=1
	header = putVarint32(header, 0x08) // tags[0]=varint field 1
	header = putVarint32(header, 0)    // next_nodes[0]=0
	header = append(header, 15)        // subtypes[0]=15 (inline varint)
	// Missing first_node

	data = nil
	data = append(data, 0x00)
	data = putVarint64(data, uint64(len(header)))
	data = append(data, header...)

	_, err = Decode(data, 1, 1)
	if err == nil {
		t.Error("should fail: truncated first_node")
	}
}

// TestDecodeBufferIndexOutOfRange tests buffer index >= numBuffers.
func TestDecodeBufferIndexOutOfRange(t *testing.T) {
	// Build header with a fixed32 tag referencing buffer index 5, but only 1 buffer exists.
	var header []byte
	header = putVarint32(header, 1)  // numBuckets=1
	header = putVarint32(header, 1)  // numBuffers=1
	header = putVarint64(header, 4)  // bucket_lengths[0]=4
	header = putVarint64(header, 4)  // buffer_lengths[0]=4
	header = putVarint32(header, 2)  // stateMachineSize=2
	header = putVarint32(header, 0x0D) // tags[0]=field1 fixed32
	header = putVarint32(header, 3)    // tags[1]=StartOfMessage
	header = putVarint32(header, 1)    // next_nodes[0]=1
	header = putVarint32(header, 0)    // next_nodes[1]=0
	// No subtypes (fixed32 doesn't have subtypes)
	header = putVarint32(header, 5)  // buffer_index for state 0 = 5 (>= numBuffers=1)
	header = putVarint32(header, 0)  // first_node=0

	var data []byte
	data = append(data, 0x00)
	data = putVarint64(data, uint64(len(header)))
	data = append(data, header...)
	data = append(data, 0x01, 0x00, 0x00, 0x00) // bucket data
	data = append(data, 0x00)                     // transitions

	_, err := Decode(data, 1, 5)
	if err == nil {
		t.Error("should fail: buffer index out of range")
	}
}

// TestDecodeSyntheticNonProto tests decoding non-proto records.
// Two non-proto records: "Hi" (2 bytes) and "Bye" (3 bytes).
func TestDecodeSyntheticNonProto(t *testing.T) {
	// NonProto records use MessageIDNonProto(1) for each record.
	// The last buffer is the nonproto_lengths buffer.
	// Buffer 0: data for record payloads ("HiBye" = 5 bytes)
	// Buffer 1: nonproto_lengths (varint 2, varint 3)
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    2,
		bucketLengths: []uint64{7},           // 5 + 2 = 7
		bufferLengths: []uint64{5, 2},        // data=5, lengths=2
		smSize:        1,                     // 1 state: NonProto
		tags:          []uint32{1},           // MessageIDNonProto
		nextNodes:     []uint32{0},           // loops to self
		subtypeBytes:  nil,
		bufferIndices: []uint32{0},           // NonProto data buffer = 0
		firstNode:     0,
		bucketData:    [][]byte{{0x48, 0x69, 0x42, 0x79, 0x65, 0x02, 0x03}}, // "HiBye" + lengths [2,3]
		transitions:   []byte{0x00},          // one transition for second record
	}
	data := b.build()

	records, err := Decode(data, 2, 5)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if string(records[0]) != "Bye" {
		t.Errorf("record[0] = %q, want %q", records[0], "Bye")
	}
	if string(records[1]) != "Hi" {
		t.Errorf("record[1] = %q, want %q", records[1], "Hi")
	}
}

// TestDecodeSyntheticNoOp tests the NoOp message ID in the state machine.
func TestDecodeSyntheticNoOp(t *testing.T) {
	// State 0: NoOp (tag=0), next→1
	// State 1: tag=0x08 (varint inline 5), next→2
	// State 2: MessageIDStartOfMessage (tag=3), next→0
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        3,
		tags:          []uint32{0, 0x08, 3}, // NoOp, varint, StartOfMessage
		nextNodes:     []uint32{1, 2, 0},
		subtypeBytes:  []byte{15}, // varint inline 5
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00, 0x00},
	}
	data := b.build()

	records, err := Decode(data, 1, 2)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := []byte{0x08, 0x05}
	if !bytes.Equal(records[0], want) {
		t.Errorf("record[0] = %x, want %x", records[0], want)
	}
}

// TestDecodeSubmessageStillOpenAtEnd tests "submessages still open at end" error.
func TestDecodeSubmessageStillOpenAtEnd(t *testing.T) {
	// Push a submessage end but never a StartOfSubmessage to pop it.
	// State 0: tag=0x16 (field 2, wire 6=submessage end), next→1
	// State 1: MessageIDStartOfMessage (tag=3), next→0
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x16, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil,
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 0)
	if err == nil {
		t.Error("should fail: submessages still open at end")
	}
}

// TestDecodeSubmessageStackUnderflow tests "submessage stack underflow" error.
func TestDecodeSubmessageStackUnderflow(t *testing.T) {
	// StartOfSubmessage without prior EndOfSubmessage.
	// State 0: MessageIDStartOfSubmessage (tag=2), next→1
	// State 1: MessageIDStartOfMessage (tag=3), next→0
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{2, 3}, // StartOfSubmessage, StartOfMessage
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil,
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 0)
	if err == nil {
		t.Error("should fail: submessage stack underflow")
	}
}

// TestDecodeNotEnoughSpaceVarint tests "not enough space for varint field".
func TestDecodeNotEnoughSpaceVarint(t *testing.T) {
	// Record with varint field (3 bytes: tag + 2-byte varint) but decodedDataSize=1.
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    1,
		bucketLengths: []uint64{2},
		bufferLengths: []uint64{2},
		smSize:        2,
		tags:          []uint32{0x08, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  []byte{1}, // 2-byte varint
		bufferIndices: []uint32{0},
		firstNode:     0,
		bucketData:    [][]byte{{0x2C, 0x02}},
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 1) // decodedDataSize too small
	if err == nil {
		t.Error("should fail: not enough space for varint")
	}
}

// TestDecodeNotEnoughSpaceString tests "not enough space for string field".
func TestDecodeNotEnoughSpaceString(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    1,
		bucketLengths: []uint64{3},
		bufferLengths: []uint64{3},
		smSize:        2,
		tags:          []uint32{0x12, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil,
		bufferIndices: []uint32{0},
		firstNode:     0,
		bucketData:    [][]byte{{0x02, 0x41, 0x42}}, // length=2 + "AB"
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 1) // decodedDataSize too small for 4 bytes
	if err == nil {
		t.Error("should fail: not enough space for string")
	}
}

// TestDecodeNotEnoughSpaceFixed32 tests "not enough space for fixed32".
func TestDecodeNotEnoughSpaceFixed32(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    1,
		bucketLengths: []uint64{4},
		bufferLengths: []uint64{4},
		smSize:        2,
		tags:          []uint32{0x0D, 3}, // field 1, fixed32
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil,
		bufferIndices: []uint32{0},
		firstNode:     0,
		bucketData:    [][]byte{{0x01, 0x00, 0x00, 0x00}},
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 1) // too small for 5 bytes (tag + 4)
	if err == nil {
		t.Error("should fail: not enough space for fixed32")
	}
}

// TestDecodeNotEnoughSpaceFixed64 tests "not enough space for fixed64".
func TestDecodeNotEnoughSpaceFixed64(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    1,
		numBuffers:    1,
		bucketLengths: []uint64{8},
		bufferLengths: []uint64{8},
		smSize:        2,
		tags:          []uint32{0x09, 3}, // field 1, fixed64
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil,
		bufferIndices: []uint32{0},
		firstNode:     0,
		bucketData:    [][]byte{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}},
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 1) // too small for 9 bytes (tag + 8)
	if err == nil {
		t.Error("should fail: not enough space for fixed64")
	}
}

// TestDecodeNotEnoughSpaceInlineVarint tests "not enough space for inline varint".
func TestDecodeNotEnoughSpaceInlineVarint(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x08, 3},
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  []byte{15}, // inline 5
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 0) // decodedDataSize=0, can't fit 2 bytes
	if err == nil {
		t.Error("should fail: not enough space for inline varint")
	}
}

// TestDecodeNotEnoughSpaceStartGroup tests "not enough space for start group".
func TestDecodeNotEnoughSpaceStartGroup(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x1B, 3}, // field 3, start group
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil,
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 0) // decodedDataSize=0
	if err == nil {
		t.Error("should fail: not enough space for start group")
	}
}

// TestDecodeNotEnoughSpaceEndGroup tests "not enough space for end group".
func TestDecodeNotEnoughSpaceEndGroup(t *testing.T) {
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{0x1C, 3}, // field 3, end group
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil,
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 0) // decodedDataSize=0
	if err == nil {
		t.Error("should fail: not enough space for end group")
	}
}

// TestDecodeInvalidTagInStateMachine tests "invalid tag" error.
func TestDecodeInvalidTagInStateMachine(t *testing.T) {
	// Wire type 7 is invalid (field 1, wire 7 = tag 15).
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        2,
		tags:          []uint32{15, 3}, // field 1, wire 7 (invalid)
		nextNodes:     []uint32{1, 0},
		subtypeBytes:  nil,
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00},
	}
	data := b.build()

	_, err := Decode(data, 1, 1)
	if err == nil {
		t.Error("should fail: invalid tag")
	}
}

// TestDecodeNonProtoNoBuffers tests "has non-proto but no buffers" error.
func TestDecodeNonProtoNoBuffers(t *testing.T) {
	// Manually build to include the buffer index in header but with numBuffers=0
	var header []byte
	header = putVarint32(header, 0) // numBuckets=0
	header = putVarint32(header, 0) // numBuffers=0
	header = putVarint32(header, 1) // smSize=1
	header = putVarint32(header, 1) // tags[0]=NonProto
	header = putVarint32(header, 0) // nextNodes[0]=0
	header = putVarint32(header, 0) // bufferIndex for NonProto=0
	header = putVarint32(header, 0) // firstNode=0

	var data []byte
	data = append(data, 0x00) // NoCompression
	data = putVarint64(data, uint64(len(header)))
	data = append(data, header...)

	_, err := Decode(data, 1, 1)
	if err == nil {
		t.Error("should fail: non-proto buffer index out of range or no buffers")
	}
}

// TestDecodeNotEnoughSpaceSubmessageHeader tests "not enough space for submessage header".
func TestDecodeNotEnoughSpaceSubmessageHeader(t *testing.T) {
	// Create submessage with inner data that consumes all space, leaving no room for header.
	// State 0: EndOfSubmessage (tag=0x16, field 2, wire 6)
	// State 1: varint inline 5 (tag=0x08), consumes 2 bytes
	// State 2: StartOfSubmessage (tag=2)
	// State 3: StartOfMessage (tag=3)
	b := &transposedChunkBuilder{
		numBuckets:    0,
		numBuffers:    0,
		smSize:        4,
		tags:          []uint32{0x16, 0x08, 2, 3},
		nextNodes:     []uint32{1, 2, 3, 0},
		subtypeBytes:  []byte{15}, // varint inline 5
		bufferIndices: nil,
		firstNode:     0,
		transitions:   []byte{0x00, 0x00, 0x00},
	}
	data := b.build()

	// Submessage = 2 bytes inner (0x08 0x05), needs tag(1)+length(1)=2 more = 4 total.
	// But we say decodedDataSize=2, so no room for the header.
	_, err := Decode(data, 1, 2)
	if err == nil {
		t.Error("should fail: not enough space for submessage header")
	}
}

// TestDecodeGoldenMultiBrotli tests the multi-record brotli transposed file.
func TestDecodeGoldenMultiBrotli(t *testing.T) {
	data, err := os.ReadFile("../../testdata/golden/transposed_multi_brotli.riegeli")
	if err != nil {
		t.Skipf("golden file not available: %v", err)
	}
	if len(data) < 65 {
		t.Fatalf("golden file too short: %d bytes", len(data))
	}

	chunkHeaderData := data[64 : 64+40]
	chdr := parseChunkHeader(chunkHeaderData)
	chunkData := data[64+40 : 64+40+int(chdr.dataSize)]

	records, err := Decode(chunkData, chdr.numRecords, chdr.decodedDataSize)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if uint64(len(records)) != chdr.numRecords {
		t.Fatalf("got %d records, want %d", len(records), chdr.numRecords)
	}
}
