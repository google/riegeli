// Package transpose provides encoding and decoding of transposed ('t') chunks.
// Transposed chunks decompose Protocol Buffer messages into columnar format
// for better compression.
package transpose

// MessageID constants used in the state machine.
const (
	MessageIDNoOp              = 0
	MessageIDNonProto          = 1
	MessageIDStartOfSubmessage = 2
	MessageIDStartOfMessage    = 3
	MessageIDRoot              = 4
)

// Wire types from Protocol Buffer encoding.
const (
	WireVarint          = 0
	WireFixed64         = 1
	WireLengthDelimited = 2
	WireStartGroup      = 3
	WireEndGroup        = 4
	WireFixed32         = 5
	WireSubmessageEnd   = 6 // Riegeli-specific: marks end of submessage
)

// Subtype constants for VARINT wire type.
const (
	SubtypeVarint1   = 0
	SubtypeVarintMax = 9  // kVarint1 + MaxLenVarint64 - 1
	SubtypeVarintInline0   = 10
	SubtypeVarintInlineMax = 137 // kVarintInline0 + 0x7f
)

// Subtype constants for LENGTH_DELIMITED wire type.
const (
	SubtypeLengthDelimitedString          = 0
	SubtypeLengthDelimitedStartOfSubmessage = 1
	SubtypeLengthDelimitedEndOfSubmessage   = 2
)

// MaxTransition is the maximum state offset in a single transition byte.
const MaxTransition = 63

// GetTagWireType extracts the wire type from a proto tag.
func GetTagWireType(tag uint32) uint32 {
	return tag & 7
}

// GetTagFieldNumber extracts the field number from a proto tag.
func GetTagFieldNumber(tag uint32) int {
	return int(tag >> 3)
}

// MakeTag creates a proto tag from field number and wire type.
func MakeTag(fieldNumber int, wireType uint32) uint32 {
	return (uint32(fieldNumber) << 3) | wireType
}

// HasSubtype returns true if the given tag has a subtype (only VARINT wire type).
func HasSubtype(tag uint32) bool {
	return GetTagWireType(tag) == WireVarint
}

// HasDataBuffer returns whether a (tag, subtype) pair reads from a data buffer.
func HasDataBuffer(tag uint32, subtype uint8) bool {
	switch GetTagWireType(tag) {
	case WireVarint:
		return subtype < SubtypeVarintInline0
	case WireFixed32, WireFixed64:
		return true
	case WireLengthDelimited:
		return subtype == SubtypeLengthDelimitedString
	case WireStartGroup, WireEndGroup:
		return false
	default:
		return false
	}
}

// ValidTag returns true if tag is a valid protocol buffer tag.
func ValidTag(tag uint32) bool {
	wt := GetTagWireType(tag)
	switch wt {
	case WireVarint, WireFixed64, WireLengthDelimited, WireStartGroup, WireEndGroup, WireFixed32:
		return tag >= 8 // field number >= 1
	default:
		return false
	}
}
