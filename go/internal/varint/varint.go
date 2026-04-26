// Package varint provides encoding and decoding of unsigned variable-length integers.
package varint

import (
	"encoding/binary"
	"errors"
	"io"
)

// MaxLenVarint32 is the maximum number of bytes in a varint-encoded uint32.
const MaxLenVarint32 = 5

// MaxLenVarint64 is the maximum number of bytes in a varint-encoded uint64.
const MaxLenVarint64 = 10

var errOverflow = errors.New("varint: overflow")
var errTruncated = errors.New("varint: truncated")

// PutUvarint64 encodes a uint64 into buf and returns the number of bytes written.
// buf must be at least MaxLenVarint64 bytes.
func PutUvarint64(buf []byte, x uint64) int {
	return binary.PutUvarint(buf, x)
}

// PutUvarint32 encodes a uint32 into buf and returns the number of bytes written.
// buf must be at least MaxLenVarint32 bytes.
func PutUvarint32(buf []byte, x uint32) int {
	return binary.PutUvarint(buf, uint64(x))
}

// Uvarint64 decodes a uint64 from buf and returns the value and number of bytes read.
// If buf is too short, returns (0, 0, errTruncated).
// If the varint overflows, returns (0, 0, errOverflow).
func Uvarint64(buf []byte) (uint64, int, error) {
	x, n := binary.Uvarint(buf)
	if n == 0 {
		return 0, 0, errTruncated
	}
	if n < 0 {
		return 0, 0, errOverflow
	}
	return x, n, nil
}

// Uvarint32 decodes a uint32 from buf and returns the value and number of bytes read.
func Uvarint32(buf []byte) (uint32, int, error) {
	x, n, err := Uvarint64(buf)
	if err != nil {
		return 0, 0, err
	}
	if x > 0xFFFFFFFF {
		return 0, 0, errOverflow
	}
	return uint32(x), n, nil
}

// ReadUvarint64 reads a varint-encoded uint64 from an io.ByteReader.
func ReadUvarint64(r io.ByteReader) (uint64, error) {
	x, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	return x, nil
}

// ReadUvarint32 reads a varint-encoded uint32 from an io.ByteReader.
func ReadUvarint32(r io.ByteReader) (uint32, error) {
	x, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	if x > 0xFFFFFFFF {
		return 0, errOverflow
	}
	return uint32(x), nil
}

// LenUvarint64 returns the number of bytes needed to encode x as a varint.
func LenUvarint64(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// LenUvarint32 returns the number of bytes needed to encode x as a varint.
func LenUvarint32(x uint32) int {
	return LenUvarint64(uint64(x))
}
