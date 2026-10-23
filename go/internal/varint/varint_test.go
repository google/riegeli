package varint

import (
	"bytes"
	"math"
	"testing"
)

func TestPutAndReadUvarint64(t *testing.T) {
	tests := []struct {
		name string
		val  uint64
	}{
		{"zero", 0},
		{"one", 1},
		{"127", 127},
		{"128", 128},
		{"300", 300},
		{"16384", 16384},
		{"max_uint32", math.MaxUint32},
		{"max_uint64", math.MaxUint64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf [MaxLenVarint64]byte
			n := PutUvarint64(buf[:], tt.val)

			got, nn, err := Uvarint64(buf[:n])
			if err != nil {
				t.Fatalf("Uvarint64(%d): %v", tt.val, err)
			}
			if nn != n {
				t.Errorf("Uvarint64 read %d bytes, PutUvarint64 wrote %d", nn, n)
			}
			if got != tt.val {
				t.Errorf("Uvarint64 = %d, want %d", got, tt.val)
			}
		})
	}
}

func TestPutAndReadUvarint32(t *testing.T) {
	tests := []uint32{0, 1, 127, 128, 300, 16384, math.MaxUint32}
	for _, v := range tests {
		var buf [MaxLenVarint32]byte
		n := PutUvarint32(buf[:], v)
		got, nn, err := Uvarint32(buf[:n])
		if err != nil {
			t.Fatalf("Uvarint32(%d): %v", v, err)
		}
		if nn != n {
			t.Errorf("Uvarint32(%d) read %d bytes, wrote %d", v, nn, n)
		}
		if got != v {
			t.Errorf("Uvarint32 = %d, want %d", got, v)
		}
	}
}

func TestReadUvarint64FromReader(t *testing.T) {
	var buf [MaxLenVarint64]byte
	n := PutUvarint64(buf[:], 300)
	r := bytes.NewReader(buf[:n])
	got, err := ReadUvarint64(r)
	if err != nil {
		t.Fatalf("ReadUvarint64(300): %v", err)
	}
	if got != 300 {
		t.Errorf("ReadUvarint64 = %d, want 300", got)
	}
}

func TestUvarint64Truncated(t *testing.T) {
	_, _, err := Uvarint64(nil)
	if err == nil {
		t.Error("expected error for empty buffer")
	}
	_, _, err = Uvarint64([]byte{0x80})
	if err == nil {
		t.Error("expected error for truncated varint")
	}
}

func TestLenUvarint64(t *testing.T) {
	tests := []struct {
		val      uint64
		expected int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{math.MaxUint64, MaxLenVarint64},
	}
	for _, tt := range tests {
		got := LenUvarint64(tt.val)
		if got != tt.expected {
			t.Errorf("LenUvarint64(%d) = %d, want %d", tt.val, got, tt.expected)
		}
	}
}

func TestKnownEncodings(t *testing.T) {
	// From the format examples doc.
	tests := []struct {
		val  uint64
		hex  []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{300, []byte{0xAC, 0x02}},
		{16384, []byte{0x80, 0x80, 0x01}},
	}
	for _, tt := range tests {
		var buf [MaxLenVarint64]byte
		n := PutUvarint64(buf[:], tt.val)
		if !bytes.Equal(buf[:n], tt.hex) {
			t.Errorf("PutUvarint64(%d) = %x, want %x", tt.val, buf[:n], tt.hex)
		}
	}
}

func TestUvarint32Overflow(t *testing.T) {
	// Encode a uint64 that exceeds uint32 max.
	var buf [MaxLenVarint64]byte
	n := PutUvarint64(buf[:], math.MaxUint64)
	_, _, err := Uvarint32(buf[:n])
	if err == nil {
		t.Error("expected overflow error for MaxUint64 decoded as uint32")
	}
}

func TestReadUvarint32(t *testing.T) {
	tests := []uint32{0, 1, 127, 128, 300, 16384, math.MaxUint32}
	for _, v := range tests {
		var buf [MaxLenVarint32]byte
		n := PutUvarint32(buf[:], v)
		r := bytes.NewReader(buf[:n])
		got, err := ReadUvarint32(r)
		if err != nil {
			t.Fatalf("ReadUvarint32(%d): %v", v, err)
		}
		if got != v {
			t.Errorf("ReadUvarint32 = %d, want %d", got, v)
		}
	}
}

func TestReadUvarint32Overflow(t *testing.T) {
	// Encode MaxUint64 and try to read as uint32.
	var buf [MaxLenVarint64]byte
	n := PutUvarint64(buf[:], math.MaxUint64)
	r := bytes.NewReader(buf[:n])
	_, err := ReadUvarint32(r)
	if err == nil {
		t.Error("expected overflow error for MaxUint64 read as uint32")
	}
}

func TestReadUvarint64Error(t *testing.T) {
	// Empty reader should return error.
	r := bytes.NewReader(nil)
	_, err := ReadUvarint64(r)
	if err == nil {
		t.Error("expected error for empty reader")
	}
}

func TestLenUvarint32(t *testing.T) {
	tests := []struct {
		val  uint32
		want int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{math.MaxUint32, MaxLenVarint32},
	}
	for _, tt := range tests {
		got := LenUvarint32(tt.val)
		if got != tt.want {
			t.Errorf("LenUvarint32(%d) = %d, want %d", tt.val, got, tt.want)
		}
	}
}

func TestUvarint64Overflow(t *testing.T) {
	// Build an overlong varint (11 bytes, all with continuation bits).
	buf := make([]byte, 11)
	for i := range buf {
		buf[i] = 0x80
	}
	buf[10] = 0x02 // makes the varint overflow
	_, _, err := Uvarint64(buf)
	if err == nil {
		t.Error("expected overflow error for overlong varint")
	}
}
