package hashutil

import (
	"testing"
)

func TestHashEmpty(t *testing.T) {
	// From the file signature chunk: data_hash of empty data.
	// The chunk header at offset 24 has data_hash at bytes 0x28-0x2F:
	// e1 9f 13 c0 e9 b1 c3 72 = 0x72c3b1e9c0139fe1 (little-endian)
	h := Hash(nil)
	want := uint64(0x72c3b1e9c0139fe1)
	if h != want {
		t.Errorf("Hash(nil) = 0x%016x, want 0x%016x", h, want)
	}
}

func TestHashBlockHeaderBytes(t *testing.T) {
	// The block header at position 0 has:
	// header_hash = Hash(bytes 8-23) = Hash([previous_chunk=0, next_chunk=64])
	// = 0x3f4a880dd170af83
	blockPayload := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk = 0
		0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk = 64
	}
	h := Hash(blockPayload)
	want := uint64(0x3f4a880dd170af83)
	if h != want {
		t.Errorf("Hash(block header payload) = 0x%016x, want 0x%016x", h, want)
	}
}

func TestHashChunkHeaderBytes(t *testing.T) {
	// The file signature chunk header bytes 8-39 (32 bytes after header_hash).
	// From the format example at offset 0x18-0x3F:
	chunkHeaderPayload := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size = 0
		0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, // data_hash
		0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type='s' | num_records=0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size = 0
	}
	h := Hash(chunkHeaderPayload)
	// Expected: 0xa9e187923cc2ba91 (from byte sequence 91 ba c2 3c 92 87 e1 a9 at offset 0x18)
	want := uint64(0xa9e187923cc2ba91)
	if h != want {
		t.Errorf("Hash(chunk header payload) = 0x%016x, want 0x%016x", h, want)
	}
}

func TestHashDeterministic(t *testing.T) {
	data := []byte("Hello, World!")
	h1 := Hash(data)
	h2 := Hash(data)
	if h1 != h2 {
		t.Errorf("Hash not deterministic: %x != %x", h1, h2)
	}
}
