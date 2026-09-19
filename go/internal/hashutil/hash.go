// Package hashutil provides HighwayHash64 with the Riegeli-specific key.
package hashutil

import (
	"github.com/minio/highwayhash"
)

// RiegeliHashKey is the fixed 32-byte HighwayHash key used by Riegeli.
// It encodes the ASCII string "Riegeli/records\nRiegeli/records\n".
var RiegeliHashKey = [32]byte{
	// 0x2f696c6567656952 = "Riegeli/" (little-endian)
	0x52, 0x69, 0x65, 0x67, 0x65, 0x6c, 0x69, 0x2f,
	// 0x0a7364726f636572 = "records\n" (little-endian)
	0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x0a,
	// 0x2f696c6567656952 = "Riegeli/" (little-endian)
	0x52, 0x69, 0x65, 0x67, 0x65, 0x6c, 0x69, 0x2f,
	// 0x0a7364726f636572 = "records\n" (little-endian)
	0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x0a,
}

// Hash computes the HighwayHash64 of data using the Riegeli hash key.
func Hash(data []byte) uint64 {
	return highwayhash.Sum64(data, RiegeliHashKey[:])
}
