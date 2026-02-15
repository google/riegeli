package compression

import "io"

// readFull reads exactly len(buf) bytes from r.
func readFull(r io.Reader, buf []byte) (int, error) {
	return io.ReadFull(r, buf)
}
