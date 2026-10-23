package riegeli

import "errors"

// Sentinel errors for Riegeli file operations.
var (
	// ErrCorrupted indicates the file has corrupted data (hash mismatch).
	ErrCorrupted = errors.New("riegeli: corrupted data")

	// ErrTruncated indicates the file is truncated.
	ErrTruncated = errors.New("riegeli: truncated file")

	// ErrInvalidSignature indicates the file does not have a valid Riegeli file signature.
	ErrInvalidSignature = errors.New("riegeli: invalid file signature")

	// ErrUnsupportedCompression indicates an unsupported compression type.
	ErrUnsupportedCompression = errors.New("riegeli: unsupported compression type")

	// ErrUnsupportedChunkType indicates an unknown chunk type.
	ErrUnsupportedChunkType = errors.New("riegeli: unsupported chunk type")
)
