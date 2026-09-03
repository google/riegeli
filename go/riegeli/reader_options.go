package riegeli

// ReaderOption configures a RecordReader.
type ReaderOption func(*readerOptions)

type readerOptions struct {
	verifyHashes bool
	recovery     func(SkippedRegion)
}

// SkippedRegion describes a region of the file that was skipped during recovery.
type SkippedRegion struct {
	Begin uint64
	End   uint64
}

func defaultReaderOptions() readerOptions {
	return readerOptions{
		verifyHashes: true,
	}
}

// WithVerifyHashes controls whether hashes are verified during reading.
// Default is true.
func WithVerifyHashes(verify bool) ReaderOption {
	return func(o *readerOptions) {
		o.verifyHashes = verify
	}
}

// WithRecovery sets a callback that is invoked when corrupted data is skipped
// during reading.
func WithRecovery(fn func(SkippedRegion)) ReaderOption {
	return func(o *readerOptions) {
		o.recovery = fn
	}
}
