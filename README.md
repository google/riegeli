# Riegeli

*Riegeli/records* is a file format for storing a sequence of string records,
typically serialized protocol buffers. It supports dense compression, fast
decoding, seeking, detection and optional skipping of data corruption, filtering
of proto message fields for even faster decoding, and parallel encoding.

See [documentation](doc/index.md).

# Status

Riegeli file format will only change in a backward compatible way (i.e. future
readers will understand current files, but current readers might not understand
files using future features).

Riegeli C++ API might change in incompatible ways.
