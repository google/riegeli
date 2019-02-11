# Riegeli

*Riegeli/records* is a file format for storing a sequence of string records,
typically serialized protocol buffers. It supports dense compression, fast
decoding, seeking, detection and optional skipping of data corruption, filtering
of proto message fields for even faster decoding, and parallel encoding.

*   [Specification of Riegeli/records file format](riegeli_records_file_format.md).
*   [Specifying options for writing Riegeli/records files](record_writer_options.md).
