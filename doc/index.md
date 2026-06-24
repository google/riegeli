# Riegeli

*Riegeli/records* is a file format for storing a sequence of string records,
typically serialized protocol buffers. It supports dense compression, fast
decoding, seeking, detection and optional skipping of data corruption, filtering
of proto message fields for even faster decoding, and parallel encoding.

## Documentation

### Quick Start

*   [Quick reference](quick_reference.md) -
    Fast reference for constants, structures, chunk types, algorithms, and
    implementation checklists. Perfect for quick lookups during implementation.

### File Format Specification

*   [Specification of Riegeli/records file format](riegeli_records_file_format.md) - 
    Main specification covering block headers, chunk headers, chunk types, and 
    overall file structure.
*   [Data encoding primitives](data_encoding_primitives.md) - 
    Detailed specification of varint encoding, fixed-width integers, compressed
    blocks, hash functions, and Protocol Buffer wire format basics.
*   [Transposed chunk format](transposed_chunk_format.md) - 
    Complete specification of the transposed chunk format for efficient Protocol
    Buffer compression, including state machine encoding, buffer organization,
    and decoding algorithms.

### Writer Configuration

*   [Specifying options for writing Riegeli/records files](record_writer_options.md) -
    Complete reference for writer options including compression algorithms,
    chunk sizes, padding, and parallelism.

### Implementation Guide

*   [Implementation guide](implementation_guide.md) -
    Practical guide for implementing Riegeli readers and writers in other
    programming languages, including phased approach, testing strategy,
    common pitfalls, and language-specific considerations.
*   [Format examples](format_examples.md) -
    Concrete byte-level examples of Riegeli file structures, including file
    signatures, simple chunks, transposed chunks, block headers, varints,
    Protocol Buffer tags, and test vectors.
