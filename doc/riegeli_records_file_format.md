# Riegeli/records file format specification

## Summary

File contents are interpreted as a sequence of variable-sized *chunks,* where a
chunk encodes some number of *records.* A record can be any byte sequence but
Riegeli has special support for the common case where it is a serialized proto
message.

In order to support seeking and recovery after data corruption, the sequence of
chunks is interrupted by a *block header* at every multiple of the block size
which is 64 KiB. After the block header the interrupted chunk continues.

A record can be identified by the position of the chunk beginning and the index
of the record within the chunk. A record can also be identified by a number
resembling a file position, defined as the sum of the chunk beginning and the
record index.

## Conventions

Numbers in block headers and chunk headers are encoded as unsigned Little-Endian
integers.

Hashes are 64-bit [HighwayHash](https://github.com/google/highwayhash) values
with the key {0x2f696c6567656952, 0x0a7364726f636572, 0x2f696c6567656952,
0x0a7364726f636572} ('Riegeli/', 'records\n', 'Riegeli/', 'records\n').

## Block header

A block header allows to locate the chunk that the block header interrupts.
Block headers can interrupt a chunk at arbitrary points, including in the middle
of the chunk header.

If a block header lies exactly between chunks, it is considered to interrupt the
next chunk; this includes the situation at the beginning of the file. In this
case the chunk formally begins at the beginning of the block, even though it
contains no bytes before the block header.

*   Block header (24 bytes):
    *   `header_hash` (8 bytes) — hash of the rest of the header
        (`previous_chunk` and `next_chunk`)
    *   `previous_chunk` (8 bytes) — distance from the beginning of the chunk
        interrupted by this block header to the beginning of the block
    *   `next_chunk` (8 bytes) — distance from the beginning of the block to the
        end of the chunk interrupted by this block header

If `header_hash` does not match, then this block header is corrupted and must be
ignored. Block headers can be skipped during sequential file reading, they are
useful only for seeking and for error recovery.

## Chunk

A chunk must not begin inside nor immediately after a block header.

*   Chunk header (40 bytes):
    *   `header_hash` (8 bytes) — hash of the rest of the header (`data_size` up
        to and including `decoded_data_size`)
    *   `data_size` (8 bytes) — size of `data` (excluding intervening block
        headers)
    *   `data_hash` (8 bytes) — hash of `data`
    *   `chunk_type` (1 byte) — determines how to interpret `data`
    *   `num_records` (7 bytes) — number of records after decoding
    *   `decoded_data_size` (8 bytes) — sum of record sizes after decoding
*   `data` (`data_size` bytes) — encoded records or other data
*   `padding` — ignored (usually filled with zeros by the encoder)

If `header_hash` does not match, header contents cannot be trusted; if skipping
over corruption is desired, a valid chunk should be located using block headers.
If `data_hash` does not match, `data` is corrupted; if skipping over corruption
is desired, the chunk must be ignored.

The size of `padding` is the minimal size which satisfies the following
constraints:

*   The chunk (including chunk header, `data`, `padding`, and intervening block
    headers) has at least as many bytes as `num_records`.
*   The chunk does not end inside nor immediately after a block header.

If `num_records` is 0, `decoded_data_size` has a meaning depending on the chunk
type.

*Rationale:*

*The presence of `padding` allows to assign unique numbers resembling file
positions to records.*

*`decoded_data_size` is stored in the chunk header, instead of being implied by
or stored in `data`, to help decoders decide how many chunks to potentially read
ahead.*

## Chunk data

Some parts of chunk data are compressed. The compression format is generally
specified as `compression_type` (byte):

*   0 — none
*   0x62 ('b') — [Brotli](https://github.com/google/brotli)
*   0x7a ('z') — [Zstd](https://facebook.github.io/zstd/)

Any compressed block is prefixed with its decompressed size (varint64) unless
`compression_type` is 0.

*Rationale:*

*Knowing the decompressed size can make easier for the decoder to decompress
data into a preallocated array.*

### File signature

`chunk_type` is 0x73 ('s').

A file signature chunk must be present at the beginning of the file. It may also
be present elsewhere, in which case it encodes no records and is ignored.

`data_size`, `num_records`, and `decoded_data_size` must be 0.

This makes the first 64 bytes of a Riegeli/records file fixed:

```data
83 af 70 d1 0d 88 4a 3f 00 00 00 00 00 00 00 00
40 00 00 00 00 00 00 00 91 ba c2 3c 92 87 e1 a9
00 00 00 00 00 00 00 00 e1 9f 13 c0 e9 b1 c3 72
73 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
```

### File metadata

`chunk_type` is 0x6d ('m').

A file metadata chunk provides information describing the records. Metadata are
not necessary to read the records but might be helpful to interpret their
contents.

If present, metadata should be written immediately after file signature.

The chunk is encoded like a transposed chunk with a single record containing a
serialized `RecordsMetadata` proto message, except that `chunk_type` is
different and `num_records` is 0.

### Padding chunk

`chunk_type` is 0x70 ('p').

A padding chunk encodes no records and only occupies file space.

`num_records` and `decoded_data_size` must be 0. `data` is ignored (usually
filled with zeros by the encoder).

This can be used for more efficient file concatenation (bringing the file offset
modulo `kBlockSize` to 0 allows for physical concatenation of files without
examining their contents), or for syncing to a file system which requires a
particular file offset granularity in order for the sync to be effective.

### Simple chunk with records

`chunk_type` is 0x72 ('r').

Simple chunks store record sizes and concatenated record contents in two
buffers, possibly compressed.

The format:

*   `compression_type` (byte) — compression type for sizes and values
*   `compressed_sizes_size` (varint64) — size of `compressed_sizes`
*   `compressed_sizes` (`compressed_sizes_size` bytes) - compressed buffer with
    record sizes
*   `compressed_values` (the rest of `data`) — compressed buffer with record
    values

`compressed_sizes`, after decompression, contains `num_records` varint64s: the
size of each record.

`compressed_values`, after decompression, contains `decoded_data_size` bytes:
concatenation of record values.

### Transposed chunk with records

`chunk_type` is 0x74 ('t').

TODO: Document this.

## Properties of the file format

*   Data corruption anywhere is detected whenever the hash allows this, and it
    causes only a local data loss of up to a chunk (if chunk data are damaged)
    or block (if chunk header is damaged).
*   It is possible to open for append and write more records, even without
    reading the original file contents; the original file size must be taken
    into account though.
*   Seeking to the chunk closest to the given file position requires a seek +
    small read, then iterating through chunk headers in a block.

## Implementation notes

The following formulas clarify how certain field values and positions can be
computed.

Constants for fixed sizes:

```c++
kBlockSize = 1 << 16;
kBlockHeaderSize = 24;
kUsableBlockSize = kBlockSize - kBlockHeaderSize;
kChunkHeaderSize = 40;
```

Constraints for chunk boundary distances in a block header:

```c++
previous_chunk % kBlockSize < kUsableBlockSize &&
next_chunk > 0 &&
(next_chunk - 1) % kBlockSize >= kBlockHeaderSize
```

End position of a chunk which begins at `chunk_begin`:

```c++
NumOverheadBlocks(pos, size) =
    (size + (pos + kUsableBlockSize - 1) % kBlockSize) / kUsableBlockSize;
AddWithOverhead(pos, size) =
    pos + size + NumOverheadBlocks(pos, size) * kBlockHeaderSize;

// Equivalent implementation using unsigned arithmetic modulo 1 << 64:
// RemainingInBlock(pos) = (-pos) % kBlockSize;
RemainingInBlock(pos) = kBlockSize - 1 - (pos + kBlockSize - 1) % kBlockSize;
SaturatingSub(a, b) = a > b ? a - b : 0;
// 0 -> 0, 1..25 -> 25, 26 -> 26, ..., 64K -> 64K, 64K+1..64K+25 -> 64K+25 etc.
RoundUpToPossibleChunkBoundary(pos) =
    pos + SaturatingSub(RemainingInBlock(pos), kUsableBlockSize - 1);

chunk_end = max(AddWithOverhead(chunk_begin, kChunkHeaderSize + data_size),
                RoundUpToPossibleChunkBoundary(chunk_begin + num_records));
```

Fields of a block header at `block_begin` which interrupts a chunk at
`chunk_begin`:

```c++
prev_chunk = block_begin - chunk_begin;
next_chunk = chunk_end - block_begin;
```
