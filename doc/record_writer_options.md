# Specifying options for writing Riegeli/records files

Options for writing Riegeli/records files can be specified as a string:

```data
  options ::= option? ("," option?)*
  option ::=
    "default" |
    "transpose" (":" ("true" | "false"))? |
    "uncompressed" |
    "brotli" (":" brotli_level)? |
    "zstd" (":" zstd_level)? |
    "snappy" |
    "window_log" ":" window_log |
    "chunk_size" ":" chunk_size |
    "bucket_fraction" ":" bucket_fraction |
    "pad_to_block_boundary" (":" ("true" | "false"))? |
    "parallelism" ":" parallelism
  brotli_level ::= integer 0..11 (default 6)
  zstd_level ::= integer -131072..22 (default 3)
  window_log ::= "auto" or integer 10..31
  chunk_size ::= "auto" or integer expressed as real with optional suffix
    [BkKMGTPE], 1..
  bucket_fraction ::= real 0..1
  parallelism ::= integer 0..
```

An empty string is the same as `default`.

## `transpose`

If `true` (`transpose` is the same as `transpose:true`), records should be
serialized proto messages (but nothing will break if they are not). A chunk of
records will be processed in a way which allows for better compression.

If `false`, a chunk of records will be stored in a simpler format, directly or
with compression.

Default: `false`.

## Compression algorithms

### `uncompressed`

Changes compression algorithm to none.

### `brotli`

Changes compression algorithm to [Brotli](https://github.com/google/brotli).
Sets compression level which tunes the tradeoff between compression density and
compression speed (higher = better density but slower).

`brotli_level` must be between 0 and 11. Default: `6`.

This is the default compression algorithm.

### `zstd`

Changes compression algorithm to [Zstd](https://facebook.github.io/zstd/). Sets
compression level which tunes the tradeoff between compression density and
compression speed (higher = better density but slower).

`zstd_level` must be between -131072 and 22. Level 0 is currently equivalent to
3. Default: `3`.

### `snappy`

Changes compression algorithm to [Snappy](https://google.github.io/snappy/).

There are no Snappy compression levels to tune.

## `window_log`

Logarithm of the LZ77 sliding window size. This tunes the tradeoff between
compression density and memory usage (higher = better density but more memory).

Special value `auto` means to keep the default (`brotli`: 22, `zstd`: derived
from compression level and chunk size).

For `uncompressed` and `snappy`, `window_log` must be `auto`. For `brotli`,
`window_log` must be `auto` or between 10 and 30. For `zstd`, `window_log` must
be `auto` or between 10 and 30 in 32-bit build, 31 in 64-bit build.

Default: `auto`.

## `chunk_size`

Sets the desired uncompressed size of a chunk which groups messages to be
transposed, compressed, and written together.

A larger chunk size improves compression density; a smaller chunk size allows to
read pieces of the file independently with finer granularity, and reduces memory
usage of both writer and reader.

Special value `auto` means to keep the default (compressed: 1M, uncompressed:
4k).

Default: `auto`.

## `bucket_fraction`

Sets the desired uncompressed size of a bucket which groups values of several
fields of the given wire type to be compressed together, relative to the desired
chunk size, on the scale between 0.0 (compress each field separately) to 1.0
(put all fields of the same wire type in the same bucket.

This is meaningful if transpose and compression are enabled. A larger bucket
size improves compression density; a smaller bucket size makes reading with
projection faster, allowing to skip decompression of values of fields which are
not included.

Default `1.0`.

## `pad_to_block_boundary`

If `true` (`pad_to_block_boundary` is the same as `pad_to_block_boundary:true`),
padding is written to reach a 64KB block boundary when the `RecordWriter` is
created, closed, or flushed. Consequences:

*   Even if the existing file was corrupted or truncated, data appended to it
    will be readable.
*   Physical concatenation of separately written files yields a valid file
    (setting metadata in subsequent files is wasteful but harmless).
*   Up to 64KB is wasted when padding is written.

Default: `false`.

## `parallelism`

Sets the maximum number of chunks being encoded in parallel in background.
Larger parallelism can increase throughput, up to a point where it no longer
matters; smaller parallelism reduces memory usage.

If `parallelism > 0`, chunks are written in background and reporting writing
errors is delayed.

Default: `0`.
