# Data Encoding Primitives

This document describes the low-level data encoding formats used throughout the Riegeli/records file format.

## Varint Encoding

Riegeli uses standard Protocol Buffer varint encoding for variable-length integers.

### Varint Format

A varint encodes an unsigned integer using one or more bytes. Each byte in the encoding has:
- Bit 7 (most significant bit): **continuation bit**
  - `1` = more bytes follow
  - `0` = this is the last byte
- Bits 6-0: payload (7 bits of data)

The integer value is reconstructed by:
1. Taking the lower 7 bits from each byte
2. Concatenating them in **little-endian order** (first byte contains least significant 7 bits)
3. Forming the final unsigned integer

### Examples

```
Value: 0
Encoding: 0x00
Explanation: Single byte, continuation bit = 0, value = 0

Value: 1
Encoding: 0x01
Explanation: Single byte, continuation bit = 0, value = 1

Value: 127
Encoding: 0x7F
Explanation: Single byte, continuation bit = 0, value = 127

Value: 128
Encoding: 0x80 0x01
Explanation: 
  Byte 0: 0x80 = 0b10000000 (continuation=1, payload=0b0000000)
  Byte 1: 0x01 = 0b00000001 (continuation=0, payload=0b0000001)
  Value: 0b0000001_0000000 = 128

Value: 300
Encoding: 0xAC 0x02
Explanation:
  Byte 0: 0xAC = 0b10101100 (continuation=1, payload=0b0101100)
  Byte 1: 0x02 = 0b00000010 (continuation=0, payload=0b0000010)
  Value: 0b0000010_0101100 = 256 + 44 = 300
```

### Length Constraints

- **Varint32**: Maximum 5 bytes
  - Can encode values 0 to 2³²-1
- **Varint64**: Maximum 10 bytes
  - Can encode values 0 to 2⁶⁴-1

### Implementation Notes

- Decoders SHOULD accept non-canonical (longer than necessary) representations for robustness
- Decoders MUST reject varints longer than the maximum length
- Decoders MUST reject varints with bits set outside the valid range
- Encoders SHOULD use canonical (shortest) representations

### Canonical Representation

The canonical (shortest) representation never has a trailing zero byte, except for the value 0 itself:
- Value 0: `0x00` (canonical)
- Value 128: `0x80 0x01` (canonical), `0x80 0x81 0x00` (non-canonical, rejected by strict readers)

## Fixed-Width Integers

Fixed-width integers are encoded as **unsigned little-endian** values.

### uint32 (4 bytes)

```
Value: 0x12345678
Encoding: 0x78 0x56 0x34 0x12
(least significant byte first)
```

### uint64 (8 bytes)

```
Value: 0x0123456789ABCDEF
Encoding: 0xEF 0xCD 0xAB 0x89 0x67 0x45 0x23 0x01
(least significant byte first)
```

## Compressed Block Format

When data is compressed (compression_type ≠ 0), the compressed block has this structure:

```
<decompressed_size: varint64><compressed_data: bytes>
```

- `decompressed_size`: A varint64 indicating the size of the data after decompression
- `compressed_data`: The compressed bytes (format depends on compression_type)

### Rationale

The decompressed size allows decoders to pre-allocate the exact buffer size needed, improving performance and reducing memory fragmentation.

## Compression Types

The following compression type bytes are used throughout the file format:

| Value | Compression Algorithm |
|-------|----------------------|
| `0x00` | None (uncompressed) |
| `0x62` ('b') | [Brotli](https://github.com/google/brotli) |
| `0x7a` ('z') | [Zstd](https://facebook.github.io/zstd/) |
| `0x73` ('s') | [Snappy](https://google.github.io/snappy/) |

### Decompression Requirements

Implementations must support:
- **None** (required for minimal implementation)
- **Brotli** (recommended, commonly used default)
- **Zstd** (recommended)
- **Snappy** (optional, used for low-latency scenarios)

A minimal implementation may reject files using unsupported compression algorithms.

## Hash Function

Riegeli uses **HighwayHash** for integrity checking.

### HighwayHash Configuration

- **Algorithm**: [HighwayHash](https://github.com/google/highwayhash)
- **Output**: 64-bit hash value
- **Key** (32 bytes): 
  ```
  0x2f696c6567656952  ('Riegeli/')
  0x0a7364726f636572  ('records\n')
  0x2f696c6567656952  ('Riegeli/')
  0x0a7364726f636572  ('records\n')
  ```

### Usage

Hashes are computed over byte sequences and stored as **8-byte little-endian unsigned integers**.

```c++
uint64_t ComputeHash(const uint8_t* data, size_t size) {
  static const uint64_t key[4] = {
    0x2f696c6567656952ULL,  // 'Riegeli/'
    0x0a7364726f636572ULL,  // 'records\n'
    0x2f696c6567656952ULL,  // 'Riegeli/'
    0x0a7364726f636572ULL   // 'records\n'
  };
  return HighwayHash64(data, size, key);
}
```

## Protocol Buffer Wire Format

Riegeli's transposed chunk format works with Protocol Buffer encoded messages. Understanding the wire format is essential for implementing transposed chunks.

### Wire Types

Protocol Buffer fields are encoded with a tag that includes the wire type:

| Wire Type | Value | Meaning |
|-----------|-------|---------|
| `VARINT` | 0 | Variable-length integer |
| `FIXED64` | 1 | 64-bit fixed-width value |
| `LENGTH_DELIMITED` | 2 | Length-prefixed data (strings, bytes, embedded messages) |
| `START_GROUP` | 3 | Start of group (deprecated) |
| `END_GROUP` | 4 | End of group (deprecated) |
| `FIXED32` | 5 | 32-bit fixed-width value |

### Tag Format

A tag is encoded as a varint with the structure:

```
tag = (field_number << 3) | wire_type
```

- **Field number**: Protocol Buffer field identifier (bits 3+)
- **Wire type**: One of the wire types above (bits 0-2)

### Example

For field number 5 with wire type VARINT:
```
tag = (5 << 3) | 0 = 40 = 0x28
```

### Reading Protocol Buffer Data

To parse a Protocol Buffer message:
1. Read tag (varint)
2. Extract wire type: `wire_type = tag & 0x07`
3. Extract field number: `field_number = tag >> 3`
4. Read value based on wire type:
   - `VARINT`: Read varint
   - `FIXED64`: Read 8 bytes
   - `FIXED32`: Read 4 bytes
   - `LENGTH_DELIMITED`: Read varint length, then that many bytes

## Block Size and Alignment

- **Block size**: 64 KiB (65536 bytes = 0x10000 bytes)
- **Block header size**: 24 bytes
- **Usable block size**: 64 KiB - 24 bytes = 65512 bytes

Block headers appear at every multiple of 64 KiB in the file, interrupting chunks that span block boundaries.
