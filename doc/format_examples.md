# Riegeli Format Examples

This document provides concrete byte-level examples of Riegeli/records file structures to aid implementation and testing.

## Example 1: Minimal File with File Signature

The smallest valid Riegeli file contains only a file signature chunk.

### File Contents (64 bytes)

```
Offset  Hex                                              ASCII
------  -----------------------------------------------  ----------------
0x0000  83 af 70 d1 0d 88 4a 3f 00 00 00 00 00 00 00 00  ..p...J?........
0x0010  40 00 00 00 00 00 00 00 91 ba c2 3c 92 87 e1 a9  @..........<....
0x0020  00 00 00 00 00 00 00 00 e1 9f 13 c0 e9 b1 c3 72  ...............r
0x0030  73 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  s...............
```

### Breakdown

**Chunk Header (bytes 0-39):**
```
Offset  Field                Value (hex)              Value (decimal)
------  -------------------  -----------------------  ---------------
0x0000  header_hash          83 af 70 d1 0d 88 4a 3f  (HighwayHash)
0x0008  data_size            00 00 00 00 00 00 00 00  0
0x0010  data_hash            40 00 00 00 00 00 00 00  (HighwayHash of empty)
0x0018  chunk_type           91 ba c2 3c 92 87 e1 a9  (last byte: 's' = 0x73)
0x0019  num_records (7 bytes) 00 00 00 00 00 00 00    0
0x0020  decoded_data_size    00 00 00 00 00 00 00 00  0
```

Note: The chunk_type byte is at offset 0x0020, value 0x73 ('s').

**Data (bytes 40-63):**
- 24 bytes of zeros (padding to reach 64 bytes minimum chunk size)

### Key Points

- This is the required file signature that must appear at the beginning of every Riegeli file
- `data_size = 0`, `num_records = 0`, `decoded_data_size = 0` for signature chunks
- The hash values are fixed for file signature chunks
- Padding ensures the chunk is at least 64 bytes (to satisfy the constraint that chunk size ≥ num_records)

## Example 2: Simple Chunk with One Record (Uncompressed)

A file with file signature followed by a simple chunk containing one record: "Hello"

### Chunk Header (40 bytes)

```
Offset  Field                Value (hex)              Meaning
------  -------------------  -----------------------  ---------------
0x0040  header_hash          [8 bytes]                Hash of following 32 bytes
0x0048  data_size            06 00 00 00 00 00 00 00  6 bytes
0x0050  data_hash            [8 bytes]                Hash of data section
0x0058  chunk_type           72                       'r' (simple chunk)
0x0059  num_records          01 00 00 00 00 00 00     1 record
0x0060  decoded_data_size    05 00 00 00 00 00 00 00  5 bytes
```

### Data Section (6 bytes)

```
Offset  Field                Value (hex)              Meaning
------  -------------------  -----------------------  ---------------
0x0068  compression_type     00                       Uncompressed
0x0069  sizes_size           01                       1 byte (varint64 = 1)
0x006A  sizes                05                       Size = 5 (varint64)
0x006B  values               48 65 6C 6C 6F           "Hello"
```

### Explanation

1. **compression_type = 0**: No compression
2. **sizes_size = 1**: The sizes buffer is 1 byte long
3. **sizes = 0x05**: Single varint64 encoding the value 5 (length of "Hello")
4. **values = "Hello"**: The actual record data

## Example 3: Simple Chunk with Three Records (Uncompressed)

Records: "a", "bc", "def"

### Data Section Detail

```
Offset  Bytes                 Field                    Value
------  --------------------  -----------------------  -----
0x0068  00                    compression_type         0 (none)
0x0069  03                    sizes_size               3 bytes
0x006A  01                    sizes[0]                 1 (length of "a")
0x006B  02                    sizes[1]                 2 (length of "bc")
0x006C  03                    sizes[2]                 3 (length of "def")
0x006D  61                    values[0]                'a'
0x006E  62 63                 values[1]                'b' 'c'
0x0070  64 65 66              values[2]                'd' 'e' 'f'
```

### Chunk Header Fields

```
data_size = 10 (compression_type + sizes_size + sizes + values)
          = 1 + 1 + 3 + 6 = 11 bytes
num_records = 3
decoded_data_size = 1 + 2 + 3 = 6 bytes
```

## Example 4: Simple Chunk with Compression

Same three records as Example 3, but with Brotli compression.

### Data Section Structure

```
Offset  Field                   Meaning
------  ----------------------  ----------------------------------
0x0068  compression_type        0x62 ('b' = Brotli)
0x0069  sizes_size (varint64)   Length of compressed sizes
0x006A+ compressed_sizes        Brotli(<decompressed_size><01 02 03>)
0x00??  compressed_values       Brotli(<decompressed_size><a|bc|def>)
```

### Compressed Sizes Detail

The sizes buffer after compression:
```
<decompressed_size: varint64><brotli_compressed_data>
```

For sizes `[01, 02, 03]`:
- Decompressed size: 3 (encoded as varint64: `0x03`)
- Compressed data: Brotli compressed bytes

### Compressed Values Detail

The values buffer after compression:
```
<decompressed_size: varint64><brotli_compressed_data>
```

For values `"a" "bc" "def"`:
- Decompressed size: 6 (encoded as varint64: `0x06`)
- Compressed data: Brotli compressed bytes

## Example 5: Block Header

A block header at file position 0x10000 (64 KiB) interrupting a chunk that began at position 0xF000.

### Block Header (24 bytes at offset 0x10000)

```
Offset  Field                Value (hex)              Value (decimal)
------  -------------------  -----------------------  ---------------
0x10000 header_hash          [8 bytes]                Hash of next 16 bytes
0x10008 previous_chunk       00 10 00 00 00 00 00 00  0x1000 = 4096
0x10010 next_chunk           [8 bytes]                Distance to chunk end
```

### Explanation

- **previous_chunk = 0x1000 (4096)**: 
  - Chunk began at 0x10000 - 0x1000 = 0xF000
  - Distance from chunk start to block start = 4096 bytes

- **next_chunk**: 
  - Distance from block start (0x10000) to chunk end
  - This value depends on the chunk's total size

### Usage

When seeking to position 0x10000:
1. Read block header
2. Calculate chunk_begin = 0x10000 - 0x1000 = 0xF000
3. Seek to 0xF000 to read chunk header
4. Skip to next chunk after block header if needed

## Example 6: Varint Encoding Examples

### Single-Byte Varints

```
Value   Encoding (hex)  Binary                          Explanation
-----   --------------  ------------------------------  -----------
0       00              0 0000000                       Last byte, value 0
1       01              0 0000001                       Last byte, value 1
127     7F              0 1111111                       Last byte, value 127
```

### Multi-Byte Varints

```
Value   Encoding (hex)  Binary                          Explanation
-----   --------------  ------------------------------  -----------
128     80 01           1 0000000  0 0000001            Byte 1: continue, bits 0-6 = 0
                                                        Byte 2: last, bits 7-13 = 1
                                                        Value: 0b10000000 = 128

300     AC 02           1 0101100  0 0000010            Byte 1: continue, bits 0-6 = 44
                                                        Byte 2: last, bits 7-13 = 2
                                                        Value: (2 << 7) | 44 = 256 + 44 = 300

16384   80 80 01        1 0000000  1 0000000  0 0000001 3 bytes
                                                        Value: 0b100000000000000 = 16384
```

### Decoding Algorithm

```python
def decode_varint(data, offset):
    result = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result, offset
```

## Example 7: Protocol Buffer Tag Examples

### Field Tag Structure

```
tag = (field_number << 3) | wire_type
```

### Examples

```
Field#  Wire Type  Tag (decimal)  Tag (varint hex)  Meaning
------  ---------  -------------  ----------------  -------
1       VARINT     8              08                int32/int64 field 1
1       FIXED64    9              09                fixed64/double field 1
1       LENGTH_DEL 10             0A                string/bytes/message field 1
2       VARINT     16             10                int32/int64 field 2
5       LENGTH_DEL 42             2A                string/bytes/message field 5
16      VARINT     128            80 01             int32/int64 field 16
```

### Parsing Example

Reading tag `0x2A` (42 decimal):
```
wire_type = 42 & 0x07 = 2 (LENGTH_DELIMITED)
field_number = 42 >> 3 = 5
```

This is a length-delimited field (string, bytes, or embedded message) with field number 5.

## Example 8: Simple Transposed Chunk Structure

A transposed chunk containing two identical messages: `{id: 10, name: "Alice"}`

### Message Definition

```protobuf
message Person {
  optional int32 id = 1;
  optional string name = 2;
}
```

### Wire Format (Non-Transposed)

Each message as bytes:
```
08 0A 12 05 41 6C 69 63 65
│  │  │  │  └─────────────── "Alice"
│  │  │  └──────────────────── length = 5
│  │  └─────────────────────── tag 2, wire type 2
│  └────────────────────────── value = 10
└───────────────────────────── tag 1, wire type 0
```

### Transposed Structure (Conceptual)

**State Machine States:**
1. State 0: kStartOfMessage (tag = 3)
2. State 1: tag = 0x08 (field 1, varint), subtype = 20 (inline value 10)
3. State 2: tag = 0x12 (field 2, length-delimited), subtype = 0, buffer = 0
4. Back to state 0 for second message

**Buffers:**
- Buffer 0 (strings): `05 41 6C 69 63 65 05 41 6C 69 63 65`
  - Length 5, "Alice", Length 5, "Alice"

**Transitions:**
- For 2 messages: State sequence is 0→1→2→0→1→2
- Transition bytes encode these state changes

**Benefit:**
- All "Alice" strings are adjacent in buffer
- Better compression than two separate encoded messages
- Can skip decompressing buffer 0 if only field 1 (id) is needed

## Example 9: Inline Varint in Transposed Chunk

For small varint values (0-127), the value is encoded in the subtype rather than a buffer.

### Example: Field with Value 42

**State:**
- tag = 0x08 (field 1, wire type VARINT)
- subtype = 52 (kVarintInline0 + 42 = 10 + 42 = 52)
- No buffer needed

**Encoding Inline Varints:**
```
Value   Subtype     Meaning
-----   ----------  -------
0       10          kVarintInline0 + 0
1       11          kVarintInline0 + 1
42      52          kVarintInline0 + 42
127     137         kVarintInline0 + 127
128     0           kVarint1 (requires buffer)
```

**Benefit:**
- No buffer space or decompression needed for common small values
- Especially effective for boolean fields (0 or 1)
- Reduces state machine buffer references

## Example 10: Nested Message in Transposed Format

```protobuf
message Outer {
  optional Inner inner = 1;
  message Inner {
    optional int32 value = 1;
  }
}
```

Data: `outer.inner.value = 42`

### State Sequence

1. **State 0**: kStartOfMessage (3) - Start of Outer message
2. **State 1**: tag 0x0A (field 1, length-delimited) - Start of "inner" field
3. **State 2**: kStartOfSubmessage (2) - Enter Inner message
4. **State 3**: tag 0x08 (field 1, varint), subtype=52 (inline 42) - Inner.value
5. **State 4**: tag 0x0E (submessage end marker, wire type 6) - Exit Inner message
6. Back to State 0 for next record

### End of Submessage Marker

Original tag: 0x0A = (1 << 3) | 2
End marker: 0x0A - 2 + 6 = 0x0E

This signals the end of the submessage for field 1.

### Message ID Assignment

- Root: MessageId::kRoot (4)
- Outer message instance: MessageId (5)
- Inner message instance: MessageId (6)

Each submessage gets a unique message ID to distinguish fields at different nesting levels.

## Example 11: Non-Proto Record in Transposed Chunk

When a transposed chunk contains a record that isn't a valid Protocol Buffer:

### State for Non-Proto Record

- tag = 1 (kNonProto reserved ID)
- buffer_index = points to non-proto buffer
- No subtype

### Non-Proto Buffer Format

```
<length_1 (varint64)><data_1><length_2 (varint64)><data_2>...
```

Same as LENGTH_DELIMITED string format.

### Example

Two records: Valid proto `{id: 10}` and non-proto bytes `[0xFF, 0xFE]`

**States:**
1. kStartOfMessage → tag 0x08 (id field) → kStartOfMessage
2. kNonProto (tag = 1) → kStartOfMessage

**Buffers:**
- Non-proto buffer: `02 FF FE` (length 2, bytes 0xFF 0xFE)

## Testing Your Implementation

### Test Vector 1: Parse File Signature

Input: 64-byte file signature (Example 1)
Expected: Successfully parse, no errors

### Test Vector 2: Simple Record

Input: File with one record "Hello" (Example 2)
Expected: Read exactly one record with content "Hello"

### Test Vector 3: Multiple Records

Input: File with records "a", "bc", "def" (Example 3)
Expected: Read three records with correct content

### Test Vector 4: Varint Decoding

Input: Byte sequences from Example 6
Expected: Correct decimal values

### Test Vector 5: Block Header

Input: Chunk spanning 64 KiB boundary (Example 5)
Expected: Correctly skip block header and reconstruct chunk

### Test Vector 6: Compressed Chunk

Input: Brotli-compressed simple chunk (Example 4)
Expected: Decompress and read records correctly

### Test Vector 7: Protocol Buffer Tag

Input: Various tags from Example 7
Expected: Correct field number and wire type extraction

## Generating Test Files

You can generate test files using the C++ implementation:

```cpp
#include "riegeli/records/record_writer.h"

riegeli::RecordWriter writer(
    riegeli::RecordWriterBase::Options()
        .set_uncompressed()  // For easier inspection
        .set_transpose(false),  // Simple chunks
    "test.riegeli");

writer.WriteRecord("Hello");
writer.WriteRecord("World");
writer.Close();
```

Then examine with `hexdump`:
```bash
hexdump -C test.riegeli
```

Or use the summary tool:
```bash
bazel run //riegeli/records/tools:describe_riegeli_file -- test.riegeli
```

## Binary Viewing Tools

Recommended tools for examining Riegeli files:

- **hexdump**: `hexdump -C file.riegeli | less`
- **xxd**: `xxd file.riegeli | less`
- **od**: `od -A x -t x1z -v file.riegeli`
- **Hex editor**: HxD (Windows), Hex Fiend (Mac), ghex (Linux)

## Next Steps

After reviewing these examples:

1. Implement varint decoder/encoder (Example 6)
2. Parse file signature (Example 1)
3. Parse simple chunks (Examples 2-4)
4. Handle block headers (Example 5)
5. Add compression support (Example 4)
6. Implement transposed chunk decoder (Examples 8-11)

See [implementation_guide.md](implementation_guide.md) for detailed guidance.
