# Riegeli/records Quick Reference

Fast reference for Riegeli file format constants and structures.

## Constants

```
kBlockSize = 65536 (0x10000, 64 KiB)
kBlockHeaderSize = 24
kUsableBlockSize = 65512 (kBlockSize - kBlockHeaderSize)
kChunkHeaderSize = 40
kMaxTransition = 63
```

## Chunk Types

| Type | Value | Name | Purpose |
|------|-------|------|---------|
| 's' | 0x73 | File Signature | Required at file start |
| 'm' | 0x6D | File Metadata | Optional, contains RecordsMetadata proto |
| 'p' | 0x70 | Padding | Occupies space for alignment |
| 'r' | 0x72 | Simple | Records in sizes+values format |
| 't' | 0x74 | Transposed | Protocol Buffer columnar format |

## Compression Types

| Type | Value | Algorithm |
|------|-------|-----------|
| None | 0x00 | Uncompressed |
| Brotli | 0x62 ('b') | Brotli compression |
| Zstd | 0x7A ('z') | Zstandard compression |
| Snappy | 0x73 ('s') | Snappy compression |

## Block Header (24 bytes)

```
Offset  Size  Type     Field
------  ----  -------  ----------------
0       8     uint64   header_hash
8       8     uint64   previous_chunk
16      8     uint64   next_chunk
```

- **header_hash**: HighwayHash of bytes 8-23
- **previous_chunk**: Distance from chunk start to block start
- **next_chunk**: Distance from block start to chunk end

## Chunk Header (40 bytes)

```
Offset  Size  Type     Field
------  ----  -------  ----------------
0       8     uint64   header_hash
8       8     uint64   data_size
16      8     uint64   data_hash
24      1     uint8    chunk_type
25      7     uint56   num_records
32      8     uint64   decoded_data_size
```

- **header_hash**: HighwayHash of bytes 8-39
- **data_size**: Size of chunk data excluding block headers
- **data_hash**: HighwayHash of chunk data
- **chunk_type**: One of 's', 'm', 'p', 'r', 't'
- **num_records**: Number of records (7 bytes, little-endian)
- **decoded_data_size**: Sum of record sizes after decoding

## HighwayHash Key

```c
const uint64_t key[4] = {
    0x2f696c6567656952,  // 'Riegeli/'
    0x0a7364726f636572,  // 'records\n'
    0x2f696c6567656952,  // 'Riegeli/'
    0x0a7364726f636572   // 'records\n'
};
```

## Varint Encoding

```
Byte format: [continuation_bit][7 bits of data]
- continuation_bit = 1: more bytes follow
- continuation_bit = 0: last byte
- Value assembled little-endian from 7-bit chunks
```

### Examples

```
0:      00
1:      01
127:    7F
128:    80 01
300:    AC 02
16384:  80 80 01
```

## Protocol Buffer Wire Types

| Wire Type | Value | Fields |
|-----------|-------|--------|
| VARINT | 0 | int32, int64, uint32, uint64, sint32, sint64, bool, enum |
| FIXED64 | 1 | fixed64, sfixed64, double |
| LENGTH_DELIMITED | 2 | string, bytes, embedded messages, packed repeated |
| START_GROUP | 3 | group start (deprecated) |
| END_GROUP | 4 | group end (deprecated) |
| FIXED32 | 5 | fixed32, sfixed32, float |

### Tag Format

```
tag = (field_number << 3) | wire_type

Example: Field 5, wire type LENGTH_DELIMITED
tag = (5 << 3) | 2 = 42 = 0x2A
```

## Simple Chunk Format (chunk_type = 'r')

```
┌────────────────────────────────┐
│ compression_type (1 byte)      │
├────────────────────────────────┤
│ compressed_sizes_size (varint) │
├────────────────────────────────┤
│ compressed_sizes (with prefix) │
│   <decompressed_size:varint64> │ ← If compression enabled
│   <compressed_data>            │
├────────────────────────────────┤
│ compressed_values (with prefix)│
│   <decompressed_size:varint64> │ ← If compression enabled
│   <compressed_data>            │
└────────────────────────────────┘

After decompression:
- sizes: num_records varint64 values
- values: concatenated records (decoded_data_size bytes)
```

## Transposed Chunk Format (chunk_type = 't')

```
┌───────────────────────────────────────┐
│ compression_type (1 byte)             │
├───────────────────────────────────────┤
│ header_length (varint64)              │ ← If compression enabled
├───────────────────────────────────────┤
│ Header (possibly compressed):         │
│   num_buckets (varint64)              │
│   num_buffers (varint64)              │
│   bucket_sizes[...] (varint64)        │
│   buffer_lengths[...] (varint64)      │
│   num_states (varint64)               │
│   State Machine:                      │
│     tags[num_states] (varint32)       │
│     bases[num_states] (varint32)      │
│     subtypes (byte array)             │
│     buffer_indices (varint32 array)   │
│   initial_state (varint32)            │
├───────────────────────────────────────┤
│ Data Buckets (each with prefix):     │
│   Bucket 0, 1, ... num_buckets-1      │
├───────────────────────────────────────┤
│ Transitions (possibly compressed):    │
│   <decompressed_size><data>           │
└───────────────────────────────────────┘
```

## Reserved IDs (Transposed Format)

| ID | Name | Purpose |
|----|------|---------|
| 0 | kNoOp | No operation state (navigation) |
| 1 | kNonProto | Non-Protocol Buffer record |
| 2 | kStartOfSubmessage | Start of embedded message |
| 3 | kStartOfMessage | Start of top-level message |
| 4 | kRoot | Root node (not encoded) |
| 6 | kSubmessageWireType | End of submessage marker |

## Subtypes (Transposed Format)

### VARINT Fields

| Subtype | Meaning | Buffer? |
|---------|---------|---------|
| 0-9 | Varint length 1-10 bytes | Yes |
| 10-137 | Inline value (value = subtype - 10) | No |

### LENGTH_DELIMITED Fields

| Subtype | Meaning | Buffer? |
|---------|---------|---------|
| 0 | String/bytes | Yes |
| 1 | Start of submessage | No |
| 2 | End of submessage | No |

### FIXED32/FIXED64/Groups

Subtype always 0 (kTrivial).

## State Machine

### Base and Transitions

- Each state has a **base index**
- States can transition to: `[base, base + 63]`
- Transition value: `next_state = current_base + transition_byte`
- Transition bytes: 0-63

### Implicit Transitions

If a state has exactly one destination:
- Base encoded as: `base + num_states` (signals implicit)
- No transition byte written
- Decoder automatically takes the single transition

### Multi-Hop Transitions

If destination outside [base, base+63]:
- Navigate through intermediate NoOp states
- Each NoOp brings you closer to destination
- Multiple transition bytes written

## File Structure Example

```
┌─────────────────────────────────────┐  Position 0x00000
│ File Signature Chunk (64+ bytes)    │
│   - chunk_type = 's'                │
│   - Must be first chunk             │
├─────────────────────────────────────┤
│ File Metadata Chunk (optional)      │
│   - chunk_type = 'm'                │
│   - RecordsMetadata proto           │
├─────────────────────────────────────┤
│ Record Chunk(s)                     │
│   - chunk_type = 'r' or 't'         │
│   - Contains actual records         │
├─────────────────────────────────────┤  Position 0x10000 (64 KiB)
│ Block Header (24 bytes)             │  ← Interrupts chunk
├─────────────────────────────────────┤
│ (Chunk continues)                   │
├─────────────────────────────────────┤
│ More chunks...                      │
├─────────────────────────────────────┤  Position 0x20000 (128 KiB)
│ Block Header (24 bytes)             │
├─────────────────────────────────────┤
│ ...                                 │
└─────────────────────────────────────┘
```

## Reading Algorithm (Simple Chunks)

```python
1. Read file signature (verify magic bytes)
2. For each chunk:
   a. Read chunk header (40 bytes)
   b. Verify header_hash
   c. Read data_size bytes (skipping block headers)
   d. Verify data_hash
   e. Parse based on chunk_type:
      - 'r': Parse simple chunk
      - 't': Parse transposed chunk
      - 'm': Parse metadata
      - 'p', 's': Skip
3. For simple chunk:
   a. Read compression_type
   b. Read and decompress sizes buffer
   c. Read and decompress values buffer
   d. Extract num_records records using sizes
```

## Reading Algorithm (Transposed Chunks)

```python
1. Read and decompress header
2. Parse state machine
3. Decompress buckets (or on-demand)
4. Decompress transitions
5. Reconstruct records:
   state = initial_state
   transition_idx = 0
   for each record:
       while not at record boundary:
           process state (write tag, read buffer value)
           if implicit transition:
               state = implicit_destination
           else:
               state = base + transitions[transition_idx]
               transition_idx += 1
```

## Common Formulas

### Chunk End Position

```c++
NumOverheadBlocks(pos, size) =
    (size + (pos + kUsableBlockSize - 1) % kBlockSize) / kUsableBlockSize;

AddWithOverhead(pos, size) =
    pos + size + NumOverheadBlocks(pos, size) * kBlockHeaderSize;

chunk_end = max(
    AddWithOverhead(chunk_begin, kChunkHeaderSize + data_size),
    RoundUpToPossibleChunkBoundary(chunk_begin + num_records)
);
```

### Block Header Fields

```c++
previous_chunk = block_begin - chunk_begin;
next_chunk = chunk_end - block_begin;
```

## Implementation Checklist

### Minimal Reader
- [ ] Varint64 decoding
- [ ] Little-endian integer parsing
- [ ] File signature verification
- [ ] Block header parsing and skipping
- [ ] Chunk header parsing
- [ ] Simple chunk decoding (uncompressed)

### Complete Reader
- [ ] All minimal reader features
- [ ] HighwayHash verification
- [ ] Brotli decompression
- [ ] Zstd decompression
- [ ] Snappy decompression
- [ ] Transposed chunk header parsing
- [ ] State machine decoding
- [ ] Buffer reconstruction
- [ ] Transition decoding
- [ ] Record assembly from state machine

### Minimal Writer
- [ ] Varint64 encoding
- [ ] Little-endian integer writing
- [ ] HighwayHash computation
- [ ] File signature generation
- [ ] Block header insertion
- [ ] Chunk header creation
- [ ] Simple chunk encoding
- [ ] At least one compression algorithm

### Complete Writer
- [ ] All minimal writer features
- [ ] Protocol Buffer parsing
- [ ] Tree building from proto messages
- [ ] State machine construction
- [ ] Transition encoding
- [ ] Buffer bucketing
- [ ] Transposed chunk encoding
- [ ] File metadata writing

## Testing Quick Start

```bash
# Generate test file
echo "Hello" | bazel run //riegeli/records/tools:records_tool -- \
    --output=test.riegeli

# Inspect file
hexdump -C test.riegeli | head -20

# Verify
bazel run //riegeli/records/tools:describe_riegeli_file -- test.riegeli
```

## Resources

- [Main Specification](riegeli_records_file_format.md)
- [Data Primitives](data_encoding_primitives.md)
- [Transposed Format](transposed_chunk_format.md)
- [Implementation Guide](implementation_guide.md)
- [Format Examples](format_examples.md)
- [Writer Options](record_writer_options.md)
