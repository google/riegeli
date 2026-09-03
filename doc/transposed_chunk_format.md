# Transposed Chunk Format Specification

This document provides a complete specification of the transposed chunk format (`chunk_type` = 0x74 = 't'), which is used for efficient compression of Protocol Buffer messages.

## Overview

Transposed chunks decompose Protocol Buffer messages into a columnar format where all values of the same field across multiple messages are stored together. This dramatically improves compression ratios (typically 20%+ better) because similar values are adjacent in memory.

## High-Level Structure

A transposed chunk consists of:

1. **Compression type** (1 byte)
2. **Compressed header length** (varint64, if compression enabled)
3. **Header** (possibly compressed):
   - Bucket metadata
   - Buffer metadata  
   - State machine definition
   - Initial state index
4. **Data buckets** (possibly compressed): Concatenated field values
5. **Compressed transitions** (possibly compressed): State machine transitions

```
┌─────────────────────────────────────────┐
│ compression_type (1 byte)               │
├─────────────────────────────────────────┤
│ header_length (varint64)                │  ← Only if compression_type ≠ 0
├─────────────────────────────────────────┤
│ Header (possibly compressed):           │
│   - num_buckets (varint64)              │
│   - num_buffers (varint64)              │
│   - bucket_sizes[num_buckets] (varint)  │
│   - buffer_lengths[num_buffers] (varint)│
│   - num_states (varint64)               │
│   - State machine:                      │
│     - tags[num_states] (varint32)       │
│     - bases[num_states] (varint32)      │
│     - subtypes (raw bytes)              │
│     - buffer_indices (varint32s)        │
│   - initial_state (varint32)            │
├─────────────────────────────────────────┤
│ Buckets (each possibly compressed):     │
│   Bucket 0: <size><compressed data>     │
│   Bucket 1: <size><compressed data>     │
│   ...                                   │
├─────────────────────────────────────────┤
│ Transitions (possibly compressed):      │
│   <decompressed_size><compressed data>  │
└─────────────────────────────────────────┘
```

## Detailed Format

### 1. Compression Type

```
compression_type: 1 byte
```

Values:
- `0x00`: None (uncompressed)
- `0x62` ('b'): Brotli
- `0x7a` ('z'): Zstd
- `0x73` ('s'): Snappy

This compression type applies to the header, data buckets, and transitions.

### 2. Header

The header describes the structure of the transposed data. If `compression_type` ≠ 0:
- A varint64 `header_length` is written first
- The header is compressed
- `header_length` is the compressed size

#### Header Contents

##### 2.1 Bucket Metadata

```
num_buckets: varint64
```

Number of data buckets. Buckets group related field values for better compression locality while allowing selective decompression when using field projection.

```
bucket_sizes: array of num_buckets varint64 values
```

Size of each bucket in bytes. If `compression_type` ≠ 0, this is the **compressed** size.

##### 2.2 Buffer Metadata

```
num_buffers: varint64
```

Number of data buffers. Each buffer contains all values for a specific (tag, subtype) pair across all messages.

```
buffer_lengths: array of num_buffers varint64 values  
```

Uncompressed length of each buffer in bytes.

##### 2.3 State Machine

The state machine encodes the sequence of Protocol Buffer tags encountered across all messages.

```
num_states: varint64
```

Number of states in the state machine.

###### States Encoding (4 Arrays)

States are encoded in four separate arrays for better compression:

**Array 1: Tags/Reserved IDs** (`num_states` varint32 values)

Each entry is one of:
- **Protocol Buffer tag** (varint32): Regular field tag = `(field_number << 3) | wire_type`
- **Special reserved ID** (varint32): One of:
  - `0` = `kNoOp`: No operation state (used for state machine navigation)
  - `1` = `kNonProto`: Non-Protocol Buffer record
  - `2` = `kStartOfSubmessage`: Start of embedded message
  - `3` = `kStartOfMessage`: Start of top-level message
  - `6` = `kSubmessageWireType`: End of embedded message (modified wire type)

For end-of-submessage, the tag is modified:
```
tag_with_end_marker = original_tag - WIRE_TYPE_LENGTH_DELIMITED + kSubmessageWireType
                    = original_tag - 2 + 6  
                    = original_tag + 4
```

**Array 2: Base Indices** (`num_states` varint32 values)

Each state has a base index that defines which states are reachable via transitions:
- States can transition to states in range `[base, base + 63]`
- Value `0` means no outgoing transitions
- If the state has exactly one possible destination (implicit transition), the base is encoded as `base + num_states` to signal this

**Array 3: Subtypes** (raw byte array, variable length)

Contains subtype bytes for states that have subtypes (see "Subtypes" section below). Only present for states where `HasSubtype(tag)` is true.

**Array 4: Buffer Indices** (varint32 array, variable length)

Contains buffer indices for states that have associated data buffers (see "Data Buffers" section below). Only present for states where `HasDataBuffer(tag, subtype)` is true.

##### 2.4 Initial State

```
initial_state: varint32
```

Index of the state machine state that corresponds to the first tag in the sequence.

### 3. Data Buckets

Buckets contain the concatenated data buffers, optionally split into multiple compressed blocks for selective decompression.

For each bucket (0 to `num_buckets-1`):

If `compression_type` ≠ 0:
```
decompressed_size: varint64
compressed_data: byte array (length = bucket_sizes[i])
```

If `compression_type` = 0:
```
uncompressed_data: byte array
```

The bucket contains concatenated data buffers. Buffer boundaries are determined from `buffer_lengths` array in the header.

### 4. Transitions

The transitions section encodes the state machine transitions taken while parsing all messages.

If `compression_type` ≠ 0:
```
decompressed_size: varint64
compressed_transitions: byte array
```

After decompression, the transitions are encoded as:
```
transitions: byte array
```

See "Transition Encoding" section below for the format.

## Subtypes

Subtypes provide additional information about how field values are encoded.

### Wire Type: VARINT (0)

Subtypes distinguish between different varint lengths and inline values:

| Subtype | Meaning | Has Buffer? |
|---------|---------|-------------|
| 0 | 1-byte varint | Yes |
| 1 | 2-byte varint | Yes |
| 2 | 3-byte varint | Yes |
| ... | ... | Yes |
| 9 | 10-byte varint | Yes |
| 10-137 | Inline value (value = subtype - 10) | No |

- Subtypes 0-9 (`kVarint1` to `kVarintMax`): Varint stored in buffer, length = subtype + 1
- Subtypes 10-137 (`kVarintInline0` to `kVarintInlineMax`): Value encoded inline in subtype, actual value = subtype - 10 (range 0-127)

### Wire Type: FIXED32 (5)

Subtype is always 0 (`kTrivial`). Values are stored as 4-byte little-endian integers in the buffer.

### Wire Type: FIXED64 (1)

Subtype is always 0 (`kTrivial`). Values are stored as 8-byte little-endian integers in the buffer.

### Wire Type: LENGTH_DELIMITED (2)

| Subtype | Meaning | Has Buffer? |
|---------|---------|-------------|
| 0 | String/bytes field | Yes |
| 1 | Start of submessage | No |
| 2 | End of submessage | No |

- `kLengthDelimitedString` (0): String or bytes field, buffer contains varint lengths followed by data
- `kLengthDelimitedStartOfSubmessage` (1): Marks start of embedded message
- `kLengthDelimitedEndOfSubmessage` (2): Marks end of embedded message

### Wire Type: START_GROUP (3) and END_GROUP (4)

Subtype is always 0 (`kTrivial`). No buffer. (Groups are deprecated in Protocol Buffers)

### HasSubtype

A state has a subtype byte in the subtypes array if:
- Wire type is `VARINT` (0)

For other wire types, the subtype is implicit (always 0).

### HasDataBuffer

A state has an associated data buffer if:
- Wire type is `VARINT` and subtype < 10 (not inline)
- Wire type is `FIXED32` or `FIXED64`
- Wire type is `LENGTH_DELIMITED` and subtype is 0 (string/bytes)

Special IDs:
- `kNonProto` (1): Has a data buffer for non-proto records
- `kNoOp` (0), `kStartOfSubmessage` (2), `kStartOfMessage` (3): No buffer

## Data Buffer Contents

### VARINT Fields

Buffer contains the varint values with continuation bits removed, one per field occurrence:
```
<varint_data_byte_1><varint_data_byte_2>...
```

Each varint value occupies `subtype + 1` bytes in the buffer.

**Example**: For varint value 300 (encoded as `0xAC 0x02` in normal protobuf):
- Subtype = 1 (2-byte varint)
- Buffer contains: `0x2C 0x02` (continuation bits removed: 0xAC → 0x2C)

### FIXED32 Fields

Buffer contains 4-byte little-endian values:
```
<value_1 (4 bytes)><value_2 (4 bytes)>...
```

### FIXED64 Fields

Buffer contains 8-byte little-endian values:
```
<value_1 (8 bytes)><value_2 (8 bytes)>...
```

### LENGTH_DELIMITED Fields (Strings/Bytes)

Buffer contains varint lengths followed by data:
```
<length_1 (varint64)><data_1><length_2 (varint64)><data_2>...
```

### Non-Proto Records

Non-proto records (arbitrary byte sequences that aren't valid Protocol Buffers) are stored in a special buffer associated with the `kNonProto` (1) reserved ID.

The buffer contains:
```
<length_1 (varint64)><data_1><length_2 (varint64)><data_2>...
```

## Transition Encoding

Transitions encode the sequence of state machine states traversed while parsing all messages. The transition stream represents how to navigate from one state to the next.

### Transition Values

Each transition is encoded as a value in the range [0, 63] that represents the relative offset from the current base:

```
transition_value = destination_state_index - current_base
```

Where `current_base` is the base index from the current state.

### Constraints

- Maximum transition value: 63 (`kMaxTransition`)
- Each state can reach states in range `[base, base + 63]`

### Multi-Step Transitions

If a destination is not directly reachable (outside the [base, base+63] range), the transition is encoded as multiple steps through intermediate `kNoOp` states:

```
<transition_to_noop_1><transition_to_noop_2>...<transition_to_destination>
```

Each intermediate step updates the current base, eventually reaching a base from which the destination is reachable.

### Implicit Transitions

If a state has exactly one possible destination (encoded as `base + num_states` in the base array), no transition byte is written. The decoder automatically transitions to that destination.

### Transition Byte Stream Format

Transitions are written as a sequence of bytes, where each byte is a transition value (0-63):

```
<transition_1 (byte)><transition_2 (byte)>...
```

The decoder:
1. Starts at `initial_state`
2. Reads the tag and processes the field
3. If not implicit transition, reads next transition byte
4. Calculates next state: `next_state = current_base + transition_value`
5. Updates current_base from new state
6. Repeats until all records processed

## Decoding Algorithm

### Initialization

1. Read and decompress header
2. Parse state machine
3. Optionally decompress data buckets (or decompress on-demand with field projection)
4. Decompress transitions

### Record Reconstruction

```python
def decode_records(state_machine, transitions, buffers, num_records):
    records = []
    state_index = initial_state
    transition_index = 0
    buffer_positions = [0] * len(buffers)
    
    for record_num in range(num_records):
        record = bytearray()
        
        # Reconstruct one record
        while not at_record_boundary(state_index):
            state = state_machine[state_index]
            tag = state.tag
            
            # Write tag to record
            if is_regular_tag(tag):
                write_varint(record, tag)
                
                # Write value based on wire type
                if has_buffer(state):
                    buffer_idx = state.buffer_index
                    value = read_value_from_buffer(
                        buffers[buffer_idx], 
                        buffer_positions[buffer_idx],
                        state.subtype
                    )
                    buffer_positions[buffer_idx] += value_size
                    write_value(record, value, get_wire_type(tag))
                elif is_inline_varint(state):
                    # Inline varint: extract value from subtype
                    value = state.subtype - 10
                    write_varint(record, value)
            
            # Get next state
            if state.has_implicit_transition():
                # Automatic transition
                state_index = get_implicit_destination(state)
            else:
                # Read transition from stream
                transition = transitions[transition_index]
                transition_index += 1
                state_index = state.base + transition
        
        records.append(bytes(record))
    
    return records
```

## Encoding Algorithm (Summary)

### 1. Parse Messages

For each Protocol Buffer message:
- Parse tags and values
- Decompose into tree structure based on field paths
- Track submessage boundaries
- Separate non-proto records

### 2. Build State Machine

- Create a node for each unique (parent_message_id, tag, subtype) tuple
- Build transition graph based on tag sequences
- Optimize frequent transitions into private lists
- Add `kNoOp` states for infrequent transitions
- Assign base indices to ensure all transitions fit in [0, 63]

### 3. Organize Buffers into Buckets

- Group buffers by type (varint, fixed32, fixed64, string, non-proto)
- Split into buckets to optimize compression vs. field projection tradeoff
- Default: Single bucket (best compression)
- Smaller buckets: Faster field projection, slightly worse compression

### 4. Write Chunk

1. Write compression type
2. Write compressed header with state machine
3. Write compressed data buckets
4. Write compressed transitions

## Implementation Notes

### Field Projection

Field projection allows reading only specific Protocol Buffer fields without decompressing all data. This is enabled by:
- Splitting buffers into multiple buckets
- Only decompressing buckets containing requested fields
- Following only relevant state machine paths

### Bucket Size Tuning

- **Larger buckets** (or single bucket): Better compression, slower field projection
- **Smaller buckets**: Faster field projection, slightly worse compression
- Typical: 1 MB chunk size with buckets sized as fraction of chunk size

### State Machine Optimization

The state machine is optimized for:
- **Space efficiency**: Transitions encoded in 1 byte (0-63)
- **Compression**: Similar state sequences compress well
- **Decode speed**: Simple arithmetic to compute next state

### Maximum Values

- Maximum transition: 63 (frozen in file format)
- Maximum varint length: 10 bytes
- Maximum inline varint value: 127
- Reserved IDs: 0-4 (IDs 5-7 are available for future use)

## Examples

### Example 1: Simple Message

Consider a single Protocol Buffer message:
```protobuf
message Person {
  optional int32 id = 1;
  optional string name = 2;
}
```

With data: `id=10, name="Alice"`

**Encoded as:**
- Tag 1: `(1 << 3) | 0 = 8` (varint)
- Value 10: Inline (subtype = 20)
- Tag 2: `(2 << 3) | 2 = 18` (length-delimited)
- Length: 5 (varint)
- Data: "Alice"

**Transposed representation:**

State machine states:
1. State 0: tag=3 (`kStartOfMessage`)
2. State 1: tag=8 (field 1, varint), subtype=20 (inline value 10)
3. State 2: tag=18 (field 2, string), subtype=0, buffer_index=0
4. (back to State 0 for next record)

Buffers:
- Buffer 0: `05 41 6C 69 63 65` (varint length 5, then "Alice")

Transitions:
- 0→1: transition byte = 1-0 = 1
- 1→2: transition byte = 2-1 = 1
- 2→0: transition byte = 0-2+num_states (depends on base assignment)

### Example 2: Nested Message

```protobuf
message Outer {
  optional Inner inner = 1;
  message Inner {
    optional int32 value = 1;
  }
}
```

With data: `inner.value=42`

**States:**
1. `kStartOfMessage` (3)
2. Tag 10 (field 1, length-delimited)
3. `kStartOfSubmessage` (2)
4. Tag 8 (field 1, varint) in submessage
5. Tag 10+4=14 (end of submessage, wire type 6)
6. Back to state 1

This demonstrates how submessages create message IDs and use special start/end markers.

## Compatibility Notes

- The transposed format is **transparent**: Files can be read without understanding the encoding if only the outer chunk structure is parsed
- A decoder that doesn't implement field projection can still decompress and decode entire chunks
- The format is designed to be **forward-compatible**: Unknown reserved IDs can be skipped
- **Backward compatibility**: New reserved IDs (5-7) may be added in future versions

## References

- Protocol Buffer encoding: https://developers.google.com/protocol-buffers/docs/encoding
- HighwayHash: https://github.com/google/highwayhash
- Main specification: [riegeli_records_file_format.md](riegeli_records_file_format.md)
- Data primitives: [data_encoding_primitives.md](data_encoding_primitives.md)
