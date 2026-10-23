# Riegeli Implementation Guide

This guide provides practical advice for implementing Riegeli/records readers and writers in a new programming language.

## Implementation Levels

You can implement Riegeli support at different levels of functionality:

### Level 1: Basic Reader (Minimal)

**Capabilities:**
- Read file signature
- Parse block headers
- Read simple chunks (chunk_type 'r')
- Support uncompressed files

**Requires:**
- Varint64 decoding
- Little-endian integer parsing
- Block header parsing
- Simple chunk parsing

**Use case:** Read Riegeli files written without compression or transposition.

### Level 2: Complete Reader

**Capabilities:**
- All of Level 1
- Brotli/Zstd/Snappy decompression
- Read transposed chunks (chunk_type 't')
- Seeking to block boundaries
- Error detection and recovery

**Requires:**
- All Level 1 components
- Compression library integration
- HighwayHash implementation or library
- State machine decoder for transposed chunks

**Use case:** Read any Riegeli file, including those with compression and Protocol Buffer transposition.

### Level 3: Basic Writer

**Capabilities:**
- Write file signature
- Write block headers
- Write simple chunks (chunk_type 'r')
- Support at least one compression algorithm (Brotli recommended)

**Requires:**
- Varint64 encoding
- Little-endian integer writing
- HighwayHash implementation
- At least one compression library

**Use case:** Create Riegeli files for any record type with compression.

### Level 4: Complete Writer

**Capabilities:**
- All of Level 3
- Write transposed chunks (chunk_type 't')
- Protocol Buffer parsing and transposition
- Multiple compression algorithms
- File metadata chunks
- Optimized buffering and parallel encoding

**Requires:**
- All Level 3 components
- Protocol Buffer wire format parser
- State machine builder for transposed chunks
- Advanced buffer management

**Use case:** Create optimally compressed Riegeli files for Protocol Buffer data.

## Recommended Implementation Order

### Phase 1: Reader Foundation (Days 1-3)

1. **Data primitives** (`data_encoding_primitives.md`):
   - Implement varint64 decoder
   - Test with various values: 0, 127, 128, 300, MAX_UINT64
   - Implement little-endian integer parsing

2. **Block structure**:
   - Implement block header parser
   - Handle 64 KiB block size and interruptions
   - Calculate chunk boundaries across blocks

3. **Basic chunk parsing**:
   - Read file signature (first 64 bytes)
   - Parse chunk headers (40 bytes)
   - Verify file signature magic bytes

### Phase 2: Simple Reader (Days 4-7)

1. **Simple chunk decoder**:
   - Parse simple chunk format (chunk_type 'r')
   - Decode sizes buffer
   - Decode values buffer
   - Reconstruct individual records

2. **Compression support**:
   - Integrate Brotli library
   - Handle compressed block format (decompressed_size + data)
   - Test with various compression types

3. **Hash verification**:
   - Integrate or implement HighwayHash
   - Verify block header hashes
   - Verify chunk header and data hashes
   - Handle corrupted data gracefully

### Phase 3: Transposed Reader (Days 8-15)

1. **Protocol Buffer basics**:
   - Understand wire types and tag format
   - Implement tag parsing (field_number << 3 | wire_type)

2. **Transposed chunk header**:
   - Parse bucket metadata
   - Parse buffer metadata
   - Parse state machine (4 arrays: tags, bases, subtypes, buffer_indices)
   - Identify initial state

3. **State machine decoder**:
   - Implement transition decoding
   - Handle implicit transitions
   - Navigate NoOp states
   - Decode transitions with base offset calculation

4. **Buffer reconstruction**:
   - Decompress buckets on demand
   - Extract values based on wire type and subtype
   - Handle inline varints (subtypes 10-137)
   - Reconstruct varint values with continuation bits

5. **Record assembly**:
   - Follow state machine to reconstruct messages
   - Write Protocol Buffer tags and values
   - Handle submessage boundaries
   - Process non-proto records

### Phase 4: Basic Writer (Days 16-25)

1. **Chunk construction**:
   - Implement varint64 encoder
   - Buffer records
   - Separate sizes and values

2. **Compression**:
   - Compress sizes buffer
   - Compress values buffer
   - Write compression metadata

3. **Block and chunk headers**:
   - Compute HighwayHash
   - Write block headers at 64 KiB boundaries
   - Calculate chunk boundaries and padding
   - Handle chunk interruption by block headers

4. **File structure**:
   - Write file signature chunk
   - Write optional metadata chunk
   - Write record chunks
   - Handle padding chunks if needed

### Phase 5: Transposed Writer (Days 26-45)

1. **Protocol Buffer parsing**:
   - Parse Protocol Buffer wire format
   - Extract tags, wire types, and values
   - Track submessage boundaries

2. **Tree building**:
   - Build tag tree with parent message IDs
   - Assign unique node IDs
   - Classify buffer types (varint, fixed32, fixed64, string, non-proto)

3. **Buffer organization**:
   - Separate values by (tag, subtype)
   - Group into buckets
   - Compute buffer lengths

4. **State machine construction**:
   - Create states for each unique (tag, subtype)
   - Build transition graph
   - Optimize frequent transitions
   - Assign base indices
   - Add NoOp states for infrequent transitions

5. **Encoding**:
   - Write transposed chunk header
   - Write compressed buckets
   - Write compressed transitions
   - Verify output is valid

## Key Implementation Challenges

### Challenge 1: Block Header Interruption

Block headers can interrupt chunks at **any point**, including mid-chunk-header.

**Solution:**
- When reading: Skip block headers during sequential reading
- When writing: Account for block header overhead in chunk size calculations
- Use formulas from spec:
  ```
  NumOverheadBlocks(pos, size) = 
      (size + (pos + kUsableBlockSize - 1) % kBlockSize) / kUsableBlockSize
  AddWithOverhead(pos, size) = 
      pos + size + NumOverheadBlocks(pos, size) * kBlockHeaderSize
  ```

### Challenge 2: State Machine Navigation

Transitions are limited to offset [0, 63], requiring multi-hop navigation.

**Solution:**
- Implement NoOp state handling
- Follow canonical_source chain for unreachable states
- Cache base indices for performance
- Test with deeply nested Protocol Buffers

### Challenge 3: Varint Continuation Bits

Transposed chunks store varints **without continuation bits**, requiring reconstruction.

**Solution:**
- Strip continuation bits when transposing (encoder)
- Add continuation bits when decoding (decoder)
- Track subtype to know varint length
- Example: `0xAC 0x02` (normal) → `0x2C 0x02` (transposed, length=2)

### Challenge 4: Buffer Index Mapping

State machine references buffers by index, requiring correct mapping.

**Solution:**
- Build map of (tag, subtype) → buffer_index during header parsing
- Use HasDataBuffer() logic to determine which states have buffers
- Maintain buffer position cursors during decoding

### Challenge 5: Submessage Handling

Submessages create new message IDs and use special markers.

**Solution:**
- Use stack to track message nesting
- Recognize kStartOfSubmessage (2) and kSubmessageWireType (6)
- End marker: original_tag + 4 (modified wire type from 2 to 6)
- Assign unique message IDs to each submessage instance

## Testing Strategy

### Unit Tests

1. **Primitives:**
   - Varint: 0, 1, 127, 128, 300, MAX_UINT32, MAX_UINT64
   - Fixed integers: 0, 1, MAX_UINT32, MAX_UINT64
   - Block calculations: Various positions and sizes

2. **Simple chunks:**
   - Empty chunk (0 records)
   - Single record
   - Multiple records of varying sizes
   - Records with 0 length
   - Uncompressed and compressed variants

3. **Transposed chunks:**
   - Single flat message
   - Multiple flat messages
   - Nested messages (1, 2, 3+ levels)
   - Inline varints (values 0-127)
   - All wire types (varint, fixed32, fixed64, length-delimited)
   - Non-proto records mixed with proto records

### Integration Tests

1. **Interoperability:**
   - Read files created by C++ implementation
   - Have C++ implementation read your files
   - Test with real-world Protocol Buffer schemas

2. **Corruption handling:**
   - Corrupted block headers
   - Corrupted chunk headers
   - Corrupted data
   - Truncated files
   - Verify hash mismatches are detected

3. **Edge cases:**
   - Files at exactly 64 KiB boundaries
   - Chunks spanning multiple blocks
   - Very large records (> 64 KiB)
   - Very large files (> 1 GB)

### Fuzzing

Consider fuzzing your implementation with:
- Random valid files
- Files with random corruption
- Protocol Buffers with random schemas
- Edge case inputs (empty fields, max values, deep nesting)

## Performance Optimization

### Reader Optimizations

1. **Buffered I/O**: Read larger blocks (e.g., 64 KiB) at a time
2. **Memory mapping**: Use mmap for large files on supported platforms
3. **Lazy decompression**: Only decompress buckets when needed (field projection)
4. **State caching**: Cache frequently accessed state machine nodes
5. **Buffer pooling**: Reuse decompression buffers

### Writer Optimizations

1. **Buffering**: Accumulate records before creating chunks
2. **Parallel compression**: Compress chunks in background threads
3. **Bucket tuning**: Adjust bucket size for compression/projection tradeoff
4. **State machine caching**: Reuse state machines for similar schemas
5. **Compression level tuning**: Balance speed vs. size

## Common Pitfalls

### Pitfall 1: Ignoring Block Headers
❌ **Wrong:** Read file sequentially without handling block headers
✅ **Right:** Skip or account for 24-byte block headers at every 64 KiB boundary

### Pitfall 2: Incorrect Varint Encoding
❌ **Wrong:** Use big-endian or forget continuation bits
✅ **Right:** Use little-endian with MSB as continuation bit

### Pitfall 3: Missing Hash Verification
❌ **Wrong:** Trust all data without verification
✅ **Right:** Verify HighwayHash on headers and data, handle mismatches gracefully

### Pitfall 4: Incorrect Transition Decoding
❌ **Wrong:** Use absolute state indices from transition bytes
✅ **Right:** Add transition byte to current base: `next_state = base + transition`

### Pitfall 5: Wrong Subtype Handling
❌ **Wrong:** Assume all states have subtypes
✅ **Right:** Only VARINT wire type has subtypes; check with HasSubtype()

### Pitfall 6: Buffer Position Bugs
❌ **Wrong:** Reset buffer positions for each record
✅ **Right:** Maintain persistent buffer positions across all records

### Pitfall 7: Endianness Errors
❌ **Wrong:** Use native endianness
✅ **Right:** Always use little-endian for integers, even on big-endian platforms

## Language-Specific Considerations

### C/C++
- **Pros:** Direct memory access, excellent performance
- **Cons:** Manual memory management
- **Tips:** Use existing HighwayHash library, consider memory-mapped I/O

### Java
- **Pros:** Good library ecosystem, Protocol Buffer support
- **Cons:** GC pressure from buffer allocation
- **Tips:** Use ByteBuffer for endianness handling, pool buffers

### Python
- **Pros:** Easy prototyping, good for testing
- **Cons:** Performance overhead
- **Tips:** Use `struct` module for binary parsing, consider Cython for critical paths

### Go
- **Pros:** Good concurrency, reasonable performance
- **Cons:** Limited SIMD support for hashing
- **Tips:** Use `encoding/binary` for endianness, goroutines for parallel compression

### Rust
- **Pros:** Safety + performance, excellent for compression
- **Cons:** Learning curve for borrow checker
- **Tips:** Use `byteorder` crate, leverage zero-copy where possible

## Reference Implementation

The C++ implementation in this repository is the reference:
- `riegeli/chunk_encoding/simple_encoder.h`: Simple chunk writer
- `riegeli/chunk_encoding/simple_decoder.h`: Simple chunk reader
- `riegeli/chunk_encoding/transpose_encoder.h`: Transposed chunk writer
- `riegeli/chunk_encoding/transpose_decoder.h`: Transposed chunk reader
- `riegeli/records/record_writer.h`: High-level writer API
- `riegeli/records/record_reader.h`: High-level reader API

## Validation Tools

Use these tools to validate your implementation:

1. **riegeli_summary**: Inspect file structure
   ```bash
   bazel run //riegeli/records/tools:riegeli_summary -- your_file.riegeli
   ```

2. **describe_riegeli_file**: Show detailed chunk information
   ```bash
   bazel run //riegeli/records/tools:describe_riegeli_file -- your_file.riegeli
   ```

3. **Python bindings**: Compare output with Python implementation
   ```python
   import riegeli
   with riegeli.RecordReader(file_path) as reader:
       for record in reader:
           print(record)
   ```

## Resources

- **Main specification**: [riegeli_records_file_format.md](riegeli_records_file_format.md)
- **Data primitives**: [data_encoding_primitives.md](data_encoding_primitives.md)
- **Transposed format**: [transposed_chunk_format.md](transposed_chunk_format.md)
- **Writer options**: [record_writer_options.md](record_writer_options.md)
- **HighwayHash**: https://github.com/google/highwayhash
- **Protocol Buffers**: https://developers.google.com/protocol-buffers/docs/encoding
- **Brotli**: https://github.com/google/brotli
- **Zstd**: https://facebook.github.io/zstd/
- **Snappy**: https://google.github.io/snappy/

## Support

For questions or issues with your implementation:
1. Review the specifications thoroughly
2. Check the C++ reference implementation
3. File issues on GitHub: https://github.com/google/riegeli/issues
4. Include details: language, level attempting, specific error

## License Note

The file format specification is openly documented and can be implemented freely. Individual compression libraries (Brotli, Zstd, Snappy) have their own licenses - check their documentation.
