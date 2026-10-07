# Riegeli/records Documentation

This directory contains comprehensive documentation for the Riegeli/records file format, designed to enable implementation in any programming language.

## Start Here

**New to Riegeli?** Start with [index.md](index.md) for an overview and navigation guide.

**Implementing Riegeli?** Follow this path:
1. [implementation_guide.md](implementation_guide.md) - Step-by-step implementation roadmap
2. [quick_reference.md](quick_reference.md) - Constants and structures reference  
3. [format_examples.md](format_examples.md) - Concrete byte-level examples
4. Detailed specs as needed (see below)

**Quick lookup?** Jump to [quick_reference.md](quick_reference.md).

## Documentation Structure

### 📋 Quick Start

| Document | Purpose | When to Use |
|----------|---------|-------------|
| [index.md](index.md) | Navigation hub | Finding the right doc |
| [quick_reference.md](quick_reference.md) | Fast lookup reference | Quick checks during implementation |
| [DOCUMENTATION_SUMMARY.md](DOCUMENTATION_SUMMARY.md) | What's documented | Understanding documentation scope |

### 📖 File Format Specifications

| Document | Purpose | Completeness |
|----------|---------|--------------|
| [riegeli_records_file_format.md](riegeli_records_file_format.md) | Main format specification | **Complete** - All chunk types, block/chunk headers, file structure |
| [data_encoding_primitives.md](data_encoding_primitives.md) | Low-level encoding details | **Complete** - Varints, fixed integers, compression, hashing |
| [transposed_chunk_format.md](transposed_chunk_format.md) | Transposed chunk specification | **Complete** - State machine, buffers, transitions, algorithms |

### ⚙️ Configuration

| Document | Purpose |
|----------|---------|
| [record_writer_options.md](record_writer_options.md) | Writer options reference - compression, chunk size, padding, parallelism |

### 🛠️ Implementation Resources

| Document | Purpose | Target Audience |
|----------|---------|-----------------|
| [implementation_guide.md](implementation_guide.md) | Phased implementation roadmap with testing strategy | **Implementers** |
| [format_examples.md](format_examples.md) | 11 byte-level examples with hex dumps | **Implementers & Testers** |

## Reading Paths

### Path 1: Understanding the Format (Readers)

```
1. index.md → Overview
2. quick_reference.md → Constants and basic structures
3. riegeli_records_file_format.md → Block/chunk structure
   ├─ Block headers (seeking, error recovery)
   ├─ Chunk headers (hashing, verification)
   └─ Chunk types (signature, metadata, padding)
4. data_encoding_primitives.md → Varints, hashing, compression
5. Simple chunks:
   └─ riegeli_records_file_format.md § "Simple chunk with records"
6. Transposed chunks:
   └─ transposed_chunk_format.md (complete specification)
```

### Path 2: Implementing a Reader (Fast Track)

```
1. implementation_guide.md → Phase 1-3 (Days 1-15)
2. format_examples.md → Examples 1-7 (Simple chunks)
3. quick_reference.md → As needed for lookups
4. riegeli_records_file_format.md → Reference for details
5. data_encoding_primitives.md → Varint/compression details
6. For transposed support:
   ├─ transposed_chunk_format.md
   └─ format_examples.md → Examples 8-11
```

### Path 3: Implementing a Writer

```
1. implementation_guide.md → Phase 4 (Days 16-25)
2. format_examples.md → Examples 1-4 (Simple chunk construction)
3. riegeli_records_file_format.md → Block header insertion
4. data_encoding_primitives.md → Hashing requirements
5. For transposed support:
   ├─ implementation_guide.md → Phase 5 (Days 26-45)
   ├─ transposed_chunk_format.md → State machine construction
   └─ format_examples.md → Examples 8-11
```

### Path 4: Debugging Issues

```
1. format_examples.md → Compare your output with examples
2. quick_reference.md → Verify constants and structures
3. implementation_guide.md § "Common Pitfalls"
4. Detailed specs for specific issues:
   ├─ Block issues → riegeli_records_file_format.md § "Block header"
   ├─ Varint issues → data_encoding_primitives.md § "Varint Encoding"
   ├─ State machine issues → transposed_chunk_format.md § "State Machine"
   └─ Transition issues → transposed_chunk_format.md § "Transition Encoding"
```

## What Each Document Contains

### [index.md](index.md)
- Overview of Riegeli/records
- Links to all documentation
- Document descriptions

### [quick_reference.md](quick_reference.md)
- Constants (block size, header sizes, max values)
- Chunk types table
- Compression types table
- Block/chunk header layouts
- Wire types and tag format
- Varint examples
- Format diagrams
- Pseudocode algorithms
- Implementation checklists

### [riegeli_records_file_format.md](riegeli_records_file_format.md)
- Overall file structure
- Block headers (24 bytes) - for seeking and error recovery
- Chunk headers (40 bytes) - size, type, hashes
- File signature chunk (required at start)
- File metadata chunk (optional RecordsMetadata proto)
- Padding chunks (for alignment)
- Simple chunks (sizes + values format)
- Transposed chunks (overview, links to full spec)
- Implementation formulas

### [data_encoding_primitives.md](data_encoding_primitives.md)
- Varint encoding format with examples
- Canonical vs. non-canonical varints
- Fixed-width integers (little-endian)
- Compressed block format (size prefix + data)
- Compression types (None, Brotli, Zstd, Snappy)
- HighwayHash configuration and usage
- Protocol Buffer wire format basics
- Block size and alignment rules

### [transposed_chunk_format.md](transposed_chunk_format.md)
- Overview of transposition (columnar format)
- Complete format specification:
  - Compression type
  - Header format (buckets, buffers, state machine)
  - Data buckets
  - Transitions
- Subtypes for all wire types
- Reserved IDs (kNoOp, kNonProto, etc.)
- Data buffer contents by type
- State machine structure (4 arrays)
- Transition encoding (0-63 range, multi-hop)
- Complete decoding algorithm
- Complete encoding algorithm
- Field projection explanation
- Examples (flat, nested, non-proto)

### [record_writer_options.md](record_writer_options.md)
- Option string format
- Transpose on/off
- Compression algorithms (Brotli, Zstd, Snappy)
- Compression levels
- Window size tuning
- Chunk size configuration
- Bucket fraction (for field projection)
- Padding options
- Parallelism configuration

### [implementation_guide.md](implementation_guide.md)
- Four implementation levels:
  1. Basic Reader (minimal, uncompressed only)
  2. Complete Reader (all features)
  3. Basic Writer (simple chunks)
  4. Complete Writer (transposed chunks)
- Phased implementation timeline (45 days)
- Day-by-day breakdown for each phase
- Key implementation challenges with solutions:
  - Block header interruption
  - State machine navigation
  - Varint continuation bits
  - Buffer index mapping
  - Submessage handling
- Testing strategy (unit, integration, fuzzing)
- Performance optimizations
- Common pitfalls with corrections
- Language-specific considerations
- C++ reference implementation pointers
- Validation tools usage

### [format_examples.md](format_examples.md)
- 11 detailed examples with hex dumps:
  1. Minimal file (signature only)
  2. Simple chunk, one record
  3. Simple chunk, three records
  4. Simple chunk with compression
  5. Block header interrupting chunk
  6. Varint encoding examples
  7. Protocol Buffer tag examples
  8. Simple transposed chunk
  9. Inline varint in transposed chunk
  10. Nested message transposed
  11. Non-proto record handling
- Test vectors for validation
- Instructions for generating test files
- Binary viewing tool recommendations

### [DOCUMENTATION_SUMMARY.md](DOCUMENTATION_SUMMARY.md)
- What was added (5 new documents, 2 enhanced)
- What was missing before (transposed format, etc.)
- Why documentation is now implementation-ready
- How to use the documentation
- Statistics (6000+ lines added)
- Future enhancement ideas

## Implementation Status

| Feature | Specification Status | C++ Implementation |
|---------|---------------------|-------------------|
| File signature | ✅ Complete | ✅ Available |
| Block headers | ✅ Complete | ✅ Available |
| Chunk headers | ✅ Complete | ✅ Available |
| Simple chunks | ✅ Complete with examples | ✅ Available |
| Transposed chunks | ✅ Complete with examples | ✅ Available |
| Metadata chunks | ✅ Complete with proto | ✅ Available |
| Padding chunks | ✅ Complete | ✅ Available |
| Compression (all types) | ✅ Complete | ✅ Available |
| Field projection | ✅ Explained | ✅ Available |
| Seeking | ✅ Formulas provided | ✅ Available |
| Error recovery | ✅ Explained | ✅ Available |

## Validation

All specifications have been validated against the C++ reference implementation in this repository. Implementations based on this documentation should be fully interoperable with the C++ implementation.

### How to Validate Your Implementation

1. **Generate test files** using C++ tools:
   ```bash
   bazel run //riegeli/records/tools:records_tool -- --output=test.riegeli
   ```

2. **Compare with examples**:
   ```bash
   hexdump -C test.riegeli
   # Compare with format_examples.md
   ```

3. **Verify with C++ reader**:
   ```bash
   bazel run //riegeli/records/tools:describe_riegeli_file -- your_file.riegeli
   ```

4. **Cross-test**:
   - Read C++ generated files with your reader
   - Have C++ reader read your writer's output

## Getting Help

### Documentation Issues

If you find errors, unclear sections, or missing information:
1. Check cross-references to other documents
2. Look for examples in format_examples.md
3. Review the C++ implementation for clarification
4. File an issue: https://github.com/google/riegeli/issues

### Implementation Questions

For questions during implementation:
1. Check implementation_guide.md § "Common Pitfalls"
2. Review format_examples.md for concrete examples
3. Consult quick_reference.md for quick checks
4. File an issue with:
   - Your language/platform
   - Implementation level attempting
   - Specific problem description
   - What you've tried

## Contributing

If you'd like to improve this documentation:
1. Ensure changes maintain consistency across documents
2. Add examples for new concepts
3. Update quick_reference.md if adding constants
4. Update DOCUMENTATION_SUMMARY.md with changes
5. Cross-reference between documents
6. Validate against C++ implementation

## Tools

### C++ Tools in This Repository

Located in `riegeli/records/tools/`:
- `records_tool` - Read/write Riegeli files
- `describe_riegeli_file` - Detailed file analysis
- `riegeli_summary` - File structure overview

### Binary Viewing Tools

Recommended for examining files:
- `hexdump -C file.riegeli`
- `xxd file.riegeli`
- `od -A x -t x1z -v file.riegeli`
- Hex editors: HxD, Hex Fiend, ghex

## External Resources

- **HighwayHash**: https://github.com/google/highwayhash
- **Protocol Buffers**: https://developers.google.com/protocol-buffers/docs/encoding
- **Brotli**: https://github.com/google/brotli
- **Zstd**: https://facebook.github.io/zstd/
- **Snappy**: https://google.github.io/snappy/

## License

This documentation is part of the Riegeli project and is licensed under Apache License 2.0.

---

**Ready to implement?** Start with [implementation_guide.md](implementation_guide.md)!

**Need a quick answer?** Check [quick_reference.md](quick_reference.md)!

**Want examples?** See [format_examples.md](format_examples.md)!
