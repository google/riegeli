// Copyright 2017 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RIEGELI_RECORDS_BLOCK_H_
#define RIEGELI_RECORDS_BLOCK_H_

#include <stddef.h>
#include <stdint.h>

#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/constexpr.h"
#include "riegeli/base/types.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/hash.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/endian/endian_writing.h"

namespace riegeli {
namespace records_internal {

class BlockHeader {
 public:
  BlockHeader() = default;

  explicit BlockHeader(uint64_t previous_chunk, uint64_t next_chunk) {
    set_previous_chunk(previous_chunk);
    set_next_chunk(next_chunk);
    set_header_hash(computed_header_hash());
  }

  BlockHeader(const BlockHeader& that) = default;
  BlockHeader& operator=(const BlockHeader& that) = default;

  char* bytes() { return bytes_; }
  const char* bytes() const { return bytes_; }
  static constexpr size_t size() { return sizeof(bytes_); }

  uint64_t computed_header_hash() const {
    return chunk_encoding_internal::Hash(absl::string_view(
        bytes() + sizeof(uint64_t), size() - sizeof(uint64_t)));
  }
  uint64_t stored_header_hash() const { return ReadLittleEndian64(bytes_); }
  uint64_t previous_chunk() const {
    return ReadLittleEndian64(bytes_ + sizeof(uint64_t));
  }
  uint64_t next_chunk() const {
    return ReadLittleEndian64(bytes_ + 2 * sizeof(uint64_t));
  }

 private:
  void set_header_hash(uint64_t value) { WriteLittleEndian64(value, bytes_); }
  void set_previous_chunk(uint64_t value) {
    WriteLittleEndian64(value, bytes_ + sizeof(uint64_t));
  }
  void set_next_chunk(uint64_t value) {
    WriteLittleEndian64(value, bytes_ + 2 * sizeof(uint64_t));
  }

  // Representation (Little Endian):
  //  - `uint64_t`: `header_hash`
  //  - `uint64_t`: `previous_chunk`
  //  - `uint64_t`: `next_chunk`
  char bytes_[3 * sizeof(uint64_t)];
};

RIEGELI_INLINE_CONSTEXPR(Position, kBlockSize, Position{1} << 16);

RIEGELI_INLINE_CONSTEXPR(Position, kUsableBlockSize,
                         kBlockSize - BlockHeader::size());

// Whether `pos` is a block boundary (immediately before a block header).
inline bool IsBlockBoundary(Position pos) { return pos % kBlockSize == 0; }

// The nearest block boundary at or before `pos`.
inline Position RoundDownToBlockBoundary(Position pos) {
  return pos - pos % kBlockSize;
}

// How many bytes remain until the end of the block (0 at a block boundary).
inline Position RemainingInBlock(Position pos) { return (-pos) % kBlockSize; }

// Whether `pos` is a possible chunk boundary (not inside nor immediately after
// a block header).
inline bool IsPossibleChunkBoundary(Position pos) {
  return RemainingInBlock(pos) < kUsableBlockSize;
}

// The nearest possible chunk boundary at or after `pos` (chunk boundaries are
// not valid inside or immediately after a block header).
inline Position RoundUpToPossibleChunkBoundary(Position pos) {
  return pos + SaturatingSub(RemainingInBlock(pos), kUsableBlockSize - 1);
}

// If `pos` is immediately before or inside a block header, how many bytes
// remain until the end of the block header, otherwise 0.
inline size_t RemainingInBlockHeader(Position pos) {
  return SaturatingSub(BlockHeader::size(), pos % kBlockSize);
}

// For a chunk beginning at `chunk_begin`, the position after `length`, adding
// intervening block headers.
inline Position AddWithOverhead(Position chunk_begin, Position length) {
  RIEGELI_ASSERT_LT(RemainingInBlock(chunk_begin), kUsableBlockSize)
      << "Failed precondition of AddWithOverhead(): invalid chunk boundary";
  const Position num_overhead_blocks =
      (length + (chunk_begin + kUsableBlockSize - 1) % kBlockSize) /
      kUsableBlockSize;
  return chunk_begin + length + num_overhead_blocks * BlockHeader::size();
}

// For a chunk beginning at `chunk_begin`, the length until `pos`, subtracting
// intervening block headers.
inline Position DistanceWithoutOverhead(Position chunk_begin, Position pos) {
  RIEGELI_ASSERT_LE(chunk_begin, pos)
      << "Failed precondition of DistanceWithoutOverhead(): "
         "positions in the wrong order";
  const Position num_overhead_blocks =
      pos / kBlockSize - chunk_begin / kBlockSize;
  return (pos - UnsignedMin(pos % kBlockSize, BlockHeader::size())) -
         (chunk_begin -
          UnsignedMin(chunk_begin % kBlockSize, BlockHeader::size())) -
         num_overhead_blocks * BlockHeader::size();
}

// The position after a chunk which begins at `chunk_begin`.
inline Position ChunkEnd(const ChunkHeader& header, Position chunk_begin) {
  return UnsignedMax(
      AddWithOverhead(chunk_begin, header.size() + header.data_size()),
      RoundUpToPossibleChunkBoundary(chunk_begin + header.num_records()));
}

}  // namespace records_internal
}  // namespace riegeli

#endif  // RIEGELI_RECORDS_BLOCK_H_
