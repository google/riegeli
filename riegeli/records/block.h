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

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/endian.h"
#include "riegeli/base/string_view.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/hash.h"

namespace riegeli {
namespace internal {

class BlockHeader {
 public:
  BlockHeader() = default;

  BlockHeader(uint64_t previous_chunk, uint64_t next_chunk) {
    set_previous_chunk(previous_chunk);
    set_next_chunk(next_chunk);
    set_header_hash(computed_header_hash());
  }

  BlockHeader(const BlockHeader&) noexcept = default;
  BlockHeader& operator=(const BlockHeader&) noexcept = default;

  char* bytes() { return reinterpret_cast<char*>(words_); }
  const char* bytes() const { return reinterpret_cast<const char*>(words_); }
  static constexpr size_t size() { return sizeof(words_); }

  uint64_t computed_header_hash() const {
    return internal::Hash(string_view(reinterpret_cast<const char*>(words_ + 1),
                                      size() - sizeof(uint64_t)));
  }
  uint64_t stored_header_hash() const { return ReadLittleEndian64(words_[0]); }
  uint64_t previous_chunk() const { return ReadLittleEndian64(words_[1]); }
  uint64_t next_chunk() const { return ReadLittleEndian64(words_[2]); }

 private:
  void set_header_hash(uint64_t value) {
    words_[0] = WriteLittleEndian64(value);
  }
  void set_previous_chunk(uint64_t value) {
    words_[1] = WriteLittleEndian64(value);
  }
  void set_next_chunk(uint64_t value) {
    words_[2] = WriteLittleEndian64(value);
  }

  uint64_t words_[3];
};

constexpr Position kBlockSize() { return Position{1} << 16; }

constexpr Position kUsableBlockSize() {
  return kBlockSize() - BlockHeader::size();
}

// Subtraction of unsigned values, or 0 if that would underflow.
template <typename T>
T SaturatingSub(T a, T b) {
  return a > b ? a - b : 0;
}

// Whether pos is a block boundary (immediately before a block header).
inline bool IsBlockBoundary(Position pos) { return pos % kBlockSize() == 0; }

// How many bytes remain until the end of the block (0 at a block boundary).
inline Position RemainingInBlock(Position pos) { return (-pos) % kBlockSize(); }

// Whether pos is a possible chunk boundary (not inside nor immediately after
// a block header).
inline bool IsPossibleChunkBoundary(Position pos) {
  return RemainingInBlock(pos) < kUsableBlockSize();
}

// The nearest possible chunk boundary at or after pos (chunk boundaries are not
// valid inside or immediately after a block header).
inline Position RoundToPossibleChunkBoundary(Position pos) {
  return pos + SaturatingSub(RemainingInBlock(pos), kUsableBlockSize() - 1);
}

// If pos is immediately before or inside a block header, how many bytes remain
// until the end of the block header, otherwise 0.
inline size_t RemainingInBlockHeader(Position pos) {
  return SaturatingSub(BlockHeader::size(),
                       static_cast<size_t>(pos % kBlockSize()));
}

// For a given data size beginning at the given position, the position after
// the data which includes intervening block headers.
inline Position AddWithOverhead(Position pos, Position size) {
  RIEGELI_ASSERT_LT(RemainingInBlock(pos), kUsableBlockSize());
  const Position num_overhead_blocks =
      (size + (pos + kUsableBlockSize() - 1) % kBlockSize()) /
      kUsableBlockSize();
  return pos + size + num_overhead_blocks * BlockHeader::size();
}

// The position after a chunk which begins at the given position.
inline Position ChunkEnd(const ChunkHeader& header, Position chunk_begin) {
  return UnsignedMax(
      AddWithOverhead(chunk_begin, header.size() + header.data_size()),
      RoundToPossibleChunkBoundary(chunk_begin + header.num_records()));
}

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_RECORDS_BLOCK_H_
