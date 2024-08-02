// Copyright 2024 Google LLC
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

#include "riegeli/base/byte_fill.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <ios>
#include <limits>
#include <ostream>

#include "absl/numeric/bits.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/global.h"
#include "riegeli/base/shared_buffer.h"
#include "riegeli/base/types.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr size_t ByteFill::kBlockOfZerosSize;
constexpr size_t ByteFill::Blocks::SmallBlock::kSize;
#endif

Chain::Block ByteFill::ZeroBlock::ToChainBlock(absl::string_view substr) {
  if (substr.size() == kBlockOfZerosSize) {
    return Global([] {
      return Chain::Block(ZeroBlock(),
                          absl::string_view(BlockOfZeros(), kBlockOfZerosSize));
    });
  }
  return Chain::Block(ZeroBlock(), substr);
}

absl::Cord ByteFill::ZeroBlock::ToCord(absl::string_view substr) {
  static constexpr auto kNullReleaser = [] {};
  if (substr.size() == kBlockOfZerosSize) {
    return Global([] {
      return absl::MakeCordFromExternal(
          absl::string_view(BlockOfZeros(), kBlockOfZerosSize), kNullReleaser);
    });
  }
  return absl::MakeCordFromExternal(substr, kNullReleaser);
}

void ByteFill::ZeroBlock::DumpStructure(std::ostream& out) {
  out << "[zero_fill] { }";
}

ByteFill::Blocks::Blocks(Position size, char fill) {
  if (size == 0) return;
  if (fill == '\0') {
    RIEGELI_ASSERT(absl::holds_alternative<ZeroBlock>(block_));
    num_blocks_ = (size - 1) / kBlockOfZerosSize + 1;
    non_last_block_size_ = uint32_t{kBlockOfZerosSize};
    last_block_size_ =
        static_cast<uint32_t>(size - 1) % uint32_t{kBlockOfZerosSize} + 1;
    data_ = BlockOfZeros();
    return;
  }
  if (size <= SmallBlock::kSize) {
    num_blocks_ = 1;
    last_block_size_ = IntCast<uint32_t>(size);
    data_ = block_.emplace<SmallBlock>(fill).data();
    return;
  }
  // Find a balance between the number of blocks and the block size.
  // The following parameters yield:
  //  *    1K =   1 *  1K
  //  *    2K =   1 *  2K
  //  *    4K =   2 *  2K
  //  *    8K =   2 *  4K
  //  *   16K =   4 *  4K
  //  *   32K =   4 *  8K
  //  *   64K =   8 *  8K
  //  *  128K =   8 * 16K
  //  *  256K =  16 * 16K
  //  *  512K =  16 * 32K
  //  * 1M    =  32 * 32K
  //  * 2M    =  32 * 64K
  //  * 4M    =  64 * 64K
  //  * 8M    = 128 * 64K
  static constexpr int kBlockSizeBitsBias = 10;
  if (size <= Position{1} << (kBlockSizeBitsBias + 1)) {
    num_blocks_ = 1;
    non_last_block_size_ = IntCast<uint32_t>(size);
    last_block_size_ = non_last_block_size_;
  } else {
    const int block_size_bits =
        SignedMin((kBlockSizeBitsBias + absl::bit_width(size)) / 2, 16);
    num_blocks_ = ((size - 1) >> block_size_bits) + 1;
    non_last_block_size_ = uint32_t{1} << block_size_bits;
    last_block_size_ =
        (static_cast<uint32_t>(size - 1) & (non_last_block_size_ - 1)) + 1;
  }
  SharedBuffer& shared_buffer =
      block_.emplace<SharedBuffer>(non_last_block_size_);
  std::memset(shared_buffer.mutable_data(), fill, non_last_block_size_);
  data_ = shared_buffer.data();
}

inline const char* ByteFill::BlockOfZeros() {
  return Global([] { return new char[kBlockOfZerosSize](); });
}

ByteFill::operator Chain() const {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max())
      << "Chain size overflow";
  Chain dest;
  Chain::Options options;
  options.set_size_hint(IntCast<size_t>(size_));
  for (const BlockRef block : this->blocks()) {
    dest.Append(block, options);
  }
  return dest;
}

ByteFill::operator absl::Cord() const {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max())
      << "Cord size overflow";
  absl::Cord dest;
  for (const BlockRef block : this->blocks()) {
    ExternalRef(block).AppendTo(dest);
  }
  return dest;
}

void ByteFill::AppendTo(Chain& dest) const {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Chain size overflow";
  for (const BlockRef block : this->blocks()) {
    dest.Append(block);
  }
}

void ByteFill::AppendTo(Chain& dest, Chain::Options options) const {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Chain size overflow";
  for (const BlockRef block : this->blocks()) {
    dest.Append(block, options);
  }
}

void ByteFill::AppendTo(absl::Cord& dest) const {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Cord size overflow";
  for (const BlockRef block : this->blocks()) {
    ExternalRef(block).AppendTo(dest);
  }
}

void ByteFill::PrependTo(Chain& dest) const {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Chain size overflow";
  const Blocks blocks = this->blocks();
  for (BlockIterator iter = blocks.cend(); iter != blocks.cbegin();) {
    --iter;
    dest.Prepend(*iter);
  }
}

void ByteFill::PrependTo(Chain& dest, Chain::Options options) const {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Chain size overflow";
  const Blocks blocks = this->blocks();
  for (BlockIterator iter = blocks.cend(); iter != blocks.cbegin();) {
    --iter;
    dest.Prepend(*iter, options);
  }
}

void ByteFill::PrependTo(absl::Cord& dest) const {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Cord size overflow";
  const Blocks blocks = this->blocks();
  for (BlockIterator iter = blocks.cend(); iter != blocks.cbegin();) {
    --iter;
    ExternalRef(*iter).PrependTo(dest);
  }
}

void ByteFill::Output(std::ostream& out) const {
  for (const absl::string_view fragment : blocks()) {
    out.write(fragment.data(), IntCast<std::streamsize>(fragment.size()));
  }
}

}  // namespace riegeli
