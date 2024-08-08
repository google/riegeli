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
#include <utility>

#include "absl/numeric/bits.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "absl/types/variant.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/global.h"
#include "riegeli/base/shared_buffer.h"
#include "riegeli/base/types.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr size_t ByteFill::ZeroBlock::kSize;
constexpr size_t ByteFill::SmallBlock::kSize;
constexpr int ByteFill::Blocks::kBlockSizeBitsBias;
constexpr Position ByteFill::Blocks::kMaxSizeForSingleBlock;
#endif

inline const char* ByteFill::ZeroBlock::Data() {
  return Global([] { return new char[kSize](); });
}

Chain::Block ByteFill::ZeroBlock::ToChainBlock(absl::string_view substr) {
  if (substr.size() == kSize) {
    return Global([] {
      return Chain::Block(ZeroBlock(), absl::string_view(Data(), kSize));
    });
  }
  return Chain::Block(ZeroBlock(), substr);
}

absl::Cord ByteFill::ZeroBlock::ToCord(absl::string_view substr) {
  static constexpr auto kNullReleaser = [] {};
  if (substr.size() == kSize) {
    return Global([] {
      return absl::MakeCordFromExternal(absl::string_view(Data(), kSize),
                                        kNullReleaser);
    });
  }
  return absl::MakeCordFromExternal(substr, kNullReleaser);
}

void ByteFill::ZeroBlock::DumpStructure(std::ostream& out) {
  out << "[zero_fill] { }";
}

void ByteFill::LargeBlock::DumpStructure(absl::string_view substr,
                                         std::ostream& out) const {
  out << "[large_fill] {";
  const size_t ref_count = buffer_.GetRefCount();
  if (ref_count != 1) out << " ref_count: " << ref_count;
  if (buffer_.capacity() != substr.size()) {
    out << " capacity: " << buffer_.capacity();
  }
  out << " }";
}

ByteFill::Blocks::Blocks(Position size, char fill) {
  if (size == 0) return;
  if (fill == '\0') {
    RIEGELI_ASSERT(absl::holds_alternative<ZeroBlock>(block_));
    num_blocks_ = (size - 1) / ZeroBlock::kSize + 1;
    non_last_block_size_ = uint32_t{ZeroBlock::kSize};
    last_block_size_ =
        static_cast<uint32_t>(size - 1) % uint32_t{ZeroBlock::kSize} + 1;
    data_ = ZeroBlock::Data();
    return;
  }
  if (size <= SmallBlock::kSize) {
    num_blocks_ = 1;
    last_block_size_ = IntCast<uint32_t>(size);
    data_ = block_.emplace<SmallBlock>(fill).data();
    return;
  }
  if (size <= kMaxSizeForSingleBlock) {
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
  data_ = block_.emplace<LargeBlock>(non_last_block_size_, fill).data();
}

ByteFill::operator Chain() const {
  Chain dest;
  if (size_ <= (fill_ == '\0' ? Chain::kMaxBytesToCopyToEmpty
                              : Blocks::kMaxSizeForSingleBlock)) {
    if (size_ > 0) {
      const absl::Span<char> buffer = dest.AppendFixedBuffer(
          IntCast<size_t>(size_),
          Chain::Options().set_size_hint(IntCast<size_t>(size_)));
      std::memset(buffer.data(), fill_, buffer.size());
    }
  } else {
    RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max())
        << "Chain size overflow";
    Chain::Options options;
    options.set_size_hint(IntCast<size_t>(size_));
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cbegin();
    RIEGELI_ASSERT(iter != blocks.cend()) << "Empty ByteFill handled above";
    do {
      dest.Append(*iter, options);
    } while (++iter != blocks.cend());
  }
  return dest;
}

ByteFill::operator absl::Cord() const {
  absl::Cord dest;
  if (size_ <= UnsignedMin(fill_ == '\0'
                               ? cord_internal::kMaxBytesToCopyToEmptyCord
                               : Blocks::kMaxSizeForSingleBlock,
                           absl::CordBuffer::kDefaultLimit)) {
    if (size_ > 0) {
      absl::CordBuffer buffer =
          absl::CordBuffer::CreateWithDefaultLimit(IntCast<size_t>(size_));
      buffer.SetLength(IntCast<size_t>(size_));
      std::memset(buffer.data(), fill_, IntCast<size_t>(size_));
      dest.Append(std::move(buffer));
    }
  } else {
    RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max())
        << "Cord size overflow";
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cbegin();
    RIEGELI_ASSERT(iter != blocks.cend()) << "Empty ByteFill handled above";
    do {
      ExternalRef(*iter).AppendTo(dest);
    } while (++iter != blocks.cend());
  }
  return dest;
}

void ByteFill::AssignTo(Chain& dest) const {
  dest.Clear();
  if (size_ <= (fill_ == '\0' ? Chain::kMaxBytesToCopyToEmpty
                              : Blocks::kMaxSizeForSingleBlock)) {
    if (empty()) return;
    const absl::Span<char> buffer = dest.AppendFixedBuffer(
        IntCast<size_t>(size_),
        Chain::Options().set_size_hint(IntCast<size_t>(size_)));
    std::memset(buffer.data(), fill_, buffer.size());
  } else {
    RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max())
        << "Chain size overflow";
    Chain::Options options;
    options.set_size_hint(IntCast<size_t>(size_));
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cbegin();
    RIEGELI_ASSERT(iter != blocks.cend()) << "Empty ByteFill handled above";
    do {
      dest.Append(*iter, options);
    } while (++iter != blocks.cend());
  }
}

void ByteFill::AssignTo(absl::Cord& dest) const {
  if (size_ <= UnsignedMin(fill_ == '\0'
                               ? cord_internal::kMaxBytesToCopyToEmptyCord
                               : Blocks::kMaxSizeForSingleBlock,
                           absl::CordBuffer::kDefaultLimit)) {
    if (size_ == 0) {
      dest.Clear();
    } else {
      absl::CordBuffer buffer = dest.GetAppendBuffer(0, 0);
      dest.Clear();
      if (buffer.capacity() < IntCast<size_t>(size_)) {
        buffer =
            absl::CordBuffer::CreateWithDefaultLimit(IntCast<size_t>(size_));
      }
      buffer.SetLength(IntCast<size_t>(size_));
      std::memset(buffer.data(), fill_, IntCast<size_t>(size_));
      dest.Append(std::move(buffer));
    }
  } else {
    dest.Clear();
    RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max())
        << "Cord size overflow";
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cbegin();
    RIEGELI_ASSERT(iter != blocks.cend()) << "Empty ByteFill handled above";
    do {
      ExternalRef(*iter).AppendTo(dest);
    } while (++iter != blocks.cend());
  }
}

void ByteFill::AppendTo(Chain& dest) const {
  if (size_ <= (fill_ == '\0' ? dest.MaxBytesToCopy()
                              : Blocks::kMaxSizeForSingleBlock)) {
    size_t length = IntCast<size_t>(size_);
    while (length > 0) {
      const absl::Span<char> buffer = dest.AppendBuffer(1, length, length);
      std::memset(buffer.data(), fill_, buffer.size());
      length -= buffer.size();
    }
  } else {
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cbegin();
    RIEGELI_ASSERT(iter != blocks.cend()) << "Empty ByteFill handled above";
    do {
      dest.Append(*iter);
    } while (++iter != blocks.cend());
  }
}

void ByteFill::AppendTo(Chain& dest, Chain::Options options) const {
  if (size_ <= (fill_ == '\0' ? dest.MaxBytesToCopy(options)
                              : Blocks::kMaxSizeForSingleBlock)) {
    size_t length = IntCast<size_t>(size_);
    while (length > 0) {
      const absl::Span<char> buffer =
          dest.AppendBuffer(1, length, length, options);
      std::memset(buffer.data(), fill_, buffer.size());
      length -= buffer.size();
    }
  } else {
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cbegin();
    RIEGELI_ASSERT(iter != blocks.cend()) << "Empty ByteFill handled above";
    do {
      dest.Append(*iter, options);
    } while (++iter != blocks.cend());
  }
}

void ByteFill::AppendTo(absl::Cord& dest) const {
  if (size_ <= UnsignedMin(fill_ == '\0'
                               ? cord_internal::MaxBytesToCopyToCord(dest)
                               : Blocks::kMaxSizeForSingleBlock,
                           absl::CordBuffer::kDefaultLimit)) {
    size_t length = IntCast<size_t>(size_);
    if (length == 0) return;
    {
      absl::CordBuffer buffer = dest.GetAppendBuffer(0, 1);
      const size_t existing_length = buffer.length();
      if (existing_length > 0) {
        buffer.SetLength(
            UnsignedMin(existing_length + length, buffer.capacity()));
        std::memset(buffer.data() + existing_length, fill_,
                    buffer.length() - existing_length);
        length -= buffer.length() - existing_length;
        dest.Append(std::move(buffer));
        if (length == 0) return;
      }
    }
    absl::CordBuffer buffer = absl::CordBuffer::CreateWithDefaultLimit(length);
    buffer.SetLength(length);
    std::memset(buffer.data(), fill_, length);
    dest.Append(std::move(buffer));
  } else {
    RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
        << "Cord size overflow";
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cbegin();
    RIEGELI_ASSERT(iter != blocks.cend()) << "Empty ByteFill handled above";
    do {
      ExternalRef(*iter).AppendTo(dest);
    } while (++iter != blocks.cend());
  }
}

void ByteFill::PrependTo(Chain& dest) const {
  if (size_ <= (fill_ == '\0' ? dest.MaxBytesToCopy()
                              : Blocks::kMaxSizeForSingleBlock)) {
    size_t length = IntCast<size_t>(size_);
    while (length > 0) {
      const absl::Span<char> buffer = dest.PrependBuffer(1, length, length);
      std::memset(buffer.data(), fill_, buffer.size());
      length -= buffer.size();
    }
  } else {
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cend();
    RIEGELI_ASSERT(iter != blocks.cbegin()) << "Empty ByteFill handled above";
    do {
      --iter;
      dest.Prepend(*iter);
    } while (iter != blocks.cbegin());
  }
}

void ByteFill::PrependTo(Chain& dest, Chain::Options options) const {
  if (size_ <= (fill_ == '\0' ? dest.MaxBytesToCopy(options)
                              : Blocks::kMaxSizeForSingleBlock)) {
    size_t length = IntCast<size_t>(size_);
    while (length > 0) {
      const absl::Span<char> buffer =
          dest.PrependBuffer(1, length, length, options);
      std::memset(buffer.data(), fill_, buffer.size());
      length -= buffer.size();
    }
  } else {
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cend();
    RIEGELI_ASSERT(iter != blocks.cbegin()) << "Empty ByteFill handled above";
    do {
      --iter;
      dest.Prepend(*iter, options);
    } while (iter != blocks.cbegin());
  }
}

void ByteFill::PrependTo(absl::Cord& dest) const {
  if (size_ <= UnsignedMin(fill_ == '\0'
                               ? cord_internal::MaxBytesToCopyToCord(dest)
                               : Blocks::kMaxSizeForSingleBlock,
                           absl::CordBuffer::kDefaultLimit)) {
    if (empty()) return;
    absl::CordBuffer buffer =
        absl::CordBuffer::CreateWithDefaultLimit(IntCast<size_t>(size_));
    buffer.SetLength(IntCast<size_t>(size_));
    std::memset(buffer.data(), fill_, IntCast<size_t>(size_));
    dest.Prepend(std::move(buffer));
  } else {
    RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
        << "Cord size overflow";
    const Blocks blocks = this->blocks();
    BlockIterator iter = blocks.cend();
    RIEGELI_ASSERT(iter != blocks.cbegin()) << "Empty ByteFill handled above";
    do {
      --iter;
      ExternalRef(*iter).PrependTo(dest);
    } while (iter != blocks.cbegin());
  }
}

void ByteFill::Output(std::ostream& out) const {
  for (const absl::string_view fragment : blocks()) {
    out.write(fragment.data(), IntCast<std::streamsize>(fragment.size()));
  }
}

}  // namespace riegeli
