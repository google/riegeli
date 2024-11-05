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

#ifndef RIEGELI_BASE_BYTE_FILL_
#define RIEGELI_BASE_BYTE_FILL_

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <iosfwd>
#include <iterator>
#include <limits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/shared_buffer.h"
#include "riegeli/base/types.h"

namespace riegeli {

// Represents a byte sequence of the given size with all bytes equal to the
// given value.
class ByteFill {
 public:
  class BlockRef;
  class BlockIterator;
  class Blocks;

  // Constructs a `ByteFill` with `size` occurrences of `fill`.
  explicit ByteFill(Position size, char fill = '\0')
      : size_(size), fill_(fill) {}

  ByteFill(const ByteFill& that) = default;
  ByteFill& operator=(const ByteFill& that) = default;

  bool empty() const { return size() == 0; }
  Position size() const { return size_; }
  char fill() const { return fill_; }

  // Removes `difference` occurrences, and returns `ByteFill` corresponding
  // to the removed fragment.
  //
  // Precondition: `difference <= size()`
  ByteFill Extract(Position difference) {
    RIEGELI_ASSERT_LE(difference, size_)
        << "Failed precondition of ByteFill::Extract(): size underflow";
    size_ -= difference;
    return ByteFill(difference, fill_);
  }

  // A sequence of non-empty `absl::string_view` blocks comprising data of the
  // `ByteFill`.
  Blocks blocks() const;

  // Converts the data to `Chain`.
  explicit operator Chain() const;

  // Converts the data to `absl::Cord`.
  explicit operator absl::Cord() const;

  // Support `riegeli::Reset(Chain&, ByteFill)`.
  friend void RiegeliReset(Chain& dest, ByteFill src) { src.AssignTo(dest); }

  // Support `riegeli::Reset(absl::Cord&, ByteFill)`.
  friend void RiegeliReset(absl::Cord& dest, ByteFill src) {
    src.AssignTo(dest);
  }

  // Appends the data to `dest`.
  void AppendTo(Chain& dest) const;
  void AppendTo(Chain& dest, Chain::Options options) const;

  // Appends the data to `dest`.
  void AppendTo(absl::Cord& dest) const;

  // Prepends the data to `dest`.
  void PrependTo(Chain& dest) const;
  void PrependTo(Chain& dest, Chain::Options options) const;

  // Prepends the data to `dest`.
  void PrependTo(absl::Cord& dest) const;

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, const ByteFill& src) {
    Position length = src.size_;
    while (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max())) {
      dest.Append(std::numeric_limits<size_t>::max(), src.fill_);
      length -= std::numeric_limits<size_t>::max();
    }
    if (length > 0) dest.Append(IntCast<size_t>(length), src.fill_);
  }

  // Writes the occurrences to `out` as unformatted bytes.
  friend std::ostream& operator<<(std::ostream& dest, const ByteFill& src) {
    src.Output(dest);
    return dest;
  }

 private:
  class ZeroBlock;
  class SmallBlock;
  class LargeBlock;

  void AssignTo(Chain& dest) const;
  void AssignTo(absl::Cord& dest) const;
  void Output(std::ostream& dest) const;

  Position size_;
  char fill_;
};

// Represents a block of zeros backed by a shared array for `ExternalRef`.
class ByteFill::ZeroBlock {
 public:
  static constexpr size_t kSize = size_t{64} << 10;

  static const char* Data();

  ZeroBlock() = default;

  ZeroBlock(const ZeroBlock& that) = default;
  ZeroBlock& operator=(const ZeroBlock& that) = default;

  // Support `ExternalRef`.
  friend Chain::Block RiegeliToChainBlock(
      ABSL_ATTRIBUTE_UNUSED const ZeroBlock* self, absl::string_view substr) {
    return ToChainBlock(substr);
  }

  // Support `ExternalRef`.
  friend absl::Cord RiegeliToCord(ABSL_ATTRIBUTE_UNUSED const ZeroBlock* self,
                                  absl::string_view substr) {
    return ToCord(substr);
  }

  // Support `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(
      ABSL_ATTRIBUTE_UNUSED const ZeroBlock* self) {
    return ExternalStorage(nullptr, [](ABSL_ATTRIBUTE_UNUSED void* ptr) {});
  }

  // Support `ExternalRef` and `Chain::Block`.
  friend void RiegeliDumpStructure(ABSL_ATTRIBUTE_UNUSED const ZeroBlock* self,
                                   std::ostream& dest) {
    DumpStructure(dest);
  }

 private:
  static Chain::Block ToChainBlock(absl::string_view substr);
  static absl::Cord ToCord(absl::string_view substr);
  static void DumpStructure(std::ostream& dest);
};

class ByteFill::SmallBlock {
 public:
  static constexpr size_t kSize = 64;

  explicit SmallBlock(char fill) { std::memset(data_, fill, kSize); }

  SmallBlock(const SmallBlock& that) = default;
  SmallBlock& operator=(const SmallBlock& that) = default;

  const char* data() const { return data_; }

  // Support `ExternalRef`.
  friend bool RiegeliExternalCopy(
      ABSL_ATTRIBUTE_UNUSED const SmallBlock* self) {
    return true;
  }

 private:
  char data_[kSize];
};

class ByteFill::LargeBlock {
 public:
  explicit LargeBlock(size_t size, char fill) : buffer_(size) {
    std::memset(buffer_.mutable_data(), fill, size);
  }

  LargeBlock(const LargeBlock& that) = default;
  LargeBlock& operator=(const LargeBlock& that) = default;

  LargeBlock(LargeBlock&& that) = default;
  LargeBlock& operator=(LargeBlock&& that) = default;

  const char* data() const { return buffer_.data(); }

  // Indicate support for:
  //  * `ExternalRef(const LargeBlock&, substr)`
  //  * `ExternalRef(LargeBlock&&, substr)`
  friend void RiegeliSupportsExternalRef(const LargeBlock*) {}

  // `RiegeliExternalMemory()` is intentionally not defined so that a
  // `LargeBlock` is never considered wasteful. Even if a substring of it is
  // shared, the whole `LargeBlock` is shared nearby.

  // Support `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(LargeBlock* self) {
    return RiegeliToExternalStorage(&self->buffer_);
  }

  // Support `ExternalRef` and `Chain::Block`.
  friend void RiegeliDumpStructure(const LargeBlock* self,
                                   absl::string_view substr,
                                   std::ostream& dest) {
    self->DumpStructure(substr, dest);
  }

  // Support `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const LargeBlock* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->buffer_);
  }

 private:
  void DumpStructure(absl::string_view substr, std::ostream& dest) const;

  SharedBuffer buffer_;
};

class ByteFill::BlockRef {
 public:
  BlockRef(const BlockRef& that) = default;
  BlockRef& operator=(const BlockRef& that) = default;

  /*implicit*/ operator absl::string_view() const {
    return absl::string_view(data(), size());
  }

  bool empty() const { return false; }
  const char* data() const;
  size_t size() const;

  // Indicate support for:
  //  * `ExternalRef(BlockRef)`
  //  * `ExternalRef(BlockRef, substr)`
  friend void RiegeliSupportsExternalRef(const BlockRef*) {}

  // Support `ExternalRef`.
  template <typename Callback>
  friend void RiegeliExternalDelegate(const BlockRef* self,
                                      absl::string_view substr,
                                      Callback&& delegate_to) {
    self->ExternalDelegate(substr, std::forward<Callback>(delegate_to));
  }

 private:
  friend class ByteFill;  // For `BlockRef()`.

  explicit BlockRef(const ByteFill::Blocks* blocks,
                    Position block_index_complement)
      : blocks_(blocks), block_index_complement_(block_index_complement) {}

  template <typename Callback>
  void ExternalDelegate(absl::string_view substr, Callback&& delegate_to) const;

  const Blocks* blocks_;
  // `block_index_complement_` is `blocks_->num_blocks_ - block_index`. Working
  // with the complement makes it easier to handle special case at 1 (a block
  // with size `blocks_->last_block_size_`).
  Position block_index_complement_;
};

class ByteFill::BlockIterator : public WithCompare<BlockIterator> {
 public:
  using iterator_concept = std::random_access_iterator_tag;
  // `iterator_category` is only `std::input_iterator_tag` because the
  // `LegacyForwardIterator` requirement and above require `reference` to be
  // a true reference type.
  using iterator_category = std::input_iterator_tag;
  using value_type = BlockRef;
  using reference = value_type;
  using difference_type = ptrdiff_t;

  class pointer {
   public:
    const reference* operator->() const { return &ref_; }

   private:
    friend class BlockIterator;
    explicit pointer(reference ref) : ref_(ref) {}
    reference ref_;
  };

  BlockIterator() = default;

  BlockIterator(const BlockIterator& that) = default;
  BlockIterator& operator=(const BlockIterator& that) = default;

  reference operator*() const;
  pointer operator->() const;
  BlockIterator& operator++();
  BlockIterator operator++(int);
  BlockIterator& operator--();
  BlockIterator operator--(int);
  BlockIterator& operator+=(difference_type n);
  BlockIterator operator+(difference_type n) const;
  BlockIterator& operator-=(difference_type n);
  BlockIterator operator-(difference_type n) const;
  reference operator[](difference_type n) const;

  friend bool operator==(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT(a.blocks_ == b.blocks_)
        << "Failed precondition of operator==(ByteFill::BlockIterator): "
           "incomparable iterators";
    return b.block_index_complement_ == a.block_index_complement_;
  }
  friend StrongOrdering RIEGELI_COMPARE(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT(a.blocks_ == b.blocks_)
        << "Failed precondition of operator<=>(ByteFill::BlockIterator): "
           "incomparable iterators";
    return riegeli::Compare(b.block_index_complement_,
                            a.block_index_complement_);
  }
  friend difference_type operator-(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT(a.blocks_ == b.blocks_)
        << "Failed precondition of operator-(ByteFill::BlockIterator): "
           "incomparable iterators";
    return b.block_index_complement_ - a.block_index_complement_;
  }
  friend BlockIterator operator+(difference_type n, BlockIterator a) {
    return a + n;
  }

 private:
  friend class ByteFill;  // For `BlockIterator()`.

  explicit BlockIterator(const Blocks* blocks, Position block_index_complement)
      : blocks_(blocks), block_index_complement_(block_index_complement) {}

  const Blocks* blocks_ = nullptr;
  // `block_index_complement_` is `blocks_->num_blocks_ - block_index`. Working
  // with the complement makes it easier to handle special cases at 0 (`end()`)
  // and 1 (a block with size `blocks_->last_block_size_`).
  Position block_index_complement_ = 0;
};

class ByteFill::Blocks {
 public:
  using value_type = BlockRef;
  using reference = value_type;
  using const_reference = reference;
  using iterator = BlockIterator;
  using const_iterator = iterator;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = reverse_iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  Blocks() = default;

  Blocks(Blocks&& that) noexcept;
  Blocks& operator=(Blocks&&) = delete;

  iterator begin() const { return iterator(this, num_blocks_); }
  iterator cbegin() const { return begin(); }
  iterator end() const { return iterator(this, 0); }
  iterator cend() const { return end(); }

  reverse_iterator rbegin() const { return reverse_iterator(end()); }
  reverse_iterator crbegin() const { return rbegin(); }
  reverse_iterator rend() const { return reverse_iterator(begin()); }
  reverse_iterator crend() const { return rend(); }

  bool empty() const { return size() == 0; }
  size_type size() const { return num_blocks_; }

  reference operator[](size_type n) const {
    RIEGELI_ASSERT_LT(n, size())
        << "Failed precondition of ByteFill::Blocks::operator[]: "
           "block index out of range";
    return BlockRef(this, num_blocks_ - n);
  }
  reference at(size_type n) const {
    RIEGELI_CHECK_LT(n, size())
        << "Failed precondition of ByteFill::Blocks::at(): "
           "block index out of range";
    return BlockRef(this, num_blocks_ - n);
  }
  reference front() const {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of ByteFill::Blocks::front(): no blocks";
    return BlockRef(this, num_blocks_);
  }
  reference back() const {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of ByteFill::Blocks::back(): no blocks";
    return BlockRef(this, 1);
  }

 private:
  // For `kMaxSizeForSingleBlock`, `Blocks()`, `data()`, `size()`, and
  // `ExternalDelegate()`.
  friend class ByteFill;

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
  static constexpr Position kMaxSizeForSingleBlock =
      Position{1} << (kBlockSizeBitsBias + 1);

  explicit Blocks(Position size, char fill);

  const char* data() const { return data_; }
  size_t size(Position block_index_complement) const {
    return block_index_complement == 1 ? last_block_size_
                                       : non_last_block_size_;
  }

  template <typename Callback>
  void ExternalDelegate(Position block_index_complement,
                        absl::string_view substr, Callback&& delegate_to) const;

  Position num_blocks_ = 0;
  uint32_t non_last_block_size_ = 0;
  uint32_t last_block_size_ = 0;
  // If `num_blocks_ > 0` then `data_` is:
  //  * When `block_` is `ZeroBlock`:  `ZeroBlock::Data()`
  //  * When `block_` is `SmallBlock`: `small_block.data()`
  //  * When `block_` is `LargeBlock`: `large_block.data()`
  const char* data_ = nullptr;
  absl::variant<ZeroBlock, SmallBlock, LargeBlock> block_;
};

// Implementation details follow.

inline const char* ByteFill::BlockRef::data() const { return blocks_->data(); }

inline size_t ByteFill::BlockRef::size() const {
  return blocks_->size(block_index_complement_);
}

template <typename Callback>
inline void ByteFill::BlockRef::ExternalDelegate(absl::string_view substr,
                                                 Callback&& delegate_to) const {
  blocks_->ExternalDelegate(block_index_complement_, substr,
                            std::forward<Callback>(delegate_to));
}

inline ByteFill::BlockIterator::reference ByteFill::BlockIterator::operator*()
    const {
  RIEGELI_ASSERT_GT(block_index_complement_, 0u)
      << "Failed precondition of ByteFill::BlockIterator::operator*: "
         "iterator is end()";
  return BlockRef(blocks_, block_index_complement_);
}

inline ByteFill::BlockIterator::pointer ByteFill::BlockIterator::operator->()
    const {
  return pointer(**this);
}

inline ByteFill::BlockIterator& ByteFill::BlockIterator::operator++() {
  RIEGELI_ASSERT_GT(block_index_complement_, 0u)
      << "Failed precondition of ByteFill::BlockIterator::operator++: "
         "iterator is end()";
  --block_index_complement_;
  return *this;
}

inline ByteFill::BlockIterator ByteFill::BlockIterator::operator++(int) {
  const BlockIterator tmp = *this;
  ++*this;
  return tmp;
}

inline ByteFill::BlockIterator& ByteFill::BlockIterator::operator--() {
  RIEGELI_ASSERT_LT(block_index_complement_, blocks_->size())
      << "Failed precondition of ByteFill::BlockIterator::operator--: "
         "iterator is begin()";
  ++block_index_complement_;
  return *this;
}

inline ByteFill::BlockIterator ByteFill::BlockIterator::operator--(int) {
  const BlockIterator tmp = *this;
  --*this;
  return tmp;
}

inline ByteFill::BlockIterator& ByteFill::BlockIterator::operator+=(
    difference_type n) {
  if (n >= 0) {
    RIEGELI_ASSERT_LE(UnsignedCast(n), block_index_complement_)
        << "Failed precondition of ByteFill::BlockIterator::operator+=: "
           "iterator after end()";
  } else {
    RIEGELI_ASSERT_LE(NegatingUnsignedCast(n),
                      blocks_->size() - block_index_complement_)
        << "Failed precondition of ByteFill::BlockIterator::operator+=: "
           "iterator before begin()";
  }
  block_index_complement_ -= static_cast<Position>(n);
  return *this;
}

inline ByteFill::BlockIterator ByteFill::BlockIterator::operator+(
    difference_type n) const {
  return BlockIterator(*this) += n;
}

inline ByteFill::BlockIterator& ByteFill::BlockIterator::operator-=(
    difference_type n) {
  if (n >= 0) {
    RIEGELI_ASSERT_LE(UnsignedCast(n),
                      blocks_->size() - block_index_complement_)
        << "Failed precondition of ByteFill::BlockIterator::operator-=: "
           "iterator before begin()";
  } else {
    RIEGELI_ASSERT_LE(NegatingUnsignedCast(n), block_index_complement_)
        << "Failed precondition of ByteFill::BlockIterator::operator-=: "
           "iterator after end()";
  }
  block_index_complement_ += static_cast<Position>(n);
  return *this;
}

inline ByteFill::BlockIterator ByteFill::BlockIterator::operator-(
    difference_type n) const {
  return BlockIterator(*this) -= n;
}

inline ByteFill::BlockIterator::reference ByteFill::BlockIterator::operator[](
    difference_type n) const {
  return *(*this + n);
}

inline ByteFill::Blocks::Blocks(Blocks&& that) noexcept
    : num_blocks_(std::exchange(that.num_blocks_, 0)),
      last_block_size_(that.last_block_size_),
      data_(that.data_),
      block_(std::move(that.block_)) {
  {
    SmallBlock* const small_block = absl::get_if<SmallBlock>(&block_);
    if (small_block != nullptr) {
      data_ = small_block->data();
    }
  }
}

template <typename Callback>
inline void ByteFill::Blocks::ExternalDelegate(Position block_index_complement,
                                               absl::string_view substr,
                                               Callback&& delegate_to) const {
  struct Visitor {
    void operator()(const ZeroBlock& zero_ref) const {
      std::forward<Callback>(delegate_to)(zero_ref, substr);
    }
    void operator()(const SmallBlock& small_block) const {
      std::forward<Callback>(delegate_to)(small_block, substr);
    }
    void operator()(const LargeBlock& large_block) const {
      std::forward<Callback>(delegate_to)(large_block, substr);
    }

    absl::string_view substr;
    Callback&& delegate_to;
  };
  absl::visit(Visitor{substr, std::forward<Callback>(delegate_to)}, block_);
}

inline ByteFill::Blocks ByteFill::blocks() const {
  return Blocks(size_, fill_);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_BYTE_FILL_
