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

#include "riegeli/base/chain.h"

#include <stddef.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <ios>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/global.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/intrusive_shared_ptr.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/new_aligned.h"
#include "riegeli/base/shared_buffer.h"
#include "riegeli/base/sized_shared_buffer.h"
#include "riegeli/base/string_utils.h"
#include "riegeli/base/zeros.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr Chain::Options Chain::kDefaultOptions;
constexpr size_t Chain::kAnyLength;
constexpr size_t Chain::kMaxShortDataSize;
constexpr size_t Chain::kAllocationCost;
constexpr size_t Chain::RawBlock::kMaxCapacity;
constexpr Chain::BlockPtrPtr Chain::BlockIterator::kBeginShortData;
constexpr Chain::BlockPtrPtr Chain::BlockIterator::kEndShortData;
#endif

namespace {

void WritePadding(std::ostream& out, size_t pad) {
  char buf[64];
  std::memset(buf, out.fill(), sizeof(buf));
  while (pad > 0) {
    const size_t length = UnsignedMin(pad, sizeof(buf));
    out.write(buf, IntCast<std::streamsize>(length));
    pad -= length;
  }
}

// Stores an `absl::Cord` which must be flat, i.e.
// `src.TryFlat() != absl::nullopt`.
//
// This design relies on the fact that moving a flat `absl::Cord` results in a
// flat `absl::Cord`.
class FlatCordRef {
 public:
  explicit FlatCordRef(Initializer<absl::Cord> src);
  explicit FlatCordRef(absl::Cord::CharIterator& iter, size_t length);

  FlatCordRef(const FlatCordRef&) = delete;
  FlatCordRef& operator=(const FlatCordRef&) = delete;

  explicit operator absl::string_view() const;

  friend void RiegeliDumpStructure(
      ABSL_ATTRIBUTE_UNUSED const FlatCordRef* self, std::ostream& out) {
    out << "[cord] { }";
  }

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const FlatCordRef* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->src_);
  }

  // Appends the `absl::Cord` to `dest`.
  void AppendTo(absl::Cord& dest) const;

  // Appends [`data`..`data + length`) to `dest`.
  // [`data`..`data + length`) must be contained in `absl::string_view(*this)`.
  void AppendSubstrTo(const char* data, size_t length, absl::Cord& dest) const;

  // Prepends the `absl::Cord` to `dest`.
  void PrependTo(absl::Cord& dest) const;

  // Prepends [`data`..`data + length`) to `dest`.
  // [`data`..`data + length`) must be contained in `absl::string_view(*this)`.
  void PrependSubstrTo(const char* data, size_t length, absl::Cord& dest) const;

 private:
  // Invariant: `src_.TryFlat() != absl::nullopt`
  absl::Cord src_;
};

inline FlatCordRef::FlatCordRef(Initializer<absl::Cord> src)
    : src_(std::move(src).Construct()) {
  RIEGELI_ASSERT(src_.TryFlat() != absl::nullopt)
      << "Failed precondition of FlatCordRef::FlatCordRef(): "
         "Cord is not flat";
}

inline FlatCordRef::FlatCordRef(absl::Cord::CharIterator& iter, size_t length)
    : src_(absl::Cord::AdvanceAndRead(&iter, length)) {
  RIEGELI_ASSERT(src_.TryFlat() != absl::nullopt)
      << "Failed precondition of FlatCordRef::FlatCordRef(): "
         "Cord is not flat";
}

inline FlatCordRef::operator absl::string_view() const {
  {
    const absl::optional<absl::string_view> flat = src_.TryFlat();
    if (flat != absl::nullopt) {
      return *flat;
    }
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Failed invariant of FlatCordRef: Cord is not flat";
}

inline void FlatCordRef::AppendTo(absl::Cord& dest) const {
  RIEGELI_ASSERT_LE(src_.size(),
                    std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of FlatCordRef::AppendTo(): "
         "Cord size overflow";
  dest.Append(src_);
}

inline void FlatCordRef::AppendSubstrTo(const char* data, size_t length,
                                        absl::Cord& dest) const {
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of FlatCordRef::AppendSubstrTo(): "
         "Cord size overflow";
  if (length == src_.size()) {
    dest.Append(src_);
    return;
  }
  const absl::string_view fragment(*this);
  RIEGELI_ASSERT(std::greater_equal<>()(data, fragment.data()))
      << "Failed precondition of FlatCordRef::AppendSubstrTo(): "
         "substring not contained in data";
  RIEGELI_ASSERT(
      std::less_equal<>()(data + length, fragment.data() + fragment.size()))
      << "Failed precondition of FlatCordRef::AppendSubstrTo(): "
         "substring not contained in data";
  dest.Append(src_.Subcord(PtrDistance(fragment.data(), data), length));
}

inline void FlatCordRef::PrependTo(absl::Cord& dest) const {
  RIEGELI_ASSERT_LE(src_.size(),
                    std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of FlatCordRef::PrependTo(): "
         "Cord size overflow";
  dest.Prepend(src_);
}

inline void FlatCordRef::PrependSubstrTo(const char* data, size_t length,
                                         absl::Cord& dest) const {
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of FlatCordRef::PrependSubstrTo(): "
         "Cord size overflow";
  if (length == src_.size()) {
    dest.Prepend(src_);
    return;
  }
  const absl::string_view fragment(*this);
  RIEGELI_ASSERT(std::greater_equal<>()(data, fragment.data()))
      << "Failed precondition of FlatCordRef::PrependSubstrTo(): "
         "substring not contained in data";
  RIEGELI_ASSERT(
      std::less_equal<>()(data + length, fragment.data() + fragment.size()))
      << "Failed precondition of FlatCordRef::PrependSubstrTo(): "
         "substring not contained in data";
  dest.Prepend(src_.Subcord(PtrDistance(fragment.data(), data), length));
}

class SharedBufferRef {
 public:
  explicit SharedBufferRef(Initializer<SharedBuffer> src)
      : src_(std::move(src).Construct()) {}

  SharedBufferRef(const SharedBufferRef&) = delete;
  SharedBufferRef& operator=(const SharedBufferRef&) = delete;

  friend void RiegeliDumpStructure(const SharedBufferRef* self,
                                   absl::string_view data, std::ostream& out) {
    out << "[shared_buffer] {";
    if (!data.empty()) {
      if (data.data() != self->src_.data()) {
        out << " space_before: " << PtrDistance(self->src_.data(), data.data());
      }
      out << " space_after: "
          << PtrDistance(data.data() + data.size(),
                         self->src_.data() + self->src_.capacity());
    }
    out << " }";
  }

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const SharedBufferRef* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->src_);
  }

 private:
  SharedBuffer src_;
};

class ZeroRef {
 public:
  ZeroRef() = default;

  ZeroRef(const ZeroRef&) = delete;
  ZeroRef& operator=(const ZeroRef&) = delete;

  friend void RiegeliDumpStructure(ABSL_ATTRIBUTE_UNUSED const ZeroRef* self,
                                   std::ostream& out) {
    out << "[zero] { }";
  }
};

}  // namespace

class Chain::BlockRef {
 public:
  template <Ownership ownership>
  explicit BlockRef(RawBlock* block,
                    std::integral_constant<Ownership, ownership>);

  BlockRef(const BlockRef&) = delete;
  BlockRef& operator=(const BlockRef&) = delete;

  friend void RiegeliDumpStructure(const BlockRef* self, absl::string_view data,
                                   std::ostream& out) {
    out << "[block] { offset: "
        << PtrDistance(self->block_->data_begin(), data.data()) << " ";
    self->block_->DumpStructure(out);
    out << " }";
  }

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const BlockRef* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->block_);
  }

 private:
  IntrusiveSharedPtr<RawBlock> block_;
};

template <Chain::Ownership ownership>
inline Chain::BlockRef::BlockRef(RawBlock* block,
                                 std::integral_constant<Ownership, ownership>) {
  if (const BlockRef* const block_ref =
          block->checked_external_object<BlockRef>()) {
    // `block` is already a `BlockRef`. Refer to its target instead.
    RawBlock* const target = block_ref->block_.get();
    if (ownership == Ownership::kSteal) {
      target->Ref();
      block->Unref();
    }
    block = target;
  }
  if (ownership == Ownership::kShare) block->Ref();
  block_.Reset(block);
}

inline Chain::RawBlock* Chain::RawBlock::NewInternal(size_t min_capacity) {
  RIEGELI_ASSERT_GT(min_capacity, 0u)
      << "Failed precondition of Chain::RawBlock::NewInternal(): zero capacity";
  size_t raw_capacity;
  return SizeReturningNewAligned<RawBlock>(
      kInternalAllocatedOffset() + min_capacity, &raw_capacity, &raw_capacity);
}

inline Chain::RawBlock::RawBlock(const size_t* raw_capacity)
    : data_(allocated_begin_),
      size_(0),
      // Redundant cast is needed for `-fsanitize=bounds`.
      allocated_end_(static_cast<char*>(allocated_begin_) +
                     (*raw_capacity - kInternalAllocatedOffset())) {
  RIEGELI_ASSERT(is_internal()) << "A RawBlock with allocated_end_ != nullptr "
                                   "should be considered internal";
  RIEGELI_ASSERT_LE(capacity(), RawBlock::kMaxCapacity)
      << "Chain block capacity overflow";
}

template <Chain::Ownership ownership>
inline Chain::RawBlock* Chain::RawBlock::Copy() {
  RawBlock* const block = NewInternal(size());
  block->Append(absl::string_view(*this));
  RIEGELI_ASSERT(!block->wasteful())
      << "A full block should not be considered wasteful";
  Unref<ownership>();
  return block;
}

inline size_t Chain::RawBlock::space_before() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::RawBlock::space_before(): "
         "block not internal";
  return PtrDistance(allocated_begin_, data_begin());
}

inline size_t Chain::RawBlock::space_after() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::RawBlock::space_after(): "
         "block not internal";
  return PtrDistance(data_end(), allocated_end_);
}

inline bool Chain::RawBlock::tiny(size_t extra_size) const {
  if (is_internal()) {
    RIEGELI_ASSERT_LE(size(), capacity())
        << "Failed invariant of Chain::RawBlock: size greater than capacity";
    RIEGELI_ASSERT_LE(extra_size, capacity() - size())
        << "Failed precondition of Chain::RawBlock::tiny(): "
           "extra size greater than remaining space";
  } else {
    RIEGELI_ASSERT_EQ(extra_size, 0u)
        << "Failed precondition of Chain::RawBlock::tiny(): "
           "non-zero extra size of external block";
  }
  return size() + extra_size < kDefaultMinBlockSize;
}

inline bool Chain::RawBlock::wasteful(size_t extra_size) const {
  if (is_internal()) {
    RIEGELI_ASSERT_LE(size(), capacity())
        << "Failed invariant of Chain::RawBlock: size greater than capacity";
    RIEGELI_ASSERT_LE(extra_size, capacity() - size())
        << "Failed precondition of Chain::RawBlock::wasteful(): "
           "extra size greater than remaining space";
  } else {
    RIEGELI_ASSERT_EQ(extra_size, 0u)
        << "Failed precondition of Chain::RawBlock::wasteful(): "
           "non-zero extra size of external block";
    return false;
  }
  return Wasteful(kInternalAllocatedOffset() + capacity(), size() + extra_size);
}

inline void Chain::RawBlock::DumpStructure(std::ostream& out) const {
  out << "block {";
  const size_t ref_count = ref_count_.get_count();
  if (ref_count != 1) out << " ref_count: " << ref_count;
  out << " size: " << size();
  if (is_internal()) {
    if (space_before() > 0) out << " space_before: " << space_before();
    out << " space_after: " << space_after();
  } else {
    out << " ";
    external_.methods->dump_structure(*this, out);
  }
  out << " }";
}

size_t Chain::RawBlock::DynamicSizeOfImpl() const {
  if (is_internal()) {
    return kInternalAllocatedOffset() + capacity();
  } else {
    return external_.methods->dynamic_sizeof;
  }
}

void Chain::RawBlock::RegisterSubobjectsImpl(
    MemoryEstimator& memory_estimator) const {
  if (!is_internal()) {
    external_.methods->register_subobjects(this, memory_estimator);
  }
}

inline bool Chain::RawBlock::can_append(size_t length) const {
  return is_mutable() && (empty() ? capacity() : space_after()) >= length;
}

inline bool Chain::RawBlock::can_prepend(size_t length) const {
  return is_mutable() && (empty() ? capacity() : space_before()) >= length;
}

inline bool Chain::RawBlock::CanAppendMovingData(size_t length,
                                                 size_t& min_length_if_not) {
  RIEGELI_ASSERT_LE(length, RawBlock::kMaxCapacity - size())
      << "Failed precondition of Chain::RawBlock::CanAppendMovingData(): "
         "RawBlock size overflow";
  if (is_mutable()) {
    if (empty()) data_ = allocated_begin_;
    if (space_after() >= length) return true;
    if (size() + length <= capacity() && 2 * size() <= capacity()) {
      // Existing array has enough capacity and is at most half full: move
      // contents to the beginning of the array. This is enough to make the
      // amortized cost of adding one element constant as long as prepending
      // leaves space at both ends.
      char* const new_begin = allocated_begin_;
      std::memmove(new_begin, data_, size_);
      data_ = new_begin;
      return true;
    }
    min_length_if_not = UnsignedMin(
        UnsignedMax(length, SaturatingAdd(space_after(), capacity() / 2)),
        RawBlock::kMaxCapacity - size());
  } else {
    min_length_if_not = length;
  }
  return false;
}

inline bool Chain::RawBlock::CanPrependMovingData(size_t length,
                                                  size_t& space_after_if_not,
                                                  size_t& min_length_if_not) {
  RIEGELI_ASSERT_LE(length, RawBlock::kMaxCapacity - size())
      << "Failed precondition of Chain::RawBlock::CanPrependMovingData(): "
         "RawBlock size overflow";
  if (is_mutable()) {
    if (empty()) data_ = allocated_end_;
    if (space_before() >= length) return true;
    if (size() + length <= capacity() && 2 * size() <= capacity()) {
      // Existing array has enough capacity and is at most half full: move
      // contents to the middle of the array. This makes the amortized cost of
      // adding one element constant.
      char* const new_begin =
          allocated_begin_ + (capacity() - size() + length) / 2;
      std::memmove(new_begin, data_, size_);
      data_ = new_begin;
      return true;
    }
    min_length_if_not = UnsignedMin(
        UnsignedMax(length, SaturatingAdd(space_before(), capacity() / 2)),
        RawBlock::kMaxCapacity - size());
    space_after_if_not = UnsignedMin(
        space_after(), RawBlock::kMaxCapacity - size() - min_length_if_not);
  } else {
    min_length_if_not = length;
    space_after_if_not = 0;
  }
  return false;
}

inline absl::Span<char> Chain::RawBlock::AppendBuffer(size_t max_length) {
  RIEGELI_ASSERT(is_mutable())
      << "Failed precondition of Chain::RawBlock::AppendBuffer(): "
         "block is immutable";
  if (empty()) data_ = allocated_begin_;
  const size_t length = UnsignedMin(space_after(), max_length);
  const absl::Span<char> buffer(const_cast<char*>(data_end()), length);
  size_ += length;
  return buffer;
}

inline absl::Span<char> Chain::RawBlock::PrependBuffer(size_t max_length) {
  RIEGELI_ASSERT(is_mutable())
      << "Failed precondition of Chain::RawBlock::PrependBuffer(): "
         "block is immutable";
  if (empty()) data_ = allocated_end_;
  const size_t length = UnsignedMin(space_before(), max_length);
  const absl::Span<char> buffer(const_cast<char*>(data_begin()) - length,
                                length);
  data_ -= length;
  size_ += length;
  return buffer;
}

inline void Chain::RawBlock::Append(absl::string_view src,
                                    size_t space_before) {
  if (empty()) {
    // Redundant cast is needed for `-fsanitize=bounds`.
    data_ = static_cast<char*>(allocated_begin_) + space_before;
  }
  return AppendWithExplicitSizeToCopy(src, src.size());
}

inline void Chain::RawBlock::AppendWithExplicitSizeToCopy(absl::string_view src,
                                                          size_t size_to_copy) {
  RIEGELI_ASSERT_GE(size_to_copy, src.size())
      << "Failed precondition of "
         "Chain::RawBlock::AppendWithExplicitSizeToCopy(): "
         "size to copy too small";
  RIEGELI_ASSERT(can_append(size_to_copy))
      << "Failed precondition of "
         "Chain::RawBlock::AppendWithExplicitSizeToCopy(): "
         "not enough space";
  std::memcpy(const_cast<char*>(data_end()), src.data(), size_to_copy);
  size_ += src.size();
}

inline void Chain::RawBlock::Prepend(absl::string_view src,
                                     size_t space_after) {
  RIEGELI_ASSERT(can_prepend(src.size()))
      << "Failed precondition of Chain::RawBlock::Prepend(): "
         "not enough space";
  if (empty()) data_ = allocated_end_ - space_after;
  std::memcpy(const_cast<char*>(data_begin() - src.size()), src.data(),
              src.size());
  data_ -= src.size();
  size_ += src.size();
}

inline void Chain::RawBlock::AppendTo(Chain& dest, const Options& options) {
  RIEGELI_ASSERT_LE(size(), std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::RawBlock::AppendTo(Chain&): "
         "Chain size overflow";
  dest.AppendRawBlock(this, options);
}

template <Chain::Ownership ownership>
inline void Chain::RawBlock::AppendTo(absl::Cord& dest) {
  RIEGELI_ASSERT_LE(size(), std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::RawBlock::AppendTo(Cord&): "
         "Cord size overflow";
  if (size() <= cord_internal::MaxBytesToCopyToCord(dest) || wasteful()) {
    cord_internal::AppendToBlockyCord(absl::string_view(*this), dest);
    Unref<ownership>();
    return;
  }
  if (const FlatCordRef* const cord_ref =
          checked_external_object<FlatCordRef>()) {
    RIEGELI_ASSERT_EQ(size(), absl::string_view(*cord_ref).size())
        << "Failed invariant of Chain::RawBlock: "
           "block size differs from Cord size";
    cord_ref->AppendTo(dest);
    Unref<ownership>();
    return;
  }
  Ref<ownership>();
  dest.Append(absl::MakeCordFromExternal(absl::string_view(*this),
                                         [this] { Unref(); }));
}

inline void Chain::RawBlock::AppendSubstrTo(const char* data, size_t length,
                                            Chain& dest,
                                            const Options& options) {
  RIEGELI_ASSERT(std::greater_equal<>()(data, data_begin()))
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Chain&): "
         "substring not contained in data";
  RIEGELI_ASSERT(std::less_equal<>()(data + length, data_end()))
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Chain&): "
         "substring not contained in data";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Chain&): "
         "Chain size overflow";
  if (length == size()) {
    if (wasteful()) {
      dest.Append(absl::string_view(data, length), options);
      return;
    }
    dest.AppendRawBlock(this, options);
    return;
  }
  if (length <= kMaxBytesToCopy || wasteful()) {
    dest.Append(absl::string_view(data, length), options);
    return;
  }
  dest.Append(
      Chain::FromExternal(
          riegeli::Maker<BlockRef>(
              this, std::integral_constant<Ownership, Ownership::kShare>()),
          absl::string_view(data, length)),
      options);
}

inline void Chain::RawBlock::AppendSubstrTo(const char* data, size_t length,
                                            absl::Cord& dest) {
  RIEGELI_ASSERT(std::greater_equal<>()(data, data_begin()))
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Cord&): "
         "substring not contained in data";
  RIEGELI_ASSERT(std::less_equal<>()(data + length, data_end()))
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Cord&): "
         "substring not contained in data";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Cord&): "
         "Cord size overflow";
  if (length <= cord_internal::MaxBytesToCopyToCord(dest) || wasteful()) {
    cord_internal::AppendToBlockyCord(absl::string_view(data, length), dest);
    return;
  }
  if (const FlatCordRef* const cord_ref =
          checked_external_object<FlatCordRef>()) {
    cord_ref->AppendSubstrTo(data, length, dest);
    return;
  }
  Ref();
  dest.Append(absl::MakeCordFromExternal(absl::string_view(data, length),
                                         [this] { Unref(); }));
}

inline void Chain::RawBlock::PrependTo(Chain& dest, const Options& options) {
  RIEGELI_ASSERT_LE(size(), std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::RawBlock::PrependTo(Chain&): "
         "Chain size overflow";
  dest.PrependRawBlock(this, options);
}

template <Chain::Ownership ownership>
inline void Chain::RawBlock::PrependTo(absl::Cord& dest) {
  RIEGELI_ASSERT_LE(size(), std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::RawBlock::PrependTo(Cord&): "
         "Chain size overflow";
  if (size() <= cord_internal::MaxBytesToCopyToCord(dest) || wasteful()) {
    cord_internal::PrependToBlockyCord(absl::string_view(*this), dest);
    Unref<ownership>();
    return;
  }
  if (const FlatCordRef* const cord_ref =
          checked_external_object<FlatCordRef>()) {
    RIEGELI_ASSERT_EQ(size(), absl::string_view(*cord_ref).size())
        << "Failed invariant of Chain::RawBlock: "
           "block size differs from Cord size";
    cord_ref->PrependTo(dest);
    Unref<ownership>();
    return;
  }
  Ref<ownership>();
  dest.Prepend(absl::MakeCordFromExternal(absl::string_view(*this),
                                          [this] { Unref(); }));
}

inline void Chain::RawBlock::PrependSubstrTo(const char* data, size_t length,
                                             Chain& dest,
                                             const Options& options) {
  RIEGELI_ASSERT(std::greater_equal<>()(data, data_begin()))
      << "Failed precondition of Chain::RawBlock::PrependSubstrTo(Chain&): "
         "substring not contained in data";
  RIEGELI_ASSERT(std::less_equal<>()(data + length, data_end()))
      << "Failed precondition of Chain::RawBlock::PrependSubstrTo(Chain&): "
         "substring not contained in data";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::RawBlock::PrependSubstrTo(Chain&): "
         "Chain size overflow";
  if (length == size()) {
    if (wasteful()) {
      dest.Prepend(absl::string_view(data, length), options);
      return;
    }
    dest.PrependRawBlock(this, options);
    return;
  }
  if (length <= kMaxBytesToCopy || wasteful()) {
    dest.Prepend(absl::string_view(data, length), options);
    return;
  }
  dest.Prepend(
      Chain::FromExternal(
          riegeli::Maker<BlockRef>(
              this, std::integral_constant<Ownership, Ownership::kShare>()),
          absl::string_view(data, length)),
      options);
}

inline void Chain::RawBlock::PrependSubstrTo(const char* data, size_t length,
                                             absl::Cord& dest) {
  RIEGELI_ASSERT(std::greater_equal<>()(data, data_begin()))
      << "Failed precondition of Chain::RawBlock::PrependSubstrTo(Cord&): "
         "substring not contained in data";
  RIEGELI_ASSERT(std::less_equal<>()(data + length, data_end()))
      << "Failed precondition of Chain::RawBlock::PrependSubstrTo(Cord&): "
         "substring not contained in data";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::RawBlock::PrependSubstrTo(Cord&): "
         "Cord size overflow";
  if (length <= cord_internal::MaxBytesToCopyToCord(dest) || wasteful()) {
    cord_internal::PrependToBlockyCord(absl::string_view(data, length), dest);
    return;
  }
  if (const FlatCordRef* const cord_ref =
          checked_external_object<FlatCordRef>()) {
    cord_ref->PrependSubstrTo(data, length, dest);
    return;
  }
  Ref();
  dest.Prepend(absl::MakeCordFromExternal(absl::string_view(data, length),
                                          [this] { Unref(); }));
}

size_t Chain::BlockIterator::CharIndexInChainInternal() const {
  if (ptr_ == kBeginShortData) {
    return 0;
  } else if (ptr_ == kEndShortData ||
             ptr_ == BlockPtrPtr::from_ptr(chain_->end_)) {
    return chain_->size();
  } else if (chain_->has_here()) {
    switch (block_index()) {
      case 0:
        return 0;
      case 1:
        return chain_->begin_[0].block_ptr->size();
      default:
        RIEGELI_ASSERT_UNREACHABLE()
            << "Failed invariant of Chain: "
               "only two block pointers fit without allocating their array";
    }
  } else {
    const size_t offset_base =
        chain_->begin_[chain_->block_offsets()].block_offset;
    return ptr_.as_ptr()[chain_->block_offsets()].block_offset - offset_base;
  }
}

Chain::RawBlock* Chain::BlockIterator::PinImpl() {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::Pin(): "
         "iterator is end()";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    RawBlock* const block = RawBlock::NewInternal(kMaxShortDataSize);
    block->AppendWithExplicitSizeToCopy(chain_->short_data(),
                                        kMaxShortDataSize);
    return block;
  }
  return ptr_.as_ptr()->block_ptr->Ref();
}

void Chain::BlockIterator::AppendTo(Chain& dest, const Options& options) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::AppendTo(Chain&): "
         "iterator is end()";
  RIEGELI_CHECK_LE(chain_->size(),
                   std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::BlockIterator::AppendTo(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest.Append(chain_->short_data(), options);
  } else {
    ptr_.as_ptr()->block_ptr->AppendTo(dest, options);
  }
}

void Chain::BlockIterator::AppendTo(absl::Cord& dest) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::AppendTo(Cord&): "
         "iterator is end()";
  RIEGELI_CHECK_LE(chain_->size(),
                   std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::BlockIterator::AppendTo(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest.Append(chain_->short_data());
  } else {
    ptr_.as_ptr()->block_ptr->AppendTo<Ownership::kShare>(dest);
  }
}

void Chain::BlockIterator::AppendSubstrTo(const char* data, size_t length,
                                          Chain& dest,
                                          const Options& options) const {
  if (length == 0) return;
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Chain&): "
         "iterator is end()";
  RIEGELI_ASSERT(std::greater_equal<>()(data, (*this)->data()))
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Chain&): "
         "substring not contained in data";
  RIEGELI_ASSERT(
      std::less_equal<>()(data + length, (*this)->data() + (*this)->size()))
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Chain&): "
         "substring not contained in data";
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest.Append(absl::string_view(data, length), options);
  } else {
    ptr_.as_ptr()->block_ptr->AppendSubstrTo(data, length, dest, options);
  }
}

void Chain::BlockIterator::AppendSubstrTo(const char* data, size_t length,
                                          absl::Cord& dest) const {
  if (length == 0) return;
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Cord&): "
         "iterator is end()";
  RIEGELI_ASSERT(std::greater_equal<>()(data, (*this)->data()))
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Cord&): "
         "substring not contained in data";
  RIEGELI_ASSERT(
      std::less_equal<>()(data + length, (*this)->data() + (*this)->size()))
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Cord&): "
         "substring not contained in data";
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest.Append(absl::string_view(data, length));
  } else {
    ptr_.as_ptr()->block_ptr->AppendSubstrTo(data, length, dest);
  }
}

void Chain::BlockIterator::PrependTo(Chain& dest,
                                     const Options& options) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::PrependTo(Chain&): "
         "iterator is end()";
  RIEGELI_CHECK_LE(chain_->size(),
                   std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::BlockIterator::PrependTo(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest.Prepend(chain_->short_data(), options);
  } else {
    ptr_.as_ptr()->block_ptr->PrependTo(dest, options);
  }
}

void Chain::BlockIterator::PrependTo(absl::Cord& dest) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::PrependTo(Cord&): "
         "iterator is end()";
  RIEGELI_CHECK_LE(chain_->size(),
                   std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::BlockIterator::PrependTo(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest.Prepend(chain_->short_data());
  } else {
    ptr_.as_ptr()->block_ptr->PrependTo<Ownership::kShare>(dest);
  }
}

void Chain::BlockIterator::PrependSubstrTo(const char* data, size_t length,
                                           Chain& dest,
                                           const Options& options) const {
  if (length == 0) return;
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of "
         "Chain::BlockIterator::PrependSubstrTo(Chain&): "
         "iterator is end()";
  RIEGELI_ASSERT(std::greater_equal<>()(data, (*this)->data()))
      << "Failed precondition of "
         "Chain::BlockIterator::PrependSubstrTo(Chain&): "
         "substring not contained in data";
  RIEGELI_ASSERT(
      std::less_equal<>()(data + length, (*this)->data() + (*this)->size()))
      << "Failed precondition of "
         "Chain::BlockIterator::PrependSubstrTo(Chain&): "
         "substring not contained in data";
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of "
         "Chain::BlockIterator::PrependSubstrTo(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest.Prepend(absl::string_view(data, length), options);
  } else {
    ptr_.as_ptr()->block_ptr->PrependSubstrTo(data, length, dest, options);
  }
}

void Chain::BlockIterator::PrependSubstrTo(const char* data, size_t length,
                                           absl::Cord& dest) const {
  if (length == 0) return;
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::PrependSubstrTo(Cord&): "
         "iterator is end()";
  RIEGELI_ASSERT(std::greater_equal<>()(data, (*this)->data()))
      << "Failed precondition of Chain::BlockIterator::PrependSubstrTo(Cord&): "
         "substring not contained in data";
  RIEGELI_ASSERT(
      std::less_equal<>()(data + length, (*this)->data() + (*this)->size()))
      << "Failed precondition of Chain::BlockIterator::PrependSubstrTo(Cord&): "
         "substring not contained in data";
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::BlockIterator::PrependSubstrTo(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest.Prepend(absl::string_view(data, length));
  } else {
    ptr_.as_ptr()->block_ptr->PrependSubstrTo(data, length, dest);
  }
}

Chain::Chain(const Chain& that) : size_(that.size_) {
  if (that.begin_ == that.end_) {
    std::memcpy(block_ptrs_.short_data, that.block_ptrs_.short_data,
                kMaxShortDataSize);
  } else {
    AppendBlocks<Ownership::kShare>(that.begin_, that.end_);
  }
}

Chain& Chain::operator=(const Chain& that) {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    UnrefBlocks();
    end_ = begin_;
    if (that.begin_ == that.end_) {
      std::memcpy(block_ptrs_.short_data, that.block_ptrs_.short_data,
                  kMaxShortDataSize);
    } else {
      AppendBlocks<Ownership::kShare>(that.begin_, that.end_);
    }
    size_ = that.size_;
  }
  return *this;
}

void Chain::ClearSlow() {
  RIEGELI_ASSERT(begin_ != end_)
      << "Failed precondition of Chain::ClearSlow(): "
         "no blocks, use Clear() instead";
  BlockPtr* const new_end = begin_ + (begin_->block_ptr->TryClear() ? 1 : 0);
  UnrefBlocks(new_end, end_);
  end_ = new_end;
}

absl::string_view Chain::FlattenSlow() {
  RIEGELI_ASSERT_GT(end_ - begin_, 1)
      << "Failed precondition of Chain::FlattenSlow(): "
         "contents already flat, use Flatten() instead";
  RawBlock* const block =
      RawBlock::NewInternal(NewBlockCapacity(0, size_, size_, Options()));
  const BlockPtr* iter = begin_;
  do {
    block->Append(absl::string_view(*iter->block_ptr));
    ++iter;
  } while (iter != end_);
  UnrefBlocks(begin_, end_);
  end_ = begin_;
  PushBack(block);
  return absl::string_view(*block);
}

inline Chain::BlockPtr* Chain::NewBlockPtrs(size_t capacity) {
  return std::allocator<BlockPtr>().allocate(2 * capacity);
}

inline void Chain::EnsureHasHere() {
  RIEGELI_ASSERT(begin_ == end_)
      << "Failed precondition of Chain::EnsureHasHere(): blocks exist";
  if (ABSL_PREDICT_FALSE(has_allocated())) {
    DeleteBlockPtrs();
    begin_ = block_ptrs_.here;
    end_ = block_ptrs_.here;
  }
}

void Chain::UnrefBlocksSlow(const BlockPtr* begin, const BlockPtr* end) {
  RIEGELI_ASSERT(begin < end)
      << "Failed precondition of Chain::UnrefBlocksSlow(): "
         "no blocks, use UnrefBlocks() instead";
  do {
    (begin++)->block_ptr->Unref();
  } while (begin != end);
}

inline void Chain::DropStolenBlocks(
    std::integral_constant<Ownership, Ownership::kShare>) const {}

inline void Chain::DropStolenBlocks(
    std::integral_constant<Ownership, Ownership::kSteal>) {
  end_ = begin_;
  size_ = 0;
}

void Chain::CopyTo(char* dest) const {
  if (empty()) return;  // `std::memcpy(nullptr, _, 0)` is undefined.
  const BlockPtr* iter = begin_;
  if (iter == end_) {
    std::memcpy(dest, block_ptrs_.short_data, size_);
  } else {
    do {
      std::memcpy(dest, iter->block_ptr->data_begin(), iter->block_ptr->size());
      dest += iter->block_ptr->size();
      ++iter;
    } while (iter != end_);
  }
}

void Chain::AppendTo(std::string& dest) const& {
  const size_t size_before = dest.size();
  RIEGELI_CHECK_LE(size_, dest.max_size() - size_before)
      << "Failed precondition of Chain::AppendTo(string&): "
         "string size overflow";
  ResizeStringAmortized(dest, size_before + size_);
  CopyTo(&dest[size_before]);
}

void Chain::AppendTo(std::string& dest) && {
  const size_t size_before = dest.size();
  RIEGELI_CHECK_LE(size_, dest.max_size() - size_before)
      << "Failed precondition of Chain::AppendTo(string&): "
         "string size overflow";
  if (dest.empty() && PtrDistance(begin_, end_) == 1) {
    RawBlock* const block = front();
    if (std::string* const string_ref =
            block->checked_external_object_with_unique_owner<std::string>()) {
      RIEGELI_ASSERT_EQ(block->size(), string_ref->size())
          << "Failed invariant of Chain::RawBlock: "
             "block size differs from string size";
      if (dest.capacity() <= string_ref->capacity()) {
        dest = std::move(*string_ref);
        block->Unref();
        end_ = begin_;
        size_ = 0;
        return;
      }
    }
  }
  ResizeStringAmortized(dest, size_before + size_);
  CopyTo(&dest[size_before]);
}

void Chain::AppendTo(absl::Cord& dest) const& {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::AppendTo(Cord&): Cord size overflow";
  const BlockPtr* iter = begin_;
  if (iter == end_) {
    dest.Append(short_data());
  } else {
    do {
      iter->block_ptr->AppendTo<Ownership::kShare>(dest);
      ++iter;
    } while (iter != end_);
  }
}

void Chain::AppendTo(absl::Cord& dest) && {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::AppendTo(Cord&): Cord size overflow";
  const BlockPtr* iter = begin_;
  if (iter == end_) {
    dest.Append(short_data());
  } else {
    do {
      iter->block_ptr->AppendTo<Ownership::kSteal>(dest);
      ++iter;
    } while (iter != end_);
    end_ = begin_;
  }
  size_ = 0;
}

void Chain::PrependTo(absl::Cord& dest) const& {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::PrependTo(Cord&): Cord size overflow";
  const BlockPtr* iter = end_;
  if (iter == begin_) {
    dest.Prepend(short_data());
  } else {
    do {
      --iter;
      iter->block_ptr->PrependTo<Ownership::kShare>(dest);
    } while (iter != begin_);
  }
}

void Chain::PrependTo(absl::Cord& dest) && {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Chain::PrependTo(Cord&): Cord size overflow";
  const BlockPtr* iter = end_;
  if (iter == begin_) {
    dest.Prepend(short_data());
  } else {
    do {
      --iter;
      iter->block_ptr->PrependTo<Ownership::kSteal>(dest);
    } while (iter != begin_);
    end_ = begin_;
  }
  size_ = 0;
}

Chain::operator std::string() const& {
  std::string dest;
  AppendTo(dest);
  return dest;
}

Chain::operator std::string() && {
  if (PtrDistance(begin_, end_) == 1) {
    RawBlock* const block = front();
    if (std::string* const string_ref =
            block->checked_external_object_with_unique_owner<std::string>()) {
      RIEGELI_ASSERT_EQ(block->size(), string_ref->size())
          << "Failed invariant of Chain::RawBlock: "
             "block size differs from string size";
      const std::string dest = std::move(*string_ref);
      block->Unref();
      end_ = begin_;
      size_ = 0;
      return dest;
    }
  }
  std::string dest;
  AppendTo(dest);
  return dest;
}

Chain::operator absl::Cord() const& {
  absl::Cord dest;
  AppendTo(dest);
  return dest;
}

Chain::operator absl::Cord() && {
  absl::Cord dest;
  std::move(*this).AppendTo(dest);
  return dest;
}

Chain::BlockAndChar Chain::BlockAndCharIndex(size_t char_index_in_chain) const {
  RIEGELI_ASSERT_LE(char_index_in_chain, size())
      << "Failed precondition of Chain::BlockAndCharIndex(): "
         "position out of range";
  if (char_index_in_chain == size()) {
    return BlockAndChar{blocks().cend(), 0};
  } else if (begin_ == end_) {
    return BlockAndChar{blocks().cbegin(), char_index_in_chain};
  } else if (has_here()) {
    BlockIterator block_iter = blocks().cbegin();
    if (char_index_in_chain >= block_iter->size()) {
      char_index_in_chain -= block_iter->size();
      ++block_iter;
      RIEGELI_ASSERT_LT(char_index_in_chain, block_iter->size())
          << "Failed invariant of Chain: "
             "only two block pointers fit without allocating their array";
    }
    return BlockAndChar{block_iter, char_index_in_chain};
  } else {
    const size_t offset_base = begin_[block_offsets()].block_offset;
    const BlockPtr* const found =
        std::upper_bound(begin_ + block_offsets() + 1, end_ + block_offsets(),
                         char_index_in_chain,
                         [&](size_t value, BlockPtr element) {
                           return value < element.block_offset - offset_base;
                         }) -
        1;
    return BlockAndChar{
        BlockIterator(this, PtrDistance(begin_ + block_offsets(), found)),
        char_index_in_chain - (found->block_offset - offset_base)};
  }
}

void Chain::DumpStructure(std::ostream& out) const {
  out << "chain {\n  size: " << size_ << " memory: " << EstimateMemory();
  for (const BlockPtr* iter = begin_; iter != end_; ++iter) {
    out << "\n  ";
    iter->block_ptr->DumpStructure(out);
  }
  out << "\n}\n";
}

size_t Chain::EstimateMemory() const {
  MemoryEstimator memory_estimator;
  memory_estimator.RegisterMemory(sizeof(Chain));
  memory_estimator.RegisterSubobjects(this);
  return memory_estimator.TotalMemory();
}

void Chain::RegisterSubobjectsImpl(MemoryEstimator& memory_estimator) const {
  if (has_allocated()) {
    memory_estimator.RegisterMemory(
        2 *
        PtrDistance(block_ptrs_.allocated.begin, block_ptrs_.allocated.end) *
        sizeof(BlockPtr));
  }
  for (const BlockPtr* iter = begin_; iter != end_; ++iter) {
    if (memory_estimator.RegisterNode(iter->block_ptr)) {
      memory_estimator.RegisterDynamicObject(iter->block_ptr);
    }
  }
}

inline void Chain::PushBack(RawBlock* block) {
  ReserveBack(1);
  end_[0].block_ptr = block;
  if (has_allocated()) {
    end_[block_offsets()].block_offset =
        begin_ == end_ ? size_t{0}
                       : end_[block_offsets() - 1].block_offset +
                             end_[-1].block_ptr->size();
  }
  ++end_;
}

inline void Chain::PushFront(RawBlock* block) {
  ReserveFront(1);
  BlockPtr* const old_begin = begin_;
  --begin_;
  begin_[0].block_ptr = block;
  if (has_allocated()) {
    begin_[block_offsets()].block_offset =
        old_begin == end_ ? size_t{0}
                          : begin_[block_offsets() + 1].block_offset -
                                begin_[0].block_ptr->size();
  }
}

inline void Chain::PopBack() {
  RIEGELI_ASSERT(begin_ != end_)
      << "Failed precondition of Chain::PopBack(): no blocks";
  --end_;
}

inline void Chain::PopFront() {
  RIEGELI_ASSERT(begin_ != end_)
      << "Failed precondition of Chain::PopFront(): no blocks";
  if (has_here()) {
    // Shift the remaining 0 or 1 block pointers to the left by 1 because
    // `begin_` must remain at `block_ptrs_.here`. There might be no pointer to
    // copy; it is cheaper to copy the array slot unconditionally.
    block_ptrs_.here[0] = block_ptrs_.here[1];
    --end_;
  } else {
    ++begin_;
  }
}

template <Chain::Ownership ownership>
inline void Chain::AppendBlocks(const BlockPtr* begin, const BlockPtr* end) {
  if (begin == end) return;
  ReserveBack(PtrDistance(begin, end));
  BlockPtr* dest_iter = end_;
  dest_iter->block_ptr = begin->block_ptr->Ref<ownership>();
  if (has_allocated()) {
    const size_t offsets = block_offsets();
    size_t offset = begin_ == end_ ? size_t{0}
                                   : dest_iter[offsets - 1].block_offset +
                                         dest_iter[-1].block_ptr->size();
    dest_iter[offsets].block_offset = offset;
    ++begin;
    ++dest_iter;
    while (begin != end) {
      dest_iter->block_ptr = begin->block_ptr->Ref<ownership>();
      offset += dest_iter[-1].block_ptr->size();
      dest_iter[offsets].block_offset = offset;
      ++begin;
      ++dest_iter;
    }
  } else {
    ++begin;
    ++dest_iter;
    if (begin != end) {
      dest_iter->block_ptr = begin->block_ptr->Ref<ownership>();
      ++begin;
      ++dest_iter;
      RIEGELI_ASSERT(begin == end)
          << "Failed invariant of Chain: "
             "only two block pointers fit without allocating their array";
    }
  }
  end_ = dest_iter;
}

template <Chain::Ownership ownership>
inline void Chain::PrependBlocks(const BlockPtr* begin, const BlockPtr* end) {
  if (begin == end) return;
  ReserveFront(PtrDistance(begin, end));
  BlockPtr* dest_iter = begin_;
  BlockPtr* const old_begin = begin_;
  begin_ -= PtrDistance(begin, end);  // For `has_allocated()` to work.
  --end;
  --dest_iter;
  dest_iter->block_ptr = end->block_ptr->Ref<ownership>();
  if (has_allocated()) {
    const size_t offsets = block_offsets();
    size_t offset = old_begin == end_ ? size_t{0}
                                      : dest_iter[offsets + 1].block_offset -
                                            dest_iter->block_ptr->size();
    dest_iter[offsets].block_offset = offset;
    while (end != begin) {
      --end;
      --dest_iter;
      dest_iter->block_ptr = end->block_ptr->Ref<ownership>();
      offset -= dest_iter->block_ptr->size();
      dest_iter[offsets].block_offset = offset;
    }
  } else {
    if (end != begin) {
      --end;
      --dest_iter;
      dest_iter->block_ptr = end->block_ptr->Ref<ownership>();
      RIEGELI_ASSERT(begin == end)
          << "Failed invariant of Chain: "
             "only two block pointers fit without allocating their array";
    }
  }
}

inline void Chain::ReserveBack(size_t extra_capacity) {
  BlockPtr* const allocated_end =
      has_here() ? block_ptrs_.here + 2 : block_ptrs_.allocated.end;
  if (ABSL_PREDICT_FALSE(extra_capacity > PtrDistance(end_, allocated_end))) {
    // The slow path is in a separate function to make easier for the compiler
    // to make good inlining decisions.
    ReserveBackSlow(extra_capacity);
  }
}

inline void Chain::ReserveFront(size_t extra_capacity) {
  BlockPtr* const allocated_begin =
      has_here() ? block_ptrs_.here : block_ptrs_.allocated.begin;
  if (ABSL_PREDICT_FALSE(extra_capacity >
                         PtrDistance(allocated_begin, begin_))) {
    // The slow path is in a separate function to make easier for the compiler
    // to make good inlining decisions.
    ReserveFrontSlow(extra_capacity);
  }
}

inline void Chain::ReserveBackSlow(size_t extra_capacity) {
  RIEGELI_ASSERT_GT(extra_capacity, 0u)
      << "Failed precondition of Chain::ReserveBackSlow(): "
         "nothing to do, use ReserveBack() instead";
  BlockPtr* old_allocated_begin;
  BlockPtr* old_allocated_end;
  if (has_here()) {
    old_allocated_begin = block_ptrs_.here;
    old_allocated_end = block_ptrs_.here + 2;
  } else {
    old_allocated_begin = block_ptrs_.allocated.begin;
    old_allocated_end = block_ptrs_.allocated.end;
  }
  RIEGELI_ASSERT_GT(extra_capacity, PtrDistance(end_, old_allocated_end))
      << "Failed precondition of Chain::ReserveBackSlow(): "
         "extra capacity fits in allocated space, use ReserveBack() instead";
  RIEGELI_ASSERT_LE(extra_capacity, std::numeric_limits<size_t>::max() /
                                            (2 * sizeof(BlockPtr)) -
                                        PtrDistance(old_allocated_begin, end_))
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t old_capacity =
      PtrDistance(old_allocated_begin, old_allocated_end);
  const size_t size = PtrDistance(begin_, end_);
  if (size + extra_capacity <= old_capacity && 2 * size <= old_capacity) {
    RIEGELI_ASSERT(has_allocated())
        << "The case of has_here() if there is space without reallocation "
           "was handled in ReserveBack()";
    // Existing array has enough capacity and is at most half full: move
    // contents to the beginning of the array. This is enough to make the
    // amortized cost of adding one element constant as long as prepending
    // leaves space at both ends.
    BlockPtr* const new_begin = old_allocated_begin;
    // Moving left, so block pointers must be moved before block offsets.
    std::memmove(new_begin, begin_, size * sizeof(BlockPtr));
    std::memmove(new_begin + old_capacity, begin_ + old_capacity,
                 size * sizeof(BlockPtr));
    begin_ = new_begin;
    end_ = new_begin + size;
    return;
  }
  // Reallocate the array, without keeping space before the contents. This is
  // enough to make the amortized cost of adding one element constant if
  // prepending leaves space at both ends.
  RIEGELI_ASSERT_LE(old_capacity / 2, std::numeric_limits<size_t>::max() /
                                              (2 * sizeof(BlockPtr)) -
                                          old_capacity)
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t new_capacity =
      UnsignedMax(PtrDistance(begin_, end_) + extra_capacity,
                  old_capacity + old_capacity / 2, size_t{16});
  BlockPtr* const new_allocated_begin = NewBlockPtrs(new_capacity);
  BlockPtr* const new_allocated_end = new_allocated_begin + new_capacity;
  BlockPtr* const new_begin = new_allocated_begin;
  BlockPtr* const new_end = new_begin + size;
  std::memcpy(new_begin, begin_, size * sizeof(BlockPtr));
  if (has_allocated()) {
    std::memcpy(new_begin + new_capacity, begin_ + old_capacity,
                size * sizeof(BlockPtr));
  } else if (size >= 1) {
    RIEGELI_ASSERT_LE(size, 2u)
        << "Failed invariant of Chain: "
           "only two block pointers fit without allocating their array";
    new_begin[new_capacity].block_offset = 0;
    if (size == 2) {
      new_begin[new_capacity + 1].block_offset = new_begin[0].block_ptr->size();
    }
  }
  DeleteBlockPtrs();
  block_ptrs_.allocated.begin = new_allocated_begin;
  block_ptrs_.allocated.end = new_allocated_end;
  begin_ = new_begin;
  end_ = new_end;
}

inline void Chain::ReserveFrontSlow(size_t extra_capacity) {
  RIEGELI_ASSERT_GT(extra_capacity, 0u)
      << "Failed precondition of Chain::ReserveFrontSlow(): "
         "nothing to do, use ReserveFront() instead";
  BlockPtr* old_allocated_begin;
  BlockPtr* old_allocated_end;
  if (has_here()) {
    if (ABSL_PREDICT_TRUE(extra_capacity <=
                          PtrDistance(end_, block_ptrs_.here + 2))) {
      // There is space without reallocation. Shift 1 block pointer to the right
      // by 1, or 0 block pointers by 1 or 2, because `begin_` must remain at
      // `block_ptrs_.here`. There might be no pointer to copy; it is cheaper to
      // copy the array slot unconditionally.
      block_ptrs_.here[1] = block_ptrs_.here[0];
      begin_ += extra_capacity;
      end_ += extra_capacity;
      return;
    }
    old_allocated_begin = block_ptrs_.here;
    old_allocated_end = end_;
  } else {
    old_allocated_begin = block_ptrs_.allocated.begin;
    old_allocated_end = block_ptrs_.allocated.end;
  }
  RIEGELI_ASSERT_GT(extra_capacity, PtrDistance(old_allocated_begin, begin_))
      << "Failed precondition of Chain::ReserveFrontSlow(): "
         "extra capacity fits in allocated space, use ReserveFront() instead";
  RIEGELI_ASSERT_LE(extra_capacity, std::numeric_limits<size_t>::max() /
                                            (2 * sizeof(BlockPtr)) -
                                        PtrDistance(begin_, old_allocated_end))
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t old_capacity =
      PtrDistance(old_allocated_begin, old_allocated_end);
  const size_t size = PtrDistance(begin_, end_);
  if (size + extra_capacity <= old_capacity && 2 * size <= old_capacity) {
    RIEGELI_ASSERT(has_allocated())
        << "The case of has_here() if there is space without reallocation "
           "was handled above";
    // Existing array has enough capacity and is at most half full: move
    // contents to the middle of the array. This makes the amortized cost of
    // adding one element constant.
    BlockPtr* const new_begin =
        old_allocated_begin + (old_capacity - size + extra_capacity) / 2;
    // Moving right, so block offsets must be moved before block pointers.
    std::memmove(new_begin + old_capacity, begin_ + old_capacity,
                 size * sizeof(BlockPtr));
    std::memmove(new_begin, begin_, size * sizeof(BlockPtr));
    begin_ = new_begin;
    end_ = new_begin + size;
    return;
  }
  // Reallocate the array, keeping space after the contents unchanged. This
  // makes the amortized cost of adding one element constant.
  RIEGELI_ASSERT_LE(old_capacity / 2, std::numeric_limits<size_t>::max() /
                                              (2 * sizeof(BlockPtr)) -
                                          old_capacity)
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t new_capacity =
      UnsignedMax(PtrDistance(begin_, old_allocated_end) + extra_capacity,
                  old_capacity + old_capacity / 2, size_t{16});
  BlockPtr* const new_allocated_begin = NewBlockPtrs(new_capacity);
  BlockPtr* const new_allocated_end = new_allocated_begin + new_capacity;
  BlockPtr* const new_end =
      new_allocated_end - PtrDistance(end_, old_allocated_end);
  BlockPtr* const new_begin = new_end - size;
  std::memcpy(new_begin, begin_, size * sizeof(BlockPtr));
  if (has_allocated()) {
    std::memcpy(new_begin + new_capacity, begin_ + old_capacity,
                size * sizeof(BlockPtr));
  } else if (size >= 1) {
    RIEGELI_ASSERT_LE(size, 2u)
        << "Failed invariant of Chain: "
           "only two block pointers fit without allocating their array";
    new_begin[new_capacity].block_offset = 0;
    if (size == 2) {
      new_begin[new_capacity + 1].block_offset = new_begin[0].block_ptr->size();
    }
  }
  DeleteBlockPtrs();
  block_ptrs_.allocated.begin = new_allocated_begin;
  block_ptrs_.allocated.end = new_allocated_end;
  begin_ = new_begin;
  end_ = new_end;
}

inline size_t Chain::NewBlockCapacity(size_t replaced_length, size_t min_length,
                                      size_t recommended_length,
                                      const Options& options) const {
  RIEGELI_ASSERT_LE(replaced_length, size_)
      << "Failed precondition of Chain::NewBlockCapacity(): "
         "length to replace greater than current size";
  RIEGELI_ASSERT_LE(min_length, RawBlock::kMaxCapacity - replaced_length)
      << "Chain block capacity overflow";
  return replaced_length +
         ApplyBufferConstraints(
             ApplySizeHint(
                 UnsignedMax(size_, SaturatingSub(options.min_block_size(),
                                                  replaced_length)),
                 options.size_hint(), size_),
             min_length, recommended_length,
             SaturatingSub(options.max_block_size(), replaced_length));
}

absl::Span<char> Chain::AppendBuffer(size_t min_length,
                                     size_t recommended_length,
                                     size_t max_length,
                                     const Options& options) {
  RIEGELI_ASSERT_LE(min_length, max_length)
      << "Failed precondition of Chain::AppendBuffer(): "
         "min_length > max_length";
  RIEGELI_CHECK_LE(min_length, std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::AppendBuffer(): "
         "Chain size overflow";
  RawBlock* block;
  if (begin_ == end_) {
    RIEGELI_ASSERT_LE(size_, kMaxShortDataSize)
        << "Failed invariant of Chain: short data size too large";
    if (min_length <= kMaxShortDataSize - size_) {
      // Do not bother returning short data if `recommended_length` or
      // `size_hint` is larger, because data will likely need to be copied later
      // to a real block.
      if (recommended_length <= kMaxShortDataSize - size_ &&
          (options.size_hint() == absl::nullopt ||
           *options.size_hint() <= kMaxShortDataSize)) {
        // Append the new space to short data.
        EnsureHasHere();
        const absl::Span<char> buffer(
            block_ptrs_.short_data + size_,
            UnsignedMin(max_length, kMaxShortDataSize - size_));
        size_ += buffer.size();
        return buffer;
      } else if (min_length == 0) {
        return absl::Span<char>();
      }
    }
    // Merge short data with the new space to a new block.
    if (ABSL_PREDICT_FALSE(min_length > RawBlock::kMaxCapacity - size_)) {
      block = RawBlock::NewInternal(kMaxShortDataSize);
      block->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushBack(block);
      block = RawBlock::NewInternal(
          NewBlockCapacity(0, min_length, recommended_length, options));
    } else {
      block = RawBlock::NewInternal(NewBlockCapacity(
          size_, UnsignedMax(min_length, kMaxShortDataSize - size_),
          recommended_length, options));
      block->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
    }
    PushBack(block);
  } else {
    RawBlock* const last = back();
    if (last->can_append(min_length)) {
      // New space can be appended in place.
      block = last;
    } else if (min_length == 0) {
      return absl::Span<char>();
    } else if (last->tiny() &&
               ABSL_PREDICT_TRUE(min_length <=
                                 RawBlock::kMaxCapacity - last->size())) {
      // The last block must be rewritten. Merge it with the new space to a
      // new block.
      block = RawBlock::NewInternal(NewBlockCapacity(
          last->size(), min_length, recommended_length, options));
      block->Append(absl::string_view(*last));
      last->Unref();
      SetBack(block);
    } else {
      block = nullptr;
      if (last->wasteful()) {
        // The last block must be rewritten. Rewrite it separately from the new
        // block to avoid rewriting the same data again if the new block gets
        // only partially filled.
        SetBack(last->Copy<Ownership::kShare>());
        if (last->TryClear() && last->can_append(min_length)) {
          // Reuse this block.
          block = last;
        } else {
          last->Unref();
        }
      }
      if (block == nullptr) {
        // Append a new block.
        block = RawBlock::NewInternal(
            NewBlockCapacity(0, min_length, recommended_length, options));
      }
      PushBack(block);
    }
  }
  const absl::Span<char> buffer = block->AppendBuffer(
      UnsignedMin(max_length, std::numeric_limits<size_t>::max() - size_));
  RIEGELI_ASSERT_GE(buffer.size(), min_length)
      << "Chain::RawBlock::AppendBuffer() returned less than the free space";
  size_ += buffer.size();
  return buffer;
}

absl::Span<char> Chain::PrependBuffer(size_t min_length,
                                      size_t recommended_length,
                                      size_t max_length,
                                      const Options& options) {
  RIEGELI_ASSERT_LE(min_length, max_length)
      << "Failed precondition of Chain::PrependBuffer(): "
         "min_length > max_length";
  RIEGELI_CHECK_LE(min_length, std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::PrependBuffer(): "
         "Chain size overflow";
  RawBlock* block;
  if (begin_ == end_) {
    RIEGELI_ASSERT_LE(size_, kMaxShortDataSize)
        << "Failed invariant of Chain: short data size too large";
    if (min_length <= kMaxShortDataSize - size_) {
      // Do not bother returning short data if `recommended_length` or
      // `size_hint` is larger, because data will likely need to be copied later
      // to a real block.
      if (recommended_length <= kMaxShortDataSize - size_ &&
          (options.size_hint() == absl::nullopt ||
           *options.size_hint() <= kMaxShortDataSize)) {
        // Prepend the new space to short data.
        EnsureHasHere();
        const absl::Span<char> buffer(
            block_ptrs_.short_data,
            UnsignedMin(max_length, kMaxShortDataSize - size_));
        std::memmove(buffer.data() + buffer.size(), block_ptrs_.short_data,
                     size_);
        size_ += buffer.size();
        return buffer;
      } else if (min_length == 0) {
        return absl::Span<char>();
      }
    }
    // Merge short data with the new space to a new block.
    if (ABSL_PREDICT_FALSE(min_length > RawBlock::kMaxCapacity - size_)) {
      block = RawBlock::NewInternal(kMaxShortDataSize);
      block->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushFront(block);
      block = RawBlock::NewInternal(
          NewBlockCapacity(0, min_length, recommended_length, options));
    } else {
      block = RawBlock::NewInternal(
          NewBlockCapacity(size_, min_length, recommended_length, options));
      block->Prepend(short_data());
    }
    PushFront(block);
  } else {
    RawBlock* const first = front();
    if (first->can_prepend(min_length)) {
      // New space can be prepended in place.
      block = first;
    } else if (min_length == 0) {
      return absl::Span<char>();
    } else if (first->tiny() &&
               ABSL_PREDICT_TRUE(min_length <=
                                 RawBlock::kMaxCapacity - first->size())) {
      // The first block must be rewritten. Merge it with the new space to a
      // new block.
      block = RawBlock::NewInternal(NewBlockCapacity(
          first->size(), min_length, recommended_length, options));
      block->Prepend(absl::string_view(*first));
      first->Unref();
      SetFront(block);
    } else {
      block = nullptr;
      if (first->wasteful()) {
        // The first block must be rewritten. Rewrite it separately from the new
        // block to avoid rewriting the same data again if the new block gets
        // only partially filled.
        SetFrontSameSize(first->Copy<Ownership::kShare>());
        if (first->TryClear() && first->can_prepend(min_length)) {
          // Reuse this block.
          block = first;
        } else {
          first->Unref();
        }
      }
      if (block == nullptr) {
        // Prepend a new block.
        block = RawBlock::NewInternal(
            NewBlockCapacity(0, min_length, recommended_length, options));
      }
      PushFront(block);
    }
  }
  const absl::Span<char> buffer = block->PrependBuffer(
      UnsignedMin(max_length, std::numeric_limits<size_t>::max() - size_));
  RIEGELI_ASSERT_GE(buffer.size(), min_length)
      << "Chain::RawBlock::PrependBuffer() returned less than the free space";
  RefreshFront();
  size_ += buffer.size();
  return buffer;
}

void Chain::Append(absl::string_view src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(string_view): "
         "Chain size overflow";
  while (!src.empty()) {
    const absl::Span<char> buffer =
        AppendBuffer(1, src.size(), src.size(), options);
    std::memcpy(buffer.data(), src.data(), buffer.size());
    src.remove_prefix(buffer.size());
  }
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
void Chain::Append(Src&& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(string&&): "
         "Chain size overflow";
  if (src.size() <= kMaxBytesToCopy ||
      Wasteful(
          RawBlock::kExternalAllocatedSize<std::string>() + src.capacity() + 1,
          src.size())) {
    // Not `std::move(src)`: forward to `Append(absl::string_view)`.
    Append(src, options);
    return;
  }
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  Append(Chain::FromExternal(std::move(src)), options);
}

template void Chain::Append(std::string&& src, const Options& options);

void Chain::Append(const Chain& src, const Options& options) {
  AppendChain<Ownership::kShare>(src, options);
}

void Chain::Append(Chain&& src, const Options& options) {
  AppendChain<Ownership::kSteal>(std::move(src), options);
}

template <Chain::Ownership ownership, typename ChainRef>
inline void Chain::AppendChain(ChainRef&& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(Chain): "
         "Chain size overflow";
  if (src.begin_ == src.end_) {
    Append(src.short_data(), options);
    return;
  }
  const BlockPtr* src_iter = src.begin_;
  // If the first block of `src` is handled specially,
  // `(src_iter++)->block_ptr->Unref<ownership>()` skips it so that
  // `AppendBlocks<ownership>()` does not append it again.
  RawBlock* const src_first = src.front();
  if (begin_ == end_) {
    if (src_first->tiny() ||
        (src.end_ - src.begin_ > 1 && src_first->wasteful())) {
      // The first block of `src` must be rewritten. Merge short data with it to
      // a new block.
      if (!short_data().empty() || !src_first->empty()) {
        RIEGELI_ASSERT_LE(src_first->size(), RawBlock::kMaxCapacity - size_)
            << "Sum of sizes of short data and a tiny or wasteful block "
               "exceeds RawBlock::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(
                      size_,
                      UnsignedMax(src_first->size(), kMaxShortDataSize - size_),
                      0, options)
                : UnsignedMax(size_ + src_first->size(), kMaxShortDataSize);
        RawBlock* const merged = RawBlock::NewInternal(capacity);
        merged->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
        merged->Append(absl::string_view(*src_first));
        PushBack(merged);
      }
      (src_iter++)->block_ptr->Unref<ownership>();
    } else if (!empty()) {
      // Copy short data to a real block.
      RawBlock* const real = RawBlock::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushBack(real);
    }
  } else {
    RawBlock* const last = back();
    if (last->tiny() && src_first->tiny()) {
    merge:
      // Boundary blocks must be merged, or they are both empty or wasteful so
      // merging them is cheaper than rewriting them separately.
      if (last->empty() && src_first->empty()) {
        PopBack();
        last->Unref();
      } else if (last->can_append(src_first->size()) &&
                 (src.end_ - src.begin_ == 1 ||
                  !last->wasteful(src_first->size()))) {
        // Boundary blocks can be appended in place; this is always cheaper than
        // merging them to a new block.
        last->Append(absl::string_view(*src_first));
      } else {
        // Boundary blocks cannot be appended in place. Merge them to a new
        // block.
        RIEGELI_ASSERT_LE(src_first->size(),
                          RawBlock::kMaxCapacity - last->size())
            << "Sum of sizes of two tiny or wasteful blocks exceeds "
               "RawBlock::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(last->size(), src_first->size(), 0, options)
                : last->size() + src_first->size();
        RawBlock* const merged = RawBlock::NewInternal(capacity);
        merged->Append(absl::string_view(*last));
        merged->Append(absl::string_view(*src_first));
        last->Unref();
        SetBack(merged);
      }
      (src_iter++)->block_ptr->Unref<ownership>();
    } else if (last->empty()) {
      if (src.end_ - src.begin_ > 1 && src_first->wasteful()) goto merge;
      // The last block is empty and must be removed.
      PopBack();
      last->Unref();
    } else if (last->wasteful()) {
      if (src.end_ - src.begin_ > 1 &&
          (src_first->empty() || src_first->wasteful())) {
        goto merge;
      }
      // The last block must reduce waste.
      if (last->can_append(src_first->size()) &&
          (src.end_ - src.begin_ == 1 || !last->wasteful(src_first->size())) &&
          src_first->size() <= kAllocationCost + last->size()) {
        // Appending in place is possible and is cheaper than rewriting the last
        // block.
        last->Append(absl::string_view(*src_first));
        (src_iter++)->block_ptr->Unref<ownership>();
      } else {
        // Appending in place is not possible, or rewriting the last block is
        // cheaper.
        SetBack(last->Copy<Ownership::kSteal>());
      }
    } else if (src.end_ - src.begin_ > 1) {
      if (src_first->empty()) {
        // The first block of `src` is empty and must be skipped.
        (src_iter++)->block_ptr->Unref<ownership>();
      } else if (src_first->wasteful()) {
        // The first block of `src` must reduce waste.
        if (last->can_append(src_first->size()) &&
            !last->wasteful(src_first->size())) {
          // Appending in place is possible; this is always cheaper than
          // rewriting the first block of `src`.
          last->Append(absl::string_view(*src_first));
        } else {
          // Appending in place is not possible.
          PushBack(src_first->Copy<Ownership::kShare>());
        }
        (src_iter++)->block_ptr->Unref<ownership>();
      }
    }
  }
  AppendBlocks<ownership>(src_iter, src.end_);
  size_ += src.size_;
  src.DropStolenBlocks(std::integral_constant<Ownership, ownership>());
}

inline void Chain::AppendRawBlock(RawBlock* block, const Options& options) {
  return AppendRawBlock(block, options, [&] { return block->Ref(); });
}

template <typename RefBlock>
inline void Chain::AppendRawBlock(RawBlock* block, const Options& options,
                                  RefBlock ref_block) {
  if (begin_ == end_) {
    if (!short_data().empty()) {
      if (block->tiny()) {
        // The block must be rewritten. Merge short data with it to a new block.
        RIEGELI_ASSERT_LE(block->size(), RawBlock::kMaxCapacity - size_)
            << "Sum of sizes of short data and a tiny block exceeds "
               "RawBlock::kMaxCapacity";
        const size_t capacity = NewBlockCapacity(
            size_, UnsignedMax(block->size(), kMaxShortDataSize - size_), 0,
            options);
        RawBlock* const merged = RawBlock::NewInternal(capacity);
        merged->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
        merged->Append(absl::string_view(*block));
        PushBack(merged);
        size_ += block->size();
        return;
      }
      // Copy short data to a real block.
      RawBlock* const real = RawBlock::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushBack(real);
    }
  } else {
    RawBlock* const last = back();
    if (last->tiny() && block->tiny()) {
      // Boundary blocks must be merged.
      if (last->can_append(block->size())) {
        // Boundary blocks can be appended in place; this is always cheaper than
        // merging them to a new block.
        last->Append(absl::string_view(*block));
      } else {
        // Boundary blocks cannot be appended in place. Merge them to a new
        // block.
        RIEGELI_ASSERT_LE(block->size(), RawBlock::kMaxCapacity - last->size())
            << "Sum of sizes of two tiny blocks exceeds RawBlock::kMaxCapacity";
        RawBlock* const merged = RawBlock::NewInternal(
            NewBlockCapacity(last->size(), block->size(), 0, options));
        merged->Append(absl::string_view(*last));
        merged->Append(absl::string_view(*block));
        last->Unref();
        SetBack(merged);
      }
      size_ += block->size();
      return;
    }
    if (last->empty()) {
      // The last block is empty and must be removed.
      last->Unref();
      SetBack(ref_block());
      size_ += block->size();
      return;
    }
    if (last->wasteful()) {
      // The last block must reduce waste.
      if (last->can_append(block->size()) &&
          block->size() <= kAllocationCost + last->size()) {
        // Appending in place is possible and is cheaper than rewriting the last
        // block.
        last->Append(absl::string_view(*block));
        size_ += block->size();
        return;
      }
      // Appending in place is not possible, or rewriting the last block is
      // cheaper.
      SetBack(last->Copy<Ownership::kSteal>());
    }
  }
  PushBack(ref_block());
  size_ += block->size();
}

void Chain::Append(const absl::Cord& src, const Options& options) {
  return AppendCord(src, options);
}

void Chain::Append(absl::Cord&& src, const Options& options) {
  return AppendCord(std::move(src), options);
}

template <typename CordRef>
inline void Chain::AppendCord(CordRef&& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(Cord): "
         "Chain size overflow";
  {
    const absl::optional<absl::string_view> flat = src.TryFlat();
    if (flat != absl::nullopt) {
      if (flat->size() <= kMaxBytesToCopy) {
        Append(*flat, options);
      } else {
        Append(Chain::FromExternal(
                   riegeli::Maker<FlatCordRef>(std::forward<CordRef>(src))),
               options);
      }
      return;
    }
  }
  // Avoid creating wasteful blocks and then rewriting them: append copied
  // fragments when their accumulated size is known, tweaking `size_hint` for
  // block sizing.
  absl::InlinedVector<absl::string_view, 4> copied_fragments;
  Options copy_options = options;
  copy_options.set_size_hint(size());
  absl::Cord::CharIterator iter = src.char_begin();
  while (iter != src.char_end()) {
    const absl::string_view fragment = absl::Cord::ChunkRemaining(iter);
    if (fragment.size() <= kMaxBytesToCopy) {
      copied_fragments.push_back(fragment);
      copy_options.set_size_hint(*copy_options.size_hint() + fragment.size());
      absl::Cord::Advance(&iter, fragment.size());
    } else {
      for (const absl::string_view copied_fragment : copied_fragments) {
        Append(copied_fragment, copy_options);
      }
      copied_fragments.clear();
      Append(Chain::FromExternal(
                 riegeli::Maker<FlatCordRef>(iter, fragment.size())),
             options);
      copy_options.set_size_hint(size());
    }
  }
  for (const absl::string_view copied_fragment : copied_fragments) {
    Append(copied_fragment, options);
  }
}

void Chain::Append(const SizedSharedBuffer& src, const Options& options) {
  AppendSizedSharedBuffer(src, options);
}

void Chain::Append(SizedSharedBuffer&& src, const Options& options) {
  AppendSizedSharedBuffer(std::move(src), options);
}

template <typename SizedSharedBufferRef>
inline void Chain::AppendSizedSharedBuffer(SizedSharedBufferRef&& src,
                                           const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size())
      << "Failed precondition of Chain::Append(): "
         "Chain size overflow";
  const absl::string_view data(src);
  if (src.size() <= kMaxBytesToCopy ||
      Wasteful(
          RawBlock::kExternalAllocatedSize<SharedBufferRef>() + src.capacity(),
          src.size())) {
    Append(data, options);
    return;
  }
  Append(Chain::FromExternal(
             riegeli::Maker<SharedBufferRef>(
                 std::forward<SizedSharedBufferRef>(src).storage()),
             data),
         options);
}

void Chain::Prepend(absl::string_view src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(string_view): "
         "Chain size overflow";
  while (!src.empty()) {
    const absl::Span<char> buffer =
        PrependBuffer(1, src.size(), src.size(), options);
    std::memcpy(buffer.data(), src.data() + (src.size() - buffer.size()),
                buffer.size());
    src.remove_suffix(buffer.size());
  }
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
void Chain::Prepend(Src&& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(string&&): "
         "Chain size overflow";
  if (src.size() <= kMaxBytesToCopy ||
      Wasteful(
          RawBlock::kExternalAllocatedSize<std::string>() + src.capacity() + 1,
          src.size())) {
    // Not `std::move(src)`: forward to `Prepend(absl::string_view)`.
    Prepend(src, options);
    return;
  }
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  Prepend(Chain::FromExternal(std::move(src)), options);
}

template void Chain::Prepend(std::string&& src, const Options& options);

void Chain::Prepend(const Chain& src, const Options& options) {
  PrependChain<Ownership::kShare>(src, options);
}

void Chain::Prepend(Chain&& src, const Options& options) {
  PrependChain<Ownership::kSteal>(std::move(src), options);
}

template <Chain::Ownership ownership, typename ChainRef>
inline void Chain::PrependChain(ChainRef&& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(Chain): "
         "Chain size overflow";
  if (src.begin_ == src.end_) {
    Prepend(src.short_data(), options);
    return;
  }
  const BlockPtr* src_iter = src.end_;
  // If the last block of src is handled specially,
  // `(--src_iter)->block_ptr->Unref<ownership>()` skips it so that
  // `PrependBlocks<ownership>()` does not prepend it again.
  RawBlock* const src_last = src.back();
  if (begin_ == end_) {
    if (src_last->tiny() ||
        (src.end_ - src.begin_ > 1 && src_last->wasteful())) {
      // The last block of `src` must be rewritten. Merge short data with it to
      // a new block.
      if (!short_data().empty() || !src_last->empty()) {
        RIEGELI_ASSERT_LE(src_last->size(), RawBlock::kMaxCapacity - size_)
            << "Sum of sizes of short data and a tiny or wasteful block "
               "exceeds RawBlock::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(size_, src_last->size(), 0, options)
                : size_ + src_last->size();
        RawBlock* const merged = RawBlock::NewInternal(capacity);
        merged->Prepend(short_data());
        merged->Prepend(absl::string_view(*src_last));
        PushFront(merged);
      }
      (--src_iter)->block_ptr->Unref<ownership>();
    } else if (!empty()) {
      // Copy short data to a real block.
      RawBlock* const real = RawBlock::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushFront(real);
    }
  } else {
    RawBlock* const first = front();
    if (first->tiny() && src_last->tiny()) {
    merge:
      // Boundary blocks must be merged, or they are both empty or wasteful so
      // merging them is cheaper than rewriting them separately.
      if (src_last->empty() && first->empty()) {
        PopFront();
        first->Unref();
      } else if (first->can_prepend(src_last->size()) &&
                 (src.end_ - src.begin_ == 1 ||
                  !first->wasteful(src_last->size()))) {
        // Boundary blocks can be prepended in place; this is always cheaper
        // than merging them to a new block.
        first->Prepend(absl::string_view(*src_last));
        RefreshFront();
      } else {
        // Boundary blocks cannot be prepended in place. Merge them to a new
        // block.
        RIEGELI_ASSERT_LE(src_last->size(),
                          RawBlock::kMaxCapacity - first->size())
            << "Sum of sizes of two tiny or wasteful blocks exceeds "
               "RawBlock::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(first->size(), src_last->size(), 0, options)
                : first->size() + src_last->size();
        RawBlock* const merged = RawBlock::NewInternal(capacity);
        merged->Prepend(absl::string_view(*first));
        merged->Prepend(absl::string_view(*src_last));
        first->Unref();
        SetFront(merged);
      }
      (--src_iter)->block_ptr->Unref<ownership>();
    } else if (first->empty()) {
      if (src.end_ - src.begin_ > 1 && src_last->wasteful()) goto merge;
      // The first block is empty and must be removed.
      PopFront();
      first->Unref();
    } else if (first->wasteful()) {
      if (src.end_ - src.begin_ > 1 &&
          (src_last->empty() || src_last->wasteful())) {
        goto merge;
      }
      // The first block must reduce waste.
      if (first->can_prepend(src_last->size()) &&
          (src.end_ - src.begin_ == 1 || !first->wasteful(src_last->size())) &&
          src_last->size() <= kAllocationCost + first->size()) {
        // Prepending in place is possible and is cheaper than rewriting the
        // first block.
        first->Prepend(absl::string_view(*src_last));
        RefreshFront();
        (--src_iter)->block_ptr->Unref<ownership>();
      } else {
        // Prepending in place is not possible, or rewriting the first block is
        // cheaper.
        SetFrontSameSize(first->Copy<Ownership::kSteal>());
      }
    } else if (src.end_ - src.begin_ > 1) {
      if (src_last->empty()) {
        // The last block of `src` is empty and must be skipped.
        (--src_iter)->block_ptr->Unref<ownership>();
      } else if (src_last->wasteful()) {
        // The last block of `src` must reduce waste.
        if (first->can_prepend(src_last->size()) &&
            !first->wasteful(src_last->size())) {
          // Prepending in place is possible; this is always cheaper than
          // rewriting the last block of `src`.
          first->Prepend(absl::string_view(*src_last));
          RefreshFront();
        } else {
          // Prepending in place is not possible.
          PushFront(src_last->Copy<Ownership::kShare>());
        }
        (--src_iter)->block_ptr->Unref<ownership>();
      }
    }
  }
  PrependBlocks<ownership>(src.begin_, src_iter);
  size_ += src.size_;
  src.DropStolenBlocks(std::integral_constant<Ownership, ownership>());
}

inline void Chain::PrependRawBlock(RawBlock* block, const Options& options) {
  return PrependRawBlock(block, options, [&] { return block->Ref(); });
}

template <typename RefBlock>
inline void Chain::PrependRawBlock(RawBlock* block, const Options& options,
                                   RefBlock ref_block) {
  if (begin_ == end_) {
    if (!short_data().empty()) {
      if (block->tiny()) {
        // The block must be rewritten. Merge short data with it to a new block.
        RIEGELI_ASSERT_LE(block->size(), RawBlock::kMaxCapacity - size_)
            << "Sum of sizes of short data and a tiny block exceeds "
               "RawBlock::kMaxCapacity";
        const size_t capacity =
            NewBlockCapacity(size_, block->size(), 0, options);
        RawBlock* const merged = RawBlock::NewInternal(capacity);
        merged->Prepend(short_data());
        merged->Prepend(absl::string_view(*block));
        PushFront(merged);
        size_ += block->size();
        return;
      }
      // Copy short data to a real block.
      RawBlock* const real = RawBlock::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushFront(real);
    }
  } else {
    RawBlock* const first = front();
    if (first->tiny() && block->tiny()) {
      // Boundary blocks must be merged.
      if (first->can_prepend(block->size())) {
        // Boundary blocks can be prepended in place; this is always cheaper
        // than merging them to a new block.
        first->Prepend(absl::string_view(*block));
        RefreshFront();
      } else {
        // Boundary blocks cannot be prepended in place. Merge them to a new
        // block.
        RIEGELI_ASSERT_LE(block->size(), RawBlock::kMaxCapacity - first->size())
            << "Sum of sizes of two tiny blocks exceeds RawBlock::kMaxCapacity";
        RawBlock* const merged = RawBlock::NewInternal(
            NewBlockCapacity(first->size(), block->size(), 0, options));
        merged->Prepend(absl::string_view(*first));
        merged->Prepend(absl::string_view(*block));
        first->Unref();
        SetFront(merged);
      }
      size_ += block->size();
      return;
    }
    if (first->empty()) {
      // The first block is empty and must be removed.
      first->Unref();
      SetFront(ref_block());
      size_ += block->size();
      return;
    }
    if (first->wasteful()) {
      // The first block must reduce waste.
      if (first->can_prepend(block->size()) &&
          block->size() <= kAllocationCost + first->size()) {
        // Prepending in place is possible and is cheaper than rewriting the
        // first block.
        first->Prepend(absl::string_view(*block));
        RefreshFront();
        size_ += block->size();
        return;
      }
      // Prepending in place is not possible, or rewriting the first block is
      // cheaper.
      SetFrontSameSize(first->Copy<Ownership::kSteal>());
    }
  }
  PushFront(ref_block());
  size_ += block->size();
}

void Chain::Prepend(const absl::Cord& src, const Options& options) {
  return PrependCord(src, options);
}

void Chain::Prepend(absl::Cord&& src, const Options& options) {
  return PrependCord(std::move(src), options);
}

template <typename CordRef>
inline void Chain::PrependCord(CordRef&& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(Cord): "
         "Chain size overflow";
  {
    const absl::optional<absl::string_view> flat = src.TryFlat();
    if (flat != absl::nullopt) {
      if (flat->size() <= kMaxBytesToCopy) {
        Prepend(*flat, options);
        return;
      }
    }
  }
  Prepend(Chain(std::forward<CordRef>(src)), options);
}

void Chain::Prepend(const SizedSharedBuffer& src, const Options& options) {
  PrependSizedSharedBuffer(src, options);
}

void Chain::Prepend(SizedSharedBuffer&& src, const Options& options) {
  PrependSizedSharedBuffer(std::move(src), options);
}

template <typename SizedSharedBufferRef>
inline void Chain::PrependSizedSharedBuffer(SizedSharedBufferRef&& src,
                                            const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size())
      << "Failed precondition of Chain::Prepend(): "
         "Chain size overflow";
  const absl::string_view data(src);
  if (src.size() <= kMaxBytesToCopy ||
      Wasteful(
          RawBlock::kExternalAllocatedSize<SharedBufferRef>() + src.capacity(),
          src.size())) {
    Prepend(data, options);
    return;
  }
  Prepend(Chain::FromExternal(
              riegeli::Maker<SharedBufferRef>(
                  std::forward<SizedSharedBufferRef>(src).storage()),
              data),
          options);
}

void Chain::AppendFrom(absl::Cord::CharIterator& iter, size_t length,
                       const Options& options) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::AppendFrom(): "
         "Chain size overflow";
  // Avoid creating wasteful blocks and then rewriting them: append copied
  // fragments when their accumulated size is known, tweaking `size_hint` for
  // block sizing.
  absl::InlinedVector<absl::string_view, 4> copied_fragments;
  Options copy_options = options;
  copy_options.set_size_hint(size());
  while (length > 0) {
    absl::string_view fragment = absl::Cord::ChunkRemaining(iter);
    fragment = absl::string_view(fragment.data(),
                                 UnsignedMin(fragment.size(), length));
    if (fragment.size() <= kMaxBytesToCopy) {
      copied_fragments.push_back(fragment);
      copy_options.set_size_hint(*copy_options.size_hint() + fragment.size());
      absl::Cord::Advance(&iter, fragment.size());
    } else {
      for (const absl::string_view copied_fragment : copied_fragments) {
        Append(copied_fragment, copy_options);
      }
      copied_fragments.clear();
      Append(Chain::FromExternal(
                 riegeli::Maker<FlatCordRef>(iter, fragment.size())),
             options);
      copy_options.set_size_hint(size());
    }
    length -= fragment.size();
  }
  for (const absl::string_view copied_fragment : copied_fragments) {
    Append(copied_fragment, options);
  }
}

void Chain::RemoveSuffix(size_t length, const Options& options) {
  if (length == 0) return;
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of Chain::RemoveSuffix(): "
      << "length to remove greater than current size";
  size_ -= length;
  if (begin_ == end_) {
    // `Chain` has short data which have suffix removed in place.
    return;
  }
  BlockPtr* iter = end_;
  while (length > iter[-1].block_ptr->size()) {
    length -= iter[-1].block_ptr->size();
    (--iter)->block_ptr->Unref();
    RIEGELI_ASSERT(iter != begin_)
        << "Failed invariant of Chain: "
           "sum of block sizes smaller than Chain size";
  }
  RawBlock* const last = iter[-1].block_ptr;
  if (last->TryRemoveSuffix(length)) {
    end_ = iter;
    if (end_ - begin_ > 1 && last->tiny()) {
      RawBlock* const before_last = end_[-2].block_ptr;
      if (before_last->tiny()) {
        // Last two blocks must be merged.
        --end_;
        if (!last->empty()) {
          RIEGELI_ASSERT_LE(last->size(),
                            RawBlock::kMaxCapacity - before_last->size())
              << "Sum of sizes of two tiny blocks exceeds "
                 "RawBlock::kMaxCapacity";
          RawBlock* const merged = RawBlock::NewInternal(NewBlockCapacity(
              before_last->size() + last->size(), 0, 0, options));
          merged->Append(absl::string_view(*before_last));
          merged->Append(absl::string_view(*last));
          before_last->Unref();
          SetBack(merged);
        }
        last->Unref();
      }
    }
    return;
  }
  end_ = --iter;
  if (length == last->size()) {
    last->Unref();
    return;
  }
  absl::string_view data(*last);
  data.remove_suffix(length);
  // Compensate for increasing `size_` by `Append()`.
  size_ -= data.size();
  if (data.size() <= kMaxBytesToCopy) {
    Append(data, options);
    last->Unref();
    return;
  }
  Append(Chain::FromExternal(
             riegeli::Maker<BlockRef>(
                 last, std::integral_constant<Ownership, Ownership::kSteal>()),
             data),
         options);
}

void Chain::RemovePrefix(size_t length, const Options& options) {
  if (length == 0) return;
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of Chain::RemovePrefix(): "
      << "length to remove greater than current size";
  size_ -= length;
  if (begin_ == end_) {
    // `Chain` has short data which have prefix removed by shifting the rest.
    std::memmove(block_ptrs_.short_data, block_ptrs_.short_data + length,
                 size_);
    return;
  }
  BlockPtr* iter = begin_;
  while (length > iter[0].block_ptr->size()) {
    length -= iter[0].block_ptr->size();
    (iter++)->block_ptr->Unref();
    RIEGELI_ASSERT(iter != end_)
        << "Failed invariant of Chain: "
           "sum of block sizes smaller than Chain size";
  }
  RawBlock* const first = iter[0].block_ptr;
  if (first->TryRemovePrefix(length)) {
    if (has_here()) {
      RIEGELI_ASSERT_LE(PtrDistance(begin_, iter), 1u)
          << "Failed invariant of Chain: "
             "only two block pointers fit without allocating their array";
      if (iter > begin_) {
        // Shift 1 block pointer to the left by 1 because `begin_` must remain
        // at `block_ptrs_.here`.
        block_ptrs_.here[0] = block_ptrs_.here[1];
        --end_;
      }
    } else {
      begin_ = iter;
      RefreshFront();
    }
    if (end_ - begin_ > 1 && first->tiny()) {
      RawBlock* const after_first = begin_[1].block_ptr;
      if (after_first->tiny()) {
        // First two blocks must be merged.
        if (has_here()) {
          RIEGELI_ASSERT_EQ(PtrDistance(begin_, end_), 2u)
              << "Failed invariant of Chain: "
                 "only two block pointers fit without allocating their array";
          // Shift 1 block pointer to the left by 1 because `begin_` must remain
          // at `block_ptrs_.here`.
          block_ptrs_.here[0] = block_ptrs_.here[1];
          --end_;
        } else {
          ++begin_;
        }
        if (!first->empty()) {
          RIEGELI_ASSERT_LE(first->size(),
                            RawBlock::kMaxCapacity - after_first->size())
              << "Sum of sizes of two tiny blocks exceeds "
                 "RawBlock::kMaxCapacity";
          RawBlock* const merged = RawBlock::NewInternal(NewBlockCapacity(
              first->size() + after_first->size(), 0, 0, options));
          merged->Prepend(absl::string_view(*after_first));
          merged->Prepend(absl::string_view(*first));
          after_first->Unref();
          SetFront(merged);
        }
        first->Unref();
      }
    }
    return;
  }
  ++iter;
  if (has_here()) {
    RIEGELI_ASSERT_LE(PtrDistance(begin_, iter), 2u)
        << "Failed invariant of Chain: "
           "only two block pointers fit without allocating their array";
    // Shift 1 block pointer to the left by 1, or 0 block pointers by 1 or 2,
    // because `begin_` must remain at `block_ptrs_.here`. There might be no
    // pointer to copy; it is cheaper to copy the array slot unconditionally.
    block_ptrs_.here[0] = block_ptrs_.here[1];
    end_ -= PtrDistance(block_ptrs_.here, iter);
  } else {
    begin_ = iter;
  }
  if (length == first->size()) {
    first->Unref();
    return;
  }
  absl::string_view data(*first);
  data.remove_prefix(length);
  // Compensate for increasing `size_` by `Prepend()`.
  size_ -= data.size();
  if (data.size() <= kMaxBytesToCopy) {
    Prepend(data, options);
    first->Unref();
    return;
  }
  Prepend(
      Chain::FromExternal(
          riegeli::Maker<BlockRef>(
              first, std::integral_constant<Ownership, Ownership::kSteal>()),
          data),
      options);
}

void swap(Chain& a, Chain& b) noexcept {
  using std::swap;
  if (a.has_here()) {
    a.begin_ = b.block_ptrs_.here + (a.begin_ - a.block_ptrs_.here);
    a.end_ = b.block_ptrs_.here + (a.end_ - a.block_ptrs_.here);
  }
  if (b.has_here()) {
    b.begin_ = a.block_ptrs_.here + (b.begin_ - b.block_ptrs_.here);
    b.end_ = a.block_ptrs_.here + (b.end_ - b.block_ptrs_.here);
  }
  swap(a.block_ptrs_, b.block_ptrs_);
  swap(a.begin_, b.begin_);
  swap(a.end_, b.end_);
  swap(a.size_, b.size_);
}

StrongOrdering Chain::CompareImpl(const Chain& a, const Chain& b) {
  Chain::BlockIterator a_iter = a.blocks().cbegin();
  Chain::BlockIterator b_iter = b.blocks().cbegin();
  size_t this_pos = 0;
  size_t that_pos = 0;
  while (a_iter != a.blocks().cend()) {
    if (b_iter == b.blocks().cend()) {
      do {
        if (!a_iter->empty()) return StrongOrdering::greater;
        ++a_iter;
      } while (a_iter != a.blocks().cend());
      return StrongOrdering::equal;
    }
    const size_t length =
        UnsignedMin(a_iter->size() - this_pos, b_iter->size() - that_pos);
    {
      const int ordering = std::memcmp(a_iter->data() + this_pos,
                                       b_iter->data() + that_pos, length);
      if (ordering != 0) {
        return AsStrongOrdering(ordering);
      }
    }
    this_pos += length;
    if (this_pos == a_iter->size()) {
      ++a_iter;
      this_pos = 0;
    }
    that_pos += length;
    if (that_pos == b_iter->size()) {
      ++b_iter;
      that_pos = 0;
    }
  }
  while (b_iter != b.blocks().cend()) {
    if (!b_iter->empty()) return StrongOrdering::less;
    ++b_iter;
  }
  return StrongOrdering::equal;
}

StrongOrdering Chain::CompareImpl(const Chain& a, absl::string_view b) {
  Chain::BlockIterator a_iter = a.blocks().cbegin();
  size_t this_pos = 0;
  size_t that_pos = 0;
  while (a_iter != a.blocks().cend()) {
    if (that_pos == b.size()) {
      do {
        if (!a_iter->empty()) return StrongOrdering::greater;
        ++a_iter;
      } while (a_iter != a.blocks().cend());
      return StrongOrdering::equal;
    }
    const size_t length =
        UnsignedMin(a_iter->size() - this_pos, b.size() - that_pos);
    {
      const int ordering =
          std::memcmp(a_iter->data() + this_pos, b.data() + that_pos, length);
      if (ordering != 0) {
        return AsStrongOrdering(ordering);
      }
    }
    this_pos += length;
    if (this_pos == a_iter->size()) {
      ++a_iter;
      this_pos = 0;
    }
    that_pos += length;
  }
  return that_pos == b.size() ? StrongOrdering::equal : StrongOrdering::less;
}

void Chain::OutputImpl(std::ostream& out) const {
  std::ostream::sentry sentry(out);
  if (sentry) {
    if (ABSL_PREDICT_FALSE(
            size() >
            UnsignedCast(std::numeric_limits<std::streamsize>::max()))) {
      out.setstate(std::ios::badbit);
      return;
    }
    size_t lpad = 0;
    size_t rpad = 0;
    if (IntCast<size_t>(out.width()) > size()) {
      const size_t pad = IntCast<size_t>(out.width()) - size();
      if ((out.flags() & out.adjustfield) == out.left) {
        rpad = pad;
      } else {
        lpad = pad;
      }
    }
    if (lpad > 0) WritePadding(out, lpad);
    for (const absl::string_view fragment : blocks()) {
      out.write(fragment.data(), IntCast<std::streamsize>(fragment.size()));
    }
    if (rpad > 0) WritePadding(out, rpad);
    out.width(0);
  }
}

void Chain::VerifyInvariants() const {
#if RIEGELI_DEBUG
  if (begin_ == end_) {
    if (has_here()) {
      RIEGELI_CHECK_LE(size(), kMaxShortDataSize);
    } else {
      RIEGELI_CHECK_EQ(size(), 0u);
    }
  } else {
    RIEGELI_CHECK(begin_ <= end_);
    if (has_here()) {
      RIEGELI_CHECK_LE(PtrDistance(begin_, end_), 2u);
    } else {
      RIEGELI_CHECK(begin_ >= block_ptrs_.allocated.begin);
      RIEGELI_CHECK(end_ <= block_ptrs_.allocated.end);
    }
    bool is_tiny = false;
    size_t offset =
        has_allocated() ? begin_[block_offsets()].block_offset : size_t{0};
    const BlockPtr* iter = begin_;
    do {
      if (is_tiny) {
        RIEGELI_CHECK(!iter->block_ptr->tiny());
        is_tiny = false;
      } else {
        is_tiny = iter->block_ptr->tiny();
      }
      if (iter != begin_ && iter != end_ - 1) {
        RIEGELI_CHECK(!iter->block_ptr->empty());
        RIEGELI_CHECK(!iter->block_ptr->wasteful());
      }
      if (has_allocated()) {
        RIEGELI_CHECK_EQ(iter[block_offsets()].block_offset, offset);
      }
      offset += iter->block_ptr->size();
      ++iter;
    } while (iter != end_);
    if (has_allocated()) offset -= begin_[block_offsets()].block_offset;
    RIEGELI_CHECK_EQ(size(), offset);
  }
#endif
}

Chain ChainOfZeros(size_t length) {
  const absl::string_view kArrayOfZeros = ArrayOfZeros();
  Chain result;
  while (length >= kArrayOfZeros.size()) {
    result.Append(Global([] {
      return Chain::FromExternal(riegeli::Maker<ZeroRef>(), ArrayOfZeros());
    }));
    length -= kArrayOfZeros.size();
  }
  if (length > 0) {
    if (length <= kMaxBytesToCopy) {
      const absl::Span<char> buffer = result.AppendFixedBuffer(length);
      std::memset(buffer.data(), '\0', buffer.size());
    } else {
      result.Append(
          Chain::FromExternal(riegeli::Maker<ZeroRef>(),
                              absl::string_view(kArrayOfZeros.data(), length)));
    }
  }
  return result;
}

}  // namespace riegeli
