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

#include <atomic>
#include <cstring>
#include <functional>
#include <limits>
#include <ostream>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/memory_estimator.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr Chain::Options Chain::kDefaultOptions;
constexpr size_t Chain::kAnyLength;
constexpr size_t Chain::kMaxShortDataSize;
constexpr size_t Chain::kMaxBytesToCopyToChain;
constexpr size_t Chain::kMaxBytesToCopyFromCordToChain;
constexpr size_t Chain::kAllocationCost;
constexpr size_t Chain::RawBlock::kMaxCapacity;
constexpr Chain::BlockPtrPtr Chain::BlockIterator::kBeginShortData;
constexpr Chain::BlockPtrPtr Chain::BlockIterator::kEndShortData;
constexpr ChainBlock::Options ChainBlock::kDefaultOptions;
constexpr size_t ChainBlock::kMaxSize;
constexpr size_t ChainBlock::kAnyLength;
#endif

namespace {

// When deciding whether to copy an array of bytes or share memory to an
// `absl::Cord`, prefer copying up to this length.
//
// For an empty `absl::Cord` this matches `absl::Cord::InlineRep::kMaxInline`.
//
// For a non-empty `absl::Cord` this matches `kMaxBytesToCopy` from `cord.cc`.
// `absl::Cord::Append(absl::Cord)` chooses to copy bytes from an `absl::Cord`
// up to this length if the destination is not empty.
inline size_t MaxBytesToCopyToCord(absl::Cord* dest) {
  return dest->empty() ? 15 : 511;
}

void WritePadding(std::ostream& out, size_t pad) {
  char buf[64];
  std::memset(buf, out.fill(), sizeof(buf));
  while (pad > 0) {
    const size_t length = UnsignedMin(pad, sizeof(buf));
    out.write(buf, IntCast<std::streamsize>(length));
    pad -= length;
  }
}

}  // namespace

class Chain::BlockRef {
 public:
  template <Ownership ownership>
  explicit BlockRef(RawBlock* block,
                    std::integral_constant<Ownership, ownership>);

  BlockRef(const BlockRef&) = delete;
  BlockRef& operator=(const BlockRef&) = delete;

  ~BlockRef();

  void RegisterSubobjects(MemoryEstimator* memory_estimator) const;
  void DumpStructure(absl::string_view data, std::ostream& out) const;

 private:
  RawBlock* block_;
};

template <Chain::Ownership ownership>
inline Chain::BlockRef::BlockRef(RawBlock* block,
                                 std::integral_constant<Ownership, ownership>) {
  if (const BlockRef* const block_ref =
          block->checked_external_object<BlockRef>()) {
    // `block` is already a `BlockRef`. Refer to its target instead.
    RawBlock* const target = block_ref->block_;
    if (ownership == Ownership::kSteal) {
      target->Ref();
      block->Unref();
    }
    block = target;
  }
  if (ownership == Ownership::kShare) block->Ref();
  block_ = block;
}

inline Chain::BlockRef::~BlockRef() {
  if (block_ != nullptr) block_->Unref();
}

inline void Chain::BlockRef::RegisterSubobjects(
    MemoryEstimator* memory_estimator) const {
  block_->RegisterShared(memory_estimator);
}

inline void Chain::BlockRef::DumpStructure(absl::string_view data,
                                           std::ostream& out) const {
  out << "[block] { offset: " << PtrDistance(block_->data_begin(), data.data())
      << " ";
  block_->DumpStructure(out);
  out << " }";
}

class Chain::StringRef {
 public:
  explicit StringRef(std::string&& src) : src_(std::move(src)) {}

  StringRef(const StringRef&) = delete;
  StringRef& operator=(const StringRef&) = delete;

  explicit operator absl::string_view() const { return src_; }
  void RegisterSubobjects(MemoryEstimator* memory_estimator) const;
  void DumpStructure(std::ostream& out) const;

 private:
  friend class Chain;

  std::string src_;
};

inline void Chain::StringRef::RegisterSubobjects(
    MemoryEstimator* memory_estimator) const {
  memory_estimator->RegisterDynamicMemory(src_.capacity() + 1);
}

inline void Chain::StringRef::DumpStructure(std::ostream& out) const {
  out << "[string] { capacity: " << src_.capacity() << " }";
}

// Stores an `absl::Cord` which must be flat.
//
// This design relies on the fact that moving a flat `absl::Cord` results in a
// flat `absl::Cord`.
class Chain::FlatCordRef {
 public:
  explicit FlatCordRef(const absl::Cord& src);
  explicit FlatCordRef(absl::Cord&& src);
  explicit FlatCordRef(absl::Cord::CharIterator* iter, size_t length);

  FlatCordRef(const FlatCordRef&) = delete;
  FlatCordRef& operator=(const FlatCordRef&) = delete;

  explicit operator absl::string_view() const;
  void RegisterSubobjects(absl::string_view data,
                          MemoryEstimator* memory_estimator) const;
  void DumpStructure(std::ostream& out) const;

  // Appends the `absl::Cord` to `*dest`.
  void AppendTo(absl::Cord* dest) const;

  // Appends `substr` to `*dest`. `substr` must be contained in
  // `absl::string_view(*this)`.
  void AppendSubstrTo(absl::string_view substr, absl::Cord* dest) const;

  // Prepends the `absl::Cord` to `*dest`.
  void PrependTo(absl::Cord* dest) const;

 private:
  absl::Cord src_;
};

inline Chain::FlatCordRef::FlatCordRef(const absl::Cord& src) : src_(src) {}

inline Chain::FlatCordRef::FlatCordRef(absl::Cord&& src)
    : src_(std::move(src)) {}

inline Chain::FlatCordRef::FlatCordRef(absl::Cord::CharIterator* iter,
                                       size_t length)
    : src_(absl::Cord::AdvanceAndRead(iter, length)) {}

inline void Chain::FlatCordRef::RegisterSubobjects(
    absl::string_view data, MemoryEstimator* memory_estimator) const {
  // `absl::Cord` does not expose its internal structure, but this `absl::Cord`
  // is flat, so we can have a reasonable estimation. We identify the
  // `absl::Cord` node by the pointer to the first character. If some of the
  // characters are shared, all of them are shared.
  //
  // This assumption breaks only if the `absl::Cord` has a substring node. In
  // this case we do not take into account the substring node, and do not detect
  // sharing if the source node itself is present too or if multiple substrings
  // point to different parts of the source node.
  if (memory_estimator->RegisterNode(data.data())) {
    memory_estimator->RegisterMemory(
        SaturatingSub(src_.EstimatedMemoryUsage(), sizeof(absl::Cord)));
  }
}

inline Chain::FlatCordRef::operator absl::string_view() const {
  for (absl::string_view fragment : src_.Chunks()) return fragment;
  return absl::string_view();
}

inline void Chain::FlatCordRef::DumpStructure(std::ostream& out) const {
  out << "[cord] { }";
}

inline void Chain::FlatCordRef::AppendTo(absl::Cord* dest) const {
  RIEGELI_ASSERT_LE(src_.size(),
                    std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::FlatCordRef::AppendTo(): "
         "Cord size overflow";
  dest->Append(src_);
}

inline void Chain::FlatCordRef::AppendSubstrTo(absl::string_view substr,
                                               absl::Cord* dest) const {
  RIEGELI_ASSERT_LE(substr.size(),
                    std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::FlatCordRef::AppendSubstrTo(): "
         "Cord size overflow";
  if (substr.size() == src_.size()) {
    dest->Append(src_);
    return;
  }
  const absl::string_view fragment(*this);
  RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), fragment.data()))
      << "Failed precondition of Chain::FlatCordRef::AppendSubstrTo(): "
         "substring not contained in data";
  RIEGELI_ASSERT(std::less_equal<>()(substr.data() + substr.size(),
                                     fragment.data() + fragment.size()))
      << "Failed precondition of Chain::FlatCordRef::AppendSubstrTo(): "
         "substring not contained in data";
  dest->Append(
      src_.Subcord(PtrDistance(fragment.data(), substr.data()), substr.size()));
}

inline void Chain::FlatCordRef::PrependTo(absl::Cord* dest) const {
  RIEGELI_ASSERT_LE(src_.size(),
                    std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::FlatCordRef::PrependTo(): "
         "Cord size overflow";
  dest->Prepend(src_);
}

inline Chain::RawBlock* Chain::RawBlock::NewInternal(size_t min_capacity) {
  RIEGELI_ASSERT_GT(min_capacity, 0u)
      << "Failed precondition of Chain::RawBlock::NewInternal(): zero capacity";
  size_t raw_capacity;
  return SizeReturningNewAligned<RawBlock>(
      kInternalAllocatedOffset() + min_capacity, &raw_capacity, &raw_capacity);
}

inline Chain::RawBlock::RawBlock(const size_t* raw_capacity)
    : data_(allocated_begin_, 0),
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
  return PtrDistance(allocated_begin_, empty() ? allocated_end_ : data_begin());
}

inline size_t Chain::RawBlock::space_after() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::RawBlock::space_after(): "
         "block not internal";
  return PtrDistance(empty() ? allocated_begin_ : data_end(), allocated_end_);
}

inline size_t Chain::RawBlock::raw_space_before() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::RawBlock::raw_space_before(): "
         "block not internal";
  return PtrDistance(allocated_begin_, data_begin());
}

inline size_t Chain::RawBlock::raw_space_after() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::RawBlock::raw_space_after(): "
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
  const size_t final_size = size() + extra_size;
  return final_size < kMinBufferSize;
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
  return Wasteful(capacity(), size() + extra_size);
}

inline void Chain::RawBlock::RegisterShared(
    MemoryEstimator* memory_estimator) const {
  if (memory_estimator->RegisterNode(this)) {
    if (is_internal()) {
      memory_estimator->RegisterDynamicMemory(kInternalAllocatedOffset() +
                                              capacity());
    } else {
      external_.methods->register_unique(this, memory_estimator);
    }
  }
}

inline void Chain::RawBlock::DumpStructure(std::ostream& out) const {
  out << "block {";
  const size_t ref_count = ref_count_.load(std::memory_order_relaxed);
  if (ref_count != 1) out << " ref_count: " << ref_count;
  out << " size: " << size();
  if (is_internal()) {
    if (raw_space_before() > 0) {
      out << " space_before: " << raw_space_before();
    }
    out << " space_after: " << raw_space_after();
  } else {
    out << " ";
    external_.methods->dump_structure(this, out);
  }
  out << " }";
}

inline bool Chain::RawBlock::can_append(size_t length) const {
  return is_internal() && has_unique_owner() && space_after() >= length;
}

inline bool Chain::RawBlock::can_prepend(size_t length) const {
  return is_internal() && has_unique_owner() && space_before() >= length;
}

inline bool Chain::RawBlock::CanAppendMovingData(size_t length,
                                                 size_t* space_before_if_not) {
  if (is_internal() && has_unique_owner()) {
    if (space_after() >= length) return true;
    const size_t final_size = size() + length;
    if (final_size * 2 <= capacity()) {
      // Existing array has at least twice more space than necessary: move
      // contents to the middle of the array, which keeps the amortized cost of
      // adding one element constant.
      //
      // Redundant cast is needed for `-fsanitize=bounds`.
      char* const new_begin =
          static_cast<char*>(allocated_begin_) + (capacity() - final_size) / 2;
      std::memmove(new_begin, data_.data(), data_.size());
      data_ = absl::string_view(new_begin, data_.size());
      return true;
    }
    *space_before_if_not = space_before();
  } else {
    *space_before_if_not = 0;
  }
  return false;
}

inline bool Chain::RawBlock::CanPrependMovingData(size_t length,
                                                  size_t* space_after_if_not) {
  if (is_internal() && has_unique_owner()) {
    if (space_before() >= length) return true;
    const size_t final_size = size() + length;
    if (final_size * 2 <= capacity()) {
      // Existing array has at least twice more space than necessary: move
      // contents to the middle of the array, which keeps the amortized cost of
      // adding one element constant.
      char* const new_begin =
          allocated_end_ - (capacity() - final_size) / 2 - data_.size();
      std::memmove(new_begin, data_.data(), data_.size());
      data_ = absl::string_view(new_begin, data_.size());
      return true;
    }
    *space_after_if_not = space_after();
  } else {
    *space_after_if_not = 0;
  }
  return false;
}

inline absl::Span<char> Chain::RawBlock::AppendBuffer(size_t max_length) {
  RIEGELI_ASSERT(can_append(0))
      << "Failed precondition of Chain::RawBlock::AppendBuffer(): "
         "block is immutable";
  if (empty()) data_ = absl::string_view(allocated_begin_, 0);
  const size_t length = UnsignedMin(raw_space_after(), max_length);
  const absl::Span<char> buffer(const_cast<char*>(data_end()), length);
  data_ = absl::string_view(
      data_begin(), PtrDistance(data_begin(), buffer.data() + buffer.size()));
  return buffer;
}

inline absl::Span<char> Chain::RawBlock::PrependBuffer(size_t max_length) {
  RIEGELI_ASSERT(can_prepend(0))
      << "Failed precondition of Chain::RawBlock::PrependBuffer(): "
         "block is immutable";
  if (empty()) data_ = absl::string_view(allocated_end_, 0);
  const size_t length = UnsignedMin(raw_space_before(), max_length);
  const absl::Span<char> buffer(const_cast<char*>(data_begin()) - length,
                                length);
  data_ =
      absl::string_view(buffer.data(), PtrDistance(buffer.data(), data_end()));
  return buffer;
}

inline void Chain::RawBlock::Append(absl::string_view src,
                                    size_t space_before) {
  if (empty()) {
    // Redundant cast is needed for `-fsanitize=bounds`.
    data_ = absl::string_view(
        static_cast<char*>(allocated_begin_) + space_before, 0);
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
  data_ = absl::string_view(data_begin(), size() + src.size());
}

inline void Chain::RawBlock::Prepend(absl::string_view src,
                                     size_t space_after) {
  RIEGELI_ASSERT(can_prepend(src.size()))
      << "Failed precondition of Chain::RawBlock::Prepend(): "
         "not enough space";
  if (empty()) data_ = absl::string_view(allocated_end_ - space_after, 0);
  std::memcpy(const_cast<char*>(data_begin() - src.size()), src.data(),
              src.size());
  data_ = absl::string_view(data_begin() - src.size(), size() + src.size());
}

inline void Chain::RawBlock::AppendTo(Chain* dest, const Options& options) {
  RIEGELI_ASSERT_LE(size(), std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::RawBlock::AppendTo(Chain*): "
         "Chain size overflow";
  dest->AppendBlock<Ownership::kShare>(this, options);
}

template <Chain::Ownership ownership>
inline void Chain::RawBlock::AppendTo(absl::Cord* dest) {
  RIEGELI_ASSERT_LE(size(), std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::RawBlock::AppendTo(Cord*): "
         "Cord size overflow";
  if (size() <= MaxBytesToCopyToCord(dest)) {
    dest->Append(absl::string_view(*this));
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
  dest->Append(absl::MakeCordFromExternal(
      absl::string_view(*this), [this](absl::string_view) { Unref(); }));
}

inline void Chain::RawBlock::AppendSubstrTo(absl::string_view substr,
                                            Chain* dest,
                                            const Options& options) {
  RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), data_begin()))
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Chain*): "
         "substring not contained in data";
  RIEGELI_ASSERT(std::less_equal<>()(substr.data() + substr.size(), data_end()))
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Chain*): "
         "substring not contained in data";
  RIEGELI_ASSERT_LE(substr.size(),
                    std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Chain*): "
         "Chain size overflow";
  if (substr.size() == size()) {
    dest->AppendBlock<Ownership::kShare>(this, options);
    return;
  }
  if (substr.size() <= kMaxBytesToCopyToChain) {
    dest->Append(substr, options);
    return;
  }
  dest->Append(
      ChainBlock::FromExternal<BlockRef>(
          std::forward_as_tuple(
              this, std::integral_constant<Ownership, Ownership::kShare>()),
          substr),
      options);
}

inline void Chain::RawBlock::AppendSubstrTo(absl::string_view substr,
                                            absl::Cord* dest) {
  RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), data_begin()))
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Cord*): "
         "substring not contained in data";
  RIEGELI_ASSERT(std::less_equal<>()(substr.data() + substr.size(), data_end()))
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Cord*): "
         "substring not contained in data";
  RIEGELI_ASSERT_LE(substr.size(),
                    std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::RawBlock::AppendSubstrTo(Cord*): "
         "Cord size overflow";
  if (substr.size() <= MaxBytesToCopyToCord(dest)) {
    dest->Append(substr);
    return;
  }
  if (const FlatCordRef* const cord_ref =
          checked_external_object<FlatCordRef>()) {
    cord_ref->AppendSubstrTo(substr, dest);
    return;
  }
  Ref();
  dest->Append(absl::MakeCordFromExternal(
      substr, [this](absl::string_view) { Unref(); }));
}

template <Chain::Ownership ownership>
inline void Chain::RawBlock::PrependTo(absl::Cord* dest) {
  RIEGELI_ASSERT_LE(size(), std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::RawBlock::PrependTo(Cord*): "
         "Chain size overflow";
  if (size() <= MaxBytesToCopyToCord(dest)) {
    dest->Prepend(absl::string_view(*this));
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
  dest->Prepend(absl::MakeCordFromExternal(
      absl::string_view(*this), [this](absl::string_view) { Unref(); }));
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
  return (*ptr_.as_ptr())->Ref();
}

void Chain::BlockIterator::AppendTo(Chain* dest, const Options& options) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::AppendTo(Chain*): "
         "iterator is end()";
  RIEGELI_CHECK_LE(chain_->size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::BlockIterator::AppendTo(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest->Append(chain_->short_data(), options);
  } else {
    (*ptr_.as_ptr())->AppendTo(dest, options);
  }
}

void Chain::BlockIterator::AppendTo(absl::Cord* dest) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::AppendTo(Cord*): "
         "iterator is end()";
  RIEGELI_CHECK_LE(chain_->size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::BlockIterator::AppendTo(Cord*): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest->Append(chain_->short_data());
  } else {
    (*ptr_.as_ptr())->AppendTo<Ownership::kShare>(dest);
  }
}

void Chain::BlockIterator::AppendSubstrTo(absl::string_view substr, Chain* dest,
                                          const Options& options) const {
  if (substr.empty()) return;
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Chain*): "
         "iterator is end()";
  RIEGELI_CHECK_LE(substr.size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    RIEGELI_ASSERT(
        std::greater_equal<>()(substr.data(), chain_->short_data().data()))
        << "Failed precondition of "
           "Chain::BlockIterator::AppendSubstrTo(Chain*): "
           "substring not contained in data";
    RIEGELI_ASSERT(std::less_equal<>()(
        substr.data() + substr.size(),
        chain_->short_data().data() + chain_->short_data().size()))
        << "Failed precondition of "
           "Chain::BlockIterator::AppendSubstrTo(Chain*): "
           "substring not contained in data";
    dest->Append(substr, options);
  } else {
    (*ptr_.as_ptr())->AppendSubstrTo(substr, dest, options);
  }
}

void Chain::BlockIterator::AppendSubstrTo(absl::string_view substr,
                                          absl::Cord* dest) const {
  if (substr.empty()) return;
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Cord): "
         "iterator is end()";
  RIEGELI_CHECK_LE(substr.size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(Cord*): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    RIEGELI_ASSERT(
        std::greater_equal<>()(substr.data(), chain_->short_data().data()))
        << "Failed precondition of "
           "Chain::BlockIterator::AppendSubstrTo(Cord*): "
           "substring not contained in data";
    RIEGELI_ASSERT(std::less_equal<>()(
        substr.data() + substr.size(),
        chain_->short_data().data() + chain_->short_data().size()))
        << "Failed precondition of "
           "Chain::BlockIterator::AppendSubstrTo(Cord*): "
           "substring not contained in data";
    dest->Append(substr);
  } else {
    (*ptr_.as_ptr())->AppendSubstrTo(substr, dest);
  }
}

void Chain::BlockIterator::PrependTo(absl::Cord* dest) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::PrependTo(Cord*): "
         "iterator is end()";
  RIEGELI_CHECK_LE(chain_->size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::BlockIterator::PrependTo(Cord*): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    dest->Prepend(chain_->short_data());
  } else {
    (*ptr_.as_ptr())->PrependTo<Ownership::kShare>(dest);
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
  RawBlock** const new_end = begin_ + ((*begin_)->TryClear() ? 1 : 0);
  UnrefBlocks(new_end, end_);
  end_ = new_end;
}

inline Chain::RawBlock** Chain::NewBlockPtrs(size_t capacity) {
  return std::allocator<RawBlock*>().allocate(capacity);
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

void Chain::UnrefBlocksSlow(RawBlock* const* begin, RawBlock* const* end) {
  RIEGELI_ASSERT(begin < end)
      << "Failed precondition of Chain::UnrefBlocksSlow(): "
         "no blocks, use UnrefBlocks() instead";
  do {
    (*begin++)->Unref();
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
  RawBlock* const* iter = begin_;
  if (iter == end_) {
    std::memcpy(dest, block_ptrs_.short_data, size_);
  } else {
    do {
      std::memcpy(dest, (*iter)->data_begin(), (*iter)->size());
      dest += (*iter)->size();
      ++iter;
    } while (iter != end_);
  }
}

void Chain::AppendTo(std::string* dest) const& {
  const size_t size_before = dest->size();
  RIEGELI_CHECK_LE(size_, dest->max_size() - size_before)
      << "Failed precondition of Chain::AppendTo(string*): "
         "string size overflow";
  dest->resize(size_before + size_);
  CopyTo(&(*dest)[size_before]);
}

void Chain::AppendTo(std::string* dest) && {
  const size_t size_before = dest->size();
  RIEGELI_CHECK_LE(size_, dest->max_size() - size_before)
      << "Failed precondition of Chain::AppendTo(string*): "
         "string size overflow";
  if (dest->empty() && PtrDistance(begin_, end_) == 1) {
    RawBlock* const block = front();
    if (StringRef* const string_ref =
            block->checked_external_object_with_unique_owner<StringRef>()) {
      RIEGELI_ASSERT_EQ(block->size(), absl::string_view(*string_ref).size())
          << "Failed invariant of Chain::RawBlock: "
             "block size differs from string size";
      if (dest->capacity() <= string_ref->src_.capacity()) {
        *dest = std::move(string_ref->src_);
        block->Unref();
        end_ = begin_;
        size_ = 0;
        return;
      }
    }
  }
  dest->resize(size_before + size_);
  CopyTo(&(*dest)[size_before]);
}

void Chain::AppendTo(absl::Cord* dest) const& {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::AppendTo(Cord*): Cord size overflow";
  RawBlock* const* iter = begin_;
  if (iter == end_) {
    dest->Append(short_data());
  } else {
    do {
      (*iter)->AppendTo<Ownership::kShare>(dest);
      ++iter;
    } while (iter != end_);
  }
}

void Chain::AppendTo(absl::Cord* dest) && {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::AppendTo(Cord*): Cord size overflow";
  RawBlock* const* iter = begin_;
  if (iter == end_) {
    dest->Append(short_data());
  } else {
    do {
      (*iter)->AppendTo<Ownership::kSteal>(dest);
      ++iter;
    } while (iter != end_);
    end_ = begin_;
  }
  size_ = 0;
}

void Chain::PrependTo(absl::Cord* dest) const& {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::PrependTo(Cord*): Cord size overflow";
  RawBlock* const* iter = end_;
  if (iter == begin_) {
    dest->Prepend(short_data());
  } else {
    do {
      --iter;
      (*iter)->PrependTo<Ownership::kShare>(dest);
    } while (iter != begin_);
  }
}

void Chain::PrependTo(absl::Cord* dest) && {
  RIEGELI_CHECK_LE(size_, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::PrependTo(Cord*): Cord size overflow";
  RawBlock* const* iter = end_;
  if (iter == begin_) {
    dest->Prepend(short_data());
  } else {
    do {
      --iter;
      (*iter)->PrependTo<Ownership::kSteal>(dest);
    } while (iter != begin_);
    end_ = begin_;
  }
  size_ = 0;
}

Chain::operator std::string() const& {
  std::string dest;
  AppendTo(&dest);
  return dest;
}

Chain::operator std::string() && {
  if (PtrDistance(begin_, end_) == 1) {
    RawBlock* const block = front();
    if (StringRef* const string_ref =
            block->checked_external_object_with_unique_owner<StringRef>()) {
      RIEGELI_ASSERT_EQ(block->size(), absl::string_view(*string_ref).size())
          << "Failed invariant of Chain::RawBlock: "
             "block size differs from string size";
      const std::string dest = std::move(string_ref->src_);
      block->Unref();
      end_ = begin_;
      size_ = 0;
      return dest;
    }
  }
  std::string dest;
  AppendTo(&dest);
  return dest;
}

Chain::operator absl::Cord() const& {
  absl::Cord dest;
  AppendTo(&dest);
  return dest;
}

Chain::operator absl::Cord() && {
  absl::Cord dest;
  std::move(*this).AppendTo(&dest);
  return dest;
}

size_t Chain::EstimateMemory() const {
  MemoryEstimator memory_estimator;
  memory_estimator.RegisterMemory(sizeof(Chain));
  RegisterSubobjects(&memory_estimator);
  return memory_estimator.TotalMemory();
}

void Chain::RegisterSubobjects(MemoryEstimator* memory_estimator) const {
  if (has_allocated()) {
    memory_estimator->RegisterMemory(
        PtrDistance(block_ptrs_.allocated.begin, block_ptrs_.allocated.end) *
        sizeof(RawBlock*));
  }
  for (RawBlock* const* iter = begin_; iter != end_; ++iter) {
    (*iter)->RegisterShared(memory_estimator);
  }
}

void Chain::DumpStructure(std::ostream& out) const {
  out << "chain {\n  size: " << size_ << " memory: " << EstimateMemory();
  for (RawBlock* const* iter = begin_; iter != end_; ++iter) {
    out << "\n  ";
    (*iter)->DumpStructure(out);
  }
  out << "\n}\n";
}

inline void Chain::PushBack(RawBlock* block) {
  ReserveBack(1);
  *end_++ = block;
}

inline void Chain::PushFront(RawBlock* block) {
  ReserveFront(1);
  *--begin_ = block;
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
    // `begin_` must remain at `block_ptrs_.here`. Use `std::memcpy()` instead
    // of assignment because the pointer being copied might be invalid if there
    // are 0 block pointers; it is cheaper to copy unconditionally.
    std::memcpy(block_ptrs_.here, block_ptrs_.here + 1, sizeof(RawBlock*));
    --end_;
  } else {
    ++begin_;
  }
}

template <Chain::Ownership ownership>
inline void Chain::AppendBlocks(RawBlock* const* begin, RawBlock* const* end) {
  ReserveBack(PtrDistance(begin, end));
  RawBlock** dest_iter = end_;
  while (begin != end) *dest_iter++ = (*begin++)->Ref<ownership>();
  end_ = dest_iter;
}

template <Chain::Ownership ownership>
inline void Chain::PrependBlocks(RawBlock* const* begin, RawBlock* const* end) {
  ReserveFront(PtrDistance(begin, end));
  RawBlock** dest_iter = begin_;
  while (end != begin) *--dest_iter = (*--end)->Ref<ownership>();
  begin_ = dest_iter;
}

inline void Chain::ReserveBack(size_t extra_capacity) {
  RawBlock** const allocated_end =
      has_here() ? block_ptrs_.here + 2 : block_ptrs_.allocated.end;
  if (ABSL_PREDICT_FALSE(extra_capacity > PtrDistance(end_, allocated_end))) {
    // The slow path is in a separate function to make easier for the compiler
    // to make good inlining decisions.
    ReserveBackSlow(extra_capacity);
  }
}

inline void Chain::ReserveFront(size_t extra_capacity) {
  RawBlock** const allocated_begin =
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
  RawBlock** old_allocated_begin;
  RawBlock** old_allocated_end;
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
  RIEGELI_ASSERT_LE(extra_capacity,
                    std::numeric_limits<size_t>::max() / sizeof(RawBlock*) -
                        PtrDistance(old_allocated_begin, end_))
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t old_capacity =
      PtrDistance(old_allocated_begin, old_allocated_end);
  const size_t final_size = PtrDistance(begin_, end_) + extra_capacity;
  if (final_size * 2 <= old_capacity) {
    // Existing array has at least twice more space than necessary: move
    // contents to the middle of the array, which keeps the amortized cost of
    // adding one element constant.
    RawBlock** const new_begin =
        old_allocated_begin + (old_capacity - final_size) / 2;
    RawBlock** const new_end = new_begin + PtrDistance(begin_, end_);
    std::memmove(new_begin, begin_,
                 PtrDistance(begin_, end_) * sizeof(RawBlock*));
    begin_ = new_begin;
    end_ = new_end;
    return;
  }
  // Reallocate the array, keeping space before the contents unchanged.
  RIEGELI_ASSERT_LE(
      old_capacity / 2,
      std::numeric_limits<size_t>::max() / sizeof(RawBlock*) - old_capacity)
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t new_capacity =
      UnsignedMax(PtrDistance(old_allocated_begin, end_) + extra_capacity,
                  old_capacity + old_capacity / 2, size_t{16});
  RawBlock** const new_allocated_begin = NewBlockPtrs(new_capacity);
  RawBlock** const new_allocated_end = new_allocated_begin + new_capacity;
  RawBlock** const new_begin =
      new_allocated_begin + PtrDistance(old_allocated_begin, begin_);
  RawBlock** const new_end = new_begin + PtrDistance(begin_, end_);
  std::memcpy(new_begin, begin_, PtrDistance(begin_, end_) * sizeof(RawBlock*));
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
  RawBlock** old_allocated_begin;
  RawBlock** old_allocated_end;
  if (has_here()) {
    if (ABSL_PREDICT_TRUE(extra_capacity <=
                          PtrDistance(end_, block_ptrs_.here + 2))) {
      // There is space without reallocation. Shift 1 block pointer to the right
      // by 1, or 0 block pointers by 1 or 2, because `begin_` must remain at
      // `block_ptrs_.here`. Use `std::memcpy()` instead of assignment because
      // the pointer being copied might be invalid if there are 0 block
      // pointers; it is cheaper to copy unconditionally.
      std::memcpy(block_ptrs_.here + 1, block_ptrs_.here, sizeof(RawBlock*));
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
  RIEGELI_ASSERT_LE(extra_capacity,
                    std::numeric_limits<size_t>::max() / sizeof(RawBlock*) -
                        PtrDistance(begin_, old_allocated_end))
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t old_capacity =
      PtrDistance(old_allocated_begin, old_allocated_end);
  const size_t final_size = PtrDistance(begin_, end_) + extra_capacity;
  if (final_size * 2 <= old_capacity) {
    // Existing array has at least twice more space than necessary: move
    // contents to the middle of the array, which keeps the amortized cost of
    // adding one element constant.
    RawBlock** const new_end =
        old_allocated_end - (old_capacity - final_size) / 2;
    RawBlock** const new_begin = new_end - PtrDistance(begin_, end_);
    std::memmove(new_begin, begin_,
                 PtrDistance(begin_, end_) * sizeof(RawBlock*));
    begin_ = new_begin;
    end_ = new_end;
    return;
  }
  // Reallocate the array, keeping space after the contents unchanged.
  RIEGELI_ASSERT_LE(
      old_capacity / 2,
      std::numeric_limits<size_t>::max() / sizeof(RawBlock*) - old_capacity)
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t new_capacity =
      UnsignedMax(PtrDistance(begin_, old_allocated_end) + extra_capacity,
                  old_capacity + old_capacity / 2, size_t{16});
  RawBlock** const new_allocated_begin = NewBlockPtrs(new_capacity);
  RawBlock** const new_allocated_end = new_allocated_begin + new_capacity;
  RawBlock** const new_end =
      new_allocated_end - PtrDistance(end_, old_allocated_end);
  RawBlock** const new_begin = new_end - PtrDistance(begin_, end_);
  std::memcpy(new_begin, begin_, PtrDistance(begin_, end_) * sizeof(RawBlock*));
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
         BufferLength(min_length,
                      SaturatingSub(options.max_block_size(), replaced_length),
                      size_, options.size_hint(),
                      UnsignedMax(recommended_length, size_ - replaced_length,
                                  SaturatingSub(options.min_block_size(),
                                                replaced_length)));
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
    // Do not bother returning short data if `recommended_length` or `size_hint`
    // is larger, because data will likely need to be copied later to a real
    // block.
    if (min_length == 0 || (min_length <= kMaxShortDataSize - size_ &&
                            recommended_length <= kMaxShortDataSize - size_ &&
                            options.size_hint() <= kMaxShortDataSize)) {
      // Append the new space to short data.
      EnsureHasHere();
      const absl::Span<char> buffer(
          block_ptrs_.short_data + size_,
          UnsignedMin(max_length, kMaxShortDataSize - size_));
      size_ += buffer.size();
      return buffer;
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
      back() = block;
    } else {
      block = nullptr;
      if (last->wasteful()) {
        // The last block must be rewritten. Rewrite it separately from the new
        // block to avoid rewriting the same data again if the new block gets
        // only partially filled.
        back() = last->Copy<Ownership::kShare>();
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
    // Do not bother returning short data if `recommended_length` or `size_hint`
    // is larger, because data will likely need to be copied later to a real
    // block.
    if (min_length == 0 || (min_length <= kMaxShortDataSize - size_ &&
                            recommended_length <= kMaxShortDataSize - size_ &&
                            options.size_hint() <= kMaxShortDataSize)) {
      // Prepend the new space to short data.
      EnsureHasHere();
      const absl::Span<char> buffer(
          block_ptrs_.short_data,
          UnsignedMin(max_length, kMaxShortDataSize - size_));
      std::memmove(buffer.data() + buffer.size(), block_ptrs_.short_data,
                   size_);
      size_ += buffer.size();
      return buffer;
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
      front() = block;
    } else {
      block = nullptr;
      if (first->wasteful()) {
        // The first block must be rewritten. Rewrite it separately from the new
        // block to avoid rewriting the same data again if the new block gets
        // only partially filled.
        front() = first->Copy<Ownership::kShare>();
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
  if (src.size() <= kMaxBytesToCopyToChain ||
      Wasteful(src.capacity(), src.size())) {
    // Not `std::move(src)`: forward to `Append(absl::string_view)`.
    Append(src, options);
    return;
  }
  Append(ChainBlock::FromExternal<StringRef>(
             std::forward_as_tuple(std::move(src))),
         options);
}

template void Chain::Append(std::string&& src, const Options& options);

void Chain::Append(const absl::Cord& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(Cord): "
         "Chain size overflow";
  absl::Cord::CharIterator iter = src.char_begin();
  while (iter != src.char_end()) {
    const absl::string_view fragment = absl::Cord::ChunkRemaining(iter);
    if (fragment.size() <= kMaxBytesToCopyFromCordToChain) {
      Append(fragment, options);
      absl::Cord::Advance(&iter, fragment.size());
    } else {
      Append(ChainBlock::FromExternal<FlatCordRef>(
                 std::forward_as_tuple(&iter, fragment.size())),
             options);
    }
  }
}

void Chain::Append(const Chain& src, const Options& options) {
  AppendImpl<Ownership::kShare>(src, options);
}

void Chain::Append(Chain&& src, const Options& options) {
  AppendImpl<Ownership::kSteal>(std::move(src), options);
}

template <Chain::Ownership ownership, typename ChainRef>
inline void Chain::AppendImpl(ChainRef&& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(Chain): "
         "Chain size overflow";
  if (src.begin_ == src.end_) {
    Append(src.short_data(), options);
    return;
  }
  RawBlock* const* src_iter = src.begin_;
  // If the first block of `src` is handled specially,
  // `(*src_iter++)->Unref<ownership>()` skips it so that
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
      (*src_iter++)->Unref<ownership>();
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
        back() = merged;
      }
      (*src_iter++)->Unref<ownership>();
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
        (*src_iter++)->Unref<ownership>();
      } else {
        // Appending in place is not possible, or rewriting the last block is
        // cheaper.
        back() = last->Copy<Ownership::kSteal>();
      }
    } else if (src.end_ - src.begin_ > 1) {
      if (src_first->empty()) {
        // The first block of `src` is empty and must be skipped.
        (*src_iter++)->Unref<ownership>();
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
        (*src_iter++)->Unref<ownership>();
      }
    }
  }
  AppendBlocks<ownership>(src_iter, src.end_);
  size_ += src.size_;
  src.DropStolenBlocks(std::integral_constant<Ownership, ownership>());
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
  if (src.size() <= kMaxBytesToCopyToChain ||
      Wasteful(src.capacity(), src.size())) {
    // Not `std::move(src)`: forward to `Prepend(absl::string_view)`.
    Prepend(src, options);
    return;
  }
  Prepend(ChainBlock::FromExternal<StringRef>(
              std::forward_as_tuple(std::move(src))),
          options);
}

template void Chain::Prepend(std::string&& src, const Options& options);

void Chain::Prepend(const absl::Cord& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(Cord): "
         "Chain size overflow";
  Prepend(Chain(src), options);
}

void Chain::Prepend(const Chain& src, const Options& options) {
  PrependImpl<Ownership::kShare>(src, options);
}

void Chain::Prepend(Chain&& src, const Options& options) {
  PrependImpl<Ownership::kSteal>(std::move(src), options);
}

template <Chain::Ownership ownership, typename ChainRef>
inline void Chain::PrependImpl(ChainRef&& src, const Options& options) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(Chain): "
         "Chain size overflow";
  if (src.begin_ == src.end_) {
    Prepend(src.short_data(), options);
    return;
  }
  RawBlock* const* src_iter = src.end_;
  // If the last block of src is handled specially,
  // `(*--src_iter)->Unref<ownership>()` skips it so that
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
      (*--src_iter)->Unref<ownership>();
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
        front() = merged;
      }
      (*--src_iter)->Unref<ownership>();
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
        (*--src_iter)->Unref<ownership>();
      } else {
        // Prepending in place is not possible, or rewriting the first block is
        // cheaper.
        front() = first->Copy<Ownership::kSteal>();
      }
    } else if (src.end_ - src.begin_ > 1) {
      if (src_last->empty()) {
        // The last block of `src` is empty and must be skipped.
        (*--src_iter)->Unref<ownership>();
      } else if (src_last->wasteful()) {
        // The last block of `src` must reduce waste.
        if (first->can_prepend(src_last->size()) &&
            !first->wasteful(src_last->size())) {
          // Prepending in place is possible; this is always cheaper than
          // rewriting the last block of `src`.
          first->Prepend(absl::string_view(*src_last));
        } else {
          // Prepending in place is not possible.
          PushFront(src_last->Copy<Ownership::kShare>());
        }
        (*--src_iter)->Unref<ownership>();
      }
    }
  }
  PrependBlocks<ownership>(src.begin_, src_iter);
  size_ += src.size_;
  src.DropStolenBlocks(std::integral_constant<Ownership, ownership>());
}

void Chain::AppendFrom(absl::Cord::CharIterator* iter, size_t length,
                       const Options& options) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::AppendFrom(): "
         "Chain size overflow";
  while (length > 0) {
    absl::string_view fragment = absl::Cord::ChunkRemaining(*iter);
    fragment = absl::string_view(fragment.data(),
                                 UnsignedMin(fragment.size(), length));
    if (fragment.size() <= kMaxBytesToCopyFromCordToChain) {
      Append(fragment, options);
      absl::Cord::Advance(iter, fragment.size());
    } else {
      Append(ChainBlock::FromExternal<FlatCordRef>(
                 std::forward_as_tuple(iter, fragment.size())),
             options);
    }
    length -= fragment.size();
  }
}

template <Chain::Ownership ownership>
void Chain::AppendBlock(RawBlock* block, const Options& options) {
  RIEGELI_ASSERT_LE(block->size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::AppendBlock(): "
         "Chain size overflow";
  if (block->empty()) {
    block->Unref<ownership>();
    return;
  }
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
        block->Unref<ownership>();
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
        back() = merged;
      }
      size_ += block->size();
      block->Unref<ownership>();
      return;
    }
    if (last->empty()) {
      // The last block is empty and must be removed.
      last->Unref();
      back() = block->Ref();
      size_ += block->size();
      block->Unref<ownership>();
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
        block->Unref<ownership>();
        return;
      }
      // Appending in place is not possible, or rewriting the last block is
      // cheaper.
      back() = last->Copy<Ownership::kSteal>();
    }
  }
  PushBack(block->Ref<ownership>());
  size_ += block->size();
}

template <Chain::Ownership ownership>
void Chain::PrependBlock(RawBlock* block, const Options& options) {
  RIEGELI_ASSERT_LE(block->size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::PrependBlock(): "
         "Chain size overflow";
  if (block->empty()) {
    block->Unref<ownership>();
    return;
  }
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
        block->Unref<ownership>();
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
        front() = merged;
      }
      size_ += block->size();
      block->Unref<ownership>();
      return;
    }
    if (first->empty()) {
      // The first block is empty and must be removed.
      first->Unref();
      front() = block->Ref();
      size_ += block->size();
      block->Unref<ownership>();
      return;
    }
    if (first->wasteful()) {
      // The first block must reduce waste.
      if (first->can_prepend(block->size()) &&
          block->size() <= kAllocationCost + first->size()) {
        // Prepending in place is possible and is cheaper than rewriting the
        // first block.
        first->Prepend(absl::string_view(*block));
        size_ += block->size();
        block->Unref<ownership>();
        return;
      }
      // Prepending in place is not possible, or rewriting the first block is
      // cheaper.
      front() = first->Copy<Ownership::kSteal>();
    }
  }
  PushFront(block->Ref<ownership>());
  size_ += block->size();
}

template void Chain::AppendBlock<Chain::Ownership::kShare>(
    RawBlock* block, const Options& options);
template void Chain::AppendBlock<Chain::Ownership::kSteal>(
    RawBlock* block, const Options& options);
template void Chain::PrependBlock<Chain::Ownership::kShare>(
    RawBlock* block, const Options& options);
template void Chain::PrependBlock<Chain::Ownership::kSteal>(
    RawBlock* block, const Options& options);

void Chain::RemoveSuffixSlow(size_t length, const Options& options) {
  RIEGELI_ASSERT_GT(length, 0u)
      << "Failed precondition of Chain::RemoveSuffixSlow(): "
         "nothing to do, use RemoveSuffix() instead";
  RIEGELI_ASSERT(begin_ != end_)
      << "Failed precondition of Chain::RemoveSuffixSlow(): "
         "no blocks, use RemoveSuffix() instead";
  RawBlock** iter = end_;
  if (length > iter[-1]->size()) {
    do {
      length -= iter[-1]->size();
      (*--iter)->Unref();
      RIEGELI_ASSERT(iter != begin_)
          << "Failed invariant of Chain: "
             "sum of block sizes smaller than Chain size";
    } while (length > iter[-1]->size());
    if (iter[-1]->TryRemoveSuffix(length)) {
      end_ = iter;
      return;
    }
  }
  RawBlock* const block = *--iter;
  end_ = iter;
  if (length == block->size()) {
    block->Unref();
    return;
  }
  absl::string_view data = absl::string_view(*block);
  data.remove_suffix(length);
  // Compensate for increasing `size_` by `Append()`.
  size_ -= data.size();
  if (data.size() <= kMaxBytesToCopyToChain) {
    Append(data, options);
    block->Unref();
    return;
  }
  Append(ChainBlock::FromExternal<BlockRef>(
             std::forward_as_tuple(
                 block, std::integral_constant<Ownership, Ownership::kSteal>()),
             data),
         options);
}

void Chain::RemovePrefixSlow(size_t length, const Options& options) {
  RIEGELI_ASSERT_GT(length, 0u)
      << "Failed precondition of Chain::RemovePrefixSlow(): "
         "nothing to do, use RemovePrefix() instead";
  RIEGELI_ASSERT(begin_ != end_)
      << "Failed precondition of Chain::RemovePrefixSlow(): "
         "no blocks, use RemovePrefix() instead";
  RawBlock** iter = begin_;
  if (length > iter[0]->size()) {
    do {
      length -= iter[0]->size();
      (*iter++)->Unref();
      RIEGELI_ASSERT(iter != end_)
          << "Failed invariant of Chain: "
             "sum of block sizes smaller than Chain size";
    } while (length > iter[0]->size());
    if (iter[0]->TryRemovePrefix(length)) {
      if (has_here()) {
        // Shift 1 block pointer to the left by 1 because `begin_` must remain
        // at `block_ptrs_.here`.
        block_ptrs_.here[0] = block_ptrs_.here[1];
        --end_;
      } else {
        begin_ = iter;
      }
      return;
    }
  }
  RawBlock* const block = *iter++;
  if (has_here()) {
    // Shift 1 block pointer to the left by 1, or 0 block pointers by 1 or 2,
    // because `begin_` must remain at `block_ptrs_.here`. Use `std::memcpy()`
    // instead of assignment because the pointer being copied might be invalid
    // if there are 0 block pointers; it is cheaper to copy unconditionally.
    std::memcpy(block_ptrs_.here, block_ptrs_.here + 1, sizeof(RawBlock*));
    end_ -= PtrDistance(block_ptrs_.here, iter);
  } else {
    begin_ = iter;
  }
  if (length == block->size()) {
    block->Unref();
    return;
  }
  absl::string_view data = absl::string_view(*block);
  data.remove_prefix(length);
  // Compensate for increasing `size_` by `Prepend()`.
  size_ -= data.size();
  if (data.size() <= kMaxBytesToCopyToChain) {
    Prepend(data, options);
    block->Unref();
    return;
  }
  Prepend(
      ChainBlock::FromExternal<BlockRef>(
          std::forward_as_tuple(
              block, std::integral_constant<Ownership, Ownership::kSteal>()),
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

absl::strong_ordering Chain::Compare(absl::string_view that) const {
  Chain::BlockIterator this_iter = blocks().cbegin();
  size_t this_pos = 0;
  size_t that_pos = 0;
  while (this_iter != blocks().cend()) {
    if (that_pos == that.size()) {
      do {
        if (!this_iter->empty()) return absl::strong_ordering::greater;
        ++this_iter;
      } while (this_iter != blocks().cend());
      return absl::strong_ordering::equal;
    }
    const size_t length =
        UnsignedMin(this_iter->size() - this_pos, that.size() - that_pos);
    const int result = std::memcmp(this_iter->data() + this_pos,
                                   that.data() + that_pos, length);
    if (result < 0) return absl::strong_ordering::less;
    if (result > 0) return absl::strong_ordering::greater;
    this_pos += length;
    if (this_pos == this_iter->size()) {
      ++this_iter;
      this_pos = 0;
    }
    that_pos += length;
  }
  return that_pos == that.size() ? absl::strong_ordering::equal
                                 : absl::strong_ordering::less;
}

absl::strong_ordering Chain::Compare(const Chain& that) const {
  Chain::BlockIterator this_iter = blocks().cbegin();
  Chain::BlockIterator that_iter = that.blocks().cbegin();
  size_t this_pos = 0;
  size_t that_pos = 0;
  while (this_iter != blocks().cend()) {
    if (that_iter == that.blocks().cend()) {
      do {
        if (!this_iter->empty()) return absl::strong_ordering::greater;
        ++this_iter;
      } while (this_iter != blocks().cend());
      return absl::strong_ordering::equal;
    }
    const size_t length =
        UnsignedMin(this_iter->size() - this_pos, that_iter->size() - that_pos);
    const int result = std::memcmp(this_iter->data() + this_pos,
                                   that_iter->data() + that_pos, length);
    if (result < 0) return absl::strong_ordering::less;
    if (result > 0) return absl::strong_ordering::greater;
    this_pos += length;
    if (this_pos == this_iter->size()) {
      ++this_iter;
      this_pos = 0;
    }
    that_pos += length;
    if (that_pos == that_iter->size()) {
      ++that_iter;
      that_pos = 0;
    }
  }
  while (that_iter != that.blocks().cend()) {
    if (!that_iter->empty()) return absl::strong_ordering::less;
    ++that_iter;
  }
  return absl::strong_ordering::equal;
}

std::ostream& operator<<(std::ostream& out, const Chain& str) {
  std::ostream::sentry sentry(out);
  if (sentry) {
    if (ABSL_PREDICT_FALSE(
            str.size() > size_t{std::numeric_limits<std::streamsize>::max()})) {
      out.setstate(std::ios::badbit);
      return out;
    }
    size_t lpad = 0;
    size_t rpad = 0;
    if (IntCast<size_t>(out.width()) > str.size()) {
      const size_t pad = IntCast<size_t>(out.width()) - str.size();
      if ((out.flags() & out.adjustfield) == out.left) {
        rpad = pad;
      } else {
        lpad = pad;
      }
    }
    if (lpad > 0) WritePadding(out, lpad);
    for (const absl::string_view fragment : str.blocks()) {
      out.write(fragment.data(), IntCast<std::streamsize>(fragment.size()));
    }
    if (rpad > 0) WritePadding(out, rpad);
    out.width(0);
  }
  return out;
}

size_t ChainBlock::EstimateMemory() const {
  MemoryEstimator memory_estimator;
  memory_estimator.RegisterMemory(sizeof(ChainBlock));
  RegisterSubobjects(&memory_estimator);
  return memory_estimator.TotalMemory();
}

void ChainBlock::RegisterSubobjects(MemoryEstimator* memory_estimator) const {
  if (block_ != nullptr) block_->RegisterShared(memory_estimator);
}

void ChainBlock::DumpStructure(std::ostream& out) const {
  out << "chain_block {\n  size: " << size() << " memory: " << EstimateMemory();
  if (block_ != nullptr) {
    out << "\n  ";
    block_->DumpStructure(out);
  }
  out << "\n}\n";
}

inline size_t ChainBlock::NewBlockCapacity(size_t old_size, size_t min_length,
                                           size_t recommended_length,
                                           const Options& options) const {
  RIEGELI_ASSERT_LE(min_length, kMaxSize - old_size)
      << "Failed precondition of ChainBlock::NewBlockCapacity(): "
         "ChainBlock size overflow";
  return old_size +
         BufferLength(
             min_length,
             UnsignedMax(recommended_length, old_size,
                         SaturatingSub(options.min_block_size(), old_size)),
             options.size_hint(), old_size);
}

absl::Span<char> ChainBlock::AppendBuffer(size_t min_length,
                                          size_t recommended_length,
                                          size_t max_length,
                                          const Options& options) {
  RIEGELI_ASSERT_LE(min_length, max_length)
      << "Failed precondition of ChainBlock::AppendBuffer(): "
         "min_length > max_length";
  RIEGELI_CHECK_LE(min_length, kMaxSize - size())
      << "Failed precondition of ChainBlock::AppendBuffer(): "
         "ChainBlock size overflow";
  if (block_ == nullptr) {
    if (min_length == 0) return absl::Span<char>();
    block_ = RawBlock::NewInternal(
        NewBlockCapacity(size(), min_length, recommended_length, options));
  } else {
    size_t space_before;
    if (!block_->CanAppendMovingData(min_length, &space_before)) {
      if (min_length == 0) return absl::Span<char>();
      // Reallocate the array, keeping space before the contents unchanged.
      RawBlock* const block = RawBlock::NewInternal(NewBlockCapacity(
          space_before + size(), min_length, recommended_length, options));
      block->Append(absl::string_view(*block_), space_before);
      block_->Unref();
      block_ = block;
    }
  }
  const absl::Span<char> buffer = block_->AppendBuffer(max_length);
  RIEGELI_ASSERT_GE(buffer.size(), min_length)
      << "Chain::RawBlock::AppendBuffer() returned less than the free space";
  return buffer;
}

absl::Span<char> ChainBlock::PrependBuffer(size_t min_length,
                                           size_t recommended_length,
                                           size_t max_length,
                                           const Options& options) {
  RIEGELI_ASSERT_LE(min_length, max_length)
      << "Failed precondition of ChainBlock::PrependBuffer(): "
         "min_length > max_length";
  RIEGELI_CHECK_LE(min_length, kMaxSize - size())
      << "Failed precondition of ChainBlock::PrependBuffer(): "
         "ChainBlock size overflow";
  if (block_ == nullptr) {
    if (min_length == 0) return absl::Span<char>();
    block_ = RawBlock::NewInternal(
        NewBlockCapacity(size(), min_length, recommended_length, options));
  } else {
    size_t space_after;
    if (!block_->CanPrependMovingData(min_length, &space_after)) {
      if (min_length == 0) return absl::Span<char>();
      // Reallocate the array, keeping space after the contents unchanged.
      RawBlock* const block = RawBlock::NewInternal(NewBlockCapacity(
          space_after + size(), min_length, recommended_length, options));
      block->Prepend(absl::string_view(*block_), space_after);
      block_->Unref();
      block_ = block;
    }
  }
  const absl::Span<char> buffer = block_->PrependBuffer(max_length);
  RIEGELI_ASSERT_GE(buffer.size(), min_length)
      << "Chain::RawBlock::PrependBuffer() returned less than the free space";
  return buffer;
}

void ChainBlock::RemoveSuffixSlow(size_t length, const Options& options) {
  RIEGELI_ASSERT_GT(length, 0u)
      << "Failed precondition of ChainBlock::RemoveSuffixSlow(): "
         "nothing to do, use RemoveSuffix() instead";
  if (length == block_->size()) {
    block_->Unref();
    block_ = nullptr;
    return;
  }
  absl::string_view data = absl::string_view(*block_);
  data.remove_suffix(length);
  RawBlock* const block = RawBlock::NewInternal(
      UnsignedMax(data.size(), data.size() < options.size_hint()
                                   ? options.size_hint()
                                   : options.min_block_size()));
  block->Append(data);
  block_->Unref();
  block_ = block;
}

void ChainBlock::RemovePrefixSlow(size_t length, const Options& options) {
  RIEGELI_ASSERT_GT(length, 0u)
      << "Failed precondition of ChainBlock::RemovePrefixSlow(): "
         "nothing to do, use RemovePrefix() instead";
  if (length == block_->size()) {
    block_->Unref();
    block_ = nullptr;
    return;
  }
  absl::string_view data = absl::string_view(*block_);
  data.remove_prefix(length);
  RawBlock* const block = RawBlock::NewInternal(
      UnsignedMax(data.size(), data.size() < options.size_hint()
                                   ? options.size_hint()
                                   : options.min_block_size()));
  block->Prepend(data);
  block_->Unref();
  block_ = block;
}

void ChainBlock::AppendTo(Chain* dest, const Chain::Options& options) const {
  if (block_ == nullptr) return;
  RIEGELI_CHECK_LE(block_->size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of ChainBlock::AppendTo(Chain*): "
         "Chain size overflow";
  block_->AppendTo(dest, options);
}

void ChainBlock::AppendTo(absl::Cord* dest) const {
  if (block_ == nullptr) return;
  RIEGELI_CHECK_LE(block_->size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of ChainBlock::AppendTo(Cord*): "
         "Cord size overflow";
  block_->AppendTo<Chain::Ownership::kShare>(dest);
}

void ChainBlock::AppendSubstrTo(absl::string_view substr, Chain* dest,
                                const Chain::Options& options) const {
  if (substr.empty()) return;
  RIEGELI_CHECK_LE(substr.size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of ChainBlock::AppendSubstrTo(Chain*): "
         "Chain size overflow";
  RIEGELI_ASSERT(block_ != nullptr)
      << "Failed precondition of ChainBlock::AppendSubstrTo(Chain*): "
         "substring not contained in data";
  block_->AppendSubstrTo(substr, dest, options);
}

void ChainBlock::AppendSubstrTo(absl::string_view substr,
                                absl::Cord* dest) const {
  if (substr.empty()) return;
  RIEGELI_CHECK_LE(substr.size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of ChainBlock::AppendSubstrTo(Cord*): "
         "Cord size overflow";
  RIEGELI_ASSERT(block_ != nullptr)
      << "Failed precondition of ChainBlock::AppendSubstrTo(Cord*): "
         "substring not contained in data";
  block_->AppendSubstrTo(substr, dest);
}

}  // namespace riegeli
