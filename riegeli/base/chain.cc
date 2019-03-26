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
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/memory_estimator.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr size_t Chain::kMinBufferSize;
constexpr size_t Chain::kMaxBufferSize;
constexpr size_t Chain::kAllocationCost;
constexpr size_t Chain::Block::kMaxCapacity;
#endif

namespace {

void WritePadding(std::ostream& out, size_t pad) {
  char buf[64];
  std::memset(buf, out.fill(), sizeof(buf));
  while (pad > 0) {
    const size_t length = UnsignedMin(pad, sizeof(buf));
    out.write(buf, length);
    pad -= length;
  }
}

}  // namespace

class Chain::BlockRef {
 public:
  explicit BlockRef(Block* block, bool add_ref);

  BlockRef(BlockRef&& that) noexcept;
  BlockRef& operator=(BlockRef&& that) noexcept;

  ~BlockRef();

  void RegisterSubobjects(absl::string_view data,
                          MemoryEstimator* memory_estimator) const;
  void DumpStructure(absl::string_view data, std::ostream& out) const;

 private:
  Block* block_;
};

inline Chain::BlockRef::BlockRef(Block* block, bool add_ref) {
  if (const BlockRef* const block_ref =
          block->checked_external_object<BlockRef>()) {
    // block is already a BlockRef. Refer to its target instead.
    Block* const target = block_ref->block_;
    if (!add_ref) {
      target->Ref();
      block->Unref();
    }
    block = target;
  }
  if (add_ref) block->Ref();
  block_ = block;
}

inline Chain::BlockRef::BlockRef(BlockRef&& that) noexcept
    : block_(absl::exchange(that.block_, nullptr)) {}

inline Chain::BlockRef& Chain::BlockRef::operator=(BlockRef&& that) noexcept {
  // Exchange that.block_ early to support self-assignment.
  Block* const block = absl::exchange(that.block_, nullptr);
  if (block_ != nullptr) block_->Unref();
  block_ = block;
  return *this;
}

inline Chain::BlockRef::~BlockRef() {
  if (block_ != nullptr) block_->Unref();
}

inline void Chain::BlockRef::RegisterSubobjects(
    absl::string_view data, MemoryEstimator* memory_estimator) const {
  block_->RegisterShared(memory_estimator);
}

inline void Chain::BlockRef::DumpStructure(absl::string_view data,
                                           std::ostream& out) const {
  out << "offset: " << (data.data() - block_->data_begin()) << "; ";
  block_->DumpStructure(out);
}

class Chain::StringRef {
 public:
  explicit StringRef(std::string src) : src_(std::move(src)) {}

  StringRef(StringRef&& that) noexcept;
  StringRef& operator=(StringRef&& that) noexcept;

  absl::string_view data() const { return src_; }
  void RegisterSubobjects(absl::string_view data,
                          MemoryEstimator* memory_estimator) const;
  void DumpStructure(absl::string_view data, std::ostream& out) const;

 private:
  friend class Chain;

  std::string src_;
};

inline Chain::StringRef::StringRef(StringRef&& that) noexcept
    : src_(std::move(that.src_)) {}

inline Chain::StringRef& Chain::StringRef::operator=(
    StringRef&& that) noexcept {
  src_ = std::move(that.src_);
  return *this;
}

inline void Chain::StringRef::RegisterSubobjects(
    absl::string_view data, MemoryEstimator* memory_estimator) const {
  memory_estimator->RegisterDynamicMemory(src_.capacity() + 1);
}

inline void Chain::StringRef::DumpStructure(absl::string_view data,
                                            std::ostream& out) const {
  out << "string";
}

inline Chain::Block* Chain::Block::NewInternal(size_t min_capacity) {
  RIEGELI_ASSERT_GT(min_capacity, 0u)
      << "Failed precondition of Chain::Block::NewInternal(): zero capacity";
  size_t raw_capacity;
  return SizeReturningNewAligned<Block>(
      kInternalAllocatedOffset() + min_capacity, &raw_capacity, &raw_capacity,
      ForAppend());
}

inline Chain::Block* Chain::Block::NewInternalForPrepend(size_t min_capacity) {
  RIEGELI_ASSERT_GT(min_capacity, 0u)
      << "Failed precondition of Chain::Block::NewInternalForPrepend(): "
         "zero capacity";
  size_t raw_capacity;
  return SizeReturningNewAligned<Block>(
      kInternalAllocatedOffset() + min_capacity, &raw_capacity, &raw_capacity,
      ForPrepend());
}

inline Chain::Block::Block(const size_t* raw_capacity, ForAppend)
    : data_(allocated_begin_, 0),
      allocated_end_(data_.data() +
                     (*raw_capacity - kInternalAllocatedOffset())) {
  RIEGELI_ASSERT(is_internal())
      << "A Block with allocated_end_ != nullptr should be considered internal";
  RIEGELI_CHECK_LE(capacity(), Block::kMaxCapacity)
      << "Chain block capacity overflow";
}

inline Chain::Block::Block(const size_t* raw_capacity, ForPrepend)
    // Redundant cast is needed for -fsanitize=bounds.
    : data_(static_cast<const char*>(allocated_begin_) +
                (*raw_capacity - kInternalAllocatedOffset()),
            0),
      allocated_end_(data_.data()) {
  RIEGELI_ASSERT(is_internal())
      << "A Block with allocated_end_ != nullptr should be considered internal";
  RIEGELI_CHECK_LE(capacity(), Block::kMaxCapacity)
      << "Chain block capacity overflow";
}

inline void Chain::Block::Unref() {
  if (has_unique_owner() ||
      ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    if (is_internal()) {
      DeleteAligned<Block>(this, kInternalAllocatedOffset() + capacity());
    } else {
      external_.methods->delete_block(this);
    }
  }
}

inline Chain::Block* Chain::Block::Copy() {
  Block* const block = NewInternal(size());
  block->Append(data());
  RIEGELI_ASSERT(!block->wasteful())
      << "A full block should not be considered wasteful";
  return block;
}

inline Chain::Block* Chain::Block::CopyAndUnref() {
  Block* const block = Copy();
  Unref();
  return block;
}

inline bool Chain::Block::TryClear() {
  if (is_internal() && has_unique_owner()) {
    data_.remove_suffix(size());
    return true;
  }
  return false;
}

inline size_t Chain::Block::capacity() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::Block::capacity(): "
         "block not internal";
  return PtrDistance(allocated_begin_, allocated_end_);
}

inline size_t Chain::Block::space_before() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::Block::space_before(): "
         "block not internal";
  return PtrDistance(allocated_begin_, data_begin());
}

inline size_t Chain::Block::space_after() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::Block::space_after(): "
         "block not internal";
  return PtrDistance(data_end(), allocated_end_);
}

inline bool Chain::Block::tiny(size_t extra_size) const {
  if (is_internal()) {
    RIEGELI_ASSERT_LE(size(), capacity())
        << "Failed invariant of Chain::Block: size greater than capacity";
    RIEGELI_ASSERT_LE(extra_size, capacity() - size())
        << "Failed precondition of Chain::Block::tiny(): "
           "extra size greater than remaining space";
  } else {
    RIEGELI_ASSERT_EQ(extra_size, 0u)
        << "Failed precondition of Chain::Block::tiny(): "
           "non-zero extra size of external block";
  }
  const size_t final_size = size() + extra_size;
  return final_size < kMinBufferSize;
}

inline bool Chain::Block::wasteful(size_t extra_size) const {
  if (is_internal()) {
    RIEGELI_ASSERT_LE(size(), capacity())
        << "Failed invariant of Chain::Block: size greater than capacity";
    RIEGELI_ASSERT_LE(extra_size, capacity() - size())
        << "Failed precondition of Chain::Block::wasteful(): "
           "extra size greater than remaining space";
  } else {
    RIEGELI_ASSERT_EQ(extra_size, 0u)
        << "Failed precondition of Chain::Block::wasteful(): "
           "non-zero extra size of external block";
    return false;
  }
  const size_t final_size = size() + extra_size;
  return capacity() - final_size > UnsignedMax(final_size, kMinBufferSize);
}

inline void Chain::Block::RegisterShared(
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

inline void Chain::Block::DumpStructure(std::ostream& out) const {
  out << "Block {ref_count: " << ref_count_.load(std::memory_order_relaxed)
      << "; size: " << size() << "; ";
  if (is_internal()) {
    out << "internal; space: " << space_before() << " + " << space_after();
  } else {
    out << "external; ";
    external_.methods->dump_structure(this, out);
  }
  out << "}";
}

inline void Chain::Block::PrepareForAppend() {
  if (is_internal() && has_unique_owner() && empty()) {
    data_ = absl::string_view(allocated_begin_, 0);
  }
}

inline void Chain::Block::PrepareForPrepend() {
  if (is_internal() && has_unique_owner() && empty()) {
    data_ = absl::string_view(allocated_end_, 0);
  }
}

inline bool Chain::Block::can_append(size_t length) const {
  return is_internal() && has_unique_owner() && space_after() >= length;
}

inline bool Chain::Block::can_prepend(size_t length) const {
  return is_internal() && has_unique_owner() && space_before() >= length;
}

inline size_t Chain::Block::max_can_append() const {
  return is_internal() && has_unique_owner() ? space_after() : size_t{0};
}

inline size_t Chain::Block::max_can_prepend() const {
  return is_internal() && has_unique_owner() ? space_before() : size_t{0};
}

inline absl::Span<char> Chain::Block::AppendBuffer(size_t max_length) {
  RIEGELI_ASSERT(can_append(0))
      << "Failed precondition of Chain::Block::AppendBuffer(): "
         "block is immutable";
  const size_t length = UnsignedMin(space_after(), max_length);
  const absl::Span<char> buffer(const_cast<char*>(data_end()), length);
  data_ = absl::string_view(
      data_begin(), PtrDistance(data_begin(), buffer.data() + buffer.size()));
  return buffer;
}

inline absl::Span<char> Chain::Block::PrependBuffer(size_t max_length) {
  RIEGELI_ASSERT(can_prepend(0))
      << "Failed precondition of Chain::Block::PrependBuffer(): "
         "block is immutable";
  const size_t length = UnsignedMin(space_before(), max_length);
  const absl::Span<char> buffer(const_cast<char*>(data_begin()) - length,
                                length);
  data_ =
      absl::string_view(buffer.data(), PtrDistance(buffer.data(), data_end()));
  return buffer;
}

inline void Chain::Block::Append(absl::string_view src) {
  return AppendWithExplicitSizeToCopy(src, src.size());
}

inline void Chain::Block::AppendWithExplicitSizeToCopy(absl::string_view src,
                                                       size_t size_to_copy) {
  RIEGELI_ASSERT_GE(size_to_copy, src.size())
      << "Failed precondition of Chain::Block::AppendWithExplicitSizeToCopy(): "
         "size to copy too small";
  RIEGELI_ASSERT(can_append(size_to_copy))
      << "Failed precondition of Chain::Block::AppendWithExplicitSizeToCopy(): "
         "not enough space";
  std::memcpy(const_cast<char*>(data_end()), src.data(), size_to_copy);
  data_ = absl::string_view(data_begin(), size() + src.size());
}

inline void Chain::Block::Prepend(absl::string_view src) {
  RIEGELI_ASSERT(can_prepend(src.size()))
      << "Failed precondition of Chain::Block::Prepend(): "
         "not enough space";
  std::memcpy(const_cast<char*>(data_begin() - src.size()), src.data(),
              src.size());
  data_ = absl::string_view(data_begin() - src.size(), size() + src.size());
}

inline void Chain::Block::AppendTo(Chain* dest, size_t size_hint) {
  RIEGELI_CHECK_LE(size(), std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::Block::AppendTo(Chain*): "
         "Chain size overflow";
  dest->AppendBlock(this, size_hint);
}

inline void Chain::Block::AppendSubstrTo(absl::string_view substr, Chain* dest,
                                         size_t size_hint) {
  RIEGELI_ASSERT(std::greater_equal<const char*>()(substr.data(), data_begin()))
      << "Failed precondition of Chain::Block::AppendSubstrTo(Chain*): "
         "substring not contained in data";
  RIEGELI_ASSERT(
      std::less_equal<const char*>()(substr.data() + substr.size(), data_end()))
      << "Failed precondition of Chain::Block::AppendSubstrTo(Chain*): "
         "substring not contained in data";
  RIEGELI_CHECK_LE(substr.size(),
                   std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Chain::Block::AppendSubstrTo(Chain*): "
         "Chain size overflow";
  if (substr.size() == size()) {
    dest->AppendBlock(this, size_hint);
    return;
  }
  if (substr.size() <= kMaxBytesToCopy) {
    dest->Append(substr, size_hint);
    return;
  }
  dest->AppendExternal(BlockRef(this, true), substr, size_hint);
}

Chain::Block* const Chain::BlockIterator::kShortData[1] = {nullptr};

Chain::PinnedBlock Chain::BlockIterator::Pin() {
  RIEGELI_ASSERT(ptr_ != kEndShortData())
      << "Failed precondition of Chain::BlockIterator::Pin(): "
         "iterator is end()";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData())) {
    Block* const block = Block::NewInternal(kMaxShortDataSize);
    block->AppendWithExplicitSizeToCopy(chain_->short_data(),
                                        kMaxShortDataSize);
    return {block->data(), block};
  } else {
    return {(*ptr_)->data(), (*ptr_)->Ref()};
  }
}

void Chain::PinnedBlock::Unpin(void* token) {
  static_cast<Block*>(token)->Unref();
}

void Chain::BlockIterator::AppendTo(Chain* dest, size_t size_hint) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData())
      << "Failed precondition of Chain::BlockIterator::AppendTo(): "
         "iterator is end()";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData())) {
    dest->Append(chain_->short_data(), size_hint);
  } else {
    (*ptr_)->AppendTo(dest, size_hint);
  }
}

void Chain::BlockIterator::AppendSubstrTo(absl::string_view substr, Chain* dest,
                                          size_t size_hint) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData())
      << "Failed precondition of Chain::BlockIterator::AppendSubstrTo(): "
         "iterator is end()";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData())) {
    dest->Append(substr, size_hint);
  } else {
    (*ptr_)->AppendSubstrTo(substr, dest, size_hint);
  }
}

constexpr size_t Chain::kMaxShortDataSize;

// In converting constructors below, size_hint being src.size() optimizes for
// the case when the resulting Chain will not be appended to further, reducing
// the size of allocations.

Chain::Chain(absl::string_view src) { Append(src, src.size()); }

Chain::Chain(std::string&& src) { Append(std::move(src), src.size()); }

Chain::Chain(const Chain& that) : size_(that.size_) {
  if (that.begin_ == that.end_) {
    std::memcpy(block_ptrs_.short_data, that.block_ptrs_.short_data,
                kMaxShortDataSize);
  } else {
    RefAndAppendBlocks(that.begin_, that.end_);
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
      RefAndAppendBlocks(that.begin_, that.end_);
    }
    size_ = that.size_;
  }
  return *this;
}

void Chain::Clear() {
  if (begin_ != end_) {
    Block** const new_end = begin_ + ((*begin_)->TryClear() ? 1 : 0);
    UnrefBlocks(new_end, end_);
    end_ = new_end;
  }
  size_ = 0;
}

inline Chain::Block** Chain::NewBlockPtrs(size_t capacity) {
  return std::allocator<Block*>().allocate(capacity);
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

void Chain::UnrefBlocksSlow(Block* const* begin, Block* const* end) {
  RIEGELI_ASSERT(begin < end)
      << "Failed invariant of Chain::UnrefBlocksSlow(): "
         "no blocks, use UnrefBlocks() instead";
  do {
    (*begin++)->Unref();
  } while (begin != end);
}

void Chain::CopyTo(char* dest) const {
  if (empty()) return;  // memcpy(nullptr, _, 0) is undefined.
  Block* const* iter = begin_;
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

void Chain::AppendTo(std::string* dest) const {
  const size_t size_before = dest->size();
  RIEGELI_CHECK_LE(size_, dest->max_size() - size_before)
      << "Failed precondition of Chain::AppendTo(string*): "
         "string size overflow";
  dest->resize(size_before + size_);
  CopyTo(&(*dest)[size_before]);
}

Chain::operator std::string() const& {
  std::string dest;
  AppendTo(&dest);
  return dest;
}

Chain::operator std::string() && {
  if (end_ - begin_ == 1) {
    Block* const block = front();
    if (StringRef* const string_ref =
            block->checked_external_object_with_unique_owner<StringRef>()) {
      RIEGELI_ASSERT_EQ(block->size(), string_ref->data().size())
          << "Failed invariant of Chain::Block: "
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

size_t Chain::EstimateMemory() const {
  MemoryEstimator memory_estimator;
  memory_estimator.RegisterMemory(sizeof(Chain));
  RegisterSubobjects(&memory_estimator);
  return memory_estimator.TotalMemory();
}

void Chain::RegisterSubobjects(MemoryEstimator* memory_estimator) const {
  if (has_allocated()) {
    memory_estimator->RegisterMemory(
        sizeof(Block*) *
        PtrDistance(block_ptrs_.allocated.begin, block_ptrs_.allocated.end));
  }
  for (Block* const* iter = begin_; iter != end_; ++iter) {
    (*iter)->RegisterShared(memory_estimator);
  }
}

void Chain::DumpStructure(std::ostream& out) const {
  out << "Chain {\n"
         "  size: "
      << size_ << "; memory: " << EstimateMemory()
      << "; short_size: " << (begin_ == end_ ? size_ : size_t{0})
      << "; blocks: " << PtrDistance(begin_, end_) << ";\n";
  size_t pos = 0;
  for (Block* const* iter = begin_; iter != end_; ++iter) {
    out << "  pos: " << pos << "; ";
    (*iter)->DumpStructure(out);
    out << "\n";
    pos += (*iter)->size();
  }
  out << "}\n";
}

inline void Chain::PushBack(Block* block) {
  ReserveBack(1);
  *end_++ = block;
}

inline void Chain::PushFront(Block* block) {
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
    // Shift the remaining 0 or 1 block pointers to the left by 1 because begin_
    // must remain at block_ptrs_.here. Use memcpy() instead of assignment
    // because the pointer being copied might be invalid if there are 0 block
    // pointers; it is cheaper to copy unconditionally.
    std::memcpy(block_ptrs_.here, block_ptrs_.here + 1, sizeof(Block*));
    --end_;
  } else {
    ++begin_;
  }
}

inline void Chain::RefAndAppendBlocks(Block* const* begin, Block* const* end) {
  ReserveBack(PtrDistance(begin, end));
  Block** dest_iter = end_;
  while (begin != end) *dest_iter++ = (*begin++)->Ref();
  end_ = dest_iter;
}

inline void Chain::RefAndPrependBlocks(Block* const* begin, Block* const* end) {
  ReserveFront(PtrDistance(begin, end));
  Block** dest_iter = begin_;
  while (end != begin) *--dest_iter = (*--end)->Ref();
  begin_ = dest_iter;
}

inline void Chain::AppendBlocks(Block* const* begin, Block* const* end) {
  ReserveBack(PtrDistance(begin, end));
  Block** dest_iter = end_;
  while (begin != end) *dest_iter++ = *begin++;
  end_ = dest_iter;
}

inline void Chain::PrependBlocks(Block* const* begin, Block* const* end) {
  ReserveFront(PtrDistance(begin, end));
  Block** dest_iter = begin_;
  while (end != begin) *--dest_iter = *--end;
  begin_ = dest_iter;
}

inline void Chain::ReserveBack(size_t extra_capacity) {
  Block** const allocated_end =
      has_here() ? block_ptrs_.here + 2 : block_ptrs_.allocated.end;
  if (ABSL_PREDICT_FALSE(extra_capacity > PtrDistance(end_, allocated_end))) {
    // The slow path is in a separate function to make easier for the compiler
    // to make good inlining decisions.
    ReserveBackSlow(extra_capacity);
  }
}

inline void Chain::ReserveFront(size_t extra_capacity) {
  Block** const allocated_begin =
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
  Block** old_allocated_begin;
  Block** old_allocated_end;
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
                    std::numeric_limits<size_t>::max() / sizeof(Block*) -
                        PtrDistance(old_allocated_begin, end_))
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t old_capacity = old_allocated_end - old_allocated_begin;
  const size_t final_size = PtrDistance(begin_, end_) + extra_capacity;
  if (final_size * 2 <= old_capacity) {
    // Existing array has at least twice more space than necessary: move
    // contents to the middle of the array, which keeps the amortized cost of
    // adding one element constant.
    Block** const new_begin =
        old_allocated_begin + (old_capacity - final_size) / 2;
    Block** const new_end = new_begin + (end_ - begin_);
    std::memmove(new_begin, begin_, sizeof(Block*) * (end_ - begin_));
    begin_ = new_begin;
    end_ = new_end;
    return;
  }
  // Reallocate the array, keeping space before the contents unchanged.
  RIEGELI_ASSERT_LE(
      old_capacity / 2,
      std::numeric_limits<size_t>::max() / sizeof(Block*) - old_capacity)
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t new_capacity =
      UnsignedMax(PtrDistance(old_allocated_begin, end_) + extra_capacity,
                  old_capacity + old_capacity / 2, size_t{16});
  Block** const new_allocated_begin = NewBlockPtrs(new_capacity);
  Block** const new_allocated_end = new_allocated_begin + new_capacity;
  Block** const new_begin =
      new_allocated_begin + (begin_ - old_allocated_begin);
  Block** const new_end = new_begin + (end_ - begin_);
  std::memcpy(new_begin, begin_, sizeof(Block*) * PtrDistance(begin_, end_));
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
  Block** old_allocated_begin;
  Block** old_allocated_end;
  if (has_here()) {
    if (ABSL_PREDICT_TRUE(extra_capacity <=
                          PtrDistance(end_, block_ptrs_.here + 2))) {
      // There is space without reallocation. Shift 1 block pointer to the right
      // by 1, or 0 block pointers by 1 or 2, because begin_ must remain at
      // block_ptrs_.here. Use memcpy() instead of assignment because the
      // pointer being copied might be invalid if there are 0 block pointers;
      // it is cheaper to copy unconditionally.
      std::memcpy(block_ptrs_.here + 1, block_ptrs_.here, sizeof(Block*));
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
                    std::numeric_limits<size_t>::max() / sizeof(Block*) -
                        PtrDistance(begin_, old_allocated_end))
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t old_capacity = old_allocated_end - old_allocated_begin;
  const size_t final_size = PtrDistance(begin_, end_) + extra_capacity;
  if (final_size * 2 <= old_capacity) {
    // Existing array has at least twice more space than necessary: move
    // contents to the middle of the array, which keeps the amortized cost of
    // adding one element constant.
    Block** const new_end = old_allocated_end - (old_capacity - final_size) / 2;
    Block** const new_begin = new_end - (end_ - begin_);
    std::memmove(new_begin, begin_, sizeof(Block*) * (end_ - begin_));
    begin_ = new_begin;
    end_ = new_end;
    return;
  }
  // Reallocate the array, keeping space after the contents unchanged.
  RIEGELI_ASSERT_LE(
      old_capacity / 2,
      std::numeric_limits<size_t>::max() / sizeof(Block*) - old_capacity)
      << "Failed invariant of Chain: array of block pointers overflow, "
         "possibly blocks are too small";
  const size_t new_capacity =
      UnsignedMax(PtrDistance(begin_, old_allocated_end) + extra_capacity,
                  old_capacity + old_capacity / 2, size_t{16});
  Block** const new_allocated_begin = NewBlockPtrs(new_capacity);
  Block** const new_allocated_end = new_allocated_begin + new_capacity;
  Block** const new_end = new_allocated_end - (old_allocated_end - end_);
  Block** const new_begin = new_end - (end_ - begin_);
  std::memcpy(new_begin, begin_, sizeof(Block*) * PtrDistance(begin_, end_));
  DeleteBlockPtrs();
  block_ptrs_.allocated.begin = new_allocated_begin;
  block_ptrs_.allocated.end = new_allocated_end;
  begin_ = new_begin;
  end_ = new_end;
}

inline size_t Chain::NewBlockCapacity(size_t replaced_length, size_t min_length,
                                      size_t recommended_length,
                                      size_t size_hint) const {
  RIEGELI_ASSERT_LE(replaced_length, size_)
      << "Failed precondition of Chain::NewBlockCapacity(): "
         "length to replace greater than current size";
  RIEGELI_CHECK_LE(min_length, Block::kMaxCapacity - replaced_length)
      << "Chain block capacity overflow";
  const size_t size_before = size_ - replaced_length;
  return UnsignedMax(
      replaced_length + min_length,
      UnsignedMin(
          size_before < size_hint
              ? size_hint - size_before
              : UnsignedMax(kMinBufferSize,
                            SaturatingAdd(replaced_length, recommended_length),
                            size_before),
          kMaxBufferSize));
}

absl::Span<char> Chain::AppendBuffer(size_t min_length,
                                     size_t recommended_length,
                                     size_t size_hint) {
  RIEGELI_CHECK_LE(min_length, std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::AppendBuffer(): "
         "Chain size overflow";
  Block* block;
  if (begin_ == end_) {
    RIEGELI_ASSERT_LE(size_, kMaxShortDataSize)
        << "Failed invariant of Chain: short data size too large";
    // Do not bother returning short data if recommended_length or size_hint is
    // larger, because data will likely need to be copied later to a real block.
    if (min_length == 0 || (min_length <= kMaxShortDataSize - size_ &&
                            recommended_length <= kMaxShortDataSize - size_ &&
                            size_hint <= kMaxShortDataSize)) {
      // Append the new space to short data.
      EnsureHasHere();
      const absl::Span<char> buffer(block_ptrs_.short_data + size_,
                                    kMaxShortDataSize - size_);
      size_ = kMaxShortDataSize;
      return buffer;
    }
    // Merge short data with the new space to a new block.
    if (ABSL_PREDICT_FALSE(min_length > Block::kMaxCapacity - size_)) {
      block = Block::NewInternal(kMaxShortDataSize);
      block->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushBack(block);
      block = Block::NewInternal(
          NewBlockCapacity(0, min_length, recommended_length, size_hint));
    } else {
      block = Block::NewInternal(NewBlockCapacity(
          size_, UnsignedMax(min_length, kMaxShortDataSize - size_),
          recommended_length, size_hint));
      block->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
    }
    PushBack(block);
  } else {
    Block* const last = back();
    last->PrepareForAppend();
    if (last->can_append(min_length)) {
      // New space can be appended in place.
      block = last;
    } else if (min_length == 0) {
      return absl::Span<char>();
    } else if (last->tiny() || last->wasteful()) {
      // The last block must be rewritten. Merge it with the new space to a
      // new block.
      if (ABSL_PREDICT_FALSE(min_length > Block::kMaxCapacity - last->size())) {
        back() = last->CopyAndUnref();
        goto new_block;
      }
      block = Block::NewInternal(NewBlockCapacity(
          last->size(), min_length, recommended_length, size_hint));
      block->Append(last->data());
      last->Unref();
      back() = block;
    } else {
    new_block:
      // Append a new block.
      block = Block::NewInternal(
          NewBlockCapacity(0, min_length, recommended_length, size_hint));
      PushBack(block);
    }
  }
  const absl::Span<char> buffer =
      block->AppendBuffer(std::numeric_limits<size_t>::max() - size_);
  RIEGELI_ASSERT_GE(buffer.size(), min_length)
      << "Chain::Block::AppendBuffer() returned less than the free space";
  size_ += buffer.size();
  return buffer;
}

absl::Span<char> Chain::PrependBuffer(size_t min_length,
                                      size_t recommended_length,
                                      size_t size_hint) {
  RIEGELI_CHECK_LE(min_length, std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::PrependBuffer(): "
         "Chain size overflow";
  Block* block;
  if (begin_ == end_) {
    RIEGELI_ASSERT_LE(size_, kMaxShortDataSize)
        << "Failed invariant of Chain: short data size too large";
    // Do not bother returning short data if recommended_length or size_hint is
    // larger, because data will likely need to be copied later to a real block.
    if (min_length == 0 || (min_length <= kMaxShortDataSize - size_ &&
                            recommended_length <= kMaxShortDataSize - size_ &&
                            size_hint <= kMaxShortDataSize)) {
      // Prepend the new space to short data.
      EnsureHasHere();
      const absl::Span<char> buffer(block_ptrs_.short_data,
                                    kMaxShortDataSize - size_);
      std::memmove(buffer.data() + buffer.size(), block_ptrs_.short_data,
                   size_);
      size_ = kMaxShortDataSize;
      return buffer;
    }
    // Merge short data with the new space to a new block.
    if (ABSL_PREDICT_FALSE(min_length > Block::kMaxCapacity - size_)) {
      block = Block::NewInternal(kMaxShortDataSize);
      block->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushFront(block);
      block = Block::NewInternalForPrepend(
          NewBlockCapacity(0, min_length, recommended_length, size_hint));
    } else {
      block = Block::NewInternalForPrepend(
          NewBlockCapacity(size_, min_length, recommended_length, size_hint));
      block->Prepend(short_data());
    }
    PushFront(block);
  } else {
    Block* const first = front();
    first->PrepareForPrepend();
    if (first->can_prepend(min_length)) {
      // New space can be prepended in place.
      block = first;
    } else if (min_length == 0) {
      return absl::Span<char>();
    } else if (first->tiny() || first->wasteful()) {
      // The first block must be rewritten. Merge it with the new space to a
      // new block.
      if (ABSL_PREDICT_FALSE(min_length >
                             Block::kMaxCapacity - first->size())) {
        front() = first->CopyAndUnref();
        goto new_block;
      }
      block = Block::NewInternalForPrepend(NewBlockCapacity(
          first->size(), min_length, recommended_length, size_hint));
      block->Prepend(first->data());
      first->Unref();
      front() = block;
    } else {
    new_block:
      // Prepend a new block.
      block = Block::NewInternalForPrepend(
          NewBlockCapacity(0, min_length, recommended_length, size_hint));
      PushFront(block);
    }
  }
  const absl::Span<char> buffer =
      block->PrependBuffer(std::numeric_limits<size_t>::max() - size_);
  RIEGELI_ASSERT_GE(buffer.size(), min_length)
      << "Chain::Block::PrependBuffer() returned less than the free space";
  size_ += buffer.size();
  return buffer;
}

void Chain::Append(absl::string_view src, size_t size_hint) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(string_view): "
         "Chain size overflow";
  if (src.empty()) return;
  for (;;) {
    const absl::Span<char> buffer = AppendBuffer(1, src.size(), size_hint);
    if (src.size() <= buffer.size()) {
      std::memcpy(buffer.data(), src.data(), src.size());
      RemoveSuffix(buffer.size() - src.size());
      return;
    }
    std::memcpy(buffer.data(), src.data(), buffer.size());
    src.remove_prefix(buffer.size());
  }
}

void Chain::Append(std::string&& src, size_t size_hint) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(string&&): "
         "Chain size overflow";
  AppendExternal(StringRef(std::move(src)), size_hint);
}

void Chain::Append(const Chain& src, size_t size_hint) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(Chain): "
         "Chain size overflow";
  if (src.begin_ == src.end_) {
    Append(src.short_data(), size_hint);
    return;
  }
  Block* const* src_iter = src.begin_;
  // If the first block of src is handled specially, ++src_iter skips it so that
  // RefAndAppendBlocks() does not append it again.
  Block* const src_first = src.front();
  if (begin_ == end_) {
    if (src_first->tiny() ||
        (src.end_ - src.begin_ > 1 && src_first->wasteful())) {
      // The first block of src must be rewritten. Merge short data with it to a
      // new block.
      if (!short_data().empty() || !src_first->empty()) {
        RIEGELI_ASSERT_LE(src_first->size(), Block::kMaxCapacity - size_)
            << "Sum of sizes of short data and a tiny or wasteful block "
               "exceeds Block::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(
                      size_,
                      UnsignedMax(src_first->size(), kMaxShortDataSize - size_),
                      0, size_hint)
                : UnsignedMax(size_ + src_first->size(), kMaxShortDataSize);
        Block* const merged = Block::NewInternal(capacity);
        merged->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
        merged->Append(src_first->data());
        PushBack(merged);
      }
      ++src_iter;
    } else if (!empty()) {
      // Copy short data to a real block.
      Block* const real = Block::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushBack(real);
    }
  } else {
    Block* const last = back();
    last->PrepareForAppend();
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
        last->Append(src_first->data());
      } else {
        // Boundary blocks cannot be appended in place. Merge them to a new
        // block.
        RIEGELI_ASSERT_LE(src_first->size(), Block::kMaxCapacity - last->size())
            << "Sum of sizes of two tiny or wasteful blocks exceeds "
               "Block::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(last->size(), src_first->size(), 0,
                                   size_hint)
                : last->size() + src_first->size();
        Block* const merged = Block::NewInternal(capacity);
        merged->Append(last->data());
        merged->Append(src_first->data());
        last->Unref();
        back() = merged;
      }
      ++src_iter;
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
        last->Append(src_first->data());
        ++src_iter;
      } else {
        // Appending in place is not possible, or rewriting the last block is
        // cheaper.
        back() = last->CopyAndUnref();
      }
    } else if (src.end_ - src.begin_ > 1) {
      if (src_first->empty()) {
        // The first block of src is empty and must be skipped.
        ++src_iter;
      } else if (src_first->wasteful()) {
        // The first block of src must reduce waste.
        if (last->can_append(src_first->size()) &&
            !last->wasteful(src_first->size())) {
          // Appending in place is possible; this is always cheaper than
          // rewriting the first block of src.
          last->Append(src_first->data());
        } else {
          // Appending in place is not possible.
          PushBack(src_first->Copy());
        }
        ++src_iter;
      }
    }
  }
  RefAndAppendBlocks(src_iter, src.end_);
  size_ += src.size_;
}

void Chain::Append(Chain&& src, size_t size_hint) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Append(Chain&&): "
         "Chain size overflow";
  if (src.begin_ == src.end_) {
    Append(src.short_data(), size_hint);
    return;
  }
  Block* const* src_iter = src.begin_;
  // If the first block of src is handled specially, (*src_iter++)->Unref()
  // skips it so that AppendBlocks() does not append it again.
  Block* const src_first = src.front();
  if (begin_ == end_) {
    if (src_first->tiny() ||
        (src.end_ - src.begin_ > 1 && src_first->wasteful())) {
      // The first block of src must be rewritten. Merge short data with it to a
      // new block.
      if (!short_data().empty() || !src_first->empty()) {
        RIEGELI_ASSERT_LE(src_first->size(), Block::kMaxCapacity - size_)
            << "Sum of sizes of short data and a tiny or wasteful block "
               "exceeds Block::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(
                      size_,
                      UnsignedMax(src_first->size(), kMaxShortDataSize - size_),
                      0, size_hint)
                : UnsignedMax(size_ + src_first->size(), kMaxShortDataSize);
        Block* const merged = Block::NewInternal(capacity);
        merged->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
        merged->Append(src_first->data());
        PushBack(merged);
      }
      (*src_iter++)->Unref();
    } else if (!empty()) {
      // Copy short data to a real block.
      Block* const real = Block::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushBack(real);
    }
  } else {
    Block* const last = back();
    last->PrepareForAppend();
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
        last->Append(src_first->data());
      } else {
        // Boundary blocks cannot be appended in place. Merge them to a new
        // block.
        RIEGELI_ASSERT_LE(src_first->size(), Block::kMaxCapacity - last->size())
            << "Sum of sizes of two tiny or wasteful blocks exceeds "
               "Block::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(last->size(), src_first->size(), 0,
                                   size_hint)
                : last->size() + src_first->size();
        Block* const merged = Block::NewInternal(capacity);
        merged->Append(last->data());
        merged->Append(src_first->data());
        last->Unref();
        back() = merged;
      }
      (*src_iter++)->Unref();
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
        last->Append(src_first->data());
        (*src_iter++)->Unref();
      } else {
        // Appending in place is not possible, or rewriting the last block is
        // cheaper.
        back() = last->CopyAndUnref();
      }
    } else if (src.end_ - src.begin_ > 1) {
      if (src_first->empty()) {
        // The first block of src is empty and must be skipped.
        (*src_iter++)->Unref();
      } else if (src_first->wasteful()) {
        // The first block of src must reduce waste.
        if (last->can_append(src_first->size()) &&
            !last->wasteful(src_first->size())) {
          // Appending in place is possible; this is always cheaper than
          // rewriting the first block of src.
          last->Append(src_first->data());
        } else {
          // Appending in place is not possible.
          PushBack(src_first->Copy());
        }
        (*src_iter++)->Unref();
      }
    }
  }
  AppendBlocks(src_iter, src.end_);
  size_ += src.size_;
  src.end_ = src.begin_;
  src.size_ = 0;
}

void Chain::Prepend(absl::string_view src, size_t size_hint) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(string_view): "
         "Chain size overflow";
  if (src.empty()) return;
  for (;;) {
    const absl::Span<char> buffer = PrependBuffer(1, src.size(), size_hint);
    if (src.size() <= buffer.size()) {
      std::memcpy(buffer.data() + (buffer.size() - src.size()), src.data(),
                  src.size());
      RemovePrefix(buffer.size() - src.size());
      return;
    }
    std::memcpy(buffer.data(), src.data() + (src.size() - buffer.size()),
                buffer.size());
    src.remove_suffix(buffer.size());
  }
}

void Chain::Prepend(std::string&& src, size_t size_hint) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(string&&): "
         "Chain size overflow";
  PrependExternal(StringRef(std::move(src)), size_hint);
}

void Chain::Prepend(const Chain& src, size_t size_hint) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(Chain): "
         "Chain size overflow";
  if (src.begin_ == src.end_) {
    Prepend(src.short_data(), size_hint);
    return;
  }
  Block* const* src_iter = src.end_;
  // If the last block of src is handled specially, --src_iter skips it so that
  // RefAndPrependBlocks() does not prepend it again.
  Block* const src_last = src.back();
  if (begin_ == end_) {
    if (src_last->tiny() ||
        (src.end_ - src.begin_ > 1 && src_last->wasteful())) {
      // The last block of src must be rewritten. Merge short data with it to a
      // new block.
      if (!short_data().empty() || !src_last->empty()) {
        RIEGELI_ASSERT_LE(src_last->size(), Block::kMaxCapacity - size_)
            << "Sum of sizes of short data and a tiny or wasteful block "
               "exceeds Block::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(size_, src_last->size(), 0, size_hint)
                : size_ + src_last->size();
        Block* const merged = Block::NewInternalForPrepend(capacity);
        merged->Prepend(short_data());
        merged->Prepend(src_last->data());
        PushFront(merged);
      }
      --src_iter;
    } else if (!empty()) {
      // Copy short data to a real block.
      Block* const real = Block::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushFront(real);
    }
  } else {
    Block* const first = front();
    first->PrepareForPrepend();
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
        first->Prepend(src_last->data());
      } else {
        // Boundary blocks cannot be prepended in place. Merge them to a new
        // block.
        RIEGELI_ASSERT_LE(src_last->size(), Block::kMaxCapacity - first->size())
            << "Sum of sizes of two tiny or wasteful blocks exceeds "
               "Block::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(first->size(), src_last->size(), 0,
                                   size_hint)
                : first->size() + src_last->size();
        Block* const merged = Block::NewInternalForPrepend(capacity);
        merged->Prepend(first->data());
        merged->Prepend(src_last->data());
        first->Unref();
        front() = merged;
      }
      --src_iter;
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
        first->Prepend(src_last->data());
        --src_iter;
      } else {
        // Prepending in place is not possible, or rewriting the first block is
        // cheaper.
        front() = first->CopyAndUnref();
      }
    } else if (src.end_ - src.begin_ > 1) {
      if (src_last->empty()) {
        // The last block of src is empty and must be skipped.
        --src_iter;
      } else if (src_last->wasteful()) {
        // The last block of src must reduce waste.
        if (first->can_prepend(src_last->size()) &&
            !first->wasteful(src_last->size())) {
          // Prepending in place is possible; this is always cheaper than
          // rewriting the last block of src.
          first->Prepend(src_last->data());
        } else {
          // Prepending in place is not possible.
          PushFront(src_last->Copy());
        }
        --src_iter;
      }
    }
  }
  RefAndPrependBlocks(src.begin_, src_iter);
  size_ += src.size_;
}

void Chain::Prepend(Chain&& src, size_t size_hint) {
  RIEGELI_CHECK_LE(src.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::Prepend(Chain&&): "
         "Chain size overflow";
  if (src.begin_ == src.end_) {
    Prepend(src.short_data(), size_hint);
    return;
  }
  Block* const* src_iter = src.end_;
  // If the last block of src is handled specially, (*--src_iter)->Unref()
  // skips it so that PrependBlocks() does not prepend it again.
  Block* const src_last = src.back();
  if (begin_ == end_) {
    if (src_last->tiny() ||
        (src.end_ - src.begin_ > 1 && src_last->wasteful())) {
      // The last block of src must be rewritten. Merge short data with it to a
      // new block.
      if (!short_data().empty() || !src_last->empty()) {
        RIEGELI_ASSERT_LE(src_last->size(), Block::kMaxCapacity - size_)
            << "Sum of sizes of short data and a tiny or wasteful block "
               "exceeds Block::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(size_, src_last->size(), 0, size_hint)
                : size_ + src_last->size();
        Block* const merged = Block::NewInternalForPrepend(capacity);
        merged->Prepend(short_data());
        merged->Prepend(src_last->data());
        PushFront(merged);
      }
      (*--src_iter)->Unref();
    } else if (!empty()) {
      // Copy short data to a real block.
      Block* const real = Block::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushFront(real);
    }
  } else {
    Block* const first = front();
    first->PrepareForPrepend();
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
        first->Prepend(src_last->data());
      } else {
        // Boundary blocks cannot be prepended in place. Merge them to a new
        // block.
        RIEGELI_ASSERT_LE(src_last->size(), Block::kMaxCapacity - first->size())
            << "Sum of sizes of two tiny or wasteful blocks exceeds "
               "Block::kMaxCapacity";
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(first->size(), src_last->size(), 0,
                                   size_hint)
                : first->size() + src_last->size();
        Block* const merged = Block::NewInternalForPrepend(capacity);
        merged->Prepend(first->data());
        merged->Prepend(src_last->data());
        first->Unref();
        front() = merged;
      }
      (*--src_iter)->Unref();
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
        first->Prepend(src_last->data());
        (*--src_iter)->Unref();
      } else {
        // Prepending in place is not possible, or rewriting the first block is
        // cheaper.
        front() = first->CopyAndUnref();
      }
    } else if (src.end_ - src.begin_ > 1) {
      if (src_last->empty()) {
        // The last block of src is empty and must be skipped.
        (*--src_iter)->Unref();
      } else if (src_last->wasteful()) {
        // The last block of src must reduce waste.
        if (first->can_prepend(src_last->size()) &&
            !first->wasteful(src_last->size())) {
          // Prepending in place is possible; this is always cheaper than
          // rewriting the last block of src.
          first->Prepend(src_last->data());
        } else {
          // Prepending in place is not possible.
          PushFront(src_last->Copy());
        }
        (*--src_iter)->Unref();
      }
    }
  }
  PrependBlocks(src.begin_, src_iter);
  size_ += src.size_;
  src.end_ = src.begin_;
  src.size_ = 0;
}

inline void Chain::AppendBlock(Block* block, size_t size_hint) {
  RIEGELI_ASSERT_LE(block->size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::AppendBlock(): "
         "Chain size overflow";
  if (block->empty()) return;
  if (begin_ == end_) {
    if (block->tiny()) {
      // The block must be rewritten. Merge short data with it to a new block.
      RIEGELI_ASSERT_LE(block->size(), Block::kMaxCapacity - size_)
          << "Sum of sizes of short data and a tiny block exceeds "
             "Block::kMaxCapacity";
      const size_t capacity = NewBlockCapacity(
          size_, UnsignedMax(block->size(), kMaxShortDataSize - size_), 0,
          size_hint);
      Block* const merged = Block::NewInternal(capacity);
      merged->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      merged->Append(block->data());
      PushBack(merged);
      size_ += block->size();
      return;
    }
    if (!empty()) {
      // Copy short data to a real block.
      Block* const real = Block::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushBack(real);
    }
  } else {
    Block* const last = back();
    last->PrepareForAppend();
    if (last->tiny() && block->tiny()) {
      // Boundary blocks must be merged.
      if (last->can_append(block->size())) {
        // Boundary blocks can be appended in place; this is always cheaper than
        // merging them to a new block.
        last->Append(block->data());
      } else {
        // Boundary blocks cannot be appended in place. Merge them to a new
        // block.
        RIEGELI_ASSERT_LE(block->size(), Block::kMaxCapacity - last->size())
            << "Sum of sizes of two tiny blocks exceeds Block::kMaxCapacity";
        Block* const merged = Block::NewInternal(
            NewBlockCapacity(last->size(), block->size(), 0, size_hint));
        merged->Append(last->data());
        merged->Append(block->data());
        last->Unref();
        back() = merged;
      }
      size_ += block->size();
      return;
    }
    if (last->empty()) {
      // The last block is empty and must be removed.
      last->Unref();
      back() = block->Ref();
      size_ += block->size();
      return;
    }
    if (last->wasteful()) {
      // The last block must reduce waste.
      if (last->can_append(block->size()) &&
          block->size() <= kAllocationCost + last->size()) {
        // Appending in place is possible and is cheaper than rewriting the last
        // block.
        last->Append(block->data());
        size_ += block->size();
        return;
      }
      // Appending in place is not possible, or rewriting the last block is
      // cheaper.
      back() = last->CopyAndUnref();
    }
  }
  PushBack(block->Ref());
  size_ += block->size();
}

void Chain::RawAppendExternal(Block* (*new_block)(void*, absl::string_view),
                              void* object, absl::string_view data,
                              size_t size_hint) {
  RIEGELI_CHECK_LE(data.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::AppendExternal(): "
         "Chain size overflow";
  if (data.size() <= kMaxBytesToCopy) {
    Append(data, size_hint);
    return;
  }
  if (begin_ == end_) {
    if (!empty()) {
      // Copy short data to a real block.
      Block* const real = Block::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushBack(real);
    }
  } else {
    Block* const last = back();
    if (last->empty()) {
      // The last block is empty and must be removed.
      PopBack();
      last->Unref();
    } else if (last->wasteful()) {
      // The last block must reduce waste.
      last->PrepareForAppend();
      if (last->can_append(data.size()) &&
          data.size() <= kAllocationCost * 2 + last->size()) {
        // Appending in place is possible and is cheaper than rewriting the last
        // block and allocating an external block.
        last->Append(data);
        size_ += data.size();
        return;
      }
      // Appending in place is not possible, or rewriting the last block and
      // allocating an external block is cheaper.
      back() = last->CopyAndUnref();
    }
  }
  Block* const block = new_block(object, data);
  PushBack(block);
  size_ += block->size();
}

void Chain::RawPrependExternal(Block* (*new_block)(void*, absl::string_view),
                               void* object, absl::string_view data,
                               size_t size_hint) {
  RIEGELI_CHECK_LE(data.size(), std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of Chain::PrependExternal(): "
         "Chain size overflow";
  if (data.size() <= kMaxBytesToCopy) {
    Prepend(data, size_hint);
    return;
  }
  if (begin_ == end_) {
    if (!empty()) {
      // Copy short data to a real block.
      Block* const real = Block::NewInternal(kMaxShortDataSize);
      real->AppendWithExplicitSizeToCopy(short_data(), kMaxShortDataSize);
      PushFront(real);
    }
  } else {
    Block* const first = front();
    if (first->empty()) {
      // The first block is empty and must be removed.
      PopFront();
      first->Unref();
    } else if (first->wasteful()) {
      // The first block must reduce waste.
      first->PrepareForPrepend();
      if (first->can_prepend(data.size()) &&
          data.size() <= kAllocationCost * 2 + first->size()) {
        // Prepending in place is possible and is cheaper than rewriting the
        // first block and allocating an external block.
        first->Prepend(data);
        size_ += data.size();
        return;
      }
      // Prepending in place is not possible, or rewriting the first block and
      // allocating an external block is cheaper.
      front() = first->CopyAndUnref();
    }
  }
  Block* const block = new_block(object, data);
  PushFront(block);
  size_ += block->size();
}

void Chain::RemoveSuffixSlow(size_t length, size_t size_hint) {
  RIEGELI_ASSERT_GT(length, 0u)
      << "Failed precondition of Chain::RemoveSuffixSlow(): "
         "nothing to do, use RemoveSuffix() instead";
  RIEGELI_ASSERT(begin_ != end_)
      << "Failed precondition of Chain::RemoveSuffixSlow(): "
         "no blocks, use RemoveSuffix() instead";
  Block** iter = end_;
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
  Block* const block = *--iter;
  end_ = iter;
  if (length == block->size()) {
    block->Unref();
    return;
  }
  absl::string_view data = block->data();
  data.remove_suffix(length);
  // Compensate for increasing size_ by Append() or AppendExternal().
  size_ -= data.size();
  if (data.size() <= kMaxBytesToCopy) {
    Append(data, size_hint);
    block->Unref();
    return;
  }
  AppendExternal(BlockRef(block, false), data, size_hint);
}

void Chain::RemovePrefixSlow(size_t length, size_t size_hint) {
  RIEGELI_ASSERT_GT(length, 0u)
      << "Failed precondition of Chain::RemovePrefixSlow(): "
         "nothing to do, use RemovePrefix() instead";
  RIEGELI_ASSERT(begin_ != end_)
      << "Failed precondition of Chain::RemovePrefixSlow(): "
         "no blocks, use RemovePrefix() instead";
  Block** iter = begin_;
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
        // Shift 1 block pointer to the left by 1 because begin_ must remain at
        // block_ptrs_.here.
        block_ptrs_.here[0] = block_ptrs_.here[1];
        --end_;
      } else {
        begin_ = iter;
      }
      return;
    }
  }
  Block* const block = *iter++;
  if (has_here()) {
    // Shift 1 block pointer to the left by 1, or 0 block pointers by 1 or 2,
    // because begin_ must remain at block_ptrs_.here. Use memcpy() instead of
    // assignment because the pointer being copied might be invalid if there are
    // 0 block pointers; it is cheaper to copy unconditionally.
    std::memcpy(block_ptrs_.here, block_ptrs_.here + 1, sizeof(Block*));
    end_ -= PtrDistance(block_ptrs_.here, iter);
  } else {
    begin_ = iter;
  }
  if (length == block->size()) {
    block->Unref();
    return;
  }
  absl::string_view data = block->data();
  data.remove_prefix(length);
  // Compensate for increasing size_ by Prepend() or PrependExternal().
  size_ -= data.size();
  if (data.size() <= kMaxBytesToCopy) {
    Prepend(data, size_hint);
    block->Unref();
    return;
  }
  PrependExternal(BlockRef(block, false), data, size_hint);
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

int Chain::Compare(absl::string_view that) const {
  Chain::BlockIterator this_iter = blocks().cbegin();
  size_t this_pos = 0;
  size_t that_pos = 0;
  while (this_iter != blocks().cend()) {
    if (that_pos == that.size()) {
      do {
        if (!this_iter->empty()) return 1;
        ++this_iter;
      } while (this_iter != blocks().cend());
      return 0;
    }
    const size_t length =
        UnsignedMin(this_iter->size() - this_pos, that.size() - that_pos);
    const int result = std::memcmp(this_iter->data() + this_pos,
                                   that.data() + that_pos, length);
    if (result != 0) return result;
    this_pos += length;
    if (this_pos == this_iter->size()) {
      ++this_iter;
      this_pos = 0;
    }
    that_pos += length;
  }
  return that_pos == that.size() ? 0 : -1;
}

int Chain::Compare(const Chain& that) const {
  Chain::BlockIterator this_iter = blocks().cbegin();
  Chain::BlockIterator that_iter = that.blocks().cbegin();
  size_t this_pos = 0;
  size_t that_pos = 0;
  while (this_iter != blocks().cend()) {
    if (that_iter == that.blocks().cend()) {
      do {
        if (!this_iter->empty()) return 1;
        ++this_iter;
      } while (this_iter != blocks().cend());
      return 0;
    }
    const size_t length =
        UnsignedMin(this_iter->size() - this_pos, that_iter->size() - that_pos);
    const int result = std::memcmp(this_iter->data() + this_pos,
                                   that_iter->data() + that_pos, length);
    if (result != 0) return result;
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
    if (!that_iter->empty()) return -1;
    ++that_iter;
  }
  return 0;
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

}  // namespace riegeli
