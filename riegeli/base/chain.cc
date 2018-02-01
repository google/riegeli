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
#include <memory>
#include <new>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

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

inline Chain::Block* Chain::Block::NewInternal(size_t capacity) {
  Block* const block =
      AllocateAlignedBytes<Block>(kInternalAllocatedOffset() + capacity);
  new (block) Block(capacity, 0);
  return block;
}

inline Chain::Block* Chain::Block::NewInternalForPrepend(size_t capacity) {
  Block* const block =
      AllocateAlignedBytes<Block>(kInternalAllocatedOffset() + capacity);
  new (block) Block(capacity, capacity);
  return block;
}

inline Chain::Block::Block(size_t capacity, size_t space_before)
    : data_(string_view(allocated_begin_ + space_before, 0)),
      allocated_end_(allocated_begin_ + capacity) {
  RIEGELI_ASSERT_LE(space_before, capacity);
  RIEGELI_ASSERT(is_internal());
}

inline void Chain::Block::Unref() {
  if (has_unique_owner() ||
      ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    if (is_internal()) {
      const size_t num_bytes = kInternalAllocatedOffset() + capacity();
      this->~Block();
      FreeAlignedBytes<Block>(this, num_bytes);
    } else {
      external_.methods->delete_block(this);
    }
  }
}

inline Chain::Block* Chain::Block::Copy() {
  Block* const block = NewInternal(size());
  block->Append(data());
  RIEGELI_ASSERT(!block->wasteful());
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
  RIEGELI_ASSERT(is_internal());
  return allocated_end_ - allocated_begin_;
}

inline size_t Chain::Block::space_before() const {
  RIEGELI_ASSERT(is_internal());
  return data_begin() - allocated_begin_;
}

inline size_t Chain::Block::space_after() const {
  RIEGELI_ASSERT(is_internal());
  return allocated_end_ - data_end();
}

inline bool Chain::Block::tiny(size_t extra_size) const {
  if (is_internal()) {
    RIEGELI_ASSERT_LE(extra_size, capacity() - size());
  } else {
    RIEGELI_ASSERT_EQ(extra_size, 0u);
  }
  const size_t final_size = size() + extra_size;
  return final_size < kMinBufferSize();
}

inline bool Chain::Block::wasteful(size_t extra_size) const {
  if (is_internal()) {
    RIEGELI_ASSERT_LE(extra_size, capacity() - size());
  } else {
    RIEGELI_ASSERT_EQ(extra_size, 0u);
    return false;
  }
  const size_t final_size = size() + extra_size;
  return capacity() - final_size > UnsignedMax(final_size, kMinBufferSize());
}

inline void Chain::Block::AddUniqueTo(MemoryEstimator* memory_estimator) const {
  if (is_internal()) {
    memory_estimator->AddMemory(kInternalAllocatedOffset() + capacity());
  } else {
    external_.methods->add_unique_to(this, memory_estimator);
  }
}

inline void Chain::Block::AddSharedTo(MemoryEstimator* memory_estimator) const {
  if (memory_estimator->AddObject(this)) AddUniqueTo(memory_estimator);
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
    data_ = string_view(allocated_begin_, 0);
  }
}

inline void Chain::Block::PrepareForPrepend() {
  if (is_internal() && has_unique_owner() && empty()) {
    data_ = string_view(allocated_end_, 0);
  }
}

inline bool Chain::Block::can_append(size_t size) const {
  return is_internal() && has_unique_owner() && space_after() >= size;
}

inline bool Chain::Block::can_prepend(size_t size) const {
  return is_internal() && has_unique_owner() && space_before() >= size;
}

inline size_t Chain::Block::max_can_append() const {
  return is_internal() && has_unique_owner() ? space_after() : 0;
}

inline size_t Chain::Block::max_can_prepend() const {
  return is_internal() && has_unique_owner() ? space_before() : 0;
}

inline Chain::Buffer Chain::Block::MakeAppendBuffer() {
  RIEGELI_ASSERT(is_internal());
  RIEGELI_ASSERT(has_unique_owner());
  const Buffer buffer(const_cast<char*>(data_end()), space_after());
  data_ = string_view(data_begin(), allocated_end_ - data_begin());
  return buffer;
}

inline Chain::Buffer Chain::Block::MakePrependBuffer() {
  RIEGELI_ASSERT(is_internal());
  RIEGELI_ASSERT(has_unique_owner());
  const Buffer buffer(const_cast<char*>(allocated_begin_), space_before());
  data_ = string_view(allocated_begin_, data_end() - allocated_begin_);
  return buffer;
}

inline void Chain::Block::Append(string_view src) {
  RIEGELI_ASSERT(can_append(src.size()));
  std::memcpy(const_cast<char*>(data_end()), src.data(), src.size());
  data_ = string_view(data_begin(), size() + src.size());
}

inline void Chain::Block::Prepend(string_view src) {
  RIEGELI_ASSERT(can_prepend(src.size()));
  std::memcpy(const_cast<char*>(data_begin() - src.size()), src.data(),
              src.size());
  data_ = string_view(data_begin() - src.size(), size() + src.size());
}

class Chain::BlockRef {
 public:
  BlockRef(Block* block, bool add_ref);

  BlockRef(BlockRef&& src) noexcept;
  BlockRef& operator=(BlockRef&& src) noexcept;

  ~BlockRef();

  void AddUniqueTo(string_view data, MemoryEstimator* memory_estimator) const;
  void DumpStructure(string_view data, std::ostream& out) const;

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

inline Chain::BlockRef::BlockRef(BlockRef&& src) noexcept
    : block_(riegeli::exchange(src.block_, nullptr)) {}

inline Chain::BlockRef& Chain::BlockRef::operator=(BlockRef&& src) noexcept {
  // Exchange src.block_ early to support self-assignment.
  Block* const block = riegeli::exchange(src.block_, nullptr);
  if (block_ != nullptr) block_->Unref();
  block_ = block;
  return *this;
}

inline Chain::BlockRef::~BlockRef() {
  if (block_ != nullptr) block_->Unref();
}

inline void Chain::BlockRef::AddUniqueTo(
    string_view data, MemoryEstimator* memory_estimator) const {
  memory_estimator->AddMemory(sizeof(*this));
  block_->AddSharedTo(memory_estimator);
}

inline void Chain::BlockRef::DumpStructure(string_view data,
                                           std::ostream& out) const {
  out << "offset: " << (data.data() - block_->data_begin()) << "; ";
  block_->DumpStructure(out);
}

class Chain::StringRef {
 public:
  explicit StringRef(std::string src) : src_(std::move(src)) {}

  StringRef(StringRef&&) noexcept = default;
  // string::operator=(string&&) is not noexcept in GCC < 6.1.
  StringRef& operator=(StringRef&&) noexcept(
      std::is_nothrow_move_assignable<std::string>::value) = default;

  string_view data() const { return src_; }
  void AddUniqueTo(string_view data, MemoryEstimator* memory_estimator) const;
  void DumpStructure(string_view data, std::ostream& out) const;

 private:
  friend class Chain;

  std::string src_;
};

inline void Chain::StringRef::AddUniqueTo(
    string_view data, MemoryEstimator* memory_estimator) const {
  memory_estimator->AddMemory(sizeof(*this) + src_.capacity());
}

inline void Chain::StringRef::DumpStructure(string_view data,
                                            std::ostream& out) const {
  out << "string";
}

void Chain::BlockIterator::Unpin(void* token) {
  static_cast<Block*>(token)->Unref();
}

void Chain::BlockIterator::AppendTo(Chain* dest, size_t size_hint) const {
  dest->AppendBlock(*iter_, size_hint);
}

void Chain::BlockIterator::AppendSubstrTo(string_view substr, Chain* dest,
                                          size_t size_hint) const {
  Block* const block = *iter_;
  const string_view data = block->data();
  RIEGELI_ASSERT(substr.data() >= data.data());
  RIEGELI_ASSERT_LE(substr.size(), data.size() - (substr.data() - data.data()));
  if (substr.size() == data.size()) {
    dest->AppendBlock(block, size_hint);
    return;
  }
  if (substr.size() <= kMaxBytesToCopy()) {
    dest->Append(substr, size_hint);
    return;
  }
  dest->AppendExternal(BlockRef(block, true), substr, size_hint);
}

Chain::Chain(string_view src) {
  if (src.empty()) return;
  Block* const block = Block::NewInternal(src.size());
  block->Append(src);
  block_ptrs_.here[0] = block;
  end_ = block_ptrs_.here + 1;
  size_ = block->size();
}

Chain::Chain(std::string&& src) {
  if (src.empty()) return;
  Block* block;
  if (src.size() <= kMaxBytesToCopy()) {
    block = Block::NewInternal(src.size());
    block->Append(src);
  } else {
    block = ExternalMethodsFor<StringRef>::NewBlockImplicitData(&src, src);
  }
  block_ptrs_.here[0] = block;
  end_ = block_ptrs_.here + 1;
  size_ = block->size();
}

Chain::Chain(const Chain& src) : size_(src.size_) {
  RefAndAppendBlocks(src.begin_, src.end_);
}

Chain& Chain::operator=(const Chain& src) {
  if (&src != this) {
    UnrefBlocks();
    end_ = begin_;
    RefAndAppendBlocks(src.begin_, src.end_);
    size_ = src.size_;
  }
  return *this;
}

Chain::Chain(Chain&& src) noexcept
    : block_ptrs_(src.block_ptrs_), size_(riegeli::exchange(src.size_, 0)) {
  if (src.is_here()) {
    // src.is_here() implies that src.begin_ == src.block_ptrs_.here already.
    begin_ = block_ptrs_.here;
    end_ =
        block_ptrs_.here + (riegeli::exchange(src.end_, src.block_ptrs_.here) -
                            src.block_ptrs_.here);
  } else {
    begin_ = riegeli::exchange(src.begin_, src.block_ptrs_.here);
    end_ = riegeli::exchange(src.end_, src.block_ptrs_.here);
  }
  // It does not matter what is left in src.block_ptrs_ because src.begin_ and
  // src.end_ point to the empty prefix of src.block_ptrs_.here[].
}

Chain& Chain::operator=(Chain&& src) noexcept {
  // Exchange src.begin_ and src.end_ early to support self-assignment.
  Block** begin;
  Block** end;
  if (src.is_here()) {
    // src.is_here() implies that src.begin_ == src.block_ptrs_.here already.
    begin = block_ptrs_.here;
    end =
        block_ptrs_.here + (riegeli::exchange(src.end_, src.block_ptrs_.here) -
                            src.block_ptrs_.here);
  } else {
    begin = riegeli::exchange(src.begin_, src.block_ptrs_.here);
    end = riegeli::exchange(src.end_, src.block_ptrs_.here);
  }
  UnrefBlocks();
  DeleteBlockPtrs();
  // It does not matter what is left in src.block_ptrs_ because src.begin_ and
  // src.end_ point to the empty prefix of src.block_ptrs_.here[].
  block_ptrs_ = src.block_ptrs_;
  begin_ = begin;
  end_ = end;
  size_ = riegeli::exchange(src.size_, 0);
  return *this;
}

Chain::~Chain() {
  UnrefBlocks();
  DeleteBlockPtrs();
}

void Chain::Clear() {
  if (end_ == begin_) return;
  Block** const new_end = begin_ + ((*begin_)->TryClear() ? 1 : 0);
  UnrefBlocks(new_end, end_);
  end_ = new_end;
  size_ = 0;
}

inline Chain::Block** Chain::NewBlockPtrs(size_t capacity) {
  return std::allocator<Block*>().allocate(capacity);
}

inline void Chain::DeleteBlockPtrs() {
  if (is_allocated()) {
    std::allocator<Block*>().deallocate(
        block_ptrs_.allocated.begin,
        block_ptrs_.allocated.end - block_ptrs_.allocated.begin);
  }
}

inline void Chain::UnrefBlocks() { UnrefBlocks(begin_, end_); }

inline void Chain::UnrefBlocks(Block* const* begin, Block* const* end) {
  for (Block* const* iter = begin; iter != end; ++iter) (*iter)->Unref();
}

void Chain::CopyTo(char* dest) const {
  if (empty()) return;  // memcpy(nullptr, _, 0) is undefined.
  for (string_view fragment : blocks()) {
    std::memcpy(dest, fragment.data(), fragment.size());
    dest += fragment.size();
  }
}

void Chain::AppendTo(std::string* dest) const {
  const size_t final_size = dest->size() + size_;
  if (final_size > dest->capacity()) dest->reserve(final_size);
  for (string_view fragment : blocks()) {
    dest->append(fragment.data(), fragment.size());
  }
}

Chain::operator std::string() const& {
  std::string dest;
  AppendTo(&dest);
  return dest;
}

Chain::operator std::string() && {
  if (blocks().size() == 1) {
    Block* const block = front();
    if (StringRef* const string_ref =
            block->checked_external_object_with_unique_owner<StringRef>()) {
      RIEGELI_ASSERT_EQ(block->size(), string_ref->data().size());
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
  AddUniqueTo(&memory_estimator);
  return memory_estimator.TotalMemory();
}

void Chain::AddUniqueTo(MemoryEstimator* memory_estimator) const {
  memory_estimator->AddMemory(sizeof(Chain));
  if (is_allocated()) {
    memory_estimator->AddMemory(sizeof(Block*) * (block_ptrs_.allocated.end -
                                                  block_ptrs_.allocated.begin));
  }
  for (auto iter = blocks().cbegin(); iter != blocks().cend(); ++iter) {
    (*iter.iter_)->AddSharedTo(memory_estimator);
  }
}

void Chain::AddSharedTo(MemoryEstimator* memory_estimator) const {
  if (memory_estimator->AddObject(this)) AddUniqueTo(memory_estimator);
}

void Chain::DumpStructure(std::ostream& out) const {
  out << "Chain {\n"
         "  size: "
      << size_ << "; memory: " << EstimateMemory()
      << "; blocks:" << blocks().size() << ";\n";
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
  RIEGELI_ASSERT(end_ != begin_);
  --end_;
}

inline void Chain::PopFront() {
  RIEGELI_ASSERT(end_ != begin_);
  if (is_here()) {
    // Use memcpy() instead of assignment because the pointer being copied might
    // be invalid if end_ == block_ptrs_.here + 1. It is cheaper to copy
    // unconditionally.
    std::memcpy(block_ptrs_.here, block_ptrs_.here + 1, sizeof(Block*));
    --end_;
  } else {
    ++begin_;
  }
}

inline void Chain::RefAndAppendBlocks(Block* const* begin, Block* const* end) {
  ReserveBack(end - begin);
  Block** dest_iter = end_;
  while (begin != end) *dest_iter++ = (*begin++)->Ref();
  end_ = dest_iter;
}

inline void Chain::RefAndPrependBlocks(Block* const* begin, Block* const* end) {
  ReserveFront(end - begin);
  Block** dest_iter = begin_;
  while (end != begin) *--dest_iter = (*--end)->Ref();
  begin_ = dest_iter;
}

inline void Chain::AppendBlocks(Block* const* begin, Block* const* end) {
  ReserveBack(end - begin);
  Block** dest_iter = end_;
  while (begin != end) *dest_iter++ = *begin++;
  end_ = dest_iter;
}

inline void Chain::PrependBlocks(Block* const* begin, Block* const* end) {
  ReserveFront(end - begin);
  Block** dest_iter = begin_;
  while (end != begin) *--dest_iter = *--end;
  begin_ = dest_iter;
}

inline void Chain::ReserveBack(size_t extra_capacity) {
  Block** const allocated_end =
      is_here() ? block_ptrs_.here + 2 : block_ptrs_.allocated.end;
  RIEGELI_ASSERT(end_ <= allocated_end);
  if (RIEGELI_UNLIKELY(extra_capacity >
                       static_cast<size_t>(allocated_end - end_))) {
    // The slow path is in a separate function to make easier for the compiler
    // to make good inlining decisions.
    ReserveBackSlow(extra_capacity);
  }
}

inline void Chain::ReserveFront(size_t extra_capacity) {
  Block** const allocated_begin =
      is_here() ? block_ptrs_.here : block_ptrs_.allocated.begin;
  RIEGELI_ASSERT(begin_ >= allocated_begin);
  if (RIEGELI_UNLIKELY(extra_capacity >
                       static_cast<size_t>(begin_ - allocated_begin))) {
    // The slow path is in a separate function to make easier for the compiler
    // to make good inlining decisions.
    ReserveFrontSlow(extra_capacity);
  }
}

inline void Chain::ReserveBackSlow(size_t extra_capacity) {
  Block** old_allocated_begin;
  Block** old_allocated_end;
  if (is_here()) {
    old_allocated_begin = block_ptrs_.here;
    old_allocated_end = block_ptrs_.here + 2;
  } else {
    old_allocated_begin = block_ptrs_.allocated.begin;
    old_allocated_end = block_ptrs_.allocated.end;
  }
  RIEGELI_ASSERT(end_ <= old_allocated_end);
  RIEGELI_ASSERT_GT(extra_capacity,
                    static_cast<size_t>(old_allocated_end - end_));
  const size_t old_capacity = old_allocated_end - old_allocated_begin;
  const size_t final_size = static_cast<size_t>(end_ - begin_) + extra_capacity;
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
  const size_t new_capacity = UnsignedMax(
      static_cast<size_t>(end_ - old_allocated_begin + extra_capacity),
      old_capacity + old_capacity / 2, size_t{16});
  Block** const new_allocated_begin = NewBlockPtrs(new_capacity);
  Block** const new_allocated_end = new_allocated_begin + new_capacity;
  Block** const new_begin =
      new_allocated_begin + (begin_ - old_allocated_begin);
  Block** const new_end = new_begin + (end_ - begin_);
  std::memcpy(new_begin, begin_, sizeof(Block*) * (end_ - begin_));
  DeleteBlockPtrs();
  block_ptrs_.allocated.begin = new_allocated_begin;
  block_ptrs_.allocated.end = new_allocated_end;
  begin_ = new_begin;
  end_ = new_end;
}

inline void Chain::ReserveFrontSlow(size_t extra_capacity) {
  Block** old_allocated_begin;
  Block** old_allocated_end;
  if (is_here()) {
    RIEGELI_ASSERT(end_ <= block_ptrs_.here + 2);
    if (RIEGELI_LIKELY(extra_capacity <=
                       static_cast<size_t>(block_ptrs_.here + 2 - end_))) {
      // There is space without reallocation. Shift old blocks by extra_capacity
      // to the right because the new begin_ must remain at block_ptrs_.here.
      if (end_ != block_ptrs_.here) {
        // Shift just 1 block. It is never needed to shift 2 blocks because if
        // end_ - block_ptrs_.here == 2 then extra_capacity == 0.
        block_ptrs_.here[extra_capacity] = block_ptrs_.here[0];
      }
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
  RIEGELI_ASSERT(begin_ >= old_allocated_begin);
  RIEGELI_ASSERT_GT(extra_capacity,
                    static_cast<size_t>(begin_ - old_allocated_begin));
  const size_t old_capacity = old_allocated_end - old_allocated_begin;
  const size_t final_size = static_cast<size_t>(end_ - begin_) + extra_capacity;
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
  const size_t new_capacity = UnsignedMax(
      static_cast<size_t>(old_allocated_end - begin_ + extra_capacity),
      old_capacity + old_capacity / 2, size_t{16});
  Block** const new_allocated_begin = NewBlockPtrs(new_capacity);
  Block** const new_allocated_end = new_allocated_begin + new_capacity;
  Block** const new_end = new_allocated_end - (old_allocated_end - end_);
  Block** const new_begin = new_end - (end_ - begin_);
  std::memcpy(new_begin, begin_, sizeof(Block*) * (end_ - begin_));
  DeleteBlockPtrs();
  block_ptrs_.allocated.begin = new_allocated_begin;
  block_ptrs_.allocated.end = new_allocated_end;
  begin_ = new_begin;
  end_ = new_end;
}

inline size_t Chain::NewBlockCapacity(size_t replaced_size, size_t new_size,
                                      size_t size_hint) const {
  RIEGELI_ASSERT_LE(replaced_size, size_);
  const size_t size_before = size_ - replaced_size;
  return RoundUp<16>(UnsignedMax(
      replaced_size + new_size,
      UnsignedMin(size_before < size_hint
                      ? size_hint - size_before
                      : UnsignedMax(kMinBufferSize(), size_before / 2),
                  kMaxBufferSize())));
}

Chain::Buffer Chain::MakeAppendBuffer(size_t min_length, size_t size_hint) {
  Block* block;
  if (end_ != begin_) {
    Block* const last = back();
    last->PrepareForAppend();
    if (last->can_append(min_length)) {
      // New space can be appended in place.
      block = last;
    } else if (min_length == 0) {
      return Buffer();
    } else if (last->tiny() || last->wasteful()) {
      // The last block must be rewritten. Merge it with the new space to a
      // new block.
      block = Block::NewInternal(
          NewBlockCapacity(last->size(), min_length, size_hint));
      block->Append(last->data());
      last->Unref();
      back() = block;
    } else {
      goto new_block;
    }
  } else if (min_length == 0) {
    return Buffer();
  } else {
  new_block:
    // Append a new block.
    block = Block::NewInternal(NewBlockCapacity(0, min_length, size_hint));
    PushBack(block);
  }
  const Buffer buffer = block->MakeAppendBuffer();
  RIEGELI_ASSERT_GE(buffer.size(), min_length);
  size_ += buffer.size();
  return buffer;
}

Chain::Buffer Chain::MakePrependBuffer(size_t min_length, size_t size_hint) {
  Block* block;
  if (end_ != begin_) {
    Block* const first = front();
    first->PrepareForPrepend();
    if (first->can_prepend(min_length)) {
      // New space can be prepended in place.
      block = first;
    } else if (min_length == 0) {
      return Buffer();
    } else if (first->tiny() || first->wasteful()) {
      // The first block must be rewritten. Merge it with the new space to a
      // new block.
      block = Block::NewInternalForPrepend(
          NewBlockCapacity(first->size(), min_length, size_hint));
      block->Prepend(first->data());
      first->Unref();
      front() = block;
    } else {
      goto new_block;
    }
  } else if (min_length == 0) {
    return Buffer();
  } else {
  new_block:
    // Prepend a new block.
    block = Block::NewInternalForPrepend(
        NewBlockCapacity(0, min_length, size_hint));
    PushFront(block);
  }
  const Buffer buffer = block->MakePrependBuffer();
  RIEGELI_ASSERT_GE(buffer.size(), min_length);
  size_ += buffer.size();
  return buffer;
}

void Chain::Append(string_view src, size_t size_hint) {
  if (src.empty()) return;
  if (end_ != begin_) {
    Block* const last = back();
    last->PrepareForAppend();
    if (last->can_append(src.size())) {
      // src can be appended in place.
      last->Append(src);
      size_ += src.size();
      return;
    }
    const size_t available = last->max_can_append();
    if (last->tiny(available) || last->wasteful(available)) {
      // The last block must be rewritten. Merge it with src to a new block.
      Block* const merged = Block::NewInternal(
          NewBlockCapacity(last->size(), src.size(), size_hint));
      merged->Append(last->data());
      merged->Append(src);
      last->Unref();
      back() = merged;
      size_ += src.size();
      return;
    }
    if (available > 0) {
      last->Append(src.substr(0, available));
      size_ += available;
      src.remove_prefix(available);
    }
  }
  Block* const block =
      Block::NewInternal(NewBlockCapacity(0, src.size(), size_hint));
  block->Append(src);
  PushBack(block);
  size_ += block->size();
}

void Chain::Append(std::string&& src, size_t size_hint) {
  AppendExternal(StringRef(std::move(src)), size_hint);
}

void Chain::Append(const Chain& src, size_t size_hint) {
  if (src.empty()) return;
  Block* const* src_iter = src.begin_;
  if (end_ != begin_) {
    Block* const last = back();
    last->PrepareForAppend();
    // If the first block of src is handled specially, ++src_iter skips it so
    // that RefAndAppendBlocks() does not append it again.
    Block* const src_first = src.front();
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
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(last->size(), src_first->size(), size_hint)
                : last->size() + src_first->size();
        Block* const merged = Block::NewInternal(capacity);
        merged->Append(last->data());
        merged->Append(src_first->data());
        last->Unref();
        back() = merged;
      }
      ++src_iter;
    } else if (last->empty()) {
      if (src.end_ - src.begin_ > 1 &&
          (src_first->empty() || src_first->wasteful())) {
        goto merge;
      }
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
          src_first->size() <= kAllocationCost() + last->size()) {
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
  if (src.empty()) return;
  Block* const* src_iter = src.begin_;
  if (end_ != begin_) {
    Block* const last = back();
    last->PrepareForAppend();
    // If the first block of src is handled specially, (*src_iter++)->Unref()
    // skips it so that AppendBlocks() does not append it again.
    Block* const src_first = src.front();
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
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(last->size(), src_first->size(), size_hint)
                : last->size() + src_first->size();
        Block* const merged = Block::NewInternal(capacity);
        merged->Append(last->data());
        merged->Append(src_first->data());
        last->Unref();
        back() = merged;
      }
      (*src_iter++)->Unref();
    } else if (last->empty()) {
      if (src.end_ - src.begin_ > 1 &&
          (src_first->empty() || src_first->wasteful())) {
        goto merge;
      }
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
          src_first->size() <= kAllocationCost() + last->size()) {
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

void Chain::Prepend(string_view src, size_t size_hint) {
  if (src.empty()) return;
  if (end_ != begin_) {
    Block* const first = front();
    first->PrepareForPrepend();
    if (first->can_prepend(src.size())) {
      // src can be prepended in place.
      first->Prepend(src);
      size_ += src.size();
      return;
    }
    const size_t available = first->max_can_prepend();
    if (first->tiny(available) || first->wasteful(available)) {
      // The first block must be rewritten. Merge it with src to a new block.
      Block* const merged = Block::NewInternalForPrepend(
          NewBlockCapacity(first->size(), src.size(), size_hint));
      merged->Prepend(first->data());
      merged->Prepend(src);
      first->Unref();
      front() = merged;
      size_ += src.size();
      return;
    }
    if (available > 0) {
      first->Prepend(src.substr(src.size() - available));
      size_ += available;
      src.remove_suffix(available);
    }
  }
  Block* const block =
      Block::NewInternalForPrepend(NewBlockCapacity(0, src.size(), size_hint));
  block->Prepend(src);
  PushFront(block);
  size_ += block->size();
}

void Chain::Prepend(std::string&& src, size_t size_hint) {
  PrependExternal(StringRef(std::move(src)), size_hint);
}

void Chain::Prepend(const Chain& src, size_t size_hint) {
  if (src.empty()) return;
  Block* const* src_iter = src.end_;
  if (end_ != begin_) {
    Block* const first = front();
    first->PrepareForPrepend();
    // If the last block of src is handled specially, --src_iter skips it so
    // that RefAndPrependBlocks() does not prepend it again.
    Block* const src_last = src.back();
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
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(first->size(), src_last->size(), size_hint)
                : first->size() + src_last->size();
        Block* const merged = Block::NewInternalForPrepend(capacity);
        merged->Prepend(first->data());
        merged->Prepend(src_last->data());
        first->Unref();
        front() = merged;
      }
      --src_iter;
    } else if (first->empty()) {
      if (src.end_ - src.begin_ > 1 &&
          (src_last->empty() || src_last->wasteful())) {
        goto merge;
      }
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
          src_last->size() <= kAllocationCost() + first->size()) {
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
  if (src.empty()) return;
  Block* const* src_iter = src.end_;
  if (end_ != begin_) {
    Block* const first = front();
    first->PrepareForPrepend();
    // If the last block of src is handled specially, (*--src_iter)->Unref()
    // skips it so that PrependBlocks() does not prepend it again.
    Block* const src_last = src.back();
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
        const size_t capacity =
            src.end_ - src.begin_ == 1
                ? NewBlockCapacity(first->size(), src_last->size(), size_hint)
                : first->size() + src_last->size();
        Block* const merged = Block::NewInternalForPrepend(capacity);
        merged->Prepend(first->data());
        merged->Prepend(src_last->data());
        first->Unref();
        front() = merged;
      }
      (*--src_iter)->Unref();
    } else if (first->empty()) {
      if (src.end_ - src.begin_ > 1 &&
          (src_last->empty() || src_last->wasteful())) {
        goto merge;
      }
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
          src_last->size() <= kAllocationCost() + first->size()) {
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
  if (block->empty()) return;
  if (end_ != begin_) {
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
        Block* const merged = Block::NewInternal(
            NewBlockCapacity(last->size(), block->size(), size_hint));
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
          block->size() <= kAllocationCost() + last->size()) {
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

void Chain::RawAppendExternal(Block* (*new_block)(void*, string_view),
                              void* object, string_view data,
                              size_t size_hint) {
  if (data.size() <= kMaxBytesToCopy()) {
    Append(data, size_hint);
    return;
  }
  if (end_ != begin_) {
    Block* const last = back();
    if (last->empty()) {
      // The last block is empty and must be removed.
      PopBack();
      last->Unref();
    } else if (last->wasteful()) {
      // The last block must reduce waste.
      last->PrepareForAppend();
      if (last->can_append(data.size()) &&
          data.size() <= kAllocationCost() * 2 + last->size()) {
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

void Chain::RawPrependExternal(Block* (*new_block)(void*, string_view),
                               void* object, string_view data,
                               size_t size_hint) {
  if (data.size() <= kMaxBytesToCopy()) {
    Prepend(data, size_hint);
    return;
  }
  if (end_ != begin_) {
    Block* const first = front();
    if (first->empty()) {
      // The first block is empty and must be removed.
      PopFront();
      first->Unref();
    } else if (first->wasteful()) {
      // The first block must reduce waste.
      first->PrepareForPrepend();
      if (first->can_prepend(data.size()) &&
          data.size() <= kAllocationCost() * 2 + first->size()) {
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
  RIEGELI_ASSERT_GT(length, 0u);
  RIEGELI_ASSERT(end_ != begin_);
  Block** iter = end_;
  if (length > iter[-1]->size()) {
    do {
      length -= iter[-1]->size();
      (*--iter)->Unref();
      RIEGELI_ASSERT(iter != begin_);
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
  string_view data = block->data();
  data.remove_suffix(length);
  // Compensate for increasing size_ by Append() or AppendExternal().
  size_ -= data.size();
  if (data.size() <= kMaxBytesToCopy()) {
    Append(data, size_hint);
    block->Unref();
    return;
  }
  AppendExternal(BlockRef(block, false), data, size_hint);
}

void Chain::RemovePrefixSlow(size_t length, size_t size_hint) {
  RIEGELI_ASSERT_GT(length, 0u);
  RIEGELI_ASSERT(end_ != begin_);
  Block** iter = begin_;
  if (length > iter[0]->size()) {
    do {
      length -= iter[0]->size();
      (*iter++)->Unref();
      RIEGELI_ASSERT(iter != end_);
    } while (length > iter[0]->size());
    if (iter[0]->TryRemovePrefix(length)) {
      begin_ = iter;
      return;
    }
  }
  Block* const block = *iter++;
  begin_ = iter;
  if (length == block->size()) {
    block->Unref();
    return;
  }
  string_view data = block->data();
  data.remove_prefix(length);
  // Compensate for increasing size_ by Prepend() or PrependExternal().
  size_ -= data.size();
  if (data.size() <= kMaxBytesToCopy()) {
    Prepend(data, size_hint);
    block->Unref();
    return;
  }
  PrependExternal(BlockRef(block, false), data, size_hint);
}

void Chain::Swap(Chain* b) {
  using std::swap;
  if (is_here()) {
    begin_ = b->block_ptrs_.here + (begin_ - block_ptrs_.here);
    end_ = b->block_ptrs_.here + (end_ - block_ptrs_.here);
  }
  if (b->is_here()) {
    b->begin_ = block_ptrs_.here + (b->begin_ - b->block_ptrs_.here);
    b->end_ = block_ptrs_.here + (b->end_ - b->block_ptrs_.here);
  }
  swap(block_ptrs_, b->block_ptrs_);
  swap(begin_, b->begin_);
  swap(end_, b->end_);
  swap(size_, b->size_);
}

int Chain::Compare(string_view b) const {
  Chain::BlockIterator a_iter = blocks().begin();
  size_t a_pos = 0;
  size_t b_pos = 0;
  while (a_iter != blocks().end()) {
    if (b_pos == b.size()) {
      do {
        if (!a_iter->empty()) return 1;
        ++a_iter;
      } while (a_iter != blocks().end());
      return 0;
    }
    const size_t length = UnsignedMin(a_iter->size() - a_pos, b.size() - b_pos);
    const int result =
        std::memcmp(a_iter->data() + a_pos, b.data() + b_pos, length);
    if (result != 0) return result;
    a_pos += length;
    if (a_pos == a_iter->size()) {
      ++a_iter;
      a_pos = 0;
    }
    b_pos += length;
  }
  return b_pos == b.size() ? 0 : -1;
}

int Chain::Compare(const Chain& b) const {
  Chain::BlockIterator a_iter = blocks().begin();
  Chain::BlockIterator b_iter = b.blocks().begin();
  size_t a_pos = 0;
  size_t b_pos = 0;
  while (a_iter != blocks().end()) {
    if (b_iter == b.blocks().end()) {
      do {
        if (!a_iter->empty()) return 1;
        ++a_iter;
      } while (a_iter != blocks().end());
      return 0;
    }
    const size_t length =
        UnsignedMin(a_iter->size() - a_pos, b_iter->size() - b_pos);
    const int result =
        std::memcmp(a_iter->data() + a_pos, b_iter->data() + b_pos, length);
    if (result != 0) return result;
    a_pos += length;
    if (a_pos == a_iter->size()) {
      ++a_iter;
      a_pos = 0;
    }
    b_pos += length;
    if (b_pos == b_iter->size()) {
      ++b_iter;
      b_pos = 0;
    }
  }
  while (b_iter != b.blocks().end()) {
    if (!b_iter->empty()) return -1;
    ++b_iter;
  }
  return 0;
}

std::ostream& operator<<(std::ostream& out, const Chain& str) {
  std::ostream::sentry sentry(out);
  if (sentry) {
    size_t lpad = 0;
    size_t rpad = 0;
    RIEGELI_ASSERT_GE(out.width(), 0);
    if (static_cast<size_t>(out.width()) > str.size()) {
      size_t pad = out.width() - str.size();
      if ((out.flags() & out.adjustfield) == out.left) {
        rpad = pad;
      } else {
        lpad = pad;
      }
    }
    if (lpad > 0) WritePadding(out, lpad);
    for (auto fragment : str.blocks()) {
      out.write(fragment.data(), fragment.size());
    }
    if (rpad > 0) WritePadding(out, rpad);
    out.width(0);
  }
  return out;
}

}  // namespace riegeli
