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

#ifndef RIEGELI_BASE_CHAIN_H_
#define RIEGELI_BASE_CHAIN_H_

#include <stddef.h>
#include <atomic>
#include <iosfwd>
#include <iterator>
#include <limits>
#include <new>
#include <string>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

// A Chain represents a sequence of bytes. It supports efficient appending and
// prepending, and sharing memory with other Chains and other types. It does not
// support efficient random access.
//
// Chains can be written using ChainWriter and ChainBackwardWriter, and can be
// read using ChainReader. Chain itself exposes lower level appending/prepending
// and iteration functions.
//
// Any parameter named size_hint announces in advance the eventual Chain size.
// Providing it may reduce Chain memory usage. If the size hint turns out to not
// match reality, nothing breaks.
//
// A Chain is implemented with a sequence of blocks holding flat data fragments.
class Chain {
 public:
  class Blocks;
  class BlockIterator;
  class Buffer;

  // Not defaulted as a workaround for a bug in Clang < 3.2: a defaulted default
  // constructor of Chain is not constexpr, but an explicitly defined one can be
  // constexpr.
  constexpr Chain() noexcept {}

  explicit Chain(string_view src);
  explicit Chain(std::string&& src);
  explicit Chain(const char* src) : Chain(string_view(src)) {}

  Chain(const Chain& src);
  Chain& operator=(const Chain& src);

  // The source Chain is left cleared.
  //
  // Moving a Chain invalidates its BlockIterators, but the number of
  // BlockIterators and their respective data pointers are kept unchanged.
  Chain(Chain&& src) noexcept;
  Chain& operator=(Chain&& src) noexcept;

  ~Chain();

  void Clear();

  // A container of string_view blocks comprising data of the Chain.
  Blocks blocks() const;

  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  void CopyTo(char* dest) const;
  void AppendTo(std::string* dest) const;
  explicit operator std::string() const&;
  explicit operator std::string() &&;

  // Estimates the amount of memory used by this Chain.
  size_t EstimateMemory() const;
  // Registers this Chain with MemoryEstimator.
  void AddUniqueTo(MemoryEstimator* memory_estimator) const;
  void AddSharedTo(MemoryEstimator* memory_estimator) const;
  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;

  // Appends/prepends some uninitialized space with at least the given length.
  // Returns a larger buffer than requested if more space would be allocated
  // anyway (if min_length is 0, returns whatever space is already allocated).
  // Use RemoveSuffix()/RemovePrefix() afterwards to trim excessive size.
  Buffer MakeAppendBuffer(size_t min_length = 0, size_t size_hint = 0);
  Buffer MakePrependBuffer(size_t min_length = 0, size_t size_hint = 0);

  void Append(string_view src, size_t size_hint = 0);
  void Append(std::string&& src, size_t size_hint = 0);
  void Append(const char* src, size_t size_hint = 0);
  void Append(const Chain& src, size_t size_hint = 0);
  void Append(Chain&& src, size_t size_hint = 0);
  void Prepend(string_view src, size_t size_hint = 0);
  void Prepend(std::string&& src, size_t size_hint = 0);
  void Prepend(const char* src, size_t size_hint = 0);
  void Prepend(const Chain& src, size_t size_hint = 0);
  void Prepend(Chain&& src, size_t size_hint = 0);

  // Given an object which represents a string, appends/prepends it by attaching
  // the moved object, avoiding copying the string data.
  //
  // The type T of the object must be movable.
  //
  // If the data parameter is not given, T must support:
  //
  //   // Contents of the object.
  //   string_view data() const;
  //
  // If the data parameter is given, it must remain valid after the object is
  // moved.
  //
  // T must also support:
  //
  //   // Registers this object with MemoryEstimator.
  //   void AddUniqueTo(string_view data,
  //                    MemoryEstimator* memory_estimator) const;
  //   // Shows internal structure in a human-readable way, for debugging
  //   // (a type name is enough).
  //   void DumpStructure(string_view data, std::ostream& out) const;
  //
  // where the data parameter of AddUniqueTo() and DumpStructure() will get the
  // original value of the data parameter of AppendExternal()/PrependExternal()
  // (if given) or data() (otherwise). Having data available in these functions
  // might avoid storing it in the external object.
  //
  // AppendExternal()/PrependExternal() can decide to copy data instead. This is
  // always the case if data.size() <= kMaxBytesToCopy().
  template <typename T>
  void AppendExternal(T object, size_t size_hint = 0);
  template <typename T>
  void AppendExternal(T object, string_view data, size_t size_hint = 0);
  template <typename T>
  void PrependExternal(T object, size_t size_hint = 0);
  template <typename T>
  void PrependExternal(T object, string_view data, size_t size_hint = 0);

  void RemoveSuffix(size_t length, size_t size_hint = 0);
  void RemovePrefix(size_t length, size_t size_hint = 0);

  void Swap(Chain* b);

  int Compare(string_view b) const;
  int Compare(const Chain& b) const;

 private:
  struct ExternalMethods;
  template <typename T>
  struct ExternalMethodsFor;
  class Block;
  class BlockRef;
  class StringRef;

  struct Empty {};

  struct Allocated {
    Block** begin;
    Block** end;
  };

  union BlockPtrs {
    // Workaround for a bug in Clang < 3.2 and GCC: an implicitly generated
    // or defaulted default constructor of BlockPtrs is not constexpr, but
    // an explicitly defined one can be constexpr. Also, in GCC < 4.9 the member
    // initializer must be in the constructor if the constructor is constexpr.
    constexpr BlockPtrs() noexcept : empty() {}

    // If the Chain is empty, no block pointers are needed. Some union member is
    // needed though for the default constructor to be constexpr.
    Empty empty;
    // If is_here(), array of between 0 and 2 block pointers.
    Block* here[2];
    // If is_allocated(), pointers to a heap-allocated array of block pointers.
    Allocated allocated;
  };

  static constexpr size_t kMinBufferSize() { return 128; }
  static constexpr size_t kMaxBufferSize() { return size_t{64} << 10; }
  static constexpr size_t kAllocationCost() { return 256; }

  static Block** NewBlockPtrs(size_t capacity);
  void DeleteBlockPtrs();

  void UnrefBlocks();
  static void UnrefBlocks(Block* const* begin, Block* const* end);

  bool is_here() const { return begin_ == block_ptrs_.here; }
  bool is_allocated() const { return begin_ != block_ptrs_.here; }

  Block*& back() { return end_[-1]; }
  Block* const& back() const { return end_[-1]; }
  Block*& front() { return begin_[0]; }
  Block* const& front() const { return begin_[0]; }

  void PushBack(Block* block);
  void PushFront(Block* block);
  void PopBack();
  void PopFront();
  void RefAndAppendBlocks(Block* const* begin, Block* const* end);
  void RefAndPrependBlocks(Block* const* begin, Block* const* end);
  void AppendBlocks(Block* const* begin, Block* const* end);
  void PrependBlocks(Block* const* begin, Block* const* end);
  void ReserveBack(size_t extra_capacity);
  void ReserveFront(size_t extra_capacity);

  void ReserveBackSlow(size_t extra_capacity);
  void ReserveFrontSlow(size_t extra_capacity);

  // Decides about the capacity of a new block to be appended/prepended.
  // If replaced_size > 0, the block will replace an existing block of that
  // size. It requires the capacity of new_size in addition to replaced_size.
  size_t NewBlockCapacity(size_t replaced_size, size_t new_size,
                          size_t size_hint) const;

  void AppendBlock(Block* block, size_t size_hint);

  void RawAppendExternal(Block* (*new_block)(void*, string_view), void* object,
                         string_view data, size_t size_hint);
  void RawPrependExternal(Block* (*new_block)(void*, string_view), void* object,
                          string_view data, size_t size_hint);

  void RemoveSuffixSlow(size_t length, size_t size_hint);
  void RemovePrefixSlow(size_t length, size_t size_hint);

  BlockPtrs block_ptrs_;
  // The range of the block pointers array which is actually used.
  //
  // Invariants:
  //   begin_ <= end_
  //   if is_here() then begin_ == block_ptrs_.here
  //                 and end_ <= block_ptrs_.here + 2
  //   if is_allocated() then begin_ >= block_ptrs_.allocated.begin
  //                      and end_ <= block_ptrs_.allocated.end
  Block** begin_ = block_ptrs_.here;
  Block** end_ = block_ptrs_.here;
  // Invariant: size_ is the sum of sizes of blocks in [begin_, end)
  size_t size_ = 0;
};

inline void swap(Chain& a, Chain& b) noexcept { a.Swap(&b); }

bool operator==(const Chain& a, const Chain& b);
bool operator!=(const Chain& a, const Chain& b);
bool operator<(const Chain& a, const Chain& b);
bool operator>(const Chain& a, const Chain& b);
bool operator<=(const Chain& a, const Chain& b);
bool operator>=(const Chain& a, const Chain& b);

bool operator==(const Chain& a, string_view b);
bool operator!=(const Chain& a, string_view b);
bool operator<(const Chain& a, string_view b);
bool operator>(const Chain& a, string_view b);
bool operator<=(const Chain& a, string_view b);
bool operator>=(const Chain& a, string_view b);

bool operator==(string_view a, const Chain& b);
bool operator!=(string_view a, const Chain& b);
bool operator<(string_view a, const Chain& b);
bool operator>(string_view a, const Chain& b);
bool operator<=(string_view a, const Chain& b);
bool operator>=(string_view a, const Chain& b);

std::ostream& operator<<(std::ostream& out, const Chain& str);

class Chain::Blocks {
 public:
  using value_type = string_view;
  using pointer = value_type*;
  using const_pointer = const value_type*;
  using reference = value_type&;
  using const_reference = const value_type&;
  using const_iterator = BlockIterator;
  using iterator = const_iterator;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;
  using reverse_iterator = const_reverse_iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  Blocks() noexcept = default;

  Blocks(const Blocks&) noexcept = default;
  Blocks& operator=(const Blocks&) noexcept = default;

  const_iterator begin() const;
  const_iterator end() const;
  const_iterator cbegin() const;
  const_iterator cend() const;
  const_reverse_iterator rbegin() const;
  const_reverse_iterator rend() const;
  const_reverse_iterator crbegin() const;
  const_reverse_iterator crend() const;

  size_type size() const;
  bool empty() const;
  const_reference operator[](size_type n) const;
  const_reference at(size_type n) const;
  const_reference front() const;
  const_reference back() const;

 private:
  friend class Chain;

  Blocks(Block* const* begin, Block* const* end);

  Block* const* begin_ = nullptr;
  Block* const* end_ = nullptr;
};

class Chain::BlockIterator {
 public:
  using iterator_category = std::random_access_iterator_tag;
  using value_type = string_view;
  using pointer = const value_type*;
  using reference = const value_type&;
  using difference_type = ptrdiff_t;

  BlockIterator() noexcept = default;

  BlockIterator(const BlockIterator&) noexcept = default;
  BlockIterator& operator=(const BlockIterator&) noexcept = default;

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

  friend bool operator==(BlockIterator a, BlockIterator b);
  friend bool operator!=(BlockIterator a, BlockIterator b);
  friend bool operator<(BlockIterator a, BlockIterator b);
  friend bool operator>(BlockIterator a, BlockIterator b);
  friend bool operator<=(BlockIterator a, BlockIterator b);
  friend bool operator>=(BlockIterator a, BlockIterator b);
  friend difference_type operator-(BlockIterator a, BlockIterator b);
  friend BlockIterator operator+(difference_type n, BlockIterator a);

  // Pins the block pointed to by this iterator until Unpin() is called.
  // Returns a token to be passed to Unpin().
  //
  // Precondition: this is not past the end iterator.
  void* Pin() const;
  // Undoes the effect of one Pin() call. The argument must be the token
  // returned by Pin().
  static void Unpin(void* token);

  // Returns a pointer to the external object if this points to an external
  // block holding an object of type T, otherwise returns nullptr.
  template <typename T>
  const T* external_object() const;

  // Appends **this to *dest.
  void AppendTo(Chain* dest, size_t size_hint = 0) const;

  // Appends substr to *dest. substr must be contained in **this.
  void AppendSubstrTo(string_view substr, Chain* dest,
                      size_t size_hint = 0) const;

 private:
  friend class Chain;

  explicit BlockIterator(Block* const* iter) : iter_(iter) {}

  Block* const* iter_ = nullptr;
};

class Chain::Buffer {
 public:
  Buffer() noexcept = default;

  Buffer(char* data, size_t size) : data_(data), size_(size) {}

  Buffer(const Buffer&) noexcept = default;
  Buffer& operator=(const Buffer&) noexcept = default;

  char* data() const { return data_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

 private:
  char* data_ = nullptr;
  size_t size_ = 0;
};

// Implementation details follow.

// Chain representation consists of blocks.
//
// An internal block holds an allocated array which consists of free space
// before data, data, and free space after data. Block size is the size of
// data; block capacity is the size of the allocated array.
//
// An external block holds some object which keeps a data array alive, the
// destructor of the object, and the address of the data array.
//
// Definitions:
//  - empty block: a block with size == 0
//  - tiny block: a block with size < kMinBufferSize()
//  - wasteful block: a block with free space > max(size, kMinBufferSize())
//
// Invariants of a Chain:
//  - A block can be empty or wasteful only if it is the first or last block.
//  - Tiny blocks must not be adjacent.
class Chain::Block {
 public:
  static constexpr size_t kMaxCapacity();

  // Creates an internal block for appending.
  static Block* NewInternal(size_t capacity);

  // Creates an internal block for prepending.
  static Block* NewInternalForPrepend(size_t capacity);

  Block* Ref();
  void Unref();

  Block* Copy();
  Block* CopyAndUnref();

  bool TryClear();

  const string_view& data() const { return data_; }
  size_t size() const { return data_.size(); }
  bool empty() const { return data_.empty(); }
  const char* data_begin() const { return data_.data(); }
  const char* data_end() const { return data_begin() + size(); }

  // Returns a pointer to the external object, assuming that this is an external
  // block holding an object of type T.
  template <typename T>
  T* unchecked_external_object();
  template <typename T>
  const T* unchecked_external_object() const;

  // Returns a pointer to the external object if this is an external block
  // holding an object of type T, otherwise returns nullptr.
  template <typename T>
  const T* checked_external_object() const;

  // Returns a pointer to the external object if this is an external block
  // holding an object of type T and the block has a unique owner, otherwise
  // returns nullptr.
  template <typename T>
  T* checked_external_object_with_unique_owner();

  bool tiny(size_t extra_size = 0) const;
  bool wasteful(size_t extra_size = 0) const;

  // Registers this Block with MemoryEstimator.
  void AddUniqueTo(MemoryEstimator* memory_estimator) const;
  void AddSharedTo(MemoryEstimator* memory_estimator) const;
  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;

  void PrepareForAppend();
  void PrepareForPrepend();
  bool can_append(size_t size) const;
  bool can_prepend(size_t size) const;
  size_t max_can_append() const;
  size_t max_can_prepend() const;
  Buffer MakeAppendBuffer(size_t max_size);
  Buffer MakePrependBuffer(size_t max_size);
  void Append(string_view src);
  void Prepend(string_view src);
  bool TryRemoveSuffix(size_t length);
  bool TryRemovePrefix(size_t length);

 private:
  template <typename T>
  friend struct ExternalMethodsFor;

  struct External {
    // Type-erased methods of the object.
    const ExternalMethods* methods;
    // Lowest possible beginning of the object (actual object has a different
    // type and can begin at a higher address due to alignment).
    char object_lower_bound[1];
  };

  static constexpr size_t kInternalAllocatedOffset();
  template <typename T>
  static constexpr size_t kExternalObjectOffset();

  // Constructs an internal block.
  Block(size_t capacity, size_t space_before);

  // Constructs an external block containing the moved object and sets block
  // data to moved_object.data().
  template <typename T>
  explicit Block(T* object);

  // Constructs an external block containing the moved object and sets block
  // data to the data parameter, which must remain valid after the object is
  // moved.
  template <typename T>
  Block(T* object, string_view data);

  bool has_unique_owner() const;

  bool is_internal() const { return allocated_end_ != nullptr; }
  bool is_external() const { return allocated_end_ == nullptr; }

  size_t capacity() const;
  size_t space_before() const;
  size_t space_after() const;

  std::atomic<size_t> ref_count_{1};
  string_view data_;
  // If is_internal(), end of allocated space. If is_external(), nullptr.
  // This distinguishes internal from external blocks.
  const char* allocated_end_ = nullptr;
  union {
    // If is_internal(), beginning of data (actual allocated size is larger).
    char allocated_begin_[1];
    // If is_external(), the remaining fields.
    External external_;
  };

  // Invariant: if is_external(), data() is the same as external_object.data()
  // where external_object is the stored external object.
};

struct Chain::ExternalMethods {
  void (*delete_block)(Block* block);
  void (*add_unique_to)(const Block* block, MemoryEstimator* memory_estimator);
  void (*dump_structure)(const Block* block, std::ostream& out);
};

template <typename T>
struct Chain::ExternalMethodsFor {
  // object has type T*. Creates an external block containing the moved object
  // and sets block data to moved_object.data().
  static Block* NewBlockImplicitData(void* object, string_view unused);

  // object has type T*. Creates an external block containing the moved object
  // and sets block data to the data parameter, which must remain valid after
  // the object is moved.
  static Block* NewBlockExplicitData(void* object, string_view data);

  static const Chain::ExternalMethods methods;

 private:
  static void DeleteBlock(Block* block);
  static void AddUniqueTo(const Block* block,
                          MemoryEstimator* memory_estimator);
  static void DumpStructure(const Block* block, std::ostream& out);
};

template <typename T>
Chain::Block* Chain::ExternalMethodsFor<T>::NewBlockImplicitData(
    void* object, string_view unused) {
  Block* block =
      AllocateAlignedBytes<Block, UnsignedMax(alignof(Block), alignof(T))>(
          Block::kExternalObjectOffset<T>() + sizeof(T));
  new (block) Block(static_cast<T*>(object));
  return block;
}

template <typename T>
Chain::Block* Chain::ExternalMethodsFor<T>::NewBlockExplicitData(
    void* object, string_view data) {
  Block* block =
      AllocateAlignedBytes<Block, UnsignedMax(alignof(Block), alignof(T))>(
          Block::kExternalObjectOffset<T>() + sizeof(T));
  new (block) Block(static_cast<T*>(object), data);
  return block;
}

template <typename T>
const Chain::ExternalMethods Chain::ExternalMethodsFor<T>::methods = {
    DeleteBlock, AddUniqueTo, DumpStructure};

template <typename T>
void Chain::ExternalMethodsFor<T>::DeleteBlock(Block* block) {
  block->unchecked_external_object<T>()->~T();
  block->~Block();
  FreeAlignedBytes<Block, UnsignedMax(alignof(Block), alignof(T))>(
      block, Block::kExternalObjectOffset<T>() + sizeof(T));
}

template <typename T>
void Chain::ExternalMethodsFor<T>::AddUniqueTo(
    const Block* block, MemoryEstimator* memory_estimator) {
  memory_estimator->AddMemory(Block::kExternalObjectOffset<T>());
  block->unchecked_external_object<T>()->AddUniqueTo(block->data(),
                                                     memory_estimator);
}

template <typename T>
void Chain::ExternalMethodsFor<T>::DumpStructure(const Block* block,
                                                 std::ostream& out) {
  return block->unchecked_external_object<T>()->DumpStructure(block->data(),
                                                              out);
}

template <typename T>
Chain::Block::Block(T* object) {
  external_.methods = &ExternalMethodsFor<T>::methods;
  new (unchecked_external_object<T>()) T(std::move(*object));
  data_ = unchecked_external_object<T>()->data();
  RIEGELI_ASSERT(is_external())
      << "A Block with allocated_end_ == nullptr should be considered external";
}

template <typename T>
Chain::Block::Block(T* object, string_view data) : data_(data) {
  external_.methods = &ExternalMethodsFor<T>::methods;
  new (unchecked_external_object<T>()) T(std::move(*object));
  RIEGELI_ASSERT(is_external())
      << "A Block with allocated_end_ == nullptr should be considered external";
}

constexpr size_t Chain::Block::kInternalAllocatedOffset() {
  return offsetof(Block, allocated_begin_);
}

template <typename T>
constexpr size_t Chain::Block::kExternalObjectOffset() {
  return RoundUp<alignof(T)>(offsetof(Block, external_) +
                             offsetof(External, object_lower_bound));
}

constexpr size_t Chain::Block::kMaxCapacity() {
  return std::numeric_limits<size_t>::max() - kInternalAllocatedOffset();
}

inline Chain::Block* Chain::Block::Ref() {
  ref_count_.fetch_add(1, std::memory_order_relaxed);
  return this;
}

inline bool Chain::Block::has_unique_owner() const {
  return ref_count_.load(std::memory_order_acquire) == 1;
}

template <typename T>
T* Chain::Block::unchecked_external_object() {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::Block::unchecked_external_object(): "
      << "block not external";
  return reinterpret_cast<T*>(reinterpret_cast<char*>(this) +
                              kExternalObjectOffset<T>());
}

template <typename T>
const T* Chain::Block::unchecked_external_object() const {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::Block::unchecked_external_object(): "
      << "block not external";
  return reinterpret_cast<const T*>(reinterpret_cast<const char*>(this) +
                                    kExternalObjectOffset<T>());
}

template <typename T>
const T* Chain::Block::checked_external_object() const {
  return is_external() && external_.methods == &ExternalMethodsFor<T>::methods
             ? unchecked_external_object<T>()
             : nullptr;
}

template <typename T>
T* Chain::Block::checked_external_object_with_unique_owner() {
  return is_external() &&
                 external_.methods == &ExternalMethodsFor<T>::methods &&
                 has_unique_owner()
             ? unchecked_external_object<T>()
             : nullptr;
}

inline bool Chain::Block::TryRemoveSuffix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::Block::TryRemoveSuffix(): "
      << "length to remove greater than current size";
  if (is_internal() && has_unique_owner()) {
    data_.remove_suffix(length);
    return true;
  }
  return false;
}

inline bool Chain::Block::TryRemovePrefix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::Block::TryRemovePrefix(): "
      << "length to remove greater than current size";
  if (is_internal() && has_unique_owner()) {
    data_.remove_prefix(length);
    return true;
  }
  return false;
}

inline Chain::BlockIterator::reference Chain::BlockIterator::operator*() const {
  return (*iter_)->data();
}

inline Chain::BlockIterator::pointer Chain::BlockIterator::operator->() const {
  return &**this;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator++() {
  ++iter_;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator++(int) {
  BlockIterator tmp = *this;
  ++*this;
  return tmp;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator--() {
  --iter_;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator--(int) {
  BlockIterator tmp = *this;
  --*this;
  return tmp;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator+=(
    difference_type n) {
  iter_ += n;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator+(
    difference_type n) const {
  return BlockIterator(*this) += n;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator-=(
    difference_type n) {
  iter_ -= n;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator-(
    difference_type n) const {
  return BlockIterator(*this) -= n;
}

inline Chain::BlockIterator::reference Chain::BlockIterator::operator[](
    difference_type n) const {
  return *(*this + n);
}

inline bool operator==(Chain::BlockIterator a, Chain::BlockIterator b) {
  return a.iter_ == b.iter_;
}

inline bool operator!=(Chain::BlockIterator a, Chain::BlockIterator b) {
  return a.iter_ != b.iter_;
}

inline bool operator<(Chain::BlockIterator a, Chain::BlockIterator b) {
  return a.iter_ < b.iter_;
}

inline bool operator>(Chain::BlockIterator a, Chain::BlockIterator b) {
  return a.iter_ > b.iter_;
}

inline bool operator<=(Chain::BlockIterator a, Chain::BlockIterator b) {
  return a.iter_ <= b.iter_;
}

inline bool operator>=(Chain::BlockIterator a, Chain::BlockIterator b) {
  return a.iter_ >= b.iter_;
}

inline Chain::BlockIterator::difference_type operator-(Chain::BlockIterator a,
                                                       Chain::BlockIterator b) {
  return a.iter_ - b.iter_;
}

inline Chain::BlockIterator operator+(Chain::BlockIterator::difference_type n,
                                      Chain::BlockIterator a) {
  return a + n;
}

inline void* Chain::BlockIterator::Pin() const { return (*iter_)->Ref(); }

template <typename T>
const T* Chain::BlockIterator::external_object() const {
  return (*iter_)->checked_external_object<T>();
}

inline Chain::Blocks::Blocks(Block* const* begin, Block* const* end)
    : begin_(begin), end_(end) {}

inline Chain::Blocks::const_iterator Chain::Blocks::begin() const {
  return const_iterator(begin_);
}

inline Chain::Blocks::const_iterator Chain::Blocks::end() const {
  return const_iterator(end_);
}

inline Chain::Blocks::const_iterator Chain::Blocks::cbegin() const {
  return begin();
}

inline Chain::Blocks::const_iterator Chain::Blocks::cend() const {
  return end();
}

inline Chain::Blocks::const_reverse_iterator Chain::Blocks::rbegin() const {
  return const_reverse_iterator(end());
}

inline Chain::Blocks::const_reverse_iterator Chain::Blocks::rend() const {
  return const_reverse_iterator(begin());
}

inline Chain::Blocks::const_reverse_iterator Chain::Blocks::crbegin() const {
  return rbegin();
}

inline Chain::Blocks::const_reverse_iterator Chain::Blocks::crend() const {
  return rend();
}

inline Chain::Blocks::size_type Chain::Blocks::size() const {
  return PtrDistance(begin_, end_);
}

inline bool Chain::Blocks::empty() const { return begin_ == end_; }

inline Chain::Blocks::const_reference Chain::Blocks::operator[](
    size_type n) const {
  RIEGELI_ASSERT_LT(n, size())
      << "Failed precondition of Chain::Blocks::operator[](): "
         "block index out of range";
  return begin_[n]->data();
}

inline Chain::Blocks::const_reference Chain::Blocks::at(size_type n) const {
  RIEGELI_CHECK_LT(n, size()) << "Failed precondition of Chain::Blocks::at(): "
                                 "block index out of range";
  return begin_[n]->data();
}

inline Chain::Blocks::const_reference Chain::Blocks::front() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::front(): no blocks";
  return begin_[0]->data();
}

inline Chain::Blocks::const_reference Chain::Blocks::back() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::back(): no blocks";
  return end_[-1]->data();
}

inline Chain::Blocks Chain::blocks() const { return Blocks(begin_, end_); }

inline void Chain::Append(const char* src, size_t size_hint) {
  Append(string_view(src), size_hint);
}

inline void Chain::Prepend(const char* src, size_t size_hint) {
  Prepend(string_view(src), size_hint);
}

template <typename T>
void Chain::AppendExternal(T object, size_t size_hint) {
  RawAppendExternal(ExternalMethodsFor<T>::NewBlockImplicitData, &object,
                    object.data(), size_hint);
}

template <typename T>
void Chain::AppendExternal(T object, string_view data, size_t size_hint) {
  RawAppendExternal(ExternalMethodsFor<T>::NewBlockExplicitData, &object, data,
                    size_hint);
}

template <typename T>
void Chain::PrependExternal(T object, size_t size_hint) {
  RawPrependExternal(ExternalMethodsFor<T>::NewBlockImplicitData, &object,
                     object.data(), size_hint);
}

template <typename T>
void Chain::PrependExternal(T object, string_view data, size_t size_hint) {
  RawPrependExternal(ExternalMethodsFor<T>::NewBlockExplicitData, &object, data,
                     size_hint);
}

inline void Chain::RemoveSuffix(size_t length, size_t size_hint) {
  if (length == 0) return;
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of Chain::RemoveSuffix(): "
      << "length to remove greater than current size";
  RIEGELI_ASSERT(begin_ != end_)
      << "Failed invariant of Chain: no blocks but non-zero size";
  size_ -= length;
  if (RIEGELI_LIKELY(length <= back()->size() &&
                     back()->TryRemoveSuffix(length))) {
    return;
  }
  RemoveSuffixSlow(length, size_hint);
}

inline void Chain::RemovePrefix(size_t length, size_t size_hint) {
  if (length == 0) return;
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of Chain::RemovePrefix(): "
      << "length to remove greater than current size";
  RIEGELI_ASSERT(begin_ != end_)
      << "Failed invariant of Chain: no blocks but non-zero size";
  size_ -= length;
  if (RIEGELI_LIKELY(length <= front()->size() &&
                     front()->TryRemovePrefix(length))) {
    return;
  }
  RemovePrefixSlow(length, size_hint);
}

inline bool operator==(const Chain& a, const Chain& b) {
  return a.size() == b.size() && a.Compare(b) == 0;
}

inline bool operator!=(const Chain& a, const Chain& b) { return !(a == b); }

inline bool operator<(const Chain& a, const Chain& b) {
  return a.Compare(b) < 0;
}

inline bool operator>(const Chain& a, const Chain& b) {
  return a.Compare(b) > 0;
}

inline bool operator<=(const Chain& a, const Chain& b) {
  return a.Compare(b) <= 0;
}

inline bool operator>=(const Chain& a, const Chain& b) {
  return a.Compare(b) >= 0;
}

inline bool operator==(const Chain& a, string_view b) {
  return a.size() == b.size() && a.Compare(b) == 0;
}

inline bool operator!=(const Chain& a, string_view b) { return !(a == b); }

inline bool operator<(const Chain& a, string_view b) {
  return a.Compare(b) < 0;
}

inline bool operator>(const Chain& a, string_view b) {
  return a.Compare(b) > 0;
}

inline bool operator<=(const Chain& a, string_view b) {
  return a.Compare(b) <= 0;
}

inline bool operator>=(const Chain& a, string_view b) {
  return a.Compare(b) >= 0;
}

inline bool operator==(string_view a, const Chain& b) {
  return a.size() == b.size() && b.Compare(a) == 0;
}

inline bool operator!=(string_view a, const Chain& b) { return !(a == b); }

inline bool operator<(string_view a, const Chain& b) {
  return b.Compare(a) > 0;
}

inline bool operator>(string_view a, const Chain& b) {
  return b.Compare(a) < 0;
}

inline bool operator<=(string_view a, const Chain& b) {
  return b.Compare(a) >= 0;
}

inline bool operator>=(string_view a, const Chain& b) {
  return b.Compare(a) <= 0;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_CHAIN_H_
