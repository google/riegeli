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
#include <cstring>
#include <iosfwd>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/memory_estimator.h"

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
  struct PinnedBlock;

  constexpr Chain() noexcept {}

  explicit Chain(absl::string_view src);
  explicit Chain(std::string&& src);
  template <typename Src,
            typename Enable = absl::enable_if_t<
                std::is_convertible<Src, absl::string_view>::value>>
  explicit Chain(const Src& src) : Chain(absl::string_view(src)) {}

  Chain(const Chain& that);
  Chain& operator=(const Chain& that);

  // The source Chain is left cleared.
  //
  // Moving a Chain invalidates its BlockIterators and data pointers, but the
  // shape of blocks (their number and sizes) remains unchanged.
  Chain(Chain&& that) noexcept;
  Chain& operator=(Chain&& that) noexcept;

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

  // If the Chain contents are flat, returns them, otherwise returns nullopt.
  absl::optional<absl::string_view> TryFlat() const;

  // Estimates the amount of memory used by this Chain.
  size_t EstimateMemory() const;
  // Registers this Chain with MemoryEstimator.
  void RegisterSubobjects(MemoryEstimator* memory_estimator) const;
  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;

  // Appends/prepends some uninitialized space.
  //
  // The buffer will have length at least min_length.
  //
  // The buffer will have length at least recommended_length unless there are
  // reasons to return a smaller buffer (e.g. a smaller buffer is already
  // allocated or recommended_length exceeds internal thresholds).
  //
  // size_hint announces the intended total size, including existing data, this
  // buffer, and future data.
  //
  // If all three parameters are 0, returns whatever space is already allocated
  // (possibly an empty buffer), without invalidating existing pointers.
  //
  // Returns a larger buffer than requested if more space would be allocated
  // anyway. Use RemoveSuffix()/RemovePrefix() afterwards to trim excessive
  // length, they do not invalidate existing pointers when called directly after
  // {Append,Prepend}Buffer() with a length not exceeding the buffer size.
  absl::Span<char> AppendBuffer(size_t min_length = 0,
                                size_t recommended_length = 0,
                                size_t size_hint = 0);
  absl::Span<char> PrependBuffer(size_t min_length = 0,
                                 size_t recommended_length = 0,
                                 size_t size_hint = 0);

  void Append(absl::string_view src, size_t size_hint = 0);
  void Append(std::string&& src, size_t size_hint = 0);
  template <typename Src>
  absl::enable_if_t<std::is_convertible<Src, absl::string_view>::value, void>
  Append(const Src& src, size_t size_hint = 0) {
    Append(absl::string_view(src), size_hint);
  }
  void Append(const Chain& src, size_t size_hint = 0);
  void Append(Chain&& src, size_t size_hint = 0);
  void Prepend(absl::string_view src, size_t size_hint = 0);
  void Prepend(std::string&& src, size_t size_hint = 0);
  template <typename Src>
  absl::enable_if_t<std::is_convertible<Src, absl::string_view>::value, void>
  Prepend(const Src& src, size_t size_hint = 0) {
    Prepend(absl::string_view(src), size_hint);
  }
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
  //   absl::string_view data() const;
  //
  // If the data parameter is given, it must remain valid after the object is
  // moved.
  //
  // T must also support:
  //
  //   // Registers this object with MemoryEstimator.
  //   void RegisterSubobjects(absl::string_view data,
  //                           MemoryEstimator* memory_estimator) const;
  //   // Shows internal structure in a human-readable way, for debugging
  //   // (a type name is enough).
  //   void DumpStructure(absl::string_view data, std::ostream& out) const;
  //
  // where the data parameter of RegisterSubobjects() and DumpStructure()
  // will get the original value of the data parameter of AppendExternal()/
  // PrependExternal() (if given) or data() (otherwise). Having data available
  // in these functions might avoid storing it in the external object.
  //
  // AppendExternal()/PrependExternal() can decide to copy data instead. This is
  // always the case if data.size() <= kMaxBytesToCopy.
  template <typename T>
  void AppendExternal(T object, size_t size_hint = 0);
  template <typename T>
  void AppendExternal(T object, absl::string_view data, size_t size_hint = 0);
  template <typename T>
  void PrependExternal(T object, size_t size_hint = 0);
  template <typename T>
  void PrependExternal(T object, absl::string_view data, size_t size_hint = 0);

  void RemoveSuffix(size_t length, size_t size_hint = 0);
  void RemovePrefix(size_t length, size_t size_hint = 0);

  friend void swap(Chain& a, Chain& b) noexcept;

  int Compare(absl::string_view that) const;
  int Compare(const Chain& that) const;

  friend bool operator==(const Chain& a, const Chain& b);
  friend bool operator!=(const Chain& a, const Chain& b);
  friend bool operator<(const Chain& a, const Chain& b);
  friend bool operator>(const Chain& a, const Chain& b);
  friend bool operator<=(const Chain& a, const Chain& b);
  friend bool operator>=(const Chain& a, const Chain& b);

  friend bool operator==(const Chain& a, absl::string_view b);
  friend bool operator!=(const Chain& a, absl::string_view b);
  friend bool operator<(const Chain& a, absl::string_view b);
  friend bool operator>(const Chain& a, absl::string_view b);
  friend bool operator<=(const Chain& a, absl::string_view b);
  friend bool operator>=(const Chain& a, absl::string_view b);

  friend bool operator==(absl::string_view a, const Chain& b);
  friend bool operator!=(absl::string_view a, const Chain& b);
  friend bool operator<(absl::string_view a, const Chain& b);
  friend bool operator>(absl::string_view a, const Chain& b);
  friend bool operator<=(absl::string_view a, const Chain& b);
  friend bool operator>=(absl::string_view a, const Chain& b);

  friend std::ostream& operator<<(std::ostream& out, const Chain& str);

  // Given an object which represents a string, converts it to a Chain by
  // attaching the moved object, avoiding copying the string data.
  //
  // See AppendExternal() for details.
  template <typename T>
  friend Chain ChainFromExternal(T object);
  template <typename T>
  friend Chain ChainFromExternal(T object, absl::string_view data);

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

  static constexpr size_t kMaxShortDataSize = sizeof(Block*) * 2;

  union BlockPtrs {
    constexpr BlockPtrs() noexcept : empty() {}

    // If the Chain is empty, no block pointers are needed. Some union member is
    // needed though for the default constructor to be constexpr.
    Empty empty;
    // If begin_ == end_, size_ characters.
    //
    // If also has_here(), then there are 0 pointers in here so short_data
    // can safely contain size_ characters. If also has_allocated(), then
    // size_ == 0, and EnsureHasHere() must be called before writing to
    // short_data.
    char short_data[kMaxShortDataSize];
    // If has_here(), array of block pointers between begin_ == here and end_
    // (0 to 2 pointers).
    Block* here[2];
    // If has_allocated(), pointers to a heap-allocated array of block
    // pointers.
    Allocated allocated;
  };

  static constexpr size_t kMinBufferSize = 256;
  static constexpr size_t kMaxBufferSize = size_t{64} << 10;
  static constexpr size_t kAllocationCost = 256;

  bool has_here() const { return begin_ == block_ptrs_.here; }
  bool has_allocated() const { return begin_ != block_ptrs_.here; }

  absl::string_view short_data() const;

  static Block** NewBlockPtrs(size_t capacity);
  void DeleteBlockPtrs();
  // If has_allocated(), delete the block pointer array and make has_here()
  // true. This is used before appending to short_data.
  //
  // Precondition: begin_ == end_
  void EnsureHasHere();

  void UnrefBlocks();
  static void UnrefBlocks(Block* const* begin, Block* const* end);
  static void UnrefBlocksSlow(Block* const* begin, Block* const* end);

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
  // If replaced_length > 0, the block will replace an existing block of that
  // size. In addition to replaced_length, it requires the capacity of at least
  // min_length, preferably recommended_length.
  size_t NewBlockCapacity(size_t replaced_length, size_t min_length,
                          size_t recommended_length, size_t size_hint) const;

  void AppendBlock(Block* block, size_t size_hint);

  void RawAppendExternal(Block* (*new_block)(void*, absl::string_view),
                         void* object, absl::string_view data,
                         size_t size_hint);
  void RawPrependExternal(Block* (*new_block)(void*, absl::string_view),
                          void* object, absl::string_view data,
                          size_t size_hint);

  void RemoveSuffixSlow(size_t length, size_t size_hint);
  void RemovePrefixSlow(size_t length, size_t size_hint);

  BlockPtrs block_ptrs_;

  // The range of the block pointers array which is actually used.
  //
  // Invariants:
  //   begin_ <= end_
  //   if has_here() then begin_ == block_ptrs_.here
  //                  and end_ <= block_ptrs_.here + 2
  //   if has_allocated() then begin_ >= block_ptrs_.allocated.begin
  //                       and end_ <= block_ptrs_.allocated.end
  Block** begin_ = block_ptrs_.here;
  Block** end_ = block_ptrs_.here;

  // Invariants:
  //   if begin_ == end_ then size_ <= kMaxShortDataSize
  //   if begin_ == end_ && has_allocated() then size_ == 0
  //   if begin_ != end_ then
  //       size_ is the sum of sizes of blocks in [begin_, end_)
  size_t size_ = 0;
};

class Chain::BlockIterator {
 public:
  using iterator_category = std::input_iterator_tag;
  using value_type = absl::string_view;
  using reference = value_type;
  using difference_type = ptrdiff_t;

  class pointer {
   public:
    const value_type* operator->() const { return &value_; }

   private:
    friend class BlockIterator;

    explicit pointer(value_type value) : value_(value) {}

    value_type value_;
  };

  BlockIterator() noexcept {}

  explicit BlockIterator(const Chain* chain, size_t block_index);

  BlockIterator(const BlockIterator& that) noexcept;
  BlockIterator& operator=(const BlockIterator& that) noexcept;

  const Chain* chain() const { return chain_; }
  size_t block_index() const;

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

  // Pins the block pointed to by this iterator, keeping it alive and unchanged,
  // until PinnedBlock::Unpin() is called. Returns a PinnedBlock which contains
  // a data pointer valid for the pinned block and a void* token to be passed to
  // PinnedBlock::Unpin().
  //
  // Precondition: this is not past the end iterator.
  PinnedBlock Pin();

  // Returns a pointer to the external object if this points to an external
  // block holding an object of type T, otherwise returns nullptr.
  //
  // Precondition: this is not past the end iterator.
  template <typename T>
  const T* external_object() const;

  // Appends **this to *dest.
  //
  // Precondition: this is not past the end iterator.
  void AppendTo(Chain* dest, size_t size_hint = 0) const;

  // Appends substr to *dest. substr must be contained in **this.
  //
  // Precondition: this is not past the end iterator.
  void AppendSubstrTo(absl::string_view substr, Chain* dest,
                      size_t size_hint = 0) const;

 private:
  friend class Chain;

  // Sentinel values for iterators over a pseudo-block pointer representing
  // short data of a Chain.
  static Block* const kShortData[1];
  static Block* const* kBeginShortData() { return kShortData; }
  static Block* const* kEndShortData() { return kShortData + 1; }

  BlockIterator(const Chain* chain, Block* const* ptr) noexcept;

  const Chain* chain_ = nullptr;
  Block* const* ptr_ = nullptr;
};

// The result of BlockIterator::Pin(). Consists of a data pointer valid for the
// pinned block and a void* token to be passed to Unpin().
struct Chain::PinnedBlock {
  static void Unpin(void* token);

  absl::string_view data;
  void* token;
};

class Chain::Blocks {
 public:
  using value_type = absl::string_view;
  using const_reference = value_type;
  using reference = const_reference;
  using const_pointer = BlockIterator::pointer;
  using pointer = const_pointer;
  using const_iterator = BlockIterator;
  using iterator = const_iterator;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;
  using reverse_iterator = const_reverse_iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  Blocks() noexcept {}

  Blocks(const Blocks& that) noexcept;
  Blocks& operator=(const Blocks& that) noexcept;

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

  explicit Blocks(const Chain* chain) noexcept : chain_(chain) {}

  const Chain* chain_ = nullptr;
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
//  - tiny block: a block with size < kMinBufferSize
//  - wasteful block: a block with free space > max(size, kMinBufferSize)
//
// Invariants of a Chain:
//  - A block can be empty or wasteful only if it is the first or last block.
//  - Tiny blocks must not be adjacent.
class Chain::Block {
 public:
  // Tags for overloaded constructors. Public for SizeReturningNewAligned().
  struct ForAppend {};
  struct ForPrepend {};

  static constexpr size_t kInternalAllocatedOffset();
  static constexpr size_t kMaxCapacity =
      size_t{std::numeric_limits<ptrdiff_t>::max()};

  // Creates an internal block for appending.
  static Block* NewInternal(size_t min_capacity);

  // Creates an internal block for prepending.
  static Block* NewInternalForPrepend(size_t min_capacity);

  // Constructs an internal block. These constructors are public for
  // SizeReturningNewAligned().
  explicit Block(const size_t* raw_capacity, ForAppend);
  explicit Block(const size_t* raw_capacity, ForPrepend);

  // Constructs an external block containing the moved object and sets block
  // data to moved_object.data(). This constructor is public for NewAligned().
  template <typename T>
  explicit Block(T* object);

  // Constructs an external block containing the moved object and sets block
  // data to the data parameter, which must remain valid after the object is
  // moved. This constructor is public for NewAligned().
  template <typename T>
  explicit Block(T* object, absl::string_view data);

  Block* Ref();
  void Unref();

  Block* Copy();
  Block* CopyAndUnref();

  bool TryClear();

  const absl::string_view& data() const { return data_; }
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
  void RegisterShared(MemoryEstimator* memory_estimator) const;
  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;

  void PrepareForAppend();
  void PrepareForPrepend();
  bool can_append(size_t length) const;
  bool can_prepend(size_t length) const;
  size_t max_can_append() const;
  size_t max_can_prepend() const;
  absl::Span<char> AppendBuffer(size_t max_length);
  absl::Span<char> PrependBuffer(size_t max_length);
  void Append(absl::string_view src);
  // Reads size_to_copy from src.data() but account for src.size(). Faster
  // than Append() if size_to_copy is a compile time constant, but requires
  // size_to_copy bytes to be readable, possibly past the end of src.
  //
  // Precondition: size_to_copy >= src.size().
  void AppendWithExplicitSizeToCopy(absl::string_view src, size_t size_to_copy);
  void Prepend(absl::string_view src);
  bool TryRemoveSuffix(size_t length);
  bool TryRemovePrefix(size_t length);

  void AppendTo(Chain* dest, size_t size_hint);

  void AppendSubstrTo(absl::string_view substr, Chain* dest, size_t size_hint);

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

  template <typename T>
  static constexpr size_t kExternalObjectOffset();

  bool has_unique_owner() const;

  bool is_internal() const { return allocated_end_ != nullptr; }
  bool is_external() const { return allocated_end_ == nullptr; }

  size_t capacity() const;
  size_t space_before() const;
  size_t space_after() const;

  std::atomic<size_t> ref_count_{1};
  absl::string_view data_;
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
  void (*register_unique)(const Block* block,
                          MemoryEstimator* memory_estimator);
  void (*dump_structure)(const Block* block, std::ostream& out);
};

template <typename T>
struct Chain::ExternalMethodsFor {
  // object has type T*. Creates an external block containing the moved object
  // and sets block data to moved_object.data().
  static Block* NewBlockImplicitData(
      void* object, absl::string_view unused = absl::string_view());

  // object has type T*. Creates an external block containing the moved object
  // and sets block data to the data parameter, which must remain valid after
  // the object is moved.
  static Block* NewBlockExplicitData(void* object, absl::string_view data);

  static const Chain::ExternalMethods methods;

 private:
  static void DeleteBlock(Block* block);
  static void RegisterUnique(const Block* block,
                             MemoryEstimator* memory_estimator);
  static void DumpStructure(const Block* block, std::ostream& out);
};

template <typename T>
inline Chain::Block* Chain::ExternalMethodsFor<T>::NewBlockImplicitData(
    void* object, absl::string_view unused) {
  return NewAligned<Block, UnsignedMax(alignof(Block), alignof(T))>(
      Block::kExternalObjectOffset<T>() + sizeof(T), static_cast<T*>(object));
}

template <typename T>
inline Chain::Block* Chain::ExternalMethodsFor<T>::NewBlockExplicitData(
    void* object, absl::string_view data) {
  return NewAligned<Block, UnsignedMax(alignof(Block), alignof(T))>(
      Block::kExternalObjectOffset<T>() + sizeof(T), static_cast<T*>(object),
      data);
}

template <typename T>
const Chain::ExternalMethods Chain::ExternalMethodsFor<T>::methods = {
    DeleteBlock, RegisterUnique, DumpStructure};

template <typename T>
void Chain::ExternalMethodsFor<T>::DeleteBlock(Block* block) {
  block->unchecked_external_object<T>()->~T();
  DeleteAligned<Block, UnsignedMax(alignof(Block), alignof(T))>(
      block, Block::kExternalObjectOffset<T>() + sizeof(T));
}

template <typename T>
void Chain::ExternalMethodsFor<T>::RegisterUnique(
    const Block* block, MemoryEstimator* memory_estimator) {
  memory_estimator->RegisterDynamicMemory(Block::kExternalObjectOffset<T>() +
                                          sizeof(T));
  block->unchecked_external_object<T>()->RegisterSubobjects(block->data(),
                                                            memory_estimator);
}

template <typename T>
void Chain::ExternalMethodsFor<T>::DumpStructure(const Block* block,
                                                 std::ostream& out) {
  return block->unchecked_external_object<T>()->DumpStructure(block->data(),
                                                              out);
}

template <typename T>
inline Chain::Block::Block(T* object) {
  external_.methods = &ExternalMethodsFor<T>::methods;
  new (unchecked_external_object<T>()) T(std::move(*object));
  data_ = unchecked_external_object<T>()->data();
  RIEGELI_ASSERT(is_external())
      << "A Block with allocated_end_ == nullptr should be considered external";
}

template <typename T>
inline Chain::Block::Block(T* object, absl::string_view data) : data_(data) {
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

inline Chain::Block* Chain::Block::Ref() {
  ref_count_.fetch_add(1, std::memory_order_relaxed);
  return this;
}

inline bool Chain::Block::has_unique_owner() const {
  return ref_count_.load(std::memory_order_acquire) == 1;
}

template <typename T>
inline T* Chain::Block::unchecked_external_object() {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::Block::unchecked_external_object(): "
      << "block not external";
  return reinterpret_cast<T*>(reinterpret_cast<char*>(this) +
                              kExternalObjectOffset<T>());
}

template <typename T>
inline const T* Chain::Block::unchecked_external_object() const {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::Block::unchecked_external_object(): "
      << "block not external";
  return reinterpret_cast<const T*>(reinterpret_cast<const char*>(this) +
                                    kExternalObjectOffset<T>());
}

template <typename T>
inline const T* Chain::Block::checked_external_object() const {
  return is_external() && external_.methods == &ExternalMethodsFor<T>::methods
             ? unchecked_external_object<T>()
             : nullptr;
}

template <typename T>
inline T* Chain::Block::checked_external_object_with_unique_owner() {
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

inline Chain::BlockIterator::BlockIterator(const Chain* chain,
                                           size_t block_index)
    : chain_(chain),
      ptr_((ABSL_PREDICT_FALSE(chain_ == nullptr)
                ? nullptr
                : chain_->begin_ == chain_->end_
                      ? chain_->empty() ? BlockIterator::kEndShortData()
                                        : BlockIterator::kBeginShortData()
                      : chain_->begin_) +
           block_index) {}

inline Chain::BlockIterator::BlockIterator(const Chain* chain,
                                           Block* const* ptr) noexcept
    : chain_(chain), ptr_(ptr) {}

inline Chain::BlockIterator::BlockIterator(const BlockIterator& that) noexcept
    : chain_(that.chain_), ptr_(that.ptr_) {}

inline Chain::BlockIterator& Chain::BlockIterator::operator=(
    const BlockIterator& that) noexcept {
  chain_ = that.chain_;
  ptr_ = that.ptr_;
  return *this;
}

inline size_t Chain::BlockIterator::block_index() const {
  return PtrDistance(ABSL_PREDICT_FALSE(chain_ == nullptr)
                         ? nullptr
                         : chain_->begin_ == chain_->end_
                               ? chain_->empty()
                                     ? BlockIterator::kEndShortData()
                                     : BlockIterator::kBeginShortData()
                               : chain_->begin_,
                     ptr_);
}

inline Chain::BlockIterator::reference Chain::BlockIterator::operator*() const {
  RIEGELI_ASSERT(ptr_ != kEndShortData())
      << "Failed precondition of Chain::BlockIterator::operator*(): "
         "iterator is end()";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData())) {
    return chain_->short_data();
  } else {
    return (*ptr_)->data();
  }
}

inline Chain::BlockIterator::pointer Chain::BlockIterator::operator->() const {
  return pointer(**this);
}

inline Chain::BlockIterator& Chain::BlockIterator::operator++() {
  ++ptr_;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator++(int) {
  BlockIterator tmp = *this;
  ++*this;
  return tmp;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator--() {
  --ptr_;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator--(int) {
  BlockIterator tmp = *this;
  --*this;
  return tmp;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator+=(
    difference_type n) {
  ptr_ += n;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator+(
    difference_type n) const {
  return BlockIterator(*this) += n;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator-=(
    difference_type n) {
  ptr_ -= n;
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
  RIEGELI_ASSERT(a.chain_ == b.chain_)
      << "Failed precondition of operator==(Chain::BlockIterator): "
         "incomparable iterators";
  return a.ptr_ == b.ptr_;
}

inline bool operator!=(Chain::BlockIterator a, Chain::BlockIterator b) {
  RIEGELI_ASSERT(a.chain_ == b.chain_)
      << "Failed precondition of operator!=(Chain::BlockIterator): "
         "incomparable iterators";
  return a.ptr_ != b.ptr_;
}

inline bool operator<(Chain::BlockIterator a, Chain::BlockIterator b) {
  RIEGELI_ASSERT(a.chain_ == b.chain_)
      << "Failed precondition of operator<(Chain::BlockIterator): "
         "incomparable iterators";
  return a.ptr_ < b.ptr_;
}

inline bool operator>(Chain::BlockIterator a, Chain::BlockIterator b) {
  RIEGELI_ASSERT(a.chain_ == b.chain_)
      << "Failed precondition of operator>(Chain::BlockIterator): "
         "incomparable iterators";
  return a.ptr_ > b.ptr_;
}

inline bool operator<=(Chain::BlockIterator a, Chain::BlockIterator b) {
  RIEGELI_ASSERT(a.chain_ == b.chain_)
      << "Failed precondition of operator<=(Chain::BlockIterator): "
         "incomparable iterators";
  return a.ptr_ <= b.ptr_;
}

inline bool operator>=(Chain::BlockIterator a, Chain::BlockIterator b) {
  RIEGELI_ASSERT(a.chain_ == b.chain_)
      << "Failed precondition of operator>=(Chain::BlockIterator): "
         "incomparable iterators";
  return a.ptr_ >= b.ptr_;
}

inline Chain::BlockIterator::difference_type operator-(Chain::BlockIterator a,
                                                       Chain::BlockIterator b) {
  RIEGELI_ASSERT(a.chain_ == b.chain_)
      << "Failed precondition of operator-(Chain::BlockIterator): "
         "incomparable iterators";
  return a.ptr_ - b.ptr_;
}

inline Chain::BlockIterator operator+(Chain::BlockIterator::difference_type n,
                                      Chain::BlockIterator a) {
  return a + n;
}

template <typename T>
inline const T* Chain::BlockIterator::external_object() const {
  RIEGELI_ASSERT(ptr_ != kEndShortData())
      << "Failed precondition of Chain::BlockIterator::external_object(): "
         "iterator is end()";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData())) {
    return nullptr;
  } else {
    return (*ptr_)->checked_external_object<T>();
  }
}

inline Chain::Blocks::Blocks(const Blocks& that) noexcept
    : chain_(that.chain_) {}

inline Chain::Blocks& Chain::Blocks::operator=(const Blocks& that) noexcept {
  chain_ = that.chain_;
  return *this;
}

inline Chain::Blocks::const_iterator Chain::Blocks::begin() const {
  return BlockIterator(chain_, chain_->begin_ == chain_->end_
                                   ? chain_->empty()
                                         ? BlockIterator::kEndShortData()
                                         : BlockIterator::kBeginShortData()
                                   : chain_->begin_);
}

inline Chain::Blocks::const_iterator Chain::Blocks::end() const {
  return BlockIterator(chain_, chain_->begin_ == chain_->end_
                                   ? BlockIterator::kEndShortData()
                                   : chain_->end_);
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
  if (chain_->begin_ == chain_->end_) {
    return chain_->empty() ? 0 : 1;
  } else {
    return PtrDistance(chain_->begin_, chain_->end_);
  }
}

inline bool Chain::Blocks::empty() const {
  return chain_->begin_ == chain_->end_ && chain_->empty();
}

inline Chain::Blocks::const_reference Chain::Blocks::operator[](
    size_type n) const {
  RIEGELI_ASSERT_LT(n, size())
      << "Failed precondition of Chain::Blocks::operator[](): "
         "block index out of range";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return chain_->begin_[n]->data();
  }
}

inline Chain::Blocks::const_reference Chain::Blocks::at(size_type n) const {
  RIEGELI_CHECK_LT(n, size()) << "Failed precondition of Chain::Blocks::at(): "
                                 "block index out of range";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return chain_->begin_[n]->data();
  }
}

inline Chain::Blocks::const_reference Chain::Blocks::front() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::front(): no blocks";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return chain_->begin_[0]->data();
  }
}

inline Chain::Blocks::const_reference Chain::Blocks::back() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::back(): no blocks";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return chain_->end_[-1]->data();
  }
}

inline Chain::Chain(Chain&& that) noexcept
    : size_(absl::exchange(that.size_, 0)) {
  // Use memcpy() instead of copy constructor to silence -Wmaybe-uninitialized
  // in gcc.
  std::memcpy(&block_ptrs_, &that.block_ptrs_, sizeof(BlockPtrs));
  if (that.has_here()) {
    // that.has_here() implies that that.begin_ == that.block_ptrs_.here
    // already.
    begin_ = block_ptrs_.here;
    end_ =
        block_ptrs_.here + (absl::exchange(that.end_, that.block_ptrs_.here) -
                            that.block_ptrs_.here);
  } else {
    begin_ = absl::exchange(that.begin_, that.block_ptrs_.here);
    end_ = absl::exchange(that.end_, that.block_ptrs_.here);
  }
  // It does not matter what is left in that.block_ptrs_ because that.begin_ and
  // that.end_ point to the empty prefix of that.block_ptrs_.here[].
}

inline Chain& Chain::operator=(Chain&& that) noexcept {
  // Exchange that.begin_ and that.end_ early to support self-assignment.
  Block** begin;
  Block** end;
  if (that.has_here()) {
    // that.has_here() implies that that.begin_ == that.block_ptrs_.here
    // already.
    begin = block_ptrs_.here;
    end = block_ptrs_.here + (absl::exchange(that.end_, that.block_ptrs_.here) -
                              that.block_ptrs_.here);
  } else {
    begin = absl::exchange(that.begin_, that.block_ptrs_.here);
    end = absl::exchange(that.end_, that.block_ptrs_.here);
  }
  UnrefBlocks();
  DeleteBlockPtrs();
  // It does not matter what is left in that.block_ptrs_ because that.begin_ and
  // that.end_ point to the empty prefix of that.block_ptrs_.here[].
  // Use memcpy() instead of assignment to silence -Wmaybe-uninitialized in gcc.
  std::memcpy(&block_ptrs_, &that.block_ptrs_, sizeof(BlockPtrs));
  begin_ = begin;
  end_ = end;
  size_ = absl::exchange(that.size_, 0);
  return *this;
}

inline Chain::~Chain() {
  UnrefBlocks();
  DeleteBlockPtrs();
}

inline absl::string_view Chain::short_data() const {
  RIEGELI_ASSERT(begin_ == end_)
      << "Failed precondition of Chain::short_data(): blocks exist";
  return absl::string_view(block_ptrs_.short_data, size_);
}

inline void Chain::DeleteBlockPtrs() {
  if (has_allocated()) {
    std::allocator<Block*>().deallocate(
        block_ptrs_.allocated.begin,
        block_ptrs_.allocated.end - block_ptrs_.allocated.begin);
  }
}

inline void Chain::UnrefBlocks() { UnrefBlocks(begin_, end_); }

inline void Chain::UnrefBlocks(Block* const* begin, Block* const* end) {
  if (begin != end) UnrefBlocksSlow(begin, end);
}

inline Chain::Blocks Chain::blocks() const { return Blocks(this); }

inline absl::optional<absl::string_view> Chain::TryFlat() const {
  switch (end_ - begin_) {
    case 0:
      return short_data();
    case 1:
      return front()->data();
    default:
      return absl::nullopt;
  }
}

template <typename T>
inline void Chain::AppendExternal(T object, size_t size_hint) {
  RawAppendExternal(ExternalMethodsFor<T>::NewBlockImplicitData, &object,
                    object.data(), size_hint);
}

template <typename T>
inline void Chain::AppendExternal(T object, absl::string_view data,
                                  size_t size_hint) {
  RawAppendExternal(ExternalMethodsFor<T>::NewBlockExplicitData, &object, data,
                    size_hint);
}

template <typename T>
inline void Chain::PrependExternal(T object, size_t size_hint) {
  RawPrependExternal(ExternalMethodsFor<T>::NewBlockImplicitData, &object,
                     object.data(), size_hint);
}

template <typename T>
inline void Chain::PrependExternal(T object, absl::string_view data,
                                   size_t size_hint) {
  RawPrependExternal(ExternalMethodsFor<T>::NewBlockExplicitData, &object, data,
                     size_hint);
}

inline void Chain::RemoveSuffix(size_t length, size_t size_hint) {
  if (length == 0) return;
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of Chain::RemoveSuffix(): "
      << "length to remove greater than current size";
  size_ -= length;
  if (begin_ == end_) {
    // Chain has short data which have suffix removed in place.
    return;
  }
  if (ABSL_PREDICT_TRUE(length <= back()->size() &&
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
  size_ -= length;
  if (begin_ == end_) {
    // Chain has short data which have prefix removed by shifting the rest.
    std::memmove(block_ptrs_.short_data, block_ptrs_.short_data + length,
                 size_);
    return;
  }
  if (ABSL_PREDICT_TRUE(length <= front()->size() &&
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

inline bool operator==(const Chain& a, absl::string_view b) {
  return a.size() == b.size() && a.Compare(b) == 0;
}

inline bool operator!=(const Chain& a, absl::string_view b) {
  return !(a == b);
}

inline bool operator<(const Chain& a, absl::string_view b) {
  return a.Compare(b) < 0;
}

inline bool operator>(const Chain& a, absl::string_view b) {
  return a.Compare(b) > 0;
}

inline bool operator<=(const Chain& a, absl::string_view b) {
  return a.Compare(b) <= 0;
}

inline bool operator>=(const Chain& a, absl::string_view b) {
  return a.Compare(b) >= 0;
}

inline bool operator==(absl::string_view a, const Chain& b) {
  return a.size() == b.size() && b.Compare(a) == 0;
}

inline bool operator!=(absl::string_view a, const Chain& b) {
  return !(a == b);
}

inline bool operator<(absl::string_view a, const Chain& b) {
  return b.Compare(a) > 0;
}

inline bool operator>(absl::string_view a, const Chain& b) {
  return b.Compare(a) < 0;
}

inline bool operator<=(absl::string_view a, const Chain& b) {
  return b.Compare(a) >= 0;
}

inline bool operator>=(absl::string_view a, const Chain& b) {
  return b.Compare(a) <= 0;
}

template <typename T>
inline Chain ChainFromExternal(T object) {
  const absl::string_view data = object.data();
  Chain result;
  result.RawAppendExternal(Chain::ExternalMethodsFor<T>::NewBlockImplicitData,
                           &object, data, data.size());
  return result;
}

template <typename T>
inline Chain ChainFromExternal(T object, absl::string_view data) {
  Chain result;
  result.RawAppendExternal(Chain::ExternalMethodsFor<T>::NewBlockExplicitData,
                           &object, data, data.size());
  return result;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_CHAIN_H_
