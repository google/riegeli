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
#include <stdint.h>

#include <atomic>
#include <cstring>
#include <iosfwd>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/resetter.h"

namespace riegeli {

class ChainBlock;

// A Chain represents a sequence of bytes. It supports efficient appending and
// prepending, and sharing memory with other Chains and other types. It does not
// support efficient random access.
//
// Chains can be written using ChainWriter and ChainBackwardWriter, and can be
// read using ChainReader. Chain itself exposes lower level appending/prepending
// and iteration functions.
//
// Any parameter named size_hint announces the expected final size, or 0 if
// unknown. Providing it may improve performance and memory usage. If the size
// hint turns out to not match reality, nothing breaks.
//
// A Chain is implemented with a sequence of blocks holding flat data fragments.
class Chain {
 public:
  class Blocks;
  class BlockIterator;

  // A sentinel value for the max_length parameter of
  // AppendBuffer()/PrependBuffer().
  static constexpr size_t kAnyLength = std::numeric_limits<size_t>::max();

  // Given an object which owns a byte array, converts it to a Chain by
  // attaching the object, avoiding copying the bytes.
  //
  // If an object of type T is given, it is copied or moved to the Chain.
  //
  // If a tuple is given, an object of type T is constructed from elements of
  // the tuple. This avoids constructing a temporary object and moving from it.
  //
  // After the object or tuple, if the data parameter is given, data must be
  // valid for the copied, moved, or newly constructed object.
  //
  // If the data parameter is not given, T must support:
  //
  //   // Contents of the object.
  //   absl::string_view data() const;
  //
  // T may also support the following member functions, either with or without
  // the data parameter, with the following definitions assumed by default:
  //
  //   // Called once before the destructor, except on a moved-from object.
  //   void operator()(absl::string_view data) const {}
  //
  //   // Registers this object with MemoryEstimator.
  //   void RegisterSubobjects(absl::string_view data,
  //                           MemoryEstimator* memory_estimator) const {
  //     if (memory_estimator->RegisterNode(data.data())) {
  //       memory_estimator->RegisterDynamicMemory(data.size());
  //     }
  //   }
  //
  //   // Shows internal structure in a human-readable way, for debugging.
  //   void DumpStructure(absl::string_view data, std::ostream& out) const {
  //     out << "External";
  //   }
  //
  // The data parameter of these member functions, if present, will get the data
  // used by FromExternal(). Having data available in these functions might
  // avoid storing data in the external object.
  template <typename T>
  static Chain FromExternal(T&& object);
  template <typename T>
  static Chain FromExternal(T&& object, absl::string_view data);
  template <typename T, typename... Args>
  static Chain FromExternal(std::tuple<Args...> args);
  template <typename T, typename... Args>
  static Chain FromExternal(std::tuple<Args...> args, absl::string_view data);

  constexpr Chain() noexcept {}

  explicit Chain(absl::string_view src);
  explicit Chain(std::string&& src);
  explicit Chain(const char* src);
  explicit Chain(const ChainBlock& src);
  explicit Chain(ChainBlock&& src);

  Chain(const Chain& that);
  Chain& operator=(const Chain& that);

  // The source Chain is left cleared.
  //
  // Moving a Chain invalidates its BlockIterators and data pointers, but the
  // shape of blocks (their number and sizes) remains unchanged.
  Chain(Chain&& that) noexcept;
  Chain& operator=(Chain&& that) noexcept;

  ~Chain();

  // Makes *this equivalent to a newly constructed Chain. This avoids
  // constructing a temporary Chain and moving from it.
  void Reset();
  void Reset(absl::string_view src);
  void Reset(std::string&& src);
  void Reset(const char* src);
  void Reset(const ChainBlock& src);
  void Reset(ChainBlock&& src);

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

  // Appends/prepends some uninitialized space. The buffer will have length at
  // least min_length, preferably recommended_length, and at most max_length.
  //
  // If min_length == 0, returns whatever space was already allocated (possibly
  // an empty buffer) without invalidating existing pointers. If the Chain was
  // empty then the empty contents can be moved.
  //
  // If recommended_length < min_length, recommended_length is assumed to be
  // min_length.
  //
  // If max_length == kAnyLength, there is no maximum.
  //
  // Precondition: min_length <= max_length
  absl::Span<char> AppendBuffer(size_t min_length,
                                size_t recommended_length = 0,
                                size_t max_length = kAnyLength,
                                size_t size_hint = 0);
  absl::Span<char> PrependBuffer(size_t min_length,
                                 size_t recommended_length = 0,
                                 size_t max_length = kAnyLength,
                                 size_t size_hint = 0);

  // Equivalent to AppendBuffer/PrependBuffer with min_length == max_length.
  absl::Span<char> AppendFixedBuffer(size_t length, size_t size_hint = 0);
  absl::Span<char> PrependFixedBuffer(size_t length, size_t size_hint = 0);

  void Append(absl::string_view src, size_t size_hint = 0);
  void Append(std::string&& src, size_t size_hint = 0);
  void Append(const char* src, size_t size_hint = 0);
  void Append(const ChainBlock& src, size_t size_hint = 0);
  void Append(ChainBlock&& src, size_t size_hint = 0);
  void Append(const Chain& src, size_t size_hint = 0);
  void Append(Chain&& src, size_t size_hint = 0);
  void Prepend(absl::string_view src, size_t size_hint = 0);
  void Prepend(std::string&& src, size_t size_hint = 0);
  void Prepend(const char* src, size_t size_hint = 0);
  void Prepend(const ChainBlock& src, size_t size_hint = 0);
  void Prepend(ChainBlock&& src, size_t size_hint = 0);
  void Prepend(const Chain& src, size_t size_hint = 0);
  void Prepend(Chain&& src, size_t size_hint = 0);

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

 private:
  friend class ChainBlock;

  struct ExternalMethods;
  template <typename T>
  struct ExternalMethodsFor;
  class RawBlock;
  struct BlockPtrPtr;
  class BlockRef;
  class StringRef;

  friend ptrdiff_t operator-(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator==(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator!=(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator<(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator>(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator<=(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator>=(BlockPtrPtr a, BlockPtrPtr b);

  struct Empty {};

  struct Allocated {
    RawBlock** begin;
    RawBlock** end;
  };

  static constexpr size_t kMaxShortDataSize = 2 * sizeof(RawBlock*);

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
    RawBlock* here[2];
    // If has_allocated(), pointers to a heap-allocated array of block
    // pointers.
    Allocated allocated;
  };

  // Specifies how ownership of a potentially shared object is transferred,
  // for cases when this is not implied by parameter types.
  enum class Ownership {
    // The original owner keeps its reference. The reference count is increased
    // if the new owner also gets a reference.
    kShare,
    // The original owner drops its reference. The reference count is decreased
    // unless the new owner gets a reference instead.
    kSteal,
  };

  static constexpr size_t kAllocationCost = 256;

  void ClearSlow();

  bool has_here() const { return begin_ == block_ptrs_.here; }
  bool has_allocated() const { return begin_ != block_ptrs_.here; }

  absl::string_view short_data() const;

  static RawBlock** NewBlockPtrs(size_t capacity);
  void DeleteBlockPtrs();
  // If has_allocated(), delete the block pointer array and make has_here()
  // true. This is used before appending to short_data.
  //
  // Precondition: begin_ == end_
  void EnsureHasHere();

  void UnrefBlocks();
  static void UnrefBlocks(RawBlock* const* begin, RawBlock* const* end);
  static void UnrefBlocksSlow(RawBlock* const* begin, RawBlock* const* end);

  void DropStolenBlocks(
      std::integral_constant<Ownership, Ownership::kShare>) const;
  void DropStolenBlocks(std::integral_constant<Ownership, Ownership::kSteal>);

  RawBlock*& back() { return end_[-1]; }
  RawBlock* const& back() const { return end_[-1]; }
  RawBlock*& front() { return begin_[0]; }
  RawBlock* const& front() const { return begin_[0]; }

  void PushBack(RawBlock* block);
  void PushFront(RawBlock* block);
  void PopBack();
  void PopFront();
  template <Ownership ownership>
  void AppendBlocks(RawBlock* const* begin, RawBlock* const* end);
  template <Ownership ownership>
  void PrependBlocks(RawBlock* const* begin, RawBlock* const* end);
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

  template <Ownership ownership, typename ChainRef>
  void AppendImpl(ChainRef&& src, size_t size_hint);
  template <Ownership ownership, typename ChainRef>
  void PrependImpl(ChainRef&& src, size_t size_hint);

  template <Ownership ownership>
  void AppendBlock(RawBlock* block, size_t size_hint);
  template <Ownership ownership>
  void PrependBlock(RawBlock* block, size_t size_hint);

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
  RawBlock** begin_ = block_ptrs_.here;
  RawBlock** end_ = block_ptrs_.here;

  // Invariants:
  //   if begin_ == end_ then size_ <= kMaxShortDataSize
  //   if begin_ == end_ && has_allocated() then size_ == 0
  //   if begin_ != end_ then
  //       size_ is the sum of sizes of blocks in [begin_, end_)
  size_t size_ = 0;
};

// Represents either RawBlock* const*, or one of two special values
// (kBeginShortData and kEndShortData) behaving as if they were pointers in a
// single-element RawBlock* array.
struct Chain::BlockPtrPtr {
 public:
  static BlockPtrPtr from_ptr(RawBlock* const* ptr);

  bool is_special() const;
  RawBlock* const* as_ptr() const;

  BlockPtrPtr operator+(ptrdiff_t n) const;
  BlockPtrPtr operator-(ptrdiff_t n) const;
  friend ptrdiff_t operator-(BlockPtrPtr a, BlockPtrPtr b);

  friend bool operator==(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator!=(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator<(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator>(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator<=(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator>=(BlockPtrPtr a, BlockPtrPtr b);

  uintptr_t repr;
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

  // Returns a ChainBlock which pins the block pointed to by this iterator,
  // keeping it alive and unchanged, until either the ChainBlock is destroyed or
  // ChainBlock::Release() and ChainBlock::DeleteReleased() are called.
  //
  // Warning: the data pointer of the returned ChainBlock is not necessarily the
  // same as the data pointer of this BlockIterator (because of short Chain
  // optimization). Convert the ChainBlock to string_view or use
  // ChainBlock::data() for a data pointer valid for the pinned block.
  //
  // Precondition: this is not past the end iterator.
  ChainBlock Pin();

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

  // Appends substr to *dest. substr must be empty or contained in **this.
  //
  // Precondition:
  //   if substr is not empty then this is not past the end iterator.
  void AppendSubstrTo(absl::string_view substr, Chain* dest,
                      size_t size_hint = 0) const;

 private:
  friend class Chain;

  static constexpr BlockPtrPtr kBeginShortData{0};
  static constexpr BlockPtrPtr kEndShortData{sizeof(RawBlock*)};

  BlockIterator(const Chain* chain, BlockPtrPtr ptr) noexcept;

  RawBlock* PinImpl();

  const Chain* chain_ = nullptr;
  // If chain_ == nullptr, kBeginShortData.
  // If *chain_ has no block pointers and no short data, kEndShortData.
  // If *chain_ has short data, kBeginShortData or kEndShortData.
  // If *chain_ has block pointers, a pointer to the block pointer array.
  BlockPtrPtr ptr_ = kBeginShortData;
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

// A simplified variant of Chain constrained to have at most one block.
//
// ChainBlock uses the same block representation as Chain and thus can be
// efficiently appended to a Chain.
//
// ChainBlock uses no short data optimization.
class ChainBlock {
 public:
  // Maximum size of a ChainBlock.
  static constexpr size_t kMaxSize =
      size_t{std::numeric_limits<ptrdiff_t>::max()};

  // A sentinel value for the max_length parameter of
  // AppendBuffer()/PrependBuffer().
  static constexpr size_t kAnyLength = Chain::kAnyLength;

  // Given an object which owns a byte array, converts it to a ChainBlock by
  // attaching the object, avoiding copying the bytes.
  //
  // See Chain::FromExternal() for details.
  template <typename T>
  static ChainBlock FromExternal(T&& object);
  template <typename T>
  static ChainBlock FromExternal(T&& object, absl::string_view data);
  template <typename T, typename... Args>
  static ChainBlock FromExternal(std::tuple<Args...> args);
  template <typename T, typename... Args>
  static ChainBlock FromExternal(std::tuple<Args...> args,
                                 absl::string_view data);

  constexpr ChainBlock() noexcept {}

  ChainBlock(const ChainBlock& that) noexcept;
  ChainBlock& operator=(const ChainBlock& that) noexcept;

  // The source ChainBlock is left cleared.
  //
  // Moving a ChainBlock keeps its data pointers unchanged.
  ChainBlock(ChainBlock&& that) noexcept;
  ChainBlock& operator=(ChainBlock&& that) noexcept;

  ~ChainBlock();

  void Clear();

  explicit operator absl::string_view() const;
  const char* data() const;
  size_t size() const;
  bool empty() const;

  // Appends/prepends some uninitialized space. The buffer will have length at
  // least min_length, preferably recommended_length, and at most max_length.
  //
  // If min_length == 0, returns whatever space was already allocated (possibly
  // an empty buffer). without invalidating existing pointers. If the ChainBlock
  // was empty then the empty contents can be moved.
  //
  // If recommended_length < min_length, recommended_length is assumed to be
  // min_length.
  //
  // If max_length == kAnyLength, there is no maximum.
  //
  // Precondition: min_length <= max_length
  absl::Span<char> AppendBuffer(size_t min_length,
                                size_t recommended_length = 0,
                                size_t max_length = kAnyLength,
                                size_t size_hint = 0);
  absl::Span<char> PrependBuffer(size_t min_length,
                                 size_t recommended_length = 0,
                                 size_t max_length = kAnyLength,
                                 size_t size_hint = 0);

  // Equivalent to AppendBuffer/PrependBuffer with min_length == max_length.
  absl::Span<char> AppendFixedBuffer(size_t length, size_t size_hint = 0);
  absl::Span<char> PrependFixedBuffer(size_t length, size_t size_hint = 0);

  void RemoveSuffix(size_t length, size_t size_hint = 0);
  void RemovePrefix(size_t length, size_t size_hint = 0);

  // Appends *this to *dest.
  void AppendTo(Chain* dest, size_t size_hint = 0) const;

  // Appends substr to *dest. substr must be empty or contained in *this.
  void AppendSubstrTo(absl::string_view substr, Chain* dest,
                      size_t size_hint = 0) const;

  // Releases the ownership of the block, which must be deleted using
  // DeleteReleased() if not nullptr.
  void* Release();

  // Deletes the pointer obtained by Release().
  static void DeleteReleased(void* ptr);

 private:
  friend class Chain;

  using RawBlock = Chain::RawBlock;

  explicit ChainBlock(RawBlock* block) : block_(block) {}

  // Decides about the capacity of a new block to be appended/prepended.
  size_t NewBlockCapacity(size_t min_length, size_t recommended_length,
                          size_t size_hint) const;

  void RemoveSuffixSlow(size_t length, size_t size_hint);
  void RemovePrefixSlow(size_t length, size_t size_hint);

  RawBlock* block_ = nullptr;
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
class Chain::RawBlock {
 public:
  template <typename T>
  struct ExternalType {};

  static constexpr size_t kInternalAllocatedOffset();
  static constexpr size_t kMaxCapacity = ChainBlock::kMaxSize;

  // Creates an internal block.
  static RawBlock* NewInternal(size_t min_capacity);

  // Constructs an internal block. This constructor is public for
  // SizeReturningNewAligned().
  explicit RawBlock(const size_t* raw_capacity);

  // Constructs an external block containing an external object constructed from
  // args, and sets block data to object.data(). This constructor is public for
  // NewAligned().
  template <typename T, typename... Args>
  explicit RawBlock(ExternalType<T>, std::tuple<Args...> args);

  // Constructs an external block containing an external object constructed from
  // args, and sets block data to the data parameter. This constructor is public
  // for NewAligned().
  template <typename T, typename... Args>
  explicit RawBlock(ExternalType<T>, std::tuple<Args...> args,
                    absl::string_view data);

  template <Ownership ownership = Ownership::kShare>
  RawBlock* Ref();

  template <Ownership ownership = Ownership::kSteal>
  void Unref();

  template <Ownership ownership>
  RawBlock* Copy();

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

  // Registers this RawBlock with MemoryEstimator.
  void RegisterShared(MemoryEstimator* memory_estimator) const;
  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;

  bool can_append(size_t length) const;
  bool can_prepend(size_t length) const;
  bool CanAppendMovingData(size_t length);
  bool CanPrependMovingData(size_t length);
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

  template <typename T, typename... Args, size_t... Indices>
  void ConstructExternal(std::tuple<Args...> args,
                         absl::index_sequence<Indices...>);

  bool has_unique_owner() const;

  bool is_internal() const { return allocated_end_ != nullptr; }
  bool is_external() const { return allocated_end_ == nullptr; }

  size_t capacity() const;
  size_t space_before() const;
  size_t space_after() const;
  size_t raw_space_before() const;
  size_t raw_space_after() const;

  std::atomic<size_t> ref_count_{1};
  absl::string_view data_;
  // If is_internal(), end of allocated space. If is_external(), nullptr.
  // This distinguishes internal from external blocks.
  char* allocated_end_ = nullptr;
  union {
    // If is_internal(), beginning of data (actual allocated size is larger).
    char allocated_begin_[1];
    // If is_external(), the remaining fields.
    External external_;
  };
};

struct Chain::ExternalMethods {
  void (*delete_block)(RawBlock* block);
  void (*register_unique)(const RawBlock* block,
                          MemoryEstimator* memory_estimator);
  void (*dump_structure)(const RawBlock* block, std::ostream& out);
};

namespace internal {

template <typename T, typename Enable = void>
struct HasCallOperatorWithData : public std::false_type {};

template <typename T>
struct HasCallOperatorWithData<T, absl::void_t<decltype(std::declval<T>()(
                                      std::declval<absl::string_view>()))>>
    : public std::true_type {};

template <typename T, typename Enable = void>
struct HasCallOperatorWithoutData : public std::false_type {};

template <typename T>
struct HasCallOperatorWithoutData<T,
                                  absl::void_t<decltype(std::declval<T>()())>>
    : public std::true_type {};

template <typename T>
inline absl::enable_if_t<HasCallOperatorWithData<T>::value, void> CallOperator(
    T* object, absl::string_view data) {
  (*object)(data);
}

template <typename T>
inline absl::enable_if_t<!HasCallOperatorWithData<T>::value &&
                             HasCallOperatorWithoutData<T>::value,
                         void>
CallOperator(T* object, absl::string_view data) {
  (*object)();
}

template <typename T>
inline absl::enable_if_t<!HasCallOperatorWithData<T>::value &&
                             !HasCallOperatorWithoutData<T>::value,
                         void>
CallOperator(T* object, absl::string_view data) {}

template <typename T, typename Enable = void>
struct HasRegisterSubobjectsWithData : public std::false_type {};

template <typename T>
struct HasRegisterSubobjectsWithData<
    T,
    absl::void_t<decltype(std::declval<T>().RegisterSubobjects(
        std::declval<absl::string_view>(), std::declval<MemoryEstimator*>()))>>
    : public std::true_type {};

template <typename T, typename Enable = void>
struct HasRegisterSubobjectsWithoutData : public std::false_type {};

template <typename T>
struct HasRegisterSubobjectsWithoutData<
    T, absl::void_t<decltype(std::declval<T>().RegisterSubobjects(
           std::declval<MemoryEstimator*>()))>> : public std::true_type {};

template <typename T>
inline absl::enable_if_t<HasRegisterSubobjectsWithData<T>::value, void>
RegisterSubobjects(T* object, absl::string_view data,
                   MemoryEstimator* memory_estimator) {
  object->RegisterSubobjects(data, memory_estimator);
}

template <typename T>
inline absl::enable_if_t<!HasRegisterSubobjectsWithData<T>::value &&
                             HasRegisterSubobjectsWithoutData<T>::value,
                         void>
RegisterSubobjects(T* object, absl::string_view data,
                   MemoryEstimator* memory_estimator) {
  object->RegisterSubobjects(memory_estimator);
}

template <typename T>
inline absl::enable_if_t<!HasRegisterSubobjectsWithData<T>::value &&
                             !HasRegisterSubobjectsWithoutData<T>::value,
                         void>
RegisterSubobjects(T* object, absl::string_view data,
                   MemoryEstimator* memory_estimator) {
  if (memory_estimator->RegisterNode(data.data())) {
    memory_estimator->RegisterDynamicMemory(data.size());
  }
}

template <typename T, typename Enable = void>
struct HasDumpStructureWithData : public std::false_type {};

template <typename T>
struct HasDumpStructureWithData<
    T, absl::void_t<decltype(std::declval<T>().DumpStructure(
           std::declval<absl::string_view>(), std::declval<std::ostream&>()))>>
    : public std::true_type {};

template <typename T, typename Enable = void>
struct HasDumpStructureWithoutData : public std::false_type {};

template <typename T>
struct HasDumpStructureWithoutData<
    T, absl::void_t<decltype(std::declval<T>().DumpStructure(
           std::declval<std::ostream&>()))>> : public std::true_type {};

template <typename T>
inline absl::enable_if_t<HasDumpStructureWithData<T>::value, void>
DumpStructure(T* object, absl::string_view data, std::ostream& out) {
  object->DumpStructure(data, out);
}

template <typename T>
inline absl::enable_if_t<!HasDumpStructureWithData<T>::value &&
                             HasDumpStructureWithoutData<T>::value,
                         void>
DumpStructure(T* object, absl::string_view data, std::ostream& out) {
  object->DumpStructure(out);
}

template <typename T>
inline absl::enable_if_t<!HasDumpStructureWithData<T>::value &&
                             !HasDumpStructureWithoutData<T>::value,
                         void>
DumpStructure(T* object, absl::string_view data, std::ostream& out) {
  out << "External";
}

}  // namespace internal

template <typename T>
struct Chain::ExternalMethodsFor {
  // Creates an external block containing an external object constructed from
  // args, and sets block data to object.data().
  template <typename... Args>
  static RawBlock* NewBlock(std::tuple<Args...> args);

  // Creates an external block containing an external object constructed from
  // args, and sets block data to the data parameter.
  template <typename... Args>
  static RawBlock* NewBlock(std::tuple<Args...> args, absl::string_view data);

  static const Chain::ExternalMethods methods;

 private:
  static void DeleteBlock(RawBlock* block);
  static void RegisterUnique(const RawBlock* block,
                             MemoryEstimator* memory_estimator);
  static void DumpStructure(const RawBlock* block, std::ostream& out);
};

template <typename T>
template <typename... Args>
inline Chain::RawBlock* Chain::ExternalMethodsFor<T>::NewBlock(
    std::tuple<Args...> args) {
  return NewAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
      RawBlock::kExternalObjectOffset<T>() + sizeof(T),
      RawBlock::ExternalType<T>(), std::move(args));
}

template <typename T>
template <typename... Args>
inline Chain::RawBlock* Chain::ExternalMethodsFor<T>::NewBlock(
    std::tuple<Args...> args, absl::string_view data) {
  return NewAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
      RawBlock::kExternalObjectOffset<T>() + sizeof(T),
      RawBlock::ExternalType<T>(), std::move(args), data);
}

template <typename T>
const Chain::ExternalMethods Chain::ExternalMethodsFor<T>::methods = {
    DeleteBlock, RegisterUnique, DumpStructure};

template <typename T>
void Chain::ExternalMethodsFor<T>::DeleteBlock(RawBlock* block) {
  internal::CallOperator(block->unchecked_external_object<T>(), block->data());
  block->unchecked_external_object<T>()->~T();
  DeleteAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
      block, RawBlock::kExternalObjectOffset<T>() + sizeof(T));
}

template <typename T>
void Chain::ExternalMethodsFor<T>::RegisterUnique(
    const RawBlock* block, MemoryEstimator* memory_estimator) {
  memory_estimator->RegisterDynamicMemory(RawBlock::kExternalObjectOffset<T>() +
                                          sizeof(T));
  internal::RegisterSubobjects(block->unchecked_external_object<T>(),
                               block->data(), memory_estimator);
}

template <typename T>
void Chain::ExternalMethodsFor<T>::DumpStructure(const RawBlock* block,
                                                 std::ostream& out) {
  internal::DumpStructure(block->unchecked_external_object<T>(), block->data(),
                          out);
}

template <typename T, typename... Args>
inline Chain::RawBlock::RawBlock(ExternalType<T>, std::tuple<Args...> args) {
  ConstructExternal<T>(std::move(args), absl::index_sequence_for<Args...>());
  data_ = unchecked_external_object<T>()->data();
  RIEGELI_ASSERT(is_external()) << "A RawBlock with allocated_end_ == nullptr "
                                   "should be considered external";
}

template <typename T, typename... Args>
inline Chain::RawBlock::RawBlock(ExternalType<T>, std::tuple<Args...> args,
                                 absl::string_view data)
    : data_(data) {
  ConstructExternal<T>(std::move(args), absl::index_sequence_for<Args...>());
  RIEGELI_ASSERT(is_external()) << "A RawBlock with allocated_end_ == nullptr "
                                   "should be considered external";
}

constexpr size_t Chain::RawBlock::kInternalAllocatedOffset() {
  return offsetof(RawBlock, allocated_begin_);
}

template <typename T>
constexpr size_t Chain::RawBlock::kExternalObjectOffset() {
  return RoundUp<alignof(T)>(offsetof(RawBlock, external_) +
                             offsetof(External, object_lower_bound));
}

template <Chain::Ownership ownership>
inline Chain::RawBlock* Chain::RawBlock::Ref() {
  if (ownership == Ownership::kShare) {
    ref_count_.fetch_add(1, std::memory_order_relaxed);
  }
  return this;
}

template <Chain::Ownership ownership>
void Chain::RawBlock::Unref() {
  if (ownership == Ownership::kSteal &&
      (has_unique_owner() ||
       ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1)) {
    if (is_internal()) {
      DeleteAligned<RawBlock>(this, kInternalAllocatedOffset() + capacity());
    } else {
      external_.methods->delete_block(this);
    }
  }
}

template <typename T, typename... Args, size_t... Indices>
inline void Chain::RawBlock::ConstructExternal(
    ABSL_ATTRIBUTE_UNUSED std::tuple<Args...> args,
    absl::index_sequence<Indices...>) {
  external_.methods = &ExternalMethodsFor<T>::methods;
  new (unchecked_external_object<T>()) T(std::move(std::get<Indices>(args))...);
}

inline bool Chain::RawBlock::has_unique_owner() const {
  return ref_count_.load(std::memory_order_acquire) == 1;
}

inline size_t Chain::RawBlock::capacity() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::RawBlock::capacity(): "
         "block not internal";
  return PtrDistance(allocated_begin_, allocated_end_);
}

template <typename T>
inline T* Chain::RawBlock::unchecked_external_object() {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::RawBlock::unchecked_external_object(): "
      << "block not external";
  return reinterpret_cast<T*>(reinterpret_cast<char*>(this) +
                              kExternalObjectOffset<T>());
}

template <typename T>
inline const T* Chain::RawBlock::unchecked_external_object() const {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::RawBlock::unchecked_external_object(): "
      << "block not external";
  return reinterpret_cast<const T*>(reinterpret_cast<const char*>(this) +
                                    kExternalObjectOffset<T>());
}

template <typename T>
inline const T* Chain::RawBlock::checked_external_object() const {
  return is_external() && external_.methods == &ExternalMethodsFor<T>::methods
             ? unchecked_external_object<T>()
             : nullptr;
}

template <typename T>
inline T* Chain::RawBlock::checked_external_object_with_unique_owner() {
  return is_external() &&
                 external_.methods == &ExternalMethodsFor<T>::methods &&
                 has_unique_owner()
             ? unchecked_external_object<T>()
             : nullptr;
}

inline bool Chain::RawBlock::TryClear() {
  if (is_internal() && has_unique_owner()) {
    data_.remove_suffix(size());
    return true;
  }
  return false;
}

inline bool Chain::RawBlock::TryRemoveSuffix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::RawBlock::TryRemoveSuffix(): "
      << "length to remove greater than current size";
  if (is_internal() && has_unique_owner()) {
    data_.remove_suffix(length);
    return true;
  }
  return false;
}

inline bool Chain::RawBlock::TryRemovePrefix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::RawBlock::TryRemovePrefix(): "
      << "length to remove greater than current size";
  if (is_internal() && has_unique_owner()) {
    data_.remove_prefix(length);
    return true;
  }
  return false;
}

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::from_ptr(RawBlock* const* ptr) {
  return BlockPtrPtr{reinterpret_cast<uintptr_t>(ptr)};
}

inline bool Chain::BlockPtrPtr::is_special() const {
  return repr <= sizeof(RawBlock*);
}

inline Chain::RawBlock* const* Chain::BlockPtrPtr::as_ptr() const {
  RIEGELI_ASSERT(!is_special()) << "Unexpected special BlockPtrPtr value";
  return reinterpret_cast<RawBlock* const*>(repr);
}

// Code conditional on is_special() is written such that both branches typically
// compile to the same code, allowing the compiler eliminate the is_special()
// checks.

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::operator+(ptrdiff_t n) const {
  if (is_special()) {
    return BlockPtrPtr{repr + n * ptrdiff_t{sizeof(RawBlock*)}};
  }
  return BlockPtrPtr::from_ptr(as_ptr() + n);
}

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::operator-(ptrdiff_t n) const {
  if (is_special()) {
    return BlockPtrPtr{repr - n * ptrdiff_t{sizeof(RawBlock*)}};
  }
  return BlockPtrPtr::from_ptr(as_ptr() - n);
}

inline ptrdiff_t operator-(Chain::BlockPtrPtr a, Chain::BlockPtrPtr b) {
  RIEGELI_ASSERT_EQ(a.is_special(), b.is_special())
      << "Incompatible BlockPtrPtr values";
  if (a.is_special()) {
    const ptrdiff_t byte_diff =
        static_cast<ptrdiff_t>(a.repr) - static_cast<ptrdiff_t>(b.repr);
    // Pointer subtraction with the element size being a power of 2 typically
    // rounds in the same way as right shift (towards -inf), not as division
    // (towards zero), so the right shift allows the compiler to eliminate the
    // is_special() check.
    switch (sizeof(Chain::RawBlock*)) {
      case 1 << 2:
        return byte_diff >> 2;
      case 1 << 3:
        return byte_diff >> 3;
      default:
        return byte_diff / ptrdiff_t{sizeof(Chain::RawBlock*)};
    }
  }
  return a.as_ptr() - b.as_ptr();
}

inline bool operator==(Chain::BlockPtrPtr a, Chain::BlockPtrPtr b) {
  return a.repr == b.repr;
}

inline bool operator!=(Chain::BlockPtrPtr a, Chain::BlockPtrPtr b) {
  return a.repr != b.repr;
}

inline bool operator<(Chain::BlockPtrPtr a, Chain::BlockPtrPtr b) {
  RIEGELI_ASSERT_EQ(a.is_special(), b.is_special())
      << "Incompatible BlockPtrPtr values";
  if (a.is_special()) {
    return a.repr < b.repr;
  }
  return a.as_ptr() < b.as_ptr();
}

inline bool operator>(Chain::BlockPtrPtr a, Chain::BlockPtrPtr b) {
  RIEGELI_ASSERT_EQ(a.is_special(), b.is_special())
      << "Incompatible BlockPtrPtr values";
  if (a.is_special()) {
    return a.repr > b.repr;
  }
  return a.as_ptr() > b.as_ptr();
}

inline bool operator<=(Chain::BlockPtrPtr a, Chain::BlockPtrPtr b) {
  RIEGELI_ASSERT_EQ(a.is_special(), b.is_special())
      << "Incompatible BlockPtrPtr values";
  if (a.is_special()) {
    return a.repr <= b.repr;
  }
  return a.as_ptr() <= b.as_ptr();
}

inline bool operator>=(Chain::BlockPtrPtr a, Chain::BlockPtrPtr b) {
  RIEGELI_ASSERT_EQ(a.is_special(), b.is_special())
      << "Incompatible BlockPtrPtr values";
  if (a.is_special()) {
    return a.repr >= b.repr;
  }
  return a.as_ptr() >= b.as_ptr();
}

inline Chain::BlockIterator::BlockIterator(const Chain* chain,
                                           size_t block_index)
    : chain_(chain),
      ptr_((ABSL_PREDICT_FALSE(chain_ == nullptr)
                ? kBeginShortData
                : chain_->begin_ == chain_->end_
                      ? kBeginShortData + (chain_->empty() ? 1 : 0)
                      : BlockPtrPtr::from_ptr(chain_->begin_)) +
           IntCast<ptrdiff_t>(block_index)) {}

inline Chain::BlockIterator::BlockIterator(const Chain* chain,
                                           BlockPtrPtr ptr) noexcept
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
  return IntCast<size_t>(
      ptr_ - (ABSL_PREDICT_FALSE(chain_ == nullptr)
                  ? kBeginShortData
                  : chain_->begin_ == chain_->end_
                        ? kBeginShortData + (chain_->empty() ? 1 : 0)
                        : BlockPtrPtr::from_ptr(chain_->begin_)));
}

inline Chain::BlockIterator::reference Chain::BlockIterator::operator*() const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::operator*(): "
         "iterator is end()";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    return chain_->short_data();
  } else {
    return (*ptr_.as_ptr())->data();
  }
}

inline Chain::BlockIterator::pointer Chain::BlockIterator::operator->() const {
  return pointer(**this);
}

inline Chain::BlockIterator& Chain::BlockIterator::operator++() {
  ptr_ = ptr_ + 1;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator++(int) {
  BlockIterator tmp = *this;
  ++*this;
  return tmp;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator--() {
  ptr_ = ptr_ - 1;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator--(int) {
  BlockIterator tmp = *this;
  --*this;
  return tmp;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator+=(
    difference_type n) {
  ptr_ = ptr_ + n;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator+(
    difference_type n) const {
  return BlockIterator(*this) += n;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator-=(
    difference_type n) {
  ptr_ = ptr_ - n;
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
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::external_object(): "
         "iterator is end()";
  if (ABSL_PREDICT_FALSE(ptr_ == kBeginShortData)) {
    return nullptr;
  } else {
    return (*ptr_.as_ptr())->checked_external_object<T>();
  }
}

inline ChainBlock Chain::BlockIterator::Pin() { return ChainBlock(PinImpl()); }

inline Chain::Blocks::Blocks(const Blocks& that) noexcept
    : chain_(that.chain_) {}

inline Chain::Blocks& Chain::Blocks::operator=(const Blocks& that) noexcept {
  chain_ = that.chain_;
  return *this;
}

inline Chain::Blocks::const_iterator Chain::Blocks::begin() const {
  return BlockIterator(
      chain_, chain_->begin_ == chain_->end_
                  ? BlockIterator::kBeginShortData + (chain_->empty() ? 1 : 0)
                  : BlockPtrPtr::from_ptr(chain_->begin_));
}

inline Chain::Blocks::const_iterator Chain::Blocks::end() const {
  return BlockIterator(chain_, chain_->begin_ == chain_->end_
                                   ? BlockIterator::kEndShortData
                                   : BlockPtrPtr::from_ptr(chain_->end_));
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

template <typename T>
inline Chain Chain::FromExternal(T&& object) {
  return Chain(ChainBlock::FromExternal(std::forward<T>(object)));
}

template <typename T>
inline Chain Chain::FromExternal(T&& object, absl::string_view data) {
  return Chain(ChainBlock::FromExternal(std::forward<T>(object), data));
}

template <typename T, typename... Args>
inline Chain Chain::FromExternal(std::tuple<Args...> args) {
  return Chain(ChainBlock::FromExternal(std::move(args)));
}

template <typename T, typename... Args>
inline Chain Chain::FromExternal(std::tuple<Args...> args,
                                 absl::string_view data) {
  return Chain(ChainBlock::FromExternal(std::move(args), data));
}

inline Chain::Chain(absl::string_view src) { Append(src, src.size()); }

inline Chain::Chain(std::string&& src) { Append(std::move(src), src.size()); }

inline Chain::Chain(const char* src) : Chain(absl::string_view(src)) {}

inline Chain::Chain(const ChainBlock& src) {
  if (src.block_ != nullptr) {
    RawBlock* const block = src.block_->Ref();
    *end_++ = block;
    size_ = block->size();
  }
}

inline Chain::Chain(ChainBlock&& src) {
  if (src.block_ != nullptr) {
    RawBlock* const block = absl::exchange(src.block_, nullptr);
    *end_++ = block;
    size_ = block->size();
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
  RawBlock** begin;
  RawBlock** end;
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

inline void Chain::Reset() { Clear(); }

inline void Chain::Reset(absl::string_view src) {
  Clear();
  Append(src, src.size());
}

inline void Chain::Reset(std::string&& src) {
  Clear();
  Append(std::move(src), src.size());
}

inline void Chain::Reset(const char* src) { Reset(absl::string_view(src)); }

inline void Chain::Reset(const ChainBlock& src) {
  Clear();
  Append(src, src.size());
}

inline void Chain::Reset(ChainBlock&& src) {
  Clear();
  Append(std::move(src), src.size());
}

inline void Chain::Clear() {
  if (begin_ != end_) ClearSlow();
  size_ = 0;
}

inline absl::string_view Chain::short_data() const {
  RIEGELI_ASSERT(begin_ == end_)
      << "Failed precondition of Chain::short_data(): blocks exist";
  return absl::string_view(block_ptrs_.short_data, size_);
}

inline void Chain::DeleteBlockPtrs() {
  if (has_allocated()) {
    std::allocator<RawBlock*>().deallocate(
        block_ptrs_.allocated.begin,
        block_ptrs_.allocated.end - block_ptrs_.allocated.begin);
  }
}

inline void Chain::UnrefBlocks() { UnrefBlocks(begin_, end_); }

inline void Chain::UnrefBlocks(RawBlock* const* begin, RawBlock* const* end) {
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

inline absl::Span<char> Chain::AppendFixedBuffer(size_t length,
                                                 size_t size_hint) {
  return AppendBuffer(length, length, length, size_hint);
}

inline absl::Span<char> Chain::PrependFixedBuffer(size_t length,
                                                  size_t size_hint) {
  return PrependBuffer(length, length, length, size_hint);
}

inline void Chain::Append(const char* src, size_t size_hint) {
  Append(absl::string_view(src), size_hint);
}

inline void Chain::Prepend(const char* src, size_t size_hint) {
  Prepend(absl::string_view(src), size_hint);
}

inline void Chain::Append(const ChainBlock& src, size_t size_hint) {
  if (src.block_ != nullptr) {
    AppendBlock<Ownership::kShare>(src.block_, size_hint);
  }
}

inline void Chain::Prepend(const ChainBlock& src, size_t size_hint) {
  if (src.block_ != nullptr) {
    PrependBlock<Ownership::kShare>(src.block_, size_hint);
  }
}

inline void Chain::Append(ChainBlock&& src, size_t size_hint) {
  if (src.block_ != nullptr) {
    AppendBlock<Ownership::kSteal>(absl::exchange(src.block_, nullptr),
                                   size_hint);
  }
}

inline void Chain::Prepend(ChainBlock&& src, size_t size_hint) {
  if (src.block_ != nullptr) {
    PrependBlock<Ownership::kSteal>(absl::exchange(src.block_, nullptr),
                                    size_hint);
  }
}

extern template void Chain::AppendBlock<Chain::Ownership::kShare>(
    RawBlock* block, size_t size_hint);
extern template void Chain::AppendBlock<Chain::Ownership::kSteal>(
    RawBlock* block, size_t size_hint);
extern template void Chain::PrependBlock<Chain::Ownership::kShare>(
    RawBlock* block, size_t size_hint);
extern template void Chain::PrependBlock<Chain::Ownership::kSteal>(
    RawBlock* block, size_t size_hint);

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
inline ChainBlock ChainBlock::FromExternal(T&& object) {
  return ChainBlock(Chain::ExternalMethodsFor<absl::decay_t<T>>::NewBlock(
      std::forward_as_tuple(std::forward<T>(object))));
}

template <typename T>
inline ChainBlock ChainBlock::FromExternal(T&& object, absl::string_view data) {
  return ChainBlock(Chain::ExternalMethodsFor<absl::decay_t<T>>::NewBlock(
      std::forward_as_tuple(std::forward<T>(object)), data));
}

template <typename T, typename... Args>
inline ChainBlock ChainBlock::FromExternal(std::tuple<Args...> args) {
  return ChainBlock(Chain::ExternalMethodsFor<T>::NewBlock(std::move(args)));
}

template <typename T, typename... Args>
inline ChainBlock ChainBlock::FromExternal(std::tuple<Args...> args,
                                           absl::string_view data) {
  return ChainBlock(
      Chain::ExternalMethodsFor<T>::NewBlock(std::move(args), data));
}

inline ChainBlock::ChainBlock(ChainBlock&& that) noexcept
    : block_(absl::exchange(that.block_, nullptr)) {}

inline ChainBlock& ChainBlock::operator=(ChainBlock&& that) noexcept {
  // Exchange that.block_ early to support self-assignment.
  RawBlock* const block = absl::exchange(that.block_, nullptr);
  if (block_ != nullptr) block_->Unref();
  block_ = block;
  return *this;
}

inline ChainBlock::ChainBlock(const ChainBlock& that) noexcept
    : block_(that.block_) {
  if (block_ != nullptr) block_->Ref();
}

inline ChainBlock& ChainBlock::operator=(const ChainBlock& that) noexcept {
  RawBlock* const block = that.block_;
  if (block != nullptr) block->Ref();
  if (block_ != nullptr) block_->Unref();
  block_ = block;
  return *this;
}

inline ChainBlock::~ChainBlock() {
  if (block_ != nullptr) block_->Unref();
}

inline void ChainBlock::Clear() {
  if (block_ != nullptr && !block_->TryClear()) {
    block_->Unref();
    block_ = nullptr;
  }
}

inline ChainBlock::operator absl::string_view() const {
  return block_ == nullptr ? absl::string_view() : block_->data();
}

inline const char* ChainBlock::data() const {
  return block_ == nullptr ? nullptr : block_->data().data();
}

inline size_t ChainBlock::size() const {
  return block_ == nullptr ? size_t{0} : block_->data().size();
}

inline bool ChainBlock::empty() const {
  return block_ == nullptr || block_->empty();
}

inline absl::Span<char> ChainBlock::AppendFixedBuffer(size_t length,
                                                      size_t size_hint) {
  return AppendBuffer(length, length, length, size_hint);
}

inline absl::Span<char> ChainBlock::PrependFixedBuffer(size_t length,
                                                       size_t size_hint) {
  return PrependBuffer(length, length, length, size_hint);
}

inline void ChainBlock::RemoveSuffix(size_t length, size_t size_hint) {
  if (length == 0) return;
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of ChainBlock::RemoveSuffix(): "
      << "length to remove greater than current size";
  if (ABSL_PREDICT_TRUE(block_->TryRemoveSuffix(length))) {
    return;
  }
  RemoveSuffixSlow(length, size_hint);
}

inline void ChainBlock::RemovePrefix(size_t length, size_t size_hint) {
  if (length == 0) return;
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of ChainBlock::RemovePrefix(): "
      << "length to remove greater than current size";
  if (ABSL_PREDICT_TRUE(block_->TryRemovePrefix(length))) {
    return;
  }
  RemovePrefixSlow(length, size_hint);
}

inline void* ChainBlock::Release() { return absl::exchange(block_, nullptr); }

inline void ChainBlock::DeleteReleased(void* ptr) {
  if (ptr != nullptr) static_cast<RawBlock*>(ptr)->Unref();
}

template <>
struct Resetter<Chain> : ResetterByReset<Chain> {};

}  // namespace riegeli

#endif  // RIEGELI_BASE_CHAIN_H_
