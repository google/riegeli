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
#include <new>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/compare.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/resetter.h"

namespace riegeli {

namespace internal {

// `Chain::Options` is defined at the namespace scope because clang has problems
// with using nested classes in `constexpr` context.
class ChainOptions {
 public:
  constexpr ChainOptions() noexcept {}

  // Expected final size, or 0 if unknown. This may improve performance and
  // memory usage.
  //
  // If the size hint turns out to not match reality, nothing breaks.
  ChainOptions& set_size_hint(size_t size_hint) & {
    size_hint_ = size_hint;
    return *this;
  }
  ChainOptions&& set_size_hint(size_t size_hint) && {
    return std::move(set_size_hint(size_hint));
  }
  size_t size_hint() const { return size_hint_; }

  // Minimal size of a block of allocated data.
  //
  // This is used initially, while the destination is small.
  //
  // Default: `kMinBufferSize` (256)
  ChainOptions& set_min_block_size(size_t min_block_size) & {
    min_block_size_ = min_block_size;
    return *this;
  }
  ChainOptions&& set_min_block_size(size_t min_block_size) && {
    return std::move(set_min_block_size(min_block_size));
  }
  size_t min_block_size() const { return min_block_size_; }

  // Maximal size of a block of allocated data.
  //
  // This does not apply to attached external objects which can be arbitrarily
  // long.
  //
  // Default: `kMaxBufferSize` (64K)
  ChainOptions& set_max_block_size(size_t max_block_size) & {
    RIEGELI_ASSERT_GT(max_block_size, 0u)
        << "Failed precondition of Chain::Options::set_max_block_size(): "
           "zero block size";
    max_block_size_ = max_block_size;
    return *this;
  }
  ChainOptions&& set_max_block_size(size_t max_block_size) && {
    return std::move(set_max_block_size(max_block_size));
  }
  size_t max_block_size() const { return max_block_size_; }

 private:
  size_t size_hint_ = 0;
  size_t min_block_size_ = kMinBufferSize;
  size_t max_block_size_ = kMaxBufferSize;
};

// `ChainBlock::Options` is defined at the namespace scope because clang has
// problems with using nested classes in `constexpr` context.
class ChainBlockOptions {
 public:
  constexpr ChainBlockOptions() noexcept {}

  // Expected final size, or 0 if unknown. This may improve performance and
  // memory usage.
  //
  // If the size hint turns out to not match reality, nothing breaks.
  ChainBlockOptions& set_size_hint(size_t size_hint) & {
    size_hint_ = size_hint;
    return *this;
  }
  ChainBlockOptions&& set_size_hint(size_t size_hint) && {
    return std::move(set_size_hint(size_hint));
  }
  size_t size_hint() const { return size_hint_; }

  // Minimal size of a block of allocated data.
  //
  // This is used initially, while the destination is small.
  //
  // Default: `kMinBufferSize` (256)
  ChainBlockOptions& set_min_block_size(size_t min_block_size) & {
    min_block_size_ = min_block_size;
    return *this;
  }
  ChainBlockOptions&& set_min_block_size(size_t min_block_size) && {
    return std::move(set_min_block_size(min_block_size));
  }
  size_t min_block_size() const { return min_block_size_; }

 private:
  size_t size_hint_ = 0;
  size_t min_block_size_ = kMinBufferSize;
};

}  // namespace internal

class ChainBlock;

// A `Chain` represents a sequence of bytes. It supports efficient appending and
// prepending, and sharing memory with other `Chain`s and other types. It does
// not support efficient random access.
//
// A `Chain` can be written using `ChainWriter` and `ChainBackwardWriter`,
// and can be read using `ChainReader`. `Chain` itself exposes lower level
// appending/prepending and iteration functions.
//
// A `Chain` is implemented with a sequence of blocks holding flat data
// fragments.
class Chain {
 public:
  using Options = internal::ChainOptions;

  static constexpr Options kDefaultOptions = Options();

  class Blocks;
  class BlockIterator;
  class CharPosition;

  // A sentinel value for the `max_length` parameter of
  // `AppendBuffer()`/`PrependBuffer()`.
  static constexpr size_t kAnyLength = std::numeric_limits<size_t>::max();

  // Given an object which owns a byte array, converts it to a `Chain` by
  // attaching the object, avoiding copying the bytes.
  //
  // If an object of type `T` is given, it is copied or moved to the `Chain`.
  //
  // If a tuple is given, an object of type `T` is constructed from elements of
  // the tuple. This avoids constructing a temporary object and moving from it.
  //
  // After the object or tuple, if the `data` parameter is given, `data` must be
  // valid for the copied, moved, or newly constructed object.
  //
  // If the `data` parameter is not given, `T` must support:
  // ```
  //   // Contents of the object.
  //   explicit operator absl::string_view() const;
  // ```
  //
  // In particular to attach static immutable memory, `T` can be
  // `absl::string_view` itself.
  //
  // `T` may also support the following member functions, either with or without
  // the `data` parameter, with the following definitions assumed by default:
  // ```
  //   // Called once before the destructor, except on a moved-from object.
  //   // If only this function is needed, `T` can be a lambda.
  //   void operator()(absl::string_view data) const {}
  //
  //   // Registers this object with MemoryEstimator.
  //   void RegisterSubobjects(absl::string_view data,
  //                           MemoryEstimator& memory_estimator) const {
  //     if (std::is_same<T, absl::string_view>::value) return;
  //     if (memory_estimator.RegisterNode(data.data())) {
  //       memory_estimator.RegisterDynamicMemory(data.size());
  //     }
  //   }
  //
  //   // Shows internal structure in a human-readable way, for debugging.
  //   void DumpStructure(absl::string_view data, std::ostream& out) const {
  //     out << "[external] { }";
  //   }
  // ```
  //
  // The `data` parameter of these member functions, if present, will get the
  // `data` used by `FromExternal()`. Having `data` available in these functions
  // might avoid storing `data` in the external object.
  template <typename T>
  static Chain FromExternal(T&& object);
  template <typename T>
  static Chain FromExternal(T&& object, absl::string_view data);
  template <typename T, typename... Args>
  static Chain FromExternal(std::tuple<Args...> args);
  template <typename T, typename... Args>
  static Chain FromExternal(std::tuple<Args...> args, absl::string_view data);

  constexpr Chain() noexcept {}

  // Converts from a string-like type.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `std::string_view`
  // (e.g. `const char*`).
  explicit Chain(absl::string_view src);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  explicit Chain(Src&& src);
  explicit Chain(const ChainBlock& src);
  explicit Chain(ChainBlock&& src);
  explicit Chain(const absl::Cord& src);
  explicit Chain(absl::Cord&& src);

  Chain(const Chain& that);
  Chain& operator=(const Chain& that);

  // The source `Chain` is left cleared.
  //
  // Moving a `Chain` invalidates its `BlockIterator`s and data pointers, but
  // the shape of blocks (their number and sizes) remains unchanged.
  Chain(Chain&& that) noexcept;
  Chain& operator=(Chain&& that) noexcept;

  ~Chain();

  // Makes `*this` equivalent to a newly constructed `Chain`. This avoids
  // constructing a temporary `Chain` and moving from it.
  void Reset();
  void Reset(absl::string_view src);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  void Reset(Src&& src);
  void Reset(const ChainBlock& src);
  void Reset(ChainBlock&& src);
  void Reset(const absl::Cord& src);
  void Reset(absl::Cord&& src);

  void Clear();

  // A container of `absl::string_view` blocks comprising data of the `Chain`.
  Blocks blocks() const;

  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  void CopyTo(char* dest) const;
  void AppendTo(std::string& dest) const&;
  void AppendTo(std::string& dest) &&;
  void AppendTo(absl::Cord& dest) const&;
  void AppendTo(absl::Cord& dest) &&;
  void PrependTo(absl::Cord& dest) const&;
  void PrependTo(absl::Cord& dest) &&;
  explicit operator std::string() const&;
  explicit operator std::string() &&;
  explicit operator absl::Cord() const&;
  explicit operator absl::Cord() &&;

  // If the `Chain` contents are flat, returns them, otherwise returns
  // `absl::nullopt`.
  absl::optional<absl::string_view> TryFlat() const;

  // Locates the block containing the given character position, and the
  // character index within the block.
  //
  // Precondition: `pos <= size()`
  CharPosition FindPosition(size_t pos) const;

  // Estimates the amount of memory used by this `Chain`.
  size_t EstimateMemory() const;
  // Registers this `Chain` with `MemoryEstimator`.
  void RegisterSubobjects(MemoryEstimator& memory_estimator) const;
  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;

  // Appends/prepends some uninitialized space. The buffer will have length at
  // least `min_length`, preferably `recommended_length`, and at most
  // `max_length`.
  //
  // If `min_length == 0`, returns whatever space was already allocated
  // (possibly an empty buffer) without invalidating existing pointers. If the
  // `Chain` was empty then the empty contents can be moved.
  //
  // If `recommended_length < min_length`, `recommended_length` is assumed to be
  // `min_length`.
  //
  // If `max_length == kAnyLength`, there is no maximum.
  //
  // Precondition: `min_length <= max_length`
  absl::Span<char> AppendBuffer(size_t min_length,
                                size_t recommended_length = 0,
                                size_t max_length = kAnyLength,
                                const Options& options = kDefaultOptions);
  absl::Span<char> PrependBuffer(size_t min_length,
                                 size_t recommended_length = 0,
                                 size_t max_length = kAnyLength,
                                 const Options& options = kDefaultOptions);

  // Equivalent to `AppendBuffer()`/`PrependBuffer()` with
  // `min_length == max_length`.
  absl::Span<char> AppendFixedBuffer(size_t length,
                                     const Options& options = kDefaultOptions);
  absl::Span<char> PrependFixedBuffer(size_t length,
                                      const Options& options = kDefaultOptions);

  // Appends/prepends a string-like type.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `std::string_view`
  // (e.g. `const char*`).
  void Append(absl::string_view src, const Options& options = kDefaultOptions);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  void Append(Src&& src, const Options& options = kDefaultOptions);
  void Append(const Chain& src, const Options& options = kDefaultOptions);
  void Append(Chain&& src, const Options& options = kDefaultOptions);
  void Append(const ChainBlock& src, const Options& options = kDefaultOptions);
  void Append(ChainBlock&& src, const Options& options = kDefaultOptions);
  void Append(const absl::Cord& src, const Options& options = kDefaultOptions);
  void Append(absl::Cord&& src, const Options& options = kDefaultOptions);
  void Prepend(absl::string_view src, const Options& options = kDefaultOptions);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  void Prepend(Src&& src, const Options& options = kDefaultOptions);
  void Prepend(const Chain& src, const Options& options = kDefaultOptions);
  void Prepend(Chain&& src, const Options& options = kDefaultOptions);
  void Prepend(const ChainBlock& src, const Options& options = kDefaultOptions);
  void Prepend(ChainBlock&& src, const Options& options = kDefaultOptions);
  void Prepend(const absl::Cord& src, const Options& options = kDefaultOptions);
  void Prepend(absl::Cord&& src, const Options& options = kDefaultOptions);

  // `AppendFrom(iter, length)` is equivalent to
  // `Append(absl::Cord::AdvanceAndRead(&iter, length))` but more efficient.
  void AppendFrom(absl::Cord::CharIterator& iter, size_t length,
                  const Options& options = kDefaultOptions);

  void RemoveSuffix(size_t length, const Options& options = kDefaultOptions);
  void RemovePrefix(size_t length, const Options& options = kDefaultOptions);

  friend void swap(Chain& a, Chain& b) noexcept;

  absl::strong_ordering Compare(absl::string_view that) const;
  absl::strong_ordering Compare(const Chain& that) const;

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

  // For testing. If `RIEGELI_DEBUG` is defined, verifies internal invariants,
  // otherwise does nothing.
  void VerifyInvariants() const;

 private:
  friend class ChainBlock;

  struct ExternalMethods;
  template <typename T>
  struct ExternalMethodsFor;
  class RawBlock;
  struct BlockPtrPtr;
  class BlockRef;
  class StringRef;
  class FlatCordRef;

  friend ptrdiff_t operator-(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator==(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator!=(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator<(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator>(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator<=(BlockPtrPtr a, BlockPtrPtr b);
  friend bool operator>=(BlockPtrPtr a, BlockPtrPtr b);

  struct Empty {};

  // A union of either a block pointer or a block offset. Having a union makes
  // easier to allocate an array containing both kinds of data, with block
  // offsets following block pointers.
  union BlockPtr {
    RawBlock* block_ptr;
    size_t block_offset;
  };

  struct Allocated {
    // The extent of the allocated array of block pointers. This array is
    // immediately followed by the array of block offsets of the same size,
    // used for efficient finding of the block covering the given position.
    // Only some middle portion of each array is filled.
    //
    // The offset of the first block is not necessarily 0 but an arbitrary value
    // (with possible wrapping around the `size_t` range), to avoid having to
    // update all offsets in `Prepend()` or `RemovePrefix()`.
    BlockPtr* begin;
    BlockPtr* end;
  };

  static constexpr size_t kMaxShortDataSize = 2 * sizeof(BlockPtr);

  union BlockPtrs {
    constexpr BlockPtrs() noexcept : empty() {}

    // If the `Chain` is empty, no block pointers are needed. Some union member
    // is needed though for the default constructor to be constexpr.
    Empty empty;
    // If `begin_ == end_`, `size_` characters.
    //
    // If also `has_here()`, then there are 0 pointers in here so `short_data`
    // can safely contain `size_` characters. If also `has_allocated()`, then
    // `size_ == 0`, and `EnsureHasHere()` must be called before writing to
    // `short_data`.
    char short_data[kMaxShortDataSize];
    // If `has_here()`, array of block pointers between `begin_` i.e. `here` and
    // `end_` (0 to 2 pointers). In this case block offsets are implicit.
    BlockPtr here[2];
    // If `has_allocated()`, pointers to a heap-allocated array of block
    // pointers and block offsets.
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

  // When deciding whether to copy an array of bytes or perform a small memory
  // allocation, prefer copying up to this length.
  static constexpr size_t kAllocationCost = 256;

  void ClearSlow();

  bool has_here() const { return begin_ == block_ptrs_.here; }
  bool has_allocated() const { return begin_ != block_ptrs_.here; }

  absl::string_view short_data() const;

  static BlockPtr* NewBlockPtrs(size_t capacity);
  void DeleteBlockPtrs();
  // If `has_allocated()`, delete the block pointer array and make `has_here()`
  // `true`. This is used before appending to `short_data`.
  //
  // Precondition: `begin_ == end_`
  void EnsureHasHere();

  void UnrefBlocks();
  static void UnrefBlocks(const BlockPtr* begin, const BlockPtr* end);
  static void UnrefBlocksSlow(const BlockPtr* begin, const BlockPtr* end);

  void DropStolenBlocks(
      std::integral_constant<Ownership, Ownership::kShare>) const;
  void DropStolenBlocks(std::integral_constant<Ownership, Ownership::kSteal>);

  // The offset of the block offsets part of the block pointer array, in array
  // elements.
  size_t block_offsets() const {
    RIEGELI_ASSERT(has_allocated())
        << "Failed precondition of block_offsets(): "
           "block pointer array is not allocated";
    return PtrDistance(block_ptrs_.allocated.begin, block_ptrs_.allocated.end);
  }

  // Returns the last block. Can be changed in place (if its own constraints
  // allow that).
  RawBlock* const& back() const { return end_[-1].block_ptr; }
  // Returns the first block. If its size changes, this must be reflected in the
  // array of block offset, e.g. with `RefreshFront()`.
  RawBlock* const& front() const { return begin_[0].block_ptr; }

  void SetBack(RawBlock* block);
  void SetFront(RawBlock* block);
  // Like `SetFront()`, but skips the `RefreshFront()` step. This is enough if
  // the block has the same size as the block being replaced.
  void SetFrontSameSize(RawBlock* block);
  // Recomputes the block offset of the first block if needed.
  void RefreshFront();
  void PushBack(RawBlock* block);
  void PushFront(RawBlock* block);
  void PopBack();
  void PopFront();
  // This template is defined and used only in chain.cc.
  template <Ownership ownership>
  void AppendBlocks(const BlockPtr* begin, const BlockPtr* end);
  // This template is defined and used only in chain.cc.
  template <Ownership ownership>
  void PrependBlocks(const BlockPtr* begin, const BlockPtr* end);
  void ReserveBack(size_t extra_capacity);
  void ReserveFront(size_t extra_capacity);

  void ReserveBackSlow(size_t extra_capacity);
  void ReserveFrontSlow(size_t extra_capacity);

  // Decides about the capacity of a new block to be appended/prepended.
  // If `replaced_length > 0`, the block will replace an existing block of that
  // size. In addition to `replaced_length`, it requires the capacity of at
  // least `min_length`, preferably `recommended_length`.
  size_t NewBlockCapacity(size_t replaced_length, size_t min_length,
                          size_t recommended_length,
                          const Options& options) const;

  // This template is defined and used only in chain.cc.
  template <Ownership ownership, typename ChainRef>
  void AppendChain(ChainRef&& src, const Options& options);
  // This template is defined and used only in chain.cc.
  template <Ownership ownership, typename ChainRef>
  void PrependChain(ChainRef&& src, const Options& options);

  // This template is explicitly instantiated.
  template <Ownership ownership>
  void AppendBlock(RawBlock* block, const Options& options);
  // This template is explicitly instantiated.
  template <Ownership ownership>
  void PrependBlock(RawBlock* block, const Options& options);

  // This template is defined and used only in chain.cc.
  template <typename CordRef>
  void AppendCord(CordRef&& src, const Options& options);
  // This template is defined and used only in chain.cc.
  template <typename CordRef>
  void PrependCord(CordRef&& src, const Options& options);

  BlockPtrs block_ptrs_;

  // The range of the block pointers array which is actually used.
  //
  // Invariants:
  //   `begin_ <= end_`
  //   if `has_here()` then `begin_ == block_ptrs_.here`
  //                    and `end_ <= block_ptrs_.here + 2`
  //   if `has_allocated()` then `begin_ >= block_ptrs_.allocated.begin`
  //                         and `end_ <= block_ptrs_.allocated.end`
  BlockPtr* begin_ = block_ptrs_.here;
  BlockPtr* end_ = block_ptrs_.here;

  // Invariants:
  //   if `begin_ == end_` then `size_ <= kMaxShortDataSize`
  //   if `begin_ == end_ && has_allocated()` then `size_ == 0`
  //   if `begin_ != end_` then
  //       `size_` is the sum of sizes of blocks in [`begin_`, `end_`)
  size_t size_ = 0;
};

// Represents either `const BlockPtr*`, or one of two special values
// (`kBeginShortData` and `kEndShortData`) behaving as if they were pointers in
// a single-element `BlockPtr` array.
struct Chain::BlockPtrPtr {
 public:
  static BlockPtrPtr from_ptr(const BlockPtr* ptr);

  bool is_special() const;
  const BlockPtr* as_ptr() const;

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

  // Returns a `ChainBlock` which pins the block pointed to by this iterator,
  // keeping it alive and unchanged, until either the `ChainBlock` is destroyed
  // or `ChainBlock::Release()` and `ChainBlock::DeleteReleased()` are called.
  //
  // Warning: the data pointer of the returned `ChainBlock` is not necessarily
  // the same as the data pointer of this `BlockIterator` (because of short
  // `Chain` optimization). Convert the `ChainBlock` to `absl::string_view` or
  // use `ChainBlock::data()` for a data pointer valid for the pinned block.
  //
  // Precondition: this is not past the end iterator.
  ChainBlock Pin();

  // Returns a pointer to the external object if this points to an external
  // block holding an object of type `T`, otherwise returns `nullptr`.
  //
  // Precondition: this is not past the end iterator.
  template <typename T>
  const T* external_object() const;

  // Appends `**this` to `dest`.
  //
  // Precondition: this is not past the end iterator.
  void AppendTo(Chain& dest, const Options& options = kDefaultOptions) const;
  void AppendTo(absl::Cord& dest) const;

  // Appends `substr` to `dest`. `substr` must be empty or contained in
  // `**this`.
  //
  // Precondition:
  //   if `substr` is not empty then this is not past the end iterator.
  void AppendSubstrTo(absl::string_view substr, Chain& dest,
                      const Options& options = kDefaultOptions) const;
  void AppendSubstrTo(absl::string_view substr, absl::Cord& dest) const;

  // Prepends `**this` to `dest`.
  //
  // Precondition: this is not past the end iterator.
  void PrependTo(absl::Cord& dest) const;

 private:
  friend class Chain;

  static constexpr BlockPtrPtr kBeginShortData{0};
  static constexpr BlockPtrPtr kEndShortData{sizeof(BlockPtr)};

  explicit BlockIterator(const Chain* chain, BlockPtrPtr ptr) noexcept;

  RawBlock* PinImpl();

  const Chain* chain_ = nullptr;
  // If `chain_ == nullptr`, `kBeginShortData`.
  // If `*chain_` has no block pointers and no short data, `kEndShortData`.
  // If `*chain_` has short data, `kBeginShortData` or `kEndShortData`.
  // If `*chain_` has block pointers, a pointer to the block pointer array.
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

// Represents the position of a character in a `Chain`.
//
// A `CharIterator` is not provided because it is more efficient to iterate by
// blocks and process character ranges within a block.
struct Chain::CharPosition {
  // Intended invariant:
  //   if `block_iter == block_iter.chain()->blocks().cend()`
  //       then `char_index == 0`
  //       else `char_index < block_iter->size()`
  BlockIterator block_iter;
  size_t char_index;
};

// A simplified variant of `Chain` constrained to have at most one block.
//
// `ChainBlock` uses the same block representation as `Chain` and thus can be
// efficiently appended to a `Chain`.
//
// `ChainBlock` uses no short data optimization.
class ChainBlock {
 public:
  using Options = internal::ChainBlockOptions;

  static constexpr Options kDefaultOptions = Options();

  // Maximum size of a `ChainBlock`.
  static constexpr size_t kMaxSize =
      size_t{std::numeric_limits<ptrdiff_t>::max()};

  // A sentinel value for the `max_length` parameter of
  // `AppendBuffer()`/`PrependBuffer()`.
  static constexpr size_t kAnyLength = Chain::kAnyLength;

  // Given an object which owns a byte array, converts it to a `ChainBlock` by
  // attaching the object, avoiding copying the bytes.
  //
  // See `Chain::FromExternal()` for details.
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

  // The source `ChainBlock` is left cleared.
  //
  // Moving a `ChainBlock` keeps its data pointers unchanged.
  ChainBlock(ChainBlock&& that) noexcept;
  ChainBlock& operator=(ChainBlock&& that) noexcept;

  ~ChainBlock();

  void Clear();

  explicit operator absl::string_view() const;
  const char* data() const;
  size_t size() const;
  bool empty() const;

  // Estimates the amount of memory used by this `ChainBlock`.
  size_t EstimateMemory() const;
  // Registers this `ChainBlock` with `MemoryEstimator`.
  void RegisterSubobjects(MemoryEstimator& memory_estimator) const;
  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;

  // Appends/prepends some uninitialized space. The buffer will have length at
  // least `min_length`, preferably `recommended_length`, and at most
  // `max_length`.
  //
  // If `min_length == 0`, returns whatever space was already allocated
  // (possibly an empty buffer). without invalidating existing pointers. If the
  // `ChainBlock` was empty then the empty contents can be moved.
  //
  // If `recommended_length < min_length`, `recommended_length` is assumed to be
  // `min_length`.
  //
  // If `max_length == kAnyLength`, there is no maximum.
  //
  // Precondition: `min_length <= max_length`
  absl::Span<char> AppendBuffer(size_t min_length,
                                size_t recommended_length = 0,
                                size_t max_length = kAnyLength,
                                const Options& options = kDefaultOptions);
  absl::Span<char> PrependBuffer(size_t min_length,
                                 size_t recommended_length = 0,
                                 size_t max_length = kAnyLength,
                                 const Options& options = kDefaultOptions);

  // Equivalent to `AppendBuffer()`/`PrependBuffer()` with
  // `min_length == max_length`.
  absl::Span<char> AppendFixedBuffer(size_t length,
                                     const Options& options = kDefaultOptions);
  absl::Span<char> PrependFixedBuffer(size_t length,
                                      const Options& options = kDefaultOptions);

  void RemoveSuffix(size_t length, const Options& options = kDefaultOptions);
  void RemovePrefix(size_t length, const Options& options = kDefaultOptions);

  // Appends `*this` to `dest`.
  void AppendTo(Chain& dest,
                const Chain::Options& options = Chain::kDefaultOptions) const;
  void AppendTo(absl::Cord& dest) const;

  // Appends `substr` to `dest`. `substr` must be empty or contained in `*this`.
  void AppendSubstrTo(
      absl::string_view substr, Chain& dest,
      const Chain::Options& options = Chain::kDefaultOptions) const;
  void AppendSubstrTo(absl::string_view substr, absl::Cord& dest) const;

  // Releases the ownership of the block, which must be deleted using
  // `DeleteReleased()` if not `nullptr`.
  void* Release();

  // Deletes the pointer obtained by `Release()`.
  static void DeleteReleased(void* ptr);

 private:
  friend class Chain;

  using RawBlock = Chain::RawBlock;

  explicit ChainBlock(RawBlock* block) : block_(block) {}

  // Decides about the capacity of a new block to be appended/prepended.
  size_t NewBlockCapacity(size_t old_size, size_t min_length,
                          size_t recommended_length,
                          const Options& options) const;

  void RemoveSuffixSlow(size_t length, const Options& options);
  void RemovePrefixSlow(size_t length, const Options& options);

  RawBlock* block_ = nullptr;
};

// Implementation details follow.

// `Chain` representation consists of blocks.
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
//  - tiny block: a block with size < `kMinBufferSize`
//  - wasteful block: a block with free space > max(size, `kMinBufferSize`)
//
// Invariants of a `Chain`:
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
  // `SizeReturningNewAligned()`.
  explicit RawBlock(const size_t* raw_capacity);

  // Constructs an external block containing an external object constructed from
  // args, and sets block data to `absl::string_view(object)`. This constructor
  // is public for `NewAligned()`.
  template <typename T, typename... Args>
  explicit RawBlock(ExternalType<T>, std::tuple<Args...> args);

  // Constructs an external block containing an external object constructed from
  // args, and sets block data to the data parameter. This constructor is public
  // for `NewAligned()`.
  template <typename T, typename... Args>
  explicit RawBlock(ExternalType<T>, std::tuple<Args...> args,
                    absl::string_view data);

  template <Ownership ownership = Ownership::kShare>
  RawBlock* Ref();

  template <Ownership ownership = Ownership::kSteal>
  void Unref();

  // This template is defined and used only in chain.cc.
  template <Ownership ownership>
  RawBlock* Copy();

  bool TryClear();

  explicit operator absl::string_view() const { return data_; }
  size_t size() const { return data_.size(); }
  bool empty() const { return data_.empty(); }
  const char* data_begin() const { return data_.data(); }
  const char* data_end() const { return data_begin() + size(); }

  // Returns a pointer to the external object, assuming that this is an external
  // block holding an object of type `T`.
  template <typename T>
  T* unchecked_external_object();
  template <typename T>
  const T* unchecked_external_object() const;

  // Returns a pointer to the external object if this is an external block
  // holding an object of type `T`, otherwise returns `nullptr`.
  template <typename T>
  const T* checked_external_object() const;

  // Returns a pointer to the external object if this is an external block
  // holding an object of type `T` and the block has a unique owner, otherwise
  // returns `nullptr`.
  template <typename T>
  T* checked_external_object_with_unique_owner();

  bool tiny(size_t extra_size = 0) const;
  bool wasteful(size_t extra_size = 0) const;

  // Registers this `RawBlock` with `MemoryEstimator`.
  void RegisterShared(MemoryEstimator& memory_estimator) const;
  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;

  bool can_append(size_t length) const;
  bool can_prepend(size_t length) const;
  bool CanAppendMovingData(size_t length, size_t* space_before_if_not);
  bool CanPrependMovingData(size_t length, size_t* space_after_if_not);
  absl::Span<char> AppendBuffer(size_t max_length);
  absl::Span<char> PrependBuffer(size_t max_length);
  void Append(absl::string_view src, size_t space_before = 0);
  // Reads `size_to_copy` from `src.data()` but accounts for `src.size()`.
  // Faster than `Append()` if `size_to_copy` is a compile time constant, but
  // requires `size_to_copy` bytes to be readable, possibly past the end of src.
  //
  // Precondition: `size_to_copy >= src.size()`
  void AppendWithExplicitSizeToCopy(absl::string_view src, size_t size_to_copy);
  void Prepend(absl::string_view src, size_t space_after = 0);
  bool TryRemoveSuffix(size_t length);
  bool TryRemovePrefix(size_t length);

  void AppendTo(Chain& dest, const Options& options);
  // This template is defined and used only in chain.cc.
  template <Ownership ownership>
  void AppendTo(absl::Cord& dest);

  void AppendSubstrTo(absl::string_view substr, Chain& dest,
                      const Options& options);
  void AppendSubstrTo(absl::string_view substr, absl::Cord& dest);

  // This template is defined and used only in chain.cc.
  template <Ownership ownership>
  void PrependTo(absl::Cord& dest);

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
  void ConstructExternal(std::tuple<Args...>&& args,
                         std::index_sequence<Indices...>);

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
  // If `is_internal()`, end of allocated space. If `is_external()`, `nullptr`.
  // This distinguishes internal from external blocks.
  char* allocated_end_ = nullptr;
  union {
    // If `is_internal()`, beginning of data (actual allocated size is larger).
    char allocated_begin_[1];
    // If `is_external()`, the remaining fields.
    External external_;
  };
};

struct Chain::ExternalMethods {
  void (*delete_block)(RawBlock* block);
  void (*register_unique)(const RawBlock* block,
                          MemoryEstimator& memory_estimator);
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

template <typename T,
          absl::enable_if_t<HasCallOperatorWithData<T>::value, int> = 0>
inline void CallOperator(T* object, absl::string_view data) {
  (*object)(data);
}

template <typename T,
          absl::enable_if_t<!HasCallOperatorWithData<T>::value &&
                                HasCallOperatorWithoutData<T>::value,
                            int> = 0>
inline void CallOperator(T* object, absl::string_view data) {
  (*object)();
}

template <typename T,
          absl::enable_if_t<!HasCallOperatorWithData<T>::value &&
                                !HasCallOperatorWithoutData<T>::value,
                            int> = 0>
inline void CallOperator(T* object, absl::string_view data) {}

template <typename T, typename Enable = void>
struct HasRegisterSubobjectsWithData : public std::false_type {};

template <typename T>
struct HasRegisterSubobjectsWithData<
    T,
    absl::void_t<decltype(std::declval<T>().RegisterSubobjects(
        std::declval<absl::string_view>(), std::declval<MemoryEstimator&>()))>>
    : public std::true_type {};

template <typename T, typename Enable = void>
struct HasRegisterSubobjectsWithoutData : public std::false_type {};

template <typename T>
struct HasRegisterSubobjectsWithoutData<
    T, absl::void_t<decltype(std::declval<T>().RegisterSubobjects(
           std::declval<MemoryEstimator&>()))>> : public std::true_type {};

template <typename T,
          absl::enable_if_t<HasRegisterSubobjectsWithData<T>::value, int> = 0>
inline void RegisterSubobjects(T* object, absl::string_view data,
                               MemoryEstimator& memory_estimator) {
  object->RegisterSubobjects(data, memory_estimator);
}

template <typename T,
          absl::enable_if_t<!HasRegisterSubobjectsWithData<T>::value &&
                                HasRegisterSubobjectsWithoutData<T>::value,
                            int> = 0>
inline void RegisterSubobjects(T* object, absl::string_view data,
                               MemoryEstimator& memory_estimator) {
  object->RegisterSubobjects(memory_estimator);
}

template <typename T,
          absl::enable_if_t<!HasRegisterSubobjectsWithData<T>::value &&
                                !HasRegisterSubobjectsWithoutData<T>::value,
                            int> = 0>
inline void RegisterSubobjects(T* object, absl::string_view data,
                               MemoryEstimator& memory_estimator) {
  if (memory_estimator.RegisterNode(data.data())) {
    memory_estimator.RegisterDynamicMemory(data.size());
  }
}

template <>
inline void RegisterSubobjects(absl::string_view* object,
                               absl::string_view data,
                               MemoryEstimator& memory_estimator) {}

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

template <typename T,
          std::enable_if_t<HasDumpStructureWithData<T>::value, int> = 0>
inline void DumpStructure(T* object, absl::string_view data,
                          std::ostream& out) {
  object->DumpStructure(data, out);
}

template <typename T,
          std::enable_if_t<!HasDumpStructureWithData<T>::value &&
                               HasDumpStructureWithoutData<T>::value,
                           int> = 0>
inline void DumpStructure(T* object, absl::string_view data,
                          std::ostream& out) {
  object->DumpStructure(out);
}

template <typename T,
          std::enable_if_t<!HasDumpStructureWithData<T>::value &&
                               !HasDumpStructureWithoutData<T>::value,
                           int> = 0>
inline void DumpStructure(T* object, absl::string_view data,
                          std::ostream& out) {
  out << "[external] { }";
}

}  // namespace internal

template <typename T>
struct Chain::ExternalMethodsFor {
  // Creates an external block containing an external object constructed from
  // `args`, and sets block data to `absl::string_view(object)`.
  template <typename... Args>
  static RawBlock* NewBlock(std::tuple<Args...> args);

  // Creates an external block containing an external object constructed from
  // `args`, and sets block data to the `data` parameter.
  template <typename... Args>
  static RawBlock* NewBlock(std::tuple<Args...> args, absl::string_view data);

  static const Chain::ExternalMethods methods;

 private:
  static void DeleteBlock(RawBlock* block);
  static void RegisterUnique(const RawBlock* block,
                             MemoryEstimator& memory_estimator);
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
  internal::CallOperator(block->unchecked_external_object<T>(),
                         absl::string_view(*block));
  block->unchecked_external_object<T>()->~T();
  DeleteAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
      block, RawBlock::kExternalObjectOffset<T>() + sizeof(T));
}

template <typename T>
void Chain::ExternalMethodsFor<T>::RegisterUnique(
    const RawBlock* block, MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterDynamicMemory(RawBlock::kExternalObjectOffset<T>() +
                                         sizeof(T));
  internal::RegisterSubobjects(block->unchecked_external_object<T>(),
                               absl::string_view(*block), memory_estimator);
}

template <typename T>
void Chain::ExternalMethodsFor<T>::DumpStructure(const RawBlock* block,
                                                 std::ostream& out) {
  internal::DumpStructure(block->unchecked_external_object<T>(),
                          absl::string_view(*block), out);
}

template <typename T, typename... Args>
inline Chain::RawBlock::RawBlock(ExternalType<T>, std::tuple<Args...> args) {
  ConstructExternal<T>(std::move(args), std::index_sequence_for<Args...>());
  data_ = absl::string_view(*unchecked_external_object<T>());
  RIEGELI_ASSERT(is_external()) << "A RawBlock with allocated_end_ == nullptr "
                                   "should be considered external";
}

template <typename T, typename... Args>
inline Chain::RawBlock::RawBlock(ExternalType<T>, std::tuple<Args...> args,
                                 absl::string_view data)
    : data_(data) {
  ConstructExternal<T>(std::move(args), std::index_sequence_for<Args...>());
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
    ABSL_ATTRIBUTE_UNUSED std::tuple<Args...>&& args,
    std::index_sequence<Indices...>) {
  external_.methods = &ExternalMethodsFor<T>::methods;
  new (unchecked_external_object<T>())
      T(std::forward<Args>(std::get<Indices>(args))...);
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

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::from_ptr(const BlockPtr* ptr) {
  return BlockPtrPtr{reinterpret_cast<uintptr_t>(ptr)};
}

inline bool Chain::BlockPtrPtr::is_special() const {
  return repr <= sizeof(BlockPtr);
}

inline const Chain::BlockPtr* Chain::BlockPtrPtr::as_ptr() const {
  RIEGELI_ASSERT(!is_special()) << "Unexpected special BlockPtrPtr value";
  return reinterpret_cast<const BlockPtr*>(repr);
}

// Code conditional on `is_special()` is written such that both branches
// typically compile to the same code, allowing the compiler eliminate the
// `is_special()` checks.

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::operator+(ptrdiff_t n) const {
  if (is_special()) {
    return BlockPtrPtr{IntCast<uintptr_t>(IntCast<ptrdiff_t>(repr) +
                                          n * ptrdiff_t{sizeof(RawBlock*)})};
  }
  return BlockPtrPtr::from_ptr(as_ptr() + n);
}

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::operator-(ptrdiff_t n) const {
  if (is_special()) {
    return BlockPtrPtr{IntCast<uintptr_t>(IntCast<ptrdiff_t>(repr) -
                                          n * ptrdiff_t{sizeof(RawBlock*)})};
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
    // `is_special()` check.
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
    return absl::string_view(*ptr_.as_ptr()->block_ptr);
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
    return ptr_.as_ptr()->block_ptr->checked_external_object<T>();
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
    return absl::string_view(*chain_->begin_[n].block_ptr);
  }
}

inline Chain::Blocks::const_reference Chain::Blocks::at(size_type n) const {
  RIEGELI_CHECK_LT(n, size()) << "Failed precondition of Chain::Blocks::at(): "
                                 "block index out of range";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->begin_[n].block_ptr);
  }
}

inline Chain::Blocks::const_reference Chain::Blocks::front() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::front(): no blocks";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->begin_[0].block_ptr);
  }
}

inline Chain::Blocks::const_reference Chain::Blocks::back() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::back(): no blocks";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->end_[-1].block_ptr);
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

// In converting constructors below, `set_size_hint(src.size())` optimizes
// for the case when the resulting `Chain` will not be appended to further,
// reducing the size of allocations.

inline Chain::Chain(absl::string_view src) {
  Append(src, Options().set_size_hint(src.size()));
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline Chain::Chain(Src&& src) {
  const size_t size = src.size();
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  Append(std::move(src), Options().set_size_hint(size));
}

inline Chain::Chain(const ChainBlock& src) {
  if (src.block_ != nullptr) {
    RawBlock* const block = src.block_->Ref();
    (end_++)->block_ptr = block;
    size_ = block->size();
  }
}

inline Chain::Chain(ChainBlock&& src) {
  if (src.block_ != nullptr) {
    RawBlock* const block = std::exchange(src.block_, nullptr);
    (end_++)->block_ptr = block;
    size_ = block->size();
  }
}

inline Chain::Chain(const absl::Cord& src) {
  Append(src, Options().set_size_hint(src.size()));
}

inline Chain::Chain(absl::Cord&& src) {
  const size_t size = src.size();
  Append(std::move(src), Options().set_size_hint(size));
}

inline Chain::Chain(Chain&& that) noexcept
    : size_(std::exchange(that.size_, 0)) {
  // Use `std::memcpy()` instead of copy constructor to silence
  // `-Wmaybe-uninitialized` in gcc.
  std::memcpy(&block_ptrs_, &that.block_ptrs_, sizeof(BlockPtrs));
  if (that.has_here()) {
    // `that.has_here()` implies that `that.begin_ == that.block_ptrs_.here`
    // already.
    begin_ = block_ptrs_.here;
    end_ = block_ptrs_.here + (std::exchange(that.end_, that.block_ptrs_.here) -
                               that.block_ptrs_.here);
  } else {
    begin_ = std::exchange(that.begin_, that.block_ptrs_.here);
    end_ = std::exchange(that.end_, that.block_ptrs_.here);
  }
  // It does not matter what is left in `that.block_ptrs_` because `that.begin_`
  // and `that.end_` point to the empty prefix of `that.block_ptrs_.here[]`.
}

inline Chain& Chain::operator=(Chain&& that) noexcept {
  // Exchange `that.begin_` and `that.end_` early to support self-assignment.
  BlockPtr* begin;
  BlockPtr* end;
  if (that.has_here()) {
    // `that.has_here()` implies that `that.begin_ == that.block_ptrs_.here`
    // already.
    begin = block_ptrs_.here;
    end = block_ptrs_.here + (std::exchange(that.end_, that.block_ptrs_.here) -
                              that.block_ptrs_.here);
  } else {
    begin = std::exchange(that.begin_, that.block_ptrs_.here);
    end = std::exchange(that.end_, that.block_ptrs_.here);
  }
  UnrefBlocks();
  DeleteBlockPtrs();
  // It does not matter what is left in `that.block_ptrs_` because `that.begin_`
  // and `that.end_` point to the empty prefix of `that.block_ptrs_.here[]`. Use
  // `std::memcpy()` instead of assignment to silence `-Wmaybe-uninitialized` in
  // gcc.
  std::memcpy(&block_ptrs_, &that.block_ptrs_, sizeof(BlockPtrs));
  begin_ = begin;
  end_ = end;
  size_ = std::exchange(that.size_, 0);
  return *this;
}

inline Chain::~Chain() {
  UnrefBlocks();
  DeleteBlockPtrs();
}

inline void Chain::Reset() { Clear(); }

inline void Chain::Reset(absl::string_view src) {
  Clear();
  Append(src, Options().set_size_hint(src.size()));
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline void Chain::Reset(Src&& src) {
  Clear();
  const size_t size = src.size();
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  Append(std::move(src), Options().set_size_hint(size));
}

inline void Chain::Reset(const ChainBlock& src) {
  Clear();
  Append(src, Options().set_size_hint(src.size()));
}

inline void Chain::Reset(ChainBlock&& src) {
  Clear();
  Append(std::move(src), Options().set_size_hint(src.size()));
}

inline void Chain::Reset(const absl::Cord& src) {
  Clear();
  Append(src, Options().set_size_hint(src.size()));
}

inline void Chain::Reset(absl::Cord&& src) {
  Clear();
  const size_t size = src.size();
  Append(std::move(src), Options().set_size_hint(size));
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
    std::allocator<BlockPtr>().deallocate(
        block_ptrs_.allocated.begin,
        2 * PtrDistance(block_ptrs_.allocated.begin,
                        block_ptrs_.allocated.end));
  }
}

inline void Chain::UnrefBlocks() { UnrefBlocks(begin_, end_); }

inline void Chain::UnrefBlocks(const BlockPtr* begin, const BlockPtr* end) {
  if (begin != end) UnrefBlocksSlow(begin, end);
}

inline void Chain::SetBack(RawBlock* block) {
  end_[-1].block_ptr = block;
  // There is no need to adjust block offsets because the size of the last block
  // is not reflected in block offsets.
}

inline void Chain::SetFront(RawBlock* block) {
  SetFrontSameSize(block);
  RefreshFront();
}

inline void Chain::SetFrontSameSize(RawBlock* block) {
  begin_[0].block_ptr = block;
}

inline void Chain::RefreshFront() {
  if (has_allocated()) {
    begin_[block_offsets()].block_offset =
        begin_ + 1 == end_ ? size_t{0}
                           : begin_[block_offsets() + 1].block_offset -
                                 begin_[0].block_ptr->size();
  }
}

inline Chain::Blocks Chain::blocks() const { return Blocks(this); }

inline absl::optional<absl::string_view> Chain::TryFlat() const {
  switch (end_ - begin_) {
    case 0:
      return short_data();
    case 1:
      return absl::string_view(*front());
    default:
      return absl::nullopt;
  }
}

inline absl::Span<char> Chain::AppendFixedBuffer(size_t length,
                                                 const Options& options) {
  return AppendBuffer(length, length, length, options);
}

inline absl::Span<char> Chain::PrependFixedBuffer(size_t length,
                                                  const Options& options) {
  return PrependBuffer(length, length, length, options);
}

extern template void Chain::Append(std::string&& src, const Options& options);
extern template void Chain::Prepend(std::string&& src, const Options& options);

inline void Chain::Append(const ChainBlock& src, const Options& options) {
  if (src.block_ != nullptr) {
    AppendBlock<Ownership::kShare>(src.block_, options);
  }
}

inline void Chain::Prepend(const ChainBlock& src, const Options& options) {
  if (src.block_ != nullptr) {
    PrependBlock<Ownership::kShare>(src.block_, options);
  }
}

inline void Chain::Append(ChainBlock&& src, const Options& options) {
  if (src.block_ != nullptr) {
    AppendBlock<Ownership::kSteal>(std::exchange(src.block_, nullptr), options);
  }
}

inline void Chain::Prepend(ChainBlock&& src, const Options& options) {
  if (src.block_ != nullptr) {
    PrependBlock<Ownership::kSteal>(std::exchange(src.block_, nullptr),
                                    options);
  }
}

extern template void Chain::AppendBlock<Chain::Ownership::kShare>(
    RawBlock* block, const Options& options);
extern template void Chain::AppendBlock<Chain::Ownership::kSteal>(
    RawBlock* block, const Options& options);
extern template void Chain::PrependBlock<Chain::Ownership::kShare>(
    RawBlock* block, const Options& options);
extern template void Chain::PrependBlock<Chain::Ownership::kSteal>(
    RawBlock* block, const Options& options);

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
  return ChainBlock(Chain::ExternalMethodsFor<std::decay_t<T>>::NewBlock(
      std::forward_as_tuple(std::forward<T>(object))));
}

template <typename T>
inline ChainBlock ChainBlock::FromExternal(T&& object, absl::string_view data) {
  return ChainBlock(Chain::ExternalMethodsFor<std::decay_t<T>>::NewBlock(
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
    : block_(std::exchange(that.block_, nullptr)) {}

inline ChainBlock& ChainBlock::operator=(ChainBlock&& that) noexcept {
  // Exchange `that.block_` early to support self-assignment.
  RawBlock* const block = std::exchange(that.block_, nullptr);
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
  return block_ == nullptr ? absl::string_view() : absl::string_view(*block_);
}

inline const char* ChainBlock::data() const {
  return block_ == nullptr ? nullptr : block_->data_begin();
}

inline size_t ChainBlock::size() const {
  return block_ == nullptr ? size_t{0} : block_->size();
}

inline bool ChainBlock::empty() const {
  return block_ == nullptr || block_->empty();
}

inline absl::Span<char> ChainBlock::AppendFixedBuffer(size_t length,
                                                      const Options& options) {
  return AppendBuffer(length, length, length, options);
}

inline absl::Span<char> ChainBlock::PrependFixedBuffer(size_t length,
                                                       const Options& options) {
  return PrependBuffer(length, length, length, options);
}

inline void ChainBlock::RemoveSuffix(size_t length, const Options& options) {
  if (length == 0) return;
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of ChainBlock::RemoveSuffix(): "
      << "length to remove greater than current size";
  if (ABSL_PREDICT_TRUE(block_->TryRemoveSuffix(length))) {
    return;
  }
  RemoveSuffixSlow(length, options);
}

inline void ChainBlock::RemovePrefix(size_t length, const Options& options) {
  if (length == 0) return;
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of ChainBlock::RemovePrefix(): "
      << "length to remove greater than current size";
  if (ABSL_PREDICT_TRUE(block_->TryRemovePrefix(length))) {
    return;
  }
  RemovePrefixSlow(length, options);
}

inline void* ChainBlock::Release() { return std::exchange(block_, nullptr); }

inline void ChainBlock::DeleteReleased(void* ptr) {
  if (ptr != nullptr) static_cast<RawBlock*>(ptr)->Unref();
}

template <>
struct Resetter<Chain> : ResetterByReset<Chain> {};

}  // namespace riegeli

#endif  // RIEGELI_BASE_CHAIN_H_
