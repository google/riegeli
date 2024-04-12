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

#include <cstring>
#include <iosfwd>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/intrusive_ref_count.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/new_aligned.h"
#include "riegeli/base/sized_shared_buffer.h"

namespace riegeli {

namespace chain_internal {

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
  // Default: `kDefaultMinBlockSize` (256).
  ChainOptions& set_min_block_size(size_t min_block_size) & {
    min_block_size_ = UnsignedMin(min_block_size, uint32_t{1} << 31);
    return *this;
  }
  ChainOptions&& set_min_block_size(size_t min_block_size) && {
    return std::move(set_min_block_size(min_block_size));
  }
  size_t min_block_size() const { return min_block_size_; }

  // Maximal size of a block of allocated data.
  //
  // This is for performance tuning, not a guarantee: does not apply to objects
  // allocated separately and then appended to this `Chain`.
  //
  // Default: `kDefaultMaxBlockSize` (64K).
  ChainOptions& set_max_block_size(size_t max_block_size) & {
    RIEGELI_ASSERT_GT(max_block_size, 0u)
        << "Failed precondition of Chain::Options::set_max_block_size(): "
           "zero block size";
    max_block_size_ = UnsignedMin(max_block_size, uint32_t{1} << 31);
    return *this;
  }
  ChainOptions&& set_max_block_size(size_t max_block_size) && {
    return std::move(set_max_block_size(max_block_size));
  }
  size_t max_block_size() const { return max_block_size_; }

  // A shortcut for `set_min_block_size(block_size)` with
  // `set_max_block_size(block_size)`.
  ChainOptions& set_block_size(size_t block_size) & {
    return set_min_block_size(block_size).set_max_block_size(block_size);
  }
  ChainOptions&& set_block_size(size_t block_size) && {
    return std::move(set_block_size(block_size));
  }

 private:
  size_t size_hint_ = 0;
  // Use `uint32_t` instead of `size_t` to reduce the object size.
  uint32_t min_block_size_ = uint32_t{kDefaultMinBlockSize};
  uint32_t max_block_size_ = uint32_t{kDefaultMaxBlockSize};
};

}  // namespace chain_internal

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
class Chain : public WithCompare<Chain> {
 public:
  using Options = chain_internal::ChainOptions;

  static constexpr Options kDefaultOptions = Options();

  class Blocks;
  class PinnedBlock;
  class BlockIterator;
  struct BlockAndChar;

  // A sentinel value for the `max_length` parameter of
  // `AppendBuffer()`/`PrependBuffer()`.
  static constexpr size_t kAnyLength = std::numeric_limits<size_t>::max();

  // Given an object which owns a byte array, converts it to a `Chain` by
  // attaching the object, avoiding copying the bytes.
  //
  // `object` supports `riegeli::Maker<T>(args...)` to construct `T` in-place.
  //
  // If the `data` parameter is given, `data` must be valid for the new object.
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
  //   // Shows internal structure in a human-readable way, for debugging.
  //   void DumpStructure(absl::string_view data, std::ostream& out) const {
  //     out << "[external] { }";
  //   }
  //
  //   // Registers this object with `MemoryEstimator`.
  //   //
  //   // By default calls `memory_estimator.RegisterUnknownType<T>()` and
  //   // as an approximation of memory usage of an unknown type, registers just
  //   // the stored `data` if unique.
  //   friend void RiegeliRegisterSubobjects(
  //       const T* self, riegeli::MemoryEstimator& memory_estimator);
  // ```
  //
  // The `data` parameter of these member functions, if present, will get the
  // `data` used by `FromExternal()`. Having `data` available in these functions
  // might avoid storing `data` in the external object.
  template <typename T, std::enable_if_t<
                            std::is_constructible<absl::string_view,
                                                  InitializerTargetT<T>>::value,
                            int> = 0>
  static Chain FromExternal(T&& object);
  template <typename T>
  static Chain FromExternal(T&& object, absl::string_view data);

  // Allocated size of an external block containing an external object of type
  // `T`.
  template <typename T>
  static constexpr size_t kExternalAllocatedSize();

  constexpr Chain() = default;

  // Converts from a string-like type.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  explicit Chain(absl::string_view src);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  explicit Chain(Src&& src);
  explicit Chain(const absl::Cord& src);
  explicit Chain(absl::Cord&& src);
  explicit Chain(const SizedSharedBuffer& src);
  explicit Chain(SizedSharedBuffer&& src);

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
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::string_view src);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const absl::Cord& src);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::Cord&& src);

  // Removes all data.
  ABSL_ATTRIBUTE_REINITIALIZES void Clear();

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

  // If the `Chain` contents are not flat, flattens them in place. Returns flat
  // contents.
  absl::string_view Flatten();

  // Locates the block containing the given character position, and the
  // character index within the block.
  //
  // The opposite conversion is `Chain::BlockIterator::CharIndexInChain()`.
  //
  // Precondition: `char_index_in_chain <= size()`
  BlockAndChar BlockAndCharIndex(size_t char_index_in_chain) const;

  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;
  // Estimates the amount of memory used by this `Chain`.
  size_t EstimateMemory() const;
  // Registers this `Chain` with `MemoryEstimator`.
  friend void RiegeliRegisterSubobjects(const Chain* self,
                                        MemoryEstimator& memory_estimator) {
    self->RegisterSubobjectsImpl(memory_estimator);
  }

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
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  void Append(absl::string_view src, const Options& options = kDefaultOptions);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  void Append(Src&& src, const Options& options = kDefaultOptions);
  void Append(const Chain& src, const Options& options = kDefaultOptions);
  void Append(Chain&& src, const Options& options = kDefaultOptions);
  void Append(const absl::Cord& src, const Options& options = kDefaultOptions);
  void Append(absl::Cord&& src, const Options& options = kDefaultOptions);
  void Append(const SizedSharedBuffer& src,
              const Options& options = kDefaultOptions);
  void Append(SizedSharedBuffer&& src,
              const Options& options = kDefaultOptions);
  void Prepend(absl::string_view src, const Options& options = kDefaultOptions);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  void Prepend(Src&& src, const Options& options = kDefaultOptions);
  void Prepend(const Chain& src, const Options& options = kDefaultOptions);
  void Prepend(Chain&& src, const Options& options = kDefaultOptions);
  void Prepend(const absl::Cord& src, const Options& options = kDefaultOptions);
  void Prepend(absl::Cord&& src, const Options& options = kDefaultOptions);
  void Prepend(const SizedSharedBuffer& src,
               const Options& options = kDefaultOptions);
  void Prepend(SizedSharedBuffer&& src,
               const Options& options = kDefaultOptions);

  // `AppendFrom(iter, length)` is equivalent to
  // `Append(absl::Cord::AdvanceAndRead(&iter, length))` but more efficient.
  void AppendFrom(absl::Cord::CharIterator& iter, size_t length,
                  const Options& options = kDefaultOptions);

  // Removes suffix/prefix of the given length.
  //
  // Precondition: `length <= size()`
  void RemoveSuffix(size_t length, const Options& options = kDefaultOptions);
  void RemovePrefix(size_t length, const Options& options = kDefaultOptions);

  friend void swap(Chain& a, Chain& b) noexcept;

  friend bool operator==(const Chain& a, const Chain& b) {
    return a.size() == b.size() && CompareImpl(a, b) == 0;
  }
  friend StrongOrdering RIEGELI_COMPARE(const Chain& a, const Chain& b) {
    return CompareImpl(a, b);
  }

  friend bool operator==(const Chain& a, absl::string_view b) {
    return a.size() == b.size() && CompareImpl(a, b) == 0;
  }
  friend StrongOrdering RIEGELI_COMPARE(const Chain& a, absl::string_view b) {
    return CompareImpl(a, b);
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, const Chain& self) {
    return self.AbslHashValueImpl(std::move(hash_state));
  }

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& sink, const Chain& self) {
    self.AbslStringifyImpl(sink);
  }

  friend std::ostream& operator<<(std::ostream& out, const Chain& self) {
    self.OutputImpl(out);
    return out;
  }

  // Support `absl::Format(&chain, format, args...)`.
  friend void AbslFormatFlush(Chain* dest, absl::string_view src) {
    dest->Append(src);
  }

  // For testing. If `RIEGELI_DEBUG` is defined, verifies internal invariants,
  // otherwise does nothing.
  void VerifyInvariants() const;

 private:
  struct ExternalMethods;
  template <typename T>
  struct ExternalMethodsFor;
  class RawBlock;
  class BlockPtrPtr;
  class BlockRef;

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
    // If also `has_here()`, then there are 0 pointers in `here` so `short_data`
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

  explicit Chain(RawBlock* block);

  void ClearSlow();
  absl::string_view FlattenSlow();

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
  //
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

  // If `ref_block` is present, it should be a functor equivalent to
  // `[&] { return block->Ref(); }`, but it can achieve that by stealing
  // a reference to `block` from elsewhere instead.
  void AppendRawBlock(RawBlock* block, const Options& options);
  template <typename RefBlock>
  void AppendRawBlock(RawBlock* block, const Options& options,
                      RefBlock ref_block);
  void PrependRawBlock(RawBlock* block, const Options& options);
  template <typename RefBlock>
  void PrependRawBlock(RawBlock* block, const Options& options,
                       RefBlock ref_block);

  // This template is defined and used only in chain.cc.
  template <typename CordRef>
  void AppendCord(CordRef&& src, const Options& options);
  // This template is defined and used only in chain.cc.
  template <typename CordRef>
  void PrependCord(CordRef&& src, const Options& options);

  // This template is defined and used only in chain.cc.
  template <typename SizedSharedBufferRef>
  void AppendSizedSharedBuffer(SizedSharedBufferRef&& src,
                               const Options& options);
  // This template is defined and used only in chain.cc.
  template <typename SizedSharedBufferRef>
  void PrependSizedSharedBuffer(SizedSharedBufferRef&& src,
                                const Options& options);

  void RegisterSubobjectsImpl(MemoryEstimator& memory_estimator) const;
  static StrongOrdering CompareImpl(const Chain& a, const Chain& b);
  static StrongOrdering CompareImpl(const Chain& a, absl::string_view b);
  template <typename HashState>
  HashState AbslHashValueImpl(HashState hash_state) const;
  template <typename Sink>
  void AbslStringifyImpl(Sink& sink) const;
  void OutputImpl(std::ostream& out) const;

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
  //       `size_` is the sum of sizes of blocks in the range [`begin_`..`end_`)
  size_t size_ = 0;
};

// Represents either `const BlockPtr*`, or one of two special values
// (`kBeginShortData` and `kEndShortData`) behaving as if they were pointers in
// a single-element `BlockPtr` array.
class Chain::BlockPtrPtr : public WithCompare<BlockPtrPtr> {
 public:
  explicit constexpr BlockPtrPtr(uintptr_t repr) : repr_(repr) {}
  static BlockPtrPtr from_ptr(const BlockPtr* ptr);

  bool is_special() const;
  const BlockPtr* as_ptr() const;

  BlockPtrPtr operator+(ptrdiff_t n) const;
  BlockPtrPtr operator-(ptrdiff_t n) const;
  friend ptrdiff_t operator-(BlockPtrPtr a, BlockPtrPtr b) {
    return Subtract(a, b);
  }

  friend bool operator==(BlockPtrPtr a, BlockPtrPtr b) {
    return a.repr_ == b.repr_;
  }
  friend StrongOrdering RIEGELI_COMPARE(BlockPtrPtr a, BlockPtrPtr b) {
    RIEGELI_ASSERT_EQ(a.is_special(), b.is_special())
        << "Incompatible BlockPtrPtr values";
    if (a.is_special()) return Compare(a.repr_, b.repr_);
    return Compare(a.as_ptr(), b.as_ptr());
  }

 private:
  // `operator-` body is defined in a member function to gain access to private
  // `Chain::RawBlock` under gcc.
  static ptrdiff_t Subtract(BlockPtrPtr a, BlockPtrPtr b) {
    RIEGELI_ASSERT_EQ(a.is_special(), b.is_special())
        << "Incompatible BlockPtrPtr values";
    if (a.is_special()) {
      const ptrdiff_t byte_diff =
          static_cast<ptrdiff_t>(a.repr_) - static_cast<ptrdiff_t>(b.repr_);
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

  uintptr_t repr_;
};

class Chain::BlockIterator : public WithCompare<BlockIterator> {
 public:
  using iterator_concept = std::random_access_iterator_tag;
  // `iterator_category` is only `std::input_iterator_tag` because the
  // `LegacyForwardIterator` requirement and above require `reference` to be
  // a true reference type.
  using iterator_category = std::input_iterator_tag;
  using value_type = absl::string_view;
  using reference = value_type;
  using difference_type = ptrdiff_t;

  class pointer {
   public:
    const reference* operator->() const { return &ref_; }

   private:
    friend class BlockIterator;
    friend class PinnedBlock;
    explicit pointer(reference ref) : ref_(ref) {}
    reference ref_;
  };

  BlockIterator() = default;

  explicit BlockIterator(const Chain* chain, size_t block_index);

  BlockIterator(const BlockIterator& that) = default;
  BlockIterator& operator=(const BlockIterator& that) = default;

  const Chain* chain() const { return chain_; }
  size_t block_index() const;

  // Returns the char index relative to the beginning of the chain, given the
  // corresponding char index relative to the beginning of the block.
  //
  // The opposite conversion is `Chain::BlockAndCharIndex()`.
  size_t CharIndexInChain(size_t char_index_in_block = 0) const;

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
    RIEGELI_ASSERT(a.chain_ == b.chain_)
        << "Failed precondition of operator==(Chain::BlockIterator): "
           "incomparable iterators";
    return a.ptr_ == b.ptr_;
  }
  friend StrongOrdering RIEGELI_COMPARE(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT(a.chain_ == b.chain_)
        << "Failed precondition of operator<=>(Chain::BlockIterator): "
           "incomparable iterators";
    return Compare(a.ptr_, b.ptr_);
  }
  friend difference_type operator-(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT(a.chain_ == b.chain_)
        << "Failed precondition of operator-(Chain::BlockIterator): "
           "incomparable iterators";
    return a.ptr_ - b.ptr_;
  }
  friend BlockIterator operator+(difference_type n, BlockIterator a) {
    return a + n;
  }

  // Returns a `PinnedBlock` which pins the block pointed to by this iterator,
  // keeping it alive and unchanged until the `PinnedBlock` is destroyed or
  // reassigned, or `PinnedBlock::Share() &&` and `PinnedBlock::DeleteShared()`
  // are called.
  //
  // Warning: the data pointer of the returned `PinnedBlock` is not necessarily
  // the same as the data pointer of this `BlockIterator` (because of short
  // `Chain` optimization). Dereference the `PinnedBlock` for a data pointer
  // valid for the pinned block.
  //
  // Precondition: this is not past the end iterator
  PinnedBlock Pin();

  // Returns a pointer to the external object if this points to an external
  // block holding an object of type `T`, otherwise returns `nullptr`.
  //
  // Precondition: this is not past the end iterator
  template <typename T>
  const T* external_object() const;

  // Appends `**this` to `dest`.
  //
  // Precondition: this is not past the end iterator
  void AppendTo(Chain& dest, const Options& options = kDefaultOptions) const;
  void AppendTo(absl::Cord& dest) const;

  // Appends [`data`..`data + length`) to `dest`.
  //
  // If `length > 0` then [`data`..`data + length`) must be contained in
  // `**this`.
  //
  // Precondition:
  //   if `length > 0` then this is not past the end iterator
  void AppendSubstrTo(const char* data, size_t length, Chain& dest,
                      const Options& options = kDefaultOptions) const;
  void AppendSubstrTo(const char* data, size_t length, absl::Cord& dest) const;

  // Prepends `**this` to `dest`.
  //
  // Precondition: this is not past the end iterator
  void PrependTo(Chain& dest, const Options& options = kDefaultOptions) const;
  void PrependTo(absl::Cord& dest) const;

  // Prepends [`data`..`data + length`) to `dest`.
  //
  // If `length > 0` then [`data`..`data + length`) must be contained in
  // `**this`.
  //
  // Precondition:
  //   if `length > 0` then this is not past the end iterator
  void PrependSubstrTo(const char* data, size_t length, Chain& dest,
                       const Options& options = kDefaultOptions) const;
  void PrependSubstrTo(const char* data, size_t length, absl::Cord& dest) const;

 private:
  friend class Chain;

  static constexpr BlockPtrPtr kBeginShortData{0};
  static constexpr BlockPtrPtr kEndShortData{sizeof(BlockPtr)};

  explicit BlockIterator(const Chain* chain, BlockPtrPtr ptr) noexcept;

  size_t CharIndexInChainInternal() const;

  RawBlock* PinImpl();

  const Chain* chain_ = nullptr;
  // If `chain_ == nullptr`, `kBeginShortData`.
  // If `*chain_` has no block pointers and no short data, `kEndShortData`.
  // If `*chain_` has short data, `kBeginShortData` or `kEndShortData`.
  // If `*chain_` has block pointers, a pointer to the block pointer array.
  BlockPtrPtr ptr_ = kBeginShortData;
};

class Chain::PinnedBlock {
 public:
  using pointer = BlockIterator::pointer;

  PinnedBlock() = default;

  PinnedBlock(const PinnedBlock& that) = default;
  PinnedBlock& operator=(const PinnedBlock& that) = default;

  PinnedBlock(PinnedBlock&& that) = default;
  PinnedBlock& operator=(PinnedBlock&& that) = default;

  absl::string_view operator*() const;
  pointer operator->() const;

  // Returns an opaque pointer, which represents a share of ownership of the
  // data; an active share keeps the data alive. The returned pointer must be
  // deleted using `DeleteShared()`.
  void* Share() const&;
  void* Share() &&;

  // Deletes the pointer obtained by `Share()`.
  static void DeleteShared(void* ptr);

 private:
  friend class BlockIterator;
  explicit PinnedBlock(RawBlock* block) : block_(block) {}
  RefCountedPtr<RawBlock> block_;
};

class Chain::Blocks {
 public:
  using value_type = absl::string_view;
  using reference = value_type;
  using const_reference = reference;
  using iterator = BlockIterator;
  using const_iterator = iterator;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = reverse_iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  Blocks() = default;

  Blocks(const Blocks& that) = default;
  Blocks& operator=(const Blocks& that) = default;

  iterator begin() const;
  iterator cbegin() const { return begin(); }
  iterator end() const;
  iterator cend() const { return end(); }

  reverse_iterator rbegin() const { return reverse_iterator(end()); }
  reverse_iterator crbegin() const { return rbegin(); }
  reverse_iterator rend() const { return reverse_iterator(begin()); }
  reverse_iterator crend() const { return rend(); }

  size_type size() const;
  bool empty() const;

  reference operator[](size_type n) const;
  reference at(size_type n) const;
  reference front() const;
  reference back() const;

 private:
  friend class Chain;

  explicit Blocks(const Chain* chain) noexcept : chain_(chain) {}

  const Chain* chain_ = nullptr;
};

// Represents the position of a character in a `Chain`.
//
// A `CharIterator` is not provided because it is more efficient to iterate by
// blocks and process character ranges within a block.
struct Chain::BlockAndChar {
  // Intended invariant:
  //   if `block_iter == block_iter.chain()->blocks().cend()`
  //       then `char_index == 0`
  //       else `char_index < block_iter->size()`
  BlockIterator block_iter;
  size_t char_index;
};

// Returns the given number of zero bytes.
Chain ChainOfZeros(size_t length);

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
//  - tiny block: a block with size < `kDefaultMinBlockSize`
//  - wasteful block: a block with free space >
//    max(size, `kDefaultMinBlockSize`)
//
// Invariants of a `Chain`:
//  - A block can be empty or wasteful only if it is the first or last block.
//  - Tiny blocks must not be adjacent.
class Chain::RawBlock {
 public:
  static constexpr size_t kInternalAllocatedOffset();
  static constexpr size_t kMaxCapacity =
      size_t{std::numeric_limits<ptrdiff_t>::max()};

  // Creates an internal block.
  static RawBlock* NewInternal(size_t min_capacity);

  // Constructs an internal block. This constructor is public for
  // `SizeReturningNewAligned()`.
  explicit RawBlock(const size_t* raw_capacity);

  // Constructs an external block containing an external object of type `T`, and
  // sets block data to `absl::string_view(new_object)`. This constructor is
  // public for `NewAligned()`.
  template <typename T>
  explicit RawBlock(Initializer<T> object);

  // Constructs an external block containing an external object of type `T`, and
  // sets block data to `data`. This constructor is public for `NewAligned()`.
  template <typename T>
  explicit RawBlock(Initializer<T> object, absl::string_view data);

  // Allocated size of an external block containing an external object of type
  // `T`.
  template <typename T>
  static constexpr size_t kExternalAllocatedSize();

  template <Ownership ownership = Ownership::kShare>
  RawBlock* Ref();

  template <Ownership ownership = Ownership::kSteal>
  void Unref();

  // This template is defined and used only in chain.cc.
  template <Ownership ownership>
  RawBlock* Copy();

  bool TryClear();

  explicit operator absl::string_view() const {
    return absl::string_view(data_, size_);
  }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }
  const char* data_begin() const { return data_; }
  const char* data_end() const { return data_ + size_; }

  // Returns a reference to the external object, assuming that this is an
  // external block holding an object of type `T`.
  template <typename T>
  T& unchecked_external_object();
  template <typename T>
  const T& unchecked_external_object() const;

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

  // Shows internal structure in a human-readable way, for debugging.
  void DumpStructure(std::ostream& out) const;
  // Registers this `RawBlock` with `MemoryEstimator`.
  friend size_t RiegeliDynamicSizeOf(const RawBlock* self) {
    return self->DynamicSizeOfImpl();
  }
  friend void RiegeliRegisterSubobjects(const RawBlock* self,
                                        MemoryEstimator& memory_estimator) {
    self->RegisterSubobjectsImpl(memory_estimator);
  }

  bool can_append(size_t length) const;
  bool can_prepend(size_t length) const;
  // If `length` can be appended/prepended while possibly moving existing data,
  // moves existing data if needed, and returns `true`. If not, sets
  // `min_length_if_not` to the new minimum length to append/prepend
  // (for amortized constant time of a reallocation), and returns `false`.
  bool CanAppendMovingData(size_t length, size_t& min_length_if_not);
  // If `length` can be prepended while possibly moving existing data, moves
  // existing data if needed, and returns `true`. If not, sets
  // `space_after_if_not` to space after data (to be kept unchanged, to avoid
  // repeated moves back and forth), sets `min_length_if_not` to the new minimum
  // length to prepend (for amortized constant time of a reallocation), and
  // returns `false`.
  bool CanPrependMovingData(size_t length, size_t& space_after_if_not,
                            size_t& min_length_if_not);
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

  void AppendSubstrTo(const char* data, size_t length, Chain& dest,
                      const Options& options);
  void AppendSubstrTo(const char* data, size_t length, absl::Cord& dest);

  void PrependTo(Chain& dest, const Options& options);
  // This template is defined and used only in chain.cc.
  template <Ownership ownership>
  void PrependTo(absl::Cord& dest);

  void PrependSubstrTo(const char* data, size_t length, Chain& dest,
                       const Options& options);
  void PrependSubstrTo(const char* data, size_t length, absl::Cord& dest);

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

  bool is_mutable() const { return is_internal() && has_unique_owner(); }

  bool has_unique_owner() const;

  bool is_internal() const { return allocated_end_ != nullptr; }
  bool is_external() const { return allocated_end_ == nullptr; }

  size_t capacity() const;
  size_t space_before() const;
  size_t space_after() const;

  size_t DynamicSizeOfImpl() const;
  void RegisterSubobjectsImpl(MemoryEstimator& memory_estimator) const;

  RefCount ref_count_;
  const char* data_;
  size_t size_;
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
  void (*dump_structure)(const RawBlock& block, std::ostream& out);
  size_t dynamic_sizeof;
  void (*register_subobjects)(const RawBlock* block,
                              MemoryEstimator& memory_estimator);
};

namespace chain_internal {

template <typename T, typename Enable = void>
struct HasCallOperatorWithData : std::false_type {};

template <typename T>
struct HasCallOperatorWithData<T,
                               absl::void_t<decltype(std::declval<const T&>()(
                                   std::declval<absl::string_view>()))>>
    : std::true_type {};

template <typename T, typename Enable = void>
struct HasCallOperatorWithoutData : std::false_type {};

template <typename T>
struct HasCallOperatorWithoutData<
    T, absl::void_t<decltype(std::declval<const T&>()())>> : std::true_type {};

template <typename T,
          std::enable_if_t<HasCallOperatorWithData<T>::value, int> = 0>
inline void CallOperator(const T& object, absl::string_view data) {
  object(data);
}

template <typename T,
          std::enable_if_t<
              absl::conjunction<absl::negation<HasCallOperatorWithData<T>>,
                                HasCallOperatorWithoutData<T>>::value,
              int> = 0>
inline void CallOperator(const T& object,
                         ABSL_ATTRIBUTE_UNUSED absl::string_view data) {
  object();
}

template <
    typename T,
    std::enable_if_t<
        absl::conjunction<absl::negation<HasCallOperatorWithData<T>>,
                          absl::negation<HasCallOperatorWithoutData<T>>>::value,
        int> = 0>
inline void CallOperator(ABSL_ATTRIBUTE_UNUSED T& object,
                         ABSL_ATTRIBUTE_UNUSED absl::string_view data) {}

template <typename T, typename Enable = void>
struct HasDumpStructureWithData : std::false_type {};

template <typename T>
struct HasDumpStructureWithData<
    T, absl::void_t<decltype(std::declval<T>().DumpStructure(
           std::declval<absl::string_view>(), std::declval<std::ostream&>()))>>
    : std::true_type {};

template <typename T, typename Enable = void>
struct HasDumpStructureWithoutData : std::false_type {};

template <typename T>
struct HasDumpStructureWithoutData<
    T, absl::void_t<decltype(std::declval<T>().DumpStructure(
           std::declval<std::ostream&>()))>> : std::true_type {};

template <typename T,
          std::enable_if_t<HasDumpStructureWithData<T>::value, int> = 0>
inline void DumpStructure(T& object, absl::string_view data,
                          std::ostream& out) {
  object.DumpStructure(data, out);
}

template <typename T,
          std::enable_if_t<
              absl::conjunction<absl::negation<HasDumpStructureWithData<T>>,
                                HasDumpStructureWithoutData<T>>::value,
              int> = 0>
inline void DumpStructure(T& object,
                          ABSL_ATTRIBUTE_UNUSED absl::string_view data,
                          std::ostream& out) {
  object.DumpStructure(out);
}

template <
    typename T,
    std::enable_if_t<absl::conjunction<
                         absl::negation<HasDumpStructureWithData<T>>,
                         absl::negation<HasDumpStructureWithoutData<T>>>::value,
                     int> = 0>
inline void DumpStructure(ABSL_ATTRIBUTE_UNUSED T& object,
                          ABSL_ATTRIBUTE_UNUSED absl::string_view data,
                          std::ostream& out) {
  out << "[external] { }";
}

template <typename T,
          std::enable_if_t<RegisterSubobjectsIsGood<T>::value, int> = 0>
inline void RegisterSubobjects(const T* object,
                               ABSL_ATTRIBUTE_UNUSED absl::string_view data,
                               MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterSubobjects(object);
}

template <typename T,
          std::enable_if_t<!RegisterSubobjectsIsGood<T>::value, int> = 0>
inline void RegisterSubobjects(ABSL_ATTRIBUTE_UNUSED const T* object,
                               absl::string_view data,
                               MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterUnknownType<T>();
  // As an approximation of memory usage of an unknown type, register just the
  // stored data if unique.
  if (memory_estimator.RegisterNode(data.data())) {
    memory_estimator.RegisterDynamicMemory(data.size());
  }
}

}  // namespace chain_internal

template <typename T>
struct Chain::ExternalMethodsFor {
  // Creates an external block containing an external object constructed from
  // `object`, and sets block data to `absl::string_view(new_object)`.
  template <typename... Args>
  static RawBlock* NewBlock(Initializer<T> object);

  // Creates an external block containing an external object constructed from
  // `object`, and sets block data to `data`.
  template <typename... Args>
  static RawBlock* NewBlock(Initializer<T> object, absl::string_view data);

 private:
  static void DeleteBlock(RawBlock* block);
  static void DumpStructure(const RawBlock& block, std::ostream& out);
  static constexpr size_t DynamicSizeOf();
  static void RegisterSubobjects(const RawBlock* block,
                                 MemoryEstimator& memory_estimator);

 public:
  static constexpr ExternalMethods kMethods = {
      DeleteBlock, DumpStructure, DynamicSizeOf(), RegisterSubobjects};
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename T>
constexpr Chain::ExternalMethods Chain::ExternalMethodsFor<T>::kMethods;
#endif

template <typename T>
template <typename... Args>
inline Chain::RawBlock* Chain::ExternalMethodsFor<T>::NewBlock(
    Initializer<T> object) {
  return NewAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
      RawBlock::kExternalAllocatedSize<T>(), std::move(object));
}

template <typename T>
template <typename... Args>
inline Chain::RawBlock* Chain::ExternalMethodsFor<T>::NewBlock(
    Initializer<T> object, absl::string_view data) {
  return NewAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
      RawBlock::kExternalAllocatedSize<T>(), std::move(object), data);
}

template <typename T>
void Chain::ExternalMethodsFor<T>::DeleteBlock(RawBlock* block) {
  chain_internal::CallOperator(block->unchecked_external_object<T>(),
                               absl::string_view(*block));
  block->unchecked_external_object<T>().~T();
  DeleteAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
      block, RawBlock::kExternalAllocatedSize<T>());
}

template <typename T>
void Chain::ExternalMethodsFor<T>::DumpStructure(const RawBlock& block,
                                                 std::ostream& out) {
  chain_internal::DumpStructure(block.unchecked_external_object<T>(),
                                absl::string_view(block), out);
}

template <typename T>
constexpr size_t Chain::ExternalMethodsFor<T>::DynamicSizeOf() {
  return RawBlock::kExternalAllocatedSize<T>();
}

template <typename T>
void Chain::ExternalMethodsFor<T>::RegisterSubobjects(
    const RawBlock* block, MemoryEstimator& memory_estimator) {
  chain_internal::RegisterSubobjects(&block->unchecked_external_object<T>(),
                                     absl::string_view(*block),
                                     memory_estimator);
}

template <typename T>
inline Chain::RawBlock::RawBlock(Initializer<T> object) {
  external_.methods = &ExternalMethodsFor<T>::kMethods;
  new (&unchecked_external_object<T>()) T(std::move(object).Construct());
  const absl::string_view data(unchecked_external_object<T>());
  data_ = data.data();
  size_ = data.size();
  RIEGELI_ASSERT(is_external()) << "A RawBlock with allocated_end_ == nullptr "
                                   "should be considered external";
}

template <typename T>
inline Chain::RawBlock::RawBlock(Initializer<T> object, absl::string_view data)
    : data_(data.data()), size_(data.size()) {
  external_.methods = &ExternalMethodsFor<T>::kMethods;
  new (&unchecked_external_object<T>()) T(std::move(object).Construct());
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

template <typename T>
constexpr size_t Chain::RawBlock::kExternalAllocatedSize() {
  return kExternalObjectOffset<T>() + sizeof(T);
}

template <Chain::Ownership ownership>
inline Chain::RawBlock* Chain::RawBlock::Ref() {
  if (ownership == Ownership::kShare) ref_count_.Ref();
  return this;
}

template <Chain::Ownership ownership>
void Chain::RawBlock::Unref() {
  // Optimization: avoid an expensive atomic read-modify-write operation if the
  // reference count is 1.
  if (ownership == Ownership::kSteal && ref_count_.Unref()) {
    if (is_internal()) {
      DeleteAligned<RawBlock>(this, kInternalAllocatedOffset() + capacity());
    } else {
      external_.methods->delete_block(this);
    }
  }
}

inline bool Chain::RawBlock::has_unique_owner() const {
  return ref_count_.has_unique_owner();
}

inline size_t Chain::RawBlock::capacity() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::RawBlock::capacity(): "
         "block not internal";
  return PtrDistance(allocated_begin_, allocated_end_);
}

template <typename T>
inline T& Chain::RawBlock::unchecked_external_object() {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::RawBlock::unchecked_external_object(): "
      << "block not external";
  return *
#if __cpp_lib_launder >= 201606
      std::launder
#endif
      (reinterpret_cast<T*>(reinterpret_cast<char*>(this) +
                            kExternalObjectOffset<T>()));
}

template <typename T>
inline const T& Chain::RawBlock::unchecked_external_object() const {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::RawBlock::unchecked_external_object(): "
      << "block not external";
  return *
#if __cpp_lib_launder >= 201606
      std::launder
#endif
      (reinterpret_cast<const T*>(reinterpret_cast<const char*>(this) +
                                  kExternalObjectOffset<T>()));
}

template <typename T>
inline const T* Chain::RawBlock::checked_external_object() const {
  return is_external() && external_.methods == &ExternalMethodsFor<T>::kMethods
             ? &unchecked_external_object<T>()
             : nullptr;
}

template <typename T>
inline T* Chain::RawBlock::checked_external_object_with_unique_owner() {
  return is_external() &&
                 external_.methods == &ExternalMethodsFor<T>::kMethods &&
                 has_unique_owner()
             ? &unchecked_external_object<T>()
             : nullptr;
}

inline bool Chain::RawBlock::TryClear() {
  if (is_mutable()) {
    size_ = 0;
    return true;
  }
  return false;
}

inline bool Chain::RawBlock::TryRemoveSuffix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::RawBlock::TryRemoveSuffix(): "
      << "length to remove greater than current size";
  if (is_mutable()) {
    size_ -= length;
    return true;
  }
  return false;
}

inline bool Chain::RawBlock::TryRemovePrefix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::RawBlock::TryRemovePrefix(): "
      << "length to remove greater than current size";
  if (is_mutable()) {
    data_ += length;
    size_ -= length;
    return true;
  }
  return false;
}

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::from_ptr(const BlockPtr* ptr) {
  return BlockPtrPtr(reinterpret_cast<uintptr_t>(ptr));
}

inline bool Chain::BlockPtrPtr::is_special() const {
  return repr_ <= sizeof(BlockPtr);
}

inline const Chain::BlockPtr* Chain::BlockPtrPtr::as_ptr() const {
  RIEGELI_ASSERT(!is_special()) << "Unexpected special BlockPtrPtr value";
  return reinterpret_cast<const BlockPtr*>(repr_);
}

// Code conditional on `is_special()` is written such that both branches
// typically compile to the same code, allowing the compiler eliminate the
// `is_special()` checks.

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::operator+(ptrdiff_t n) const {
  if (is_special()) {
    return BlockPtrPtr(IntCast<uintptr_t>(IntCast<ptrdiff_t>(repr_) +
                                          n * ptrdiff_t{sizeof(RawBlock*)}));
  }
  return BlockPtrPtr::from_ptr(as_ptr() + n);
}

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::operator-(ptrdiff_t n) const {
  if (is_special()) {
    return BlockPtrPtr(IntCast<uintptr_t>(IntCast<ptrdiff_t>(repr_) -
                                          n * ptrdiff_t{sizeof(RawBlock*)}));
  }
  return BlockPtrPtr::from_ptr(as_ptr() - n);
}

inline Chain::BlockIterator::BlockIterator(const Chain* chain,
                                           size_t block_index)
    : chain_(chain),
      ptr_((ABSL_PREDICT_FALSE(chain_ == nullptr) ? kBeginShortData
            : chain_->begin_ == chain_->end_
                ? (chain_->empty() ? kEndShortData : kBeginShortData)
                : BlockPtrPtr::from_ptr(chain_->begin_)) +
           IntCast<ptrdiff_t>(block_index)) {}

inline Chain::BlockIterator::BlockIterator(const Chain* chain,
                                           BlockPtrPtr ptr) noexcept
    : chain_(chain), ptr_(ptr) {}

inline size_t Chain::BlockIterator::block_index() const {
  if (ptr_ == kBeginShortData) {
    return 0;
  } else if (ptr_ == kEndShortData) {
    return chain_->empty() ? 0 : 1;
  } else {
    return PtrDistance(chain_->begin_, ptr_.as_ptr());
  }
}

inline size_t Chain::BlockIterator::CharIndexInChain(
    size_t char_index_in_block) const {
  return CharIndexInChainInternal() + char_index_in_block;
}

inline Chain::BlockIterator::reference Chain::BlockIterator::operator*() const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::operator*: "
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
  const BlockIterator tmp = *this;
  ++*this;
  return tmp;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator--() {
  ptr_ = ptr_ - 1;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator--(int) {
  const BlockIterator tmp = *this;
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

inline Chain::PinnedBlock Chain::BlockIterator::Pin() {
  return PinnedBlock(PinImpl());
}

inline absl::string_view Chain::PinnedBlock::operator*() const {
  RIEGELI_ASSERT(block_ != nullptr)
      << "Failed precondition of Chain::PinnedBlock::operator*: null pointer";
  return absl::string_view(*block_);
}

inline Chain::PinnedBlock::pointer Chain::PinnedBlock::operator->() const {
  RIEGELI_ASSERT(block_ != nullptr)
      << "Failed precondition of Chain::PinnedBlock::operator->: null pointer";
  return pointer(**this);
}

inline void* Chain::PinnedBlock::Share() const& {
  RIEGELI_ASSERT(block_ != nullptr)
      << "Failed precondition of Chain::PinnedBlock::Share(): null pointer";
  return block_->Ref();
}

inline void* Chain::PinnedBlock::Share() && {
  RIEGELI_ASSERT(block_ != nullptr)
      << "Failed precondition of Chain::PinnedBlock::Share(): null pointer";
  return block_.release();
}

inline void Chain::PinnedBlock::DeleteShared(void* ptr) {
  static_cast<RawBlock*>(ptr)->Unref();
}

inline Chain::Blocks::iterator Chain::Blocks::begin() const {
  return BlockIterator(chain_,
                       chain_->begin_ == chain_->end_
                           ? (chain_->empty() ? BlockIterator::kEndShortData
                                              : BlockIterator::kBeginShortData)
                           : BlockPtrPtr::from_ptr(chain_->begin_));
}

inline Chain::Blocks::iterator Chain::Blocks::end() const {
  return BlockIterator(chain_, chain_->begin_ == chain_->end_
                                   ? BlockIterator::kEndShortData
                                   : BlockPtrPtr::from_ptr(chain_->end_));
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

inline Chain::Blocks::reference Chain::Blocks::operator[](size_type n) const {
  RIEGELI_ASSERT_LT(n, size())
      << "Failed precondition of Chain::Blocks::operator[]: "
         "block index out of range";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->begin_[n].block_ptr);
  }
}

inline Chain::Blocks::reference Chain::Blocks::at(size_type n) const {
  RIEGELI_CHECK_LT(n, size()) << "Failed precondition of Chain::Blocks::at(): "
                                 "block index out of range";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->begin_[n].block_ptr);
  }
}

inline Chain::Blocks::reference Chain::Blocks::front() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::front(): no blocks";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->begin_[0].block_ptr);
  }
}

inline Chain::Blocks::reference Chain::Blocks::back() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::back(): no blocks";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->end_[-1].block_ptr);
  }
}

template <typename T,
          std::enable_if_t<std::is_constructible<absl::string_view,
                                                 InitializerTargetT<T>>::value,
                           int>>
inline Chain Chain::FromExternal(T&& object) {
  return Chain(Chain::ExternalMethodsFor<InitializerTargetT<T>>::NewBlock(
      std::forward<T>(object)));
}

template <typename T>
inline Chain Chain::FromExternal(T&& object, absl::string_view data) {
  return Chain(Chain::ExternalMethodsFor<InitializerTargetT<T>>::NewBlock(
      std::forward<T>(object), data));
}

template <typename T>
constexpr size_t Chain::kExternalAllocatedSize() {
  return RawBlock::kExternalAllocatedSize<T>();
}

inline Chain::Chain(RawBlock* block) {
  (end_++)->block_ptr = block;
  size_ = block->size();
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

inline Chain::Chain(const absl::Cord& src) {
  Append(src, Options().set_size_hint(src.size()));
}

inline Chain::Chain(absl::Cord&& src) {
  const size_t size = src.size();
  Append(std::move(src), Options().set_size_hint(size));
}

inline Chain::Chain(const SizedSharedBuffer& src) {
  Append(src, Options().set_size_hint(src.size()));
}

inline Chain::Chain(SizedSharedBuffer&& src) {
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

inline absl::string_view Chain::Flatten() {
  switch (end_ - begin_) {
    case 0:
      return short_data();
    case 1:
      return absl::string_view(*front());
    default:
      return FlattenSlow();
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

template <typename HashState>
HashState Chain::AbslHashValueImpl(HashState hash_state) const {
  {
    const absl::optional<absl::string_view> flat = TryFlat();
    if (flat != absl::nullopt) {
      return HashState::combine(std::move(hash_state), *flat);
    }
  }
  typename HashState::AbslInternalPiecewiseCombiner combiner;
  for (const absl::string_view block : blocks()) {
    hash_state =
        combiner.add_buffer(std::move(hash_state), block.data(), block.size());
  }
  return HashState::combine(combiner.finalize(std::move(hash_state)), size());
}

template <typename Sink>
void Chain::AbslStringifyImpl(Sink& sink) const {
  for (const absl::string_view block : blocks()) {
    sink.Append(block);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_CHAIN_H_
