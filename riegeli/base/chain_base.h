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

#ifndef RIEGELI_BASE_CHAIN_BASE_H_
#define RIEGELI_BASE_CHAIN_BASE_H_

// IWYU pragma: private, include "riegeli/base/chain.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <iosfwd>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/intrusive_shared_ptr.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/ownership.h"
#include "riegeli/base/ref_count.h"
#include "riegeli/base/to_string_view.h"

namespace riegeli {

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
 private:
  class RawBlock;

  // A union of either a block pointer or a block offset. Having a union makes
  // easier to allocate an array containing both kinds of data, with block
  // offsets following block pointers.
  union BlockPtr {
    RawBlock* block_ptr;
    size_t block_offset;
  };

  static constexpr size_t kMaxShortDataSize = 2 * sizeof(BlockPtr);

 public:
  class Options {
   public:
    Options() noexcept {}

    // Expected final size, or `absl::nullopt` if unknown. This may improve
    // performance and memory usage.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    Options& set_size_hint(absl::optional<size_t> size_hint) & {
      if (size_hint == absl::nullopt) {
        size_hint_ = std::numeric_limits<size_t>::max();
      } else {
        size_hint_ =
            UnsignedMin(*size_hint, std::numeric_limits<size_t>::max() - 1);
      }
      return *this;
    }
    Options&& set_size_hint(absl::optional<size_t> size_hint) && {
      return std::move(set_size_hint(size_hint));
    }
    absl::optional<size_t> size_hint() const {
      if (size_hint_ == std::numeric_limits<size_t>::max()) {
        return absl::nullopt;
      } else {
        return size_hint_;
      }
    }

    // Minimal size of a block of allocated data.
    //
    // This is used initially, while the destination is small.
    //
    // Default: `kDefaultMinBlockSize` (256).
    Options& set_min_block_size(size_t min_block_size) & {
      min_block_size_ = UnsignedMin(min_block_size, uint32_t{1} << 31);
      return *this;
    }
    Options&& set_min_block_size(size_t min_block_size) && {
      return std::move(set_min_block_size(min_block_size));
    }
    size_t min_block_size() const { return min_block_size_; }

    // Maximal size of a block of allocated data.
    //
    // This is for performance tuning, not a guarantee: does not apply to
    // objects allocated separately and then appended to this `Chain`.
    //
    // Default: `kDefaultMaxBlockSize` (64K).
    Options& set_max_block_size(size_t max_block_size) & {
      RIEGELI_ASSERT_GT(max_block_size, 0u)
          << "Failed precondition of Chain::Options::set_max_block_size(): "
             "zero block size";
      max_block_size_ = UnsignedMin(max_block_size, uint32_t{1} << 31);
      return *this;
    }
    Options&& set_max_block_size(size_t max_block_size) && {
      return std::move(set_max_block_size(max_block_size));
    }
    size_t max_block_size() const { return max_block_size_; }

    // A shortcut for `set_min_block_size(block_size)` with
    // `set_max_block_size(block_size)`.
    Options& set_block_size(size_t block_size) & {
      return set_min_block_size(block_size).set_max_block_size(block_size);
    }
    Options&& set_block_size(size_t block_size) && {
      return std::move(set_block_size(block_size));
    }

   private:
    // `absl::nullopt` is encoded as `std::numeric_limits<size_t>::max()` to
    // reduce object size.
    size_t size_hint_ = std::numeric_limits<size_t>::max();
    // Use `uint32_t` instead of `size_t` to reduce the object size.
    uint32_t min_block_size_ = uint32_t{kDefaultMinBlockSize};
    uint32_t max_block_size_ = uint32_t{kDefaultMaxBlockSize};
  };

  class Block;
  struct MakeBlock;
  class BlockIterator;
  class Blocks;
  struct BlockAndChar;

  // A sentinel value for the `max_length` parameter of
  // `AppendBuffer()`/`PrependBuffer()`.
  static constexpr size_t kAnyLength = std::numeric_limits<size_t>::max();

  static constexpr size_t kMaxBytesToCopyToEmpty = kMaxShortDataSize;

  size_t MaxBytesToCopy(Options options = Options()) const {
    if (options.size_hint() != absl::nullopt && size() < *options.size_hint()) {
      return UnsignedMin(*options.size_hint() - size() - 1, kMaxBytesToCopy);
    }
    if (empty()) return kMaxBytesToCopyToEmpty;
    return kMaxBytesToCopy;
  }

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
  explicit Chain(Block src);
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
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::string_view src);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Block src);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const absl::Cord& src);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::Cord&& src);

  // Removes all data.
  ABSL_ATTRIBUTE_REINITIALIZES void Clear();

  // A container of `absl::string_view` blocks comprising data of the `Chain`.
  Blocks blocks() const;

  bool empty() const { return size_ == 0; }
  size_t size() const { return size_; }

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
  // Support `MemoryEstimator`.
  friend void RiegeliRegisterSubobjects(const Chain* self,
                                        MemoryEstimator& memory_estimator) {
    self->RegisterSubobjects(memory_estimator);
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
                                Options options = Options());
  absl::Span<char> PrependBuffer(size_t min_length,
                                 size_t recommended_length = 0,
                                 size_t max_length = kAnyLength,
                                 Options options = Options());

  // Equivalent to `AppendBuffer()`/`PrependBuffer()` with
  // `min_length == max_length`.
  absl::Span<char> AppendFixedBuffer(size_t length,
                                     Options options = Options());
  absl::Span<char> PrependFixedBuffer(size_t length,
                                      Options options = Options());

  // Appends/prepends a string-like type.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  void Append(absl::string_view src, Options options = Options());
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  void Append(Src&& src, Options options = Options());
  void Append(const Chain& src, Options options = Options());
  void Append(Chain&& src, Options options = Options());
  void Append(const Block& src, Options options = Options());
  void Append(Block&& src, Options options = Options());
  void Append(const absl::Cord& src, Options options = Options());
  void Append(absl::Cord&& src, Options options = Options());
  void Prepend(absl::string_view src, Options options = Options());
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  void Prepend(Src&& src, Options options = Options());
  void Prepend(const Chain& src, Options options = Options());
  void Prepend(Chain&& src, Options options = Options());
  void Prepend(const Block& src, Options options = Options());
  void Prepend(Block&& src, Options options = Options());
  void Prepend(const absl::Cord& src, Options options = Options());
  void Prepend(absl::Cord&& src, Options options = Options());

  // `AppendFrom(iter, length)` is equivalent to
  // `Append(absl::Cord::AdvanceAndRead(&iter, length))` but more efficient.
  void AppendFrom(absl::Cord::CharIterator& iter, size_t length,
                  Options options = Options());

  // Removes suffix/prefix of the given length.
  //
  // Precondition: `length <= size()`
  void RemoveSuffix(size_t length, Options options = Options());
  void RemovePrefix(size_t length, Options options = Options());

  friend void swap(Chain& a, Chain& b) noexcept;

  friend bool operator==(const Chain& a, const Chain& b) {
    return a.size() == b.size() && Compare(a, b) == 0;
  }
  friend StrongOrdering RIEGELI_COMPARE(const Chain& a, const Chain& b) {
    return Compare(a, b);
  }

  friend bool operator==(const Chain& a, absl::string_view b) {
    return a.size() == b.size() && Compare(a, b) == 0;
  }
  friend StrongOrdering RIEGELI_COMPARE(const Chain& a, absl::string_view b) {
    return Compare(a, b);
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, const Chain& self) {
    return self.HashValue(std::move(hash_state));
  }

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& sink, const Chain& self) {
    self.Stringify(sink);
  }

  friend std::ostream& operator<<(std::ostream& out, const Chain& self) {
    self.Output(out);
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
  class BlockPtrPtr;
  struct ExternalMethods;
  template <typename T>
  struct ExternalMethodsFor;

  struct Empty {};

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

  // When deciding whether to copy an array of bytes or perform a small memory
  // allocation, prefer copying up to this length.
  static constexpr size_t kAllocationCost = 256;

  bool ClearSlow();
  absl::string_view FlattenSlow();

  bool has_here() const { return begin_ == block_ptrs_.here; }
  bool has_allocated() const { return begin_ != block_ptrs_.here; }

  absl::string_view short_data() const;
  char* short_data_begin();
  const char* short_data_begin() const;

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

  void DropPassedBlocks(PassOwnership);
  void DropPassedBlocks(ShareOwnership) const;

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

  void Initialize(absl::string_view src);
  void InitializeSlow(absl::string_view src);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  void Initialize(Src&& src);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  void InitializeSlow(Src&& src);
  void Initialize(Block src);
  void Initialize(const absl::Cord& src);
  void Initialize(absl::Cord&& src);
  // This template is defined and used only in chain.cc.
  template <typename CordRef>
  void InitializeFromCord(CordRef&& src);
  void Initialize(const Chain& src);
  void CopyToSlow(char* dest) const;
  std::string ToString() const;
  void AppendToSlow(absl::Cord& dest) const&;
  void AppendToSlow(absl::Cord& dest) &&;
  void PrependToSlow(absl::Cord& dest) const&;
  void PrependToSlow(absl::Cord& dest) &&;

  IntrusiveSharedPtr<RawBlock> SetBack(IntrusiveSharedPtr<RawBlock> block);
  IntrusiveSharedPtr<RawBlock> SetFront(IntrusiveSharedPtr<RawBlock> block);
  // Like `SetFront()`, but skips the `RefreshFront()` step. This is enough if
  // the block has the same size as the block being replaced.
  IntrusiveSharedPtr<RawBlock> SetFrontSameSize(
      IntrusiveSharedPtr<RawBlock> block);
  // Recomputes the block offset of the first block if needed.
  void RefreshFront();
  void PushBack(IntrusiveSharedPtr<RawBlock> block);
  void PushFront(IntrusiveSharedPtr<RawBlock> block);
  IntrusiveSharedPtr<RawBlock> PopBack();
  IntrusiveSharedPtr<RawBlock> PopFront();
  // This template is defined and used only in chain.cc.
  template <typename Ownership>
  void AppendBlocks(const BlockPtr* begin, const BlockPtr* end);
  // This template is defined and used only in chain.cc.
  template <typename Ownership>
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
                          size_t recommended_length, Options options) const;

  // This template is defined and used only in chain.cc.
  template <typename Ownership, typename ChainRef>
  void AppendChain(ChainRef&& src, Options options);
  // This template is defined and used only in chain.cc.
  template <typename Ownership, typename ChainRef>
  void PrependChain(ChainRef&& src, Options options);

  // This template is defined and used only in chain.cc.
  template <typename RawBlockPtrRef>
  void AppendRawBlock(RawBlockPtrRef&& block, Options options = Options());
  // This template is defined and used only in chain.cc.
  template <typename RawBlockPtrRef>
  void PrependRawBlock(RawBlockPtrRef&& block, Options options = Options());

  // This template is defined and used only in chain.cc.
  template <typename CordRef>
  void AppendCord(CordRef&& src, Options options);
  // This template is defined and used only in chain.cc.
  template <typename CordRef>
  void AppendCordSlow(CordRef&& src, Options options);
  // This template is defined and used only in chain.cc.
  template <typename CordRef>
  void PrependCord(CordRef&& src, Options options);

  void RegisterSubobjects(MemoryEstimator& memory_estimator) const;
  static StrongOrdering Compare(const Chain& a, const Chain& b);
  static StrongOrdering Compare(const Chain& a, absl::string_view b);
  template <typename HashState>
  HashState HashValue(HashState hash_state) const;
  template <typename Sink>
  void Stringify(Sink& sink) const;
  void Output(std::ostream& out) const;

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
//  - wasteful block: a block with free space > size + `kDefaultMinBlockSize`
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
  static IntrusiveSharedPtr<RawBlock> NewInternal(size_t min_capacity);

  // Constructs an internal block. This constructor is public for
  // `SizeReturningNewAligned()`.
  explicit RawBlock(const size_t* raw_capacity);

  // Constructs an external block containing an external object of type `T`,
  // and sets block data to `RiegeliToStringView(&new_object)`,
  // `absl::string_view(new_object)`, or `absl::Span<const char>(new_object)`.
  // This constructor is public for `NewAligned()`.
  template <typename T>
  explicit RawBlock(Initializer<T> object);

  // Constructs an external block containing an external object of type `T`, and
  // sets block data to `data`. This constructor is public for `NewAligned()`.
  template <typename T>
  explicit RawBlock(Initializer<T> object, absl::string_view substr);

  // Allocated size of an external block containing an external object of type
  // `T`.
  template <typename T>
  static constexpr size_t kExternalAllocatedSize();

  template <typename Ownership = ShareOwnership>
  RawBlock* Ref();

  template <typename Ownership = PassOwnership>
  void Unref();

  IntrusiveSharedPtr<RawBlock> Copy();

  bool TryClear();

  size_t ExternalMemory() const;

  explicit operator absl::string_view() const {
    return absl::string_view(data_, size_);
  }
  bool empty() const { return size_ == 0; }
  size_t size() const { return size_; }
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

  // Support `MemoryEstimator`.
  friend size_t RiegeliDynamicSizeOf(const RawBlock* self) {
    return self->DynamicSizeOf();
  }
  friend void RiegeliRegisterSubobjects(const RawBlock* self,
                                        MemoryEstimator& memory_estimator) {
    self->RegisterSubobjects(memory_estimator);
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

  size_t DynamicSizeOf() const;
  void RegisterSubobjects(MemoryEstimator& memory_estimator) const;

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

// Represents a reference counted pointer to a single block of a `Chain`.
class Chain::Block {
 public:
  // Creates an empty `Block`.
  Block() = default;

  // Given an object which owns a byte array, converts it to a `Block` by
  // attaching the object, avoiding copying the bytes.
  //
  // `ExternalRef` is a higher level mechanism which chooses between sharing the
  // object and copying the data.
  //
  // The `object` parameter supports `riegeli::Maker<T>(args...)` to construct
  // `T` in-place.
  //
  // If the `substr` parameter is given, `substr` must be valid for the new
  // object.
  //
  // If the `substr` parameter is not given, `T` must support any of:
  // ```
  //   // Returns contents of the object. Called when it is moved to its final
  //   // location.
  //   friend absl::string_view RiegeliToStringView(const T* self);
  //   explicit operator absl::string_view() const;
  //   explicit operator absl::Span<const char>() const;
  // ```
  //
  // `T` may also support the following member functions, either with or without
  // the `substr` parameter, with the following definitions assumed by default:
  // ```
  //   // Called once before the destructor, except on a moved-from object.
  //   // If only this function is needed, `T` can be a lambda.
  //   void operator()(absl::string_view substr) && {}
  //
  //   // Shows internal structure in a human-readable way, for debugging.
  //   friend void RiegeliDumpStructure(const T* self, absl::string_view substr,
  //                                    std::ostream& out) {
  //     out << "[external] { }";
  //   }
  //
  //   // Registers this object with `MemoryEstimator`.
  //   //
  //   // By default calls `memory_estimator.RegisterUnknownType<T>()` and
  //   // as an approximation of memory usage of an unknown type, registers just
  //   // the stored `substr` if unique.
  //   friend void RiegeliRegisterSubobjects(
  //       const T* self, riegeli::MemoryEstimator& memory_estimator);
  // ```
  //
  // The `substr` parameter of these member functions, if present, will get the
  // `substr` parameter passed to `FromExternal()`. Having `substr` available in
  // these functions might avoid storing `substr` in the external object.
  template <typename T,
            std::enable_if_t<SupportsToStringView<InitializerTargetT<T>>::value,
                             int> = 0>
  explicit Block(T&& object);
  template <typename T>
  explicit Block(T&& object, absl::string_view substr);

  Block(const Block& that) = default;
  Block& operator=(const Block& that) = default;

  Block(Block&& that) = default;
  Block& operator=(Block&& that) = default;

  // Support `ExternalRef`.
  friend size_t RiegeliExternalMemory(const Block* self) {
    return self->ExternalMemory();
  }

  // Support `ExternalRef`.
  friend Block RiegeliToChainBlock(Block* self, absl::string_view substr) {
    return std::move(*self).ToChainBlock(substr);
  }

  // Support `ExternalRef`.
  friend absl::Cord RiegeliToCord(Block* self, absl::string_view substr) {
    return std::move(*self).ToCord(substr);
  }

  // Support `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(Block* self) {
    return std::move(*self).ToExternalStorage();
  }

  // Support `ExternalRef` and `Chain::Block`.
  friend void RiegeliDumpStructure(const Block* self, absl::string_view substr,
                                   std::ostream& out) {
    self->DumpStructure(substr, out);
  }

  // Support `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const Block* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->block_);
  }

 private:
  friend class Chain;  // For `Block()` and `raw_block()`.

  explicit Block(RawBlock* block);
  explicit Block(IntrusiveSharedPtr<RawBlock> block);

  const IntrusiveSharedPtr<RawBlock>& raw_block() const& { return block_; }
  IntrusiveSharedPtr<RawBlock>&& raw_block() && { return std::move(block_); }

  size_t ExternalMemory() const;
  Block ToChainBlock(absl::string_view substr) &&;
  absl::Cord ToCord(absl::string_view substr) &&;
  ExternalStorage ToExternalStorage() &&;
  void DumpStructure(absl::string_view substr, std::ostream& out) const;

  IntrusiveSharedPtr<RawBlock> block_;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_CHAIN_BASE_H_
