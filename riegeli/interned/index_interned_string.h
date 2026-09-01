// Copyright 2026 Google LLC
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

#ifndef RIEGELI_INTERNED_INDEX_INTERNED_STRING_H_
#define RIEGELI_INTERNED_INDEX_INTERNED_STRING_H_

#include <stddef.h>

#include <array>
#include <cstddef>
#include <limits>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/numeric/bits.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/interned/arena_interned_string.h"
#include "riegeli/interned/index_interned_object_internal.h"
#include "riegeli/interned/index_interned_string_internal.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/string_arena.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Default template parameter `Encoder` for `IndexInternedString`.
using interned_internal::DefaultStringEncoder;

// Mutex type that does not lock.
using interned_internal::NullMutex;

// Default template parameter `num_shards` for `Concurrent` nested types.
using interned_internal::kDefaultInternerNumShards;

// Default template parameters for `IndexInternedString::Interner` and
// `IndexInternedString::Archive`.
using interned_internal::kDefaultArenaFixedBlockSize;
using interned_internal::kDefaultArenaMaxBlockSize;
using interned_internal::kDefaultArenaMinBlockSize;

template <typename Numeric, typename Encoder, typename Tag, size_t alignment>
class IndexInternedString;

namespace interned_internal {

template <typename Numeric, typename Tag, typename Address, size_t alignment,
          size_t static_min_block_size, size_t static_max_block_size>
class IndexStringArchive;

template <typename Numeric, typename Encoder, typename Tag, typename Address,
          typename Mutex, size_t num_shards, size_t alignment,
          size_t static_min_block_size, size_t static_max_block_size>
class IndexStringInterner;

// The public name of `OptionalIndexInternedString<Numeric>` is
// `IndexInternedString<Numeric>::Optional`.
//
// `IndexInternedString<Numeric>` refers to a string by a numeric index,
// ensuring that equal strings are shared to minimize memory usage.
//
// In contrast to `IndexInternedString`,
// `IndexInternedString::Optional` can be null. It is more efficient than
// `std::optional<IndexInternedString>`.
//
// See `IndexInternedString` for details.
template <typename Numeric, typename Encoder, typename Tag, size_t alignment>
class OptionalIndexInternedString
    : public WithCompare<
          OptionalIndexInternedString<Numeric, Encoder, Tag, alignment>,
          std::nullptr_t> {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Navigates between `IndexInternedString` and
  // `IndexInternedString::Optional`.
  using NotOptional = IndexInternedString<Numeric, Encoder, Tag, alignment>;
  using Optional = OptionalIndexInternedString;

  // The type of the arena-interned string resolved from an index.
  // See `IndexInternedString::Resolved` for details.
  using Resolved = BasicArenaInternedString<Encoder, Tag, alignment>;

  // The default interner type. See `IndexInternedString::Interner` for details.
  using Interner = interned_internal::IndexStringInterner<
      Numeric, Encoder, Tag, void, NullMutex, /*num_shards=*/1, alignment,
      kDefaultArenaMinBlockSize, kDefaultArenaMaxBlockSize>;

  // The default archive type. See `IndexInternedString::Archive` for details.
  using Archive =
      interned_internal::IndexStringArchive<Numeric, Tag, void, alignment,
                                            kDefaultArenaMinBlockSize,
                                            kDefaultArenaMaxBlockSize>;

  // An interner type using a numeric address rather than pointers in the
  // directory. See `IndexInternedString::InternerWithAddress` for details.
  template <typename Address>
  using InternerWithAddress = interned_internal::IndexStringInterner<
      Numeric, Encoder, Tag, Address, NullMutex, /*num_shards=*/1, alignment,
      kDefaultArenaFixedBlockSize, kDefaultArenaFixedBlockSize>;

  // An archive type using a numeric address rather than pointers in the
  // directory. See `IndexInternedString::ArchiveWithAddress` for details.
  template <typename Address>
  using ArchiveWithAddress =
      interned_internal::IndexStringArchive<Numeric, Tag, Address, alignment,
                                            kDefaultArenaFixedBlockSize,
                                            kDefaultArenaFixedBlockSize>;

  // Maximum supported string size.
  static constexpr size_t kMaxSize = Resolved::kMaxSize;

  // Creates a null `IndexInternedString::Optional`.
  //
  // This differs from the default constructor of `IndexInternedString`,
  // which is deleted.
  OptionalIndexInternedString() = default;
  /*implicit*/ OptionalIndexInternedString(std::nullptr_t) {}
  OptionalIndexInternedString& operator=(std::nullptr_t) {
    numeric_ = kNullNumeric<Numeric>;
    return *this;
  }

  OptionalIndexInternedString(const OptionalIndexInternedString& that) =
      default;
  OptionalIndexInternedString& operator=(
      const OptionalIndexInternedString& that) = default;

  // Returns `true` if not null.
  explicit operator bool() const {
    return numeric_or_max() != kNullNumeric<Numeric>;
  }

  // Converts from `IndexInternedString::Optional` to `IndexInternedString`.
  NotOptional not_optional() const {
    RIEGELI_ASSERT(*this) << "Failed precondition of "
                             "IndexInternedString::Optional::not_optional(): "
                             "null index";
    return NotOptional(numeric_or_max());
  }
  NotOptional NotOptionalOrDie() const {
    RIEGELI_CHECK(*this)
        << "Failed precondition of "
           "IndexInternedString::Optional::NotOptionalOrDie(): "
           "null index";
    return NotOptional(numeric_or_max());
  }

  // Equality of non-null `IndexInternedString` or
  // `IndexInternedString::Optional` objects corresponds to equality of the
  // strings they refer to, but is fast, comparing the indices.
  //
  // Other comparisons sort strings by the order of their construction in the
  // interner, with null being the minimum.
  //
  // All comparisons are valid only for `IndexInternedString` or
  // `IndexInternedString::Optional` objects coming from the same interner.

  friend bool operator==(Optional a, Optional b) {
    return a.numeric_or_max() == b.numeric_or_max();
  }
  friend StrongOrdering RIEGELI_COMPARE(Optional a, Optional b) {
    return riegeli::Compare(a.ordered_numeric(), b.ordered_numeric());
  }
  friend bool operator==(Optional a, std::nullptr_t) { return !a; }
  friend StrongOrdering RIEGELI_COMPARE(Optional a, std::nullptr_t) {
    if (!a) return StrongOrdering::equal;
    return StrongOrdering::greater;
  }

  // Hashing `IndexInternedString` or `IndexInternedString::Optional` is fast,
  // hashing the index.
  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, Optional self) {
    return HashState::combine(std::move(hash_state), self.numeric_or_max());
  }

  // Default hash and equality for containers with `IndexInternedString` or
  // `IndexInternedString::Optional` as the key type, hashing and comparing by
  // index, supporting heterogeneous lookup against `NotOptional` and
  // `Optional`.
  struct absl_container_hash;
  struct absl_container_eq;

  // Supports `riegeli::Debug()`.
  template <typename DebugStream>
  friend void RiegeliDebug(Optional src, DebugStream& dest) {
    if (src == nullptr) {
      dest.Debug(nullptr);
    } else {
      dest.Debug(src.numeric_or_max());
    }
  }

 protected:
  explicit OptionalIndexInternedString(Numeric numeric) : numeric_(numeric) {}

  Numeric numeric_or_max() const { return numeric_; }

  Numeric ordered_numeric() const {
    if constexpr (std::is_signed_v<Numeric>) {
      return numeric_ == kNullNumeric<Numeric> ? -1 : numeric_;
    } else {
      return static_cast<Numeric>(numeric_ + 1);
    }
  }

 private:
  friend NotOptional;  // For `ordered_numeric()`.
  // For `Optional(Numeric)`.
  template <typename NumericParam, typename EncoderParam, typename TagParam,
            typename Address, typename MutexParam, size_t num_shards,
            size_t alignment_param, size_t static_min_block_size,
            size_t static_max_block_size>
  friend class IndexStringInterner;

  Numeric numeric_ = kNullNumeric<Numeric>;
};

}  // namespace interned_internal

// `IndexInternedString<Numeric>` refers to a string by a numeric index,
// ensuring that equal strings are shared to minimize memory usage.
//
// `IndexInternedString` is never null. See
// `IndexInternedString::Optional` for a variant that can be null.
// `IndexInternedString` is generally preferred over
// `IndexInternedString::Optional`.
//
// See `IndexInterned` for a general variant supporting other types of
// objects.
//
// `IndexInternedString` objects are created by an interner, which
// maintains a set of arena-allocated strings to share. The interner is managed
// explicitly. Interned strings are destroyed and erased when the interner is
// destroyed.
//
// Since strings are owned by the interner, using an arena interner risks
// exhausting the numeric space or address space, or running out of memory
// unless the number of distinct strings ever interned by the given interner is
// limited.
//
// See `ArenaInternedString` for a variant that refers to strings by a
// pointer-like handle. An index can be more compact than a pointer and indices
// are allocated consecutively, which allows representing dense maps as vectors,
// but the interner is needed to resolve an index to the string and the numeric
// space or address space can be exhausted. In contrast to
// `ArenaInternedString::GlobalInterner`, a global version of
// `IndexInternedString::Interner` is not provided.
//
// By default, interning a string supports heterogeneous lookup against
// `absl::Cord`. To extend this to other types, provide `Encoder` which provides
// `Hash` and `Eq` type aliases and the following static members:
// ```
//   using Hash = ...;
//   using Eq = ...;
//   static bool EncodedEmpty(const T& src);
//   static size_t EncodedSize(const T& src);
//   static void Encode(const T& src, char* dest);
// ```
//
// Asymptotic memory usage per interned string, assuming length up to 127:
//   for `Interner`:
//     non-concurrent: length + 1.65 * sizeof(Numeric) + 12.5
//     concurrent: length + 1.65 * sizeof(Numeric) + 24.5
//     archived: length + 9
//   for `InternerWithAddress`:
//     non-concurrent:
//       length + 1.65 * sizeof(Numeric) + 1.23 * sizeof(Address) + 2.65
//     concurrent:
//       length + 1.65 * sizeof(Numeric) + 2.73 * sizeof(Address) + 2.65
//     archived: length + sizeof(Address) + 1
//
// Breakdown:
//  + entry in `absl::flat_hash_set<Numeric>`:
//      8 / (7 * ln(2)) * (sizeof(Numeric) + 1) unless archived
//  for `Interner`:
//    + entry in `riegeli::ConcurrentVector<ArenaString>`:
//        non-concurrent: (1.5 - 1) / ln(1.5) * 8
//        concurrent: 3 / ln(3) * 8
//        archived: 8
//  for `InternerWithAddress`:
//    + entry in `riegeli::ConcurrentVector<Address>`:
//        non-concurrent: (1.5 - 1) / ln(1.5) * sizeof(Address)
//        concurrent: 3 / ln(3) * sizeof(Address)
//        archived: sizeof(Address)
//  + arena-allocated {
//    + length: 1, 2, or 8
//    + contents: length
//  }
//
// Interned handle: sizeof(Numeric)
//
// Among the template parameters, only `Numeric` and optionally `Encoder` should
// be specified explicitly. Other parameters should be specified by nested types
// `WithTag` and `WithAlignment`. Further parameters are applied to `Interner`
// (or `InternerWithAddress`), or `Archive` (or `ArchiveWithAddress`).
//
// `IndexInternedString` derives from `IndexInternedString::Optional`.
// See `IndexInternedString::Optional` for inherited operations.
template <typename Numeric, typename Encoder = DefaultStringEncoder,
          typename Tag = void, size_t alignment = 1>
class IndexInternedString
    : public interned_internal::OptionalIndexInternedString<Numeric, Encoder,
                                                            Tag, alignment>,
      public WithCompare<IndexInternedString<Numeric, Encoder, Tag, alignment>,
                         interned_internal::OptionalIndexInternedString<
                             Numeric, Encoder, Tag, alignment>,
                         std::nullptr_t> {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Changes the tag type of the interner.
  //
  // Index interned strings with distinct tags are managed by separate types of
  // interners, even if other template parameters are the same. This allows
  // annotating the type with its role for improved type safety. This forces
  // separation of interners, which can make lookups more efficient.
  template <typename NewTag>
  using WithTag = IndexInternedString<Numeric, Encoder, NewTag, alignment>;

  // Configures the alignment of string data.
  //
  // This guarantees that the string data begins at a memory address that is a
  // multiple of `alignment`, which must be a power of 2. This is useful when
  // they can be `reinterpret_cast` to a type with the required alignment.
  template <size_t new_alignment>
  using WithAlignment =
      IndexInternedString<Numeric, Encoder, Tag, new_alignment>;

  // Navigates between `IndexInternedString` and
  // `IndexInternedString::Optional`.
  using NotOptional = IndexInternedString;
  using Optional = typename IndexInternedString::Optional;

  // The type of the arena-interned string resolved from an index.
  //
  // `IndexInternedString` resolves to an `ArenaInternedString` given
  // the interner, which implicitly refers to a string.
  using Resolved = typename IndexInternedString::Resolved;

  // The default interner type. It is used for interning new strings, for
  // resolving indices to strings, and provides statistics.
  //
  // Further parameters should be specified by `Interner` nested types
  // `Concurrent` and `WithBlockSize`.
  using Interner = typename IndexInternedString::Interner;

  // The default archive type. It can be used to hold interned strings after
  // all strings have been interned and the `Interner` has been destroyed, for
  // resolving indices to strings, and provides statistics.
  //
  // Further parameters should be specified by `Archive` nested type
  // `WithBlockSize`.
  using Archive = typename IndexInternedString::Archive;

  // An interner type using a numeric address rather than pointers in the
  // directory.
  //
  // An internal directory stores numeric addresses to the string arena rather
  // than pointers. This uses less memory, but resolving an index is slightly
  // slower.
  //
  // `Address` must be an unsigned integer type.
  //
  // An address consists of the block index and the offset within the block.
  // The address type should be wide enough to address all string contents,
  // together with lengths stored before the contents, and unused space at the
  // end of a block. This is an estimation; there is no guarantee exactly how
  // much address space is needed for a particular set of strings.
  //
  // `AddressSpaceUsed()` on the interner or archive can be used to inspect the
  // current address space usage against `AddressSpaceLimit()`.
  template <typename Address>
  using InternerWithAddress =
      typename IndexInternedString::template InternerWithAddress<Address>;

  // An archive type using a numeric address rather than pointers in the
  // directory.
  //
  // `Address` must be an unsigned integer type.
  template <typename Address>
  using ArchiveWithAddress =
      typename IndexInternedString::template ArchiveWithAddress<Address>;

  // The default constructor is present in `IndexInternedString::Optional`
  // but deleted in `IndexInternedString`.
  IndexInternedString() = delete;

  // Constructor from `nullptr` is present in `IndexInternedString::Optional`
  // but deleted in `IndexInternedString`.
  IndexInternedString(std::nullptr_t) = delete;
  IndexInternedString& operator=(std::nullptr_t) = delete;

  IndexInternedString(const IndexInternedString& that) = default;
  IndexInternedString& operator=(const IndexInternedString& that) = default;

  // Returns `true` because `IndexInternedString` is never null.
  explicit operator bool() const { return true; }

  // Returns this index.
  NotOptional value() const { return *this; }

  // Returns the numeric value. Indices are allocated consecutively and are
  // non-negative even if `Numeric` is signed.
  //
  // `std::numeric_limits<Numeric>::max()` is unused.
  Numeric numeric() const { return this->numeric_or_max(); }

  // Equality of `IndexInternedString` objects corresponds to equality of the
  // strings they refer to, but is fast, comparing the indices.
  //
  // Other comparisons sort strings by the order of their construction in the
  // interner.
  //
  // All comparisons are valid only for `IndexInternedString` objects coming
  // from the same interner.

  friend bool operator==(NotOptional a, NotOptional b) {
    return a.numeric() == b.numeric();
  }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional a, NotOptional b) {
    return riegeli::Compare(a.numeric(), b.numeric());
  }

  friend bool operator==(NotOptional a, Optional b) {
    return a.numeric() == b.numeric_or_max();
  }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional a, Optional b) {
    return riegeli::Compare(a.ordered_numeric(), b.ordered_numeric());
  }

  friend bool operator==(NotOptional /*a*/, std::nullptr_t) { return false; }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional /*a*/, std::nullptr_t) {
    return StrongOrdering::greater;
  }

  // Restores an `IndexInternedString` from a numeric index of a previously
  // interned string.
  //
  // `numeric` must not be `std::numeric_limits<Numeric>::max()` and must be
  // non-negative.
  static NotOptional BackFromNumeric(Numeric numeric) {
    RIEGELI_ASSERT_NE(numeric, interned_internal::kNullNumeric<Numeric>)
        << "Failed precondition of IndexInternedString::BackFromNumeric(): "
           "null numeric value";
    if constexpr (std::is_signed_v<Numeric>) {
      RIEGELI_ASSERT_GE(numeric, 0)
          << "Failed precondition of IndexInternedString::BackFromNumeric(): "
             "negative numeric value";
    }
    return NotOptional(numeric);
  }

  // Supports `HybridDirectMap` and `HybridDirectSet`.
  friend std::make_unsigned_t<Numeric> RiegeliHybridDirectToRawKey(
      NotOptional key) {
    return IntCast<std::make_unsigned_t<Numeric>>(key.numeric());
  }
  friend NotOptional RiegeliHybridDirectFromRawKey(
      std::make_unsigned_t<Numeric> raw_key, NotOptional*) {
    return BackFromNumeric(IntCast<Numeric>(raw_key));
  }

 private:
  friend Optional;  // For `IndexInternedString(Numeric)`.
  // For `IndexInternedString(Numeric)`.
  template <typename NumericParam, typename EncoderParam, typename TagParam,
            typename AddressParam, typename MutexParam, size_t num_shards,
            size_t alignment_param, size_t static_min_block_size,
            size_t static_max_block_size>
  friend class interned_internal::IndexStringInterner;

  explicit IndexInternedString(Numeric numeric) : Optional(numeric) {}
};

namespace interned_internal {

// The public name of `IndexStringArchive<Numeric>` is
// `IndexInternedString<Numeric>::Archive`.
//
// `IndexInternedString::Archive` holds interned strings after all strings have
// been interned and the `Interner` has been destroyed, and is used to resolve
// indices to strings, and provides statistics.
//
// Strings obtained from the `Interner` remain valid as long as the `Archive`
// is valid.
//
// This saves memory by releasing the lookup structures of the `Interner`
// once they are no longer needed.
template <typename Numeric, typename Tag, typename Address, size_t alignment,
          size_t static_min_block_size, size_t static_max_block_size>
class IndexStringArchive {
 public:
  static_assert(absl::has_single_bit(alignment));
  static_assert(std::is_void_v<Address> ||
                    (static_min_block_size > 0 &&
                     static_min_block_size == static_max_block_size),
                "IndexInternedString::ArchiveWithAddress requires "
                "a static fixed block size");

  // Configures the block size of the arena, in bytes. See
  // `IndexInternedString::Interner::WithBlockSize` for details.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize =
      IndexStringArchive<Numeric, Tag, Address, alignment,
                         new_static_min_block_size, new_static_max_block_size>;

  // Configures the block size of the arena to be dynamic. See
  // `IndexInternedString::Interner::WithDynamicBlockSize` for details.
  using WithDynamicBlockSize =
      IndexStringArchive<Numeric, Tag, Address, alignment,
                         /*static_min_block_size=*/0,
                         /*static_max_block_size=*/0>;

  // Creates an empty `Archive`.
  IndexStringArchive()
      : arena_([] {
          if constexpr (static_min_block_size == 0 &&
                        static_max_block_size == 0) {
            // The arena of a default-constructed `Archive` is always empty.
            // Its dynamic block size does not matter.
            return Arena(0);
          } else {
            static_assert(
                static_min_block_size > 0 && static_max_block_size > 0,
                "static_min_block_size and static_max_block_size "
                "must be both zero or both positive");
            return Arena();
          }
        }()) {}

  // A moved-from `Archive` is left empty.
  IndexStringArchive(IndexStringArchive&& that) = default;
  IndexStringArchive& operator=(IndexStringArchive&& that) = default;

  // `size()` is the same as `NumObjects()`. The name `size()` indicates that
  // it is efficient, not involving locking.
  size_t size() const { return directory_.size(); }

  // Resolves an `IndexInternedString` to the string.
  //
  // `index` must have been provided by the `Interner` from which this `Archive`
  // was extracted.
  template <typename Encoder>
  BasicArenaInternedString<Encoder, Tag, alignment> operator[](
      IndexInternedString<Numeric, Encoder, Tag, alignment> index) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_CHECK_LT(IntCast<size_t>(index.numeric()), size())
        << "Failed precondition of "
           "IndexInternedString::Archive::operator[]: "
           "index out of bounds";
    if constexpr (std::is_void_v<Address>) {
      return BasicArenaInternedString<Encoder, Tag, alignment>::BackFromData(
          directory_[IntCast<size_t>(index.numeric())].data());
    } else {
      return BasicArenaInternedString<Encoder, Tag, alignment>::BackFromData(
          arena_
              .template ResolveAddress<alignment>(
                  IntCast<size_t>(directory_[IntCast<size_t>(index.numeric())]))
              .data());
    }
  }

  // Returns the number of strings in the archive. It does not change.
  size_t NumObjects() const { return size(); }

  // Returns an estimate of the usage of the address space. Comparing that
  // against `AddressSpaceLimit()` can be used to check how close the address
  // space is to exhaustion, or whether applying `InternerWithAddress` with a
  // particular address type would be safe.
  //
  // If the address space approaches exhaustion, widen the `Address` type, or
  // omit `InternerWithAddress` altogether to use pointers, at the cost of
  // increasing memory usage.
  size_t AddressSpaceUsed() const {
    return arena_.template AddressSpaceUsed<alignment>();
  }

  // Returns the approximate upper bound of `AddressSpaceUsed()`.
  //
  // Because the address limit applies to the start address of a string and
  // string contents extend past that, `AddressSpaceUsed()` can slightly exceed
  // `AddressSpaceLimit()` after the last successful allocation.
  template <typename TargetAddress = Address>
  static constexpr size_t AddressSpaceLimit() {
    if constexpr (std::is_void_v<TargetAddress>) {
      return std::numeric_limits<size_t>::max();
    } else {
      return SaturatingAdd(
          SaturatingIntCast<size_t>(std::numeric_limits<TargetAddress>::max()),
          size_t{1});
    }
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const IndexStringArchive* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->arena_);
    memory_estimator.RegisterSubobjects(&self->directory_);
  }

 private:
  // For `IndexStringArchive(Arena&&, Directory&&)`.
  template <typename NumericParam, typename Encoder, typename TagParam,
            typename AddressParam, typename MutexParam, size_t num_shards,
            size_t alignment_param, size_t other_static_min_block_size,
            size_t other_static_max_block_size>
  friend class IndexStringInterner;

  using Element = ArenaString::WithAlignment<alignment>;
  using Arena =
      StringArena::WithBlockSize<static_min_block_size, static_max_block_size>;
  using DirectoryElement =
      std::conditional_t<std::is_void_v<Address>, Element, Address>;
  using Directory =
      StringDirectory<DirectoryElement, /*concurrent_reads=*/false>;

  explicit IndexStringArchive(Arena&& arena, Directory&& directory)
      : arena_(std::move(arena)), directory_(std::move(directory)) {
    directory_.ShrinkToFit();
  }

  Arena arena_;
  Directory directory_;
};

// The public name of `IndexStringInterner<Numeric>` is
// `IndexInternedString<Numeric>::Interner`.
//
// `IndexInternedString::Interner` represents an explicitly managed
// interner. It arena-allocates and manages a set of interned strings. The
// strings are owned by the interner and are destroyed when the interner is
// destroyed.
template <typename Numeric, typename Encoder, typename Tag, typename Address,
          typename Mutex, size_t num_shards, size_t alignment,
          size_t static_min_block_size, size_t static_max_block_size>
class IndexStringInterner {
 public:
  static_assert(absl::has_single_bit(alignment));
  static_assert(std::is_void_v<Address> ||
                    (static_min_block_size > 0 &&
                     static_min_block_size == static_max_block_size),
                "IndexInternedString::InternerWithAddress requires "
                "a static fixed block size");

  // Makes the interner thread-safe and tunes it for concurrency.
  //
  // By default, the interner is not thread-safe and has a single shard.
  // With more shards, parallel usage is less likely to cause contention.
  //
  // `Mutex` protects the set of string indices in each shard.
  //
  // A mutex must support `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent =
      IndexStringInterner<Numeric, Encoder, Tag, Address, NewMutex,
                          new_num_shards, alignment, static_min_block_size,
                          static_max_block_size>;

  // Configures the block size of the arena, in bytes.
  //
  // Strings are allocated in blocks of this size. A larger block size improves
  // memory locality and reduces the number of allocations, but increases wasted
  // memory if only a small number of strings is interned.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize =
      IndexStringInterner<Numeric, Encoder, Tag, Address, Mutex, num_shards,
                          alignment, new_static_min_block_size,
                          new_static_max_block_size>;

  // Configures the block size of the arena to be specified dynamically in the
  // constructor.
  using WithDynamicBlockSize =
      IndexStringInterner<Numeric, Encoder, Tag, Address, Mutex, num_shards,
                          alignment, /*static_min_block_size=*/0,
                          /*static_max_block_size=*/0>;

  // References to interned strings. See `IndexInternedString` and
  // `IndexInternedString::Optional` for details.
  using Index = IndexInternedString<Numeric, Encoder, Tag, alignment>;
  using OptionalIndex =
      OptionalIndexInternedString<Numeric, Encoder, Tag, alignment>;

  // The type of the arena-interned string resolved from an index.
  // See `IndexInternedString::Resolved` for details.
  using Resolved = BasicArenaInternedString<Encoder, Tag, alignment>;

  // The archive type. See `IndexInternedString::Archive` for details.
  using Archive =
      IndexStringArchive<Numeric, Tag, Address, alignment,
                         static_min_block_size, static_max_block_size>;

  // Creates an empty `Interner` with a static block size.
  IndexStringInterner() noexcept {
    static_assert(static_min_block_size > 0 && static_max_block_size > 0);
  }

  // Creates an empty `Interner` with a fixed dynamic block size.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<dependent_static_min_block_size == 0 &&
                                 dependent_static_max_block_size == 0,
                             int> = 0>
  explicit IndexStringInterner(size_t block_size) : arena_(block_size) {}

  // Creates an empty `Interner` with an adaptive dynamic block size.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<dependent_static_min_block_size == 0 &&
                                 dependent_static_max_block_size == 0,
                             int> = 0>
  explicit IndexStringInterner(size_t min_block_size, size_t max_block_size)
      : arena_(min_block_size, max_block_size) {}

  IndexStringInterner(const IndexStringInterner&) = delete;
  IndexStringInterner& operator=(const IndexStringInterner&) = delete;

  // Resets the interner to the empty state.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<(dependent_static_min_block_size > 0 &&
                              dependent_static_max_block_size > 0),
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    arena_.Reset();
    directory_.Reset();
    ResetShards();
  }

  // Resets the interner to the empty state with a fixed dynamic block size.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<dependent_static_min_block_size == 0 &&
                                 dependent_static_max_block_size == 0,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t block_size) {
    arena_.Reset(block_size);
    directory_.Reset();
    ResetShards();
  }

  // Resets the interner to the empty state with an adaptive dynamic block size.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<dependent_static_min_block_size == 0 &&
                                 dependent_static_max_block_size == 0,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t min_block_size,
                                          size_t max_block_size) {
    arena_.Reset(min_block_size, max_block_size);
    directory_.Reset();
    ResetShards();
  }

  // Prepares the interner for the expected number of distinct strings and
  // optionally the expected total size of allocated strings in bytes.
  // This reduces reallocations.
  void Reserve(size_t capacity, size_t bytes_capacity = 0) {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInternedString::Interner::Reserve(): "
           "interner is archived in-place";
    if (capacity > 0) {
      {
        MutexLock<ArenaMutex> arena_lock(arena_mutex_);
        directory_.Reserve(capacity);
      }
      const size_t capacity_per_shard = capacity / num_shards;
      if (capacity_per_shard > 0) {
        for (Shard& shard : shards_) {
          shard.Reserve(capacity_per_shard);
        }
      }
    }
    if (bytes_capacity > 0) {
      MutexLock<ArenaMutex> arena_lock(arena_mutex_);
      arena_.ReserveBytes(bytes_capacity);
    }
  }

  // `size()` is the same as `NumObjects()`. The name `size()` indicates that
  // it is efficient, not involving locking.
  size_t size() const { return directory_.size(); }

  // Resolves an `IndexInternedString` to the string.
  //
  // `index` must have been provided by this `Interner`.
  Resolved operator[](Index index) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_CHECK_LT(IntCast<size_t>(index.numeric()), size())
        << "Failed precondition of "
           "IndexInternedString::Interner::operator[]: "
           "index out of bounds";
    if constexpr (std::is_void_v<Address>) {
      return Resolved::BackFromData(
          directory_[IntCast<size_t>(index.numeric())].data());
    } else {
      return Resolved::BackFromData(
          arena_
              .template ResolveAddress<alignment>(
                  IntCast<size_t>(directory_[IntCast<size_t>(index.numeric())]))
              .data());
    }
  }

  // Optimized overload for an empty string.
  OptionalIndex Intern() { return Intern(absl::string_view()); }

  // Creates an `IndexInternedString` referring to the constructed string,
  // or sharing an existing string if an equal string already exists.
  //
  // `Intern()` returns null if the numeric space or address space for a new
  // index is exhausted.
  //
  // If `likely_new` is `true`, `Intern()` is optimized for the case where an
  // equal string does not exist yet.
  //
  // If `is_new != nullptr`, `*is_new` is set to `true` if a new string was
  // created, or `false` if an equal string already existed.
  template <bool likely_new = false, typename Arg,
            std::enable_if_t<interned_internal::SupportedByEncoderForIntern<
                                 Arg, Encoder>::value,
                             int> = 0>
  OptionalIndex Intern(const Arg& arg, bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(InternInternal<likely_new>(arg, is_new));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <bool likely_new = false, typename DependentEncoder = Encoder,
            std::enable_if_t<interned_internal::SupportedByEncoderForIntern<
                                 absl::string_view, DependentEncoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalIndex
  Intern(const char* arg, bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(
        InternInternal<likely_new>(absl::string_view(arg), is_new));
  }

  // Const `Intern()` overload enabled only when thread-safe.
  //
  // `Intern()` returns null if the numeric space or address space for a new
  // index is exhausted.
  //
  // If `likely_new` is `true`, `Intern()` is optimized for the case where an
  // equal string does not exist yet.
  //
  // If `is_new != nullptr`, `*is_new` is set to `true` if a new string was
  // created, or `false` if an equal string already existed.

  template <
      typename DependentMutex = Mutex,
      std::enable_if_t<!std::is_same_v<DependentMutex, NullMutex>, int> = 0>
  OptionalIndex Intern() const {
    return Intern(absl::string_view());
  }

  template <
      bool likely_new = false, typename Arg, typename DependentMutex = Mutex,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_same<DependentMutex, NullMutex>>,
              interned_internal::SupportedByEncoderForIntern<Arg, Encoder>>,
          int> = 0>
  OptionalIndex Intern(const Arg& arg,
                       bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(InternInternal<likely_new>(arg, is_new));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <bool likely_new = false, typename DependentMutex = Mutex,
            typename DependentEncoder = Encoder,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    interned_internal::SupportedByEncoderForIntern<
                        absl::string_view, DependentEncoder>>,
                int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalIndex
  Intern(const char* arg, bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(
        InternInternal<likely_new>(absl::string_view(arg), is_new));
  }

  // Finds an existing `IndexInternedString` matching the given argument, or
  // returns null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the string already exists, such as looking up in a map
  // with interned keys.
  template <typename Arg,
            std::enable_if_t<interned_internal::SupportedByEncoderForIntern<
                                 Arg, Encoder>::value,
                             int> = 0>
  OptionalIndex Find(const Arg& arg) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInternedString::Interner::Find(): "
           "interner is archived in-place";
    const size_t hash = typename Encoder::Hash()(arg);
    return OptionalIndex(GetShard(hash).Find(arg, hash));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentEncoder = Encoder,
            std::enable_if_t<interned_internal::SupportedByEncoderForIntern<
                                 absl::string_view, DependentEncoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalIndex Find(const char* arg) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInternedString::Interner::Find(): "
           "interner is archived in-place";
    const absl::string_view string_view_arg(arg);
    const size_t hash = typename Encoder::Hash()(string_view_arg);
    return OptionalIndex(GetShard(hash).Find(string_view_arg, hash));
  }

  // Returns the `IndexInternedString` referring to the same string as `value`.
  //
  // `value` must have been interned in this interner.
  Index IndexOf(Resolved value) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInternedString::Interner::IndexOf(): "
           "interner is archived in-place";
    const size_t hash = typename Encoder::Hash()(absl::string_view(value));
    return Index(GetShard(hash).IndexOf(value, hash));
  }

  // Returns the number of strings managed by the interner.
  size_t NumObjects() const { return size(); }

  // Returns an estimate of the usage of the address space. Comparing that
  // against `AddressSpaceLimit()` can be used to check how close the address
  // space is to exhaustion, or whether applying `InternerWithAddress` with a
  // particular address type would be safe.
  //
  // If the address space approaches exhaustion, widen the `Address` type, or
  // omit `InternerWithAddress` altogether to use pointers, at the cost of
  // increasing memory usage.
  size_t AddressSpaceUsed() const {
    ReaderMutexLock<ArenaMutex> arena_lock(arena_mutex_);
    return arena_.template AddressSpaceUsed<alignment>();
  }

  // Returns the approximate upper bound of `AddressSpaceUsed()`.
  //
  // Because the address limit applies to the start address of a string and
  // string contents extend past that, `AddressSpaceUsed()` can slightly exceed
  // `AddressSpaceLimit()` after the last successful allocation.
  template <typename TargetAddress = Address>
  static constexpr size_t AddressSpaceLimit() {
    if constexpr (std::is_void_v<TargetAddress>) {
      return std::numeric_limits<size_t>::max();
    } else {
      return SaturatingAdd(
          SaturatingIntCast<size_t>(std::numeric_limits<TargetAddress>::max()),
          size_t{1});
    }
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const IndexStringInterner* self,
                                        MemoryEstimator& memory_estimator) {
    {
      ReaderMutexLock<ArenaMutex> arena_lock(self->arena_mutex_);
      memory_estimator.RegisterSubobjects(&self->arena_);
      memory_estimator.RegisterSubobjects(&self->directory_);
    }
    memory_estimator.RegisterSubobjects(&self->shards_);
  }

  // Shrinks capacity of internal data structures to fit their current sizes.
  void ShrinkToFit() {
    MutexLock<ArenaMutex> arena_lock(arena_mutex_);
    directory_.ShrinkToFit();
  }

  // Extracts the storage of the strings as an `Archive`. The `Interner` is left
  // empty.
  //
  // See `IndexInternedString::Archive` for details.
  Archive ExtractArchive() && {
    for (Shard& shard : shards_) {
      shard.Archive();
    }
    return Archive(std::move(arena_).ExtractArchive(),
                   std::move(directory_).ExtractArchive());
  }

  // Archives the storage of the strings in place, releasing the lookup
  // structures of the `Interner`.
  //
  // This saves memory once all strings have been interned.
  //
  // `Intern()` and `Find()` must not be called after `ArchiveInPlace()`.
  //
  // `ExtractArchive()` is preferred because it avoids putting the interner in a
  // partially usable state.
  void ArchiveInPlace() {
    if (is_archived_in_place_) return;
    is_archived_in_place_ = true;
    for (Shard& shard : shards_) {
      shard.Archive();
    }
    MutexLock<ArenaMutex> arena_lock(arena_mutex_);
    directory_.ShrinkToFit();
  }

 private:
  static constexpr bool kConcurrent = !std::is_same_v<Mutex, NullMutex>;
  static constexpr bool kArenaConcurrentReads =
      kConcurrent && !std::is_void_v<Address>;

  using ArenaMutex = std::conditional_t<kConcurrent, absl::Mutex, NullMutex>;
  using Element = ArenaString::WithAlignment<alignment>;
  using DirectoryElement =
      std::conditional_t<std::is_void_v<Address>, Element, Address>;
  using Arena =
      typename StringArena::WithConcurrentReads<kArenaConcurrentReads>::
          template WithBlockSize<static_min_block_size, static_max_block_size>;
  using Directory = StringDirectory<DirectoryElement, kConcurrent>;
  using Shard =
      IndexStringInternerShard<Numeric, Encoder, Address, Mutex, ArenaMutex,
                               alignment, static_min_block_size,
                               static_max_block_size>;

  void ResetShards() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    for (Shard& shard : shards_) {
      shard.Reset();
    }
    is_archived_in_place_ = false;
  }

  template <bool likely_new, typename Arg>
  Numeric InternInternal(const Arg& value, bool* absl_nullable is_new) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInternedString::Interner::Intern(): "
           "interner is archived in-place";
    const size_t hash = typename Encoder::Hash()(value);
    bool is_new_internal;
    Numeric result;
    if constexpr (likely_new) {
      result = GetShard(hash).template InternNew</*verified_new=*/false>(
          value, hash, arena_, directory_, arena_mutex_, is_new_internal);
    } else {
      result = GetShard(hash).Intern(value, hash, arena_, directory_,
                                     arena_mutex_, is_new_internal);
    }
    if (is_new != nullptr) *is_new = is_new_internal;
    return result;
  }

  template <size_t... indices>
  std::array<Shard, num_shards> MakeShards(std::index_sequence<indices...>) {
    return {((void)indices, Shard(&arena_, &directory_))...};
  }

  Shard& GetShard(size_t hash) const {
    return shards_[ShardIndex<num_shards>(hash)];
  }

  mutable ArenaMutex arena_mutex_;
  mutable Arena arena_;
  mutable Directory directory_;
  mutable std::array<Shard, num_shards> shards_{
      MakeShards(std::make_index_sequence<num_shards>())};
  bool is_archived_in_place_ = false;
};

// Implementation details follow.

template <typename Numeric, typename Encoder, typename Tag, size_t alignment>
struct OptionalIndexInternedString<Numeric, Encoder, Tag,
                                   alignment>::absl_container_hash {
  using is_transparent = void;
  size_t operator()(Optional self) const {
    return absl::HashOf(self.numeric_or_max());
  }
  size_t operator()(NotOptional self) const {
    return absl::HashOf(self.numeric());
  }
  size_t operator()(std::nullptr_t) const {
    return absl::HashOf(kNullNumeric<Numeric>);
  }
};

template <typename Numeric, typename Encoder, typename Tag, size_t alignment>
struct OptionalIndexInternedString<Numeric, Encoder, Tag,
                                   alignment>::absl_container_eq {
  using is_transparent = void;
  bool operator()(Optional a, Optional b) const {
    return a.numeric_or_max() == b.numeric_or_max();
  }
  bool operator()(Optional a, NotOptional b) const {
    return a.numeric_or_max() == b.numeric();
  }
  bool operator()(NotOptional a, Optional b) const {
    return a.numeric() == b.numeric_or_max();
  }
  bool operator()(NotOptional a, NotOptional b) const {
    return a.numeric() == b.numeric();
  }
  bool operator()(Optional a, std::nullptr_t) const { return !a; }
  bool operator()(std::nullptr_t, Optional b) const { return !b; }
  bool operator()(NotOptional /*a*/, std::nullptr_t) const { return false; }
  bool operator()(std::nullptr_t, NotOptional /*b*/) const { return false; }
};

}  // namespace interned_internal

}  // namespace riegeli

#endif  // RIEGELI_INTERNED_INDEX_INTERNED_STRING_H_
