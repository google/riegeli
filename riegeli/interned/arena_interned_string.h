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

#ifndef RIEGELI_INTERNED_ARENA_INTERNED_STRING_H_
#define RIEGELI_INTERNED_ARENA_INTERNED_STRING_H_

#include <stddef.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <ostream>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/hash/hash.h"
#include "absl/numeric/bits.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/global.h"
#include "riegeli/base/iterable.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/interned/arena_interned_string_internal.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/string_arena.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Default template parameter `Encoder` for `ArenaInternedString`.
using interned_internal::DefaultStringEncoder;

// Mutex type that does not lock.
using interned_internal::NullMutex;

// Default template parameter `num_shards` for
// `ArenaInternedString::GlobalInterner`.
// Also, a default template parameter for `Concurrent` nested types.
using interned_internal::kDefaultInternerNumShards;

// Default template parameters for `ArenaInternedString::Interner`
// and `ArenaInternedString::GlobalInterner`.
using interned_internal::kDefaultArenaMaxBlockSize;
using interned_internal::kDefaultArenaMinBlockSize;

template <typename Encoder, typename Tag, size_t alignment>
class BasicArenaInternedString;

namespace interned_internal {

template <typename Tag, size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
class LocalArenaStringArchive;

template <typename Encoder, typename Tag, typename Mutex, size_t num_shards,
          size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
class LocalArenaStringInterner;

template <typename Encoder, typename Tag, typename Mutex, size_t num_shards,
          size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
class GlobalArenaStringInterner;

// The public name of `OptionalArenaInternedString` is
// `ArenaInternedString::Optional`.
//
// `ArenaInternedString` refers to a string, ensuring that equal strings are
// shared to minimize memory usage.
//
// In contrast to `ArenaInternedString`, `ArenaInternedString::Optional` can be
// null. It is more efficient than `std::optional<ArenaInternedString>`.
//
// See `ArenaInternedString` for details.
template <typename Encoder, typename Tag, size_t alignment>
class OptionalArenaInternedString
    : public WithCompare<OptionalArenaInternedString<Encoder, Tag, alignment>,
                         std::nullptr_t> {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Navigates between `ArenaInternedString` and
  // `ArenaInternedString::Optional`.
  using NotOptional = BasicArenaInternedString<Encoder, Tag, alignment>;
  using Optional = OptionalArenaInternedString;

  // The underlying arena string type.
  using Element = ArenaString::WithAlignment<alignment>;

  // The default interner type. See `ArenaInternedString::Interner` for details.
  using Interner =
      LocalArenaStringInterner<Encoder, Tag, NullMutex, /*num_shards=*/1,
                               alignment, kDefaultArenaMinBlockSize,
                               kDefaultArenaMaxBlockSize>;

  // The archive type. See `ArenaInternedString::Archive` for details.
  using Archive =
      LocalArenaStringArchive<Tag, alignment, kDefaultArenaMinBlockSize,
                              kDefaultArenaMaxBlockSize>;

  // The global interner type. See `ArenaInternedString::GlobalInterner` for
  // details.
  using GlobalInterner = GlobalArenaStringInterner<
      Encoder, Tag, absl::Mutex, kDefaultInternerNumShards<absl::Mutex>,
      alignment, /*static_min_block_size=*/kDefaultArenaMaxBlockSize,
      kDefaultArenaMaxBlockSize>;

  // Maximum supported string size.
  static constexpr size_t kMaxSize = Element::kMaxSize;

  // Creates a null `ArenaInternedString::Optional`.
  //
  // This differs from the default constructor of `ArenaInternedString`.
  OptionalArenaInternedString() = default;
  /*implicit*/ OptionalArenaInternedString(std::nullptr_t) {}
  OptionalArenaInternedString& operator=(std::nullptr_t) {
    repr_ = nullptr;
    return *this;
  }

  OptionalArenaInternedString(const OptionalArenaInternedString& that) =
      default;
  OptionalArenaInternedString& operator=(
      const OptionalArenaInternedString& that) = default;

  // Returns `true` if not null.
  explicit operator bool() const { return get() != nullptr; }

  // Converts from `ArenaInternedString::Optional` to `ArenaInternedString`.
  NotOptional not_optional() const { return NotOptional(get().not_optional()); }
  NotOptional NotOptionalOrDie() const {
    return NotOptional(get().NotOptionalOrDie());
  }

  // Returns the underlying `ArenaString::Optional`.
  typename Element::Optional get() const { return repr_; }

  // Dereferences the pointer.
  absl::string_view operator*() const { return *get(); }
  ArrowProxy<absl::string_view> operator->() const {
    return ArrowProxy<absl::string_view>(*get());
  }

  // Dereferences the pointer, crashing the process if null.
  absl::string_view value() const { return get().value(); }

  // Equality of non-null `ArenaInternedString::Optional` objects corresponds to
  // equality of the strings they refer to, but is fast, comparing the pointers.
  //
  // Other comparisons are also consistent with the strings, but only the case
  // of equal strings is optimized.
  //
  // All comparisons are valid only for `ArenaInternedString::Optional` objects
  // coming from the same interner.
  friend bool operator==(Optional a, Optional b) {
    return a.get().by_address() == b.get().by_address();
  }
  friend StrongOrdering RIEGELI_COMPARE(Optional a, Optional b) {
    if (a.get().by_address() == b.get().by_address()) {
      return StrongOrdering::equal;
    }
    if (a.get() == nullptr) return StrongOrdering::less;
    if (b.get() == nullptr) return StrongOrdering::greater;
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(Optional a, std::nullptr_t) {
    return a.get() == nullptr;
  }
  friend StrongOrdering RIEGELI_COMPARE(Optional a, std::nullptr_t) {
    if (a.get() == nullptr) return StrongOrdering::equal;
    return StrongOrdering::greater;
  }

  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithEqualMarker<Other>, Other>>,
              HasEqual<absl::string_view, const Other&>>,
          int> = 0>
  friend bool operator==(Optional a, const Other& b) {
    if (a.get() == nullptr) return false;
    return *a == b;
  }
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithCompareMarker<Other>, Other>>,
              std::disjunction<HasCompare<absl::string_view, const Other&>,
                               HasLessThan<absl::string_view, const Other&>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(Optional a, const Other& b) {
    if constexpr (HasCompare<absl::string_view, const Other&>::value) {
      using Ordering = decltype(riegeli::Compare(*a, b));
      if (a.get() == nullptr) return Ordering(StrongOrdering::less);
      return riegeli::Compare(*a, b);
    } else {
      if (a.get() == nullptr) return StrongOrdering::less;
      if (*a == b) return StrongOrdering::equal;
      if (*a < b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }

#if !__cpp_impl_three_way_comparison
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithEqualMarker<Other>, Other>>,
              HasEqual<const Other&, absl::string_view>>,
          int> = 0>
  friend bool operator==(const Other& a, Optional b) {
    if (b.get() == nullptr) return false;
    return a == *b;
  }
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithCompareMarker<Other>, Other>>,
              std::disjunction<HasCompare<const Other&, absl::string_view>,
                               HasLessThan<const Other&, absl::string_view>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const Other& a, Optional b) {
    if constexpr (HasCompare<const Other&, absl::string_view>::value) {
      using Ordering = decltype(riegeli::Compare(a, *b));
      if (b.get() == nullptr) return Ordering(StrongOrdering::greater);
      return riegeli::Compare(a, *b);
    } else {
      if (b.get() == nullptr) return StrongOrdering::greater;
      if (a == *b) return StrongOrdering::equal;
      if (a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }
#endif

  // `ArenaInternedString::ByAddress` is implicitly convertible from
  // `ArenaInternedString` or `ArenaInternedString::Optional`, but instances
  // are compared by address. This is more efficient, but the order is
  // arbitrary, consistent within the process.
  //
  // `std::less<ByAddress>` can be used as a comparator for algorithms over
  // `ArenaInternedString` or `ArenaInternedString::Optional`.
  class ByAddress;

  // Returns this object wrapped in `ByAddress`.
  ByAddress by_address() const { return ByAddress(*this); }

  // Hashing `ArenaInternedString` or `ArenaInternedString::Optional` is fast,
  // hashing the pointer.
  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, Optional self) {
    return HashState::combine(std::move(hash_state), self.get().by_address());
  }

  // Default hash and equality for containers with `ArenaInternedString` or
  // `ArenaInternedString::Optional` as the key type, hashing and comparing by
  // address, supporting heterogeneous lookup against `NotOptional` and
  // `Optional`.
  struct absl_container_hash;
  struct absl_container_eq;

  // Hash and equality for containers with `ArenaInternedString` or
  // `ArenaInternedString::Optional` as the key type, consistent with the
  // underlying value, supporting heterogeneous lookup. This is opt-in because
  // heterogeneous hashing is more expensive than pointer hashing.
  struct ValueHash;
  struct ValueEq;

  // Supports `riegeli::Debug()`.
  template <typename DebugStream>
  friend void RiegeliDebug(Optional src, DebugStream& dest) {
    dest.Debug(src.get());
  }

 protected:
  explicit OptionalArenaInternedString(typename Element::Optional element)
      : repr_(element) {}

 private:
  friend NotOptional;  // For `Optional(Element::Optional)`.
  // For `Optional(Element::Optional)`.
  template <typename EncoderParam, typename TagParam, typename MutexParam,
            size_t num_shards, size_t alignment_param,
            size_t static_min_block_size, size_t static_max_block_size>
  friend class LocalArenaStringInterner;
  // For `Optional(Element::Optional)`.
  template <typename EncoderParam, typename TagParam, typename MutexParam,
            size_t num_shards, size_t alignment_param,
            size_t static_min_block_size, size_t static_max_block_size>
  friend class GlobalArenaStringInterner;

  typename Element::Optional repr_;
};

}  // namespace interned_internal

// The recommended name of `BasicArenaInternedString<>` with default template
// parameters is `ArenaInternedString`, avoiding spelling `<>` in the common
// case.
//
// `ArenaInternedString` refers to a string, ensuring that equal strings are
// shared to minimize memory usage.
//
// `ArenaInternedString` is never null. See `ArenaInternedString::Optional`
// for a variant that can be null. `ArenaInternedString` is generally preferred
// over `ArenaInternedString::Optional`.
//
// See `ArenaInterned` for a general variant supporting other types of objects.
//
// `ArenaInternedString` objects are created by an interner, which maintains
// a set of arena-allocated strings to share. An interner can be local (managed
// explicitly) or global (represented by a stateless type). The default is
// local.
//
// Interned strings are destroyed and erased only when the interner is
// destroyed. See `InternedString` for a variant that is slower but deletes
// individual strings when all references to them are dropped.
//
// Since strings are owned by the interner, using an arena interner risks
// running out of memory unless the number of distinct strings ever interned
// by the given interner is limited.
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
//   active: length + 15.8
//   archived: length + 1
//
// Breakdown:
//  + entry in `absl::flat_hash_set<const char*>`:
//      8 / (7 * ln(2)) * (8 + 1) unless archived
//  + arena-allocated {
//    + length: 1, 2, or 8
//    + contents: length
//  }
//
// Interned handle: 8
//
// Among the template parameters, only `Encoder` should be specified explicitly.
// Other parameters should be specified by nested types `WithTag` and
// `WithAlignment`. Further parameters are applied to `Interner` or
// `GlobalInterner`.
//
// `ArenaInternedString` derives from `ArenaInternedString::Optional`. See
// `ArenaInternedString::Optional` for inherited operations.
template <typename Encoder = DefaultStringEncoder, typename Tag = void,
          size_t alignment = 1>
class BasicArenaInternedString
    : public interned_internal::OptionalArenaInternedString<Encoder, Tag,
                                                            alignment>,
      public WithCompare<BasicArenaInternedString<Encoder, Tag, alignment>,
                         interned_internal::OptionalArenaInternedString<
                             Encoder, Tag, alignment>,
                         std::nullptr_t> {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Changes the tag type of the interner.
  //
  // Arena interned strings with distinct tags are managed by separate types of
  // interners, even if other template parameters are the same. This allows
  // annotating the type with its role for improved type safety. This forces
  // separation of interners, which can make lookups more efficient.
  template <typename NewTag>
  using WithTag = BasicArenaInternedString<Encoder, NewTag, alignment>;

  // Configures the alignment of string data.
  //
  // A larger alignment can be used to tag interned pointers in the lowest bits,
  // encoding other objects together with the pointers.
  //
  // Also, if the strings encode another type (supported by custom `Encoder`),
  // they can be `reinterpret_cast` to a type with the required alignment.
  template <size_t new_alignment>
  using WithAlignment = BasicArenaInternedString<Encoder, Tag, new_alignment>;

  // Navigates between `ArenaInternedString` and
  // `ArenaInternedString::Optional`.
  using NotOptional = typename BasicArenaInternedString::NotOptional;
  using Optional = typename BasicArenaInternedString::Optional;

  // The underlying arena string type.
  using Element = typename BasicArenaInternedString::Element;

  // The default interner type. It is used for interning new strings. The
  // interner also provides statistics.
  //
  // Further parameters should be specified by `Interner` nested types
  // `Concurrent` and `WithBlockSize`.
  using Interner = typename BasicArenaInternedString::Interner;

  // The archive type. It can be used to hold interned strings after all strings
  // have been interned and the `Interner` has been destroyed, and provides
  // statistics.
  using Archive = typename BasicArenaInternedString::Archive;

  // The global interner type. It is used for interning new strings into a
  // global interner.
  //
  // Further parameters should be specified by `GlobalInterner` nested types
  // `Concurrent` and `WithBlockSize`.
  using GlobalInterner = typename BasicArenaInternedString::GlobalInterner;

  // A default-constructed `ArenaInternedString` holds an empty string. The
  // empty string is immortal.
  //
  // This differs from the default constructor of
  // `ArenaInternedString::Optional`.
  BasicArenaInternedString() noexcept : Optional(Element()) {}

  // Constructor from `nullptr` is present in `ArenaInternedString::Optional`
  // but deleted in `ArenaInternedString`.
  BasicArenaInternedString(std::nullptr_t) = delete;
  BasicArenaInternedString& operator=(std::nullptr_t) = delete;

  BasicArenaInternedString(const BasicArenaInternedString& that) = default;
  BasicArenaInternedString& operator=(const BasicArenaInternedString& that) =
      default;

  // Restores an `ArenaInternedString` from a raw pointer to a previously
  // interned string.
  static NotOptional BackFromData(const char* data) {
    return NotOptional(Element::BackFromData(data));
  }

  // Returns `true` because `ArenaInternedString` is never null.
  explicit operator bool() const { return true; }

  // Returns the underlying `ArenaString`.
  Element get() const { return this->Optional::get().not_optional(); }

  // Dereferences the pointer.
  /*implicit*/ operator absl::string_view() const { return get(); }
  absl::string_view operator*() const { return *get(); }
  ArrowProxy<absl::string_view> operator->() const {
    return ArrowProxy<absl::string_view>(*get());
  }
  absl::string_view value() const { return get().value(); }

  bool empty() const { return get().empty(); }
  const char* data() const { return get().data(); }
  size_t size() const { return get().size(); }

  const char& operator[](size_t index) const { return get()[index]; }
  const char& at(size_t index) const { return get().at(index); }
  const char& front() const { return get().front(); }
  const char& back() const { return get().back(); }

  // Equality of `ArenaInternedString` objects corresponds to equality of the
  // strings they refer to, but is fast, comparing the pointers.
  //
  // Other comparisons are also consistent with the strings, but only the case
  // of equal strings is optimized.
  //
  // All comparisons are valid only for `ArenaInternedString` objects coming
  // from the same interner.

  friend bool operator==(NotOptional a, NotOptional b) {
    return a.get().by_address() == b.get().by_address();
  }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional a, NotOptional b) {
    if (a.get().by_address() == b.get().by_address()) {
      return StrongOrdering::equal;
    }
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(NotOptional a, Optional b) {
    return a.get().by_address() == b.get().by_address();
  }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional a, Optional b) {
    if (a.get().by_address() == b.get().by_address()) {
      return StrongOrdering::equal;
    }
    if (b.get() == nullptr) return StrongOrdering::greater;
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(NotOptional /*a*/, std::nullptr_t) { return false; }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional /*a*/, std::nullptr_t) {
    return StrongOrdering::greater;
  }

  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithEqualMarker<Other>, Other>>,
              HasEqual<absl::string_view, const Other&>>,
          int> = 0>
  friend bool operator==(NotOptional a, const Other& b) {
    return *a == b;
  }
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithCompareMarker<Other>, Other>>,
              std::disjunction<HasCompare<absl::string_view, const Other&>,
                               HasLessThan<absl::string_view, const Other&>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(NotOptional a, const Other& b) {
    if constexpr (HasCompare<absl::string_view, const Other&>::value) {
      return riegeli::Compare(*a, b);
    } else {
      if (*a == b) return StrongOrdering::equal;
      if (*a < b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }

#if !__cpp_impl_three_way_comparison
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithEqualMarker<Other>, Other>>,
              HasEqual<const Other&, absl::string_view>>,
          int> = 0>
  friend bool operator==(const Other& a, NotOptional b) {
    return a == *b;
  }
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithCompareMarker<Other>, Other>>,
              std::disjunction<HasCompare<const Other&, absl::string_view>,
                               HasLessThan<const Other&, absl::string_view>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const Other& a, NotOptional b) {
    if constexpr (HasCompare<const Other&, absl::string_view>::value) {
      return riegeli::Compare(a, *b);
    } else {
      if (a == *b) return StrongOrdering::equal;
      if (a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }
#endif

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, NotOptional src) {
    AbslStringify(dest, src.get());
  }

  friend std::ostream& operator<<(std::ostream& dest, NotOptional src) {
    return dest << src.get();
  }

 private:
  friend Optional;  // For `BasicArenaInternedString(Element)`.
  // For `BasicArenaInternedString(Element)`.
  template <typename EncoderParam, typename TagParam, typename MutexParam,
            size_t num_shards, size_t alignment_param,
            size_t static_min_block_size, size_t static_max_block_size>
  friend class interned_internal::LocalArenaStringInterner;
  // For `BasicArenaInternedString(Element)`.
  template <typename EncoderParam, typename TagParam, typename MutexParam,
            size_t num_shards, size_t alignment_param,
            size_t static_min_block_size, size_t static_max_block_size>
  friend class interned_internal::GlobalArenaStringInterner;

  explicit BasicArenaInternedString(Element element) : Optional(element) {}
};

// `ArenaInternedString` is `BasicArenaInternedString<>` with default
// template parameters, avoiding spelling `<>` in the common case.
// See `BasicArenaInternedString` for details.
using ArenaInternedString = BasicArenaInternedString<>;

namespace interned_internal {

// The public name of `LocalArenaStringArchive` is
// `ArenaInternedString::Archive`.
//
// `ArenaInternedString::Archive` holds interned strings after all strings have
// been interned and the `Interner` has been destroyed, and provides statistics.
//
// Strings obtained from the `Interner` remain valid as long as the `Archive`
// is valid.
//
// This saves memory by releasing the lookup structures of the `Interner`
// once they are no longer needed.
template <typename Tag, size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
class LocalArenaStringArchive {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Configures the block size of the arena, in bytes. See
  // `ArenaInternedString::Interner::WithBlockSize` for details.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize =
      LocalArenaStringArchive<Tag, alignment, new_static_min_block_size,
                              new_static_max_block_size>;

  // Configures the block size of the arena to be dynamic. See
  // `ArenaInternedString::Interner::WithDynamicBlockSize` for details.
  using WithDynamicBlockSize =
      LocalArenaStringArchive<Tag, alignment, /*static_min_block_size=*/0,
                              /*static_max_block_size=*/0>;

  // Creates an empty `Archive`.
  LocalArenaStringArchive()
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
  LocalArenaStringArchive(LocalArenaStringArchive&& that) noexcept
      : arena_(std::move(that.arena_)),
        num_objects_(std::exchange(that.num_objects_, 0)) {}
  LocalArenaStringArchive& operator=(LocalArenaStringArchive&& that) noexcept {
    arena_ = std::move(that.arena_);
    num_objects_ = std::exchange(that.num_objects_, 0);
    return *this;
  }

  // Returns the number of strings in the archive. It does not change.
  size_t NumObjects() const { return num_objects_; }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const LocalArenaStringArchive* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->arena_);
  }

 private:
  // For `LocalArenaStringArchive(Arena&&, size_t)`.
  template <typename Encoder, typename TagParam, typename MutexParam,
            size_t num_shards, size_t alignment_param,
            size_t other_static_min_block_size,
            size_t other_static_max_block_size>
  friend class LocalArenaStringInterner;

  using Arena =
      StringArena::WithBlockSize<static_min_block_size, static_max_block_size>;

  explicit LocalArenaStringArchive(Arena&& arena, size_t num_objects)
      : arena_(std::move(arena)), num_objects_(num_objects) {}

  Arena arena_;
  size_t num_objects_ = 0;
};

// The public name of `LocalArenaStringInterner` is
// `ArenaInternedString::Interner`.
//
// `ArenaInternedString::Interner` represents an explicitly managed interner.
// It arena-allocates and manages a set of interned strings. The strings are
// owned by the interner and are destroyed when the interner is destroyed.
//
// See `ArenaInternedString::GlobalInterner` for a global version.
template <typename Encoder, typename Tag, typename Mutex, size_t num_shards,
          size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
class LocalArenaStringInterner {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Makes the interner thread-safe and tunes it for concurrency.
  //
  // By default, a global interner is thread-safe and has multiple shards,
  // while a local interner is not thread-safe and has a single shard.
  // With more shards, parallel usage is less likely to cause contention.
  //
  // `Mutex` protects the set of string pointers in each shard.
  //
  // A mutex must support `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent =
      LocalArenaStringInterner<Encoder, Tag, NewMutex, new_num_shards,
                               alignment, static_min_block_size,
                               static_max_block_size>;

  // Configures the block size of the arena, in bytes.
  //
  // String data is allocated in blocks of sizes within this range. A larger
  // block size improves memory locality and reduces the number of allocations,
  // but increases wasted memory if only a small number of strings is interned.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize =
      LocalArenaStringInterner<Encoder, Tag, Mutex, num_shards, alignment,
                               new_static_min_block_size,
                               new_static_max_block_size>;

  // Configures the block size of the arena to be specified dynamically in the
  // constructor.
  using WithDynamicBlockSize =
      LocalArenaStringInterner<Encoder, Tag, Mutex, num_shards, alignment,
                               /*static_min_block_size=*/0,
                               /*static_max_block_size=*/0>;

  // References to interned strings. See `ArenaInternedString` and
  // `ArenaInternedString::Optional` for details.
  using Interned = BasicArenaInternedString<Encoder, Tag, alignment>;
  using OptionalInterned = OptionalArenaInternedString<Encoder, Tag, alignment>;

  // The archive type. See `ArenaInternedString::Archive` for details.
  using Archive = LocalArenaStringArchive<Tag, alignment, static_min_block_size,
                                          static_max_block_size>;

  // Creates an empty `Interner` with a static block size.
  LocalArenaStringInterner() noexcept {
    static_assert(static_min_block_size > 0 && static_max_block_size > 0);
  }

  // Creates an empty `Interner` with a fixed dynamic block size.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<dependent_static_min_block_size == 0 &&
                                 dependent_static_max_block_size == 0,
                             int> = 0>
  explicit LocalArenaStringInterner(size_t block_size) : arena_(block_size) {}

  // Creates an empty `Interner` with an adaptive dynamic block size.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<dependent_static_min_block_size == 0 &&
                                 dependent_static_max_block_size == 0,
                             int> = 0>
  explicit LocalArenaStringInterner(size_t min_block_size,
                                    size_t max_block_size)
      : arena_(min_block_size, max_block_size) {}

  // A moved-from `Interner` is left empty.
  LocalArenaStringInterner(LocalArenaStringInterner&& that) noexcept
      : arena_(std::move(that.arena_)),
        shards_(std::move(that.shards_)),
        has_empty_string_([&] {
          if constexpr (kConcurrent) {
            return that.has_empty_string_.exchange(false,
                                                   std::memory_order_relaxed);
          } else {
            return std::exchange(that.has_empty_string_, false);
          }
        }()),
        num_objects_([&] {
          if constexpr (kConcurrent) {
            return that.num_objects_.exchange(0, std::memory_order_relaxed);
          } else {
            return std::exchange(that.num_objects_, 0);
          }
        }()),
        is_archived_in_place_(
            std::exchange(that.is_archived_in_place_, false)) {}
  LocalArenaStringInterner& operator=(
      LocalArenaStringInterner&& that) noexcept {
    arena_ = std::move(that.arena_);
    shards_ = std::move(that.shards_);
    if constexpr (kConcurrent) {
      has_empty_string_.store(
          that.has_empty_string_.exchange(false, std::memory_order_relaxed),
          std::memory_order_relaxed);
      num_objects_.store(
          that.num_objects_.exchange(0, std::memory_order_relaxed),
          std::memory_order_relaxed);
    } else {
      has_empty_string_ = std::exchange(that.has_empty_string_, false);
      num_objects_ = std::exchange(that.num_objects_, 0);
    }
    is_archived_in_place_ = std::exchange(that.is_archived_in_place_, false);
    return *this;
  }

  // Resets the interner to the empty state.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<(dependent_static_min_block_size > 0 &&
                              dependent_static_max_block_size > 0),
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    arena_.Reset();
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
    ResetShards();
  }

  // Prepares the interner for the expected number of distinct strings and
  // optionally the expected total size of allocated strings in bytes.
  // This reduces reallocations.
  void Reserve(size_t capacity, size_t bytes_capacity = 0) {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of ArenaInternedString::Interner::Reserve(): "
           "interner is archived in-place";
    if (capacity > 0) {
      const size_t capacity_per_shard = capacity / num_shards;
      if (capacity_per_shard > 0) {
        for (Shard& shard : shards_) {
          shard.Reserve(capacity_per_shard);
        }
      }
    }
    if (bytes_capacity > 0) {
      arena_.ReserveBytes(bytes_capacity);
    }
  }

  Interned Intern() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Intern(absl::string_view());
  }

  // Creates an `ArenaInternedString` holding the copied string, or sharing an
  // existing string if an equal string already exists.
  //
  // If `likely_new` is `true`, `Intern()` is optimized for the case where an
  // equal string does not exist yet.
  //
  // If `is_new != nullptr`, `*is_new` is set to `true` if a new string was
  // created, or `false` if an equal string already existed.
  template <bool likely_new = false, typename Arg,
            std::enable_if_t<SupportedByEncoderForIntern<Arg, Encoder>::value,
                             int> = 0>
  Interned Intern(const Arg& arg, bool* absl_nullable is_new = nullptr)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(arg, is_new));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <bool likely_new = false, typename DependentEncoder = Encoder,
            std::enable_if_t<SupportedByEncoderForIntern<
                                 absl::string_view, DependentEncoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE Interned
  Intern(const char* arg,
         bool* absl_nullable is_new = nullptr) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(absl::string_view(arg), is_new));
  }

  // Const `Intern()` overload enabled only when thread-safe.
  //
  // If `likely_new` is `true`, `Intern()` is optimized for the case where an
  // equal string does not exist yet.
  //
  // If `is_new != nullptr`, `*is_new` is set to `true` if a new string was
  // created, or `false` if an equal string already existed.
  template <
      typename DependentMutex = Mutex,
      std::enable_if_t<!std::is_same_v<DependentMutex, NullMutex>, int> = 0>
  Interned Intern() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Intern(absl::string_view());
  }
  template <bool likely_new = false, typename Arg,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    SupportedByEncoderForIntern<Arg, Encoder>>,
                int> = 0>
  Interned Intern(const Arg& arg, bool* absl_nullable is_new = nullptr) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(arg, is_new));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      bool likely_new = false, typename DependentMutex = Mutex,
      typename DependentEncoder = Encoder,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_same<DependentMutex, NullMutex>>,
              SupportedByEncoderForIntern<absl::string_view, DependentEncoder>>,
          int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE Interned
  Intern(const char* arg, bool* absl_nullable is_new = nullptr) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(absl::string_view(arg), is_new));
  }

  // Finds an existing `ArenaInternedString` matching the given argument, or
  // returns null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the string already exists, such as looking up in a map
  // with interned keys.
  template <typename Arg,
            std::enable_if_t<SupportedByEncoderForIntern<Arg, Encoder>::value,
                             int> = 0>
  OptionalInterned Find(const Arg& arg) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return OptionalInterned(FindInternal(arg));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentEncoder = Encoder,
            std::enable_if_t<SupportedByEncoderForIntern<
                                 absl::string_view, DependentEncoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalInterned
  Find(const char* arg) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return OptionalInterned(FindInternal(absl::string_view(arg)));
  }

  // Returns the number of strings managed by the interner.
  size_t NumObjects() const {
    if constexpr (kConcurrent) {
      return num_objects_.load(std::memory_order_relaxed);
    } else {
      return num_objects_;
    }
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const LocalArenaStringInterner* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->arena_);
    memory_estimator.RegisterSubobjects(&self->shards_);
  }

  // Extracts the storage of the strings as an `Archive`. The `Interner` is left
  // empty.
  //
  // See `ArenaInternedString::Archive` for details.
  Archive ExtractArchive() && {
    for (Shard& shard : shards_) {
      shard.Archive();
    }
    size_t num_objects;
    if constexpr (kConcurrent) {
      num_objects = num_objects_.load(std::memory_order_relaxed);
    } else {
      num_objects = num_objects_;
    }
    return Archive(std::move(arena_).ExtractArchive(), num_objects);
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
  }

 private:
  // For `InternInternal()` and `FindInternal()`.
  friend class GlobalArenaStringInterner<Encoder, Tag, Mutex, num_shards,
                                         alignment, static_min_block_size,
                                         static_max_block_size>;

  using Element = ArenaString::WithAlignment<alignment>;

  static constexpr bool kConcurrent = !std::is_same_v<Mutex, NullMutex>;

  using ArenaMutex = std::conditional_t<kConcurrent, absl::Mutex, NullMutex>;
  using Arena =
      typename StringArena::Concurrent<ArenaMutex>::template WithBlockSize<
          static_min_block_size, static_max_block_size>;
  using Shard = StringArenaShard<Encoder, Mutex, alignment>;

  void ResetShards() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    for (Shard& shard : shards_) {
      shard.Reset();
    }
    if constexpr (kConcurrent) {
      has_empty_string_.store(false, std::memory_order_relaxed);
      num_objects_.store(0, std::memory_order_relaxed);
    } else {
      has_empty_string_ = false;
      num_objects_ = 0;
    }
    is_archived_in_place_ = false;
  }

  template <bool likely_new, typename Arg>
  Element InternInternal(const Arg& value, bool* absl_nullable is_new) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of ArenaInternedString::Interner::Intern(): "
           "interner is archived in-place";
    if (Encoder::EncodedEmpty(value)) {
      bool inserted;
      if constexpr (kConcurrent) {
        inserted = !has_empty_string_.exchange(true, std::memory_order_relaxed);
      } else {
        inserted = !std::exchange(has_empty_string_, true);
      }
      if (inserted) {
        if constexpr (kConcurrent) {
          num_objects_.fetch_add(1, std::memory_order_relaxed);
        } else {
          ++num_objects_;
        }
      }
      if (is_new != nullptr) *is_new = inserted;
      return Element();
    }
    return InternInternalNonEmpty<likely_new>(value, is_new);
  }

  template <bool likely_new, typename Arg>
  Element InternInternalNonEmpty(const Arg& value,
                                 bool* absl_nullable is_new) const {
    const size_t hash = typename Encoder::Hash()(value);
    bool is_new_internal;
    Element result;
    if constexpr (likely_new) {
      result = GetShard(hash).template InternNew</*verified_new=*/false>(
          value, hash, arena_, is_new_internal);
    } else {
      result = GetShard(hash).Intern(value, hash, arena_, is_new_internal);
    }
    if (is_new_internal) {
      if constexpr (kConcurrent) {
        num_objects_.fetch_add(1, std::memory_order_relaxed);
      } else {
        ++num_objects_;
      }
    }
    if (is_new != nullptr) *is_new = is_new_internal;
    return result;
  }

  template <typename Arg>
  typename Element::Optional FindInternal(const Arg& arg) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of ArenaInternedString::Interner::Find(): "
           "interner is archived in-place";
    if (Encoder::EncodedEmpty(arg)) {
      if constexpr (kConcurrent) {
        if (has_empty_string_.load(std::memory_order_relaxed)) return Element();
      } else {
        if (has_empty_string_) return Element();
      }
      return nullptr;
    }
    return FindInternalNonEmpty(arg);
  }

  template <typename Arg>
  typename Element::Optional FindInternalNonEmpty(const Arg& arg) const {
    const size_t hash = typename Encoder::Hash()(arg);
    return GetShard(hash).Find(arg, hash);
  }

  Shard& GetShard(size_t hash) const {
    return shards_[ShardIndex<num_shards>(hash)];
  }

  mutable Arena arena_;
  mutable std::array<Shard, num_shards> shards_;
  mutable std::conditional_t<kConcurrent, std::atomic<bool>, bool>
      has_empty_string_{false};
  mutable std::conditional_t<kConcurrent, std::atomic<size_t>, size_t>
      num_objects_{0};
  bool is_archived_in_place_ = false;
};

// The public name of `GlobalArenaStringInterner` is
// `ArenaInternedString::GlobalInterner`.
//
// `ArenaInternedString::GlobalInterner` represents a global interner for the
// given template parameters. See `ArenaInternedString::Interner` for a
// non-global version.
//
// Since strings are owned by the interner, using a global arena interner risks
// running out of memory unless the number of strings ever interned is limited.
// A non-global interner restricts the risk to a smaller scope and is preferred.
template <typename Encoder, typename Tag, typename Mutex, size_t num_shards,
          size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
class GlobalArenaStringInterner {
 public:
  static_assert(absl::has_single_bit(alignment));
  static_assert(static_min_block_size > 0 && static_max_block_size > 0,
                "Global interner cannot have dynamic block size");

  // Makes the interner thread-safe and tunes it for concurrency. See
  // `ArenaInternedString::Interner::Concurrent` for details.
  //
  // By default, a global interner is tuned for concurrency and has multiple
  // shards.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent =
      GlobalArenaStringInterner<Encoder, Tag, NewMutex, new_num_shards,
                                alignment, static_min_block_size,
                                static_max_block_size>;

  // Configures the block size of the arena. See
  // `ArenaInternedString::Interner::WithBlockSize` for details.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize =
      GlobalArenaStringInterner<Encoder, Tag, Mutex, num_shards, alignment,
                                new_static_min_block_size,
                                new_static_max_block_size>;

  // References to interned strings. See `ArenaInternedString` and
  // `ArenaInternedString::Optional` for details.
  using Interned = BasicArenaInternedString<Encoder, Tag, alignment>;
  using OptionalInterned = OptionalArenaInternedString<Encoder, Tag, alignment>;

  // Since `ArenaInternedString::GlobalInterner` is stateless, all instances
  // are equivalent. Member functions are static. Instantiation is provided for
  // consistency with other interner categories.
  GlobalArenaStringInterner() = default;

  GlobalArenaStringInterner(const GlobalArenaStringInterner& that) = default;
  GlobalArenaStringInterner& operator=(const GlobalArenaStringInterner& that) =
      default;

  // Optimized overload for an empty string.
  static Interned Intern() {
    return Interned(riegeli::Global<LocalInterner>().Intern());
  }

  // Creates an `ArenaInternedString` holding the copied string, or sharing an
  // existing string if an equal string already exists.
  //
  // If `likely_new` is `true`, `Intern()` is optimized for the case where an
  // equal string does not exist yet.
  //
  // If `is_new != nullptr`, `*is_new` is set to `true` if a new string was
  // created, or `false` if an equal string already existed.
  template <bool likely_new = false, typename Arg,
            std::enable_if_t<SupportedByEncoderForIntern<Arg, Encoder>::value,
                             int> = 0>
  static Interned Intern(const Arg& arg, bool* absl_nullable is_new = nullptr) {
    return Interned(InternInternal<likely_new>(arg, is_new));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <bool likely_new = false, typename DependentEncoder = Encoder,
            std::enable_if_t<SupportedByEncoderForIntern<
                                 absl::string_view, DependentEncoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static Interned Intern(
      const char* arg, bool* absl_nullable is_new = nullptr) {
    return Interned(InternInternal<likely_new>(absl::string_view(arg), is_new));
  }

  // Finds an existing `ArenaInternedString` matching the given argument, or
  // returns null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the string already exists, such as looking up in a map
  // with interned keys.
  template <typename Arg,
            std::enable_if_t<SupportedByEncoderForIntern<Arg, Encoder>::value,
                             int> = 0>
  static OptionalInterned Find(const Arg& arg) {
    return OptionalInterned(FindInternal(arg));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentEncoder = Encoder,
            std::enable_if_t<SupportedByEncoderForIntern<
                                 absl::string_view, DependentEncoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static OptionalInterned Find(const char* arg) {
    return OptionalInterned(FindInternal(absl::string_view(arg)));
  }

  // Returns an immortal `ArenaInternedString` with a specific value.
  //
  // This avoids finding the string each time.
  //
  // The `construct` callable should be a lambda with no captures, returning
  // an argument for some `Intern()` overload.
  template <typename Construct,
            std::enable_if_t<std::conjunction_v<std::is_empty<Construct>,
                                                std::is_invocable<Construct>>,
                             int> = 0>
  static const Interned& Immortal(Construct /*construct*/) {
    return riegeli::Global([] { return Intern(Construct()()); });
  }

  // Returns a snapshot of the number of strings managed by the interner.
  static size_t NumObjects() {
    return riegeli::Global<LocalInterner>().NumObjects();
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(
      const GlobalArenaStringInterner* /*self*/,
      MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&riegeli::Global<LocalInterner>());
  }

 private:
  using Element = ArenaString::WithAlignment<alignment>;
  using LocalInterner =
      LocalArenaStringInterner<Encoder, Tag, Mutex, num_shards, alignment,
                               static_min_block_size, static_max_block_size>;

  template <bool likely_new, typename Arg>
  static Element InternInternal(const Arg& value, bool* absl_nullable is_new) {
    return riegeli::Global<LocalInterner>().template InternInternal<likely_new>(
        value, is_new);
  }

  template <typename Arg>
  static typename Element::Optional FindInternal(const Arg& value) {
    return riegeli::Global<LocalInterner>().FindInternal(value);
  }
};

// Implementation details follow.

template <typename Encoder, typename Tag, size_t alignment>
class OptionalArenaInternedString<Encoder, Tag, alignment>::ByAddress
    : public WithCompare<ByAddress> {
 public:
  /*implicit*/ ByAddress(Optional view) : repr_(view.get().by_address()) {}

  ByAddress(const ByAddress& that) = default;
  ByAddress& operator=(const ByAddress& that) = default;

  friend bool operator==(ByAddress a, ByAddress b) {
    return a.repr_ == b.repr_;
  }
  friend StrongOrdering RIEGELI_COMPARE(ByAddress a, ByAddress b) {
    return riegeli::Compare(a.repr_, b.repr_);
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, ByAddress self) {
    return HashState::combine(std::move(hash_state), self.repr_);
  }

 private:
  typename Element::ByAddress repr_;
};

template <typename Encoder, typename Tag, size_t alignment>
struct OptionalArenaInternedString<Encoder, Tag, alignment>::ValueHash {
  using is_transparent = void;
  size_t operator()(Optional self) const {
    if (self.get() == nullptr) {
      if constexpr (HasTransparentNullptrHash<typename Encoder::Hash>::value) {
        return hash(nullptr);
      } else {
        return absl::HashOf(nullptr);
      }
    }
    return hash(*self);
  }
  size_t operator()(NotOptional self) const { return hash(*self); }
  size_t operator()(std::nullptr_t) const {
    if constexpr (HasTransparentNullptrHash<typename Encoder::Hash>::value) {
      return hash(nullptr);
    } else {
      return absl::HashOf(nullptr);
    }
  }
  size_t operator()(absl::string_view arg) const { return hash(arg); }
  template <typename PassedKey, typename DependentHash = typename Encoder::Hash,
            typename = typename DependentHash::is_transparent,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<Optional, PassedKey>,
                    NotSameRef<std::nullptr_t, PassedKey>,
                    NotSameRef<absl::string_view, PassedKey>,
                    std::is_invocable<const DependentHash&, const PassedKey&>>,
                int> = 0>
  size_t operator()(const PassedKey& arg) const {
    return hash(arg);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS typename Encoder::Hash hash;
};

template <typename Encoder, typename Tag, size_t alignment>
struct OptionalArenaInternedString<Encoder, Tag,
                                   alignment>::absl_container_hash {
  using is_transparent = void;
  size_t operator()(Optional self) const {
    return absl::HashOf(self.get().by_address());
  }
  size_t operator()(NotOptional self) const {
    return absl::HashOf(self.get().by_address());
  }
  size_t operator()(std::nullptr_t) const { return absl::HashOf(nullptr); }
};

template <typename Encoder, typename Tag, size_t alignment>
struct OptionalArenaInternedString<Encoder, Tag, alignment>::absl_container_eq {
  using is_transparent = void;
  bool operator()(Optional a, Optional b) const {
    return a.get().by_address() == b.get().by_address();
  }
  bool operator()(Optional a, NotOptional b) const {
    return a.get().by_address() == b.get().by_address();
  }
  bool operator()(NotOptional a, Optional b) const {
    return a.get().by_address() == b.get().by_address();
  }
  bool operator()(NotOptional a, NotOptional b) const {
    return a.get().by_address() == b.get().by_address();
  }
  bool operator()(Optional a, std::nullptr_t) const {
    return a.get() == nullptr;
  }
  bool operator()(std::nullptr_t, Optional b) const {
    return b.get() == nullptr;
  }
  bool operator()(NotOptional /*a*/, std::nullptr_t) const { return false; }
  bool operator()(std::nullptr_t, NotOptional /*b*/) const { return false; }
};

template <typename Encoder, typename Tag, size_t alignment>
struct OptionalArenaInternedString<Encoder, Tag, alignment>::ValueEq {
  using is_transparent = void;
  bool operator()(Optional a, Optional b) const {
    return a.get().by_address() == b.get().by_address();
  }
  bool operator()(Optional a, NotOptional b) const {
    return a.get().by_address() == b.get().by_address();
  }
  bool operator()(NotOptional a, Optional b) const {
    return a.get().by_address() == b.get().by_address();
  }
  bool operator()(NotOptional a, NotOptional b) const {
    return a.get().by_address() == b.get().by_address();
  }
  bool operator()(Optional a, std::nullptr_t) const {
    return a.get() == nullptr;
  }
  bool operator()(std::nullptr_t, Optional b) const {
    return b.get() == nullptr;
  }
  bool operator()(NotOptional /*a*/, std::nullptr_t) const { return false; }
  bool operator()(std::nullptr_t, NotOptional /*b*/) const { return false; }
  bool operator()(Optional a, absl::string_view b) const {
    if (a.get() == nullptr) return false;
    return eq(*a, b);
  }
  bool operator()(absl::string_view a, Optional b) const {
    if (b.get() == nullptr) return false;
    return eq(*b, a);
  }
  bool operator()(NotOptional a, absl::string_view b) const {
    return eq(*a, b);
  }
  bool operator()(absl::string_view a, NotOptional b) const {
    return eq(*b, a);
  }
  template <typename PassedKey, typename DependentEq = typename Encoder::Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<Optional, PassedKey>,
                                   NotSameRef<std::nullptr_t, PassedKey>,
                                   NotSameRef<absl::string_view, PassedKey>,
                                   std::is_invocable<const DependentEq&,
                                                     const absl::string_view&,
                                                     const PassedKey&>>,
                int> = 0>
  bool operator()(Optional a, const PassedKey& b) const {
    if (a.get() == nullptr) return false;
    return eq(*a, b);
  }
  template <typename PassedKey, typename DependentEq = typename Encoder::Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<Optional, PassedKey>,
                                   NotSameRef<std::nullptr_t, PassedKey>,
                                   NotSameRef<absl::string_view, PassedKey>,
                                   std::is_invocable<const DependentEq&,
                                                     const absl::string_view&,
                                                     const PassedKey&>>,
                int> = 0>
  bool operator()(const PassedKey& a, Optional b) const {
    if (b.get() == nullptr) return false;
    return eq(*b, a);
  }
  template <typename PassedKey, typename DependentEq = typename Encoder::Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<Optional, PassedKey>,
                                   NotSameRef<std::nullptr_t, PassedKey>,
                                   NotSameRef<absl::string_view, PassedKey>,
                                   std::is_invocable<const DependentEq&,
                                                     const absl::string_view&,
                                                     const PassedKey&>>,
                int> = 0>
  bool operator()(NotOptional a, const PassedKey& b) const {
    return eq(*a, b);
  }
  template <typename PassedKey, typename DependentEq = typename Encoder::Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<Optional, PassedKey>,
                                   NotSameRef<std::nullptr_t, PassedKey>,
                                   NotSameRef<absl::string_view, PassedKey>,
                                   std::is_invocable<const DependentEq&,
                                                     const absl::string_view&,
                                                     const PassedKey&>>,
                int> = 0>
  bool operator()(const PassedKey& a, NotOptional b) const {
    return eq(*b, a);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS typename Encoder::Eq eq;
};

}  // namespace interned_internal

}  // namespace riegeli

#endif  // RIEGELI_INTERNED_ARENA_INTERNED_STRING_H_
