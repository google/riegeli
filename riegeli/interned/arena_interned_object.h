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

#ifndef RIEGELI_INTERNED_ARENA_INTERNED_OBJECT_H_
#define RIEGELI_INTERNED_ARENA_INTERNED_OBJECT_H_

#include <stddef.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/hash_container_defaults.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/global.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/interned/arena_interned_object_internal.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/object_arena.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Mutex type that does not lock.
using interned_internal::NullMutex;

// Default template parameter `num_shards` for
// `ArenaInterned::GlobalInterner`.
// Also, a default template parameter for `Concurrent` nested types.
using interned_internal::kDefaultInternerNumShards;

// Default template parameters for `ArenaInterned::Interner` and
// `ArenaInterned::GlobalInterner`.
using interned_internal::kDefaultArenaMaxBlockSize;
using interned_internal::kDefaultArenaMinBlockSize;

template <typename T, typename Hash, typename Eq, typename Tag>
class ArenaInterned;

namespace interned_internal {

template <typename T, typename Tag, size_t static_min_block_size,
          size_t static_max_block_size>
class LocalArenaArchive;

template <typename T, typename Hash, typename Eq, typename Tag, typename Mutex,
          size_t num_shards, size_t static_min_block_size,
          size_t static_max_block_size>
class LocalArenaInterner;

template <typename T, typename Hash, typename Eq, typename Tag, typename Mutex,
          size_t num_shards, size_t static_min_block_size,
          size_t static_max_block_size>
class GlobalArenaInterner;

// The public name of `OptionalArenaInterned<T>` is
// `ArenaInterned<T>::Optional`.
//
// `ArenaInterned<T>` refers to an object of type `T`, ensuring that equal
// objects are shared to minimize memory usage.
//
// In contrast to `ArenaInterned`, `ArenaInterned::Optional` can be null. It is
// more efficient than `std::optional<ArenaInterned>`.
//
// See `ArenaInterned` for details.
template <typename T, typename Hash, typename Eq, typename Tag>
class OptionalArenaInterned
    : public WithCompare<OptionalArenaInterned<T, Hash, Eq, Tag>,
                         std::nullptr_t> {
 public:
  // Navigates between `ArenaInterned` and `ArenaInterned::Optional`.
  using NotOptional = ArenaInterned<T, Hash, Eq, Tag>;
  using Optional = OptionalArenaInterned;

  // The default interner type. See `ArenaInterned::Interner` for details.
  using Interner = interned_internal::LocalArenaInterner<
      T, Hash, Eq, Tag, NullMutex, /*num_shards=*/1, kDefaultArenaMinBlockSize,
      kDefaultArenaMaxBlockSize>;

  // The archive type. See `ArenaInterned::Archive` for details.
  using Archive =
      interned_internal::LocalArenaArchive<T, Tag, kDefaultArenaMinBlockSize,
                                           kDefaultArenaMaxBlockSize>;

  // The global interner type. See `ArenaInterned::GlobalInterner` for details.
  using GlobalInterner = interned_internal::GlobalArenaInterner<
      T, Hash, Eq, Tag, absl::Mutex, kDefaultInternerNumShards<absl::Mutex>,
      /*static_min_block_size=*/kDefaultArenaMaxBlockSize,
      kDefaultArenaMaxBlockSize>;

  // Creates a null `ArenaInterned::Optional`.
  //
  // This differs from the default constructor of `ArenaInterned`.
  OptionalArenaInterned() = default;
  /*implicit*/ OptionalArenaInterned(std::nullptr_t) {}
  OptionalArenaInterned& operator=(std::nullptr_t) {
    repr_ = nullptr;
    return *this;
  }

  OptionalArenaInterned(const OptionalArenaInterned& that) = default;
  OptionalArenaInterned& operator=(const OptionalArenaInterned& that) = default;

  // Returns `true` if not null.
  explicit operator bool() const { return get() != nullptr; }

  // Converts from `ArenaInterned::Optional` to `ArenaInterned`.
  NotOptional not_optional() const {
    RIEGELI_ASSERT(get() != nullptr)
        << "Failed precondition of ArenaInterned::Optional::not_optional(): "
           "null pointer";
    return NotOptional(get());
  }
  NotOptional NotOptionalOrDie() const {
    RIEGELI_CHECK(get() != nullptr)
        << "Failed precondition of "
           "ArenaInterned::Optional::NotOptionalOrDie(): "
           "null pointer";
    return NotOptional(get());
  }

  // Returns a pointer to the object, or `nullptr` if null.
  const T* absl_nullable get() const { return repr_; }

  // Dereferences the pointer.
  const T& operator*() const {
    RIEGELI_ASSERT(get() != nullptr)
        << "Failed precondition of ArenaInterned::Optional::operator*: "
           "null pointer";
    return *get();
  }
  const T* operator->() const {
    RIEGELI_ASSERT(get() != nullptr)
        << "Failed precondition of ArenaInterned::Optional::operator->: "
           "null pointer";
    return get();
  }

  // Dereferences the pointer, crashing the process if null.
  const T& value() const {
    RIEGELI_CHECK(get() != nullptr)
        << "Failed precondition of ArenaInterned::Optional::value(): "
           "null pointer";
    return *get();
  }

  // Equality of non-null `ArenaInterned::Optional` objects corresponds to
  // equality of the objects they refer to, as specified by `Eq`, but is fast,
  // comparing the pointers.
  //
  // Other comparisons are also consistent with the objects, but only the case
  // of equal objects is optimized.
  //
  // All comparisons are valid only for `ArenaInterned::Optional` objects
  // coming from the same interner.

  friend bool operator==(Optional a, Optional b) { return a.get() == b.get(); }
  template <
      typename DependentT = T,
      std::enable_if_t<
          std::disjunction_v<HasCompare<const DependentT&, const DependentT&>,
                             HasLessThan<const DependentT&, const DependentT&>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(Optional a, Optional b) {
    if constexpr (HasCompare<const T&, const T&>::value) {
      using Ordering = decltype(riegeli::Compare(*a, *b));
      if (a.get() == b.get()) return Ordering(StrongOrdering::equal);
      if (a.get() == nullptr) return Ordering(StrongOrdering::less);
      if (b.get() == nullptr) return Ordering(StrongOrdering::greater);
      return riegeli::Compare(*a, *b);
    } else {
      if (a.get() == b.get()) return StrongOrdering::equal;
      if (a.get() == nullptr) return StrongOrdering::less;
      if (b.get() == nullptr) return StrongOrdering::greater;
      if (*a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
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
      std::enable_if_t<std::conjunction_v<NotSameRef<Optional, Other>,
                                          std::negation<std::is_base_of<
                                              WithEqualMarker<Other>, Other>>,
                                          HasEqual<const T&, const Other&>>,
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
              std::disjunction<HasCompare<const T&, const Other&>,
                               HasLessThan<const T&, const Other&>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(Optional a, const Other& b) {
    if constexpr (HasCompare<const T&, const Other&>::value) {
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
      std::enable_if_t<std::conjunction_v<NotSameRef<Optional, Other>,
                                          std::negation<std::is_base_of<
                                              WithEqualMarker<Other>, Other>>,
                                          HasEqual<const Other&, const T&>>,
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
              std::disjunction<HasCompare<const Other&, const T&>,
                               HasLessThan<const Other&, const T&>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const Other& a, Optional b) {
    if constexpr (HasCompare<const Other&, const T&>::value) {
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

  // `ArenaInterned::ByAddress` is implicitly convertible from `ArenaInterned`
  // or `ArenaInterned::Optional`, but instances are compared by address. This
  // is more efficient, but the order is arbitrary, consistent within the
  // process.
  //
  // `std::less<ByAddress>` can be used as a comparator for algorithms over
  // `ArenaInterned` or `ArenaInterned::Optional`.
  class ByAddress;

  // Returns this object wrapped in `ByAddress`.
  ByAddress by_address() const { return ByAddress(*this); }

  // Hashing `ArenaInterned` or `ArenaInterned::Optional` is fast, hashing the
  // pointer.
  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, Optional self) {
    return HashState::combine(std::move(hash_state), self.get());
  }

  // Default hash and equality for containers with `ArenaInterned` or
  // `ArenaInterned::Optional` as the key type, hashing and comparing by
  // address, supporting heterogeneous lookup against `NotOptional` and
  // `Optional`.
  struct absl_container_hash;
  struct absl_container_eq;

  // Hash and equality for containers with `ArenaInterned` or
  // `ArenaInterned::Optional` as the key type, consistent with the underlying
  // value, supporting heterogeneous lookup. This is opt-in because
  // heterogeneous hashing is more expensive than pointer hashing.
  struct ValueHash;
  struct ValueEq;

  // Supports `riegeli::Debug()`.
  template <typename DebugStream>
  friend void RiegeliDebug(Optional src, DebugStream& dest) {
    if (src == nullptr) {
      dest.Debug(nullptr);
    } else {
      dest.Debug(*src);
    }
  }

 protected:
  explicit OptionalArenaInterned(const T* element) : repr_(element) {}

 private:
  friend NotOptional;  // For `Optional(const T*)`.
  // For `Optional(const T*)`.
  template <typename TParam, typename HashParam, typename EqParam,
            typename TagParam, typename MutexParam, size_t num_shards,
            size_t static_min_block_size, size_t static_max_block_size>
  friend class LocalArenaInterner;
  // For `Optional(const T*)`.
  template <typename TParam, typename HashParam, typename EqParam,
            typename TagParam, typename MutexParam, size_t num_shards,
            size_t static_min_block_size, size_t static_max_block_size>
  friend class GlobalArenaInterner;

  const T* absl_nullable repr_ = nullptr;
};

}  // namespace interned_internal

// `ArenaInterned<T>` refers to an object of type `T`, ensuring that equal
// objects are shared to minimize memory usage.
//
// `ArenaInterned` is never null. See `ArenaInterned::Optional` for a variant
// that can be null. `ArenaInterned` is generally preferred over
// `ArenaInterned::Optional`.
//
// See `ArenaInternedString` for a variant optimized for strings.
//
// `ArenaInterned` objects are created by an interner, which maintains a set
// of arena-allocated objects to share. An interner can be local (managed
// explicitly) or global (represented by a stateless type). The default is
// local.
//
// Interned objects are destroyed and erased only when the interner is
// destroyed. See `Interned` for a variant that is slower but deletes individual
// objects when all references to them are dropped.
//
// Since objects are owned by the interner, using an arena interner risks
// running out of memory unless the number of distinct objects ever interned
// by the given interner is limited.
//
// See `IndexInterned` for a variant that refers to objects by a numeric index.
// An index can be more compact than a pointer and indices are allocated
// consecutively, which allows representing dense maps as vectors, but the
// interner is needed to resolve an index to the object, which is slightly
// slower, and the numeric index space can be exhausted.
//
// Asymptotic memory usage per interned object:
//   active: sizeof(T) + 14.8
//   archived: sizeof(T)
//
// Breakdown:
//  + entry in `absl::flat_hash_set<T*>`:
//      8 / (7 * ln(2)) * (8 + 1) unless archived
//  + arena-allocated object: sizeof(T)
//
// Interned handle: 8
//
// Among the template parameters, only `T` and optionally `Hash` and `Eq` should
// be specified explicitly. Other parameters should be specified by nested type
// `WithTag`. Further parameters are applied to `Interner` or `GlobalInterner`.
//
// `ArenaInterned` derives from `ArenaInterned::Optional`. See
// `ArenaInterned::Optional` for inherited operations.
template <typename T, typename Hash = absl::DefaultHashContainerHash<T>,
          typename Eq = absl::DefaultHashContainerEq<T>, typename Tag = void>
class ArenaInterned
    : public interned_internal::OptionalArenaInterned<T, Hash, Eq, Tag>,
      public WithCompare<
          ArenaInterned<T, Hash, Eq, Tag>,
          interned_internal::OptionalArenaInterned<T, Hash, Eq, Tag>,
          std::nullptr_t> {
 public:
  // Changes the tag type of the interner.
  //
  // Arena interned objects with distinct tags are managed by separate types of
  // interners, even if other template parameters are the same. This allows
  // annotating the type with its role for improved type safety. This forces
  // separation of interners, which can make lookups more efficient.
  template <typename NewTag>
  using WithTag = ArenaInterned<T, Hash, Eq, NewTag>;

  // Navigates between `ArenaInterned` and `ArenaInterned::Optional`.
  using NotOptional = typename ArenaInterned::NotOptional;
  using Optional = typename ArenaInterned::Optional;

  // The default interner type. It is used for interning new objects. The
  // interner also provides statistics.
  //
  // Further parameters should be specified by `Interner` nested types
  // `Concurrent` and `WithBlockSize`.
  using Interner = typename ArenaInterned::Interner;

  // The archive type. It can be used to hold interned objects after all objects
  // have been interned and the `Interner` has been destroyed, and provides
  // statistics.
  using Archive = typename ArenaInterned::Archive;

  // The global interner type. It is used for interning new objects into a
  // global interner.
  //
  // Further parameters should be specified by `GlobalInterner` nested types
  // `Concurrent` and `WithBlockSize`.
  using GlobalInterner = typename ArenaInterned::GlobalInterner;

  // Constructor from `nullptr` is present in `ArenaInterned::Optional` but
  // deleted in `ArenaInterned`.
  ArenaInterned(std::nullptr_t) = delete;
  ArenaInterned& operator=(std::nullptr_t) = delete;

  ArenaInterned(const ArenaInterned& that) = default;
  ArenaInterned& operator=(const ArenaInterned& that) = default;

  // Restores an `ArenaInterned` from a raw pointer to a previously interned
  // object.
  static NotOptional BackFromData(const T* data) {
    RIEGELI_ASSERT(data != nullptr)
        << "Failed precondition of ArenaInterned::BackFromData(): "
           "null pointer";
    return NotOptional(data);
  }

  // Returns `true` because `ArenaInterned` is never null.
  explicit operator bool() const { return true; }

  // Returns a pointer to the object.
  const T* get() const {
    const T* const repr = this->Optional::get();
    RIEGELI_ASSERT(repr != nullptr)
        << "Failed invariant of ArenaInterned: null pointer";
    return repr;
  }

  // Dereferences the pointer.
  const T& operator*() const { return *get(); }
  const T* operator->() const { return get(); }
  const T& value() const { return *get(); }

  // Equality of `ArenaInterned` objects corresponds to equality of the objects
  // they refer to, as specified by `Eq`, but is fast, comparing the pointers.
  //
  // Other comparisons are also consistent with the objects, but only the case
  // of equal objects is optimized.
  //
  // All comparisons are valid only for `ArenaInterned` objects coming from the
  // same interner.
  friend bool operator==(NotOptional a, NotOptional b) {
    return a.get() == b.get();
  }
  template <
      typename DependentT = T,
      std::enable_if_t<
          std::disjunction_v<HasCompare<const DependentT&, const DependentT&>,
                             HasLessThan<const DependentT&, const DependentT&>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(NotOptional a, NotOptional b) {
    if constexpr (HasCompare<const T&, const T&>::value) {
      using Ordering = decltype(riegeli::Compare(*a, *b));
      if (a.get() == b.get()) return Ordering(StrongOrdering::equal);
      return riegeli::Compare(*a, *b);
    } else {
      if (a.get() == b.get()) return StrongOrdering::equal;
      if (*a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }

  friend bool operator==(NotOptional a, Optional b) {
    return a.get() == b.get();
  }
  template <
      typename DependentT = T,
      std::enable_if_t<
          std::disjunction_v<HasCompare<const DependentT&, const DependentT&>,
                             HasLessThan<const DependentT&, const DependentT&>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(NotOptional a, Optional b) {
    if constexpr (HasCompare<const T&, const T&>::value) {
      using Ordering = decltype(riegeli::Compare(*a, *b));
      if (a.get() == b.get()) return Ordering(StrongOrdering::equal);
      if (b.get() == nullptr) return Ordering(StrongOrdering::greater);
      return riegeli::Compare(*a, *b);
    } else {
      if (a.get() == b.get()) return StrongOrdering::equal;
      if (b.get() == nullptr) return StrongOrdering::greater;
      if (*a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }

  friend bool operator==(NotOptional /*a*/, std::nullptr_t) { return false; }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional /*a*/, std::nullptr_t) {
    return StrongOrdering::greater;
  }

  template <
      typename Other,
      std::enable_if_t<std::conjunction_v<NotSameRef<Optional, Other>,
                                          std::negation<std::is_base_of<
                                              WithEqualMarker<Other>, Other>>,
                                          HasEqual<const T&, const Other&>>,
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
              std::disjunction<HasCompare<const T&, const Other&>,
                               HasLessThan<const T&, const Other&>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(NotOptional a, const Other& b) {
    if constexpr (HasCompare<const T&, const Other&>::value) {
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
      std::enable_if_t<std::conjunction_v<NotSameRef<Optional, Other>,
                                          std::negation<std::is_base_of<
                                              WithEqualMarker<Other>, Other>>,
                                          HasEqual<const Other&, const T&>>,
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
              std::disjunction<HasCompare<const Other&, const T&>,
                               HasLessThan<const Other&, const T&>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const Other& a, NotOptional b) {
    if constexpr (HasCompare<const Other&, const T&>::value) {
      return riegeli::Compare(a, *b);
    } else {
      if (a == *b) return StrongOrdering::equal;
      if (a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }
#endif

 private:
  friend Optional;  // For `ArenaInterned(const T*)`.
  // For `ArenaInterned(const T*)`.
  template <typename TParam, typename HashParam, typename EqParam,
            typename TagParam, typename MutexParam, size_t num_shards,
            size_t static_min_block_size, size_t static_max_block_size>
  friend class interned_internal::LocalArenaInterner;
  // For `ArenaInterned(const T*)`.
  template <typename TParam, typename HashParam, typename EqParam,
            typename TagParam, typename MutexParam, size_t num_shards,
            size_t static_min_block_size, size_t static_max_block_size>
  friend class interned_internal::GlobalArenaInterner;

  explicit ArenaInterned(const T* element) : Optional(element) {}
};

namespace interned_internal {

// The public name of `LocalArenaArchive<T>` is `ArenaInterned<T>::Archive`.
//
// `ArenaInterned::Archive` holds interned objects after all objects have been
// interned and the `Interner` has been destroyed, and provides statistics.
//
// Objects obtained from the `Interner` remain valid as long as the `Archive`
// is valid.
//
// This saves memory by releasing the lookup structures of the `Interner`
// once they are no longer needed.
template <typename T, typename Tag, size_t static_min_block_size,
          size_t static_max_block_size>
class LocalArenaArchive {
 public:
  // Configures the block size of the arena, in bytes. See
  // `ArenaInterned::Interner::WithBlockSize` for details.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize = LocalArenaArchive<T, Tag, new_static_min_block_size,
                                          new_static_max_block_size>;

  // Configures the block size of the arena to be dynamic. See
  // `ArenaInterned::Interner::WithDynamicBlockSize` for details.
  using WithDynamicBlockSize =
      LocalArenaArchive<T, Tag, /*static_min_block_size=*/0,
                        /*static_max_block_size=*/0>;

  // Creates an empty `Archive`.
  LocalArenaArchive()
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
  LocalArenaArchive(LocalArenaArchive&& that) noexcept
      : arena_(std::move(that.arena_)),
        num_objects_(std::exchange(that.num_objects_, 0)) {}
  LocalArenaArchive& operator=(LocalArenaArchive&& that) noexcept {
    arena_ = std::move(that.arena_);
    num_objects_ = std::exchange(that.num_objects_, 0);
    return *this;
  }

  // Returns the number of objects in the archive. It does not change.
  size_t NumObjects() const { return num_objects_; }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const LocalArenaArchive* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->arena_);
  }

 private:
  // For `LocalArenaArchive(Arena&&, size_t)`.
  template <typename TParam, typename Hash, typename Eq, typename TagParam,
            typename MutexParam, size_t num_shards,
            size_t other_static_min_block_size,
            size_t other_static_max_block_size>
  friend class LocalArenaInterner;

  using Arena =
      typename ObjectArena<T>::template WithBlockSize<static_min_block_size,
                                                      static_max_block_size>;

  explicit LocalArenaArchive(Arena&& arena, size_t num_objects)
      : arena_(std::move(arena)), num_objects_(num_objects) {}

  Arena arena_;
  size_t num_objects_ = 0;
};

// The public name of `LocalArenaInterner<T>` is `ArenaInterned<T>::Interner`.
//
// `ArenaInterned<T>::Interner` represents an explicitly managed interner.
// It arena-allocates and manages a set of interned objects. The objects are
// owned by the interner and are destroyed when the interner is destroyed.
//
// See `ArenaInterned<T>::GlobalInterner` for a global version.
template <typename T, typename Hash, typename Eq, typename Tag, typename Mutex,
          size_t num_shards, size_t static_min_block_size,
          size_t static_max_block_size>
class LocalArenaInterner {
 public:
  // Makes the interner thread-safe and tunes it for concurrency.
  //
  // By default, a global interner is thread-safe and has multiple shards,
  // while a local interner is not thread-safe and has a single shard.
  // With more shards, parallel usage is less likely to cause contention.
  //
  // `Mutex` protects the set of object pointers in each shard.
  //
  // A mutex must support `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent =
      LocalArenaInterner<T, Hash, Eq, Tag, NewMutex, new_num_shards,
                         static_min_block_size, static_max_block_size>;

  // Configures the block size of the arena, in bytes.
  //
  // Objects are allocated in blocks of sizes within this range. A larger block
  // size improves memory locality and reduces the number of allocations, but
  // increases wasted memory if only a small number of objects is interned.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize =
      LocalArenaInterner<T, Hash, Eq, Tag, Mutex, num_shards,
                         new_static_min_block_size, new_static_max_block_size>;

  // Configures the block size of the arena to be specified dynamically in the
  // constructor.
  using WithDynamicBlockSize =
      LocalArenaInterner<T, Hash, Eq, Tag, Mutex, num_shards,
                         /*static_min_block_size=*/0,
                         /*static_max_block_size=*/0>;

  // References to interned objects. See `ArenaInterned` and
  // `ArenaInterned::Optional` for details.
  using Interned = ArenaInterned<T, Hash, Eq, Tag>;
  using OptionalInterned = OptionalArenaInterned<T, Hash, Eq, Tag>;

  // The archive type. See `ArenaInterned::Archive` for details.
  using Archive =
      LocalArenaArchive<T, Tag, static_min_block_size, static_max_block_size>;

  // Creates an empty `Interner` with a static block size.
  LocalArenaInterner() {
    static_assert(static_min_block_size > 0 && static_max_block_size > 0);
  }

  // Creates an empty `Interner` with a fixed dynamic block size.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<dependent_static_min_block_size == 0 &&
                                 dependent_static_max_block_size == 0,
                             int> = 0>
  explicit LocalArenaInterner(size_t block_size) : arena_(block_size) {}

  // Creates an empty `Interner` with an adaptive dynamic block size.
  template <size_t dependent_static_min_block_size = static_min_block_size,
            size_t dependent_static_max_block_size = static_max_block_size,
            std::enable_if_t<dependent_static_min_block_size == 0 &&
                                 dependent_static_max_block_size == 0,
                             int> = 0>
  explicit LocalArenaInterner(size_t min_block_size, size_t max_block_size)
      : arena_(min_block_size, max_block_size) {}

  // A moved-from `Interner` is left empty.
  LocalArenaInterner(LocalArenaInterner&& that) noexcept
      : arena_(std::move(that.arena_)),
        shards_(std::move(that.shards_)),
        num_objects_([&] {
          if constexpr (kConcurrent) {
            return that.num_objects_.exchange(0, std::memory_order_relaxed);
          } else {
            return std::exchange(that.num_objects_, 0);
          }
        }()),
        is_archived_in_place_(
            std::exchange(that.is_archived_in_place_, false)) {}
  LocalArenaInterner& operator=(LocalArenaInterner&& that) noexcept {
    arena_ = std::move(that.arena_);
    shards_ = std::move(that.shards_);
    if constexpr (kConcurrent) {
      num_objects_.store(
          that.num_objects_.exchange(0, std::memory_order_relaxed),
          std::memory_order_relaxed);
    } else {
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

  // Prepares the interner for the expected number of distinct objects.
  // This reduces reallocations.
  void Reserve(size_t capacity) {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of ArenaInterned::Interner::Reserve(): "
           "interner is archived in-place";
    if (capacity == 0) return;
    arena_.Reserve(capacity);
    const size_t capacity_per_shard = capacity / num_shards;
    if (capacity_per_shard > 0) {
      for (Shard& shard : shards_) {
        shard.Reserve(capacity_per_shard);
      }
    }
  }

  // Creates an `ArenaInterned` referring to the constructed object, or sharing
  // an existing object if an equal object already exists.
  //
  // If `likely_new` is `true`, `Intern()` is optimized for the case where an
  // equal object does not exist yet.
  //
  // If `is_new != nullptr`, `*is_new` is set to `true` if a new object was
  // created, or `false` if an equal object already existed.

  template <
      typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  Interned Intern() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal());
  }

  // This function handles the general case. Specific argument types are
  // optimized by separate overloads below.
  template <bool likely_new = false>
  Interned Intern(Initializer<T> arg, bool* absl_nullable is_new = nullptr)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(
        InternInternal<likely_new>(std::move(arg).Reference(), is_new));
  }

  // Creates an object from a brace-enclosed initializer list, with the type
  // defaulting to `T` if it is not specified.
  template <bool likely_new = false, typename Arg = T,
            std::enable_if_t<std::is_same_v<std::decay_t<Arg>, T>, int> = 0>
  Interned Intern(Arg&& arg, bool* absl_nullable is_new = nullptr)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
  }

  // Optimized overload for heterogeneous lookup. The argument is implicitly
  // convertible to `T` and is supported by `Hash` and `Eq`.
  template <
      bool likely_new = false, typename Arg,
      std::enable_if_t<
          std::conjunction_v<NotSameRef<OptionalInterned, Arg>,
                             NotSameRef<std::nullptr_t, Arg>,
                             NotSameRef<T, Arg>, std::is_convertible<Arg&&, T>,
                             SupportedByHashAndEq<Arg, T, Hash, Eq>>,
          int> = 0>
  Interned Intern(Arg&& arg, bool* absl_nullable is_new = nullptr)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      bool likely_new = false, typename DependentT = T,
      std::enable_if_t<
          std::conjunction_v<
              std::is_convertible<const char*, DependentT>,
              std::is_constructible<DependentT, absl::string_view>,
              SupportedByHashAndEq<absl::string_view, DependentT, Hash, Eq>>,
          int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE Interned
  Intern(const char* arg,
         bool* absl_nullable is_new = nullptr) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(absl::string_view(arg), is_new));
  }

  // Optimized overload for heterogeneous lookup. The argument is
  // `riegeli::Maker(arg)` or `riegeli::MakerFor<T>(arg)`, with `arg` being
  // explicitly convertible to `T` and supported by `Hash` and `Eq`.
  template <bool likely_new = false, typename Arg,
            std::enable_if_t<
                std::conjunction_v<std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  Interned Intern(MakerType<Arg> arg, bool* absl_nullable is_new = nullptr)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(
        InternInternal<likely_new>(std::move(arg).template arg<0>(), is_new));
  }
  template <bool likely_new = false, typename Arg,
            std::enable_if_t<
                std::conjunction_v<std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  Interned Intern(MakerTypeFor<T, Arg> arg,
                  bool* absl_nullable is_new = nullptr)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(
        InternInternal<likely_new>(std::move(arg).template arg<0>(), is_new));
  }

  // Optimized overload for a default-constructed object. The argument is
  // `riegeli::Maker()` or `riegeli::Maker<T>()`.
  template <
      bool likely_new = false, typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  Interned Intern(MakerType<> /*arg*/, bool* absl_nullable is_new = nullptr)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(is_new));
  }
  template <
      bool likely_new = false, typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  Interned Intern(MakerTypeFor<T> /*arg*/, bool* absl_nullable is_new = nullptr)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(is_new));
  }

  // Const `Intern()` overloads enabled only when thread-safe.
  //
  // If `likely_new` is `true`, `Intern()` is optimized for the case where an
  // equal object does not exist yet.
  //
  // If `is_new != nullptr`, `*is_new` is set to `true` if a new object was
  // created, or `false` if an equal object already existed.

  template <typename DependentT = T, typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_default_constructible<DependentT>>,
                int> = 0>
  Interned Intern() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal());
  }

  // This function handles the general case. Specific argument types are
  // optimized by separate overloads below.
  template <
      bool likely_new = false, typename DependentMutex = Mutex,
      std::enable_if_t<std::negation_v<std::is_same<DependentMutex, NullMutex>>,
                       int> = 0>
  Interned Intern(Initializer<T> arg, bool* absl_nullable is_new = nullptr)
      const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(
        InternInternal<likely_new>(std::move(arg).Reference(), is_new));
  }

  // Creates an object from a brace-enclosed initializer list, with the type
  // defaulting to `T` if it is not specified.
  template <bool likely_new = false, typename Arg = T,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_same<std::decay_t<Arg>, T>>,
                int> = 0>
  Interned Intern(Arg&& arg, bool* absl_nullable is_new = nullptr) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
  }

  // Optimized overload for heterogeneous lookup. The argument is implicitly
  // convertible to `T` and is supported by `Hash` and `Eq`.
  template <bool likely_new = false, typename Arg,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    NotSameRef<OptionalInterned, Arg>,
                    NotSameRef<std::nullptr_t, Arg>, NotSameRef<T, Arg>,
                    std::is_convertible<Arg&&, T>,
                    SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  Interned Intern(Arg&& arg, bool* absl_nullable is_new = nullptr) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      bool likely_new = false, typename DependentMutex = Mutex,
      typename DependentT = T,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_same<DependentMutex, NullMutex>>,
              std::is_convertible<const char*, DependentT>,
              std::is_constructible<DependentT, absl::string_view>,
              SupportedByHashAndEq<absl::string_view, DependentT, Hash, Eq>>,
          int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE Interned
  Intern(const char* arg, bool* absl_nullable is_new = nullptr) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(absl::string_view(arg), is_new));
  }

  // Optimized overload for heterogeneous lookup. The argument is
  // `riegeli::Maker(arg)` or `riegeli::MakerFor<T>(arg)`, with `arg` being
  // explicitly convertible to `T` and supported by `Hash` and `Eq`.
  template <bool likely_new = false, typename Arg,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_constructible<T, Arg&&>,
                    SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  Interned Intern(MakerType<Arg> arg, bool* absl_nullable is_new = nullptr)
      const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(
        InternInternal<likely_new>(std::move(arg).template arg<0>(), is_new));
  }
  template <bool likely_new = false, typename Arg,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_constructible<T, Arg&&>,
                    SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  Interned Intern(MakerTypeFor<T, Arg> arg,
                  bool* absl_nullable is_new = nullptr) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(
        InternInternal<likely_new>(std::move(arg).template arg<0>(), is_new));
  }

  // Optimized overload for a default-constructed object. The argument is
  // `riegeli::Maker()` or `riegeli::Maker<T>()`.
  template <bool likely_new = false, typename DependentT = T,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_default_constructible<DependentT>>,
                int> = 0>
  Interned Intern(MakerType<> /*arg*/, bool* absl_nullable is_new = nullptr)
      const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(is_new));
  }
  template <bool likely_new = false, typename DependentT = T,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_default_constructible<DependentT>>,
                int> = 0>
  Interned Intern(MakerTypeFor<T> /*arg*/, bool* absl_nullable is_new = nullptr)
      const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Interned(InternInternal<likely_new>(is_new));
  }

  // Finds an existing `ArenaInterned` matching the given argument, or returns
  // null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the object already exists, such as looking up in a map
  // with interned keys.
  template <
      typename Arg,
      std::enable_if_t<SupportedByHashAndEq<Arg, T, Hash, Eq>::value, int> = 0>
  OptionalInterned Find(const Arg& arg) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return OptionalInterned(FindInternal(arg));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentT = T,
            std::enable_if_t<SupportedByHashAndEq<absl::string_view, DependentT,
                                                  Hash, Eq>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalInterned
  Find(const char* arg) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return OptionalInterned(FindInternal(absl::string_view(arg)));
  }

  // Returns the number of objects managed by the interner.
  size_t NumObjects() const {
    if constexpr (kConcurrent) {
      return num_objects_.load(std::memory_order_relaxed);
    } else {
      return num_objects_;
    }
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const LocalArenaInterner* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->arena_);
    memory_estimator.RegisterSubobjects(&self->shards_);
  }

  // Extracts the storage of the objects as an `Archive`. The `Interner` is left
  // empty.
  //
  // See `ArenaInterned::Archive` for details.
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

  // Archives the storage of the objects in place, releasing the lookup
  // structures of the `Interner`.
  //
  // This saves memory once all objects have been interned.
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
  friend class GlobalArenaInterner<T, Hash, Eq, Tag, Mutex, num_shards,
                                   static_min_block_size,
                                   static_max_block_size>;

  static constexpr bool kConcurrent = !std::is_same_v<Mutex, NullMutex>;

  using ArenaMutex = std::conditional_t<kConcurrent, absl::Mutex, NullMutex>;
  using Arena = typename ObjectArena<T>::template Concurrent<ArenaMutex>::
      template WithBlockSize<static_min_block_size, static_max_block_size>;
  using Shard = ObjectArenaShard<T, Hash, Eq, Mutex>;

  void ResetShards() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    for (Shard& shard : shards_) {
      shard.Reset();
    }
    if constexpr (kConcurrent) {
      num_objects_.store(0, std::memory_order_relaxed);
    } else {
      num_objects_ = 0;
    }
    is_archived_in_place_ = false;
  }

  template <bool likely_new, typename Arg>
  const T* InternInternal(Arg&& arg, bool* absl_nullable is_new) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of ArenaInterned::Interner::Intern(): "
           "interner is archived in-place";
    const size_t hash = Hash()(arg);
    bool is_new_internal;
    const T* result;
    if constexpr (likely_new) {
      result = GetShard(hash).template InternNew</*verified_new=*/false>(
          std::forward<Arg>(arg), hash, arena_, is_new_internal);
    } else {
      result = GetShard(hash).Intern(std::forward<Arg>(arg), hash, arena_,
                                     is_new_internal);
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

  template <bool likely_new = false>
  const T* InternInternal(bool* absl_nullable is_new = nullptr) const {
    if constexpr (std::is_copy_constructible_v<T>) {
      return InternInternal<likely_new>(riegeli::Global<T>(), is_new);
    } else {
      return InternInternal<likely_new>(T(), is_new);
    }
  }

  template <typename Arg>
  const T* FindInternal(const Arg& arg) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of ArenaInterned::Interner::Find(): "
           "interner is archived in-place";
    const size_t hash = Hash()(arg);
    return GetShard(hash).Find(arg, hash);
  }

  Shard& GetShard(size_t hash) const {
    return shards_[ShardIndex<num_shards>(hash)];
  }

  mutable Arena arena_;
  mutable std::array<Shard, num_shards> shards_;
  mutable std::conditional_t<kConcurrent, std::atomic<size_t>, size_t>
      num_objects_{0};
  bool is_archived_in_place_ = false;
};

// The public name of `GlobalArenaInterner<T>` is
// `ArenaInterned<T>::GlobalInterner`.
//
// `ArenaInterned<T>::GlobalInterner` represents a global interner for the
// given `T` and other template parameters. See `ArenaInterned::Interner` for a
// non-global version.
//
// Since objects are owned by the interner, using a global arena interner risks
// running out of memory unless the number of objects ever interned is limited.
// A non-global interner restricts the risk to a smaller scope and is preferred.
template <typename T, typename Hash, typename Eq, typename Tag, typename Mutex,
          size_t num_shards, size_t static_min_block_size,
          size_t static_max_block_size>
class GlobalArenaInterner {
 public:
  static_assert(static_min_block_size > 0 && static_max_block_size > 0,
                "Global interner cannot have dynamic block size");

  // Makes the interner thread-safe and tunes it for concurrency. See
  // `ArenaInterned::Interner::Concurrent` for details.
  //
  // By default, a global interner is tuned for concurrency and has multiple
  // shards.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent =
      GlobalArenaInterner<T, Hash, Eq, Tag, NewMutex, new_num_shards,
                          static_min_block_size, static_max_block_size>;

  // Configures the block size of the arena. See
  // `ArenaInterned::Interner::WithBlockSize` for details.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize =
      GlobalArenaInterner<T, Hash, Eq, Tag, Mutex, num_shards,
                          new_static_min_block_size, new_static_max_block_size>;

  // References to interned objects. See `ArenaInterned` and
  // `ArenaInterned::Optional` for details.
  using Interned = ArenaInterned<T, Hash, Eq, Tag>;
  using OptionalInterned = OptionalArenaInterned<T, Hash, Eq, Tag>;

  // Since `ArenaInterned::GlobalInterner` is stateless, all instances are
  // equivalent. Member functions are static. Instantiation is provided for
  // consistency with other interner categories.
  GlobalArenaInterner() = default;

  GlobalArenaInterner(const GlobalArenaInterner& that) = default;
  GlobalArenaInterner& operator=(const GlobalArenaInterner& that) = default;

  // Creates an `ArenaInterned` referring to the constructed object, or sharing
  // an existing object if an equal object already exists.
  //
  // If `likely_new` is `true`, `Intern()` is optimized for the case where an
  // equal object does not exist yet.
  //
  // If `is_new != nullptr`, `*is_new` is set to `true` if a new object was
  // created, or `false` if an equal object already existed.

  // Optimized overload for a default-constructed object.
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  static Interned Intern() {
    return Interned(InternInternal());
  }

  // This function handles the general case. Specific argument types are
  // optimized by separate overloads below.
  template <bool likely_new = false>
  static Interned Intern(Initializer<T> arg,
                         bool* absl_nullable is_new = nullptr) {
    return Interned(
        InternInternal<likely_new>(std::move(arg).Reference(), is_new));
  }

  // Creates an object from a brace-enclosed initializer list, with the type
  // defaulting to `T` if it is not specified.
  template <bool likely_new = false, typename Arg = T,
            std::enable_if_t<std::is_same_v<std::decay_t<Arg>, T>, int> = 0>
  static Interned Intern(Arg&& arg, bool* absl_nullable is_new = nullptr) {
    return Interned(InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
  }

  // Optimized overload for heterogeneous lookup. The argument is implicitly
  // convertible to `T` and is supported by `Hash` and `Eq`.
  template <
      bool likely_new = false, typename Arg,
      std::enable_if_t<
          std::conjunction_v<NotSameRef<OptionalInterned, Arg>,
                             NotSameRef<std::nullptr_t, Arg>,
                             NotSameRef<T, Arg>, std::is_convertible<Arg&&, T>,
                             SupportedByHashAndEq<Arg, T, Hash, Eq>>,
          int> = 0>
  static Interned Intern(Arg&& arg, bool* absl_nullable is_new = nullptr) {
    return Interned(InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      bool likely_new = false, typename DependentT = T,
      std::enable_if_t<
          std::conjunction_v<
              std::is_convertible<const char*, DependentT>,
              std::is_constructible<DependentT, absl::string_view>,
              SupportedByHashAndEq<absl::string_view, DependentT, Hash, Eq>>,
          int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static Interned Intern(
      const char* arg, bool* absl_nullable is_new = nullptr) {
    return Interned(InternInternal<likely_new>(absl::string_view(arg), is_new));
  }

  // Optimized overload for heterogeneous lookup. The argument is
  // `riegeli::Maker(arg)` or `riegeli::MakerFor<T>(arg)`, with `arg` being
  // explicitly convertible to `T` and supported by `Hash` and `Eq`.
  template <bool likely_new = false, typename Arg,
            std::enable_if_t<
                std::conjunction_v<std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  static Interned Intern(MakerType<Arg> arg,
                         bool* absl_nullable is_new = nullptr) {
    return Interned(
        InternInternal<likely_new>(std::move(arg).template arg<0>(), is_new));
  }
  template <bool likely_new = false, typename Arg,
            std::enable_if_t<
                std::conjunction_v<std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  static Interned Intern(MakerTypeFor<T, Arg> arg,
                         bool* absl_nullable is_new = nullptr) {
    return Interned(
        InternInternal<likely_new>(std::move(arg).template arg<0>(), is_new));
  }

  // Optimized overload for a default-constructed object. The argument is
  // `riegeli::Maker()` or `riegeli::Maker<T>()`.
  template <
      bool likely_new = false, typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  static Interned Intern(MakerType<> /*arg*/,
                         bool* absl_nullable is_new = nullptr) {
    return Interned(InternInternal(is_new));
  }
  template <
      bool likely_new = false, typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  static Interned Intern(MakerTypeFor<T> /*arg*/,
                         bool* absl_nullable is_new = nullptr) {
    return Interned(InternInternal(is_new));
  }

  // Finds an existing `ArenaInterned` matching the given argument, or returns
  // null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the object already exists, such as looking up in a map
  // with interned keys.
  template <
      typename Arg,
      std::enable_if_t<SupportedByHashAndEq<Arg, T, Hash, Eq>::value, int> = 0>
  static OptionalInterned Find(const Arg& arg) {
    return OptionalInterned(FindInternal(arg));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentT = T,
            std::enable_if_t<SupportedByHashAndEq<absl::string_view, DependentT,
                                                  Hash, Eq>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static OptionalInterned Find(const char* arg) {
    return OptionalInterned(FindInternal(absl::string_view(arg)));
  }

  // Returns an immortal `ArenaInterned` with a specific value.
  //
  // This avoids finding the object each time.
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

  // Returns a snapshot of the number of objects managed by the interner.
  static size_t NumObjects() {
    return riegeli::Global<LocalInterner>().NumObjects();
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const GlobalArenaInterner* /*self*/,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&riegeli::Global<LocalInterner>());
  }

 private:
  friend Interned;  // For `InternInternal()`.

  using LocalInterner =
      LocalArenaInterner<T, Hash, Eq, Tag, Mutex, num_shards,
                         static_min_block_size, static_max_block_size>;

  template <bool likely_new, typename Arg>
  static const T* InternInternal(Arg&& arg, bool* absl_nullable is_new) {
    return riegeli::Global<LocalInterner>().template InternInternal<likely_new>(
        std::forward<Arg>(arg), is_new);
  }

  static const T* InternInternal(bool* absl_nullable is_new = nullptr) {
    if (is_new != nullptr) {
      return riegeli::Global<LocalInterner>()
          .template InternInternal</*likely_new=*/false>(is_new);
    }
    return riegeli::Global(
        [] { return riegeli::Global<LocalInterner>().InternInternal(); });
  }

  template <typename Arg>
  static const T* FindInternal(const Arg& arg) {
    return riegeli::Global<LocalInterner>().FindInternal(arg);
  }
};

// Implementation details follow.

template <typename T, typename Hash, typename Eq, typename Tag>
class OptionalArenaInterned<T, Hash, Eq, Tag>::ByAddress
    : public WithCompare<ByAddress> {
 public:
  /*implicit*/ ByAddress(Optional view) : repr_(view.get()) {}

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
  const T* absl_nullable repr_;
};

template <typename T, typename Hash, typename Eq, typename Tag>
struct OptionalArenaInterned<T, Hash, Eq, Tag>::absl_container_hash {
  using is_transparent = void;
  size_t operator()(Optional self) const { return absl::HashOf(self.get()); }
  size_t operator()(NotOptional self) const { return absl::HashOf(self.get()); }
  size_t operator()(std::nullptr_t) const { return absl::HashOf(nullptr); }
};

template <typename T, typename Hash, typename Eq, typename Tag>
struct OptionalArenaInterned<T, Hash, Eq, Tag>::absl_container_eq {
  using is_transparent = void;
  bool operator()(Optional a, Optional b) const { return a.get() == b.get(); }
  bool operator()(Optional a, NotOptional b) const {
    return a.get() == b.get();
  }
  bool operator()(NotOptional a, Optional b) const {
    return a.get() == b.get();
  }
  bool operator()(NotOptional a, NotOptional b) const {
    return a.get() == b.get();
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

template <typename T, typename Hash, typename Eq, typename Tag>
struct OptionalArenaInterned<T, Hash, Eq, Tag>::ValueHash {
  using is_transparent = void;
  size_t operator()(Optional self) const {
    if (self.get() == nullptr) {
      if constexpr (HasTransparentNullptrHash<Hash>::value) {
        return hash(nullptr);
      } else {
        return absl::HashOf(nullptr);
      }
    }
    return hash(*self);
  }
  size_t operator()(NotOptional self) const { return hash(*self); }
  size_t operator()(std::nullptr_t) const {
    if constexpr (HasTransparentNullptrHash<Hash>::value) {
      return hash(nullptr);
    } else {
      return absl::HashOf(nullptr);
    }
  }
  size_t operator()(const T& arg) const { return hash(arg); }
  template <
      typename PassedKey, typename DependentHash = Hash,
      typename = typename DependentHash::is_transparent,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, PassedKey>,
              NotSameRef<std::nullptr_t, PassedKey>, NotSameRef<T, PassedKey>,
              std::is_invocable<const DependentHash&, const PassedKey&>>,
          int> = 0>
  size_t operator()(const PassedKey& arg) const {
    return hash(arg);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Hash hash;
};

template <typename T, typename Hash, typename Eq, typename Tag>
struct OptionalArenaInterned<T, Hash, Eq, Tag>::ValueEq {
  using is_transparent = void;
  bool operator()(Optional a, Optional b) const { return a.get() == b.get(); }
  bool operator()(Optional a, NotOptional b) const {
    return a.get() == b.get();
  }
  bool operator()(NotOptional a, Optional b) const {
    return a.get() == b.get();
  }
  bool operator()(NotOptional a, NotOptional b) const {
    return a.get() == b.get();
  }
  bool operator()(Optional a, std::nullptr_t) const {
    return a.get() == nullptr;
  }
  bool operator()(std::nullptr_t, Optional b) const {
    return b.get() == nullptr;
  }
  bool operator()(NotOptional /*a*/, std::nullptr_t) const { return false; }
  bool operator()(std::nullptr_t, NotOptional /*b*/) const { return false; }
  bool operator()(Optional a, const T& b) const {
    if (a.get() == nullptr) return false;
    return eq(*a, b);
  }
  bool operator()(const T& a, Optional b) const {
    if (b.get() == nullptr) return false;
    return eq(*b, a);
  }
  bool operator()(NotOptional a, const T& b) const { return eq(*a, b); }
  bool operator()(const T& a, NotOptional b) const { return eq(*b, a); }
  template <typename PassedKey, typename DependentEq = Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<std::conjunction_v<
                                 NotSameRef<Optional, PassedKey>,
                                 NotSameRef<std::nullptr_t, PassedKey>,
                                 NotSameRef<T, PassedKey>,
                                 std::is_invocable<const DependentEq&, const T&,
                                                   const PassedKey&>>,
                             int> = 0>
  bool operator()(Optional a, const PassedKey& b) const {
    if (a.get() == nullptr) return false;
    return eq(*a, b);
  }
  template <typename PassedKey, typename DependentEq = Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<std::conjunction_v<
                                 NotSameRef<Optional, PassedKey>,
                                 NotSameRef<std::nullptr_t, PassedKey>,
                                 NotSameRef<T, PassedKey>,
                                 std::is_invocable<const DependentEq&, const T&,
                                                   const PassedKey&>>,
                             int> = 0>
  bool operator()(const PassedKey& a, Optional b) const {
    if (b.get() == nullptr) return false;
    return eq(*b, a);
  }
  template <typename PassedKey, typename DependentEq = Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<std::conjunction_v<
                                 NotSameRef<Optional, PassedKey>,
                                 NotSameRef<std::nullptr_t, PassedKey>,
                                 NotSameRef<T, PassedKey>,
                                 std::is_invocable<const DependentEq&, const T&,
                                                   const PassedKey&>>,
                             int> = 0>
  bool operator()(NotOptional a, const PassedKey& b) const {
    return eq(*a, b);
  }
  template <typename PassedKey, typename DependentEq = Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<std::conjunction_v<
                                 NotSameRef<Optional, PassedKey>,
                                 NotSameRef<std::nullptr_t, PassedKey>,
                                 NotSameRef<T, PassedKey>,
                                 std::is_invocable<const DependentEq&, const T&,
                                                   const PassedKey&>>,
                             int> = 0>
  bool operator()(const PassedKey& a, NotOptional b) const {
    return eq(*b, a);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Eq eq;
};

}  // namespace interned_internal

}  // namespace riegeli

#endif  // RIEGELI_INTERNED_ARENA_INTERNED_OBJECT_H_
