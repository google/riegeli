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

#ifndef RIEGELI_INTERNED_INDEX_INTERNED_OBJECT_H_
#define RIEGELI_INTERNED_INDEX_INTERNED_OBJECT_H_

#include <stddef.h>

#include <array>
#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/hash_container_defaults.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/interned/arena_interned_object.h"
#include "riegeli/interned/index_interned_object_internal.h"
#include "riegeli/interned/interned_common_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Mutex type that does not lock.
using interned_internal::NullMutex;

// Default template parameter `num_shards` for `Concurrent` nested types.
using interned_internal::kDefaultInternerNumShards;

// Default template parameter `block_size` for `IndexInterned::Interner` and
// `IndexInterned::Archive`.
using interned_internal::kDefaultArenaFixedBlockSize;

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename Tag>
class IndexInterned;

namespace interned_internal {

template <typename Numeric, typename T, typename Tag, size_t block_size>
class IndexArchive;

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename Tag, typename Mutex, size_t num_shards, size_t block_size>
class IndexInterner;

// The public name of `OptionalIndexInterned<Numeric, T>` is
// `IndexInterned<Numeric, T>::Optional`.
//
// `IndexInterned<Numeric, T>` refers to an object of type `T` by a numeric
// index, ensuring that equal objects are shared to minimize memory usage.
//
// In contrast to `IndexInterned`, `IndexInterned::Optional` can be
// null. It is more efficient than `std::optional<IndexInterned>`.
//
// See `IndexInterned` for details.
template <typename Numeric, typename T, typename Hash, typename Eq,
          typename Tag>
class OptionalIndexInterned
    : public WithCompare<OptionalIndexInterned<Numeric, T, Hash, Eq, Tag>,
                         std::nullptr_t> {
 public:
  // Navigates between `IndexInterned` and `IndexInterned::Optional`.
  using NotOptional = IndexInterned<Numeric, T, Hash, Eq, Tag>;
  using Optional = OptionalIndexInterned;

  // The type of the arena-interned object resolved from an index.
  // See `IndexInterned::Resolved` for details.
  using Resolved = ArenaInterned<T, Hash, Eq, Tag>;

  // The default interner type. See `IndexInterned::Interner` for details.
  using Interner =
      interned_internal::IndexInterner<Numeric, T, Hash, Eq, Tag, NullMutex,
                                       /*num_shards=*/1,
                                       kDefaultArenaFixedBlockSize>;

  // The default archive type. See `IndexInterned::Archive` for details.
  using Archive = interned_internal::IndexArchive<Numeric, T, Tag,
                                                  kDefaultArenaFixedBlockSize>;

  // Creates a null `IndexInterned::Optional`.
  //
  // This differs from the default constructor of `IndexInterned`, which is
  // deleted.
  OptionalIndexInterned() = default;
  /*implicit*/ OptionalIndexInterned(std::nullptr_t) {}
  OptionalIndexInterned& operator=(std::nullptr_t) {
    numeric_ = kNullNumeric<Numeric>;
    return *this;
  }

  OptionalIndexInterned(const OptionalIndexInterned& that) = default;
  OptionalIndexInterned& operator=(const OptionalIndexInterned& that) = default;

  // Returns `true` if not null.
  explicit operator bool() const {
    return numeric_or_max() != kNullNumeric<Numeric>;
  }

  // Converts from `IndexInterned::Optional` to `IndexInterned`.
  NotOptional not_optional() const {
    RIEGELI_ASSERT(*this) << "Failed precondition of "
                             "IndexInterned::Optional::not_optional(): "
                             "null index";
    return NotOptional(numeric_or_max());
  }
  NotOptional NotOptionalOrDie() const {
    RIEGELI_CHECK(*this) << "Failed precondition of "
                            "IndexInterned::Optional::NotOptionalOrDie(): "
                            "null index";
    return NotOptional(numeric_or_max());
  }

  // Equality of non-null `IndexInterned` or `IndexInterned::Optional`
  // objects corresponds to equality of the objects they refer to, but is fast,
  // comparing the indices.
  //
  // Other comparisons sort objects by the order of their construction in the
  // interner, with null being the minimum.
  //
  // All comparisons are valid only for `IndexInterned` or
  // `IndexInterned::Optional` objects coming from the same interner.

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

  // Hashing `IndexInterned` or `IndexInterned::Optional` is fast, hashing the
  // index.
  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, Optional self) {
    return HashState::combine(std::move(hash_state), self.numeric_or_max());
  }

  // Default hash and equality for containers with `IndexInterned` or
  // `IndexInterned::Optional` as the key type, hashing and comparing by index,
  // supporting heterogeneous lookup against `NotOptional` and `Optional`.
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
  explicit OptionalIndexInterned(Numeric numeric) : numeric_(numeric) {}

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
  template <typename NumericParam, typename TParam, typename HashParam,
            typename EqParam, typename TagParam, typename MutexParam,
            size_t num_shards, size_t block_size>
  friend class IndexInterner;

  Numeric numeric_ = kNullNumeric<Numeric>;
};

}  // namespace interned_internal

// `IndexInterned<Numeric, T>` refers to an object of type `T` by a numeric
// index, ensuring that equal objects are shared to minimize memory usage.
//
// `IndexInterned` is never null. See `IndexInterned::Optional` for a
// variant that can be null. `IndexInterned` is generally preferred over
// `IndexInterned::Optional`.
//
// See `IndexInternedString` for a variant optimized for strings.
//
// `IndexInterned` objects are created by an interner, which maintains a
// set of arena-allocated objects to share. The interner is managed explicitly.
// Interned objects are destroyed and erased when the interner is destroyed.
//
// Since objects are owned by the interner, using an arena interner risks
// exhausting the numeric space or running out of memory unless the number
// of distinct objects ever interned by the given interner is limited.
//
// See `ArenaInterned` for a variant that refers to objects by a pointer-like
// handle. An index can be more compact than a pointer and indices are allocated
// consecutively, which allows representing dense maps as vectors, but the
// interner is needed to resolve an index to the object, which is slightly
// slower, and the numeric space can be exhausted. In contrast to
// `ArenaInterned::GlobalInterner`, a global version of
// `IndexInterned::Interner` is not provided.
//
// Asymptotic memory usage per interned object:
//   active: sizeof(T) + 1.65 * sizeof(Numeric) + 1.65
//   archived: sizeof(T)
//
// Breakdown:
//  + entry in `absl::flat_hash_set<Numeric>`:
//      8 / (7 * ln(2)) * (sizeof(Numeric) + 1) unless archived
//  + arena-allocated object: sizeof(T)
//
// Interned handle: sizeof(Numeric)
//
// Among the template parameters, only `Numeric`, `T`, and optionally `Hash` and
// `Eq` should be specified explicitly. Other parameters should be specified by
// nested type `WithTag`. Further parameters are applied to `Interner` or
// `Archive`.
//
// `IndexInterned` derives from `IndexInterned::Optional`. See
// `IndexInterned::Optional` for inherited operations.
template <typename Numeric, typename T,
          typename Hash = absl::DefaultHashContainerHash<T>,
          typename Eq = absl::DefaultHashContainerEq<T>, typename Tag = void>
class IndexInterned
    : public interned_internal::OptionalIndexInterned<Numeric, T, Hash, Eq,
                                                      Tag>,
      public WithCompare<
          IndexInterned<Numeric, T, Hash, Eq, Tag>,
          interned_internal::OptionalIndexInterned<Numeric, T, Hash, Eq, Tag>,
          std::nullptr_t> {
 public:
  // Changes the tag type of the interner.
  //
  // Index interned objects with distinct tags are managed by separate types of
  // interners, even if other template parameters are the same. This allows
  // annotating the type with its role for improved type safety. This forces
  // separation of interners, which can make lookups more efficient.
  template <typename NewTag>
  using WithTag = IndexInterned<Numeric, T, Hash, Eq, NewTag>;

  // Navigates between `IndexInterned` and `IndexInterned::Optional`.
  using NotOptional = typename IndexInterned::NotOptional;
  using Optional = typename IndexInterned::Optional;

  // The type of the arena-interned object resolved from an index.
  //
  // `IndexInterned` resolves to an `ArenaInterned` given
  // the interner, which implicitly refers to an object.
  using Resolved = typename IndexInterned::Resolved;

  // The default interner type. It is used for interning new objects, for
  // resolving indices to objects, and provides statistics.
  //
  // Further parameters should be specified by `Interner` nested types
  // `Concurrent` and `WithBlockSize`.
  using Interner = typename IndexInterned::Interner;

  // The default archive type. It can be used to hold interned objects after
  // all objects have been interned and the `Interner` has been destroyed, for
  // resolving indices to objects, and provides statistics.
  //
  // Further parameters should be specified by `Archive` nested type
  // `WithBlockSize`.
  using Archive = typename IndexInterned::Archive;

  // The default constructor is present in `IndexInterned::Optional` but
  // deleted in `IndexInterned`.
  IndexInterned() = delete;

  // Constructor from `nullptr` is present in `IndexInterned::Optional` but
  // deleted in `IndexInterned`.
  IndexInterned(std::nullptr_t) = delete;
  IndexInterned& operator=(std::nullptr_t) = delete;

  IndexInterned(const IndexInterned& that) = default;
  IndexInterned& operator=(const IndexInterned& that) = default;

  // Returns `true` because `IndexInterned` is never null.
  explicit operator bool() const { return true; }

  // Returns this index.
  NotOptional value() const { return *this; }

  // Returns the numeric value. Indices are allocated consecutively and are
  // non-negative even if `Numeric` is signed.
  //
  // `std::numeric_limits<Numeric>::max()` is unused.
  Numeric numeric() const { return this->numeric_or_max(); }

  // Equality of `IndexInterned` objects corresponds to equality of the objects
  // they refer to, but is fast, comparing the indices.
  //
  // Other comparisons sort objects by the order of their construction in the
  // interner.
  //
  // All comparisons are valid only for `IndexInterned` objects coming from the
  // same interner.

  friend bool operator==(IndexInterned a, IndexInterned b) {
    return a.numeric() == b.numeric();
  }
  friend StrongOrdering RIEGELI_COMPARE(IndexInterned a, IndexInterned b) {
    return riegeli::Compare(a.numeric(), b.numeric());
  }

  friend bool operator==(IndexInterned a, Optional b) {
    return a.numeric() == b.numeric_or_max();
  }
  friend StrongOrdering RIEGELI_COMPARE(IndexInterned a, Optional b) {
    return riegeli::Compare(a.ordered_numeric(), b.ordered_numeric());
  }

  friend bool operator==(IndexInterned /*a*/, std::nullptr_t) { return false; }
  friend StrongOrdering RIEGELI_COMPARE(IndexInterned /*a*/, std::nullptr_t) {
    return StrongOrdering::greater;
  }

  // Restores an `IndexInterned` from a numeric index of a previously interned
  // object.
  //
  // `numeric` must not be `std::numeric_limits<Numeric>::max()` and must be
  // non-negative.
  static IndexInterned BackFromNumeric(Numeric numeric) {
    RIEGELI_ASSERT_NE(numeric, interned_internal::kNullNumeric<Numeric>)
        << "Failed precondition of IndexInterned::BackFromNumeric(): "
           "null numeric value";
    if constexpr (std::is_signed_v<Numeric>) {
      RIEGELI_ASSERT_GE(numeric, 0)
          << "Failed precondition of IndexInterned::BackFromNumeric(): "
             "negative numeric value";
    }
    return IndexInterned(numeric);
  }

  // Supports `HybridDirectMap` and `HybridDirectSet`.
  friend std::make_unsigned_t<Numeric> RiegeliHybridDirectToRawKey(
      IndexInterned key) {
    return IntCast<std::make_unsigned_t<Numeric>>(key.numeric());
  }
  friend IndexInterned RiegeliHybridDirectFromRawKey(
      std::make_unsigned_t<Numeric> raw_key, IndexInterned*) {
    return BackFromNumeric(IntCast<Numeric>(raw_key));
  }

 private:
  friend Optional;  // For `IndexInterned(Numeric)`.
  // For `IndexInterned(Numeric)`.
  template <typename NumericParam, typename TParam, typename HashParam,
            typename EqParam, typename TagParam, typename MutexParam,
            size_t num_shards, size_t block_size>
  friend class interned_internal::IndexInterner;

  explicit IndexInterned(Numeric numeric) : Optional(numeric) {}
};

namespace interned_internal {

// The public name of `IndexArchive<Numeric, T>` is
// `IndexInterned<Numeric, T>::Archive`.
//
// `IndexInterned::Archive` holds interned objects after all objects have
// been interned and the `Interner` has been destroyed, and is used to resolve
// indices to objects, and provides statistics.
//
// Objects obtained from the `Interner` remain valid as long as the `Archive`
// is valid.
//
// This saves memory by releasing the lookup structures of the `Interner`
// once they are no longer needed.
template <typename Numeric, typename T, typename Tag, size_t block_size>
class IndexArchive {
 public:
  static_assert(block_size > 0, "Index archive cannot have dynamic block size");

  // Configures the block size of the arena, in bytes. See
  // `IndexInterned::Interner::WithBlockSize` for details.
  template <size_t new_block_size>
  using WithBlockSize = IndexArchive<Numeric, T, Tag, new_block_size>;

  // Creates an empty `Archive`.
  IndexArchive() = default;

  // A moved-from `Archive` is left empty.
  IndexArchive(IndexArchive&& that) = default;
  IndexArchive& operator=(IndexArchive&& that) = default;

  // `size()` is the same as `NumObjects()`. The name `size()` indicates that
  // it is efficient, not involving locking.
  size_t size() const { return directory_.size(); }

  // Resolves an `IndexInterned` to the object.
  //
  // `index` must have been provided by the `Interner` from which this `Archive`
  // was extracted.
  template <typename Hash, typename Eq>
  ArenaInterned<T, Hash, Eq, Tag> operator[](
      IndexInterned<Numeric, T, Hash, Eq, Tag> index) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_CHECK_LT(IntCast<size_t>(index.numeric()), size())
        << "Failed precondition of "
           "IndexInterned::Archive::operator[]: "
           "index out of bounds";
    return ArenaInterned<T, Hash, Eq, Tag>::BackFromData(
        &directory_[IntCast<size_t>(index.numeric())]);
  }

  // Returns the number of objects in the archive. It does not change.
  size_t NumObjects() const { return size(); }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const IndexArchive* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->directory_);
  }

 private:
  // For `IndexArchive(Directory&&)`.
  template <typename NumericParam, typename TParam, typename Hash, typename Eq,
            typename TagParam, typename MutexParam, size_t num_shards,
            size_t block_size_param>
  friend class IndexInterner;

  using Directory = Directory<T, /*concurrent_reads=*/false, block_size>;

  explicit IndexArchive(Directory&& directory)
      : directory_(std::move(directory)) {
    directory_.ShrinkToFit();
  }

  Directory directory_;
};

// The public name of `IndexInterner<Numeric, T>` is
// `IndexInterned<Numeric, T>::Interner`.
//
// `IndexInterned::Interner` represents an explicitly managed interner.
// It arena-allocates and manages a set of interned objects. The objects are
// owned by the interner and are destroyed when the interner is destroyed.
template <typename Numeric, typename T, typename Hash, typename Eq,
          typename Tag, typename Mutex, size_t num_shards, size_t block_size>
class IndexInterner {
 public:
  static_assert(block_size > 0,
                "Index interner cannot have dynamic block size");

  // Makes the interner thread-safe and tunes it for concurrency.
  //
  // By default, the interner is not thread-safe and has a single shard.
  // With more shards, parallel usage is less likely to cause contention.
  //
  // `Mutex` protects the set of object indices in each shard.
  //
  // A mutex must support `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent = IndexInterner<Numeric, T, Hash, Eq, Tag, NewMutex,
                                   new_num_shards, block_size>;

  // Configures the block size of the arena, in bytes.
  //
  // Objects are allocated in blocks of this size. A larger block size improves
  // memory locality and reduces the number of allocations, but increases wasted
  // memory if only a small number of objects is interned.
  template <size_t new_block_size>
  using WithBlockSize = IndexInterner<Numeric, T, Hash, Eq, Tag, Mutex,
                                      num_shards, new_block_size>;

  // References to interned objects. See `IndexInterned` and
  // `IndexInterned::Optional` for details.
  using Index = IndexInterned<Numeric, T, Hash, Eq, Tag>;
  using OptionalIndex = OptionalIndexInterned<Numeric, T, Hash, Eq, Tag>;

  // The type of the arena-interned object resolved from an index.
  // See `IndexInterned::Resolved` for details.
  using Resolved = ArenaInterned<T, Hash, Eq, Tag>;

  // The archive type. See `IndexInterned::Archive` for details.
  using Archive = IndexArchive<Numeric, T, Tag, block_size>;

  // Creates an empty `Interner`.
  IndexInterner() = default;

  IndexInterner(const IndexInterner&) = delete;
  IndexInterner& operator=(const IndexInterner&) = delete;

  // Resets the interner to the empty state.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    directory_.Reset();
    ResetShards();
  }

  // Prepares the interner for the expected number of distinct objects.
  // This reduces reallocations.
  void Reserve(size_t capacity) {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInterned::Interner::Reserve(): "
           "interner is archived in-place";
    if (capacity == 0) return;
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

  // `size()` is the same as `NumObjects()`. The name `size()` indicates that
  // it is efficient, not involving locking.
  size_t size() const { return directory_.size(); }

  // Resolves an `IndexInterned` to the object.
  //
  // `index` must have been provided by this `Interner`.
  Resolved operator[](Index index) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_CHECK_LT(IntCast<size_t>(index.numeric()), size())
        << "Failed precondition of IndexInterned::Interner::operator[]: "
           "index out of bounds";
    return Resolved::BackFromData(
        &directory_[IntCast<size_t>(index.numeric())]);
  }

  // Creates an `IndexInterned` referring to the constructed object, or
  // sharing an existing object if an equal object already exists.
  //
  // `Intern()` returns null if the numeric space for a new index is exhausted.
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
  OptionalIndex Intern() {
    return OptionalIndex(InternInternal());
  }

  // This function handles the general case. Specific argument types are
  // optimized by separate overloads below.
  template <bool likely_new = false>
  OptionalIndex Intern(Initializer<T> arg,
                       bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(
        InternInternal<likely_new>(std::move(arg).Reference(), is_new));
  }

  // Creates an object from a brace-enclosed initializer list, with the type
  // defaulting to `T` if it is not specified.
  template <bool likely_new = false, typename Arg = T,
            std::enable_if_t<std::is_same_v<std::decay_t<Arg>, T>, int> = 0>
  OptionalIndex Intern(Arg&& arg, bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(
        InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
  }

  // Optimized overload for heterogeneous lookup. The argument is implicitly
  // convertible to `T` and is supported by `Hash` and `Eq`.
  template <
      bool likely_new = false, typename Arg,
      std::enable_if_t<
          std::conjunction_v<NotSameRef<T, Arg>, std::is_convertible<Arg&&, T>,
                             SupportedByHashAndEq<Arg, T, Hash, Eq>>,
          int> = 0>
  OptionalIndex Intern(Arg&& arg, bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(
        InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
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
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalIndex
  Intern(const char* arg, bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(
        InternInternal<likely_new>(absl::string_view(arg), is_new));
  }

  // Optimized overload for heterogeneous lookup. The argument is
  // `riegeli::Maker(arg)` or `riegeli::MakerFor<T>(arg)`, with `arg` being
  // explicitly convertible to `T` and supported by `Hash` and `Eq`.
  template <bool likely_new = false, typename Arg,
            std::enable_if_t<
                std::conjunction_v<std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  OptionalIndex Intern(MakerType<Arg> arg,
                       bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(
        InternInternal<likely_new>(std::move(arg).template arg<0>(), is_new));
  }
  template <bool likely_new = false, typename Arg,
            std::enable_if_t<
                std::conjunction_v<std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  OptionalIndex Intern(MakerTypeFor<T, Arg> arg,
                       bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(
        InternInternal<likely_new>(std::move(arg).template arg<0>(), is_new));
  }

  // Optimized overload for a default-constructed object. The argument is
  // `riegeli::Maker()` or `riegeli::Maker<T>()`.
  template <
      bool likely_new = false, typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  OptionalIndex Intern(MakerType<> /*arg*/,
                       bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(InternInternal<likely_new>(is_new));
  }
  template <
      bool likely_new = false, typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  OptionalIndex Intern(MakerTypeFor<T> /*arg*/,
                       bool* absl_nullable is_new = nullptr) {
    return OptionalIndex(InternInternal<likely_new>(is_new));
  }

  // Const `Intern()` overloads enabled only when thread-safe.
  //
  // `Intern()` returns null if the numeric space for a new index is exhausted.
  //
  // If `likely_new` is `true`, `Intern()` is optimized for the case where an
  // equal object does not exist yet.
  //
  // If `is_new != nullptr`, `*is_new` is set to `true` if a new object was
  // created, or `false` if an equal object already existed.

  // Optimized overload for a default-constructed object.
  template <typename DependentT = T, typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_default_constructible<DependentT>>,
                int> = 0>
  OptionalIndex Intern() const {
    return OptionalIndex(InternInternal());
  }

  // This function handles the general case. Specific argument types are
  // optimized by separate overloads below.
  template <
      bool likely_new = false, typename DependentMutex = Mutex,
      std::enable_if_t<!std::is_same_v<DependentMutex, NullMutex>, int> = 0>
  OptionalIndex Intern(Initializer<T> arg,
                       bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(
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
  OptionalIndex Intern(Arg&& arg, bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(
        InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
  }

  // Optimized overload for heterogeneous lookup. The argument is implicitly
  // convertible to `T` and is supported by `Hash` and `Eq`.
  template <bool likely_new = false, typename Arg,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    NotSameRef<T, Arg>, std::is_convertible<Arg&&, T>,
                    SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  OptionalIndex Intern(Arg&& arg, bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(
        InternInternal<likely_new>(std::forward<Arg>(arg), is_new));
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
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalIndex
  Intern(const char* arg, bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(
        InternInternal<likely_new>(absl::string_view(arg), is_new));
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
  OptionalIndex Intern(MakerType<Arg> arg,
                       bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(
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
  OptionalIndex Intern(MakerTypeFor<T, Arg> arg,
                       bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(
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
  OptionalIndex Intern(MakerType<> /*arg*/,
                       bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(InternInternal<likely_new>(is_new));
  }
  template <bool likely_new = false, typename DependentT = T,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_default_constructible<DependentT>>,
                int> = 0>
  OptionalIndex Intern(MakerTypeFor<T> /*arg*/,
                       bool* absl_nullable is_new = nullptr) const {
    return OptionalIndex(InternInternal<likely_new>(is_new));
  }

  // Finds an existing `IndexInterned` matching the given argument, or returns
  // null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the object already exists, such as looking up in a map
  // with interned keys.
  template <
      typename Arg,
      std::enable_if_t<SupportedByHashAndEq<Arg, T, Hash, Eq>::value, int> = 0>
  OptionalIndex Find(const Arg& arg) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInterned::Interner::Find(): "
           "interner is archived in-place";
    const size_t hash = Hash()(arg);
    return OptionalIndex(GetShard(hash).Find(arg, hash));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentT = T,
            std::enable_if_t<SupportedByHashAndEq<absl::string_view, DependentT,
                                                  Hash, Eq>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalIndex Find(const char* arg) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInterned::Interner::Find(): "
           "interner is archived in-place";
    const absl::string_view string_view_arg(arg);
    const size_t hash = Hash()(string_view_arg);
    return OptionalIndex(GetShard(hash).Find(string_view_arg, hash));
  }

  // Returns the `IndexInterned` referring to the same object as `value`.
  //
  // `value` must have been interned in this interner.
  Index IndexOf(Resolved value) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInterned::Interner::IndexOf(): "
           "interner is archived in-place";
    const size_t hash = Hash()(*value);
    return Index(GetShard(hash).IndexOf(value, hash));
  }

  // Returns the number of objects managed by the interner.
  size_t NumObjects() const { return size(); }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const IndexInterner* self,
                                        MemoryEstimator& memory_estimator) {
    {
      ReaderMutexLock<ArenaMutex> arena_lock(self->arena_mutex_);
      memory_estimator.RegisterSubobjects(&self->directory_);
    }
    memory_estimator.RegisterSubobjects(&self->shards_);
  }

  // Shrinks capacity of internal data structures to fit their current sizes.
  void ShrinkToFit() {
    MutexLock<ArenaMutex> arena_lock(arena_mutex_);
    directory_.ShrinkToFit();
  }

  // Extracts the storage of the objects as an `Archive`. The `Interner` is left
  // empty.
  //
  // See `IndexInterned::Archive` for details.
  Archive ExtractArchive() && {
    for (Shard& shard : shards_) {
      shard.Archive();
    }
    return Archive(std::move(directory_).ExtractArchive());
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
    MutexLock<ArenaMutex> arena_lock(arena_mutex_);
    directory_.ShrinkToFit();
  }

 private:
  static constexpr bool kConcurrent = !std::is_same_v<Mutex, NullMutex>;

  using ArenaMutex = std::conditional_t<kConcurrent, absl::Mutex, NullMutex>;
  using Directory = Directory<T, kConcurrent, block_size>;
  using Shard =
      IndexInternerShard<Numeric, T, Hash, Eq, Mutex, ArenaMutex, block_size>;

  template <bool likely_new, typename Arg>
  Numeric InternInternal(Arg&& arg, bool* absl_nullable is_new) const {
    RIEGELI_ASSERT(!is_archived_in_place_)
        << "Failed precondition of IndexInterned::Interner::Intern(): "
           "interner is archived in-place";
    const size_t hash = Hash()(arg);
    bool is_new_internal;
    Numeric result;
    if constexpr (likely_new) {
      result = GetShard(hash).template InternNew</*verified_new=*/false>(
          std::forward<Arg>(arg), hash, directory_, arena_mutex_,
          is_new_internal);
    } else {
      result = GetShard(hash).Intern(std::forward<Arg>(arg), hash, directory_,
                                     arena_mutex_, is_new_internal);
    }
    if (is_new != nullptr) *is_new = is_new_internal;
    return result;
  }

  template <bool likely_new = false>
  Numeric InternInternal(bool* absl_nullable is_new = nullptr) const {
    if constexpr (std::is_copy_constructible_v<T>) {
      return InternInternal<likely_new>(riegeli::Global<T>(), is_new);
    } else {
      return InternInternal<likely_new>(T(), is_new);
    }
  }

  template <size_t... indices>
  std::array<Shard, num_shards> MakeShards(std::index_sequence<indices...>) {
    return {((void)indices, Shard(&directory_))...};
  }

  Shard& GetShard(size_t hash) const {
    return shards_[ShardIndex<num_shards>(hash)];
  }

  void ResetShards() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    for (Shard& shard : shards_) {
      shard.Reset();
    }
    is_archived_in_place_ = false;
  }

  mutable ArenaMutex arena_mutex_;
  mutable Directory directory_;
  mutable std::array<Shard, num_shards> shards_{
      MakeShards(std::make_index_sequence<num_shards>())};
  bool is_archived_in_place_ = false;
};

// Implementation details follow.

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename Tag>
struct OptionalIndexInterned<Numeric, T, Hash, Eq, Tag>::absl_container_hash {
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

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename Tag>
struct OptionalIndexInterned<Numeric, T, Hash, Eq, Tag>::absl_container_eq {
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

#endif  // RIEGELI_INTERNED_INDEX_INTERNED_OBJECT_H_
