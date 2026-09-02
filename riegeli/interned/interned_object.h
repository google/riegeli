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

#ifndef RIEGELI_INTERNED_INTERNED_OBJECT_H_
#define RIEGELI_INTERNED_INTERNED_OBJECT_H_

#include <stddef.h>

#include <array>
#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/container/hash_container_defaults.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/global.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/intrusive_shared_ptr.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/shared_ptr.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/interned_internal.h"
#include "riegeli/interned/interned_object_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Default template parameter `num_shards` for `Interned`.
// Also, a default template parameter for `Concurrent` nested types.
using interned_internal::kDefaultInternerNumShards;

template <typename T, typename Hash, typename Eq, typename Interner>
class Interned;

namespace interned_internal {

template <typename T, typename Hash, typename Eq, typename Tag, typename Mutex,
          size_t num_shards>
class GlobalInterner;

template <typename T, typename Hash, typename Eq, typename Tag, typename Mutex,
          size_t num_shards>
class LocalInterner;

// The public name of `OptionalInterned<T>` is `Interned<T>::Optional`.
//
// `Interned<T>` refers to an object of type `T`, ensuring that equal objects
// are shared to minimize memory usage.
//
// In contrast to `Interned`, `Interned::Optional` can be null. It is more
// efficient than `std::optional<Interned>`.
//
// See `Interned` for details.
template <typename T, typename Hash, typename Eq, typename InternerParam>
class OptionalInterned
    : public WithCompare<OptionalInterned<T, Hash, Eq, InternerParam>,
                         std::nullptr_t> {
 public:
  // Navigates between `Interned` and `Interned::Optional`.
  using NotOptional = Interned<T, Hash, Eq, InternerParam>;
  using Optional = OptionalInterned;

  // The interner type. See `Interned::Interner` for details.
  using Interner = InternerParam;

  // Creates a null `Interned::Optional`.
  //
  // This differs from the default constructor of `Interned`.
  OptionalInterned() = default;
  /*implicit*/ OptionalInterned(std::nullptr_t) {}
  OptionalInterned& operator=(std::nullptr_t) {
    shared_repr_ = nullptr;
    return *this;
  }

  // Creates an `Interned::Optional` referring to the constructed object,
  // or sharing an existing object if an equal object already exists.
  //
  // Most constructors of `Interned::Optional` are available when the `Interner`
  // is global. For a local interner, `Interner::Intern()` must be used instead.
  //
  // Most constructors of `Interned::Optional` correspond to those of
  // `Interned`, which are preferred. Optimized overloads apply to both classes
  // and are presented here.

  // This constructor handles the general case. Specific argument types are
  // optimized by separate overloads below.
  template <typename DependentInterner = Interner,
            std::enable_if_t<std::is_empty_v<DependentInterner>, int> = 0>
  explicit OptionalInterned(Initializer<T> arg)
      : shared_repr_(Interner::InternInternal(std::move(arg).Reference())) {}

  // Constructor from a brace-enclosed initializer list, with the type
  // defaulting to `T` if it is not specified.
  template <
      typename Arg = T, typename DependentInterner = Interner,
      std::enable_if_t<std::conjunction_v<std::is_empty<DependentInterner>,
                                          std::is_same<std::decay_t<Arg>, T>>,
                       int> = 0>
  explicit OptionalInterned(Arg&& arg)
      : shared_repr_(Interner::InternInternal(std::forward<Arg>(arg))) {}

  // Optimized constructor for heterogeneous lookup. The argument is implicitly
  // convertible to `T` and is supported by `Hash` and `Eq`.
  template <typename Arg, typename DependentInterner = Interner,
            std::enable_if_t<
                std::conjunction_v<
                    std::is_empty<DependentInterner>, NotSameRef<Optional, Arg>,
                    NotSameRef<std::nullptr_t, Arg>, NotSameRef<T, Arg>,
                    std::is_convertible<Arg&&, T>,
                    SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  explicit OptionalInterned(Arg&& arg)
      : shared_repr_(Interner::InternInternal(std::forward<Arg>(arg))) {}

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      typename DependentT = T, typename DependentInterner = Interner,
      std::enable_if_t<
          std::conjunction_v<
              std::is_empty<DependentInterner>,
              std::is_convertible<const char*, DependentT>,
              std::is_constructible<DependentT, absl::string_view>,
              SupportedByHashAndEq<absl::string_view, DependentT, Hash, Eq>>,
          int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE explicit OptionalInterned(const char* arg)
      : shared_repr_(Interner::InternInternal(absl::string_view(arg))) {}

  // Optimized constructor for heterogeneous lookup. The argument is
  // `riegeli::Maker(arg)` or `riegeli::MakerFor<T>(arg)`, with `arg` being
  // explicitly convertible to `T` and supported by `Hash` and `Eq`.
  template <typename Arg, typename DependentInterner = Interner,
            std::enable_if_t<
                std::conjunction_v<std::is_empty<DependentInterner>,
                                   std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  explicit OptionalInterned(MakerType<Arg> arg)
      : shared_repr_(
            Interner::InternInternal(std::move(arg).template arg<0>())) {}
  template <typename Arg, typename DependentInterner = Interner,
            std::enable_if_t<
                std::conjunction_v<std::is_empty<DependentInterner>,
                                   std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  explicit OptionalInterned(MakerTypeFor<T, Arg> arg)
      : shared_repr_(
            Interner::InternInternal(std::move(arg).template arg<0>())) {}

  // Optimized constructor for a default-constructed object. The argument
  // is `riegeli::Maker()` or `riegeli::Maker<T>()`. The object is immortal,
  // and adjusting its reference count is optimized.
  //
  // This is equivalent to a default-constructed `Interned`, which is preferred.
  // That differs from the default-constructed `Interned::Optional`.
  template <typename DependentT = T, typename DependentInterner = Interner,
            std::enable_if_t<
                std::conjunction_v<std::is_empty<DependentInterner>,
                                   std::is_default_constructible<DependentT>>,
                int> = 0>
  explicit OptionalInterned(MakerType<> /*arg*/)
      : shared_repr_(Interner::InternInternal()) {}
  template <typename DependentT = T, typename DependentInterner = Interner,
            std::enable_if_t<
                std::conjunction_v<std::is_empty<DependentInterner>,
                                   std::is_default_constructible<DependentT>>,
                int> = 0>
  explicit OptionalInterned(MakerTypeFor<T> /*arg*/)
      : shared_repr_(Interner::InternInternal()) {}

  OptionalInterned(const OptionalInterned& that) = default;
  OptionalInterned& operator=(const OptionalInterned& that) = default;

  // A moved-from `Interned::Optional` is left null.
  OptionalInterned(OptionalInterned&& that) = default;
  OptionalInterned& operator=(OptionalInterned&& that) = default;

  // Returns an immortal `Interned` with a specific value. This function is
  // available when the `Interner` is global.
  //
  // This avoids finding the object each time, and adjusting its reference count
  // is optimized.
  //
  // The `construct` callable should be a lambda with no captures, returning
  // an argument for some `Interned` constructor.
  template <typename Construct,
            std::enable_if_t<std::conjunction_v<std::is_empty<Interner>,
                                                std::is_empty<Construct>,
                                                std::is_invocable<Construct>>,
                             int> = 0>
  static const NotOptional& Immortal(Construct construct) {
    return Interner::Immortal(construct);
  }

  // Returns `true` if not null.
  explicit operator bool() const { return repr() != nullptr; }

  // Converts from `Interned::Optional` to `Interned`.
  NotOptional not_optional() const& {
    RIEGELI_ASSERT(repr() != nullptr)
        << "Failed precondition of Interned::Optional::not_optional(): "
           "null pointer";
    return NotOptional(shared_repr_);
  }
  NotOptional not_optional() && {
    RIEGELI_ASSERT(repr() != nullptr)
        << "Failed precondition of Interned::Optional::not_optional(): "
           "null pointer";
    return NotOptional(std::move(shared_repr_));
  }
  NotOptional NotOptionalOrDie() const& {
    RIEGELI_CHECK(repr() != nullptr)
        << "Failed precondition of Interned::Optional::NotOptionalOrDie(): "
           "null pointer";
    return NotOptional(shared_repr_);
  }
  NotOptional NotOptionalOrDie() && {
    RIEGELI_CHECK(repr() != nullptr)
        << "Failed precondition of Interned::Optional::NotOptionalOrDie(): "
           "null pointer";
    return NotOptional(std::move(shared_repr_));
  }

  // Returns a pointer to the object, or `nullptr` if null.
  const T* absl_nullable get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    if (repr() == nullptr) return nullptr;
    return &repr()->value();
  }

  // Dereferences the pointer.
  const T& operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(repr() != nullptr)
        << "Failed precondition of Interned::Optional::operator*: null pointer";
    return repr()->value();
  }
  const T* operator->() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(repr() != nullptr)
        << "Failed precondition of Interned::Optional::operator->: "
           "null pointer";
    return &repr()->value();
  }

  // Dereferences the pointer, crashing the process if null.
  const T& value() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_CHECK(repr() != nullptr)
        << "Failed precondition of Interned::Optional::value(): null pointer";
    return repr()->value();
  }

  // Equality of non-null `Interned::Optional` objects corresponds to equality
  // of the objects they refer to, as specified by `Eq`, but is fast, comparing
  // the pointers.
  //
  // Other comparisons are also consistent with the objects, but only the case
  // of equal objects is optimized.
  //
  // All comparisons are valid only for `Interned::Optional` objects coming from
  // the same interner.

  friend bool operator==(const Optional& a, const Optional& b) {
    return a.repr() == b.repr();
  }
  template <
      typename DependentT = T,
      std::enable_if_t<
          std::disjunction_v<HasCompare<const DependentT&, const DependentT&>,
                             HasLessThan<const DependentT&, const DependentT&>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const Optional& a, const Optional& b) {
    if constexpr (HasCompare<const T&, const T&>::value) {
      using Ordering = decltype(riegeli::Compare(*a, *b));
      if (a.repr() == b.repr()) return Ordering(StrongOrdering::equal);
      if (a.repr() == nullptr) return Ordering(StrongOrdering::less);
      if (b.repr() == nullptr) return Ordering(StrongOrdering::greater);
      return riegeli::Compare(*a, *b);
    } else {
      if (a.repr() == b.repr()) return StrongOrdering::equal;
      if (a.repr() == nullptr) return StrongOrdering::less;
      if (b.repr() == nullptr) return StrongOrdering::greater;
      if (*a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }

  friend bool operator==(const Optional& a, std::nullptr_t) {
    return a.repr() == nullptr;
  }
  friend StrongOrdering RIEGELI_COMPARE(const Optional& a, std::nullptr_t) {
    if (a.repr() == nullptr) return StrongOrdering::equal;
    return StrongOrdering::greater;
  }

  template <
      typename Other,
      std::enable_if_t<std::conjunction_v<NotSameRef<Optional, Other>,
                                          std::negation<std::is_base_of<
                                              WithEqualMarker<Other>, Other>>,
                                          HasEqual<const T&, const Other&>>,
                       int> = 0>
  friend bool operator==(const Optional& a, const Other& b) {
    if (a.repr() == nullptr) return false;
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
  friend auto RIEGELI_COMPARE(const Optional& a, const Other& b) {
    if constexpr (HasCompare<const T&, const Other&>::value) {
      using Ordering = decltype(riegeli::Compare(*a, b));
      if (a.repr() == nullptr) return Ordering(StrongOrdering::less);
      return riegeli::Compare(*a, b);
    } else {
      if (a.repr() == nullptr) return StrongOrdering::less;
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
  friend bool operator==(const Other& a, const Optional& b) {
    if (b.repr() == nullptr) return false;
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
  friend auto RIEGELI_COMPARE(const Other& a, const Optional& b) {
    if constexpr (HasCompare<const Other&, const T&>::value) {
      using Ordering = decltype(riegeli::Compare(a, *b));
      if (b.repr() == nullptr) return Ordering(StrongOrdering::greater);
      return riegeli::Compare(a, *b);
    } else {
      if (b.repr() == nullptr) return StrongOrdering::greater;
      if (a == *b) return StrongOrdering::equal;
      if (a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }
#endif

  // `Interned::ByAddress` is implicitly convertible from `Interned` or
  // `Interned::Optional`, but instances are compared by address. This is more
  // efficient, but the order is arbitrary, consistent within the process.
  //
  // `std::less<ByAddress>` can be used as a comparator for algorithms over
  // `Interned` or `Interned::Optional`.
  class ByAddress;

  // Returns this object wrapped in `ByAddress`.
  ByAddress by_address() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return ByAddress(*this);
  }

  // Hashing `Interned` or `Interned::Optional` is fast, hashing the pointer.
  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, const Optional& self) {
    return HashState::combine(std::move(hash_state), self.repr());
  }

  // Default hash and equality for containers with `Interned` or
  // `Interned::Optional` as the key type, hashing and comparing by address,
  // supporting heterogeneous lookup against `NotOptional` and `Optional`.
  struct absl_container_hash;
  struct absl_container_eq;

  // Hash and equality for containers with `Interned` or `Interned::Optional`
  // as the key type, consistent with the underlying value, supporting
  // heterogeneous lookup. This is opt-in because heterogeneous hashing is more
  // expensive than pointer hashing.
  struct ValueHash;
  struct ValueEq;

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const Optional* self,
                                        MemoryEstimator& memory_estimator) {
    if (self->repr() == nullptr) return;
    memory_estimator.RegisterSubobjects(&self->shared_repr());
  }

  // Supports `riegeli::Debug()`.
  template <typename DebugStream>
  friend void RiegeliDebug(const Optional& src, DebugStream& dest) {
    if (src == nullptr) {
      dest.Debug(nullptr);
    } else {
      dest.Debug(*src);
    }
  }

  // Returns a snapshot of the current reference count.
  size_t GetRefCount() const { return shared_repr().GetRefCount(); }

 protected:
  using InternedRepr = InternedRepr<T, Hash, Eq, Interner>;
  using SharedRepr = IntrusiveSharedPtr<const InternedRepr>;

  explicit OptionalInterned(Initializer<T> arg, const Interner& interner)
      : shared_repr_(interner.InternInternal(std::move(arg).Reference())) {}

  template <typename Arg = T,
            std::enable_if_t<std::is_same_v<std::decay_t<Arg>, T>, int> = 0>
  explicit OptionalInterned(Arg&& arg, const Interner& interner)
      : shared_repr_(interner.InternInternal(std::forward<Arg>(arg))) {}

  template <typename Arg,
            std::enable_if_t<std::conjunction_v<
                                 NotSameRef<Optional, Arg>, NotSameRef<T, Arg>,
                                 std::is_convertible<Arg&&, T>,
                                 SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                             int> = 0>
  explicit OptionalInterned(Arg&& arg, const Interner& interner)
      : shared_repr_(interner.InternInternal(std::forward<Arg>(arg))) {}

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      typename DependentT = T,
      std::enable_if_t<
          std::conjunction_v<
              std::is_convertible<const char*, DependentT>,
              std::is_constructible<DependentT, absl::string_view>,
              SupportedByHashAndEq<absl::string_view, DependentT, Hash, Eq>>,
          int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE explicit OptionalInterned(
      const char* arg, const Interner& interner)
      : shared_repr_(interner.InternInternal(absl::string_view(arg))) {}

  template <typename Arg,
            std::enable_if_t<
                std::conjunction_v<std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  explicit OptionalInterned(MakerType<Arg> arg, const Interner& interner)
      : shared_repr_(
            interner.InternInternal(std::move(arg).template arg<0>())) {}

  template <typename Arg,
            std::enable_if_t<
                std::conjunction_v<std::is_constructible<T, Arg&&>,
                                   SupportedByHashAndEq<Arg, T, Hash, Eq>>,
                int> = 0>
  explicit OptionalInterned(MakerTypeFor<T, Arg> arg, const Interner& interner)
      : shared_repr_(
            interner.InternInternal(std::move(arg).template arg<0>())) {}

  template <
      typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  explicit OptionalInterned(MakerType<> /*arg*/, const Interner& interner)
      : shared_repr_(interner.InternInternal()) {}

  template <
      typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  explicit OptionalInterned(MakerTypeFor<T> /*arg*/, const Interner& interner)
      : shared_repr_(interner.InternInternal()) {}

  explicit OptionalInterned(absl_nullable SharedRepr repr)
      : shared_repr_(std::move(repr)) {}

  const SharedRepr& shared_repr() const {
    RIEGELI_ASSERT(shared_repr_ != nullptr)
        << "Failed precondition of Interned::Optional::shared_repr(): "
           "null pointer";
    return shared_repr_;
  }
  const InternedRepr* absl_nullable repr() const { return shared_repr_.get(); }

 private:
  friend NotOptional;  // For `repr()`.
  friend Interner;     // For `Optional(SharedRepr)`.

  absl_nullable SharedRepr shared_repr_;
};

}  // namespace interned_internal

// `Interned<T>` refers to an object of type `T`, ensuring that equal objects
// are shared to minimize memory usage.
//
// `Interned` is never null, except when moved-from, in which case most
// operations are undefined. See `Interned::Optional` for a variant that can
// be null. `Interned` is generally preferred over `Interned::Optional`.
//
// See `InternedString` for a variant optimized for strings.
//
// `Interned` objects are created by an interner, which maintains a set of
// objects to share. An interner can be global (represented by a stateless type)
// or local (managed explicitly). The default is global.
//
// Interned objects are destroyed and erased from the interner when all
// references to them are dropped. See `ArenaInterned` for a variant that is
// faster but does not delete objects until the interner is destroyed.
//
// Asymptotic memory usage per interned object:
//   global interner: heap(sizeof(T) + 8) + 14.8
//   local interner: heap(sizeof(T) + 16) + 14.8
//
// Breakdown:
//  + entry in `absl::flat_hash_set<T*>`: 8 / (7 * ln(2)) * (8 + 1)
//  + heap-allocated {
//    + object: sizeof(T)
//    + reference count: 8
//    + interner: 8 if local
//  }
//
// Interned handle: 8
//
// Among the template parameters, only `T` and optionally `Hash` and `Eq` should
// be specified explicitly. Other parameters should be specified by using
// `LocallyInterned` or by nested types `WithTag` and `Concurrent`.
//
// `Interned` derives from `Interned::Optional`. See `Interned::Optional` for
// inherited operations.
template <typename T, typename Hash = absl::DefaultHashContainerHash<T>,
          typename Eq = absl::DefaultHashContainerEq<T>,
          typename InternerParam = interned_internal::GlobalInterner<
              T, Hash, Eq, /*Tag=*/void, absl::Mutex,
              kDefaultInternerNumShards<absl::Mutex>>>
class Interned
    : public interned_internal::OptionalInterned<T, Hash, Eq, InternerParam>,
      public WithCompare<
          Interned<T, Hash, Eq, InternerParam>,
          interned_internal::OptionalInterned<T, Hash, Eq, InternerParam>,
          std::nullptr_t> {
 public:
  // Changes the tag type of the interner.
  //
  // Interned objects with distinct tags are managed by separate types of
  // interners, even if other template parameters are the same. This allows
  // annotating the type with its role for improved type safety. This forces
  // separation of interners, which can make lookups more efficient.
  template <typename NewTag>
  using WithTag =
      Interned<T, Hash, Eq, typename InternerParam::template WithTag<NewTag>>;

  // Tunes the interner for concurrency.
  //
  // By default, a global interner has multiple shards, while a local interner
  // has a single shard. With more shards, parallel usage is less likely to
  // cause contention.
  //
  // `Mutex` protects the set of object pointers in each shard.
  //
  // A mutex must support `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent = Interned<
      T, Hash, Eq,
      typename InternerParam::template Concurrent<NewMutex, new_num_shards>>;

  // Navigates between `Interned` and `Interned::Optional`.
  using NotOptional = typename Interned::NotOptional;
  using Optional = typename Interned::Optional;

  // The interner type. It is used for interning new objects, although a global
  // interner is usually accessed implicitly by the constructors of `Interned`.
  // The interner also provides statistics.
  using Interner = typename Interned::Interner;

  // A default-constructed `Interned` holds a default-constructed object.
  // The object is immortal, and adjusting its reference count is optimized.
  //
  // This constructor is available when the `Interner` is global.
  //
  // This differs from the default constructor of `Interned::Optional`.
  Interned() noexcept
#if __cpp_concepts
    requires std::is_empty_v<Interner> && std::is_default_constructible_v<T>
#endif
      : Optional(riegeli::Maker()) {
#if !__cpp_concepts
    static_assert(std::is_empty_v<Interner>);
    static_assert(std::is_default_constructible_v<T>);
#endif
  }

  // Constructor from `nullptr` is present in `Interned::Optional` but deleted
  // in `Interned`.
  Interned(std::nullptr_t) = delete;
  Interned& operator=(std::nullptr_t) = delete;

  // Most constructors of `Interned` are available when the `Interner` is
  // global. They are equivalent to calling `Interner::Intern()`. For a local
  // interner, `Interner::Intern()` must be used instead.

  // Constructor forwarding to `Interned::Optional`. See the constructors of
  // `Interned::Optional` for details about optimized overloads.
  template <typename Arg, typename DependentInterner = Interner,
            std::enable_if_t<
                std::conjunction_v<
                    std::is_empty<DependentInterner>, NotSameRef<Optional, Arg>,
                    NotSameRef<std::nullptr_t, Arg>, NotSameRef<T, Arg>,
                    std::is_convertible<Arg&&, Initializer<T>>>,
                int> = 0>
  explicit Interned(Arg&& arg) : Optional(std::forward<Arg>(arg)) {}

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      typename DependentT = T, typename DependentInterner = Interner,
      std::enable_if_t<std::conjunction_v<
                           std::is_empty<DependentInterner>,
                           std::is_convertible<const char*, DependentT>,
                           std::is_constructible<DependentT, absl::string_view>,
                           interned_internal::SupportedByHashAndEq<
                               absl::string_view, DependentT, Hash, Eq>>,
                       int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE explicit Interned(const char* arg)
      : Optional(arg) {}

  // Constructor from a brace-enclosed initializer list, with the type
  // defaulting to `T` if it is not specified.
  template <
      typename Arg = T, typename DependentInterner = Interner,
      std::enable_if_t<std::conjunction_v<std::is_empty<DependentInterner>,
                                          std::is_same<std::decay_t<Arg>, T>>,
                       int> = 0>
  explicit Interned(Arg&& arg) : Optional(std::forward<Arg>(arg)) {}

  Interned(const Interned& that) = default;
  Interned& operator=(const Interned& that) = default;

  // A moved-from `Interned` does not contain an object. Most operations are
  // undefined, except for assignment and `valueless_after_move()`.
  Interned(Interned&& that) = default;
  Interned& operator=(Interned&& that) = default;

  // Returns `true` because `Interned` is never null, except when moved-from.
  explicit operator bool() const {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from Interned";
    return true;
  }

  // Returns `true` when the `Interned` is null because it was moved-from.
  bool valueless_after_move() const { return this->repr() == nullptr; }

  // Returns a pointer to the object.
  const T* get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from Interned";
    return &this->repr()->value();
  }

  // Dereferences the pointer.
  const T& value() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_CHECK(this->repr() != nullptr) << "Moved-from Interned";
    return this->repr()->value();
  }

  // Equality of `Interned` objects corresponds to equality of the objects they
  // refer to, as specified by `Eq`, but is fast, comparing the pointers.
  //
  // Other comparisons are also consistent with the objects, but only the case
  // of equal objects is optimized.
  //
  // All comparisons are valid only for `Interned` objects coming from the same
  // interner.

  friend bool operator==(const NotOptional& a, const NotOptional& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    return a.repr() == b.repr();
  }
  template <
      typename DependentT = T,
      std::enable_if_t<
          std::disjunction_v<HasCompare<const DependentT&, const DependentT&>,
                             HasLessThan<const DependentT&, const DependentT&>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const NotOptional& a, const NotOptional& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    if constexpr (HasCompare<const T&, const T&>::value) {
      using Ordering = decltype(riegeli::Compare(*a, *b));
      if (a.repr() == b.repr()) return Ordering(StrongOrdering::equal);
      return riegeli::Compare(*a, *b);
    } else {
      if (a.repr() == b.repr()) return StrongOrdering::equal;
      if (*a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }

  friend bool operator==(const NotOptional& a, const Optional& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    return a.repr() == b.repr();
  }
  template <
      typename DependentT = T,
      std::enable_if_t<
          std::disjunction_v<HasCompare<const DependentT&, const DependentT&>,
                             HasLessThan<const DependentT&, const DependentT&>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const NotOptional& a, const Optional& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    if constexpr (HasCompare<const T&, const T&>::value) {
      using Ordering = decltype(riegeli::Compare(*a, *b));
      if (a.repr() == b.repr()) return Ordering(StrongOrdering::equal);
      if (b.repr() == nullptr) return Ordering(StrongOrdering::greater);
      return riegeli::Compare(*a, *b);
    } else {
      if (a.repr() == b.repr()) return StrongOrdering::equal;
      if (b.repr() == nullptr) return StrongOrdering::greater;
      if (*a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }

  friend bool operator==(const NotOptional& a, std::nullptr_t) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    return false;
  }
  friend auto RIEGELI_COMPARE(const NotOptional& a, std::nullptr_t) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    return StrongOrdering::greater;
  }

  template <
      typename Other,
      std::enable_if_t<std::conjunction_v<NotSameRef<Optional, Other>,
                                          std::negation<std::is_base_of<
                                              WithEqualMarker<Other>, Other>>,
                                          HasEqual<const T&, const Other&>>,
                       int> = 0>
  friend bool operator==(const NotOptional& a, const Other& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
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
  friend auto RIEGELI_COMPARE(const NotOptional& a, const Other& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
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
  friend bool operator==(const Other& a, const NotOptional& b) {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
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
  friend auto RIEGELI_COMPARE(const Other& a, const NotOptional& b) {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    if constexpr (HasCompare<const Other&, const T&>::value) {
      return riegeli::Compare(a, *b);
    } else {
      if (a == *b) return StrongOrdering::equal;
      if (a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }
#endif

  // Indicates support for:
  //  * `ExternalRef(const Interned&, substr)`
  //  * `ExternalRef(Interned&&, substr)`
  friend void RiegeliSupportsExternalRef(const NotOptional* absl_nullable) {}

  // Supports `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(NotOptional* self) {
    return RiegeliToExternalStorage(&self->shared_repr_);
  }

 private:
  friend Optional;  // For `Optional(SharedRepr)`.
  friend Interner;  // For `Interned(..., const Interner&)`.

  explicit Interned(typename Interned::SharedRepr repr)
      : Optional(std::move(repr)) {}

  template <typename Arg>
  explicit Interned(Arg&& arg, const Interner& interner)
      : Optional(std::forward<Arg>(arg), interner) {}
};

template <typename T,
          std::enable_if_t<!std::is_same_v<T, std::nullptr_t>, int> = 0>
explicit Interned(T&& value) -> Interned<TargetT<T>>;

// `LocallyInterned<T>` is an instantiation of `Interned<T>` with a local
// interner. See `LocalInterner` for details.
template <typename T, typename Hash = absl::DefaultHashContainerHash<T>,
          typename Eq = absl::DefaultHashContainerEq<T>>
using LocallyInterned =
    Interned<T, Hash, Eq,
             interned_internal::LocalInterner<T, Hash, Eq, /*Tag=*/void,
                                              absl::Mutex, /*num_shards=*/1>>;

namespace interned_internal {

// The public name of `GlobalInterner<T>` is `Interned<T>::Interner`.
//
// `Interned<T>::Interner` represents a global interner for the given `T` and
// other template parameters. See `LocallyInterned::Interner` for a non-global
// version.
template <typename T, typename Hash, typename Eq, typename Tag,
          typename MutexParam, size_t num_shards>
class GlobalInterner {
 public:
  // Changes the tag type of the interner. See `Interned::WithTag` for details.
  template <typename NewTag>
  using WithTag = GlobalInterner<T, Hash, Eq, NewTag, MutexParam, num_shards>;

  // Tunes the interner for concurrency. See `Interned::Concurrent` for details.
  //
  // By default, a global interner is tuned for concurrency and has multiple
  // shards.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent = GlobalInterner<T, Hash, Eq, Tag, NewMutex, new_num_shards>;

  // References to interned objects. See `Interned` and `Interned::Optional`
  // for details.
  using Interned = Interned<T, Hash, Eq, GlobalInterner>;
  using OptionalInterned = OptionalInterned<T, Hash, Eq, GlobalInterner>;

  // Since `Interned::Interner` is stateless, all instances are equivalent.
  // Member functions are static. Instantiation is provided for consistency
  // with other interner categories.
  GlobalInterner() = default;

  GlobalInterner(const GlobalInterner& that) = default;
  GlobalInterner& operator=(const GlobalInterner& that) = default;

  // `Intern()` is equivalent to constructors of `Interned`, which are
  // preferred. `Intern()` is provided for consistency with other interner
  // categories.

  template <
      typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  static const Interned& Intern() {
    return riegeli::Global([] { return Interned(InternInternal()); });
  }

  // Forwards to a constructor of `Interned::Optional`. See the constructors of
  // `Interned::Optional` for details about optimized overloads.
  template <
      typename Arg,
      std::enable_if_t<std::conjunction_v<
                           NotSameRef<OptionalInterned, Arg>,
                           NotSameRef<std::nullptr_t, Arg>, NotSameRef<T, Arg>,
                           std::is_convertible<Arg&&, Initializer<T>>>,
                       int> = 0>
  static Interned Intern(Arg&& arg) {
    return Interned(std::forward<Arg>(arg), GlobalInterner());
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      typename DependentT = T,
      std::enable_if_t<
          std::conjunction_v<
              std::is_convertible<const char*, DependentT>,
              std::is_constructible<DependentT, absl::string_view>,
              SupportedByHashAndEq<absl::string_view, DependentT, Hash, Eq>>,
          int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static Interned Intern(const char* arg) {
    return Interned(InternInternal(absl::string_view(arg)));
  }

  // Creates an object from a brace-enclosed initializer list, with the type
  // defaulting to `T` if it is not specified.
  template <typename Arg = T,
            std::enable_if_t<std::is_same_v<std::decay_t<Arg>, T>, int> = 0>
  static Interned Intern(Arg&& arg) {
    return Interned(std::forward<Arg>(arg), GlobalInterner());
  }

  // Finds an existing `Interned` matching the given argument, or returns
  // null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the object already exists, such as looking up in a map
  // with interned keys.
  template <
      typename Arg,
      std::enable_if_t<SupportedByHashAndEq<Arg, T, Hash, Eq>::value, int> = 0>
  static OptionalInterned Find(const Arg& arg) {
    const size_t hash = Hash()(arg);
    return OptionalInterned(GetShard(hash).Find(arg, hash));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentT = T,
            std::enable_if_t<SupportedByHashAndEq<absl::string_view, DependentT,
                                                  Hash, Eq>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static OptionalInterned Find(const char* arg) {
    const absl::string_view string_view_arg(arg);
    const size_t hash = Hash()(string_view_arg);
    return OptionalInterned(GetShard(hash).Find(string_view_arg, hash));
  }

  // `Immortal()` is equivalent to `Interned::Immortal()`, which is preferred.
  // `Immortal()` is provided for consistency with other interner categories.
  //
  // See `Interned::Immortal()` for details.
  template <typename Construct,
            std::enable_if_t<std::conjunction_v<std::is_empty<Construct>,
                                                std::is_invocable<Construct>>,
                             int> = 0>
  static const Interned& Immortal(Construct /*construct*/) {
    return riegeli::Global([] {
      Interned interned{Construct()()};
      interned.repr()->Immortalize();
      return interned;
    });
  }

  // Returns a snapshot of the number of objects managed by the interner.
  static size_t NumObjects() {
    size_t count = 0;
    for (const Shard& shard : GetShards()) {
      count += shard.NumObjects();
    }
    return count;
  }

  // Returns a snapshot of the total number of references to interned objects
  // managed by the interner.
  static size_t TotalNumReferences() {
    size_t count = 0;
    for (const Shard& shard : GetShards()) {
      count += shard.TotalNumReferences();
    }
    return count;
  }

  // Supports `MemoryEstimator`.
  //
  // Given a large number of shared objects, `MemoryEstimatorSimplified` is
  // less accurate but much more efficient than `MemoryEstimatorDefault`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const GlobalInterner* /*self*/,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&GetShards());
  }

 private:
  using Mutex = MutexParam;
  using InternedRepr = InternedRepr<T, Hash, Eq, GlobalInterner>;
  using SharedRepr = IntrusiveSharedPtr<const InternedRepr>;
  using Shard = Shard<InternedRepr, SharedRepr, Hash, Eq, GlobalInterner>;
  using ShardArray = std::array<Shard, num_shards>;

  friend OptionalInterned;  // For `InternInternal()`.
  friend Shard;             // For `Mutex` and `GetShard()`.

  template <typename Arg>
  static SharedRepr InternInternal(Arg&& arg) {
    const size_t hash = Hash()(arg);
    return GetShard(hash).Intern(std::forward<Arg>(arg), hash,
                                 GlobalInterner());
  }

  static const SharedRepr& InternInternal() {
    return riegeli::Global([] {
      SharedRepr repr = InternInternal(T());
      repr->Immortalize();
      return repr;
    });
  }

  static Shard& GetShard(size_t hash) {
    return GetShards()[ShardIndex<num_shards>(hash)];
  }

  static ShardArray& GetShards() {
    return riegeli::Global([] { return ShardArray(); });
  }
};

// The public name of `LocalInterner<T>` is `LocallyInterned<T>::Interner`.
//
// `LocallyInterned<T>::Interner` represents an explicitly managed interner.
// It is a shared pointer to a set of interned objects. It does not influence
// their lifetime: if objects are dropped, they are erased from the interner,
// and they can also outlive the interner.
//
// See `Interned::Interner` for a global version. A global interner is more
// convenient to use. A local interner is required if equality of a given family
// of interned objects is meaningful only within that family, and would be
// incorrect across families, e.g. when only some fields are compared because
// other fields are implicitly equivalent within the family. Efficiency depends
// on usage patterns.
template <typename T, typename Hash, typename Eq, typename Tag,
          typename MutexParam, size_t num_shards>
class LocalInterner {
 public:
  // Changes the tag type of the interner. See `Interned::WithTag` for details.
  template <typename NewTag>
  using WithTag = LocalInterner<T, Hash, Eq, NewTag, MutexParam, num_shards>;

  // Tunes the interner for concurrency. See `Interned::Concurrent` for details.
  //
  // By default, a local interner is not tuned for concurrency and has a single
  // shard, but it is still thread-safe.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent = LocalInterner<T, Hash, Eq, Tag, NewMutex, new_num_shards>;

  // References to interned objects. See `Interned` and `Interned::Optional`
  // for details.
  using Interned = Interned<T, Hash, Eq, LocalInterner>;
  using OptionalInterned = OptionalInterned<T, Hash, Eq, LocalInterner>;

  // Creates an empty interner.
  LocalInterner() = default;

  LocalInterner(const LocalInterner& that) = default;
  LocalInterner& operator=(const LocalInterner& that) = default;

  LocalInterner(LocalInterner&& that) = default;
  LocalInterner& operator=(LocalInterner&& that) = default;

  // Prepares the interner for the expected number of distinct objects.
  // This reduces reallocations.
  void Reserve(size_t capacity) {
    RIEGELI_ASSERT(shards_ != nullptr)
        << "Moved-from LocallyInterned::Interner";
    if (capacity == 0) return;
    const size_t capacity_per_shard = capacity / num_shards;
    if (capacity_per_shard > 0) {
      for (Shard& shard : *shards_) {
        shard.Reserve(capacity_per_shard);
      }
    }
  }

  // Creates an `Interned` referring to the constructed object, or sharing an
  // existing object if an equal object already exists.

  // Analogous to the constructors of `Interned::Optional`. See the constructors
  // of `Interned::Optional` for details about optimized overloads.

  template <
      typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  Interned Intern() const {
    return Interned(InternInternal());
  }

  template <
      typename Arg,
      std::enable_if_t<std::conjunction_v<
                           NotSameRef<OptionalInterned, Arg>,
                           NotSameRef<std::nullptr_t, Arg>, NotSameRef<T, Arg>,
                           std::is_convertible<Arg&&, Initializer<T>>>,
                       int> = 0>
  Interned Intern(Arg&& arg) const {
    return Interned(std::forward<Arg>(arg), *this);
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      typename DependentT = T,
      std::enable_if_t<
          std::conjunction_v<
              std::is_convertible<const char*, DependentT>,
              std::is_constructible<DependentT, absl::string_view>,
              SupportedByHashAndEq<absl::string_view, DependentT, Hash, Eq>>,
          int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE Interned Intern(const char* arg) const {
    return Interned(InternInternal(absl::string_view(arg)));
  }

  // Creates an object from a brace-enclosed initializer list, with the type
  // defaulting to `T` if it is not specified.
  template <typename Arg = T,
            std::enable_if_t<std::is_same_v<std::decay_t<Arg>, T>, int> = 0>
  Interned Intern(Arg&& arg) const {
    return Interned(std::forward<Arg>(arg), *this);
  }

  // Finds an existing `Interned` matching the given argument, or returns
  // null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the object already exists, such as looking up in a map
  // with interned keys.
  template <
      typename Arg,
      std::enable_if_t<SupportedByHashAndEq<Arg, T, Hash, Eq>::value, int> = 0>
  OptionalInterned Find(const Arg& arg) const {
    const size_t hash = Hash()(arg);
    return OptionalInterned(GetShard(hash).Find(arg, hash));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentT = T,
            std::enable_if_t<SupportedByHashAndEq<absl::string_view, DependentT,
                                                  Hash, Eq>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalInterned Find(const char* arg) const {
    const absl::string_view string_view_arg(arg);
    const size_t hash = Hash()(string_view_arg);
    return OptionalInterned(GetShard(hash).Find(string_view_arg, hash));
  }

  // Returns a snapshot of the number of objects managed by the interner.
  size_t NumObjects() const {
    RIEGELI_ASSERT(shards_ != nullptr)
        << "Moved-from LocallyInterned::Interner";
    size_t count = 0;
    for (const Shard& shard : *shards_) {
      count += shard.NumObjects();
    }
    return count;
  }

  // Returns a snapshot of the total number of references to interned objects
  // managed by the interner.
  size_t TotalNumReferences() const {
    RIEGELI_ASSERT(shards_ != nullptr)
        << "Moved-from LocallyInterned::Interner";
    size_t count = 0;
    for (const Shard& shard : *shards_) {
      count += shard.TotalNumReferences();
    }
    return count;
  }

  // Supports `MemoryEstimator`.
  //
  // Given a large number of shared objects, `MemoryEstimatorSimplified` is
  // less accurate but much more efficient than `MemoryEstimatorDefault`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const LocalInterner* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->shards_);
  }

 private:
  using Mutex = MutexParam;
  using InternedRepr = InternedRepr<T, Hash, Eq, LocalInterner>;
  using SharedRepr = IntrusiveSharedPtr<const InternedRepr>;
  using Shard = Shard<InternedRepr, SharedRepr, Hash, Eq, LocalInterner>;
  using ShardArray = std::array<Shard, num_shards>;

  friend OptionalInterned;  // For `InternInternal()`.
  friend Shard;             // For `Mutex` and `GetShard()`.

  template <typename Arg>
  SharedRepr InternInternal(Arg&& arg) const {
    const size_t hash = Hash()(arg);
    return GetShard(hash).Intern(std::forward<Arg>(arg), hash, *this);
  }

  SharedRepr InternInternal() const {
    if constexpr (std::is_copy_constructible_v<T>) {
      return InternInternal(riegeli::Global<T>());
    } else {
      return InternInternal(T());
    }
  }

  Shard& GetShard(size_t hash) const {
    RIEGELI_ASSERT(shards_ != nullptr)
        << "Moved-from LocallyInterned::Interner";
    return (*shards_)[ShardIndex<num_shards>(hash)];
  }

  SharedPtr<ShardArray> shards_{riegeli::Maker()};
};

// Implementation details follow.

template <typename T, typename Hash, typename Eq, typename Interner>
class OptionalInterned<T, Hash, Eq, Interner>::ByAddress
    : public WithCompare<ByAddress> {
 public:
  /*implicit*/ ByAddress(const Optional& interned) : repr_(interned.repr()) {}

  ByAddress(const ByAddress& that) = default;
  ByAddress& operator=(const ByAddress& that) = default;

  friend bool operator==(ByAddress a, ByAddress b) {
    return a.repr_ == b.repr_;
  }
  friend StrongOrdering RIEGELI_COMPARE(ByAddress a, ByAddress b) {
    return riegeli::Compare(a.repr_, b.repr_);
  }

 private:
  const InternedRepr* absl_nullable repr_;
};

template <typename T, typename Hash, typename Eq, typename Interner>
struct OptionalInterned<T, Hash, Eq, Interner>::absl_container_hash {
  using is_transparent = void;
  size_t operator()(const Optional& self) const {
    return absl::HashOf(self.repr());
  }
  size_t operator()(const NotOptional& self) const {
    RIEGELI_ASSERT(self.repr() != nullptr) << "Moved-from Interned";
    return absl::HashOf(self.repr());
  }
  size_t operator()(std::nullptr_t) const { return absl::HashOf(nullptr); }
};

template <typename T, typename Hash, typename Eq, typename Interner>
struct OptionalInterned<T, Hash, Eq, Interner>::absl_container_eq {
  using is_transparent = void;
  bool operator()(const Optional& a, const Optional& b) const {
    return a.repr() == b.repr();
  }
  bool operator()(const Optional& a, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    return a.repr() == b.repr();
  }
  bool operator()(const NotOptional& a, const Optional& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    return a.repr() == b.repr();
  }
  bool operator()(const NotOptional& a, const NotOptional& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    return a.repr() == b.repr();
  }
  bool operator()(const Optional& a, std::nullptr_t) const {
    return a.repr() == nullptr;
  }
  bool operator()(std::nullptr_t, const Optional& b) const {
    return b.repr() == nullptr;
  }
  bool operator()(const NotOptional& a, std::nullptr_t) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    return false;
  }
  bool operator()(std::nullptr_t, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    return false;
  }
};

template <typename T, typename Hash, typename Eq, typename Interner>
struct OptionalInterned<T, Hash, Eq, Interner>::ValueHash {
  using is_transparent = void;
  size_t operator()(const Optional& self) const {
    if (self.repr() == nullptr) {
      if constexpr (HasTransparentNullptrHash<Hash>::value) {
        return hash(nullptr);
      } else {
        return absl::HashOf(nullptr);
      }
    }
    return hash(*self);
  }
  size_t operator()(const NotOptional& self) const {
    RIEGELI_ASSERT(self.repr() != nullptr) << "Moved-from Interned";
    return hash(*self);
  }
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

template <typename T, typename Hash, typename Eq, typename Interner>
struct OptionalInterned<T, Hash, Eq, Interner>::ValueEq {
  using is_transparent = void;
  bool operator()(const Optional& a, const Optional& b) const {
    return a.repr() == b.repr();
  }
  bool operator()(const Optional& a, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    return a.repr() == b.repr();
  }
  bool operator()(const NotOptional& a, const Optional& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    return a.repr() == b.repr();
  }
  bool operator()(const NotOptional& a, const NotOptional& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    return a.repr() == b.repr();
  }
  bool operator()(const Optional& a, std::nullptr_t) const {
    return a.repr() == nullptr;
  }
  bool operator()(std::nullptr_t, const Optional& b) const {
    return b.repr() == nullptr;
  }
  bool operator()(const NotOptional& a, std::nullptr_t) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    return false;
  }
  bool operator()(std::nullptr_t, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    return false;
  }
  bool operator()(const Optional& a, const T& b) const {
    if (a.repr() == nullptr) return false;
    return eq(*a, b);
  }
  bool operator()(const T& a, const Optional& b) const {
    if (b.repr() == nullptr) return false;
    return eq(*b, a);
  }
  bool operator()(const NotOptional& a, const T& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
    return eq(*a, b);
  }
  bool operator()(const T& a, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
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
  bool operator()(const Optional& a, const PassedKey& b) const {
    if (a.repr() == nullptr) return false;
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
  bool operator()(const PassedKey& a, const Optional& b) const {
    if (b.repr() == nullptr) return false;
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
  bool operator()(const NotOptional& a, const PassedKey& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from Interned";
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
  bool operator()(const PassedKey& a, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from Interned";
    return eq(*b, a);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Eq eq;
};

}  // namespace interned_internal

}  // namespace riegeli

#endif  // RIEGELI_INTERNED_INTERNED_OBJECT_H_
