// Copyright 2024 Google LLC
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

#ifndef RIEGELI_BASE_EXTERNAL_REF_BASE_H_
#define RIEGELI_BASE_EXTERNAL_REF_BASE_H_

// IWYU pragma: private, include "riegeli/base/external_ref.h"

#include <stddef.h>

#include <functional>
#include <memory>
#include <new>  // IWYU pragma: keep
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain_base.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/external_ref_support.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/temporary_storage.h"

namespace riegeli {

// `ExternalRef` specifies a byte array in a way which allows sharing it with
// other objects without copying if that is considered more efficient than
// copying. It mediates between the producer and the consumer of the data during
// transfer; it is not suitable for longer storage. Creating an `ExternalRef` is
// usually more efficient than creating a `Chain` or `absl::Cord` if the data
// will ultimately be copied rather than shared.
//
// `ExternalRef` is constructed from an object of some type `T` which owns the
// data, or from `Initializer<T>`. The latter allows to skip constructing the
// object if the data are known beforehand, will ultimately be copied, and the
// constructed object is not needed otherwise.
//
// `ExternalRef` can be converted to `absl::string_view`, `Chain`, `absl::Cord`,
// or `ExternalData`, or assigned, appended, or prepended to a `Chain` or
// `absl::Cord`. Apart from conversion to `absl::string_view` it can be consumed
// at most once.
//
// In contrast to `Chain::Block` and `absl::MakeCordFromExternal()`,
// `ExternalRef` chooses between sharing the object and copying the data,
// depending on the size of the data, whether the object is wasteful, the method
// of consuming the data, and the state of the destination for appending or
// prepending.
//
// `ExternalRef` itself does not own the object description nor the data, and is
// efficiently movable. The state is stored in a storage object passed as a
// default argument to the original `ExternalRef` constructor.
//
// The expected interface of the object which owns the data is a superset of the
// interfaces expected by `Chain::Block` and `absl::MakeCordFromExternal()`.
//
// `ExternalRef` constructors require the external object type to indicate
// that it supports `ExternalRef` by providing one of the following functions
// (only their presence is checked, they are never called):
// ```
//   // Indicates support for `ExternalRef(T&&, substr)`.
//   //
//   // `substr` must be owned by the object if it gets created or moved, unless
//   // `RiegeliExternalCopy()` (see below) recognizes cases when it is not.
//   //
//   // If `T` is convertible to `BytesRef`, then also indicates support for
//   // `ExternalRef(T&&)`.
//   //
//   // The parameter can also have type `const T*`. This also indicates
//   // support for `ExternalRef(const T&, substr)` and possibly
//   // `ExternalRef(const T&)`, i.e. that `T` is copyable and copying it is
//   // more efficient than copying the data.
//   //
//   // If the `ExternalRef` is later converted to `absl::Cord` and
//   // `absl::MakeCordFromExternal()` gets used, then this avoids an allocation
//   // by taking advantage of the promise that `substr` will be owned also by
//   // the moved object (`absl::MakeCordFromExternal()` requires knowing the
//   // data before specifying the object to be moved).
//   friend void RiegeliSupportsExternalRef(T*) {}
//
//   // Indicates support for `ExternalRef(T&&)`, as long as `T` is convertible
//   // to `BytesRef`.
//   //
//   // The parameter can also have type `const T*`. This also indicates support
//   // for `ExternalRef(const T&)`, i.e. that `T` is copyable and copying it is
//   // more efficient than copying the data.
//   friend void RiegeliSupportsExternalRefWhole(T*) {}
// ```
//
// `ExternalRef::From()` are like `ExternalRef` constructors, but
// `RiegeliSupportsExternalRef()` or `RiegeliSupportsExternalRefWhole()` is not
// needed. The caller is responsible for using an appropriate type of the
// external object.
//
// `T` may also support the following member functions, either with or without
// the `substr` parameter, with the following definitions assumed by default:
// ```
//   // Called once before the destructor, except on a moved-from object.
//   // If only this function is needed, `T` can be a lambda.
//   //
//   // If this is present, the object will be created unconditionally, because
//   // calling this might be needed to delete resources which already exist.
//   //
//   // This can also be a const method. If this is not a const method and the
//   // object is passed by const or lvalue reference, this will be called on a
//   // mutable copy of the object.
//   void operator()(absl::string_view substr) && {}
//
//   // If this returns `true`, the data will be copied instead of wrapping the
//   // object. The data does not need to be stable while the object is moved.
//   // `RiegeliToChainBlock()`, `RiegeliToCord()`, `RiegeliToExternalData()`,
//   // `RiegeliToExternalStorage()`, nor `RiegeliExternalDelegate()` will not
//   // be called.
//   //
//   // Typically this indicates an object with short data stored inline.
//   friend bool RiegeliExternalCopy(const T* self) { return false; }
//
//   // Converts `*self` or its `substr` to `Chain::Block`, if this can be done
//   // more efficiently than with `Chain::Block` constructor. Can modify
//   // `*self`. `operator()` will no longer be called.
//   //
//   // The `self` parameter can also have type `const T*`. If it has type `T*`
//   // and the object is passed by const or lvalue reference, this will be
//   // called on a mutable copy of the object.
//   //
//   // If the `substr` parameter was given to `ExternalRef` constructor, the
//   // `substr` parameter is required here, otherwise it is optional.
//   friend Chain::Block RiegeliToChainBlock(T* self, absl::string_view substr);
//
//   // Converts `*self` or its `substr` to `absl::Cord`, if this can be done
//   // more efficiently than with `absl::MakeCordFromExternal()`. Can modify
//   // `*self`. `operator()` will no longer be called.
//   //
//   // The `self` parameter can also have type `const T*`. If it has type `T*`
//   // and the object is passed by const or lvalue reference, this will be
//   // called on a mutable copy of the object.
//   //
//   // If the `substr` parameter was given to `ExternalRef` constructor, the
//   // `substr` parameter is required here, otherwise it is optional.
//   friend absl::Cord RiegeliToCord(T* self, absl::string_view substr);
//
//   // Converts `*self` to `ExternalData`, if this can be done more efficiently
//   // than allocating the object on the heap, e.g. if the object fits in a
//   // pointer. Can modify `*self`. `operator()` will no longer be called.
//   //
//   // The `self` parameter can also have type `const T*`. If it has type `T*`
//   // and the object is passed by const or lvalue reference, this will be
//   // called on a mutable copy of the object.
//   //
//   // If the `substr` parameter was given to `ExternalRef` constructor, the
//   // `substr` parameter is required here, otherwise it is optional.
//   friend ExternalData RiegeliToExternalData(T* self,
//                                             absl::string_view substr);
//
//   // This can be defined instead of `RiegeliToExternalData()` with `substr`,
//   // which would return `ExternalData` with the same `substr`.
//   friend ExternalStorage RiegeliToExternalStorage(T* self);
//
//   // If defined, indicates a subobject to wrap instead of the whole object.
//   // It must call `std::forward<Callback>(delegate_to)(subobject)` or
//   // `std::forward<Callback>(delegate_to)(subobject, substr)`, preferably
//   // with `std::move(subobject)`. `delegate_to` must be called exactly once.
//   //
//   // Typically this indicates a smaller object which is sufficient to keep
//   // the data alive, or the active variant if the object stores one of
//   // multiple subobjects.
//   //
//   // `RiegeliToChainBlock()`, `RiegeliToCord()`, `RiegeliToExternalData()`,
//   // and `RiegeliToExternalStorage()`, if defined, are used in preference to
//   // this.
//   //
//   // The subobject will be processed like by `ExternalRef::From()`, including
//   // the possibility of further delegation, except that `Initializer` is not
//   // supported. The subobject must not be `*self`.
//   //
//   // The `self` parameter can also have type `const T*`. If it has type `T*`
//   // and the object is passed by const or lvalue reference, this will be
//   // called on a mutable copy of the object.
//   //
//   // The `substr` parameter is optional here. If absent here while the
//   // `substr` parameter was given to `ExternalRef` constructor, then it is
//   // propagated. If absent here while the `substr` parameter was not given
//   // to `ExternalRef` constructor, then `subobject` must be convertible to
//   // `BytesRef`. If present here, it can be used to specify the data if
//   // `subobject` is not convertible to `BytesRef`.
//   template <typename Callback>
//   friend void RiegeliExternalDelegate(T* self, absl::string_view substr,
//                                       Callback&& delegate_to);
//
//   // Shows internal structure in a human-readable way, for debugging.
//   //
//   // Used for conversion to `Chain`.
//   friend void RiegeliDumpStructure(const T* self, absl::string_view substr,
//                                    std::ostream& dest) {
//     dest << "[external] { }";
//   }
//
//   // Registers this object with `MemoryEstimator`.
//   //
//   // By default calls `memory_estimator.RegisterUnknownType<T>()` and
//   // as an approximation of memory usage of an unknown type, registers just
//   // the stored `substr` if unique.
//   //
//   // Used for conversion to `Chain`.
//   friend void RiegeliRegisterSubobjects(
//       const T* self, riegeli::MemoryEstimator& memory_estimator);
// ```
//
// The `substr` parameter of these member functions, if present, will get the
// `substr` parameter passed to `ExternalRef` constructor. Having `substr`
// available in these functions might avoid storing `substr` in the external
// object.
//
// If `MemoryEstimator` is supported by customizing
// `RiegeliRegisterSubobjects()`, it is used to determine whether the object
// is considered wasteful, to avoid embedding wasteful objects.
class ExternalRef {
 private:
  using UseStringViewFunction = void (*)(void* context, absl::string_view data);
  using UseChainBlockFunction = void (*)(void* context, Chain::Block data);
  using UseCordFunction = void (*)(void* context, absl::Cord data);
  using UseExternalDataFunction = void (*)(void* context, ExternalData data);

  template <typename T>
  static bool Wasteful(const T& object, size_t used) {
    return riegeli::Wasteful(riegeli::EstimateMemorySimplified(object), used);
  }

  template <typename T, typename Enable = void>
  struct HasCallOperatorSubstr : std::false_type {};

  template <typename T>
  struct HasCallOperatorSubstr<T, std::void_t<decltype(std::declval<T&&>()(
                                      std::declval<absl::string_view>()))>>
      : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasCallOperatorWhole : std::false_type {};

  template <typename T>
  struct HasCallOperatorWhole<T, std::void_t<decltype(std::declval<T&&>()())>>
      : std::true_type {};

  template <typename T>
  struct HasCallOperator
      : std::disjunction<HasCallOperatorSubstr<T>, HasCallOperatorWhole<T>> {};

  template <typename T,
            std::enable_if_t<HasCallOperatorSubstr<T>::value, int> = 0>
  static void CallOperatorWhole(T&& object) {
    const absl::string_view data = BytesRef(object);
    std::forward<T>(object)(data);
  }
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<std::negation<HasCallOperatorSubstr<T>>,
                                   HasCallOperatorWhole<T>>,
                int> = 0>
  static void CallOperatorWhole(T&& object) {
    std::forward<T>(object)();
  }
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<std::negation<HasCallOperator<T>>,
                             HasCallOperatorSubstr<absl::remove_cvref_t<T>>>,
          int> = 0>
  static void CallOperatorWhole(T&& object) {
    absl::remove_cvref_t<T> copy(object);
    const absl::string_view data = BytesRef(copy);
    std::move(copy)(data);
  }
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<HasCallOperator<T>>,
              std::negation<HasCallOperatorSubstr<absl::remove_cvref_t<T>>>,
              HasCallOperatorWhole<absl::remove_cvref_t<T>>>,
          int> = 0>
  static void CallOperatorWhole(T&& object) {
    (absl::remove_cvref_t<T>(object))();
  }
  template <typename T,
            std::enable_if_t<!HasCallOperator<absl::remove_cvref_t<T>>::value,
                             int> = 0>
  static void CallOperatorWhole(ABSL_ATTRIBUTE_UNUSED T&& object) {}

  template <typename T,
            std::enable_if_t<HasCallOperatorSubstr<T>::value, int> = 0>
  static void CallOperatorSubstr(T&& object, absl::string_view substr) {
    std::forward<T>(object)(substr);
  }
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<std::negation<HasCallOperatorSubstr<T>>,
                                   HasCallOperatorWhole<T>>,
                int> = 0>
  static void CallOperatorSubstr(
      T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {
    std::forward<T>(object)();
  }
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<std::negation<HasCallOperator<T>>,
                             HasCallOperatorSubstr<absl::remove_cvref_t<T>>>,
          int> = 0>
  static void CallOperatorSubstr(
      T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {
    absl::remove_cvref_t<T> copy(object);
    const absl::string_view data = BytesRef(copy);
    std::move(copy)(data);
  }
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<HasCallOperator<T>>,
              std::negation<HasCallOperatorSubstr<absl::remove_cvref_t<T>>>,
              HasCallOperatorWhole<absl::remove_cvref_t<T>>>,
          int> = 0>
  static void CallOperatorSubstr(
      T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {
    (absl::remove_cvref_t<T>(object))();
  }
  template <typename T,
            std::enable_if_t<!HasCallOperator<absl::remove_cvref_t<T>>::value,
                             int> = 0>
  static void CallOperatorSubstr(
      ABSL_ATTRIBUTE_UNUSED T&& object,
      ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {}

  template <typename T>
  static external_ref_internal::PointerTypeT<T> Pointer(T&& object) {
    return &object;
  }

#if RIEGELI_DEBUG
  template <typename T, std::enable_if_t<
                            std::is_convertible_v<const T&, BytesRef>, int> = 0>
  static void AssertSubstr(const T& object, absl::string_view substr) {
    if (!substr.empty()) {
      const BytesRef whole = object;
      RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), whole.data()))
          << "Failed precondition of ExternalRef: "
             "substring not contained in whole data";
      RIEGELI_ASSERT(std::less_equal<>()(substr.data() + substr.size(),
                                         whole.data() + whole.size()))
          << "Failed precondition of ExternalRef: "
             "substring not contained in whole data";
    }
  }
  template <
      typename T,
      std::enable_if_t<!std::is_convertible_v<const T&, BytesRef>, int> = 0>
#else
  template <typename T>
#endif
  static void AssertSubstr(ABSL_ATTRIBUTE_UNUSED const T& object,
                           ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {
  }

  template <typename T, typename Callback, typename Enable = void>
  struct HasRiegeliExternalDelegateWhole : std::false_type {};

  template <typename T, typename Callback>
  struct HasRiegeliExternalDelegateWhole<
      T, Callback,
      std::void_t<decltype(RiegeliExternalDelegate(
          std::declval<external_ref_internal::PointerTypeT<T>>(),
          std::declval<Callback>()))>> : std::true_type {};

  template <typename T, typename Callback, typename Enable = void>
  struct HasRiegeliExternalDelegateSubstr : std::false_type {};

  template <typename T, typename Callback>
  struct HasRiegeliExternalDelegateSubstr<
      T, Callback,
      std::void_t<decltype(RiegeliExternalDelegate(
          std::declval<external_ref_internal::PointerTypeT<T>>(),
          std::declval<absl::string_view>(), std::declval<Callback>()))>>
      : std::true_type {};

  template <typename T, typename Callback>
  struct HasExternalDelegateWhole
      : std::disjunction<HasRiegeliExternalDelegateWhole<T, Callback>,
                         HasRiegeliExternalDelegateSubstr<T, Callback>> {};

  template <typename T, typename Callback,
            std::enable_if_t<
                HasRiegeliExternalDelegateWhole<T, Callback>::value, int> = 0>
  static void ExternalDelegateWhole(T&& object, Callback&& delegate_to) {
    RiegeliExternalDelegate(ExternalRef::Pointer(std::forward<T>(object)),
                            std::forward<Callback>(delegate_to));
  }
  template <typename T, typename Callback,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<HasRiegeliExternalDelegateWhole<T, Callback>>,
                    HasRiegeliExternalDelegateSubstr<T, Callback>>,
                int> = 0>
  static void ExternalDelegateWhole(T&& object, Callback&& delegate_to) {
    const absl::string_view data = BytesRef(object);
    RiegeliExternalDelegate(ExternalRef::Pointer(std::forward<T>(object)), data,
                            std::forward<Callback>(delegate_to));
  }

  template <typename T, typename Callback,
            std::enable_if_t<
                HasRiegeliExternalDelegateWhole<T, Callback>::value, int> = 0>
  static void ExternalDelegateWhole(
      T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data,
      Callback&& delegate_to) {
    RiegeliExternalDelegate(ExternalRef::Pointer(std::forward<T>(object)),
                            std::forward<Callback>(delegate_to));
  }
  template <typename T, typename Callback,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<HasRiegeliExternalDelegateWhole<T, Callback>>,
                    HasRiegeliExternalDelegateSubstr<T, Callback>>,
                int> = 0>
  static void ExternalDelegateWhole(T&& object, absl::string_view data,
                                    Callback&& delegate_to) {
    RiegeliExternalDelegate(ExternalRef::Pointer(std::forward<T>(object)), data,
                            std::forward<Callback>(delegate_to));
  }

  template <typename T, typename Callback>
  struct HasExternalDelegateSubstr
      : std::disjunction<HasRiegeliExternalDelegateSubstr<T, Callback>,
                         HasRiegeliExternalDelegateWhole<T, Callback>> {};

  template <typename T, typename Callback,
            std::enable_if_t<
                HasRiegeliExternalDelegateSubstr<T, Callback>::value, int> = 0>
  static void ExternalDelegateSubstr(T&& object, absl::string_view substr,
                                     Callback&& delegate_to) {
    RiegeliExternalDelegate(ExternalRef::Pointer(std::forward<T>(object)),
                            substr, std::forward<Callback>(delegate_to));
  }
  template <
      typename T, typename Callback,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<HasRiegeliExternalDelegateSubstr<T, Callback>>,
              HasRiegeliExternalDelegateWhole<T, Callback>>,
          int> = 0>
  static void ExternalDelegateSubstr(
      T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view substr,
      Callback&& delegate_to) {
    RiegeliExternalDelegate(ExternalRef::Pointer(std::forward<T>(object)),
                            std::forward<Callback>(delegate_to));
  }

  template <typename T, typename Enable = void>
  struct HasRiegeliToChainBlockWhole : std::false_type {};

  template <typename T>
  struct HasRiegeliToChainBlockWhole<
      T, std::enable_if_t<std::is_convertible_v<
             decltype(RiegeliToChainBlock(
                 std::declval<external_ref_internal::PointerTypeT<T>>())),
             Chain::Block>>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasRiegeliToChainBlockSubstr : std::false_type {};

  template <typename T>
  struct HasRiegeliToChainBlockSubstr<
      T, std::enable_if_t<std::is_convertible_v<
             decltype(RiegeliToChainBlock(
                 std::declval<external_ref_internal::PointerTypeT<T>>(),
                 std::declval<absl::string_view>())),
             Chain::Block>>> : std::true_type {};

  template <typename T>
  struct HasToChainBlockWhole
      : std::disjunction<HasRiegeliToChainBlockWhole<T>,
                         HasRiegeliToChainBlockSubstr<T>> {};

  template <typename T,
            std::enable_if_t<HasRiegeliToChainBlockWhole<T>::value, int> = 0>
  static Chain::Block ToChainBlockWhole(T&& object) {
    return RiegeliToChainBlock(ExternalRef::Pointer(std::forward<T>(object)));
  }
  template <typename T,
            std::enable_if_t<std::conjunction_v<
                                 std::negation<HasRiegeliToChainBlockWhole<T>>,
                                 HasRiegeliToChainBlockSubstr<T>>,
                             int> = 0>
  static Chain::Block ToChainBlockWhole(T&& object) {
    const absl::string_view data = BytesRef(object);
    return RiegeliToChainBlock(ExternalRef::Pointer(std::forward<T>(object)),
                               data);
  }

  template <typename T,
            std::enable_if_t<HasRiegeliToChainBlockWhole<T>::value, int> = 0>
  static Chain::Block ToChainBlockWhole(
      T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) {
    return RiegeliToChainBlock(ExternalRef::Pointer(std::forward<T>(object)));
  }
  template <typename T,
            std::enable_if_t<std::conjunction_v<
                                 std::negation<HasRiegeliToChainBlockWhole<T>>,
                                 HasRiegeliToChainBlockSubstr<T>>,
                             int> = 0>
  static Chain::Block ToChainBlockWhole(T&& object, absl::string_view data) {
    return RiegeliToChainBlock(ExternalRef::Pointer(std::forward<T>(object)),
                               data);
  }

  template <typename T>
  using HasToChainBlockSubstr = HasRiegeliToChainBlockSubstr<T>;

  template <typename T,
            std::enable_if_t<HasRiegeliToChainBlockSubstr<T>::value, int> = 0>
  static Chain::Block ToChainBlockSubstr(T&& object, absl::string_view substr) {
    return RiegeliToChainBlock(ExternalRef::Pointer(std::forward<T>(object)),
                               substr);
  }

  template <typename T>
  class ConverterToChainBlockWhole {
   public:
    ConverterToChainBlockWhole(const ConverterToChainBlockWhole&) = delete;
    ConverterToChainBlockWhole& operator=(const ConverterToChainBlockWhole&) =
        delete;

    template <
        typename SubT,
        std::enable_if_t<std::is_convertible_v<const SubT&, BytesRef>, int> = 0>
    void operator()(SubT&& subobject) && {
      // The constructor processes the subobject.
      const absl::string_view data = BytesRef(subobject);
      ConverterToChainBlockWhole<SubT> converter(
          std::forward<SubT>(subobject), data, context_, use_string_view_,
          use_chain_block_);
    }

    template <typename SubT>
    void operator()(SubT&& subobject, absl::string_view substr) && {
      // The constructor processes the subobject.
      ConverterToChainBlockSubstr<SubT> converter(
          std::forward<SubT>(subobject), substr, context_, use_string_view_,
          use_chain_block_);
    }

   private:
    friend class ExternalRef;

    ABSL_ATTRIBUTE_ALWAYS_INLINE
    explicit ConverterToChainBlockWhole(T&& object, absl::string_view data,
                                        void* context,
                                        UseStringViewFunction use_string_view,
                                        UseChainBlockFunction use_chain_block)
        : context_(context),
          use_string_view_(use_string_view),
          use_chain_block_(use_chain_block) {
      if (RiegeliExternalCopy(&object) || Wasteful(object, data.size())) {
        use_string_view_(context_, data);
        ExternalRef::CallOperatorWhole(std::forward<T>(object));
        return;
      }
      std::move(*this).Callback(std::forward<T>(object), data);
    }

    template <
        typename DependentT = T,
        std::enable_if_t<HasToChainBlockWhole<DependentT>::value, int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object,
                                               absl::string_view data) && {
      use_chain_block_(context_, ExternalRef::ToChainBlockWhole(
                                     std::forward<T>(object), data));
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  std::conjunction_v<
                      std::negation<HasToChainBlockWhole<DependentT>>,
                      HasToChainBlockWhole<absl::remove_cvref_t<DependentT>>>,
                  int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(
        T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) && {
      use_chain_block_(context_, ExternalRef::ToChainBlockWhole(
                                     absl::remove_cvref_t<T>(object)));
    }
    template <typename DependentT = T,
              std::enable_if_t<std::conjunction_v<
                                   std::negation<HasToChainBlockWhole<
                                       absl::remove_cvref_t<DependentT>>>,
                                   HasExternalDelegateWhole<
                                       DependentT, ConverterToChainBlockWhole>>,
                               int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object,
                                               absl::string_view data) && {
      ExternalRef::ExternalDelegateWhole(std::forward<T>(object), data,
                                         std::move(*this));
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  std::conjunction_v<
                      std::negation<HasToChainBlockWhole<
                          absl::remove_cvref_t<DependentT>>>,
                      std::negation<HasExternalDelegateWhole<
                          DependentT, ConverterToChainBlockWhole>>,
                      HasExternalDelegateWhole<absl::remove_cvref_t<DependentT>,
                                               ConverterToChainBlockWhole>>,
                  int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(
        T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) && {
      ExternalRef::ExternalDelegateWhole(absl::remove_cvref_t<T>(object),
                                         std::move(*this));
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  std::conjunction_v<std::negation<HasToChainBlockWhole<
                                         absl::remove_cvref_t<DependentT>>>,
                                     std::negation<HasExternalDelegateWhole<
                                         absl::remove_cvref_t<DependentT>,
                                         ConverterToChainBlockWhole>>>,
                  int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(
        T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) && {
      use_chain_block_(context_, Chain::Block(std::forward<T>(object)));
    }

    void* context_;
    UseStringViewFunction use_string_view_;
    UseChainBlockFunction use_chain_block_;
  };

  template <typename T>
  class ConverterToChainBlockSubstr {
   public:
    ConverterToChainBlockSubstr(const ConverterToChainBlockSubstr&) = delete;
    ConverterToChainBlockSubstr& operator=(const ConverterToChainBlockSubstr&) =
        delete;

    template <typename SubT>
    void operator()(SubT&& subobject) && {
      std::move (*this)(std::forward<SubT>(subobject), substr_);
    }

    template <typename SubT>
    void operator()(SubT&& subobject, absl::string_view substr) && {
      RIEGELI_ASSERT_EQ(substr_.size(), substr.size())
          << "ExternalRef: size mismatch";
      // The constructor processes the subobject.
      ConverterToChainBlockSubstr<SubT> converter(
          std::forward<SubT>(subobject), substr, context_, use_string_view_,
          use_chain_block_);
    }

   private:
    friend class ExternalRef;

    ABSL_ATTRIBUTE_ALWAYS_INLINE
    explicit ConverterToChainBlockSubstr(T&& object, absl::string_view substr,
                                         void* context,
                                         UseStringViewFunction use_string_view,
                                         UseChainBlockFunction use_chain_block)
        : substr_(substr),
          context_(context),
          use_string_view_(use_string_view),
          use_chain_block_(use_chain_block) {
      AssertSubstr(object, substr_);
      if (RiegeliExternalCopy(&object) || Wasteful(object, substr_.size())) {
        use_string_view_(context_, substr_);
        ExternalRef::CallOperatorSubstr(std::forward<T>(object), substr_);
        return;
      }
      std::move(*this).Callback(std::forward<T>(object));
    }

    template <
        typename DependentT = T,
        std::enable_if_t<HasToChainBlockSubstr<DependentT>::value, int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      use_chain_block_(context_, ExternalRef::ToChainBlockSubstr(
                                     std::forward<T>(object), substr_));
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  std::conjunction_v<
                      std::negation<HasToChainBlockSubstr<DependentT>>,
                      HasToChainBlockSubstr<absl::remove_cvref_t<DependentT>>>,
                  int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      use_chain_block_(context_, ExternalRef::ToChainBlockSubstr(
                                     absl::remove_cvref_t<T>(object), substr_));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<std::negation<HasToChainBlockSubstr<
                                   absl::remove_cvref_t<DependentT>>>,
                               HasExternalDelegateSubstr<
                                   DependentT, ConverterToChainBlockSubstr>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      ExternalRef::ExternalDelegateSubstr(std::forward<T>(object), substr_,
                                          std::move(*this));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<
                    HasToChainBlockSubstr<absl::remove_cvref_t<DependentT>>>,
                std::negation<HasExternalDelegateSubstr<
                    DependentT, ConverterToChainBlockSubstr>>,
                HasExternalDelegateSubstr<absl::remove_cvref_t<DependentT>,
                                          ConverterToChainBlockSubstr>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      ExternalRef::ExternalDelegateSubstr(absl::remove_cvref_t<T>(object),
                                          substr_, std::move(*this));
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  std::conjunction_v<std::negation<HasToChainBlockSubstr<
                                         absl::remove_cvref_t<DependentT>>>,
                                     std::negation<HasExternalDelegateSubstr<
                                         absl::remove_cvref_t<DependentT>,
                                         ConverterToChainBlockSubstr>>>,
                  int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      use_chain_block_(context_,
                       Chain::Block(std::forward<T>(object), substr_));
    }

    absl::string_view substr_;
    void* context_;
    UseStringViewFunction use_string_view_;
    UseChainBlockFunction use_chain_block_;
  };

  template <typename T>
  class ObjectForCordWhole {
   public:
    explicit ObjectForCordWhole(Initializer<T> object)
        : ptr_(std::move(object)) {}

    ObjectForCordWhole(ObjectForCordWhole&& that) = default;
    ObjectForCordWhole& operator=(ObjectForCordWhole&& that) = default;

    void operator()(absl::string_view substr) && {
      ExternalRef::CallOperatorSubstr(std::move(*ptr_), substr);
    }

    T& operator*() { return *ptr_; }
    const T& operator*() const { return *ptr_; }

   private:
    // Wrapped in `std::unique_ptr` so that the data are stable.
    // `absl::MakeCordFromExternal()` requires the data to be known beforehand
    // and valid for the moved external object.
    std::unique_ptr<T> ptr_;
  };

  template <typename T>
  class ObjectForCordSubstr {
   public:
    explicit ObjectForCordSubstr(Initializer<T> object,
                                 absl::string_view substr)
        : object_(std::move(object)) {
      AssertSubstr(**this, substr);
    }

    ObjectForCordSubstr(ObjectForCordSubstr&& that) = default;
    ObjectForCordSubstr& operator=(ObjectForCordSubstr&& that) = default;

    void operator()(absl::string_view substr) && {
      ExternalRef::CallOperatorSubstr(std::move(object_), substr);
    }

    T& operator*() { return object_; }
    const T& operator*() const { return object_; }

   private:
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS T object_;
  };

  template <typename T, typename Enable = void>
  struct HasRiegeliToCordWhole : std::false_type {};

  template <typename T>
  struct HasRiegeliToCordWhole<
      T, std::enable_if_t<std::is_convertible_v<
             decltype(RiegeliToCord(
                 std::declval<external_ref_internal::PointerTypeT<T>>())),
             absl::Cord>>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasRiegeliToCordSubstr : std::false_type {};

  template <typename T>
  struct HasRiegeliToCordSubstr<
      T, std::enable_if_t<std::is_convertible_v<
             decltype(RiegeliToCord(
                 std::declval<external_ref_internal::PointerTypeT<T>>(),
                 std::declval<absl::string_view>())),
             absl::Cord>>> : std::true_type {};

  template <typename T>
  struct HasToCordWhole
      : std::disjunction<HasRiegeliToCordWhole<T>, HasRiegeliToCordSubstr<T>> {
  };

  template <typename T,
            std::enable_if_t<HasRiegeliToCordWhole<T>::value, int> = 0>
  static absl::Cord ToCordWhole(T&& object) {
    return RiegeliToCord(ExternalRef::Pointer(std::forward<T>(object)));
  }
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<std::negation<HasRiegeliToCordWhole<T>>,
                                   HasRiegeliToCordSubstr<T>>,
                int> = 0>
  static absl::Cord ToCordWhole(T&& object) {
    const absl::string_view data = BytesRef(object);
    return RiegeliToCord(ExternalRef::Pointer(std::forward<T>(object)), data);
  }

  template <typename T,
            std::enable_if_t<HasRiegeliToCordWhole<T>::value, int> = 0>
  static absl::Cord ToCordWhole(T&& object,
                                ABSL_ATTRIBUTE_UNUSED absl::string_view data) {
    return RiegeliToCord(ExternalRef::Pointer(std::forward<T>(object)));
  }
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<std::negation<HasRiegeliToCordWhole<T>>,
                                   HasRiegeliToCordSubstr<T>>,
                int> = 0>
  static absl::Cord ToCordWhole(T&& object, absl::string_view data) {
    return RiegeliToCord(ExternalRef::Pointer(std::forward<T>(object)), data);
  }

  template <typename T>
  using HasToCordSubstr = HasRiegeliToCordSubstr<T>;

  template <typename T,
            std::enable_if_t<HasRiegeliToCordSubstr<T>::value, int> = 0>
  static absl::Cord ToCordSubstr(T&& object, absl::string_view substr) {
    return RiegeliToCord(ExternalRef::Pointer(std::forward<T>(object)), substr);
  }

  template <typename T>
  class ConverterToCordWhole {
   public:
    ConverterToCordWhole(const ConverterToCordWhole&) = delete;
    ConverterToCordWhole& operator=(const ConverterToCordWhole&) = delete;

    template <
        typename SubT,
        std::enable_if_t<std::is_convertible_v<const SubT&, BytesRef>, int> = 0>
    void operator()(SubT&& subobject) && {
      // The constructor processes the subobject.
      const absl::string_view data = BytesRef(subobject);
      ConverterToCordWhole<SubT> converter(std::forward<SubT>(subobject), data,
                                           context_, use_string_view_,
                                           use_cord_);
    }

    template <typename SubT>
    void operator()(SubT&& subobject, absl::string_view substr) && {
      // The constructor processes the subobject.
      ConverterToCordSubstr<SubT> converter(std::forward<SubT>(subobject),
                                            substr, context_, use_string_view_,
                                            use_cord_);
    }

   private:
    friend class ExternalRef;

    ABSL_ATTRIBUTE_ALWAYS_INLINE
    explicit ConverterToCordWhole(T&& object, absl::string_view data,
                                  void* context,
                                  UseStringViewFunction use_string_view,
                                  UseCordFunction use_cord)
        : context_(context),
          use_string_view_(use_string_view),
          use_cord_(use_cord) {
      if (RiegeliExternalCopy(&object) || Wasteful(object, data.size())) {
        use_string_view_(context_, data);
        ExternalRef::CallOperatorWhole(std::forward<T>(object));
        return;
      }
      std::move(*this).Callback(std::forward<T>(object), data);
    }

    template <typename DependentT = T,
              std::enable_if_t<HasToCordWhole<DependentT>::value, int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object,
                                               absl::string_view data) && {
      use_cord_(context_,
                ExternalRef::ToCordWhole(std::forward<T>(object), data));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<std::conjunction_v<
                             std::negation<HasToCordWhole<DependentT>>,
                             HasToCordWhole<absl::remove_cvref_t<DependentT>>>,
                         int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(
        T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) && {
      use_cord_(context_, ExternalRef::ToCordWhole(absl::remove_cvref_t<T>(
                              std::forward<T>(object))));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<HasToCordWhole<absl::remove_cvref_t<DependentT>>>,
                HasExternalDelegateWhole<DependentT, ConverterToCordWhole>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object,
                                               absl::string_view data) && {
      ExternalRef::ExternalDelegateWhole(std::forward<T>(object), data,
                                         std::move(*this));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<HasToCordWhole<absl::remove_cvref_t<DependentT>>>,
                std::negation<
                    HasExternalDelegateWhole<DependentT, ConverterToCordWhole>>,
                HasExternalDelegateWhole<absl::remove_cvref_t<DependentT>,
                                         ConverterToCordWhole>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(
        T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) && {
      ExternalRef::ExternalDelegateWhole(absl::remove_cvref_t<T>(object),
                                         std::move(*this));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<HasToCordWhole<absl::remove_cvref_t<DependentT>>>,
                std::negation<HasExternalDelegateWhole<
                    absl::remove_cvref_t<DependentT>, ConverterToCordWhole>>,
                SupportsExternalRefSubstr<std::decay_t<DependentT>>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object,
                                               absl::string_view data) && {
      // If the type indicates that substrings are stable, then
      // `ObjectForCordSubstr` can be used instead of `ObjectForCordWhole`.
      use_cord_(context_, absl::MakeCordFromExternal(
                              data, ObjectForCordSubstr<std::decay_t<T>>(
                                        std::forward<T>(object), data)));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<HasToCordWhole<absl::remove_cvref_t<DependentT>>>,
                std::negation<HasExternalDelegateWhole<
                    absl::remove_cvref_t<DependentT>, ConverterToCordWhole>>,
                std::negation<
                    SupportsExternalRefSubstr<std::decay_t<DependentT>>>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(
        T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) && {
      ObjectForCordWhole<std::decay_t<T>> object_for_cord(
          std::forward<T>(object));
      const absl::string_view moved_data = BytesRef(*object_for_cord);
      use_cord_(context_, absl::MakeCordFromExternal(
                              moved_data, std::move(object_for_cord)));
    }

    void* context_;
    UseStringViewFunction use_string_view_;
    UseCordFunction use_cord_;
  };

  template <typename T>
  class ConverterToCordSubstr {
   public:
    ConverterToCordSubstr(const ConverterToCordSubstr&) = delete;
    ConverterToCordSubstr& operator=(const ConverterToCordSubstr&) = delete;

    template <typename SubT>
    void operator()(SubT&& subobject) && {
      std::move (*this)(std::forward<SubT>(subobject), substr_);
    }

    template <typename SubT>
    void operator()(SubT&& subobject, absl::string_view substr) && {
      RIEGELI_ASSERT_EQ(substr_.size(), substr.size())
          << "ExternalRef: size mismatch";
      // The constructor processes the subobject.
      ConverterToCordSubstr<SubT> converter(std::forward<SubT>(subobject),
                                            substr, context_, use_string_view_,
                                            use_cord_);
    }

   private:
    friend class ExternalRef;

    ABSL_ATTRIBUTE_ALWAYS_INLINE
    explicit ConverterToCordSubstr(T&& object, absl::string_view substr,
                                   void* context,
                                   UseStringViewFunction use_string_view,
                                   UseCordFunction use_cord)
        : substr_(substr),
          context_(context),
          use_string_view_(use_string_view),
          use_cord_(use_cord) {
      AssertSubstr(object, substr_);
      if (RiegeliExternalCopy(&object) || Wasteful(object, substr_.size())) {
        use_string_view_(context_, substr_);
        ExternalRef::CallOperatorSubstr(std::forward<T>(object), substr_);
        return;
      }
      std::move(*this).Callback(std::forward<T>(object));
    }

    template <typename DependentT = T,
              std::enable_if_t<HasToCordSubstr<DependentT>::value, int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      use_cord_(context_,
                ExternalRef::ToCordSubstr(std::forward<T>(object), substr_));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<std::conjunction_v<
                             std::negation<HasToCordSubstr<DependentT>>,
                             HasToCordSubstr<absl::remove_cvref_t<DependentT>>>,
                         int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      use_cord_(context_, ExternalRef::ToCordSubstr(
                              absl::remove_cvref_t<T>(object), substr_));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<
                    HasToCordSubstr<absl::remove_cvref_t<DependentT>>>,
                HasExternalDelegateSubstr<DependentT, ConverterToCordSubstr>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      ExternalRef::ExternalDelegateSubstr(std::forward<T>(object), substr_,
                                          std::move(*this));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<
                    HasToCordSubstr<absl::remove_cvref_t<DependentT>>>,
                std::negation<HasExternalDelegateSubstr<DependentT,
                                                        ConverterToCordSubstr>>,
                HasExternalDelegateSubstr<absl::remove_cvref_t<DependentT>,
                                          ConverterToCordSubstr>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      ExternalRef::ExternalDelegateSubstr(absl::remove_cvref_t<T>(object),
                                          substr_, std::move(*this));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<
                    HasToCordSubstr<absl::remove_cvref_t<DependentT>>>,
                std::negation<HasExternalDelegateSubstr<
                    absl::remove_cvref_t<DependentT>, ConverterToCordSubstr>>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      use_cord_(context_, absl::MakeCordFromExternal(
                              substr_, ObjectForCordSubstr<std::decay_t<T>>(
                                           std::forward<T>(object), substr_)));
    }

    absl::string_view substr_;
    void* context_;
    UseStringViewFunction use_string_view_;
    UseCordFunction use_cord_;
  };

  template <typename T, typename Enable = void>
  class ExternalObjectWhole {
   public:
    explicit ExternalObjectWhole(Initializer<T> object)
        : object_(std::move(object)) {}

    ~ExternalObjectWhole() {
      ExternalRef::CallOperatorWhole(std::move(object_));
    }

    T& operator*() { return object_; }
    const T& operator*() const { return object_; }

   private:
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS T object_;
  };

  template <typename T, typename Enable = void>
  class ExternalObjectSubstr;

  template <typename T>
  class ExternalObjectSubstr<
      T, std::enable_if_t<HasCallOperatorSubstr<T>::value>> {
   public:
    explicit ExternalObjectSubstr(Initializer<T> object,
                                  absl::string_view substr)
        : object_(std::move(object)), substr_(substr) {
      AssertSubstr(**this, substr);
    }

    ~ExternalObjectSubstr() { std::move(object_)(substr_); }

    T& operator*() { return object_; }
    const T& operator*() const { return object_; }

   private:
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS T object_;
    absl::string_view substr_;
  };

  template <typename T>
  class ExternalObjectSubstr<T,
                             std::enable_if_t<!HasCallOperatorSubstr<T>::value>>
      : public ExternalObjectWhole<T> {
   public:
    explicit ExternalObjectSubstr(Initializer<T> object,
                                  absl::string_view substr)
        : ExternalObjectSubstr::ExternalObjectWhole(std::move(object)) {
      AssertSubstr(**this, substr);
    }
  };

  template <typename T, typename Enable = void>
  struct HasRiegeliToExternalDataWhole : std::false_type {};

  template <typename T>
  struct HasRiegeliToExternalDataWhole<
      T, std::enable_if_t<std::is_convertible_v<
             decltype(RiegeliToExternalData(
                 std::declval<external_ref_internal::PointerTypeT<T>>())),
             ExternalData>>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasRiegeliToExternalDataSubstr : std::false_type {};

  template <typename T>
  struct HasRiegeliToExternalDataSubstr<
      T, std::enable_if_t<std::is_convertible_v<
             decltype(RiegeliToExternalData(
                 std::declval<external_ref_internal::PointerTypeT<T>>(),
                 std::declval<absl::string_view>())),
             ExternalData>>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasRiegeliToExternalStorage : std::false_type {};

  template <typename T>
  struct HasRiegeliToExternalStorage<
      T, std::enable_if_t<std::is_convertible_v<
             decltype(RiegeliToExternalStorage(
                 std::declval<external_ref_internal::PointerTypeT<T>>())),
             ExternalStorage>>> : std::true_type {};

  template <typename T>
  struct HasToExternalDataSubstr
      : std::disjunction<HasRiegeliToExternalDataSubstr<T>,
                         HasRiegeliToExternalStorage<T>> {};

  template <typename T,
            std::enable_if_t<HasRiegeliToExternalDataSubstr<T>::value, int> = 0>
  static ExternalData ToExternalDataSubstr(T&& object,
                                           absl::string_view substr) {
    return RiegeliToExternalData(ExternalRef::Pointer(std::forward<T>(object)),
                                 substr);
  }

  template <
      typename T,
      std::enable_if_t<
          std::disjunction_v<std::negation<HasRiegeliToExternalDataSubstr<T>>,
                             HasRiegeliToExternalStorage<T>>,
          int> = 0>
  static ExternalData ToExternalDataSubstr(T&& object,
                                           absl::string_view substr) {
    return ExternalData{
        RiegeliToExternalStorage(ExternalRef::Pointer(std::forward<T>(object))),
        substr};
  }

  template <typename T>
  struct HasToExternalDataWhole
      : std::disjunction<HasRiegeliToExternalDataWhole<T>,
                         HasToExternalDataSubstr<T>> {};

  template <typename T,
            std::enable_if_t<HasRiegeliToExternalDataWhole<T>::value, int> = 0>
  static ExternalData ToExternalDataWhole(T&& object) {
    return RiegeliToExternalData(ExternalRef::Pointer(std::forward<T>(object)));
  }
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<std::negation<HasRiegeliToExternalDataWhole<T>>,
                             HasToExternalDataSubstr<T>>,
          int> = 0>
  static ExternalData ToExternalDataWhole(T&& object) {
    const absl::string_view data = BytesRef(object);
    return ExternalRef::ToExternalDataSubstr(std::forward<T>(object), data);
  }

  template <typename T,
            std::enable_if_t<HasRiegeliToExternalDataWhole<T>::value, int> = 0>
  static ExternalData ToExternalDataWhole(
      T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) {
    return RiegeliToExternalData(ExternalRef::Pointer(std::forward<T>(object)));
  }
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<std::negation<HasRiegeliToExternalDataWhole<T>>,
                             HasToExternalDataSubstr<T>>,
          int> = 0>
  static ExternalData ToExternalDataWhole(T&& object, absl::string_view data) {
    return ExternalRef::ToExternalDataSubstr(std::forward<T>(object), data);
  }

  template <typename T>
  class ConverterToExternalDataWhole {
   public:
    ConverterToExternalDataWhole(const ConverterToExternalDataWhole&) = delete;
    ConverterToExternalDataWhole& operator=(
        const ConverterToExternalDataWhole&) = delete;

    template <
        typename SubT,
        std::enable_if_t<std::is_convertible_v<const SubT&, BytesRef>, int> = 0>
    void operator()(SubT&& subobject) && {
      // The constructor processes the subobject.
      const absl::string_view data = BytesRef(subobject);
      ConverterToExternalDataWhole<SubT> converter(
          std::forward<SubT>(subobject), data, context_, use_external_data_);
    }

    template <typename SubT>
    void operator()(SubT&& subobject, absl::string_view substr) && {
      // The constructor processes the subobject.
      ConverterToExternalDataSubstr<SubT> converter(
          std::forward<SubT>(subobject), substr, context_, use_external_data_);
    }

   private:
    friend class ExternalRef;

    ABSL_ATTRIBUTE_ALWAYS_INLINE
    explicit ConverterToExternalDataWhole(
        T&& object, absl::string_view data, void* context,
        UseExternalDataFunction use_external_data)
        : context_(context), use_external_data_(use_external_data) {
      if (RiegeliExternalCopy(&object) || Wasteful(object, data.size())) {
        use_external_data_(context_, ExternalDataCopy(data));
        ExternalRef::CallOperatorWhole(std::forward<T>(object));
        return;
      }
      std::move(*this).Callback(std::forward<T>(object), data);
    }

    template <
        typename DependentT = T,
        std::enable_if_t<HasToExternalDataWhole<DependentT>::value, int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object,
                                               absl::string_view data) && {
      use_external_data_(context_, ExternalRef::ToExternalDataWhole(
                                       std::forward<T>(object), data));
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  std::conjunction_v<
                      std::negation<HasToExternalDataWhole<DependentT>>,
                      HasToExternalDataWhole<absl::remove_cvref_t<DependentT>>>,
                  int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(
        T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) && {
      use_external_data_(context_, ExternalRef::ToExternalDataWhole(
                                       absl::remove_cvref_t<T>(object)));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<std::negation<HasToExternalDataWhole<
                                   absl::remove_cvref_t<DependentT>>>,
                               HasExternalDelegateWhole<
                                   DependentT, ConverterToExternalDataWhole>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object,
                                               absl::string_view data) && {
      ExternalRef::ExternalDelegateWhole(std::forward<T>(object), data,
                                         std::move(*this));
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  std::conjunction_v<
                      std::negation<HasToExternalDataWhole<
                          absl::remove_cvref_t<DependentT>>>,
                      std::negation<HasExternalDelegateWhole<
                          DependentT, ConverterToExternalDataWhole>>,
                      HasExternalDelegateWhole<absl::remove_cvref_t<DependentT>,
                                               ConverterToExternalDataWhole>>,
                  int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(
        T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) && {
      ExternalRef::ExternalDelegateWhole(absl::remove_cvref_t<T>(object),
                                         std::move(*this));
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  std::conjunction_v<std::negation<HasToExternalDataWhole<
                                         absl::remove_cvref_t<DependentT>>>,
                                     std::negation<HasExternalDelegateWhole<
                                         absl::remove_cvref_t<DependentT>,
                                         ConverterToExternalDataWhole>>>,
                  int> = 0>
    void Callback(T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view data) {
      auto* const storage =
          new ExternalObjectWhole<std::decay_t<T>>(std::forward<T>(object));
      const absl::string_view moved_data = BytesRef(**storage);
      use_external_data_(
          context_,
          ExternalData{
              ExternalStorage(
                  storage,
                  [](void* ptr) {
                    delete static_cast<ExternalObjectWhole<std::decay_t<T>>*>(
                        ptr);
                  }),
              moved_data});
    }

    void* context_;
    UseExternalDataFunction use_external_data_;
  };

  template <typename T>
  class ConverterToExternalDataSubstr {
   public:
    ConverterToExternalDataSubstr(const ConverterToExternalDataSubstr&) =
        delete;
    ConverterToExternalDataSubstr& operator=(
        const ConverterToExternalDataSubstr&) = delete;

    template <typename SubT>
    void operator()(SubT&& subobject) && {
      std::move (*this)(std::forward<SubT>(subobject), substr_);
    }

    template <typename SubT>
    void operator()(SubT&& subobject, absl::string_view substr) && {
      RIEGELI_ASSERT_EQ(substr_.size(), substr.size())
          << "ExternalRef: size mismatch";
      // The constructor processes the subobject.
      ConverterToExternalDataSubstr<SubT> converter(
          std::forward<SubT>(subobject), substr, context_, use_external_data_);
    }

   private:
    friend class ExternalRef;

    ABSL_ATTRIBUTE_ALWAYS_INLINE
    explicit ConverterToExternalDataSubstr(
        T&& object, absl::string_view substr, void* context,
        UseExternalDataFunction use_external_data)
        : substr_(substr),
          context_(context),
          use_external_data_(use_external_data) {
      AssertSubstr(object, substr_);
      if (RiegeliExternalCopy(&object) || Wasteful(object, substr_.size())) {
        use_external_data_(context_, ExternalDataCopy(substr_));
        ExternalRef::CallOperatorSubstr(std::forward<T>(object), substr_);
        return;
      }
      std::move(*this).Callback(std::forward<T>(object));
    }

    template <
        typename DependentT = T,
        std::enable_if_t<HasToExternalDataSubstr<DependentT>::value, int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      use_external_data_(context_, ExternalRef::ToExternalDataSubstr(
                                       std::forward<T>(object), substr_));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<HasToExternalDataSubstr<DependentT>>,
                HasToExternalDataSubstr<absl::remove_cvref_t<DependentT>>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      use_external_data_(context_,
                         ExternalRef::ToExternalDataSubstr(
                             absl::remove_cvref_t<T>(object), substr_));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<std::negation<HasToExternalDataSubstr<
                                   absl::remove_cvref_t<DependentT>>>,
                               HasExternalDelegateSubstr<
                                   DependentT, ConverterToExternalDataSubstr>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      ExternalRef::ExternalDelegateSubstr(std::forward<T>(object), substr_,
                                          std::move(*this));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<
                    HasToExternalDataSubstr<absl::remove_cvref_t<DependentT>>>,
                std::negation<HasExternalDelegateSubstr<
                    DependentT, ConverterToExternalDataSubstr>>,
                HasExternalDelegateSubstr<absl::remove_cvref_t<DependentT>,
                                          ConverterToExternalDataSubstr>>,
            int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      ExternalRef::ExternalDelegateSubstr(absl::remove_cvref_t<T>(object),
                                          substr_, std::move(*this));
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  std::conjunction_v<std::negation<HasToExternalDataSubstr<
                                         absl::remove_cvref_t<DependentT>>>,
                                     std::negation<HasExternalDelegateSubstr<
                                         absl::remove_cvref_t<DependentT>,
                                         ConverterToExternalDataSubstr>>>,
                  int> = 0>
    ABSL_ATTRIBUTE_ALWAYS_INLINE void Callback(T&& object) && {
      use_external_data_(
          context_,
          ExternalData{
              ExternalStorage(
                  new ExternalObjectSubstr<std::decay_t<T>>(
                      std::forward<T>(object), substr_),
                  [](void* ptr) {
                    delete static_cast<ExternalObjectSubstr<std::decay_t<T>>*>(
                        ptr);
                  }),
              substr_});
    }

    absl::string_view substr_;
    void* context_;
    UseExternalDataFunction use_external_data_;
  };

  class StorageBase {
   protected:
    StorageBase() = default;

    StorageBase(const StorageBase&) = delete;
    StorageBase& operator=(const StorageBase&) = delete;

    virtual ~StorageBase() = default;

    void Initialize(absl::string_view substr) { substr_ = substr; }

   private:
    friend class ExternalRef;

    // Converts the external object either to `absl::string_view` or
    // `Chain::Block` by calling once either `use_string_view` or
    // `use_chain_block`.
    virtual void ToChainBlock(size_t max_bytes_to_copy, void* context,
                              UseStringViewFunction use_string_view,
                              UseChainBlockFunction use_chain_block) && = 0;

    // Converts the external object either to `absl::string_view` or
    // `absl::Cord` by calling once either `use_string_view` or `use_cord`.
    virtual void ToCord(size_t max_bytes_to_copy, void* context,
                        UseStringViewFunction use_string_view,
                        UseCordFunction use_cord) && = 0;

    // Converts the external object to `ExternalData` by calling once
    // `use_external_data`.
    virtual void ToExternalData(
        void* context, UseExternalDataFunction use_external_data) && = 0;

    bool empty() const { return substr_.empty(); }
    const char* data() const { return substr_.data(); }
    size_t size() const { return substr_.size(); }
    absl::string_view substr() const { return substr_; }

    absl::string_view substr_;
  };

  template <typename T>
  class StorageWholeWithoutCallOperator final : public StorageBase {
   public:
    StorageWholeWithoutCallOperator() = default;

    StorageWholeWithoutCallOperator(const StorageWholeWithoutCallOperator&) =
        delete;
    StorageWholeWithoutCallOperator& operator=(
        const StorageWholeWithoutCallOperator&) = delete;

   private:
    friend class ExternalRef;

    void Initialize(Initializer<T> object) {
      object_.emplace(
          std::move(object).Reference(std::move(temporary_storage_)));
      StorageBase::Initialize(BytesRef(*object_));
    }

    void ToChainBlock(size_t max_bytes_to_copy, void* context,
                      UseStringViewFunction use_string_view,
                      UseChainBlockFunction use_chain_block) &&
        override {
      if (size() <= max_bytes_to_copy) {
        use_string_view(context, substr());
        return;
      }
      // The constructor processes the object.
      ConverterToChainBlockWhole<T> converter(*std::move(object_), substr(),
                                              context, use_string_view,
                                              use_chain_block);
    }

    void ToCord(size_t max_bytes_to_copy, void* context,
                UseStringViewFunction use_string_view,
                UseCordFunction use_cord) &&
        override {
      if (size() <= max_bytes_to_copy) {
        use_string_view(context, substr());
        return;
      }
      // The constructor processes the object.
      ConverterToCordWhole<T> converter(*std::move(object_), substr(), context,
                                        use_string_view, use_cord);
    }

    void ToExternalData(void* context,
                        UseExternalDataFunction use_external_data) &&
        override {
      // The constructor processes the object.
      ConverterToExternalDataWhole<T> converter(*std::move(object_), substr(),
                                                context, use_external_data);
    }

    TemporaryStorage<T&&> object_;
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS TemporaryStorage<T> temporary_storage_;
  };

  template <typename T>
  class StorageWholeWithCallOperator final : public StorageBase {
   public:
    StorageWholeWithCallOperator() = default;

    StorageWholeWithCallOperator(const StorageWholeWithCallOperator&) = delete;
    StorageWholeWithCallOperator& operator=(
        const StorageWholeWithCallOperator&) = delete;

    ~StorageWholeWithCallOperator() {
      if (object_ != nullptr) {
        ExternalRef::CallOperatorSubstr(std::forward<T>(*object_), substr());
      }
    }

   private:
    friend class ExternalRef;

    void Initialize(Initializer<T> object) {
      T&& reference =
          std::move(object).Reference(std::move(temporary_storage_));
      object_ = &reference;
      StorageBase::Initialize(BytesRef(*object_));
    }

    void ToChainBlock(size_t max_bytes_to_copy, void* context,
                      UseStringViewFunction use_string_view,
                      UseChainBlockFunction use_chain_block) &&
        override {
      if (size() <= max_bytes_to_copy) {
        use_string_view(context, substr());
        return;
      }
      // The constructor processes the object.
      ConverterToChainBlockWhole<T> converter(
          ExtractObject(), substr(), context, use_string_view, use_chain_block);
    }

    void ToCord(size_t max_bytes_to_copy, void* context,
                UseStringViewFunction use_string_view,
                UseCordFunction use_cord) &&
        override {
      if (size() <= max_bytes_to_copy) {
        use_string_view(context, substr());
        return;
      }
      // The constructor processes the object.
      ConverterToCordWhole<T> converter(ExtractObject(), substr(), context,
                                        use_string_view, use_cord);
    }

    void ToExternalData(void* context,
                        UseExternalDataFunction use_external_data) &&
        override {
      // The constructor processes the object.
      ConverterToExternalDataWhole<T> converter(ExtractObject(), substr(),
                                                context, use_external_data);
    }

    T&& ExtractObject() {
      return std::forward<T>(*std::exchange(object_, nullptr));
    }

    std::remove_reference_t<T>* object_ = nullptr;
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS TemporaryStorage<T> temporary_storage_;
  };

  template <typename Arg>
  class StorageSubstrWithoutCallOperator final : public StorageBase {
   private:
    friend class ExternalRef;

    using T = TargetRefT<Arg>;

    void Initialize(Arg arg, absl::string_view substr) {
      StorageBase::Initialize(substr);
      arg_.emplace(std::forward<Arg>(arg));
    }

    void ToChainBlock(size_t max_bytes_to_copy, void* context,
                      UseStringViewFunction use_string_view,
                      UseChainBlockFunction use_chain_block) &&
        override {
      if (size() <= max_bytes_to_copy) {
        use_string_view(context, substr());
        return;
      }
      // The constructor processes the object.
      ConverterToChainBlockSubstr<T> converter(
          initializer().Reference(), substr(), context, use_string_view,
          use_chain_block);
    }

    void ToCord(size_t max_bytes_to_copy, void* context,
                UseStringViewFunction use_string_view,
                UseCordFunction use_cord) &&
        override {
      if (size() <= max_bytes_to_copy) {
        use_string_view(context, substr());
        return;
      }
      // The constructor processes the object.
      ConverterToCordSubstr<T> converter(initializer().Reference(), substr(),
                                         context, use_string_view, use_cord);
    }

    void ToExternalData(void* context,
                        UseExternalDataFunction use_external_data) &&
        override {
      // The constructor processes the object.
      ConverterToExternalDataSubstr<T> converter(
          initializer().Reference(), substr(), context, use_external_data);
    }

    Initializer<T> initializer() { return std::forward<Arg>(*arg_); }

    TemporaryStorage<Arg&&> arg_;
  };

  template <typename T>
  class StorageSubstrWithCallOperator final : public StorageBase {
   public:
    StorageSubstrWithCallOperator() = default;

    StorageSubstrWithCallOperator(const StorageSubstrWithCallOperator&) =
        delete;
    StorageSubstrWithCallOperator& operator=(
        const StorageSubstrWithCallOperator&) = delete;

    ~StorageSubstrWithCallOperator() {
      if (object_ != nullptr) {
        ExternalRef::CallOperatorSubstr(std::forward<T>(*object_), substr());
      }
    }

   private:
    friend class ExternalRef;

    void Initialize(Initializer<T> object, absl::string_view substr) {
      StorageBase::Initialize(substr);
      T&& reference =
          std::move(object).Reference(std::move(temporary_storage_));
      object_ = &reference;
    }

    void ToChainBlock(size_t max_bytes_to_copy, void* context,
                      UseStringViewFunction use_string_view,
                      UseChainBlockFunction use_chain_block) &&
        override {
      if (size() <= max_bytes_to_copy) {
        use_string_view(context, substr());
        return;
      }
      // The constructor processes the object.
      ConverterToChainBlockSubstr<T> converter(
          ExtractObject(), substr(), context, use_string_view, use_chain_block);
    }

    void ToCord(size_t max_bytes_to_copy, void* context,
                UseStringViewFunction use_string_view,
                UseCordFunction use_cord) &&
        override {
      if (size() <= max_bytes_to_copy) {
        use_string_view(context, substr());
        return;
      }
      // The constructor processes the object.
      ConverterToCordSubstr<T> converter(ExtractObject(), substr(), context,
                                         use_string_view, use_cord);
    }

    void ToExternalData(void* context,
                        UseExternalDataFunction use_external_data) &&
        override {
      // The constructor processes the object.
      ConverterToExternalDataSubstr<T> converter(ExtractObject(), substr(),
                                                 context, use_external_data);
    }

    T&& ExtractObject() {
      return std::forward<T>(*std::exchange(object_, nullptr));
    }

    std::remove_reference_t<T>* object_ = nullptr;
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS TemporaryStorage<T> temporary_storage_;
  };

  template <typename T, typename Enable = void>
  struct StorageWholeImpl;

  template <typename T>
  struct StorageWholeImpl<
      T, std::enable_if_t<!HasCallOperator<absl::remove_cvref_t<T>>::value>> {
    using type = StorageWholeWithoutCallOperator<T>;
  };

  template <typename T>
  struct StorageWholeImpl<
      T, std::enable_if_t<HasCallOperator<absl::remove_cvref_t<T>>::value>> {
    using type = StorageWholeWithCallOperator<T>;
  };

  template <typename Arg, typename Enable = void>
  struct StorageSubstrImpl;

  template <typename Arg>
  struct StorageSubstrImpl<Arg,
                           std::enable_if_t<!HasCallOperator<
                               absl::remove_cvref_t<TargetRefT<Arg>>>::value>> {
    using type = StorageSubstrWithoutCallOperator<Arg>;
  };

  template <typename Arg>
  struct StorageSubstrImpl<
      Arg, std::enable_if_t<
               HasCallOperator<absl::remove_cvref_t<TargetRefT<Arg>>>::value>> {
    using type = StorageSubstrWithCallOperator<TargetRefT<Arg>>;
  };

 public:
  // The type of the `storage` parameter for the constructor and
  // `ExternalRef::From()` which take an external object convertible
  // to `BytesRef`.
  template <typename T>
  using StorageWhole = typename StorageWholeImpl<T>::type;

  // The type of the `storage` parameter for the constructor and
  // `ExternalRef::From()` which take an external object and its substring.
  template <typename Arg>
  using StorageSubstr = typename StorageSubstrImpl<Arg>::type;

  // Constructs an `ExternalRef` from an external object or its `Initializer`.
  // See class comments for expectations on the external object.
  //
  // The object must be convertible to `BytesRef`.
  //
  // `storage` must outlive usages of the returned `ExternalRef`.
  template <typename Arg,
            std::enable_if_t<SupportsExternalRefWhole<TargetRefT<Arg>>::value,
                             int> = 0>
  /*implicit*/ ExternalRef(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND,
                           StorageWhole<TargetRefT<Arg>>&& storage
                               ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : storage_(&storage) {
    storage.Initialize(std::forward<Arg>(arg));
  }

  // Constructs an `ExternalRef` from an external object or its `Initializer`.
  // See class comments for expectations on the external object.
  //
  // `substr` must be owned by the object if it gets created or moved.
  //
  // `storage` must outlive usages of the returned `ExternalRef`.
  //
  // The object is not created if an initializer is passed rather than an
  // already constructed object, the object type does not use the call operator,
  // and only `absl::string_view` turns out to be needed. Hence `StorageSubstr`
  // is parameterized by `Arg&&` rather than `TargetRefT<Arg>`, so that it can
  // keep the original initializer.
  template <typename Arg,
            std::enable_if_t<SupportsExternalRefSubstr<TargetRefT<Arg>>::value,
                             int> = 0>
  explicit ExternalRef(
      Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND, absl::string_view substr,
      StorageSubstr<Arg&&>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : storage_(&storage) {
    storage.Initialize(std::forward<Arg>(arg), substr);
  }

  ExternalRef(ExternalRef&& that) = default;
  ExternalRef& operator=(ExternalRef&&) = delete;

  // Like `ExternalRef` constructor, but `RiegeliSupportsExternalRef()` or
  // `RiegeliSupportsExternalRefWhole()` is not needed. The caller is
  // responsible for using an appropriate type of the external object.
  template <typename Arg,
            std::enable_if_t<std::is_convertible_v<TargetRefT<Arg>, BytesRef>,
                             int> = 0>
  static ExternalRef From(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND,
                          StorageWhole<TargetRefT<Arg>>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND = {}) {
    storage.Initialize(std::forward<Arg>(arg));
    return ExternalRef(&storage);
  }

  // Like `ExternalRef` constructor, but `RiegeliSupportsExternalRef()` is not
  // needed. The caller is responsible for using an appropriate type of the
  // external object.
  //
  // The object is not created if an initializer is passed rather than an
  // already constructed object, the object type does not use the call operator,
  // and only `absl::string_view` turns out to be needed. Hence `StorageSubstr`
  // is parameterized by `Arg&&` rather than `TargetRefT<Arg>`, so that it can
  // keep the original initializer.
  template <typename Arg>
  static ExternalRef From(
      Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND, absl::string_view substr,
      StorageSubstr<Arg&&>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {}) {
    storage.Initialize(std::forward<Arg>(arg), substr);
    return ExternalRef(&storage);
  }

  // Returns `true` if the data size is 0.
  bool empty() const { return storage_->empty(); }

  // Returns the data pointer.
  const char* data() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return storage_->data();
  }

  // Returns the data size.
  size_t size() const { return storage_->size(); }

  // Returns the data as `absl::string_view`.
  //
  // This `ExternalRef` must outlive usages of the returned `absl::string_view`.
  /*implicit*/ operator absl::string_view() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return storage_->substr();
  }

  // The data can be converted to `Chain` using:
  //  * `Chain::Chain(ExternalRef)`
  //  * `Chain::Reset(ExternalRef)` or `riegeli::Reset(Chain&, ExternalRef)`
  //  * `Chain::Append(ExternalRef)`
  //  * `Chain::Prepend(ExternalRef)`

  // Converts the data to `absl::Cord`.
  explicit operator absl::Cord() && {
    absl::Cord result;
    // Destruction of a just default-constructed `absl::Cord` can be optimized
    // out. Construction in place is more efficient than assignment.
    result.~Cord();
    std::move(*storage_).ToCord(
        cord_internal::kMaxBytesToCopyToEmptyCord, &result,
        [](void* context, absl::string_view data) {
          new (context) absl::Cord(cord_internal::MakeBlockyCord(data));
        },
        [](void* context, absl::Cord data) {
          new (context) absl::Cord(std::move(data));
        });
    return result;
  }

  // Supports `riegeli::Reset(absl::Cord&, ExternalRef)`.
  friend void RiegeliReset(absl::Cord& dest, ExternalRef src) {
    std::move(src).AssignTo(dest);
  }

  // Appends the data to `dest`.
  void AppendTo(absl::Cord& dest) && {
    std::move(*storage_).ToCord(
        cord_internal::MaxBytesToCopyToCord(dest), &dest,
        [](void* context, absl::string_view data) {
          cord_internal::AppendToBlockyCord(data,
                                            *static_cast<absl::Cord*>(context));
        },
        [](void* context, absl::Cord data) {
          static_cast<absl::Cord*>(context)->Append(std::move(data));
        });
  }

  // Prepends the data to `dest`.
  void PrependTo(absl::Cord& dest) && {
    std::move(*storage_).ToCord(
        cord_internal::MaxBytesToCopyToCord(dest), &dest,
        [](void* context, absl::string_view data) {
          cord_internal::PrependToBlockyCord(
              data, *static_cast<absl::Cord*>(context));
        },
        [](void* context, absl::Cord data) {
          static_cast<absl::Cord*>(context)->Prepend(std::move(data));
        });
  }

  // Returns a type-erased external object with its deleter and data.
  explicit operator ExternalData() && {
    ExternalData result{ExternalStorage(nullptr, nullptr), absl::string_view()};
    // Destruction of just constructed `ExternalData` can be optimized out.
    // Construction in place is more efficient than assignment.
    result.~ExternalData();
    std::move(*storage_).ToExternalData(
        &result, [](void* context, ExternalData data) {
          new (context) ExternalData(std::move(data));
        });
    return result;
  }

 private:
  // For `InitializeTo()`, `AssignTo()`, `AppendTo()`, and `PrependTo()`.
  friend class Chain;

  explicit ExternalRef(StorageBase* storage) : storage_(storage) {}

  // Assigns the data to `dest` which is expected to be just
  // default-constructed.
  void InitializeTo(Chain& dest) && {
    // Destruction of a just default-constructed `Chain` can be optimized out.
    // Construction in place is more efficient than assignment.
    dest.~Chain();
    std::move(*storage_).ToChainBlock(
        Chain::kMaxBytesToCopyToEmpty, &dest,
        [](void* context, absl::string_view data) {
          new (context) Chain(data);
        },
        [](void* context, Chain::Block data) {
          new (context) Chain(std::move(data));
        });
  }

  // Assigns the data to `dest`.
  void AssignTo(Chain& dest) && {
    std::move(*storage_).ToChainBlock(
        Chain::kMaxBytesToCopyToEmpty, &dest,
        [](void* context, absl::string_view data) {
          static_cast<Chain*>(context)->Reset(data);
        },
        [](void* context, Chain::Block data) {
          static_cast<Chain*>(context)->Reset(std::move(data));
        });
  }

  // Assigns the data to `dest`.
  void AssignTo(absl::Cord& dest) && {
    std::move(*storage_).ToCord(
        cord_internal::kMaxBytesToCopyToEmptyCord, &dest,
        [](void* context, absl::string_view data) {
          cord_internal::AssignToBlockyCord(data,
                                            *static_cast<absl::Cord*>(context));
        },
        [](void* context, absl::Cord data) {
          *static_cast<absl::Cord*>(context) = std::move(data);
        });
  }

  // Appends the data to `dest`.
  void AppendTo(Chain& dest) && {
    std::move(*storage_).ToChainBlock(
        dest.MaxBytesToCopy(), &dest,
        [](void* context, absl::string_view data) {
          static_cast<Chain*>(context)->Append(data);
        },
        [](void* context, Chain::Block data) {
          static_cast<Chain*>(context)->Append(std::move(data));
        });
  }
  void AppendTo(Chain& dest, Chain::Options options) && {
    ChainWithOptions chain_with_options = {&dest, options};
    std::move(*storage_).ToChainBlock(
        dest.MaxBytesToCopy(options), &chain_with_options,
        [](void* context, absl::string_view data) {
          static_cast<ChainWithOptions*>(context)->dest->Append(
              data, static_cast<ChainWithOptions*>(context)->options);
        },
        [](void* context, Chain::Block data) {
          static_cast<ChainWithOptions*>(context)->dest->Append(
              std::move(data),
              static_cast<ChainWithOptions*>(context)->options);
        });
  }

  // Prepends the data to `dest`.
  void PrependTo(Chain& dest) && {
    std::move(*storage_).ToChainBlock(
        dest.MaxBytesToCopy(), &dest,
        [](void* context, absl::string_view data) {
          static_cast<Chain*>(context)->Prepend(data);
        },
        [](void* context, Chain::Block data) {
          static_cast<Chain*>(context)->Prepend(std::move(data));
        });
  }
  void PrependTo(Chain& dest, Chain::Options options) && {
    ChainWithOptions chain_with_options = {&dest, options};
    std::move(*storage_).ToChainBlock(
        dest.MaxBytesToCopy(options), &chain_with_options,
        [](void* context, absl::string_view data) {
          static_cast<ChainWithOptions*>(context)->dest->Prepend(
              data, static_cast<ChainWithOptions*>(context)->options);
        },
        [](void* context, Chain::Block data) {
          static_cast<ChainWithOptions*>(context)->dest->Prepend(
              std::move(data),
              static_cast<ChainWithOptions*>(context)->options);
        });
  }

  struct ChainWithOptions {
    Chain* dest;
    Chain::Options options;
  };

  StorageBase* storage_;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_EXTERNAL_REF_BASE_H_
