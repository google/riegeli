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

#include <limits>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain_base.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/to_string_view.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// Support `ExternalRef`.
inline size_t RiegeliExternalMemory(ABSL_ATTRIBUTE_UNUSED const void* self) {
  return 0;
}

// Support `ExternalRef`.
inline size_t RiegeliExternalMemory(const std::string* self) {
  // Do not bother checking for short string optimization. Such strings will
  // likely not be considered wasteful anyway.
  return self->capacity() + 1;
}

// Support `ExternalRef`.
template <typename T>
inline ExternalStorage RiegeliToExternalStorage(std::unique_ptr<T>* self) {
  return ExternalStorage(const_cast<std::remove_cv_t<T>*>(self->release()),
                         [](void* ptr) { delete static_cast<T*>(ptr); });
}

// Support `ExternalRef`.
template <typename T>
inline ExternalStorage RiegeliToExternalStorage(std::unique_ptr<T[]>* self) {
  return ExternalStorage(const_cast<std::remove_cv_t<T>*>(self->release()),
                         [](void* ptr) { delete[] static_cast<T*>(ptr); });
}

// Support `ExternalRef`.
inline ExternalStorage RiegeliToExternalStorage(ExternalStorage* self) {
  return std::move(*self);
}

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
// or `ExternalData`, or appended or prepended to a `Chain` or `absl::Cord`.
// It can be consumed at most once.
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
// If the `substr` parameter to `ExternalRef` constructor is given, `substr`
// must be valid for the new object if it gets created.
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
//   //
//   // If this is present, the object will be created unconditionally, because
//   // calling this might be needed to delete resources which already exist.
//   //
//   // This can also be a const method. If this is not a const method and the
//   // object is passed by const or lvalue reference, this will be called on a
//   // mutable copy of the object.
//   void operator()(absl::string_view substr) && {}
//
//   // Returns an approximate amount of memory allocated by the object,
//   // excluding data stored inside the object itself.
//   //
//   // When deciding whether to share an object or copy its data,
//   // `ExternalRef` uses a heuristic involving the amount of allocated memory
//   // and used memory to determine whether the object is considered wasteful,
//   // and avoids sharing wasteful objects.
//   //
//   // This can be a faster but less accurate estimate than `MemoryEstimator`
//   // with `RiegeliRegisterSubobjects()`. It considers only a single object
//   // with flat data, and normally does not take memory allocator overhead
//   // into account.
//   friend size_t RiegeliExternalMemory(const T* self);
//
//   // Converts `*self` or its `substr` to `Chain::Block`, if this can be done
//   // more efficiently than with `Chain::Block` constructor. Can modify
//   // `*self`. `operator()` will no longer be called.
//   //
//   // The `self` parameter can also have type `const T*`. If it has type `T*`
//   // and the object is passed by const or lvalue reference, this will be
//   // called on a mutable copy of the object.
//   //
//   // The `substr` parameter is given here when the `substr` parameter to
//   // `ExternalRef` constructor was given.
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
//   // The `substr` parameter is given here when the `substr` parameter to
//   // `ExternalRef` constructor was given.
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
//   // Used for conversion to `ExternalData` when the `substr` parameter to
//   // `ExternalRef` constructor was not given.
//   friend ExternalData RiegeliToExternalData(T* self);
//
//   // Convert `*self` to `ExternalStorage`, if this can be done more
//   // efficiently than allocating the object on the heap, e.g. if the object
//   // fits in a pointer. Can modify `*self`. `operator()` will no longer be
//   // called.
//   //
//   // The `self` parameter can also have type `const T*`. If it has type `T*`
//   // and the object is passed by const or lvalue reference, this will be
//   // called on a mutable copy of the object.
//   //
//   // Used for conversion to `ExternalData` when the `substr` parameter to
//   // `ExternalRef` constructor was given. The same `substr` will be used in
//   // the returned `ExternalData`.
//   friend ExternalStorage RiegeliToExternalStorage(T* self);
//
//   // Shows internal structure in a human-readable way, for debugging.
//   //
//   // Used for conversion to `Chain`.
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
class ExternalRef {
 private:
  using UseStringViewFunction = void (*)(void* context, absl::string_view data);
  using UseChainBlockFunction = void (*)(void* context, Chain::Block data);
  using UseCordFunction = void (*)(void* context, absl::Cord data);

  template <typename T>
  static bool Wasteful(const T& object, size_t extra_allocated, size_t used) {
    size_t allocated = RiegeliExternalMemory(&object);
    if (extra_allocated > std::numeric_limits<size_t>::max() - allocated) {
      RIEGELI_ASSERT_UNREACHABLE() << "Result of RiegeliExternalMemory() "
                                      "suspiciously close to size_t range";
    }
    allocated += extra_allocated;
    return allocated >= used && riegeli::Wasteful(allocated, used);
  }

  template <typename T, typename Enable = void>
  struct HasCallOperatorSubstr : std::false_type {};

  template <typename T>
  struct HasCallOperatorSubstr<T, absl::void_t<decltype(std::declval<T&&>()(
                                      std::declval<absl::string_view>()))>>
      : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasCallOperatorWhole : std::false_type {};

  template <typename T>
  struct HasCallOperatorWhole<T, absl::void_t<decltype(std::declval<T&&>()())>>
      : std::true_type {};

  template <typename T>
  struct HasCallOperator
      : absl::disjunction<HasCallOperatorSubstr<T>, HasCallOperatorWhole<T>> {};

  template <typename T,
            std::enable_if_t<HasCallOperatorSubstr<T>::value, int> = 0>
  static void CallOperatorWhole(T&& object) {
    std::forward<T>(object)(riegeli::ToStringView(object));
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<absl::negation<HasCallOperatorSubstr<T>>,
                                  HasCallOperatorWhole<T>>::value,
                int> = 0>
  static void CallOperatorWhole(T&& object) {
    std::forward<T>(object)();
  }
  template <
      typename T,
      std::enable_if_t<absl::conjunction<absl::negation<HasCallOperator<T>>,
                                         HasCallOperatorSubstr<
                                             absl::remove_cvref_t<T>>>::value,
                       int> = 0>
  static void CallOperatorWhole(T&& object) {
    absl::remove_cvref_t<T> copy(object);
    std::move(copy)(riegeli::ToStringView(copy));
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<HasCallOperator<T>>,
              absl::negation<HasCallOperatorSubstr<absl::remove_cvref_t<T>>>,
              HasCallOperatorWhole<absl::remove_cvref_t<T>>>::value,
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
                absl::conjunction<absl::negation<HasCallOperatorSubstr<T>>,
                                  HasCallOperatorWhole<T>>::value,
                int> = 0>
  static void CallOperatorSubstr(
      T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {
    std::forward<T>(object)();
  }
  template <
      typename T,
      std::enable_if_t<absl::conjunction<absl::negation<HasCallOperator<T>>,
                                         HasCallOperatorSubstr<
                                             absl::remove_cvref_t<T>>>::value,
                       int> = 0>
  static void CallOperatorSubstr(T&& object, absl::string_view substr) {
    absl::remove_cvref_t<T> copy(object);
    std::move(copy)(riegeli::ToStringView(copy));
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<HasCallOperator<T>>,
              absl::negation<HasCallOperatorSubstr<absl::remove_cvref_t<T>>>,
              HasCallOperatorWhole<absl::remove_cvref_t<T>>>::value,
          int> = 0>
  static void CallOperatorSubstr(T&& object, absl::string_view substr) {
    (absl::remove_cvref_t<T>(object))();
  }
  template <typename T,
            std::enable_if_t<!HasCallOperator<absl::remove_cvref_t<T>>::value,
                             int> = 0>
  static void CallOperatorSubstr(
      ABSL_ATTRIBUTE_UNUSED T&& object,
      ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {}

  template <typename T>
  struct PointerType {
    using type = T*;
  };
  template <typename T>
  struct PointerType<T&> {
    using type = const T*;
  };
  template <typename T>
  struct PointerType<T&&> {
    using type = T*;
  };

  template <typename T>
  using PointerTypeT = typename PointerType<T>::type;

  template <typename T>
  static PointerTypeT<T> Pointer(T&& object) {
    return &object;
  }

  template <typename T, typename Enable = void>
  struct HasRiegeliToChainBlockWhole : std::false_type {};

  template <typename T>
  struct HasRiegeliToChainBlockWhole<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToChainBlock(std::declval<PointerTypeT<T>>())),
             Chain::Block>::value>> : std::true_type {};

  template <typename T,
            std::enable_if_t<HasRiegeliToChainBlockWhole<T>::value, int> = 0>
  static Chain::Block ToChainBlockExternal(T&& object) {
    return RiegeliToChainBlock(ExternalRef::Pointer(std::forward<T>(object)));
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<HasRiegeliToChainBlockWhole<T>>,
              HasRiegeliToChainBlockWhole<absl::remove_cvref_t<T>>>::value,
          int> = 0>
  static Chain::Block ToChainBlockExternal(T&& object) {
    return RiegeliToChainBlock(
        ExternalRef::Pointer(absl::remove_cvref_t<T>(object)));
  }
  template <typename T, std::enable_if_t<!HasRiegeliToChainBlockWhole<
                                             absl::remove_cvref_t<T>>::value,
                                         int> = 0>
  static Chain::Block ToChainBlockExternal(T&& object) {
    return Chain::Block(std::forward<T>(object));
  }

  template <typename T, typename Enable = void>
  struct HasRiegeliToChainBlockSubstr : std::false_type {};

  template <typename T>
  struct HasRiegeliToChainBlockSubstr<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToChainBlock(std::declval<PointerTypeT<T>>(),
                                          std::declval<absl::string_view>())),
             Chain::Block>::value>> : std::true_type {};

  template <typename T,
            std::enable_if_t<HasRiegeliToChainBlockSubstr<T>::value, int> = 0>
  static Chain::Block ToChainBlockExternal(T&& object,
                                           absl::string_view substr) {
    return RiegeliToChainBlock(ExternalRef::Pointer(std::forward<T>(object)),
                               substr);
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<HasRiegeliToChainBlockSubstr<T>>,
              HasRiegeliToChainBlockSubstr<absl::remove_cvref_t<T>>>::value,
          int> = 0>
  static Chain::Block ToChainBlockExternal(T&& object,
                                           absl::string_view substr) {
    return RiegeliToChainBlock(
        ExternalRef::Pointer(absl::remove_cvref_t<T>(object)), substr);
  }
  template <typename T, std::enable_if_t<!HasRiegeliToChainBlockSubstr<
                                             absl::remove_cvref_t<T>>::value,
                                         int> = 0>
  static Chain::Block ToChainBlockExternal(T&& object,
                                           absl::string_view substr) {
    return Chain::Block(std::forward<T>(object), substr);
  }

  template <typename T>
  class ObjectForCordWhole {
   public:
    explicit ObjectForCordWhole(Initializer<T> object)
        : ptr_(std::make_unique<T>(std::move(object))) {}

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

  template <typename T, typename Enable = void>
  struct HasRiegeliToCordWhole : std::false_type {};

  template <typename T>
  struct HasRiegeliToCordWhole<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToCord(std::declval<PointerTypeT<T>>())),
             absl::Cord>::value>> : std::true_type {};

  template <typename T,
            std::enable_if_t<HasRiegeliToCordWhole<T>::value, int> = 0>
  static absl::Cord ToCordExternal(T&& object) {
    return RiegeliToCord(ExternalRef::Pointer(std::forward<T>(object)));
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<HasRiegeliToCordWhole<T>>,
                    HasRiegeliToCordWhole<absl::remove_cvref_t<T>>>::value,
                int> = 0>
  static absl::Cord ToCordExternal(T&& object) {
    return RiegeliToCord(ExternalRef::Pointer(absl::remove_cvref_t<T>(object)));
  }
  template <
      typename T,
      std::enable_if_t<!HasRiegeliToCordWhole<absl::remove_cvref_t<T>>::value,
                       int> = 0>
  static absl::Cord ToCordExternal(T&& object) {
    ObjectForCordWhole<std::decay_t<T>> object_for_cord(
        std::forward<T>(object));
    return absl::MakeCordFromExternal(riegeli::ToStringView(*object_for_cord),
                                      std::move(object_for_cord));
  }

  template <typename T, typename Enable = void>
  struct HasRiegeliToCordSubstr : std::false_type {};

  template <typename T>
  struct HasRiegeliToCordSubstr<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToCord(std::declval<PointerTypeT<T>>(),
                                    std::declval<absl::string_view>())),
             absl::Cord>::value>> : std::true_type {};

  template <typename T>
  class ObjectForCordSubstr {
   public:
    explicit ObjectForCordSubstr(Initializer<T> object)
        : object_(std::move(object)) {}

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

  template <typename T,
            std::enable_if_t<HasRiegeliToCordSubstr<T>::value, int> = 0>
  static absl::Cord ToCordExternal(T&& object, absl::string_view substr) {
    return RiegeliToCord(ExternalRef::Pointer(std::forward<T>(object)), substr);
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<HasRiegeliToCordSubstr<T>>,
                    HasRiegeliToCordSubstr<absl::remove_cvref_t<T>>>::value,
                int> = 0>
  static absl::Cord ToCordExternal(T&& object, absl::string_view substr) {
    return RiegeliToCord(ExternalRef::Pointer(absl::remove_cvref_t<T>(object)),
                         substr);
  }
  template <
      typename T,
      std::enable_if_t<!HasRiegeliToCordSubstr<absl::remove_cvref_t<T>>::value,
                       int> = 0>
  static absl::Cord ToCordExternal(T&& object, absl::string_view substr) {
    return absl::MakeCordFromExternal(
        substr, ObjectForCordSubstr<std::decay_t<T>>(std::forward<T>(object)));
  }

  template <typename T, typename Enable = void>
  class ExternalObjectWhole {
   public:
    explicit ExternalObjectWhole(Initializer<T> object)
        : object_(std::move(object)) {}

    ~ExternalObjectWhole() {
      ExternalRef::CallOperatorWhole(std::forward<T>(object_));
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
        : object_(std::move(object)), substr_(substr) {}

    ~ExternalObjectSubstr() { std::forward<T>(object_)(substr_); }

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
    explicit ExternalObjectSubstr(
        Initializer<T> object, ABSL_ATTRIBUTE_UNUSED absl::string_view substr)
        : ExternalObjectSubstr::ExternalObjectWhole(std::move(object)) {}
  };

  template <typename T, typename Enable = void>
  struct HasRiegeliToExternalData : std::false_type {};

  template <typename T>
  struct HasRiegeliToExternalData<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToExternalData(std::declval<PointerTypeT<T>>())),
             ExternalData>::value>> : std::true_type {};

  template <typename T,
            std::enable_if_t<HasRiegeliToExternalData<T>::value, int> = 0>
  static ExternalData ToExternalDataExternal(T&& object) {
    return RiegeliToExternalData(ExternalRef::Pointer(std::forward<T>(object)));
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<HasRiegeliToExternalData<T>>,
                    HasRiegeliToExternalData<absl::remove_cvref_t<T>>>::value,
                int> = 0>
  static ExternalData ToExternalDataExternal(T&& object) {
    return RiegeliToExternalData(
        ExternalRef::Pointer(absl::remove_cvref_t<T>(object)));
  }
  template <
      typename T,
      std::enable_if_t<
          !HasRiegeliToExternalData<absl::remove_cvref_t<T>>::value, int> = 0>
  static ExternalData ToExternalDataExternal(T&& object) {
    auto* const storage =
        new ExternalObjectWhole<std::decay_t<T>>(std::forward<T>(object));
    return ExternalData{
        ExternalStorage(
            storage,
            [](void* ptr) {
              delete static_cast<ExternalObjectWhole<std::decay_t<T>>*>(ptr);
            }),
        riegeli::ToStringView(**storage)};
  }

  template <typename T, typename Enable = void>
  struct HasRiegeliToExternalStorage : std::false_type {};

  template <typename T>
  struct HasRiegeliToExternalStorage<
      T,
      std::enable_if_t<std::is_convertible<
          decltype(RiegeliToExternalStorage(std::declval<PointerTypeT<T>>())),
          ExternalStorage>::value>> : std::true_type {};

  template <typename T,
            std::enable_if_t<HasRiegeliToExternalStorage<T>::value, int> = 0>
  static ExternalData ToExternalDataExternal(T&& object,
                                             absl::string_view substr) {
    return ExternalData{
        RiegeliToExternalStorage(ExternalRef::Pointer(std::forward<T>(object))),
        substr};
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<HasRiegeliToExternalStorage<T>>,
              HasRiegeliToExternalStorage<absl::remove_cvref_t<T>>>::value,
          int> = 0>
  static ExternalData ToExternalDataExternal(T&& object,
                                             absl::string_view substr) {
    return ExternalData{RiegeliToExternalStorage(ExternalRef::Pointer(
                            absl::remove_cvref_t<T>(object))),
                        substr};
  }
  template <typename T, std::enable_if_t<!HasRiegeliToExternalStorage<
                                             absl::remove_cvref_t<T>>::value,
                                         int> = 0>
  static ExternalData ToExternalDataExternal(T&& object,
                                             absl::string_view substr) {
    return ExternalData{
        ExternalStorage(
            new ExternalObjectSubstr<std::decay_t<T>>(std::forward<T>(object),
                                                      substr),
            [](void* ptr) {
              delete static_cast<ExternalObjectSubstr<std::decay_t<T>>*>(ptr);
            }),
        substr};
  }

  class StorageBase {
   public:
    StorageBase() = default;

    StorageBase(const StorageBase&) = delete;
    StorageBase& operator=(const StorageBase&) = delete;

   protected:
    void Initialize(absl::string_view substr) { substr_ = substr; }

   private:
    friend class ExternalRef;

    static void ToChainBlock(
        StorageBase* storage, ABSL_ATTRIBUTE_UNUSED size_t max_bytes_to_copy,
        void* context, UseStringViewFunction use_string_view,
        ABSL_ATTRIBUTE_UNUSED UseChainBlockFunction use_chain_block) {
      use_string_view(context, storage->substr());
    }

    static void ToCord(StorageBase* storage,
                       ABSL_ATTRIBUTE_UNUSED size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       ABSL_ATTRIBUTE_UNUSED UseCordFunction use_cord) {
      use_string_view(context, storage->substr());
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      return ExternalDataCopy(storage->substr());
    }

    bool empty() const { return substr_.empty(); }
    const char* data() const { return substr_.data(); }
    size_t size() const { return substr_.size(); }
    absl::string_view substr() const { return substr_; }

    absl::string_view substr_;
  };

  template <typename T>
  class StorageWholeWithoutCallOperator : public StorageBase {
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
      StorageBase::Initialize(riegeli::ToStringView(*object_));
    }

    static void ToChainBlock(StorageBase* storage, size_t max_bytes_to_copy,
                             void* context,
                             UseStringViewFunction use_string_view,
                             UseChainBlockFunction use_chain_block) {
      T&& object = StorageWholeWithoutCallOperator::object(storage);
      if (storage->size() <= max_bytes_to_copy ||
          Wasteful(object, Chain::kExternalAllocatedSize<T>(),
                   storage->size())) {
        use_string_view(context, storage->substr());
        return;
      }
      use_chain_block(
          context, ExternalRef::ToChainBlockExternal(std::forward<T>(object)));
    }

    static void ToCord(StorageBase* storage, size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       UseCordFunction use_cord) {
      T&& object = StorageWholeWithoutCallOperator::object(storage);
      if (storage->size() <= max_bytes_to_copy ||
          Wasteful(object,
                   cord_internal::kSizeOfCordRepExternal +
                       sizeof(ObjectForCordWhole<std::decay_t<T>>) + sizeof(T),
                   storage->size())) {
        use_string_view(context, storage->substr());
        return;
      }
      use_cord(context, ExternalRef::ToCordExternal(std::forward<T>(object)));
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      T&& object = StorageWholeWithoutCallOperator::object(storage);
      if (Wasteful(object, sizeof(ExternalObjectWhole<std::decay_t<T>>),
                   storage->size())) {
        return ExternalDataCopy(storage->substr());
      }
      return ExternalRef::ToExternalDataExternal(std::forward<T>(object));
    }

    static T&& object(StorageBase* storage) {
      return *std::move(
          static_cast<StorageWholeWithoutCallOperator*>(storage)->object_);
    }

    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS
    TemporaryStorage<ReferenceOrCheapValueT<T>> object_;
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS TemporaryStorage<T> temporary_storage_;
  };

  template <typename T>
  class StorageWholeWithCallOperator : public StorageBase {
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
      StorageBase::Initialize(riegeli::ToStringView(*object_));
    }

    static void ToChainBlock(StorageBase* storage, size_t max_bytes_to_copy,
                             void* context,
                             UseStringViewFunction use_string_view,
                             UseChainBlockFunction use_chain_block) {
      if (storage->size() <= max_bytes_to_copy ||
          Wasteful(object(storage), Chain::kExternalAllocatedSize<T>(),
                   storage->size())) {
        use_string_view(context, storage->substr());
        return;
      }
      use_chain_block(
          context, ExternalRef::ToChainBlockExternal(ExtractObject(storage)));
    }

    static void ToCord(StorageBase* storage, size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       UseCordFunction use_cord) {
      if (storage->size() <= max_bytes_to_copy) {
        use_string_view(context, storage->substr());
        return;
      }
      T&& object = ExtractObject(storage);
      if (Wasteful(object,
                   cord_internal::kSizeOfCordRepExternal +
                       sizeof(ObjectForCordWhole<std::decay_t<T>>) + sizeof(T),
                   storage->size())) {
        use_string_view(context, storage->substr());
        ExternalRef::CallOperatorSubstr(std::forward<T>(object),
                                        storage->substr());
        return;
      }
      use_cord(context, ExternalRef::ToCordExternal(std::forward<T>(object)));
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      T&& object = ExtractObject(storage);
      if (Wasteful(object, sizeof(ExternalObjectWhole<std::decay_t<T>>),
                   storage->size())) {
        ExternalData result = ExternalDataCopy(storage->substr());
        ExternalRef::CallOperatorSubstr(std::forward<T>(object),
                                        storage->substr());
        return result;
      }
      return ExternalRef::ToExternalDataExternal(std::forward<T>(object));
    }

    static const T& object(const StorageBase* storage) {
      return *static_cast<const StorageWholeWithCallOperator*>(storage)
                  ->object_;
    }

    static T&& ExtractObject(StorageBase* storage) {
      return std::forward<T>(*std::exchange(
          static_cast<StorageWholeWithCallOperator*>(storage)->object_,
          nullptr));
    }

    std::remove_reference_t<T>* object_ = nullptr;
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS TemporaryStorage<T> temporary_storage_;
  };

  template <typename Arg>
  class StorageSubstrWithoutCallOperator : public StorageBase {
   private:
    friend class ExternalRef;

    using T = InitializerTargetRefT<Arg>;

    void Initialize(Arg arg, absl::string_view substr) {
      StorageBase::Initialize(substr);
      arg_.emplace(std::forward<Arg>(arg));
    }

    static void ToChainBlock(StorageBase* storage, size_t max_bytes_to_copy,
                             void* context,
                             UseStringViewFunction use_string_view,
                             UseChainBlockFunction use_chain_block) {
      if (storage->size() <= max_bytes_to_copy) {
        use_string_view(context, storage->substr());
        return;
      }
      TemporaryStorage<T> temporary_storage;
      T&& object = initializer(storage).Reference(std::move(temporary_storage));
      if (Wasteful(object, Chain::kExternalAllocatedSize<T>(),
                   storage->size())) {
        use_string_view(context, storage->substr());
        return;
      }
      use_chain_block(context, ExternalRef::ToChainBlockExternal(
                                   std::forward<T>(object), storage->substr()));
    }

    static void ToCord(StorageBase* storage, size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       UseCordFunction use_cord) {
      if (storage->size() <= max_bytes_to_copy) {
        use_string_view(context, storage->substr());
        return;
      }
      TemporaryStorage<T> temporary_storage;
      T&& object = initializer(storage).Reference(std::move(temporary_storage));
      if (Wasteful(object,
                   cord_internal::kSizeOfCordRepExternal +
                       sizeof(ObjectForCordSubstr<std::decay_t<T>>),
                   storage->size())) {
        use_string_view(context, storage->substr());
        return;
      }
      use_cord(context, ExternalRef::ToCordExternal(std::forward<T>(object),
                                                    storage->substr()));
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      TemporaryStorage<T> temporary_storage;
      T&& object = initializer(storage).Reference(std::move(temporary_storage));
      if (Wasteful(object, sizeof(ExternalObjectWhole<std::decay_t<T>>),
                   storage->size())) {
        return ExternalDataCopy(storage->substr());
      }
      return ExternalRef::ToExternalDataExternal(std::forward<T>(object),
                                                 storage->substr());
    }

    static Initializer<T> initializer(StorageBase* storage) {
      return std::forward<Arg>(
          *static_cast<StorageSubstrWithoutCallOperator*>(storage)->arg_);
    }

    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS
    TemporaryStorage<ReferenceOrCheapValueT<Arg>> arg_;
  };

  template <typename T>
  class StorageSubstrWithCallOperator : public StorageBase {
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

    static void ToChainBlock(StorageBase* storage, size_t max_bytes_to_copy,
                             void* context,
                             UseStringViewFunction use_string_view,
                             UseChainBlockFunction use_chain_block) {
      if (storage->size() <= max_bytes_to_copy ||
          Wasteful(object(storage), Chain::kExternalAllocatedSize<T>(),
                   storage->size())) {
        use_string_view(context, storage->substr());
        return;
      }
      use_chain_block(context, ExternalRef::ToChainBlockExternal(
                                   ExtractObject(storage), storage->substr()));
    }

    static void ToCord(StorageBase* storage, size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       UseCordFunction use_cord) {
      if (storage->size() <= max_bytes_to_copy ||
          Wasteful(object(storage),
                   cord_internal::kSizeOfCordRepExternal +
                       sizeof(ObjectForCordSubstr<std::decay_t<T>>),
                   storage->size())) {
        use_string_view(context, storage->substr());
        return;
      }
      use_cord(context, ExternalRef::ToCordExternal(ExtractObject(storage),
                                                    storage->substr()));
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      T&& object = ExtractObject(storage);
      if (Wasteful(object, sizeof(ExternalObjectSubstr<std::decay_t<T>>),
                   storage->size())) {
        ExternalData result = ExternalDataCopy(storage->substr());
        ExternalRef::CallOperatorSubstr(std::forward<T>(object),
                                        storage->substr());
        return result;
      }
      return ExternalRef::ToExternalDataExternal(std::forward<T>(object),
                                                 storage->substr());
    }

    static const T& object(const StorageBase* storage) {
      return *static_cast<const StorageSubstrWithCallOperator*>(storage)
                  ->object_;
    }

    static T&& ExtractObject(StorageBase* storage) {
      return std::forward<T>(*std::exchange(
          static_cast<StorageSubstrWithCallOperator*>(storage)->object_,
          nullptr));
    }

    std::remove_reference_t<T>* object_ = nullptr;
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS TemporaryStorage<T> temporary_storage_;
  };

  template <typename Arg, typename Enable = void>
  struct StorageWholeImpl;

  template <typename Arg>
  struct StorageWholeImpl<
      Arg, std::enable_if_t<!HasCallOperator<
               absl::remove_cvref_t<InitializerTargetRefT<Arg>>>::value>> {
    using type = StorageWholeWithoutCallOperator<InitializerTargetRefT<Arg>>;
  };

  template <typename Arg>
  struct StorageWholeImpl<
      Arg, std::enable_if_t<HasCallOperator<
               absl::remove_cvref_t<InitializerTargetRefT<Arg>>>::value>> {
    using type = StorageWholeWithCallOperator<InitializerTargetRefT<Arg>>;
  };

  template <typename Arg, typename Enable = void>
  struct StorageSubstrImpl;

  template <typename Arg>
  struct StorageSubstrImpl<
      Arg, std::enable_if_t<!HasCallOperator<
               absl::remove_cvref_t<InitializerTargetRefT<Arg>>>::value>> {
    using type = StorageSubstrWithoutCallOperator<Arg>;
  };

  template <typename Arg>
  struct StorageSubstrImpl<
      Arg, std::enable_if_t<HasCallOperator<
               absl::remove_cvref_t<InitializerTargetRefT<Arg>>>::value>> {
    using type = StorageSubstrWithCallOperator<InitializerTargetRefT<Arg>>;
  };

 public:
  // The type of the `storage` parameter for the constructor which copies the
  // data.
  using StorageCopy = StorageBase;

  // The type of the `storage` parameter for the constructor which takes
  // an external object supporting `RiegeliToStringView(&object)`,
  // `absl::string_view(object)`, or `absl::Span<const char>(object)`.
  template <typename Arg>
  using StorageWhole = typename StorageWholeImpl<Arg>::type;

  // The type of the `storage` parameter for the constructor which takes an
  // external object and its substring.
  template <typename Arg>
  using StorageSubstr = typename StorageSubstrImpl<Arg>::type;

  // Constructs an `ExternalRef` from `data` which will be copied. No external
  // object will be created.
  //
  // `storage` must outlive usages of the returned `ExternalRef`.
  //
  // `StorageSubstr<Arg>` derives from `StorageCopy` and can also be used for
  // this constructor. This is useful in a function which conditionally uses
  // either of these constructors.
  explicit ExternalRef(
      absl::string_view data = absl::string_view(),
      StorageCopy&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND = StorageCopy())
      : methods_(&kMethods<StorageCopy>), storage_(&storage) {
    storage.Initialize(data);
  }

  // Constructs an `ExternalRef` from an external object or its `Initializer`.
  // See class comments for expectations on the external object.
  //
  // The object must be explicitly convertible to `absl::string_view`.
  //
  // If the result of conversion to `absl::string_view` is already known and it
  // remains valid when the object gets moved, it is better to use the overload
  // below which passes `substr`, because this may avoid creating the external
  // object altogether, and because conversion to `absl::Cord` avoids an
  // allocation which makes the object stable.
  //
  // `storage` must outlive usages of the returned `ExternalRef`.
  template <typename Arg,
            std::enable_if_t<
                SupportsToStringView<InitializerTargetT<Arg>>::value, int> = 0>
  explicit ExternalRef(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND,
                       StorageWhole<Arg&&>&& storage
                           ABSL_ATTRIBUTE_LIFETIME_BOUND =
                               StorageWhole<Arg&&>())
      : methods_(&kMethods<StorageWhole<Arg&&>>), storage_(&storage) {
    storage.Initialize(std::forward<Arg>(arg));
  }

  // Constructs an `ExternalRef` from an external object or its `Initializer`.
  // See class comments for expectations on the external object.
  //
  // `substr` must be valid for the new object if it gets created.
  //
  // `storage` must outlive usages of the returned `ExternalRef`.
  template <typename Arg>
  explicit ExternalRef(
      Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND, absl::string_view substr,
      StorageSubstr<Arg&&>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
          StorageSubstr<Arg&&>())
      : methods_(&kMethods<StorageSubstr<Arg&&>>), storage_(&storage) {
    storage.Initialize(std::forward<Arg>(arg), substr);
  }

  ExternalRef(ExternalRef&& that) = default;
  ExternalRef& operator=(ExternalRef&& that) = default;

  // Returns `true` if the data size is 0.
  bool empty() const { return storage_->empty(); }

  // Returns the data pointer.
  const char* data() const { return storage_->data(); }

  // Returns the data size.
  size_t size() const { return storage_->size(); }

  // Returns the data as `absl::string_view`.
  //
  // This `ExternalRef` must outlive usages of the returned `absl::string_view`.
  explicit operator absl::string_view() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return storage_->substr();
  }

  // Converts the data to `Chain`.
  explicit operator Chain() && {
    Chain result;
    // Destruction of a just default-constructed `Chain` can be optimized out.
    // Construction in place is more efficient than assignment.
    result.~Chain();
    methods_->to_chain_block(
        storage_, Chain::kMaxBytesToCopyToEmpty, &result,
        [](void* context, absl::string_view data) {
          new (context) Chain(data);
        },
        [](void* context, Chain::Block data) {
          new (context) Chain(std::move(data));
        });
    return result;
  }

  // Converts the data to `absl::Cord`.
  explicit operator absl::Cord() && {
    absl::Cord result;
    // Destruction of a just default-constructed `absl::Cord` can be optimized
    // out. Construction in place is more efficient than assignment.
    result.~Cord();
    methods_->to_cord(
        storage_, cord_internal::kMaxBytesToCopyToEmptyCord, &result,
        [](void* context, absl::string_view data) {
          new (context) absl::Cord(cord_internal::MakeBlockyCord(data));
        },
        [](void* context, absl::Cord data) {
          new (context) absl::Cord(std::move(data));
        });
    return result;
  }

  // Appends the data to `dest`.
  void AppendTo(Chain& dest) && {
    methods_->to_chain_block(
        storage_, dest.MaxBytesToCopy(), &dest,
        [](void* context, absl::string_view data) {
          static_cast<Chain*>(context)->Append(data);
        },
        [](void* context, Chain::Block data) {
          static_cast<Chain*>(context)->Append(std::move(data));
        });
  }
  void AppendTo(Chain& dest, Chain::Options options) && {
    ChainWithOptions chain_with_options = {&dest, options};
    methods_->to_chain_block(
        storage_, dest.MaxBytesToCopy(options), &chain_with_options,
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

  // Appends the data to `dest`.
  void AppendTo(absl::Cord& dest) && {
    methods_->to_cord(
        storage_, cord_internal::MaxBytesToCopyToCord(dest), &dest,
        [](void* context, absl::string_view data) {
          cord_internal::AppendToBlockyCord(data,
                                            *static_cast<absl::Cord*>(context));
        },
        [](void* context, absl::Cord data) {
          static_cast<absl::Cord*>(context)->Append(std::move(data));
        });
  }

  // Prepends the data to `dest`.
  void PrependTo(Chain& dest) && {
    methods_->to_chain_block(
        storage_, dest.MaxBytesToCopy(), &dest,
        [](void* context, absl::string_view data) {
          static_cast<Chain*>(context)->Prepend(data);
        },
        [](void* context, Chain::Block data) {
          static_cast<Chain*>(context)->Prepend(std::move(data));
        });
  }
  void PrependTo(Chain& dest, Chain::Options options) && {
    ChainWithOptions chain_with_options = {&dest, options};
    methods_->to_chain_block(
        storage_, dest.MaxBytesToCopy(options), &chain_with_options,
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

  // Prepends the data to `dest`.
  void PrependTo(absl::Cord& dest) && {
    methods_->to_cord(
        storage_, cord_internal::MaxBytesToCopyToCord(dest), &dest,
        [](void* context, absl::string_view data) {
          cord_internal::PrependToBlockyCord(
              data, *static_cast<absl::Cord*>(context));
        },
        [](void* context, absl::Cord data) {
          static_cast<absl::Cord*>(context)->Prepend(std::move(data));
        });
  }

  // Returns a type-erased external object with its deleter and data.
  operator ExternalData() && { return methods_->to_external_data(storage_); }

 private:
  struct Methods {
    // Calls once either `use_string_view` or `use_chain_block`.
    void (*to_chain_block)(StorageBase* storage, size_t max_bytes_to_copy,
                           void* context, UseStringViewFunction use_string_view,
                           UseChainBlockFunction use_chain_block);
    // Calls once either `use_string_view` or `use_cord`.
    void (*to_cord)(StorageBase* storage, size_t max_bytes_to_copy,
                    void* context, UseStringViewFunction use_string_view,
                    UseCordFunction use_cord);
    ExternalData (*to_external_data)(StorageBase* storage);
  };

  template <typename Impl>
  static constexpr Methods kMethods = {Impl::ToChainBlock, Impl::ToCord,
                                       Impl::ToExternalData};

  struct ChainWithOptions {
    Chain* dest;
    Chain::Options options;
  };

  const Methods* methods_;
  StorageBase* storage_;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_EXTERNAL_REF_BASE_H_
