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
#include "riegeli/base/intrusive_shared_ptr.h"
#include "riegeli/base/temporary_storage.h"

namespace riegeli {

// Support `ExternalRef`.
inline size_t RiegeliAllocatedMemory(ABSL_ATTRIBUTE_UNUSED const void* self) {
  return 0;
}

// Support `ExternalRef`.
inline size_t RiegeliAllocatedMemory(const std::string* self) {
  // Do not bother checking for short string optimization. Such strings will
  // likely not be considered wasteful anyway.
  return self->capacity() + 1;
}

// Support `ExternalRef`.
template <typename T>
inline ExternalStorage RiegeliToExternalStorage(std::unique_ptr<T>* self) {
  return ExternalStorage(self->release(),
                         [](void* ptr) { delete static_cast<T*>(ptr); });
}

// Support `ExternalRef`.
template <typename T>
inline ExternalStorage RiegeliToExternalStorage(std::unique_ptr<T[]>* self) {
  return ExternalStorage(self->release(),
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
// In contrast to `Chain::FromExternal()` and `absl::MakeCordFromExternal()`,
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
// interfaces expected by `Chain::FromExternal()` and
// `absl::MakeCordFromExternal()`.
//
// If the `substr` parameter to `ExternalRef` constructor is given, `substr`
// must be valid for the new object if it gets created.
//
// If the `substr` parameter is not given, `T` must support:
// ```
//   // Returns contents of the object. Called when it is moved to its final
//   // location.
//   explicit operator absl::string_view() const;
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
//   friend size_t RiegeliAllocatedMemory(const T* self);
//
//   // Converts `*self` or its `substr` to `Chain`, if this can be done more
//   // efficiently than with `Chain::FromExternal()`. Can modify `*self`.
//   // `operator()` will no longer be called.
//   //
//   // The `substr` parameter is given here when the `substr` parameter to
//   // `ExternalRef` constructor was given.
//   friend Chain RiegeliToChain(T* self, absl::string_view substr);
//
//   // Converts `*self` or its `substr` to `absl::Cord`, if this can be done
//   // more efficiently than with `absl::MakeCordFromExternal()`. Can modify
//   // `*self`. `operator()` will no longer be called.
//   //
//   // The `substr` parameter is given here when the `substr` parameter to
//   // `ExternalRef` constructor was given.
//   friend absl::Cord RiegeliToCord(T* self, absl::string_view substr);
//
//   // Converts `*self` to `ExternalData`, if this can be done more efficiently
//   // than allocating the object on the heap, e.g. if the object fits in a
//   // pointer. Can modify `*self`. `operator()` will no longer be called.
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
  using UseChainRawBlockFunction =
      void (*)(void* context, IntrusiveSharedPtr<Chain::RawBlock> data);
  using UseCordFunction = void (*)(void* context, absl::Cord data);

  template <typename T>
  static bool Wasteful(const T& object, size_t extra_allocated, size_t used) {
    size_t allocated = RiegeliAllocatedMemory(&object);
    if (extra_allocated > std::numeric_limits<size_t>::max() - allocated) {
      RIEGELI_ASSERT_UNREACHABLE() << "Result of RiegeliAllocatedMemory() "
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
  static void CallOperator(T&& object, absl::string_view substr) {
    std::forward<T>(object)(substr);
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<absl::negation<HasCallOperatorSubstr<T>>,
                                  HasCallOperatorWhole<T>>::value,
                int> = 0>
  static void CallOperator(T&& object,
                           ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {
    std::forward<T>(object)();
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<HasCallOperatorSubstr<T>>,
                            absl::negation<HasCallOperatorWhole<T>>>::value,
          int> = 0>
  static void CallOperator(ABSL_ATTRIBUTE_UNUSED T&& object,
                           ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {}

  template <typename T, typename Enable = void>
  struct HasRiegeliToChainRawBlockWhole : std::false_type {};

  template <typename T>
  struct HasRiegeliToChainRawBlockWhole<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToChainRawBlock(std::declval<T*>())),
             IntrusiveSharedPtr<Chain::RawBlock>>::value>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasRiegeliToChainWhole : std::false_type {};

  template <typename T>
  struct HasRiegeliToChainWhole<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToChain(std::declval<T*>())), Chain>::value>>
      : std::true_type {};

  template <typename T,
            std::enable_if_t<HasRiegeliToChainRawBlockWhole<T>::value, int> = 0>
  static IntrusiveSharedPtr<Chain::RawBlock> ToChainRawBlockExternal(
      T&& object) {
    // The possibility of providing `RiegeliToChainRawBlock()` is intentionally
    // undocumented because it uses private `Chain::RawBlock`. This is meant for
    // `Chain::BlockRef`.
    return RiegeliToChainRawBlock(&object);
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<HasRiegeliToChainRawBlockWhole<T>>,
                            HasRiegeliToChainWhole<T>>::value,
          int> = 0>
  static IntrusiveSharedPtr<Chain::RawBlock> ToChainRawBlockExternal(
      T&& object) {
    return RiegeliToChain(&object).ToRawBlock();
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<HasRiegeliToChainRawBlockWhole<T>>,
                            absl::negation<HasRiegeliToChainWhole<T>>>::value,
          int> = 0>
  static IntrusiveSharedPtr<Chain::RawBlock> ToChainRawBlockExternal(
      T&& object) {
    return Chain::ExternalMethodsFor<T>::NewBlock(std::forward<T>(object));
  }

  template <typename T, typename Enable = void>
  struct HasRiegeliToChainRawBlockSubstr : std::false_type {};

  template <typename T>
  struct HasRiegeliToChainRawBlockSubstr<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToChainRawBlock(
                 std::declval<T*>(), std::declval<absl::string_view>())),
             IntrusiveSharedPtr<Chain::RawBlock>>::value>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasRiegeliToChainSubstr : std::false_type {};

  template <typename T>
  struct HasRiegeliToChainSubstr<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToChain(std::declval<T*>(),
                                     std::declval<absl::string_view>())),
             Chain>::value>> : std::true_type {};

  template <typename T, std::enable_if_t<
                            HasRiegeliToChainRawBlockSubstr<T>::value, int> = 0>
  static IntrusiveSharedPtr<Chain::RawBlock> ToChainRawBlockExternal(
      T&& object, absl::string_view substr) {
    // The possibility of providing `RiegeliToChainRawBlock()` is intentionally
    // undocumented because it uses private `Chain::RawBlock`. This is meant for
    // `Chain::BlockRef`.
    return RiegeliToChainRawBlock(&object, substr);
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<HasRiegeliToChainRawBlockSubstr<T>>,
                            HasRiegeliToChainSubstr<T>>::value,
          int> = 0>
  static IntrusiveSharedPtr<Chain::RawBlock> ToChainRawBlockExternal(
      T&& object, absl::string_view substr) {
    return RiegeliToChain(&object, substr).ToRawBlock();
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<HasRiegeliToChainRawBlockSubstr<T>>,
                            absl::negation<HasRiegeliToChainSubstr<T>>>::value,
          int> = 0>
  static IntrusiveSharedPtr<Chain::RawBlock> ToChainRawBlockExternal(
      T&& object, absl::string_view substr) {
    return Chain::ExternalMethodsFor<T>::NewBlock(std::forward<T>(object),
                                                  substr);
  }

  template <typename T>
  class ObjectForCordWholeData {
   public:
    explicit ObjectForCordWholeData(T&& object)
        : ptr_(std::make_unique<T>(std::move(object))) {}

    ObjectForCordWholeData(ObjectForCordWholeData&& that) = default;
    ObjectForCordWholeData& operator=(ObjectForCordWholeData&& that) = default;

    void operator()(absl::string_view substr) && {
      ExternalRef::CallOperator(std::move(*ptr_), substr);
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
             decltype(RiegeliToCord(std::declval<T*>())), absl::Cord>::value>>
      : std::true_type {};

  template <typename T,
            std::enable_if_t<HasRiegeliToCordWhole<T>::value, int> = 0>
  static absl::Cord ToCordExternal(T&& object,
                                   ABSL_ATTRIBUTE_UNUSED size_t size) {
    return RiegeliToCord(&object);
  }
  template <typename T,
            std::enable_if_t<!HasRiegeliToCordWhole<T>::value, int> = 0>
  static absl::Cord ToCordExternal(T&& object, size_t size) {
    ObjectForCordWholeData<T> object_for_cord(std::forward<T>(object));
    const absl::string_view data(*object_for_cord);
    RIEGELI_ASSERT_EQ(size, data.size()) << "ExternalRef: size mismatch";
    return absl::MakeCordFromExternal(data, std::move(object_for_cord));
  }

  template <typename T, typename Enable = void>
  struct HasRiegeliToCordSubstr : std::false_type {};

  template <typename T>
  struct HasRiegeliToCordSubstr<
      T, std::enable_if_t<std::is_convertible<
             decltype(RiegeliToCord(std::declval<T*>(),
                                    std::declval<absl::string_view>())),
             absl::Cord>::value>> : std::true_type {};

  template <typename T>
  class ObjectForCordSubstr {
   public:
    explicit ObjectForCordSubstr(Initializer<T> object)
        : object_(std::move(object).Construct()) {}

    ObjectForCordSubstr(ObjectForCordSubstr&& that) = default;
    ObjectForCordSubstr& operator=(ObjectForCordSubstr&& that) = default;

    void operator()(absl::string_view substr) && {
      ExternalRef::CallOperator(std::move(object_), substr);
    }

    T& operator*() { return object_; }
    const T& operator*() const { return object_; }

    template <
        typename DependentT = T,
        std::enable_if_t<HasRiegeliToCordSubstr<DependentT>::value, int> = 0>
    absl::Cord ToCord(absl::string_view substr) && {
      return RiegeliToCord(&object_, substr);
    }
    template <
        typename DependentT = T,
        std::enable_if_t<!HasRiegeliToCordSubstr<DependentT>::value, int> = 0>
    absl::Cord ToCord(absl::string_view substr) && {
      return absl::MakeCordFromExternal(substr, std::move(*this));
    }

   private:
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS T object_;
  };

  template <typename T, typename Enable = void>
  class ExternalObjectWhole {
   public:
    explicit ExternalObjectWhole(T&& object)
        : object_(std::forward<T>(object)) {}

    ~ExternalObjectWhole() { CallOperator(); }

    T& operator*() { return object_; }
    const T& operator*() const { return object_; }

   private:
    template <
        typename DependentT = T,
        std::enable_if_t<HasCallOperatorSubstr<DependentT>::value, int> = 0>
    void CallOperator() {
      std::forward<T>(object_)(absl::string_view(object_));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<
            absl::conjunction<absl::negation<HasCallOperatorSubstr<DependentT>>,
                              HasCallOperatorWhole<DependentT>>::value,
            int> = 0>
    void CallOperator() {
      std::forward<T>(object_)();
    }
    template <typename DependentT = T,
              std::enable_if_t<
                  absl::conjunction<
                      absl::negation<HasCallOperatorSubstr<DependentT>>,
                      absl::negation<HasCallOperatorWhole<DependentT>>>::value,
                  int> = 0>
    void CallOperator() {}

    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS T object_;
  };

  template <typename T, typename Enable = void>
  class ExternalObjectSubstr;

  template <typename T>
  class ExternalObjectSubstr<
      T, std::enable_if_t<HasCallOperatorSubstr<T>::value>> {
   public:
    explicit ExternalObjectSubstr(T&& object, absl::string_view substr)
        : object_(std::forward<T>(object)), substr_(substr) {}

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
        T&& object, ABSL_ATTRIBUTE_UNUSED absl::string_view substr)
        : ExternalObjectSubstr::ExternalObjectWhole(std::forward<T>(object)) {}
  };

  template <typename T, typename Enable = void>
  struct HasRiegeliToExternalData : std::false_type {};

  template <typename T>
  struct HasRiegeliToExternalData<
      T, std::enable_if_t<std::is_convertible<decltype(RiegeliToExternalData(
                                                  std::declval<T*>())),
                                              ExternalData>::value>>
      : std::true_type {};

  template <typename T,
            std::enable_if_t<HasRiegeliToExternalData<T>::value, int> = 0>
  static ExternalData ToExternalDataExternal(T&& object) {
    return RiegeliToExternalData(&object);
  }
  template <typename T,
            std::enable_if_t<!HasRiegeliToExternalData<T>::value, int> = 0>
  static ExternalData ToExternalDataExternal(T&& object) {
    auto* const storage = new ExternalObjectWhole<T>(std::forward<T>(object));
    const absl::string_view data(**storage);
    return ExternalData{
        ExternalStorage(storage,
                        [](void* ptr) {
                          delete static_cast<ExternalObjectWhole<T>*>(ptr);
                        }),
        data};
  }

  template <typename T, typename Enable = void>
  struct HasRiegeliToExternalStorage : std::false_type {};

  template <typename T>
  struct HasRiegeliToExternalStorage<
      T, std::enable_if_t<std::is_convertible<decltype(RiegeliToExternalStorage(
                                                  std::declval<T*>())),
                                              ExternalStorage>::value>>
      : std::true_type {};

  template <typename T,
            std::enable_if_t<HasRiegeliToExternalStorage<T>::value, int> = 0>
  static ExternalData ToExternalDataExternal(T&& object,
                                             absl::string_view substr) {
    return ExternalData{RiegeliToExternalStorage(&object), substr};
  }
  template <typename T,
            std::enable_if_t<!HasRiegeliToExternalStorage<T>::value, int> = 0>
  static ExternalData ToExternalDataExternal(T&& object,
                                             absl::string_view substr) {
    return ExternalData{
        ExternalStorage(
            new ExternalObjectSubstr<T>(std::forward<T>(object), substr),
            [](void* ptr) {
              delete static_cast<ExternalObjectSubstr<T>*>(ptr);
            }),
        substr};
  }

  class StorageBase {
   protected:
    StorageBase() = default;

    StorageBase(const StorageBase&) = delete;
    StorageBase& operator=(const StorageBase&) = delete;

    void Initialize(size_t size) { size_ = size; }

   private:
    friend class ExternalRef;

    size_t size() const { return size_; }

    size_t size_;
  };

  class StorageSubstrBase : public StorageBase {
   public:
    StorageSubstrBase() = default;

    StorageSubstrBase(const StorageSubstrBase&) = delete;
    StorageSubstrBase& operator=(const StorageSubstrBase&) = delete;

   protected:
    void Initialize(absl::string_view substr) {
      StorageBase::Initialize(substr.size());
      data_ = substr.data();
    }

   private:
    friend class ExternalRef;

    static absl::string_view ToStringView(StorageBase* storage) {
      return substr(storage);
    }

    static void ToChain(
        StorageBase* storage, ABSL_ATTRIBUTE_UNUSED size_t max_bytes_to_copy,
        void* context, UseStringViewFunction use_string_view,
        ABSL_ATTRIBUTE_UNUSED UseChainRawBlockFunction use_chain_raw_block) {
      use_string_view(context, substr(storage));
    }

    static void ToCord(StorageBase* storage,
                       ABSL_ATTRIBUTE_UNUSED size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       ABSL_ATTRIBUTE_UNUSED UseCordFunction use_cord) {
      use_string_view(context, substr(storage));
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      return ExternalDataCopy(substr(storage));
    }

    static absl::string_view substr(const StorageBase* storage) {
      return absl::string_view(
          static_cast<const StorageSubstrBase*>(storage)->data_,
          storage->size());
    }

    const char* data_;
  };

  template <typename Arg>
  class StorageWholeWithoutCallOperator : public StorageBase {
   public:
    StorageWholeWithoutCallOperator() = default;

    StorageWholeWithoutCallOperator(const StorageWholeWithoutCallOperator&) =
        delete;
    StorageWholeWithoutCallOperator& operator=(
        const StorageWholeWithoutCallOperator&) = delete;

   private:
    friend class ExternalRef;

    using T = InitializerTargetT<Arg>;

    void Initialize(Arg arg, size_t size) {
      StorageBase::Initialize(size);
      arg_ = &arg;
    }

    static absl::string_view ToStringView(StorageBase* storage) {
      const T& object =
          initializer(storage).ConstReference(temporary_storage(storage));
      const absl::string_view data(object);
      RIEGELI_ASSERT_EQ(storage->size(), data.size())
          << "ExternalRef: size mismatch";
      return data;
    }

    static void ToChain(StorageBase* storage, size_t max_bytes_to_copy,
                        void* context, UseStringViewFunction use_string_view,
                        UseChainRawBlockFunction use_chain_raw_block) {
      if (storage->size() <= max_bytes_to_copy) {
        const T& object =
            initializer(storage).ConstReference(temporary_storage(storage));
        const absl::string_view data(object);
        RIEGELI_ASSERT_EQ(storage->size(), data.size())
            << "ExternalRef: size mismatch";
        use_string_view(context, data);
        return;
      }
      T&& object = initializer(storage).Reference(temporary_storage(storage));
      const absl::string_view data(object);
      RIEGELI_ASSERT_EQ(storage->size(), data.size())
          << "ExternalRef: size mismatch";
      if (Wasteful(object, Chain::kExternalAllocatedSize<T>(), data.size())) {
        use_string_view(context, data);
        return;
      }
      use_chain_raw_block(context, ExternalRef::ToChainRawBlockExternal(
                                       std::forward<T>(object)));
    }

    static void ToCord(StorageBase* storage, size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       UseCordFunction use_cord) {
      if (storage->size() <= max_bytes_to_copy) {
        const T& object =
            initializer(storage).ConstReference(temporary_storage(storage));
        const absl::string_view data(object);
        RIEGELI_ASSERT_EQ(storage->size(), data.size())
            << "ExternalRef: size mismatch";
        use_string_view(context, data);
        return;
      }
      T&& object = initializer(storage).Reference(temporary_storage(storage));
      const absl::string_view data(object);
      RIEGELI_ASSERT_EQ(storage->size(), data.size())
          << "ExternalRef: size mismatch";
      if (Wasteful(object,
                   cord_internal::kSizeOfCordRepExternal +
                       sizeof(ObjectForCordWholeData<T>) + sizeof(T),
                   data.size())) {
        use_string_view(context, data);
        return;
      }
      use_cord(context,
               ExternalRef::ToCordExternal(std::move(object), storage->size()));
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      T&& object = initializer(storage).Reference(temporary_storage(storage));
      const absl::string_view data(object);
      RIEGELI_ASSERT_EQ(storage->size(), data.size())
          << "ExternalRef: size mismatch";
      if (Wasteful(object, sizeof(ExternalObjectWhole<T>), data.size())) {
        return ExternalDataCopy(data);
      }
      return ExternalRef::ToExternalDataExternal(std::move(object));
    }

    static Initializer<T> initializer(StorageBase* storage) {
      return std::forward<Arg>(
          *static_cast<StorageWholeWithoutCallOperator*>(storage)->arg_);
    }

    static TemporaryStorage<T>&& temporary_storage(StorageBase* storage) {
      return std::move(static_cast<StorageWholeWithoutCallOperator*>(storage)
                           ->temporary_storage_);
    }

    std::remove_reference_t<Arg>* arg_;
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
        StorageWholeWithCallOperator::CallOperator(std::forward<T>(*object_));
      }
    }

   private:
    friend class ExternalRef;

    void Initialize(Initializer<T> object, size_t size) {
      StorageBase::Initialize(size);
      T&& reference =
          std::move(object).Reference(std::move(temporary_storage_));
      object_ = &reference;
    }

    static absl::string_view ToStringView(StorageBase* storage) {
      return absl::string_view(object(storage));
    }

    static void ToChain(StorageBase* storage, size_t max_bytes_to_copy,
                        void* context, UseStringViewFunction use_string_view,
                        UseChainRawBlockFunction use_chain_raw_block) {
      if (storage->size() <= max_bytes_to_copy ||
          Wasteful(object(storage), Chain::kExternalAllocatedSize<T>(),
                   storage->size())) {
        use_string_view(context, absl::string_view(object(storage)));
        return;
      }
      use_chain_raw_block(context, ExternalRef::ToChainRawBlockExternal(
                                       ExtractObject(storage)));
    }

    static void ToCord(StorageBase* storage, size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       UseCordFunction use_cord) {
      if (storage->size() <= max_bytes_to_copy) {
        use_string_view(context, absl::string_view(object(storage)));
        return;
      }
      T&& object = ExtractObject(storage);
      const absl::string_view data(object);
      RIEGELI_ASSERT_EQ(storage->size(), data.size())
          << "ExternalRef: size mismatch";
      if (Wasteful(object,
                   cord_internal::kSizeOfCordRepExternal +
                       sizeof(ObjectForCordWholeData<T>) + sizeof(T),
                   data.size())) {
        use_string_view(context, data);
        StorageWholeWithCallOperator::CallOperator(std::forward<T>(object));
        return;
      }
      use_cord(context,
               ExternalRef::ToCordExternal(std::move(object), storage->size()));
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      T&& object = ExtractObject(storage);
      const absl::string_view data(object);
      RIEGELI_ASSERT_EQ(storage->size(), data.size())
          << "ExternalRef: size mismatch";
      if (Wasteful(object, sizeof(ExternalObjectWhole<T>), data.size())) {
        ExternalData result = ExternalDataCopy(data);
        StorageWholeWithCallOperator::CallOperator(std::forward<T>(object));
        return result;
      }
      return ExternalRef::ToExternalDataExternal(std::move(object));
    }

    template <
        typename DependentT = T,
        std::enable_if_t<HasCallOperatorSubstr<DependentT>::value, int> = 0>
    static void CallOperator(T&& object) {
      std::forward<T>(object)(absl::string_view(object));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<!HasCallOperatorSubstr<DependentT>::value, int> = 0>
    static void CallOperator(T&& object) {
      std::forward<T>(object)();
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

    static TemporaryStorage<T>&& temporary_storage(StorageBase* storage) {
      return std::move(static_cast<StorageWholeWithCallOperator*>(storage)
                           ->temporary_storage_);
    }

    std::remove_reference_t<T>* object_ = nullptr;
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS TemporaryStorage<T> temporary_storage_;
  };

  template <typename Arg>
  class StorageSubstrWithoutCallOperator : public StorageSubstrBase {
   private:
    friend class ExternalRef;

    using T = InitializerTargetT<Arg>;

    void Initialize(Arg arg, absl::string_view substr) {
      StorageSubstrBase::Initialize(substr);
      arg_ = &arg;
    }

    static void ToChain(StorageBase* storage, size_t max_bytes_to_copy,
                        void* context, UseStringViewFunction use_string_view,
                        UseChainRawBlockFunction use_chain_raw_block) {
      if (storage->size() <= max_bytes_to_copy) {
        use_string_view(context, substr(storage));
        return;
      }
      TemporaryStorage<T> temporary_storage;
      T&& object = initializer(storage).Reference(std::move(temporary_storage));
      if (Wasteful(object, Chain::kExternalAllocatedSize<T>(),
                   storage->size())) {
        use_string_view(context, substr(storage));
        return;
      }
      use_chain_raw_block(
          context, ExternalRef::ToChainRawBlockExternal(std::forward<T>(object),
                                                        substr(storage)));
    }

    static void ToCord(StorageBase* storage, size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       UseCordFunction use_cord) {
      if (storage->size() <= max_bytes_to_copy) {
        use_string_view(context, substr(storage));
        return;
      }
      ObjectForCordSubstr<T> object_for_cord(initializer(storage));
      if (Wasteful(*object_for_cord,
                   cord_internal::kSizeOfCordRepExternal + sizeof(T),
                   storage->size())) {
        use_string_view(context, substr(storage));
        return;
      }
      use_cord(context, std::move(object_for_cord).ToCord(substr(storage)));
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      TemporaryStorage<T> temporary_storage;
      T&& object = initializer(storage).Reference(std::move(temporary_storage));
      if (Wasteful(object, sizeof(ExternalObjectWhole<T>), storage->size())) {
        return ExternalDataCopy(substr(storage));
      }
      return ExternalRef::ToExternalDataExternal(std::forward<T>(object),
                                                 substr(storage));
    }

    static Initializer<T> initializer(StorageBase* storage) {
      return std::forward<Arg>(
          *static_cast<StorageSubstrWithoutCallOperator*>(storage)->arg_);
    }

    std::remove_reference_t<Arg>* arg_;
  };

  template <typename T>
  class StorageSubstrWithCallOperator : public StorageSubstrBase {
   public:
    StorageSubstrWithCallOperator() = default;

    StorageSubstrWithCallOperator(const StorageSubstrWithCallOperator&) =
        delete;
    StorageSubstrWithCallOperator& operator=(
        const StorageSubstrWithCallOperator&) = delete;

    ~StorageSubstrWithCallOperator() {
      if (object_ != nullptr) {
        StorageSubstrWithCallOperator::CallOperator(this,
                                                    std::forward<T>(*object_));
      }
    }

   private:
    friend class ExternalRef;

    void Initialize(Initializer<T> object, absl::string_view substr) {
      StorageSubstrBase::Initialize(substr);
      T&& reference = std::move(object).Reference(temporary_storage(this));
      object_ = &reference;
    }

    static void ToChain(StorageBase* storage, size_t max_bytes_to_copy,
                        void* context, UseStringViewFunction use_string_view,
                        UseChainRawBlockFunction use_chain_raw_block) {
      if (storage->size() <= max_bytes_to_copy ||
          Wasteful(object(storage), Chain::kExternalAllocatedSize<T>(),
                   storage->size())) {
        use_string_view(context, substr(storage));
        return;
      }
      use_chain_raw_block(
          context, ExternalRef::ToChainRawBlockExternal(ExtractObject(storage),
                                                        substr(storage)));
    }

    static void ToCord(StorageBase* storage, size_t max_bytes_to_copy,
                       void* context, UseStringViewFunction use_string_view,
                       UseCordFunction use_cord) {
      if (storage->size() <= max_bytes_to_copy) {
        use_string_view(context, substr(storage));
        return;
      }
      ObjectForCordSubstr<T> object_for_cord(ExtractObject(storage));
      if (Wasteful(*object_for_cord,
                   cord_internal::kSizeOfCordRepExternal + sizeof(T),
                   storage->size())) {
        use_string_view(context, substr(storage));
        StorageSubstrWithCallOperator::CallOperator(
            storage, std::forward<T>(*object_for_cord));
        return;
      }
      use_cord(context, std::move(object_for_cord).ToCord(substr(storage)));
    }

    static ExternalData ToExternalData(StorageBase* storage) {
      T&& object = ExtractObject(storage);
      if (Wasteful(object, sizeof(ExternalObjectSubstr<T>), storage->size())) {
        ExternalData result = ExternalDataCopy(substr(storage));
        StorageSubstrWithCallOperator::CallOperator(storage,
                                                    std::forward<T>(object));
        return result;
      }
      return ExternalRef::ToExternalDataExternal(std::move(object),
                                                 substr(storage));
    }

    template <
        typename DependentT = T,
        std::enable_if_t<HasCallOperatorSubstr<DependentT>::value, int> = 0>
    static void CallOperator(StorageBase* storage, T&& object) {
      std::forward<T>(object)(substr(storage));
    }
    template <
        typename DependentT = T,
        std::enable_if_t<!HasCallOperatorSubstr<DependentT>::value, int> = 0>
    static void CallOperator(ABSL_ATTRIBUTE_UNUSED StorageBase* storage,
                             T&& object) {
      std::forward<T>(object)();
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

    static TemporaryStorage<T>&& temporary_storage(StorageBase* storage) {
      return std::move(static_cast<StorageSubstrWithCallOperator*>(storage)
                           ->temporary_storage_);
    }

    std::remove_reference_t<T>* object_ = nullptr;
    ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS TemporaryStorage<T> temporary_storage_;
  };

  template <typename Arg, typename Enable = void>
  struct StorageWholeImpl;

  template <typename Arg>
  struct StorageWholeImpl<
      Arg, std::enable_if_t<!HasCallOperator<InitializerTargetT<Arg>>::value>> {
    using type = StorageWholeWithoutCallOperator<Arg>;
  };

  template <typename Arg>
  struct StorageWholeImpl<
      Arg, std::enable_if_t<HasCallOperator<InitializerTargetT<Arg>>::value>> {
    using type = StorageWholeWithCallOperator<InitializerTargetT<Arg>>;
  };

  template <typename Arg, typename Enable = void>
  struct StorageSubstrImpl;

  template <typename Arg>
  struct StorageSubstrImpl<
      Arg, std::enable_if_t<!HasCallOperator<InitializerTargetT<Arg>>::value>> {
    using type = StorageSubstrWithoutCallOperator<Arg>;
  };

  template <typename Arg>
  struct StorageSubstrImpl<
      Arg, std::enable_if_t<HasCallOperator<InitializerTargetT<Arg>>::value>> {
    using type = StorageSubstrWithCallOperator<InitializerTargetT<Arg>>;
  };

 public:
  // The type of the `storage` parameter for the constructor which copies the
  // data.
  using StorageCopy = StorageSubstrBase;

  // The type of the `storage` parameter for the constructor which takes an
  // external object convertible to `absl::string_view` and its size.
  template <typename Arg>
  using StorageWhole = typename StorageWholeImpl<Arg>::type;

  // The type of the `storage` parameter for the constructor which takes an
  // external object and a substring.
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
  // The size of the result of this conversion must be equal to `size`.
  //
  // If the result of conversion to `absl::string_view` is already known and it
  // remains valid when the object gets moved, it is better to use the overload
  // below which passes `substr` instead of `size`, because this may avoid
  // creating the external object altogether, and because conversion to
  // `absl::Cord` avoids an allocation which makes the object stable.
  //
  // `storage` must outlive usages of the returned `ExternalRef`.
  template <typename Arg,
            std::enable_if_t<
                std::is_constructible<absl::string_view,
                                      const InitializerTargetT<Arg>&>::value,
                int> = 0>
  explicit ExternalRef(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND, size_t size,
                       StorageWhole<Arg&&>&& storage
                           ABSL_ATTRIBUTE_LIFETIME_BOUND =
                               StorageWhole<Arg&&>())
      : methods_(&kMethods<StorageWhole<Arg&&>>), storage_(&storage) {
    storage.Initialize(std::forward<Arg>(arg), size);
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
  bool empty() const { return storage_->size() == 0; }

  // Returns the data size.
  size_t size() const { return storage_->size(); }

  // Converts the data to `absl::string_view`.
  //
  // This `ExternalRef` must outlive usages of the returned `absl::string_view`.
  explicit operator absl::string_view() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return methods_->to_string_view(storage_);
  }

  // Converts the data to `Chain`.
  explicit operator Chain() && {
    Chain result;
    // Destruction of a just default-constructed `Chain` can be optimized out.
    // Construction in place is more efficient than assignment.
    result.~Chain();
    methods_->to_chain(
        storage_, Chain::kMaxBytesToCopyToEmpty, &result,
        [](void* context, absl::string_view data) {
          new (context) Chain(data);
        },
        [](void* context, IntrusiveSharedPtr<Chain::RawBlock> data) {
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
    methods_->to_chain(
        storage_, dest.MaxBytesToCopy(), &dest,
        [](void* context, absl::string_view data) {
          static_cast<Chain*>(context)->Append(data);
        },
        [](void* context, IntrusiveSharedPtr<Chain::RawBlock> data) {
          static_cast<Chain*>(context)->AppendRawBlock(std::move(data));
        });
  }
  void AppendTo(Chain& dest, Chain::Options options) && {
    ChainWithOptions chain_with_options = {&dest, options};
    methods_->to_chain(
        storage_, dest.MaxBytesToCopy(options), &chain_with_options,
        [](void* context, absl::string_view data) {
          static_cast<ChainWithOptions*>(context)->dest->Append(
              data, static_cast<ChainWithOptions*>(context)->options);
        },
        [](void* context, IntrusiveSharedPtr<Chain::RawBlock> data) {
          static_cast<ChainWithOptions*>(context)->dest->AppendRawBlock(
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
    methods_->to_chain(
        storage_, dest.MaxBytesToCopy(), &dest,
        [](void* context, absl::string_view data) {
          static_cast<Chain*>(context)->Prepend(data);
        },
        [](void* context, IntrusiveSharedPtr<Chain::RawBlock> data) {
          static_cast<Chain*>(context)->PrependRawBlock(std::move(data));
        });
  }
  void PrependTo(Chain& dest, Chain::Options options) && {
    ChainWithOptions chain_with_options = {&dest, options};
    methods_->to_chain(
        storage_, dest.MaxBytesToCopy(options), &chain_with_options,
        [](void* context, absl::string_view data) {
          static_cast<ChainWithOptions*>(context)->dest->Prepend(
              data, static_cast<ChainWithOptions*>(context)->options);
        },
        [](void* context, IntrusiveSharedPtr<Chain::RawBlock> data) {
          static_cast<ChainWithOptions*>(context)->dest->PrependRawBlock(
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
    absl::string_view (*to_string_view)(StorageBase* storage);
    // Calls once either `use_string_view` or `use_chain_raw_block`.
    void (*to_chain)(StorageBase* storage, size_t max_bytes_to_copy,
                     void* context, UseStringViewFunction use_string_view,
                     UseChainRawBlockFunction use_chain_raw_block);
    // Calls once either `use_string_view` or `use_cord`.
    void (*to_cord)(StorageBase* storage, size_t max_bytes_to_copy,
                    void* context, UseStringViewFunction use_string_view,
                    UseCordFunction use_cord);
    ExternalData (*to_external_data)(StorageBase* storage);
  };

  template <typename Impl>
  static constexpr Methods kMethods = {Impl::ToStringView, Impl::ToChain,
                                       Impl::ToCord, Impl::ToExternalData};

  struct ChainWithOptions {
    Chain* dest;
    Chain::Options options;
  };

  const Methods* methods_;
  StorageBase* storage_;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_EXTERNAL_REF_BASE_H_
