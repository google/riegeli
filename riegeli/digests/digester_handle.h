// Copyright 2023 Google LLC
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

#ifndef RIEGELI_DIGESTS_DIGESTER_HANDLE_H_
#define RIEGELI_DIGESTS_DIGESTER_HANDLE_H_

#include <stddef.h>

#include <cstddef>
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/has_absl_stringify.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/any.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/dependency_manager.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/digests/digest_converter.h"

namespace riegeli {

// `SupportsDigesterBaseHandle<T>::value` is `true` when `T&` is a valid
// constructor argument for `DigesterBaseHandle`.

template <typename T, typename Enable = void>
struct SupportsDigesterBaseHandle : std::false_type {};

template <typename T>
struct SupportsDigesterBaseHandle<
    T, std::enable_if_t<std::conjunction_v<
           std::negation<std::is_const<T>>,
           std::is_void<std::void_t<decltype(std::declval<T&>().Write(
               std::declval<absl::string_view>()))>>>>> : std::true_type {};

// Type-erased pointer to a target object called a digester, which observes data
// being read or written.
//
// The target should support:
//
// ```
//   // All of the following methods returning `bool` return `true` on success.
//   // They may also return `void` which is treated as `true`.
//
//   // If `write_size_hint` is not `std::nullopt`, hints that this amount of
//   // data will be written from the current position. This may improve
//   // performance and memory usage.
//   //
//   // If the hint turns out to not match reality, nothing breaks. It is better
//   // if `write_size_hint` is slightly too large than slightly too small.
//   //
//   // Optional. If absent, does nothing.
//   void SetWriteSizeHint(std::optional<Position> write_size_hint);
//
//   // Called with consecutive fragments of data.
//   //
//   // Precondition: the digester is open.
//   bool Write(absl::string_view src);
//
//   // Called with consecutive fragments of data.
//   //
//   // Precondition: the digester is open.
//   //
//   // Optional. If absent, implemented in terms of `Write(absl::string_view)`.
//   bool Write(const Chain& src);
//
//   // Called with consecutive fragments of data.
//   //
//   // Precondition: the digester is open.
//   //
//   // Optional. If absent, implemented in terms of `Write(absl::string_view)`.
//   bool Write(const absl::Cord& src);
//
//   // Can be called instead of `Write()` when data consists of zeros.
//   //
//   // Precondition: the digester is open.
//   //
//   // Optional. If absent, implemented in terms of `Write(absl::string_view)`.
//   bool Write(ByteFill src);
//
//   // Optionally called when nothing more will be digested. This can make
//   // `Digest()` more efficient. Resources can be freed. Marks the digester
//   // as not open.
//   //
//   // Does nothing if the digester is not open.
//   //
//   // Optional. If absent, does nothing.
//   bool Close();
//
//   // Returns an `absl::Status` describing the failure if the digester is
//   // failed.
//   //
//   // Can return `absl::OkStatus()` if tracking the status is not supported.
//   //
//   // Optional. If absent, `absl::OkStatus()` is assumed.
//   absl::Status status() const;
// ```
//
// `DigesterHandle<DigestType>` extends `DigesterBaseHandle` with `Digest()`
// returning `DigestType`.
//
// For digesting many small values it is better to use `DigestingWriter` which
// adds a buffering layer.
class ABSL_NULLABILITY_COMPATIBLE DigesterBaseHandle
    : WithEqual<DigesterBaseHandle> {
 private:
  using pointer = void*;  // For `ABSL_NULLABILITY_COMPATIBLE`.

 public:
  // Creates a `DigesterBaseHandle` which does not refer to a target.
  DigesterBaseHandle() = default;
  /*implicit*/ DigesterBaseHandle(std::nullptr_t) {}

  // Creates a `DigesterBaseHandle` which refers to `target`.
  template <
      typename T,
      std::enable_if_t<std::conjunction_v<NotSameRef<DigesterBaseHandle, T&>,
                                          SupportsDigesterBaseHandle<T>>,
                       int> = 0>
  /*implicit*/ DigesterBaseHandle(T& target ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : methods_(&kMethods<T>), target_(target) {}

  DigesterBaseHandle(const DigesterBaseHandle& that) = default;
  DigesterBaseHandle& operator=(const DigesterBaseHandle& that) = default;

  friend bool operator==(DigesterBaseHandle a, std::nullptr_t) {
    return a.target() == nullptr;
  }

  // If `write_size_hint` is not `std::nullopt`, hints that this amount of data
  // will be written from the current position. This may improve performance and
  // memory usage.
  //
  // If the hint turns out to not match reality, nothing breaks. It is better if
  // `write_size_hint` is slightly too large than slightly too small.
  void SetWriteSizeHint(std::optional<Position> write_size_hint) {
    methods_->set_write_size_hint(target_, write_size_hint);
  }

  // Called with consecutive fragments of data.
  //
  // Precondition: the digester is open.
  bool Write(char src) { return Write(absl::string_view(&src, 1)); }
#if __cpp_char8_t
  bool Write(char8_t src) { return Write(static_cast<char>(src)); }
#endif
  bool Write(BytesRef src) { return methods()->write(target(), src); }
  ABSL_ATTRIBUTE_ALWAYS_INLINE
  bool Write(const char* src) { return Write(absl::string_view(src)); }
  bool Write(const Chain& src) { return methods()->write_chain(target(), src); }
  bool Write(const absl::Cord& src) {
    return methods()->write_cord(target(), src);
  }
  bool Write(ByteFill src) { return methods()->write_byte_fill(target(), src); }
  template <
      typename Src,
      std::enable_if_t<
          std::conjunction_v<
              absl::HasAbslStringify<Src>,
              std::negation<std::is_convertible<Src&&, BytesRef>>,
              std::negation<std::is_convertible<Src&&, const Chain&>>,
              std::negation<std::is_convertible<Src&&, const absl::Cord&>>,
              std::negation<std::is_convertible<Src&&, ByteFill>>>,
          int> = 0>
  bool Write(Src&& src);

  // Numeric types supported by `Writer::Write()` are not supported by
  // `DigesterBaseHandle::Write()`. Use `DigestingWriter` instead or convert
  // them to strings.
  bool Write(signed char) = delete;
  bool Write(unsigned char) = delete;
  bool Write(short) = delete;
  bool Write(unsigned short) = delete;
  bool Write(int) = delete;
  bool Write(unsigned) = delete;
  bool Write(long) = delete;
  bool Write(unsigned long) = delete;
  bool Write(long long) = delete;
  bool Write(unsigned long long) = delete;
  bool Write(absl::int128) = delete;
  bool Write(absl::uint128) = delete;
  bool Write(float) = delete;
  bool Write(double) = delete;
  bool Write(long double) = delete;
  bool Write(bool) = delete;
  bool Write(wchar_t) = delete;
  bool Write(char16_t) = delete;
  bool Write(char32_t) = delete;

  // Called when nothing more will be digested. This can make `Digest()` more
  // efficient. Resources can be freed. Marks the digester as not open.
  //
  // Does nothing if the digester is not open.
  bool Close() { return methods()->close(target()); }

  // Returns an `absl::Status` describing the failure if the digester is
  // failed.
  //
  // Can return `absl::OkStatus()` if tracking the status is not supported.
  absl::Status status() const { return methods()->status(target()); }

 protected:
  ABSL_ATTRIBUTE_NORETURN static void FailedDigestMethodDefault();

 private:
  template <typename Function,
            std::enable_if_t<
                std::is_same_v<decltype(std::declval<Function&&>()()), bool>,
                int> = 0>
  static bool ConvertToBool(Function&& function) {
    return std::forward<Function>(function)();
  }
  template <
      typename Function,
      std::enable_if_t<std::is_void_v<decltype(std::declval<Function&&>()())>,
                       int> = 0>
  static bool ConvertToBool(Function&& function) {
    std::forward<Function>(function)();
    return true;
  }

  template <typename T, typename Enable = void>
  struct DigesterTargetHasSetWriteSizeHint : std::false_type {};

  template <typename T>
  struct DigesterTargetHasSetWriteSizeHint<
      T, std::void_t<decltype(std::declval<T&>().SetWriteSizeHint(
             std::declval<std::optional<Position>>()))>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct DigesterTargetHasWriteChain : std::false_type {};

  template <typename T>
  struct DigesterTargetHasWriteChain<
      T, std::void_t<decltype(std::declval<T&>().Write(
             std::declval<const Chain&>()))>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct DigesterTargetHasWriteCord : std::false_type {};

  template <typename T>
  struct DigesterTargetHasWriteCord<
      T, std::void_t<decltype(std::declval<T&>().Write(
             std::declval<const absl::Cord&>()))>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct DigesterTargetHasWriteByteFill : std::false_type {};

  template <typename T>
  struct DigesterTargetHasWriteByteFill<
      T,
      std::void_t<decltype(std::declval<T&>().Write(std::declval<ByteFill>()))>>
      : std::true_type {};

  template <typename T, typename Enable = void>
  struct DigesterTargetHasClose : std::false_type {};

  template <typename T>
  struct DigesterTargetHasClose<
      T, std::void_t<decltype(std::declval<T&>().Close())>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct DigesterTargetHasStatus : std::false_type {};

  template <typename T>
  struct DigesterTargetHasStatus<
      T, std::enable_if_t<std::is_convertible_v<
             decltype(std::declval<const T&>().status()), absl::Status>>>
      : std::true_type {};

  static void SetWriteSizeHintMethodDefault(
      TypeErasedRef target, std::optional<Position> write_size_hint);
  static bool WriteMethodDefault(TypeErasedRef target, absl::string_view src);
  static bool WriteChainMethodDefault(TypeErasedRef target, const Chain& src);
  static bool WriteCordMethodDefault(TypeErasedRef target,
                                     const absl::Cord& src);
  static bool WriteByteFillMethodDefault(TypeErasedRef target, ByteFill src);
  static bool CloseMethodDefault(TypeErasedRef target);
  static absl::Status StatusMethodDefault(TypeErasedRef target);

  template <
      typename T,
      std::enable_if_t<DigesterTargetHasSetWriteSizeHint<T>::value, int> = 0>
  static void SetWriteSizeHintMethod(TypeErasedRef target,
                                     std::optional<Position> write_size_hint) {
    target.Cast<T&>().SetWriteSizeHint(write_size_hint);
  }
  template <
      typename T,
      std::enable_if_t<!DigesterTargetHasSetWriteSizeHint<T>::value, int> = 0>
  static void SetWriteSizeHintMethod(
      ABSL_ATTRIBUTE_UNUSED TypeErasedRef target,
      ABSL_ATTRIBUTE_UNUSED std::optional<Position> write_size_hint) {}

  template <typename T>
  static auto RawWriteMethod(TypeErasedRef target, absl::string_view src) {
    return target.Cast<T&>().Write(src);
  }

  template <typename T>
  static bool WriteMethod(TypeErasedRef target, absl::string_view src) {
    return ConvertToBool([&] { return RawWriteMethod<T>(target, src); });
  }

  static bool WriteChainFallback(TypeErasedRef target, const Chain& src,
                                 bool (*write)(TypeErasedRef target,
                                               absl::string_view src));
  static void WriteChainFallback(TypeErasedRef target, const Chain& src,
                                 void (*write)(TypeErasedRef target,
                                               absl::string_view src));

  template <typename T,
            std::enable_if_t<DigesterTargetHasWriteChain<T>::value, int> = 0>
  static bool WriteChainMethod(TypeErasedRef target, const Chain& src) {
    return ConvertToBool([&] { return target.Cast<T&>().Write(src); });
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasWriteChain<T>::value, int> = 0>
  static bool WriteChainMethod(TypeErasedRef target, const Chain& src) {
    return ConvertToBool(
        [&] { return WriteChainFallback(target, src, RawWriteMethod<T>); });
  }

  static bool WriteCordFallback(TypeErasedRef target, const absl::Cord& src,
                                bool (*write)(TypeErasedRef target,
                                              absl::string_view src));
  static void WriteCordFallback(TypeErasedRef target, const absl::Cord& src,
                                void (*write)(TypeErasedRef target,
                                              absl::string_view src));

  template <typename T,
            std::enable_if_t<DigesterTargetHasWriteCord<T>::value, int> = 0>
  static bool WriteCordMethod(TypeErasedRef target, const absl::Cord& src) {
    return ConvertToBool([&] { return target.Cast<T&>().Write(src); });
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasWriteCord<T>::value, int> = 0>
  static bool WriteCordMethod(TypeErasedRef target, const absl::Cord& src) {
    return ConvertToBool(
        [&] { return WriteCordFallback(target, src, RawWriteMethod<T>); });
  }

  static bool WriteByteFillFallback(TypeErasedRef target, ByteFill src,
                                    bool (*write)(TypeErasedRef target,
                                                  absl::string_view src));
  static void WriteByteFillFallback(TypeErasedRef target, ByteFill src,
                                    void (*write)(TypeErasedRef target,
                                                  absl::string_view src));

  template <typename T,
            std::enable_if_t<DigesterTargetHasWriteByteFill<T>::value, int> = 0>
  static bool WriteByteFillMethod(TypeErasedRef target, ByteFill src) {
    return ConvertToBool([&] { return target.Cast<T&>().Write(src); });
  }
  template <typename T, std::enable_if_t<
                            !DigesterTargetHasWriteByteFill<T>::value, int> = 0>
  static bool WriteByteFillMethod(TypeErasedRef target, ByteFill src) {
    return ConvertToBool(
        [&] { return WriteByteFillFallback(target, src, RawWriteMethod<T>); });
  }

  template <typename T,
            std::enable_if_t<DigesterTargetHasClose<T>::value, int> = 0>
  static bool CloseMethod(TypeErasedRef target) {
    return ConvertToBool([&] { return target.Cast<T&>().Close(); });
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasClose<T>::value, int> = 0>
  static bool CloseMethod(ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return true;
  }

  template <typename T,
            std::enable_if_t<DigesterTargetHasStatus<T>::value, int> = 0>
  static absl::Status StatusMethod(TypeErasedRef target) {
    return target.Cast<const T&>().status();
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasStatus<T>::value, int> = 0>
  static absl::Status StatusMethod(ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return absl::OkStatus();
  }

 protected:
  struct Methods {
    void (*set_write_size_hint)(TypeErasedRef target,
                                std::optional<Position> write_size_hint);
    bool (*write)(TypeErasedRef target, absl::string_view src);
    bool (*write_chain)(TypeErasedRef target, const Chain& src);
    bool (*write_cord)(TypeErasedRef target, const absl::Cord& src);
    bool (*write_byte_fill)(TypeErasedRef target, ByteFill src);
    bool (*close)(TypeErasedRef target);
    absl::Status (*status)(TypeErasedRef target);
  };

  static constexpr Methods kMethodsDefault = {SetWriteSizeHintMethodDefault,
                                              WriteMethodDefault,
                                              WriteChainMethodDefault,
                                              WriteCordMethodDefault,
                                              WriteByteFillMethodDefault,
                                              CloseMethodDefault,
                                              StatusMethodDefault};

  template <typename T>
  static constexpr Methods kMethods = {SetWriteSizeHintMethod<T>,
                                       WriteMethod<T>,
                                       WriteChainMethod<T>,
                                       WriteCordMethod<T>,
                                       WriteByteFillMethod<T>,
                                       CloseMethod<T>,
                                       StatusMethod<T>};

  explicit DigesterBaseHandle(const Methods* methods, TypeErasedRef target)
      : methods_(methods), target_(target) {}

  const Methods* methods() const { return methods_; }
  TypeErasedRef target() const { return target_; }

 private:
  class DigesterStringifySink;

  const Methods* methods_ = &kMethodsDefault;
  TypeErasedRef target_;
};

namespace digester_handle_internal {

template <typename T, typename Enable = void>
struct DigestOfDigesterTarget {
  using type = void;
};

template <typename T>
struct DigestOfDigesterTarget<
    T, std::void_t<decltype(std::declval<T&>().Digest())>> {
  using type = decltype(std::declval<T&>().Digest());
};

}  // namespace digester_handle_internal

// `SupportsDigesterHandle<T, DigestType>::value` is `true` when `T&` is a valid
// constructor argument for `DigesterHandle<DigestType>`.

template <typename T, typename DigestType, typename Enable = void>
struct SupportsDigesterHandle : std::false_type {};

template <typename T, typename DigestType>
struct SupportsDigesterHandle<
    T, DigestType,
    std::enable_if_t<std::conjunction_v<
        SupportsDigesterBaseHandle<T>,
        HasDigestConverter<
            typename digester_handle_internal::DigestOfDigesterTarget<T>::type,
            DigestType>>>> : std::true_type {};

// `DigesterHandle<DigestType>` extends `DigesterBaseHandle` with `Digest()`
// returning some data of type `DigestType` called a digest, e.g. a checksum.
//
// The digester should support:
//
// ```
//   // Returns the digest of data written so far. Its type and meaning depends
//   // on the concrete digester. Unchanged by `Close()`.
//   //
//   // `OriginalDigestType` can be any type convertible to `DigestType` using
//   // `DigestConverter`.
//   //
//   // Depending on the digester, `Digest()` can be more efficient if `Close()`
//   // is called before.
//   //
//   // Many digesters support calling `Digest()` and then accepting more data
//   // or calling `Digest()` again, but this is not guaranteed.
//   OriginalDigestType Digest();
// ```
//
// `DigestType` can be `void` for digesters used for their side effects.
template <typename DigestTypeParam>
class ABSL_NULLABILITY_COMPATIBLE DigesterHandle : public DigesterBaseHandle {
 private:
  using pointer = void*;  // For `ABSL_NULLABILITY_COMPATIBLE`.

 public:
  // The type of the digest.
  using DigestType = DigestTypeParam;

  // Creates a `DigesterHandle` which does not refer to a target.
  DigesterHandle() noexcept
      : DigesterBaseHandle(&kMethodsDefault, TypeErasedRef()) {}
  /*implicit*/ DigesterHandle(std::nullptr_t) : DigesterHandle() {}

  // Creates a `DigesterHandle` which refers to `target`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<DigesterHandle, T&>,
                                   SupportsDigesterHandle<T, DigestType>>,
                int> = 0>
  /*implicit*/ DigesterHandle(T& target ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : DigesterBaseHandle(&kMethods<T>, TypeErasedRef(target)) {}

  DigesterHandle(const DigesterHandle& that) = default;
  DigesterHandle& operator=(const DigesterHandle& that) = default;

  // Returns the digest of data written so far. Its type and meaning depends on
  // the concrete digester. Unchanged by `Close()`.
  //
  // The digest is converted to `DesiredDigestType` using `DigestConverter`.
  template <
      typename DesiredDigestType = DigestType,
      std::enable_if_t<HasDigestConverter<DigestType, DesiredDigestType>::value,
                       int> = 0>
  DesiredDigestType Digest() {
    return ConvertDigest<DesiredDigestType>(
        [&]() -> DigestType { return methods()->digest(target()); });
  }

 private:
  template <typename T, typename Enable = void>
  struct DigesterTargetHasDigest : std::false_type {};

  template <typename T>
  struct DigesterTargetHasDigest<
      T, std::void_t<decltype(std::declval<T&>().Digest())>> : std::true_type {
  };

  template <typename DependentDigestType = DigestType,
            std::enable_if_t<std::is_void_v<DependentDigestType>, int> = 0>
  static DigestType DigestMethodDefault(
      ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {}
  template <typename DependentDigestType = DigestType,
            std::enable_if_t<!std::is_void_v<DependentDigestType>, int> = 0>
  static DigestType DigestMethodDefault(
      ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    FailedDigestMethodDefault();
  }

  template <typename T,
            std::enable_if_t<DigesterTargetHasDigest<T>::value, int> = 0>
  static DigestType DigestMethod(TypeErasedRef target) {
    return ConvertDigest<DigestType>(
        [&]() -> decltype(auto) { return target.Cast<T&>().Digest(); });
  }
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<std::negation<DigesterTargetHasDigest<T>>,
                                   std::is_void<DigestType>>,
                int> = 0>
  static DigestType DigestMethod(ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {}

  struct Methods : DigesterBaseHandle::Methods {
    // MSVC does not like the `DigestType` alias here for some reason.
    DigestTypeParam (*digest)(TypeErasedRef target);
  };

  static constexpr Methods kMethodsDefault = {
      DigesterBaseHandle::kMethodsDefault, DigestMethodDefault};

  template <typename T>
  static constexpr Methods kMethods = {DigesterBaseHandle::kMethods<T>,
                                       DigestMethod<T>};

  const Methods* methods() const {
    return static_cast<const Methods*>(DigesterBaseHandle::methods());
  }
};

DigesterHandle() -> DigesterHandle<DeleteCtad<>>;
DigesterHandle(std::nullptr_t) -> DigesterHandle<DeleteCtad<std::nullptr_t>>;
template <typename T,
          std::enable_if_t<SupportsDigesterBaseHandle<T>::value, int> = 0>
explicit DigesterHandle(T& target) -> DigesterHandle<
    typename digester_handle_internal::DigestOfDigesterTarget<T>::type>;

// Specialization of `DependencyImpl<DigesterBaseHandle, Manager>` when
// `DependencyManagerRef<Manager>` is a valid digester target.
//
// Specialized separately for `get()` to return `DigesterHandle<DigestType>`.
template <typename Manager>
class DependencyImpl<
    DigesterBaseHandle, Manager,
    std::enable_if_t<std::conjunction_v<
        std::is_pointer<DependencyManagerPtr<Manager>>,
        SupportsDigesterBaseHandle<DependencyManagerRef<Manager>>>>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  // Returns `DigesterHandle<DigestType>` rather than `DigesterBaseHandle`.
  DigesterHandle<typename digester_handle_internal::DigestOfDigesterTarget<
      DependencyManagerRef<Manager>>::type>
  get() const {
    return DigesterHandle<
        typename digester_handle_internal::DigestOfDigesterTarget<
            DependencyManagerRef<Manager>>::type>(*this->ptr());
  }

 protected:
  DependencyImpl(const DependencyImpl& that) = default;
  DependencyImpl& operator=(const DependencyImpl& that) = default;

  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// The type of the digest returned by the digester provided by `Digester`.
template <typename Digester>
using DigestOf =
    typename Dependency<DigesterBaseHandle, Digester>::Subhandle::DigestType;

// Type-erased digester returning a digest of type `DigestType`.
template <typename DigestType>
using AnyDigester = Any<DigesterHandle<DigestType>>;

// Implementation details follow.

class DigesterBaseHandle::DigesterStringifySink {
 public:
  explicit DigesterStringifySink(DigesterBaseHandle digester)
      : digester_(digester) {}

  void Append(size_t length, char fill) {
    if (ABSL_PREDICT_FALSE(!digester_.Write(ByteFill(length, fill)))) {
      ok_ = false;
    }
  }
  void Append(absl::string_view src) {
    if (ABSL_PREDICT_FALSE(!digester_.Write(src))) ok_ = false;
  }
  friend void AbslFormatFlush(DigesterStringifySink* dest,
                              absl::string_view src) {
    dest->Append(src);
  }

  bool ok() const { return ok_; }

 private:
  DigesterBaseHandle digester_;
  bool ok_ = true;
};

template <typename Src,
          std::enable_if_t<
              std::conjunction_v<
                  absl::HasAbslStringify<Src>,
                  std::negation<std::is_convertible<Src&&, BytesRef>>,
                  std::negation<std::is_convertible<Src&&, const Chain&>>,
                  std::negation<std::is_convertible<Src&&, const absl::Cord&>>,
                  std::negation<std::is_convertible<Src&&, ByteFill>>>,
              int>>
bool DigesterBaseHandle::Write(Src&& src) {
  DigesterStringifySink sink(*this);
  AbslStringify(sink, std::forward<Src>(src));
  return sink.ok();
}

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_DIGESTER_HANDLE_H_
