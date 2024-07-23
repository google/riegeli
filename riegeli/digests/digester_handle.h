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
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/any.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/dependency_manager.h"
#include "riegeli/base/to_string_view.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/digests/digest_converter.h"

namespace riegeli {

// `IsValidDigesterBaseTarget<T>::value` is `true` when `T*` is a valid
// constructor argument for `DigesterBaseHandle`.

template <typename T, typename Enable = void>
struct IsValidDigesterBaseTarget : std::false_type {};

template <typename T>
struct IsValidDigesterBaseTarget<
    T, std::enable_if_t<absl::conjunction<
           absl::negation<std::is_const<T>>,
           std::is_void<absl::void_t<decltype(std::declval<T&>().Write(
               std::declval<absl::string_view>()))>>>::value>>
    : std::true_type {};

// Type-erased pointer to a target object called a digester, which observes data
// being read or written.
//
// The target should support:
//
// ```
//   // All of the following methods returning `bool` return `true` on success.
//   // They may also return `void` which is treated as `true`.
//
//   // If `write_size_hint` is not `absl::nullopt`, hints that this amount of
//   // data will be written from the current position. This may improve
//   // performance and memory usage.
//   //
//   // If the hint turns out to not match reality, nothing breaks. It is better
//   // if `write_size_hint` is slightly too large than slightly too small.
//   //
//   // Optional. If absent, does nothing.
//   void SetWriteSizeHint(absl::optional<Position> write_size_hint);
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
//   bool WriteZeros(Position length);
//
//   // Called when nothing more will be digested. This can make `Digest()` more
//   // efficient. Resources can be freed. Marks the digester as not open.
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
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
        DigesterBaseHandle {
 public:
  // Creates a `DigesterBaseHandle` which does not point to a target.
  DigesterBaseHandle() = default;
  /*implicit*/ DigesterBaseHandle(std::nullptr_t) noexcept {}

  // Creates a `DigesterBaseHandle` which points to `target`.
  template <typename T,
            std::enable_if_t<IsValidDigesterBaseTarget<T>::value, int> = 0>
  explicit DigesterBaseHandle(T* target)
      : methods_(&kMethods<T>), target_(target) {}

  DigesterBaseHandle(const DigesterBaseHandle& that) = default;
  DigesterBaseHandle& operator=(const DigesterBaseHandle& that) = default;

  friend bool operator==(DigesterBaseHandle a, DigesterBaseHandle b) {
    return a.target() == b.target();
  }

  // Allow Nullability annotations on `DigesterBaseHandle`.
  using absl_nullability_compatible = void;

  // If `write_size_hint` is not `absl::nullopt`, hints that this amount of data
  // will be written from the current position. This may improve performance and
  // memory usage.
  //
  // If the hint turns out to not match reality, nothing breaks. It is better if
  // `write_size_hint` is slightly too large than slightly too small.
  void SetWriteSizeHint(absl::optional<Position> write_size_hint) {
    methods_->set_write_size_hint(target_, write_size_hint);
  }

  // Called with consecutive fragments of data.
  //
  // Precondition: the digester is open.
  bool Write(char src) { return Write(absl::string_view(&src, 1)); }
#if __cpp_char8_t
  bool Write(char8_t src) { return Write(static_cast<char>(src)); }
#endif
  bool Write(absl::string_view src) { return methods()->write(target(), src); }
  template <typename Src,
            std::enable_if_t<SupportsToStringView<Src>::value, int> = 0>
  bool Write(Src&& src) {
    return Write(riegeli::ToStringView(src));
  }
  bool Write(const Chain& src) { return methods()->write_chain(target(), src); }
  bool Write(const absl::Cord& src) {
    return methods()->write_cord(target(), src);
  }
  template <
      typename Src,
      std::enable_if_t<
          absl::conjunction<
              HasAbslStringify<Src>, absl::negation<SupportsToStringView<Src>>,
              absl::negation<std::is_convertible<Src&&, const Chain&>>,
              absl::negation<std::is_convertible<Src&&, const absl::Cord&>>>::
              value,
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

  // Can be called instead of `Write()` when data consists of zeros.
  //
  // Precondition: the digester is open.
  bool WriteZeros(riegeli::Position length) const {
    return methods()->write_zeros(target(), length);
  }

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

 private:
  template <
      typename Function,
      std::enable_if_t<
          std::is_same<decltype(std::declval<Function&&>()()), bool>::value,
          int> = 0>
  static bool ConvertToBool(Function&& function) {
    return std::forward<Function>(function)();
  }
  template <
      typename Function,
      std::enable_if_t<
          std::is_void<decltype(std::declval<Function&&>()())>::value, int> = 0>
  static bool ConvertToBool(Function&& function) {
    std::forward<Function>(function)();
    return true;
  }

  template <typename T, typename Enable = void>
  struct DigesterTargetHasSetWriteSizeHint : std::false_type {};

  template <typename T>
  struct DigesterTargetHasSetWriteSizeHint<
      T, absl::void_t<decltype(std::declval<T&>().SetWriteSizeHint(
             std::declval<absl::optional<Position>>()))>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct DigesterTargetHasWriteChain : std::false_type {};

  template <typename T>
  struct DigesterTargetHasWriteChain<
      T, absl::void_t<decltype(std::declval<T&>().Write(
             std::declval<const Chain&>()))>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct DigesterTargetHasWriteCord : std::false_type {};

  template <typename T>
  struct DigesterTargetHasWriteCord<
      T, absl::void_t<decltype(std::declval<T&>().Write(
             std::declval<const absl::Cord&>()))>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct DigesterTargetHasWriteZeros : std::false_type {};

  template <typename T>
  struct DigesterTargetHasWriteZeros<
      T, absl::void_t<decltype(std::declval<T&>().WriteZeros(
             std::declval<Position>()))>> : std::true_type {};

  template <typename T, typename Enable = void>
  struct DigesterTargetHasClose : std::false_type {};

  template <typename T>
  struct DigesterTargetHasClose<
      T, absl::void_t<decltype(std::declval<T&>().Close())>> : std::true_type {
  };

  template <typename T, typename Enable = void>
  struct DigesterTargetHasStatus : std::false_type {};

  template <typename T>
  struct DigesterTargetHasStatus<
      T, std::enable_if_t<std::is_convertible<
             decltype(std::declval<const T&>().status()), absl::Status>::value>>
      : std::true_type {};

  template <
      typename T,
      std::enable_if_t<DigesterTargetHasSetWriteSizeHint<T>::value, int> = 0>
  static void SetWriteSizeHintMethod(void* target,
                                     absl::optional<Position> write_size_hint) {
    static_cast<T*>(target)->SetWriteSizeHint(write_size_hint);
  }
  template <
      typename T,
      std::enable_if_t<!DigesterTargetHasSetWriteSizeHint<T>::value, int> = 0>
  static void SetWriteSizeHintMethod(
      ABSL_ATTRIBUTE_UNUSED void* target,
      ABSL_ATTRIBUTE_UNUSED absl::optional<Position> write_size_hint) {}

  template <typename T>
  static auto RawWriteMethod(void* target, absl::string_view src) {
    return static_cast<T*>(target)->Write(src);
  }

  template <typename T>
  static bool WriteMethod(void* target, absl::string_view src) {
    return ConvertToBool([&] { return RawWriteMethod<T>(target, src); });
  }

  static bool WriteChainFallback(void* target, const Chain& src,
                                 bool (*write)(void* target,
                                               absl::string_view src));
  static void WriteChainFallback(void* target, const Chain& src,
                                 void (*write)(void* target,
                                               absl::string_view src));

  template <typename T,
            std::enable_if_t<DigesterTargetHasWriteChain<T>::value, int> = 0>
  static bool WriteChainMethod(void* target, const Chain& src) {
    return ConvertToBool([&] { return static_cast<T*>(target)->Write(src); });
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasWriteChain<T>::value, int> = 0>
  static bool WriteChainMethod(void* target, const Chain& src) {
    return ConvertToBool(
        [&] { return WriteChainFallback(target, src, RawWriteMethod<T>); });
  }

  static bool WriteCordFallback(void* target, const absl::Cord& src,
                                bool (*write)(void* target,
                                              absl::string_view src));
  static void WriteCordFallback(void* target, const absl::Cord& src,
                                void (*write)(void* target,
                                              absl::string_view src));

  template <typename T,
            std::enable_if_t<DigesterTargetHasWriteCord<T>::value, int> = 0>
  static bool WriteCordMethod(void* target, const absl::Cord& src) {
    return ConvertToBool([&] { return static_cast<T*>(target)->Write(src); });
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasWriteCord<T>::value, int> = 0>
  static bool WriteCordMethod(void* target, const absl::Cord& src) {
    return ConvertToBool(
        [&] { return WriteCordFallback(target, src, RawWriteMethod<T>); });
  }

  static bool WriteZerosFallback(void* target, Position length,
                                 bool (*write)(void* target,
                                               absl::string_view src));
  static void WriteZerosFallback(void* target, Position length,
                                 void (*write)(void* target,
                                               absl::string_view src));

  template <typename T,
            std::enable_if_t<DigesterTargetHasWriteZeros<T>::value, int> = 0>
  static bool WriteZerosMethod(void* target, Position length) {
    return ConvertToBool(
        [&] { return static_cast<T*>(target)->WriteZeros(length); });
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasWriteZeros<T>::value, int> = 0>
  static bool WriteZerosMethod(void* target, Position length) {
    return ConvertToBool(
        [&] { return WriteZerosFallback(target, length, RawWriteMethod<T>); });
  }

  template <typename T,
            std::enable_if_t<DigesterTargetHasClose<T>::value, int> = 0>
  static bool CloseMethod(void* target) {
    return ConvertToBool([&] { return static_cast<T*>(target)->Close(); });
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasClose<T>::value, int> = 0>
  static bool CloseMethod(ABSL_ATTRIBUTE_UNUSED void* target) {
    return true;
  }

  template <typename T,
            std::enable_if_t<DigesterTargetHasStatus<T>::value, int> = 0>
  static absl::Status StatusMethod(const void* target) {
    return static_cast<const T*>(target)->status();
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasStatus<T>::value, int> = 0>
  static absl::Status StatusMethod(ABSL_ATTRIBUTE_UNUSED const void* target) {
    return absl::OkStatus();
  }

 protected:
  struct Methods {
    void (*set_write_size_hint)(void* target,
                                absl::optional<Position> write_size_hint);
    bool (*write)(void* target, absl::string_view src);
    bool (*write_chain)(void* target, const Chain& src);
    bool (*write_cord)(void* target, const absl::Cord& src);
    bool (*write_zeros)(void* target, riegeli::Position length);
    bool (*close)(void* target);
    absl::Status (*status)(const void* target);
  };

  template <typename T>
  static constexpr Methods kMethods = {SetWriteSizeHintMethod<T>,
                                       WriteMethod<T>,
                                       WriteChainMethod<T>,
                                       WriteCordMethod<T>,
                                       WriteZerosMethod<T>,
                                       CloseMethod<T>,
                                       StatusMethod<T>};

  template <typename T>
  explicit DigesterBaseHandle(const Methods* methods, T* target)
      : methods_(methods), target_(target) {}

  const Methods* methods() const { return methods_; }
  void* target() const { return target_; }

 private:
  class DigesterAbslStringifySink;

  const Methods* methods_ = nullptr;
  void* target_ = nullptr;
};

namespace digester_handle_internal {

template <typename T, typename Enable = void>
struct DigestOfDigesterTarget {
  using type = void;
};

template <typename T>
struct DigestOfDigesterTarget<
    T, absl::void_t<decltype(std::declval<T&>().Digest())>> {
  using type = decltype(std::declval<T&>().Digest());
};

}  // namespace digester_handle_internal

// `IsValidDigesterTarget<T, DigestType>::value` is `true` when `T*` is a valid
// constructor argument for `DigesterHandle<DigestType>`.

template <typename T, typename DigestType, typename Enable = void>
struct IsValidDigesterTarget : std::false_type {};

template <typename T, typename DigestType>
struct IsValidDigesterTarget<
    T, DigestType,
    std::enable_if_t<absl::conjunction<
        IsValidDigesterBaseTarget<T>,
        HasDigestConverter<
            typename digester_handle_internal::DigestOfDigesterTarget<T>::type,
            DigestType>>::value>> : std::true_type {};

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
//   OriginalDigestType Digest();
// ```
//
// `DigestType` can be `void` for digesters used for their side effects.
template <typename DigestTypeParam>
class DigesterHandle : public DigesterBaseHandle {
 public:
  // The type of the digest.
  using DigestType = DigestTypeParam;

  // Creates a `DigesterHandle` which does not point to a target.
  DigesterHandle() = default;
  /*implicit*/ DigesterHandle(std::nullptr_t) noexcept {}

  // Creates a `DigesterHandle` which points to `target`.
  template <
      typename T,
      std::enable_if_t<IsValidDigesterTarget<T, DigestType>::value, int> = 0>
  explicit DigesterHandle(T* target)
      : DigesterBaseHandle(&kMethods<T>, target) {}

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
      T, absl::void_t<decltype(std::declval<T&>().Digest())>> : std::true_type {
  };

  template <typename T,
            std::enable_if_t<DigesterTargetHasDigest<T>::value, int> = 0>
  static DigestType DigestMethod(void* target) {
    return ConvertDigest<DigestType>(
        [&]() -> decltype(auto) { return static_cast<T*>(target)->Digest(); });
  }
  template <typename T,
            std::enable_if_t<!DigesterTargetHasDigest<T>::value, int> = 0>
  static DigestType DigestMethod(ABSL_ATTRIBUTE_UNUSED void* target) {
    return ConvertDigest<DigestType>([] {});
  }

  struct Methods : DigesterBaseHandle::Methods {
    // MSVC does not like the `DigestType` alias here for some reason.
    DigestTypeParam (*digest)(void* target);
  };

#if __cpp_aggregate_bases
  template <typename T>
  static constexpr Methods kMethods = {DigesterBaseHandle::kMethods<T>,
                                       DigestMethod<T>};
#else
  template <typename T>
  static constexpr Methods MakeMethods() {
    Methods methods;
    static_cast<DigesterBaseHandle::Methods&>(methods) =
        DigesterBaseHandle::kMethods<T>;
    methods.digest = DigestMethod<T>;
    return methods;
  }
  template <typename T>
  static constexpr Methods kMethods = MakeMethods<T>();
#endif

  const Methods* methods() const {
    return static_cast<const Methods*>(DigesterBaseHandle::methods());
  }
};

// Support CTAD.
#if __cpp_deduction_guides
DigesterHandle() -> DigesterHandle<DeleteCtad<>>;
DigesterHandle(std::nullptr_t) -> DigesterHandle<DeleteCtad<std::nullptr_t>>;
template <typename T,
          std::enable_if_t<IsValidDigesterBaseTarget<T>::value, int> = 0>
explicit DigesterHandle(T* target)
    -> DigesterHandle<
        typename digester_handle_internal::DigestOfDigesterTarget<T>::type>;
#endif

// Specialization of `DependencyImpl<DigesterBaseHandle, Manager>` when
// `DependencyManagerRef<Manager>` is a valid digester target.
//
// Specialized separately for `get()` to return `DigesterHandle<DigestType>`.
//
// The case when `DependencyManagerRef<Manager>` is a `DigesterBaseHandle`
// itself is excluded by this specialization because it is handled by
// `Dependency`, letting `get()` return `*ptr()` rather than
// `DigesterHandle(ptr())` and thus avoiding wrapping the handle again.
//
// The case when `DependencyManagerPtr<Manager>` is a `DigesterBaseHandle`
// itself is handled by `Dependency`.
template <typename Manager>
class DependencyImpl<
    DigesterBaseHandle, Manager,
    std::enable_if_t<absl::conjunction<
        std::is_pointer<DependencyManagerPtr<Manager>>,
        absl::negation<std::is_convertible<DependencyManagerRef<Manager>*,
                                           DigesterBaseHandle*>>,
        IsValidDigesterBaseTarget<DependencyManagerRef<Manager>>>::value>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  // Returns `DigesterHandle<DigestType>` rather than `DigesterBaseHandle`.
  DigesterHandle<typename digester_handle_internal::DigestOfDigesterTarget<
      DependencyManagerRef<Manager>>::type>
  get() const {
    return DigesterHandle<
        typename digester_handle_internal::DigestOfDigesterTarget<
            DependencyManagerRef<Manager>>::type>(this->ptr());
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

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename T>
constexpr DigesterBaseHandle::Methods DigesterBaseHandle::kMethods;
template <typename DigestTypeParam>
template <typename T>
constexpr typename DigesterHandle<DigestTypeParam>::Methods
    DigesterHandle<DigestTypeParam>::kMethods;
#endif

class DigesterBaseHandle::DigesterAbslStringifySink {
 public:
  explicit DigesterAbslStringifySink(DigesterBaseHandle digester)
      : digester_(digester) {}

  void Append(size_t length, char src);
  void Append(absl::string_view src) {
    if (ABSL_PREDICT_FALSE(!digester_.Write(src))) ok_ = false;
  }
  friend void AbslFormatFlush(DigesterAbslStringifySink* dest,
                              absl::string_view src) {
    dest->Append(src);
  }

  bool ok() const { return ok_; }

 private:
  DigesterBaseHandle digester_;
  bool ok_ = true;
};

template <
    typename Src,
    std::enable_if_t<
        absl::conjunction<
            HasAbslStringify<Src>, absl::negation<SupportsToStringView<Src>>,
            absl::negation<std::is_convertible<Src&&, const Chain&>>,
            absl::negation<std::is_convertible<Src&&, const absl::Cord&>>>::
            value,
        int>>
inline bool DigesterBaseHandle::Write(Src&& src) {
  DigesterAbslStringifySink sink(*this);
  AbslStringify(sink, std::forward<Src>(src));
  return sink.ok();
}

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_DIGESTER_HANDLE_H_
