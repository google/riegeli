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

#ifndef RIEGELI_BASE_DEBUG_H_
#define RIEGELI_BASE_DEBUG_H_

#include <stddef.h>

#include <cstddef>
#include <ios>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/strings/has_absl_stringify.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/stream_utils.h"

namespace riegeli {

class DebugStream;

namespace debug_internal {

template <typename T, typename Enable = void>
struct HasRiegeliDebug : std::false_type {};

template <typename T>
struct HasRiegeliDebug<
    T, std::void_t<decltype(RiegeliDebug(std::declval<const T&>(),
                                         std::declval<DebugStream&>()))>>
    : std::true_type {};

template <typename T, typename Enable = void>
struct HasDebugString : std::false_type {};

template <typename T>
struct HasDebugString<
    T,
    std::enable_if_t<std::is_convertible_v<
        decltype(std::declval<const T&>().DebugString()), absl::string_view>>>
    : std::true_type {};

template <typename T, typename Enable = void>
struct HasOperatorOutput : std::false_type {};

template <typename T>
struct HasOperatorOutput<T, std::void_t<decltype(std::declval<std::ostream&>()
                                                 << std::declval<T>())>>
    : std::true_type {};

}  // namespace debug_internal

// `SupportsDebug<T>::value` is `true` if `T` supports `riegeli::Debug()`:
// writing the value in a format suitable for error messages.
//
// The value is generally written in a way which reflects as much as is compared
// by `operator==`, without indicating the type nor internal structure, using
// syntax similar to C++ expressions.
template <typename T>
struct SupportsDebug
    : std::disjunction<
          debug_internal::HasRiegeliDebug<T>, debug_internal::HasDebugString<T>,
          absl::HasAbslStringify<T>, debug_internal::HasOperatorOutput<T>> {};

// To customize `riegeli::Debug()` for a class `T`, define a free function
// `friend void RiegeliDebug(const T& src, DebugStream& dest)` as a friend of
// `T` inside class definition or in the same namespace as `T`, so that it can
// be found via ADL. `DebugStream` in the parameter type can also be a template
// parameter to reduce library dependencies.
//
// `riegeli::Debug(src)` uses the first defined form among the following:
//  * `RiegeliDebug(src, dest)`
//  * `src.DebugString()`
//  * `dest << src`
//  * `AbslStringify(dest, src)`
class DebugStream {
 public:
  // A default-constructible type used to maintain the state between calls to
  // `DebugStringFragment()`.
  class EscapeState;

  // Will write to `dest`.
  explicit DebugStream(std::ostream* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(dest) {}

  DebugStream(const DebugStream& that) = default;
  DebugStream& operator=(const DebugStream& that) = default;

  // Writes a character using `std::ostream::write()`.
  void Write(char src) { dest_->write(&src, 1); }

  // Writes a string using `std::ostream::write()`.
  void Write(absl::string_view src) {
    dest_->write(src.data(), static_cast<std::streamsize>(src.size()));
  }

  // Writes a value formatted using `operator<<`.
  //
  // Using stream manipulators is supported, but if the stream state is not
  // reset to the default before calling `Debug()`, then the results can be
  // inconsistent, depending on the type being written.
  template <typename T>
  DebugStream& operator<<(T&& src) {
    *dest_ << std::forward<T>(src);
    return *this;
  }

  // Writes a value in a format suitable for error messages. This calls the
  // first defined form among the following:
  //  * `RiegeliDebug(src, *this)`
  //  * `Write(src.DebugString())`
  //  * `AbslStringify(sink, src)` for `OStreamAbslStringifySink(dest)`
  //  * `*dest << src`
  //
  // This is used to implement `riegeli::Debug()`, and to write subobjects by
  // implementations of `RiegeliDebug()` for objects containing them.
  template <typename T, std::enable_if_t<
                            debug_internal::HasRiegeliDebug<T>::value, int> = 0>
  void Debug(const T& src) {
    RiegeliDebug(src, *this);
  }
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<std::negation<debug_internal::HasRiegeliDebug<T>>,
                             debug_internal::HasDebugString<T>>,
          int> = 0>
  void Debug(const T& src) {
    Write(src.DebugString());
  }
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<std::negation<debug_internal::HasRiegeliDebug<T>>,
                             std::negation<debug_internal::HasDebugString<T>>,
                             debug_internal::HasOperatorOutput<T>>,
          int> = 0>
  void Debug(const T& src) {
    *dest_ << src;
  }
  template <
      typename T,
      std::enable_if_t<std::conjunction_v<
                           std::negation<debug_internal::HasRiegeliDebug<T>>,
                           std::negation<debug_internal::HasDebugString<T>>,
                           std::negation<debug_internal::HasOperatorOutput<T>>,
                           absl::HasAbslStringify<T>>,
                       int> = 0>
  void Debug(const T& src) {
    OStreamAbslStringifySink sink(dest_);
    AbslStringify(sink, src);
  }

  // To implement `RiegeliDebug()` for string-like types which are not
  // represented as one fragment, the following pattern can be used:
  //
  // ```
  //   dest.DebugStringQuote();
  //   EscapeState escape_state;
  //   for (const absl::string_view fragment : fragments) {
  //     dest.DebugStringFragment(fragment, escape_state);
  //   }
  //   dest.DebugStringQuote();
  // ```
  //
  // If the representation is always flat, relying on `Debug()` for
  // `absl::string_view` is sufficient.
  void DebugStringQuote() { Write('"'); }
  void DebugStringFragment(absl::string_view src, EscapeState& escape_state);

 private:
  std::ostream* dest_;
};

// The following overloads cover supported types which do not define
// `RiegeliDebug()` themselves.

// `bool` is written as `true` or `false`.
void RiegeliDebug(bool src, DebugStream& dest);

// Non-bool and non-character numeric types, including `signed char` and
// `unsigned char`, are written as numbers.
inline void RiegeliDebug(signed char src, DebugStream& dest) {
  dest << int{src};
}
inline void RiegeliDebug(unsigned char src, DebugStream& dest) {
  dest << unsigned{src};
}
inline void RiegeliDebug(short src, DebugStream& dest) { dest << src; }
inline void RiegeliDebug(unsigned short src, DebugStream& dest) { dest << src; }
inline void RiegeliDebug(int src, DebugStream& dest) { dest << src; }
inline void RiegeliDebug(unsigned src, DebugStream& dest) { dest << src; }
inline void RiegeliDebug(long src, DebugStream& dest) { dest << src; }
inline void RiegeliDebug(unsigned long src, DebugStream& dest) { dest << src; }
inline void RiegeliDebug(long long src, DebugStream& dest) { dest << src; }
inline void RiegeliDebug(unsigned long long src, DebugStream& dest) {
  dest << src;
}
inline void RiegeliDebug(float src, DebugStream& dest) { dest << src; }
inline void RiegeliDebug(double src, DebugStream& dest) { dest << src; }
inline void RiegeliDebug(long double src, DebugStream& dest) { dest << src; }

// Character types are written in C++ character literal format.
void RiegeliDebug(char src, DebugStream& dest);
void RiegeliDebug(wchar_t src, DebugStream& dest);
#if __cpp_char8_t
void RiegeliDebug(char8_t src, DebugStream& dest);
#endif
void RiegeliDebug(char16_t src, DebugStream& dest);
void RiegeliDebug(char32_t src, DebugStream& dest);

// Enumeration types are written like their underlying types.
template <typename T, std::enable_if_t<std::is_enum_v<T>, int> = 0>
void RiegeliDebug(T src, DebugStream& dest) {
  dest.Debug(static_cast<std::underlying_type_t<T>>(src));
}

// `absl::string_view` is written in C++ string literal format.
//
// This covers types implicitly convertible to `absl::string_view` like
// `std::string` and `CompactString`.
void RiegeliDebug(absl::string_view src, DebugStream& dest);

void RiegeliDebug(std::wstring_view src, DebugStream& dest);
#if __cpp_char8_t
void RiegeliDebug(std::u8string_view src, DebugStream& dest);
#endif
void RiegeliDebug(std::u16string_view src, DebugStream& dest);
void RiegeliDebug(std::u32string_view src, DebugStream& dest);

// `absl::Cord` is written in C++ string literal format.
void RiegeliDebug(const absl::Cord& src, DebugStream& dest);

// A null pointer is written as "nullptr". Other data pointers, including char
// pointers, as well as function pointers, are written using `operator<<` for
// `const void*`.
void RiegeliDebug(std::nullptr_t src, DebugStream& dest);
void RiegeliDebug(const void* src, DebugStream& dest);
template <typename T, std::enable_if_t<std::is_function_v<T>, int> = 0>
void RiegeliDebug(T* src, DebugStream& dest) {
  dest.Debug(reinterpret_cast<void*>(src));
}

// `std::unique_ptr` and `std::shared_ptr` are written like pointers.
template <typename T, typename Deleter>
void RiegeliDebug(const std::unique_ptr<T, Deleter>& src, DebugStream& dest) {
  dest.Debug(src.get());
}
template <typename T>
void RiegeliDebug(const std::shared_ptr<T>& src, DebugStream& dest) {
  dest.Debug(src.get());
}

// `std::optional` values are written as "nullopt" when absent, or as the
// underlying data wrapped in braces when present.
void RiegeliDebug(std::nullopt_t src, DebugStream& dest);
template <typename T, std::enable_if_t<SupportsDebug<T>::value, int> = 0>
void RiegeliDebug(const std::optional<T>& src, DebugStream& dest) {
  if (src == std::nullopt) {
    dest.Debug(std::nullopt);
  } else {
    dest.Write('{');
    dest.Debug(*src);
    dest.Write('}');
  }
}

// The type returned by `riegeli::Debug()`.
template <typename T>
class DebugType {
 public:
  template <typename DependentT = T,
            std::enable_if_t<!std::is_rvalue_reference_v<DependentT>, int> = 0>
  explicit DebugType(const T& src) : src_(src) {}
  template <typename DependentT = T,
            std::enable_if_t<!std::is_lvalue_reference_v<DependentT>, int> = 0>
  explicit DebugType(T&& src) : src_(std::forward<T>(src)) {}

  DebugType(const DebugType& that) = default;
  DebugType& operator=(const DebugType& that) = default;

  DebugType(DebugType&& that) = default;
  DebugType& operator=(DebugType&& that) = default;

  template <typename Sink>
  friend void AbslStringify(Sink& dest, const DebugType& src) {
    AbslStringifyOStream stream(&dest);
    DebugStream(&stream).Debug(src.src_);
  }

  // Faster implementation if `Sink` is `OStreamAbslStringifySink`.
  friend void AbslStringify(OStreamAbslStringifySink& dest,
                            const DebugType& src) {
    DebugStream(dest.dest()).Debug(src.src_);
  }

  friend std::ostream& operator<<(std::ostream& dest, const DebugType& src) {
    DebugStream(&dest).Debug(src.src_);
    return dest;
  }

  std::string ToString() const {
    std::string dest;
    StringOStream stream(&dest);
    DebugStream(&stream).Debug(src_);
    return dest;
  }

 private:
  T src_;
};

template <typename T>
explicit DebugType(T&& src) -> DebugType<std::decay_t<T>>;

// `riegeli::Debug()` wraps an object such that it is formatted using
// `DebugStream::Debug()` when explicitly converted to `std::string` or written
// using `AbslStringify()` or `operator<<`.
//
// `riegeli::Debug()` does not own the object, even if it involves temporaries,
// hence it should be stringified by the same expression which constructed it,
// so that the temporaries outlive its usage. For storing a `DebugType` in a
// variable or returning it from a function, construct `DebugType` directly.
template <typename T, std::enable_if_t<SupportsDebug<T>::value, int> = 0>
inline DebugType<const T&> Debug(const T& src ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return DebugType<const T&>(src);
}

// Implementation details follow.

namespace debug_internal {

enum class LastEscape {
  kNormal,  // The following conditions do not apply.
  kZero,    // The last character was `\0`.
  kHex,     // The last character was written with `\x`.
};

}  // namespace debug_internal

class DebugStream::EscapeState {
 public:
  EscapeState() = default;

  EscapeState(const EscapeState& that) = default;
  EscapeState& operator=(const EscapeState& that) = default;

 private:
  friend class DebugStream;

  debug_internal::LastEscape last_escape_ = debug_internal::LastEscape::kNormal;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_DEBUG_H_
