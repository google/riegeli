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

#ifndef RIEGELI_DIGESTS_DIGESTER_H_
#define RIEGELI_DIGESTS_DIGESTER_H_

#include <stddef.h>

#include <array>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/numeric/int128.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"

namespace riegeli {

// The base class of an object observing data being read or written.
//
// Derived classes are expected to derive from `Digester<DigestType>` for some
// `DigestType` instead.
//
// For digesting many small values it is better to use `DigestingWriter` which
// adds a buffering layer.
class DigesterBase {
 public:
  // Called with consecutive fragments of data.
  //
  // Precondition: `is_open()`.
  void Write(char src) { Write(absl::string_view(&src, 1)); }
#if __cpp_char8_t
  void Write(char8_t src) { Write(static_cast<char>(src)); }
#endif
  void Write(absl::string_view src);
  ABSL_ATTRIBUTE_ALWAYS_INLINE
  void Write(const char* src) { Write(absl::string_view(src)); }
  void Write(const Chain& src);
  void Write(const absl::Cord& src);
  template <
      typename Src,
      std::enable_if_t<
          absl::conjunction<
              HasAbslStringify<Src>,
              absl::negation<std::is_convertible<Src&&, absl::string_view>>,
              absl::negation<std::is_convertible<Src&&, const Chain&>>,
              absl::negation<std::is_convertible<Src&&, const absl::Cord&>>>::
              value,
          int> = 0>
  void Write(Src&& src);

  // Numeric types supported by `Writer::Write()` are not supported by
  // `DigesterBase::Write()`. Use `DigestingWriter` instead or convert them to
  // strings.
  void Write(signed char) = delete;
  void Write(unsigned char) = delete;
  void Write(short) = delete;
  void Write(unsigned short) = delete;
  void Write(int) = delete;
  void Write(unsigned) = delete;
  void Write(long) = delete;
  void Write(unsigned long) = delete;
  void Write(long long) = delete;
  void Write(unsigned long long) = delete;
  void Write(absl::int128) = delete;
  void Write(absl::uint128) = delete;
  void Write(float) = delete;
  void Write(double) = delete;
  void Write(long double) = delete;
  void Write(bool) = delete;
  void Write(wchar_t) = delete;
  void Write(char16_t) = delete;
  void Write(char32_t) = delete;

  // Can be called instead of `Write()` when data consists of zeros.
  //
  // Precondition: `is_open()`.
  void WriteZeros(riegeli::Position length);

  // Returns `true` if the `DigesterBase` is open, i.e. not closed.
  bool is_open() const { return is_open_; }

  // Called when nothing more will be digested. This can make `Digest()` more
  // efficient. Resources can be freed.
  //
  // Does nothing if `!is_open()`.
  void Close();

 protected:
  DigesterBase() = default;

  DigesterBase(const DigesterBase& that) = default;
  DigesterBase& operator=(const DigesterBase& that) = default;

  // The source `DigesterBase` is left closed.
  DigesterBase(DigesterBase&& that) noexcept;
  DigesterBase& operator=(DigesterBase&& that) noexcept;

  virtual ~DigesterBase() = default;

  // Implementation of `Write(absl::string_view)`.
  //
  // Precondition: `is_open()`
  virtual void WriteImpl(absl::string_view src) = 0;

  // Implementation of `WriteZeros()`.
  //
  // Precondition: `is_open()`
  virtual void WriteZerosImpl(riegeli::Position length);

  // Implementation of `Close()`, called if the `DigesterBase` is not closed
  // yet.
  //
  // `Close()` returns early if `!is_open()`, otherwise calls `Done()` and marks
  // the `DigesterBase` as closed.
  //
  // Precondition: `is_open()`
  virtual void Done() {}

 private:
  class DigesterAbslStringifySink;

  bool is_open_ = true;
};

// The base class of an object observing data being read or written, and
// returning return some data of type `DigestTypeParam` called a digest,
// e.g. a checksum.
//
// `DigestTypeParam` can be `void` for digesters used for their side effects.
template <typename DigestTypeParam>
class Digester : public DigesterBase {
 public:
  // The type returned by `Digest()`.
  using DigestType = DigestTypeParam;

  // Returns the digest of data written so far. Its type and meaning depends on
  // the concrete class. Unchanged by `Close()`.
  DigestType Digest() { return DigestImpl(); }

 protected:
  Digester() = default;

  Digester(const Digester& that) = default;
  Digester& operator=(const Digester& that) = default;

  // The source `Digester` is left closed.
  Digester(Digester&& that) = default;
  Digester& operator=(Digester&& that) = default;

  // Implementation of `Digest()`.
  virtual DigestType DigestImpl() = 0;
};

// Converts a digest from `std::array<char, size>` to `std::string`.
// Intended to be used with `WrappingDigester`.
//
// You may pass a pointer to this function, without wrapping it in a lambda
// (it will not be overloaded).
template <size_t size>
std::string ArrayToString(std::array<char, size> digest) {
  return std::string(digest.data(), digest.size());
}

// Implementation details follow.

inline DigesterBase::DigesterBase(DigesterBase&& that) noexcept
    : is_open_(std::exchange(that.is_open_, false)) {}

inline DigesterBase& DigesterBase::operator=(DigesterBase&& that) noexcept {
  is_open_ = std::exchange(that.is_open_, false);
  return *this;
}

inline void DigesterBase::Write(absl::string_view src) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of DigesterBase::Write(): object closed";
  WriteImpl(src);
}

inline void DigesterBase::WriteZeros(riegeli::Position length) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of DigesterBase::WriteZeros(): object closed";
  WriteZerosImpl(length);
}

inline void DigesterBase::Close() {
  if (ABSL_PREDICT_FALSE(is_open_)) return;
  Done();
  is_open_ = false;
}

class DigesterBase::DigesterAbslStringifySink {
 public:
  explicit DigesterAbslStringifySink(DigesterBase* digester)
      : digester_(digester) {}

  void Append(size_t length, char src);
  void Append(absl::string_view src) { digester_->Write(src); }
  friend void AbslFormatFlush(DigesterAbslStringifySink* dest,
                              absl::string_view src) {
    dest->Append(src);
  }

 private:
  DigesterBase* digester_;
};

template <typename Src,
          std::enable_if_t<
              absl::conjunction<
                  HasAbslStringify<Src>,
                  absl::negation<std::is_convertible<Src&&, absl::string_view>>,
                  absl::negation<std::is_convertible<Src&&, const Chain&>>,
                  absl::negation<
                      std::is_convertible<Src&&, const absl::Cord&>>>::value,
              int>>
inline void DigesterBase::Write(Src&& src) {
  DigesterAbslStringifySink sink(this);
  AbslStringify(sink, std::forward<Src>(src));
}

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_DIGESTER_H_
