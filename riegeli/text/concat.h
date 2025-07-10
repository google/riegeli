// Copyright 2025 Google LLC
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

#ifndef RIEGELI_TEXT_CONCAT_H_
#define RIEGELI_TEXT_CONCAT_H_

#include <ostream>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "riegeli/base/initializer.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/stringify.h"
#include "riegeli/bytes/stringify_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// The type returned by `riegeli::Concat()` and `riegeli::OwningConcat()`.
template <typename... T>
class ConcatType {
 public:
  explicit ConcatType(std::tuple<Initializer<T>...> values)
      : values_(std::move(values)) {}

  template <typename Sink>
  friend void AbslStringify(Sink& dest, const ConcatType& src) {
    src.Stringify(dest);
  }
  template <typename Sink>
  friend void AbslStringify(Sink& dest, ConcatType&& src) {
    std::move(src).Stringify(dest);
  }

  friend std::ostream& operator<<(std::ostream& dest, const ConcatType& src) {
    OStreamWriter<> writer(&dest);
    src.WriteTo(writer);
    writer.Close();
    return dest;
  }
  friend std::ostream& operator<<(std::ostream& dest, ConcatType&& src) {
    OStreamWriter<> writer(&dest);
    std::move(src).WriteTo(writer);
    writer.Close();
    return dest;
  }

  friend auto RiegeliStringifiedSize(const ConcatType& src) {
    return std::apply(
        [](const T&... values) { return riegeli::StringifiedSize(values...); },
        src.values_);
  }

 private:
  template <typename Sink>
  void Stringify(Sink& dest) const& {
    StringifyWriter writer(&dest);
    WriteTo(writer);
    writer.Close();
  }
  template <typename Sink>
  void Stringify(Sink& dest) && {
    StringifyWriter writer(&dest);
    std::move(*this).WriteTo(writer);
    writer.Close();
  }

  // Faster implementation if `Sink` is `WriterStringifySink`.
  void Stringify(WriterStringifySink& dest) const& { WriteTo(*dest.dest()); }
  void Stringify(WriterStringifySink& dest) && {
    std::move(*this).WriteTo(*dest.dest());
  }

  void WriteTo(Writer& dest) const& {
    std::apply([&](const T&... values) { dest.Write(values...); }, values_);
  }
  void WriteTo(Writer& dest) && {
    std::apply([&](T&&... values) { dest.Write(std::forward<T>(values)...); },
               std::move(values_));
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::tuple<T...> values_;
};

// Wraps a sequence of values such that its stringified representation is the
// concatenation of stringified representations of the values.
//
// `riegeli::Concat()` does not own the values, even if they involve
// temporaries, hence it should be stringified by the same expression which
// constructed it, so that the temporaries outlive its usage. For storing
// a `ConcatType` in a variable or returning it from a function, use
// `riegeli::OwningConcat()` or construct `ConcatType` directly.
template <
    typename... Srcs,
    std::enable_if_t<IsStringifiable<TargetRefT<Srcs>...>::value, int> = 0>
inline ConcatType<TargetRefT<Srcs>...> Concat(
    Srcs&&... srcs ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return ConcatType<TargetRefT<Srcs>...>(
      std::forward_as_tuple(std::forward<Srcs>(srcs)...));
}

// `riegeli::OwningConcat()` is like `riegeli::Concat()`, but the arguments are
// stored by value instead of by reference. This is useful for storing the
// `ConcatType` in a variable or returning it from a function.
//
// If a particular argument is heavy and its lifetime is sufficient for storing
// it by reference, convert `const std::string&` to `absl::string_view` or wrap
// the argument in `std::cref()`.
template <typename... Srcs,
          std::enable_if_t<IsStringifiable<TargetT<Srcs>...>::value, int> = 0>
inline ConcatType<TargetT<Srcs>...> OwningConcat(Srcs&&... srcs) {
  return ConcatType<TargetT<Srcs>...>(
      std::forward_as_tuple(std::forward<Srcs>(srcs)...));
}

}  // namespace riegeli

#endif  // RIEGELI_TEXT_CONCAT_H_
