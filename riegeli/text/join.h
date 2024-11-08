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

#ifndef RIEGELI_TEXT_JOIN_H_
#define RIEGELI_TEXT_JOIN_H_

#include <algorithm>
#include <initializer_list>
#include <ostream>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/iterable.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/absl_stringify_writer.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// The default formatter for `Join()` which formats a value by using
// `Writer::Write()`.
struct DefaultFormatter {
  template <typename Value>
  void operator()(Value&& src, Writer& dest) const {
    dest.Write(std::forward<Value>(src));
  }
};

// A formatter for `Join()` which formats a value by invoking a function and
// using `Writer::Write()` on the result.
template <typename Function>
class InvokingFormatter {
 public:
  InvokingFormatter() : function_() {}

  explicit InvokingFormatter(Initializer<Function> function)
      : function_(std::move(function)) {}

  template <typename Value>
  void operator()(Value&& src, Writer& dest) const {
    dest.Write(riegeli::invoke(function_, std::forward<Value>(src)));
  }

 private:
  Function function_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Function>
explicit InvokingFormatter(Function&& function)
    -> InvokingFormatter<TargetT<Function>>;
#endif

// A formatter for `Join()` which decorates the value with a string before
// and/or a string after formatting it with another formatter.
template <typename ValueFormatter = DefaultFormatter>
class DecoratingFormatter {
 public:
  explicit DecoratingFormatter(
      absl::string_view after ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : value_formatter_(), after_(after) {}

  explicit DecoratingFormatter(Initializer<ValueFormatter> value_formatter,
                               absl::string_view after
                                   ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : value_formatter_(std::move(value_formatter)), after_(after) {}

  explicit DecoratingFormatter(
      absl::string_view before ABSL_ATTRIBUTE_LIFETIME_BOUND,
      absl::string_view after ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : before_(before), value_formatter_(), after_(after) {}

  explicit DecoratingFormatter(
      absl::string_view before ABSL_ATTRIBUTE_LIFETIME_BOUND,
      Initializer<ValueFormatter> value_formatter,
      absl::string_view after ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : before_(before),
        value_formatter_(std::move(value_formatter)),
        after_(after) {}

  template <typename Value>
  void operator()(Value&& src, Writer& dest) const {
    dest.Write(before_);
    value_formatter_(std::forward<Value>(src), dest);
    dest.Write(after_);
  }

 private:
  absl::string_view before_;
  ValueFormatter value_formatter_;
  absl::string_view after_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit DecoratingFormatter(absl::string_view after)
    -> DecoratingFormatter<DefaultFormatter>;
template <typename ValueFormatter = DefaultFormatter,
          std::enable_if_t<
              !std::is_convertible<ValueFormatter, absl::string_view>::value,
              int> = 0>
explicit DecoratingFormatter(ValueFormatter&& value_formatter,
                             absl::string_view after)
    -> DecoratingFormatter<TargetT<ValueFormatter>>;
explicit DecoratingFormatter(absl::string_view before, absl::string_view after)
    -> DecoratingFormatter<DefaultFormatter>;
template <typename ValueFormatter = DefaultFormatter>
explicit DecoratingFormatter(absl::string_view before,
                             ValueFormatter&& value_formatter,
                             absl::string_view after)
    -> DecoratingFormatter<TargetT<ValueFormatter>>;
#endif

// A formatter for `Join()` which formats a pair with a separator between the
// elements.
template <typename FirstFormatter = DefaultFormatter,
          typename SecondFormatter = DefaultFormatter>
class PairFormatter {
 public:
  explicit PairFormatter(
      absl::string_view separator ABSL_ATTRIBUTE_LIFETIME_BOUND,
      Initializer<SecondFormatter> second_formatter = SecondFormatter())
      : first_formatter_(),
        separator_(separator),
        second_formatter_(std::move(second_formatter)) {}

  explicit PairFormatter(
      Initializer<FirstFormatter> first_formatter,
      absl::string_view separator ABSL_ATTRIBUTE_LIFETIME_BOUND,
      Initializer<SecondFormatter> second_formatter = SecondFormatter())
      : first_formatter_(std::move(first_formatter)),
        separator_(separator),
        second_formatter_(std::move(second_formatter)) {}

  template <typename First, typename Second>
  void operator()(const std::pair<First, Second>& src, Writer& dest) const {
    first_formatter_(src.first, dest);
    dest.Write(separator_);
    second_formatter_(src.second, dest);
  }

  template <typename First, typename Second>
  void operator()(std::pair<First, Second>&& src, Writer& dest) const {
    first_formatter_(std::move(src.first), dest);
    dest.Write(separator_);
    second_formatter_(std::move(src.second), dest);
  }

 private:
  FirstFormatter first_formatter_;
  absl::string_view separator_;
  SecondFormatter second_formatter_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename SecondFormatter = DefaultFormatter>
explicit PairFormatter(absl::string_view separator,
                       SecondFormatter&& second_formatter = SecondFormatter())
    -> PairFormatter<DefaultFormatter, TargetT<SecondFormatter>>;
template <typename FirstFormatter = DefaultFormatter,
          typename SecondFormatter = DefaultFormatter>
explicit PairFormatter(FirstFormatter&& first_formatter,
                       absl::string_view separator,
                       SecondFormatter&& second_formatter = SecondFormatter())
    -> PairFormatter<TargetT<FirstFormatter>, TargetT<SecondFormatter>>;
#endif

// The type returned by `Join()`.
template <typename Src, typename Formatter = DefaultFormatter>
class JoinType {
 public:
  explicit JoinType(Initializer<Src> src,
                    Initializer<Formatter> formatter = Formatter())
      : src_(std::move(src)), formatter_(std::move(formatter)) {}

  explicit JoinType(Initializer<Src> src,
                    absl::string_view separator ABSL_ATTRIBUTE_LIFETIME_BOUND,
                    Initializer<Formatter> formatter = Formatter())
      : src_(std::move(src)),
        separator_(separator),
        formatter_(std::move(formatter)) {}

  JoinType(const JoinType& that) = default;
  JoinType& operator=(const JoinType& that) = default;

  JoinType(JoinType&& that) = default;
  JoinType& operator=(JoinType&& that) = default;

  template <typename Sink>
  friend void AbslStringify(Sink& dest, const JoinType& src) {
    src.Stringify(dest);
  }
  template <typename Sink>
  friend void AbslStringify(Sink& dest, JoinType&& src) {
    std::move(src).Stringify(dest);
  }

  friend std::ostream& operator<<(std::ostream& dest, const JoinType& src) {
    OStreamWriter<> writer(&dest);
    src.WriteTo(writer);
    writer.Close();
    return dest;
  }
  friend std::ostream& operator<<(std::ostream& dest, JoinType&& src) {
    OStreamWriter<> writer(&dest);
    std::move(src).WriteTo(writer);
    writer.Close();
    return dest;
  }

 private:
  template <typename Sink>
  void Stringify(Sink& dest) const& {
    AbslStringifyWriter writer(&dest);
    WriteTo(writer);
    writer.Close();
  }
  template <typename Sink>
  void Stringify(Sink& dest) && {
    AbslStringifyWriter writer(&dest);
    std::move(*this).WriteTo(writer);
    writer.Close();
  }
  // Faster implementation if `Sink` is `WriterAbslStringifySink`.
  void Stringify(WriterAbslStringifySink& dest) const& {
    WriteTo(*dest.dest());
  }
  void Stringify(WriterAbslStringifySink& dest) && {
    std::move(*this).WriteTo(*dest.dest());
  }

  void WriteTo(Writer& dest) const&;
  void WriteTo(Writer& dest) &&;

  Dependency<const void*, Src> src_;
  absl::string_view separator_;
  Formatter formatter_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Src, typename Formatter = DefaultFormatter>
explicit JoinType(Src&& src, absl::string_view separator,
                  Formatter&& formatter = Formatter())
    -> JoinType<TargetT<Src>, TargetT<Formatter>>;
#endif

// `riegeli::Join()` wraps a collection such that its stringified representation
// joins elements with a separator. Each element is formatted with the given
// formatter.
//
// `riegeli::Join()` does not own the collection nor the formatter, even if they
// involve temporaries, hence it should be stringified by the same expression
// which constructed it, so that the temporaries outlive its usage. For storing
// a `JoinType` in a variable or returning it from a function, construct
// `JoinType` directly.

template <
    typename Src, typename Formatter = DefaultFormatter,
    std::enable_if_t<
        !std::is_convertible<Formatter&&, absl::string_view>::value, int> = 0>
inline JoinType<Src&&, Formatter&&> Join(
    Src&& src ABSL_ATTRIBUTE_LIFETIME_BOUND,
    Formatter&& formatter ABSL_ATTRIBUTE_LIFETIME_BOUND = Formatter()) {
  return JoinType<Src&&, Formatter&&>(std::forward<Src>(src),
                                      std::forward<Formatter>(formatter));
}

template <
    typename Value = absl::string_view, typename Formatter = DefaultFormatter,
    std::enable_if_t<
        !std::is_convertible<Formatter&&, absl::string_view>::value, int> = 0>
inline JoinType<std::initializer_list<Value>, Formatter&&> Join(
    std::initializer_list<Value> src ABSL_ATTRIBUTE_LIFETIME_BOUND,
    Formatter&& formatter ABSL_ATTRIBUTE_LIFETIME_BOUND = Formatter()) {
  return JoinType<std::initializer_list<Value>, Formatter&&>(
      src, std::forward<Formatter>(formatter));
}

template <typename Src, typename Formatter = DefaultFormatter>
inline JoinType<Src&&, Formatter&&> Join(
    Src&& src ABSL_ATTRIBUTE_LIFETIME_BOUND,
    absl::string_view separator ABSL_ATTRIBUTE_LIFETIME_BOUND,
    Formatter&& formatter ABSL_ATTRIBUTE_LIFETIME_BOUND = Formatter()) {
  return JoinType<Src&&, Formatter&&>(std::forward<Src>(src), separator,
                                      std::forward<Formatter>(formatter));
}

template <typename Value = absl::string_view,
          typename Formatter = DefaultFormatter>
inline JoinType<std::initializer_list<Value>, Formatter&&> Join(
    std::initializer_list<Value> src ABSL_ATTRIBUTE_LIFETIME_BOUND,
    absl::string_view separator ABSL_ATTRIBUTE_LIFETIME_BOUND,
    Formatter&& formatter ABSL_ATTRIBUTE_LIFETIME_BOUND = Formatter()) {
  return JoinType<std::initializer_list<Value>, Formatter&&>(
      src, separator, std::forward<Formatter>(formatter));
}

// Implementation details follow.

template <typename Src, typename Formatter>
inline void JoinType<Src, Formatter>::WriteTo(Writer& dest) const& {
  using std::begin;
  auto iter = begin(*src_);
  using std::end;
  auto end_iter = end(*src_);
  if (iter == end_iter) return;
  for (;;) {
    formatter_(*iter, dest);
    ++iter;
    if (iter == end_iter) break;
    dest.Write(separator_);
  }
}

template <typename Src, typename Formatter>
inline void JoinType<Src, Formatter>::WriteTo(Writer& dest) && {
  using std::begin;
  auto iter = MaybeMakeMoveIterator<Src>(begin(*src_));
  using std::end;
  auto end_iter = MaybeMakeMoveIterator<Src>(end(*src_));
  if (iter == end_iter) return;
  for (;;) {
    formatter_(*iter, dest);
    ++iter;
    if (iter == end_iter) break;
    dest.Write(separator_);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_TEXT_JOIN_H_
