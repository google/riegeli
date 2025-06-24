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

#ifndef RIEGELI_TEXT_ASCII_ALIGN_H_
#define RIEGELI_TEXT_ASCII_ALIGN_H_

#include <stddef.h>

#include <algorithm>
#include <limits>
#include <ostream>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/absl_stringify_writer.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/restricted_chain_writer.h"
#include "riegeli/bytes/stringify.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/text/concat.h"

namespace riegeli {

// Options for `AsciiLeft()`, `AsciiCenter()`, and `AsciiRight()`.
class AlignOptions {
 public:
  AlignOptions() noexcept {}

  // Options can also be specified by the minimum width alone.
  /*implicit*/ AlignOptions(Position width) : width_(width) {}

  // Minimum width.
  //
  // Default: 0.
  AlignOptions& set_width(Position width) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
    width_ = width;
    return *this;
  }
  AlignOptions&& set_width(Position width) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_width(width));
  }
  Position width() const { return width_; }

  // The character to fill space before and/or after the value with.
  //
  // Default: ' '.
  AlignOptions& set_fill(char fill) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
    fill_ = fill;
    return *this;
  }
  AlignOptions&& set_fill(char fill) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_fill(fill));
  }
  char fill() const { return fill_; }

 private:
  Position width_ = 0;
  char fill_ = ' ';
};

// The type returned by `riegeli::AsciiLeft()` and `riegeli::OwningAsciiLeft()`.
template <typename... T>
class AsciiLeftType {
 public:
  explicit AsciiLeftType(std::tuple<Initializer<T>...> values,
                         AlignOptions options)
      : values_(std::move(values)), options_(std::move(options)) {}

  template <typename Sink>
  friend void AbslStringify(Sink& dest, const AsciiLeftType& src) {
    src.Stringify(dest);
  }
  template <typename Sink>
  friend void AbslStringify(Sink& dest, AsciiLeftType&& src) {
    std::move(src).Stringify(dest);
  }

  friend std::ostream& operator<<(std::ostream& dest,
                                  const AsciiLeftType& src) {
    OStreamWriter writer(&dest);
    src.WriteTo(writer);
    writer.Close();
    return dest;
  }
  friend std::ostream& operator<<(std::ostream& dest, AsciiLeftType&& src) {
    OStreamWriter writer(&dest);
    std::move(src).WriteTo(writer);
    writer.Close();
    return dest;
  }

  friend auto RiegeliStringifiedSize(const AsciiLeftType& src) {
    if constexpr (HasStringifiedSize<T...>::value) {
      return UnsignedMax(riegeli::StringifiedSize(src.values_),
                         src.options_.width());
    }
  }

 private:
  template <typename Sink>
  void Stringify(Sink& dest) const&;
  template <typename Sink>
  void Stringify(Sink& dest) &&;

  // Faster implementation if `Sink` is `WriterAbslStringifySink`.
  void Stringify(WriterAbslStringifySink& dest) const& {
    WriteTo(*dest.dest());
  }
  void Stringify(WriterAbslStringifySink& dest) && {
    std::move(*this).WriteTo(*dest.dest());
  }

  void WriteTo(Writer& dest) const&;
  void WriteTo(Writer& dest) &&;

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS ConcatType<T...> values_;
  AlignOptions options_;
};

// Wraps a sequence of values such that their concatenated stringified
// representation is filled to at least the given width, with the values on the
// left side of the field.
//
// The last argument is `AlignOptions` or the width. The remaining arguments are
// the values.
//
// The width is measured in bytes, so this is suitable only for ASCII data.
//
// `riegeli::AsciiLeft()` does not own the values, even if they involve
// temporaries, hence it should be stringified by the same expression which
// constructed it, so that the temporaries outlive its usage. For storing
// an `AsciiLeftType` in a variable or returning it from a function, use
// `riegeli::OwningAsciiLeft()` or construct `AsciiLeftType` directly.
template <
    typename... Args,
    std::enable_if_t<
        std::conjunction_v<
            std::bool_constant<sizeof...(Args) != 2>,
            std::is_convertible<GetTypeFromEndT<1, Args&&...>, AlignOptions>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, TargetRefT<Args>...>,
                                 IsStringifiable>>,
        int> = 0>
inline ApplyToTupleElementsT<AsciiLeftType,
                             RemoveTypesFromEndT<1, TargetRefT<Args>...>>
AsciiLeft(Args&&... args) {
  return ApplyToTupleElementsT<AsciiLeftType,
                               RemoveTypesFromEndT<1, TargetRefT<Args>...>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

// A specialization for one stringifiable parameter which allows to annotate the
// parameter with `ABSL_ATTRIBUTE_LIFETIME_BOUND`.
template <typename Arg,
          std::enable_if_t<IsStringifiable<TargetRefT<Arg>>::value, int> = 0>
inline AsciiLeftType<TargetRefT<Arg>> AsciiLeft(
    Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND, AlignOptions options) {
  return AsciiLeftType<TargetRefT<Arg>>(
      std::forward_as_tuple(std::forward<Arg>(arg)), std::move(options));
}

// `riegeli::OwningAsciiLeft()` is like `riegeli::AsciiLeft()`, but the
// arguments are stored by value instead of by reference. This is useful for
// storing the `AsciiLeftType` in a variable or returning it from a function.
//
// If a particular argument is heavy and its lifetime is sufficient for storing
// it by reference, convert `const std::string&` to `absl::string_view` or wrap
// the argument in `std::cref()`.
template <
    typename... Args,
    std::enable_if_t<
        std::conjunction_v<
            std::is_convertible<GetTypeFromEndT<1, Args&&...>, AlignOptions>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, TargetT<Args>...>,
                                 IsStringifiable>>,
        int> = 0>
inline ApplyToTupleElementsT<AsciiLeftType,
                             RemoveTypesFromEndT<1, TargetT<Args>...>>
OwningAsciiLeft(Args&&... args) {
  return ApplyToTupleElementsT<AsciiLeftType,
                               RemoveTypesFromEndT<1, TargetT<Args>...>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

// The type returned by `riegeli::AsciiCenter()` and
// `riegeli::OwningAsciiCenter()`.
template <typename... T>
class AsciiCenterType {
 public:
  explicit AsciiCenterType(std::tuple<Initializer<T>...> values,
                           AlignOptions options)
      : values_(std::move(values)), options_(std::move(options)) {}

  template <typename Sink>
  friend void AbslStringify(Sink& dest, const AsciiCenterType& src) {
    src.Stringify(dest);
  }
  template <typename Sink>
  friend void AbslStringify(Sink& dest, AsciiCenterType&& src) {
    std::move(src).Stringify(dest);
  }

  friend std::ostream& operator<<(std::ostream& dest,
                                  const AsciiCenterType& src) {
    OStreamWriter writer(&dest);
    src.WriteTo(writer);
    writer.Close();
    return dest;
  }
  friend std::ostream& operator<<(std::ostream& dest, AsciiCenterType&& src) {
    OStreamWriter writer(&dest);
    std::move(src).WriteTo(writer);
    writer.Close();
    return dest;
  }

  friend auto RiegeliStringifiedSize(const AsciiCenterType& src) {
    if constexpr (HasStringifiedSize<T...>::value) {
      return UnsignedMax(riegeli::StringifiedSize(src.values_),
                         src.options_.width());
    }
  }

 private:
  template <typename Sink>
  void Stringify(Sink& dest) const&;
  template <typename Sink>
  void Stringify(Sink& dest) &&;

  // Faster implementation if `Sink` is `WriterAbslStringifySink`.
  void Stringify(WriterAbslStringifySink& dest) const& {
    WriteTo(*dest.dest());
  }
  void Stringify(WriterAbslStringifySink& dest) && {
    std::move(*this).WriteTo(*dest.dest());
  }

  void WriteTo(Writer& dest) const&;
  void WriteTo(Writer& dest) &&;

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS ConcatType<T...> values_;
  AlignOptions options_;
};

// Wraps a sequence of values such that their concatenated stringified
// representation is filled to at least the given width, with the values
// centered in the field (with one fill character fewer on the left side if
// there is an odd number of them).
//
// The last argument is `AlignOptions` or the width. The remaining arguments are
// the values.
//
// The width is measured in bytes, so this is suitable only for ASCII data.
//
// `riegeli::AsciiCenter()` does not own the values, even if they involve
// temporaries, hence it should be stringified by the same expression which
// constructed it, so that the temporaries outlive its usage. For storing
// an `AsciiCenterType` in a variable or returning it from a function, use
// `riegeli::OwningAsciiCenter()` or construct `AsciiCenterType` directly.
template <
    typename... Args,
    std::enable_if_t<
        std::conjunction_v<
            std::bool_constant<sizeof...(Args) != 2>,
            std::is_convertible<GetTypeFromEndT<1, Args&&...>, AlignOptions>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, TargetRefT<Args>...>,
                                 IsStringifiable>>,
        int> = 0>
inline ApplyToTupleElementsT<AsciiCenterType,
                             RemoveTypesFromEndT<1, TargetRefT<Args>...>>
AsciiCenter(Args&&... args) {
  return ApplyToTupleElementsT<AsciiCenterType,
                               RemoveTypesFromEndT<1, TargetRefT<Args>...>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

// A specialization for one stringifiable parameter which allows to annotate the
// parameter with `ABSL_ATTRIBUTE_LIFETIME_BOUND`.
template <typename Arg,
          std::enable_if_t<IsStringifiable<TargetRefT<Arg>>::value, int> = 0>
inline AsciiCenterType<TargetRefT<Arg>> AsciiCenter(
    Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND, AlignOptions options) {
  return AsciiCenterType<TargetRefT<Arg>>(
      std::forward_as_tuple(std::forward<Arg>(arg)), std::move(options));
}

// `riegeli::OwningAsciiCenter()` is like `riegeli::AsciiCenter()`, but the
// arguments are stored by value instead of by reference. This is useful for
// storing the `AsciiCenterType` in a variable or returning it from a function.
//
// If a particular argument is heavy and its lifetime is sufficient for storing
// it by reference, convert `const std::string&` to `absl::string_view` or wrap
// the argument in `std::cref()`.
template <
    typename... Args,
    std::enable_if_t<
        std::conjunction_v<
            std::is_convertible<GetTypeFromEndT<1, Args&&...>, AlignOptions>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, TargetT<Args>...>,
                                 IsStringifiable>>,
        int> = 0>
inline ApplyToTupleElementsT<AsciiCenterType,
                             RemoveTypesFromEndT<1, TargetT<Args>...>>
OwningAsciiCenter(Args&&... args) {
  return ApplyToTupleElementsT<AsciiCenterType,
                               RemoveTypesFromEndT<1, TargetT<Args>...>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

// The type returned by `riegeli::AsciiRight()` and
// `riegeli::OwningAsciiRight()`.
template <typename... T>
class AsciiRightType {
 public:
  explicit AsciiRightType(std::tuple<Initializer<T>...> values,
                          AlignOptions options)
      : values_(std::move(values)), options_(std::move(options)) {}

  template <typename Sink>
  friend void AbslStringify(Sink& dest, const AsciiRightType& src) {
    src.Stringify(dest);
  }
  template <typename Sink>
  friend void AbslStringify(Sink& dest, AsciiRightType&& src) {
    std::move(src).Stringify(dest);
  }

  friend std::ostream& operator<<(std::ostream& dest,
                                  const AsciiRightType& src) {
    OStreamWriter writer(&dest);
    src.WriteTo(writer);
    writer.Close();
    return dest;
  }
  friend std::ostream& operator<<(std::ostream& dest, AsciiRightType&& src) {
    OStreamWriter writer(&dest);
    std::move(src).WriteTo(writer);
    writer.Close();
    return dest;
  }

  friend auto RiegeliStringifiedSize(const AsciiRightType& src) {
    if constexpr (HasStringifiedSize<T...>::value) {
      return UnsignedMax(riegeli::StringifiedSize(src.values_),
                         src.options_.width());
    }
  }

 private:
  template <typename Sink>
  void Stringify(Sink& dest) const&;
  template <typename Sink>
  void Stringify(Sink& dest) &&;

  // Faster implementation if `Sink` is `WriterAbslStringifySink`.
  void Stringify(WriterAbslStringifySink& dest) const& {
    WriteTo(*dest.dest());
  }
  void Stringify(WriterAbslStringifySink& dest) && {
    std::move(*this).WriteTo(*dest.dest());
  }

  void WriteTo(Writer& dest) const&;
  void WriteTo(Writer& dest) &&;

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS ConcatType<T...> values_;
  AlignOptions options_;
};

// Wraps a sequence of values such that their concatenated stringified
// representation is filled to at least the given width, with the values on the
// right side of the field.
//
// The last argument is `AlignOptions` or the width. The remaining arguments are
// the values.
//
// The width is measured in bytes, so this is suitable only for ASCII data.
//
// `riegeli::AsciiRight()` does not own the values, even if they involve
// temporaries, hence it should be stringified by the same expression which
// constructed it, so that the temporaries outlive its usage. For storing
// an `AsciiRightType` in a variable or returning it from a function, use
// `riegeli::OwningAsciiRight()` or construct `AsciiRightType` directly.
template <
    typename... Args,
    std::enable_if_t<
        std::conjunction_v<
            std::bool_constant<sizeof...(Args) != 2>,
            std::is_convertible<GetTypeFromEndT<1, Args&&...>, AlignOptions>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, TargetRefT<Args>...>,
                                 IsStringifiable>>,
        int> = 0>
inline ApplyToTupleElementsT<AsciiRightType,
                             RemoveTypesFromEndT<1, TargetRefT<Args>...>>
AsciiRight(Args&&... args) {
  return ApplyToTupleElementsT<AsciiRightType,
                               RemoveTypesFromEndT<1, TargetRefT<Args>...>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

// A specialization for one stringifiable parameter which allows to annotate the
// parameter with `ABSL_ATTRIBUTE_LIFETIME_BOUND`.
template <typename Arg,
          std::enable_if_t<IsStringifiable<TargetRefT<Arg>>::value, int> = 0>
inline AsciiRightType<TargetRefT<Arg>> AsciiRight(
    Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND, AlignOptions options) {
  return AsciiRightType<TargetRefT<Arg>>(
      std::forward_as_tuple(std::forward<Arg>(arg)), std::move(options));
}

// `riegeli::OwningAsciiRight()` is like `riegeli::AsciiRight()`, but the
// arguments are stored by value instead of by reference. This is useful for
// storing the `AsciiRightType` in a variable or returning it from a function.
//
// If a particular argument is heavy and its lifetime is sufficient for storing
// it by reference, convert `const std::string&` to `absl::string_view` or wrap
// the argument in `std::cref()`.
template <
    typename... Args,
    std::enable_if_t<
        std::conjunction_v<
            std::is_convertible<GetTypeFromEndT<1, Args&&...>, AlignOptions>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, TargetT<Args>...>,
                                 IsStringifiable>>,
        int> = 0>
inline ApplyToTupleElementsT<AsciiRightType,
                             RemoveTypesFromEndT<1, TargetT<Args>...>>
OwningAsciiRight(Args&&... args) {
  return ApplyToTupleElementsT<AsciiRightType,
                               RemoveTypesFromEndT<1, TargetT<Args>...>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

// Implementation details follow.

namespace align_internal {

template <typename Sink>
inline void WritePadding(Sink& dest, Position length, char fill) {
  while (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max())) {
    dest.Append(std::numeric_limits<size_t>::max(), fill);
    length -= std::numeric_limits<size_t>::max();
  }
  if (length > 0) dest.Append(IntCast<size_t>(length), fill);
}

}  // namespace align_internal

template <typename... T>
template <typename Sink>
inline void AsciiLeftType<T...>::Stringify(Sink& dest) const& {
  AbslStringifyWriter writer(&dest);
  writer.Write(values_);
  if (ABSL_PREDICT_FALSE(!writer.Close())) return;
  align_internal::WritePadding(
      dest, SaturatingSub(options_.width(), writer.pos()), options_.fill());
}

template <typename... T>
template <typename Sink>
inline void AsciiLeftType<T...>::Stringify(Sink& dest) && {
  AbslStringifyWriter writer(&dest);
  writer.Write(std::move(values_));
  if (ABSL_PREDICT_FALSE(!writer.Close())) return;
  align_internal::WritePadding(
      dest, SaturatingSub(options_.width(), writer.pos()), options_.fill());
}

template <typename... T>
inline void AsciiLeftType<T...>::WriteTo(Writer& dest) const& {
  const Position pos_before = dest.pos();
  dest.Write(values_);
  RIEGELI_ASSERT_GE(dest.pos(), pos_before)
      << "Writer::Write() decreased pos()";
  dest.Write(ByteFill(SaturatingSub(options_.width(), dest.pos() - pos_before),
                      options_.fill()));
}

template <typename... T>
inline void AsciiLeftType<T...>::WriteTo(Writer& dest) && {
  const Position pos_before = dest.pos();
  dest.Write(std::move(values_));
  RIEGELI_ASSERT_GE(dest.pos(), pos_before)
      << "Writer::Write() decreased pos()";
  dest.Write(ByteFill(SaturatingSub(options_.width(), dest.pos() - pos_before),
                      options_.fill()));
}

template <typename... T>
template <typename Sink>
inline void AsciiCenterType<T...>::Stringify(Sink& dest) const& {
  if constexpr (HasStringifiedSize<T...>::value) {
    const Position padding =
        SaturatingSub(options_.width(), riegeli::StringifiedSize(values_));
    align_internal::WritePadding(dest, padding / 2, options_.fill());
    AbslStringifyWriter writer(&dest);
    writer.Write(values_);
    if (ABSL_PREDICT_FALSE(!writer.Close())) return;
    align_internal::WritePadding(dest, padding - padding / 2, options_.fill());
  } else {
    RestrictedChainWriter chain_writer;
    chain_writer.Write(values_);
    if (ABSL_PREDICT_FALSE(!chain_writer.Close())) return;
    const Position padding =
        SaturatingSub(options_.width(), chain_writer.dest().size());
    align_internal::WritePadding(dest, padding / 2, options_.fill());
    AbslStringify(dest, chain_writer.dest());
    align_internal::WritePadding(dest, padding - padding / 2, options_.fill());
  }
}

template <typename... T>
template <typename Sink>
inline void AsciiCenterType<T...>::Stringify(Sink& dest) && {
  if constexpr (HasStringifiedSize<T...>::value) {
    const Position padding =
        SaturatingSub(options_.width(), riegeli::StringifiedSize(values_));
    align_internal::WritePadding(dest, padding / 2, options_.fill());
    AbslStringifyWriter writer(&dest);
    writer.Write(std::move(values_));
    if (ABSL_PREDICT_FALSE(!writer.Close())) return;
    align_internal::WritePadding(dest, padding - padding / 2, options_.fill());
  } else {
    RestrictedChainWriter chain_writer;
    chain_writer.Write(std::move(values_));
    if (ABSL_PREDICT_FALSE(!chain_writer.Close())) return;
    const Position padding =
        SaturatingSub(options_.width(), chain_writer.dest().size());
    align_internal::WritePadding(dest, padding / 2, options_.fill());
    AbslStringify(dest, chain_writer.dest());
    align_internal::WritePadding(dest, padding - padding / 2, options_.fill());
  }
}

template <typename... T>
inline void AsciiCenterType<T...>::WriteTo(Writer& dest) const& {
  if constexpr (HasStringifiedSize<T...>::value) {
    const Position padding =
        SaturatingSub(options_.width(), riegeli::StringifiedSize(values_));
    dest.Write(ByteFill(padding / 2, options_.fill()));
    dest.Write(values_);
    dest.Write(ByteFill(padding - padding / 2, options_.fill()));
  } else {
    RestrictedChainWriter chain_writer;
    chain_writer.Write(values_);
    if (ABSL_PREDICT_FALSE(!chain_writer.Close())) {
      dest.Fail(chain_writer.status());
      return;
    }
    const Position padding =
        SaturatingSub(options_.width(), chain_writer.dest().size());
    dest.Write(ByteFill(padding / 2, options_.fill()));
    dest.Write(std::move(chain_writer.dest()));
    dest.Write(ByteFill(padding - padding / 2, options_.fill()));
  }
}

template <typename... T>
inline void AsciiCenterType<T...>::WriteTo(Writer& dest) && {
  if constexpr (HasStringifiedSize<T...>::value) {
    const Position padding =
        SaturatingSub(options_.width(), riegeli::StringifiedSize(values_));
    dest.Write(ByteFill(padding / 2, options_.fill()));
    dest.Write(std::move(values_));
    dest.Write(ByteFill(padding - padding / 2, options_.fill()));
  } else {
    RestrictedChainWriter chain_writer;
    chain_writer.Write(std::move(values_));
    if (ABSL_PREDICT_FALSE(!chain_writer.Close())) {
      dest.Fail(chain_writer.status());
      return;
    }
    const Position padding =
        SaturatingSub(options_.width(), chain_writer.dest().size());
    dest.Write(ByteFill(padding / 2, options_.fill()));
    dest.Write(std::move(chain_writer.dest()));
    dest.Write(ByteFill(padding - padding / 2, options_.fill()));
  }
}

template <typename... T>
template <typename Sink>
inline void AsciiRightType<T...>::Stringify(Sink& dest) const& {
  if constexpr (HasStringifiedSize<T...>::value) {
    align_internal::WritePadding(
        dest,
        SaturatingSub(options_.width(), riegeli::StringifiedSize(values_)),
        options_.fill());
    AbslStringifyWriter writer(&dest);
    writer.Write(values_);
    writer.Close();
  } else {
    RestrictedChainWriter chain_writer;
    chain_writer.Write(values_);
    if (ABSL_PREDICT_FALSE(!chain_writer.Close())) return;
    const Position padding =
        SaturatingSub(options_.width(), chain_writer.dest().size());
    align_internal::WritePadding(dest, padding, options_.fill());
    AbslStringify(dest, chain_writer.dest());
  }
}

template <typename... T>
template <typename Sink>
inline void AsciiRightType<T...>::Stringify(Sink& dest) && {
  if constexpr (HasStringifiedSize<T...>::value) {
    align_internal::WritePadding(
        dest,
        SaturatingSub(options_.width(), riegeli::StringifiedSize(values_)),
        options_.fill());
    AbslStringifyWriter writer(&dest);
    writer.Write(std::move(values_));
    writer.Close();
  } else {
    RestrictedChainWriter chain_writer;
    chain_writer.Write(std::move(values_));
    if (ABSL_PREDICT_FALSE(!chain_writer.Close())) return;
    const Position padding =
        SaturatingSub(options_.width(), chain_writer.dest().size());
    align_internal::WritePadding(dest, padding, options_.fill());
    AbslStringify(dest, chain_writer.dest());
  }
}

template <typename... T>
inline void AsciiRightType<T...>::WriteTo(Writer& dest) const& {
  if constexpr (HasStringifiedSize<T...>::value) {
    dest.Write(ByteFill(
        SaturatingSub(options_.width(), riegeli::StringifiedSize(values_)),
        options_.fill()));
    dest.Write(values_);
  } else {
    RestrictedChainWriter chain_writer;
    chain_writer.Write(values_);
    if (ABSL_PREDICT_FALSE(!chain_writer.Close())) {
      dest.Fail(chain_writer.status());
      return;
    }
    dest.Write(
        ByteFill(SaturatingSub(options_.width(), chain_writer.dest().size()),
                 options_.fill()));
    dest.Write(std::move(chain_writer.dest()));
  }
}

template <typename... T>
inline void AsciiRightType<T...>::WriteTo(Writer& dest) && {
  if constexpr (HasStringifiedSize<T...>::value) {
    dest.Write(ByteFill(
        SaturatingSub(options_.width(), riegeli::StringifiedSize(values_)),
        options_.fill()));
    dest.Write(std::move(values_));
  } else {
    RestrictedChainWriter chain_writer;
    chain_writer.Write(std::move(values_));
    if (ABSL_PREDICT_FALSE(!chain_writer.Close())) {
      dest.Fail(chain_writer.status());
      return;
    }
    dest.Write(
        ByteFill(SaturatingSub(options_.width(), chain_writer.dest().size()),
                 options_.fill()));
    dest.Write(std::move(chain_writer.dest()));
  }
}

}  // namespace riegeli

#endif  // RIEGELI_TEXT_ASCII_ALIGN_H_
