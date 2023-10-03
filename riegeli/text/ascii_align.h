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

#include <limits>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/absl_stringify_writer.h"
#include "riegeli/bytes/restricted_chain_writer.h"
#include "riegeli/bytes/writer.h"

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
  AlignOptions& set_width(Position width) & {
    width_ = width;
    return *this;
  }
  AlignOptions&& set_width(Position width) && {
    return std::move(set_width(width));
  }
  Position width() const { return width_; }

  // The character to fill space before and/or after the value with.
  //
  // Default: ' '.
  AlignOptions& set_fill(char fill) & {
    fill_ = fill;
    return *this;
  }
  AlignOptions&& set_fill(char fill) && { return std::move(set_fill(fill)); }
  char fill() const { return fill_; }

 private:
  Position width_ = 0;
  char fill_ = ' ';
};

// The type returned by `AsciiLeft()`.
template <typename... T>
class AsciiLeftType {
 public:
  explicit AsciiLeftType(std::tuple<T...> values, AlignOptions options)
      : values_(std::move(values)), options_(std::move(options)) {}

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const AsciiLeftType& self) {
    self.AbslStringifyImpl(sink);
  }

 private:
  template <typename Sink, typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<
                TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value,
                int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  template <typename Sink, typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<!TupleElementsSatisfy<DependentTuple,
                                                   HasStringifiedSize>::value,
                             int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  // Faster implementations if `Sink` is `WriterAbslStringifySink`.
  template <typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<
                TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value,
                int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;
  template <typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<!TupleElementsSatisfy<DependentTuple,
                                                   HasStringifiedSize>::value,
                             int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;

  std::tuple<T...> values_;
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
// Values are captured by reference, so the result is valid while the values are
// valid.
template <
    typename... Args,
    std::enable_if_t<
        absl::conjunction<
            std::is_convertible<GetTypeFromEndT<1, Args&&...>, AlignOptions>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                 IsStringifiable>>::value,
        int> = 0>
inline ApplyToTupleElementsT<AsciiLeftType, RemoveTypesFromEndT<1, Args&&...>>
AsciiLeft(Args&&... args) {
  return ApplyToTupleElementsT<AsciiLeftType,
                               RemoveTypesFromEndT<1, Args&&...>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

// The type returned by `AsciiCenter()`.
template <typename... T>
class AsciiCenterType {
 public:
  explicit AsciiCenterType(std::tuple<T...> values, AlignOptions options)
      : values_(std::move(values)), options_(std::move(options)) {}

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const AsciiCenterType& self) {
    self.AbslStringifyImpl(sink);
  }

 private:
  template <typename Sink, typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<
                TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value,
                int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  template <typename Sink, typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<!TupleElementsSatisfy<DependentTuple,
                                                   HasStringifiedSize>::value,
                             int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  // Faster implementations if `Sink` is `WriterAbslStringifySink`.
  template <typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<
                TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value,
                int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;
  template <typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<!TupleElementsSatisfy<DependentTuple,
                                                   HasStringifiedSize>::value,
                             int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;

  std::tuple<T...> values_;
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
// Values are captured by reference, so the result is valid while the values are
// valid.
template <
    typename... Args,
    std::enable_if_t<
        absl::conjunction<
            std::is_convertible<GetTypeFromEndT<1, Args&&...>, AlignOptions>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                 IsStringifiable>>::value,
        int> = 0>
inline ApplyToTupleElementsT<AsciiCenterType, RemoveTypesFromEndT<1, Args&&...>>
AsciiCenter(Args&&... args) {
  return ApplyToTupleElementsT<AsciiCenterType,
                               RemoveTypesFromEndT<1, Args&&...>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

// The type returned by `AsciiRight()`.
template <typename... T>
class AsciiRightType {
 public:
  explicit AsciiRightType(std::tuple<T...> values, AlignOptions options)
      : values_(std::move(values)), options_(std::move(options)) {}

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const AsciiRightType& self) {
    self.AbslStringifyImpl(sink);
  }

 private:
  template <typename Sink, typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<
                TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value,
                int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  template <typename Sink, typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<!TupleElementsSatisfy<DependentTuple,
                                                   HasStringifiedSize>::value,
                             int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  // Faster implementations if `Sink` is `WriterAbslStringifySink`.
  template <typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<
                TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value,
                int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;
  template <typename DependentTuple = std::tuple<T...>,
            std::enable_if_t<!TupleElementsSatisfy<DependentTuple,
                                                   HasStringifiedSize>::value,
                             int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;

  std::tuple<T...> values_;
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
// Values are captured by reference, so the result is valid while the values are
// valid.
template <
    typename... Args,
    std::enable_if_t<
        absl::conjunction<
            std::is_convertible<GetTypeFromEndT<1, Args&&...>, AlignOptions>,
            TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                 IsStringifiable>>::value,
        int> = 0>
inline ApplyToTupleElementsT<AsciiRightType, RemoveTypesFromEndT<1, Args&&...>>
AsciiRight(Args&&... args) {
  return ApplyToTupleElementsT<AsciiRightType,
                               RemoveTypesFromEndT<1, Args&&...>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

// Implementation details follow.

namespace align_internal {

template <typename... T, size_t... indices>
Position StringifiedSizeOfTupleImpl(const std::tuple<T...>& values,
                                    std::index_sequence<indices...>) {
  return SaturatingAdd<Position>(
      riegeli::StringifiedSize(std::get<indices>(values))...);
}

template <typename... T>
Position StringifiedSizeOfTuple(const std::tuple<T...>& values) {
  return StringifiedSizeOfTupleImpl(values, std::index_sequence_for<T...>());
}

template <typename Sink>
inline void WriteChars(Sink& sink, Position length, char fill) {
  while (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max())) {
    sink.Append(std::numeric_limits<size_t>::max(), fill);
    length -= std::numeric_limits<size_t>::max();
  }
  if (length > 0) sink.Append(IntCast<size_t>(length), fill);
}

}  // namespace align_internal

template <typename... T>
template <
    typename Sink, typename DependentTuple,
    std::enable_if_t<
        TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiLeftType<T...>::AbslStringifyImpl(Sink& sink) const {
  AbslStringifyWriter writer(&sink);
  writer.WriteTuple(values_);
  if (ABSL_PREDICT_FALSE(!writer.Close())) return;
  align_internal::WriteChars(
      sink,
      SaturatingSub(options_.width(),
                    align_internal::StringifiedSizeOfTuple(values_)),
      options_.fill());
}

template <typename... T>
template <
    typename Sink, typename DependentTuple,
    std::enable_if_t<
        !TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiLeftType<T...>::AbslStringifyImpl(Sink& sink) const {
  AbslStringifyWriter writer(&sink);
  writer.WriteTuple(values_);
  if (ABSL_PREDICT_FALSE(!writer.Close())) return;
  align_internal::WriteChars(
      sink, SaturatingSub(options_.width(), writer.pos()), options_.fill());
}

template <typename... T>
template <
    typename DependentTuple,
    std::enable_if_t<
        TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiLeftType<T...>::AbslStringifyImpl(
    WriterAbslStringifySink& sink) const {
  sink.dest()->WriteTuple(values_);
  sink.dest()->WriteChars(
      SaturatingSub(options_.width(),
                    align_internal::StringifiedSizeOfTuple(values_)),
      options_.fill());
}

template <typename... T>
template <
    typename DependentTuple,
    std::enable_if_t<
        !TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiLeftType<T...>::AbslStringifyImpl(
    WriterAbslStringifySink& sink) const {
  const Position pos_before = sink.dest()->pos();
  sink.dest()->WriteTuple(values_);
  sink.dest()->WriteChars(
      SaturatingSub(options_.width(),
                    SaturatingSub(sink.dest()->pos(), pos_before)),
      options_.fill());
}

template <typename... T>
template <
    typename Sink, typename DependentTuple,
    std::enable_if_t<
        TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiCenterType<T...>::AbslStringifyImpl(Sink& sink) const {
  const Position padding = SaturatingSub(
      options_.width(), align_internal::StringifiedSizeOfTuple(values_));
  align_internal::WriteChars(sink, padding / 2, options_.fill());
  AbslStringifyWriter writer(&sink);
  writer.WriteTuple(values_);
  if (ABSL_PREDICT_FALSE(!writer.Close())) return;
  align_internal::WriteChars(sink, padding - padding / 2, options_.fill());
}

template <typename... T>
template <
    typename Sink, typename DependentTuple,
    std::enable_if_t<
        !TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiCenterType<T...>::AbslStringifyImpl(Sink& sink) const {
  RestrictedChainWriter chain_writer;
  chain_writer.WriteTuple(values_);
  if (ABSL_PREDICT_FALSE(!chain_writer.Close())) return;
  const Position padding =
      SaturatingSub(options_.width(), chain_writer.dest().size());
  align_internal::WriteChars(sink, padding / 2, options_.fill());
  AbslStringify(sink, chain_writer.dest());
  align_internal::WriteChars(sink, padding - padding / 2, options_.fill());
}

template <typename... T>
template <
    typename DependentTuple,
    std::enable_if_t<
        TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiCenterType<T...>::AbslStringifyImpl(
    WriterAbslStringifySink& sink) const {
  const Position padding = SaturatingSub(
      options_.width(), align_internal::StringifiedSizeOfTuple(values_));
  sink.dest()->WriteChars(padding / 2, options_.fill());
  sink.dest()->WriteTuple(values_);
  sink.dest()->WriteChars(padding - padding / 2, options_.fill());
}

template <typename... T>
template <
    typename DependentTuple,
    std::enable_if_t<
        !TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiCenterType<T...>::AbslStringifyImpl(
    WriterAbslStringifySink& sink) const {
  RestrictedChainWriter chain_writer;
  chain_writer.WriteTuple(values_);
  if (ABSL_PREDICT_FALSE(!chain_writer.Close())) {
    sink.dest()->Fail(chain_writer.status());
    return;
  }
  const Position padding =
      SaturatingSub(options_.width(), chain_writer.dest().size());
  sink.dest()->WriteChars(padding / 2, options_.fill());
  sink.dest()->Write(std::move(chain_writer.dest()));
  sink.dest()->WriteChars(padding - padding / 2, options_.fill());
}

template <typename... T>
template <
    typename Sink, typename DependentTuple,
    std::enable_if_t<
        TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiRightType<T...>::AbslStringifyImpl(Sink& sink) const {
  align_internal::WriteChars(
      sink,
      SaturatingSub(options_.width(),
                    align_internal::StringifiedSizeOfTuple(values_)),
      options_.fill());
  AbslStringifyWriter writer(&sink);
  writer.WriteTuple(values_);
  writer.Close();
}

template <typename... T>
template <
    typename Sink, typename DependentTuple,
    std::enable_if_t<
        !TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiRightType<T...>::AbslStringifyImpl(Sink& sink) const {
  RestrictedChainWriter chain_writer;
  chain_writer.WriteTuple(values_);
  if (ABSL_PREDICT_FALSE(!chain_writer.Close())) return;
  align_internal::WriteChars(
      sink, SaturatingSub(options_.width(), chain_writer.dest().size()),
      options_.fill());
  AbslStringify(sink, chain_writer.dest());
}

template <typename... T>
template <
    typename DependentTuple,
    std::enable_if_t<
        TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiRightType<T...>::AbslStringifyImpl(
    WriterAbslStringifySink& sink) const {
  sink.dest()->WriteChars(
      SaturatingSub(options_.width(),
                    align_internal::StringifiedSizeOfTuple(values_)),
      options_.fill());
  sink.dest()->WriteTuple(values_);
}

template <typename... T>
template <
    typename DependentTuple,
    std::enable_if_t<
        !TupleElementsSatisfy<DependentTuple, HasStringifiedSize>::value, int>>
inline void AsciiRightType<T...>::AbslStringifyImpl(
    WriterAbslStringifySink& sink) const {
  RestrictedChainWriter chain_writer;
  chain_writer.WriteTuple(values_);
  if (ABSL_PREDICT_FALSE(!chain_writer.Close())) {
    sink.dest()->Fail(chain_writer.status());
    return;
  }
  sink.dest()->WriteChars(
      SaturatingSub(options_.width(), chain_writer.dest().size()),
      options_.fill());
  sink.dest()->Write(std::move(chain_writer.dest()));
}

}  // namespace riegeli

#endif  // RIEGELI_TEXT_ASCII_ALIGN_H_
