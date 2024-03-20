// Copyright 2022 Google LLC
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

#ifndef RIEGELI_LINES_TEXT_READER_H_
#define RIEGELI_LINES_TEXT_READER_H_

#include <stddef.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/any_dependency.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/prefix_limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/lines/newline.h"

namespace riegeli {

// Template parameter independent part of `TextReader<newline, Src>` when
// `newline != ReadNewline::kLf`.
class TextReaderBase : public BufferedReader {
 public:
  using Options = BufferOptions;

  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

  bool ToleratesReadingAhead() override;
  bool SupportsRewind() override;

 protected:
  using BufferedReader::BufferedReader;

  TextReaderBase(TextReaderBase&& that) = default;
  TextReaderBase& operator=(TextReaderBase&& that) = default;

  void Initialize(Reader* src);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool SeekBehindBuffer(Position new_pos) override;

 private:
  Position initial_original_pos_ = 0;
};

namespace text_reader_internal {

template <ReadNewline newline>
class TextReaderImpl;

template <>
class TextReaderImpl<ReadNewline::kCrLfOrLf> : public TextReaderBase {
 protected:
  using TextReaderBase::TextReaderBase;

  TextReaderImpl(TextReaderImpl&& that) = default;
  TextReaderImpl& operator=(TextReaderImpl&& that) = default;

  void Initialize(Reader* src);

  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekBehindBuffer(Position new_pos) override;

 private:
  // If `true`, a CR at the end of a buffer has been read from the source.
  // If LF follows in the source, it will be skipped and LF will be written to
  // the destination, otherwise CR will be written to the destination.
  bool pending_cr_ = false;
};

template <>
class TextReaderImpl<ReadNewline::kAny> : public TextReaderBase {
 protected:
  using TextReaderBase::TextReaderBase;

  TextReaderImpl(TextReaderImpl&& that) = default;
  TextReaderImpl& operator=(TextReaderImpl&& that) = default;

  void Initialize(Reader* src);

  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekBehindBuffer(Position new_pos) override;

 private:
  // If `true`, a CR at the end of a buffer has been read from the source and
  // LF has been written to the destination. If LF follows in the source, it
  // will be skipped.
  bool pending_cr_ = false;
};

}  // namespace text_reader_internal

// A `Reader` which converts line terminators from the given representation to
// LF after getting data from another `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the second template argument can be deduced as the value
// type of the first constructor argument. This requires C++17.
//
// The original `Reader` must not be accessed until the `TextReader` is closed
// or no longer used.
//
// This primary class template is used when `newline != ReadNewline::kLf`.
template <ReadNewline newline = ReadNewline::kNative, typename Src = Reader*>
class TextReader : public text_reader_internal::TextReaderImpl<newline> {
 public:
  using Options = TextReaderBase::Options;

  // Creates a closed `TextReader`.
  explicit TextReader(Closed) noexcept : TextReader::TextReaderImpl(kClosed) {}

  // Will read from the original `Reader` provided by `src`.
  explicit TextReader(Initializer<Src> src, Options options = Options());

  TextReader(TextReader&& that) noexcept;
  TextReader& operator=(TextReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `TextReader`. This avoids
  // constructing a temporary `TextReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;
  void SetReadAllHintImpl(bool read_all_hint) override;
  void VerifyEndImpl() override;

 private:
  // The object providing and possibly owning the original `Reader`.
  Dependency<Reader*, Src> src_;
};

// Specialization of `TextReader<newline, Src>` when
// `newline == ReadNewline::kLf`.
//
// In contrast to the primary class template, this specialization exposes
// optional functionality of the original `Reader` (e.g. random access) and
// avoids adding a buffering layer.
template <typename Src>
class TextReader<ReadNewline::kLf, Src> : public PrefixLimitingReader<Src> {
 public:
  using Options = TextReaderBase::Options;

  // Creates a closed `TextReader`.
  explicit TextReader(Closed) noexcept
      : TextReader::PrefixLimitingReader(kClosed) {}

  // Will read from the original `Reader` provided by `src`.
  //
  // `options` are ignored in this class template specialization.
  explicit TextReader(Initializer<Src> src, Options options = Options());

  TextReader(TextReader&& that) = default;
  TextReader& operator=(TextReader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `TextReader`. This avoids
  // constructing a temporary `TextReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());
};

// Support CTAD.
#if __cpp_deduction_guides
explicit TextReader(Closed)
    -> TextReader<ReadNewline::kNative, DeleteCtad<Closed>>;
template <typename Src>
explicit TextReader(Src&& src,
                    TextReaderBase::Options options = TextReaderBase::Options())
    -> TextReader<ReadNewline::kNative, std::decay_t<Src>>;
template <typename... SrcArgs>
explicit TextReader(std::tuple<SrcArgs...> src_args,
                    TextReaderBase::Options options = TextReaderBase::Options())
    -> TextReader<ReadNewline::kNative, DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Wraps a `TextReader` for a line terminator specified at runtime.
template <typename Src = Reader*>
using AnyTextReader =
    AnyDependency<Reader*>::Inlining<TextReader<ReadNewline::kLf, Src>,
                                     TextReader<ReadNewline::kCrLfOrLf, Src>,
                                     TextReader<ReadNewline::kAny, Src>>;

// Options for `MakeAnyTextReader()`.
class AnyTextReaderOptions : public BufferOptionsBase<AnyTextReaderOptions> {
 public:
  AnyTextReaderOptions() noexcept {}

  // Line terminator representation to translate from LF.
  //
  // Default: `ReadNewline::kNative`.
  AnyTextReaderOptions& set_newline(ReadNewline newline) & {
    newline_ = newline;
    return *this;
  }
  AnyTextReaderOptions&& set_newline(ReadNewline newline) && {
    return std::move(set_newline(newline));
  }
  ReadNewline newline() const { return newline_; }

 private:
  ReadNewline newline_ = ReadNewline::kNative;
};

// Factory functions for `AnyTextReader`.
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, std::decay_t<Src>>::value,
                           int> = 0>
AnyTextReader<Src> MakeAnyTextReader(
    Src&& src, AnyTextReaderOptions options = AnyTextReaderOptions());
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src>::value, int> = 0>
AnyTextReader<Src> MakeAnyTextReader(
    Initializer<Src> src,
    AnyTextReaderOptions options = AnyTextReaderOptions());

// Implementation details below.

template <ReadNewline newline, typename Src>
inline TextReader<newline, Src>::TextReader(Initializer<Src> src,
                                            Options options)
    : TextReader::TextReaderImpl(options), src_(std::move(src)) {
  this->Initialize(src_.get());
}

template <ReadNewline newline, typename Src>
inline TextReader<newline, Src>::TextReader(TextReader&& that) noexcept
    : TextReader::TextReaderImpl(
          static_cast<typename TextReader::TextReaderImpl&&>(that)),
      src_(std::move(that.src_)) {}

template <ReadNewline newline, typename Src>
inline TextReader<newline, Src>& TextReader<newline, Src>::operator=(
    TextReader&& that) noexcept {
  TextReader::TextReaderImpl::operator=(
      static_cast<typename TextReader::TextReaderImpl&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <ReadNewline newline, typename Src>
inline void TextReader<newline, Src>::Reset(Closed) {
  TextReader::TextReaderImpl::Reset(kClosed);
  src_.Reset();
}

template <ReadNewline newline, typename Src>
inline void TextReader<newline, Src>::Reset(Initializer<Src> src,
                                            Options options) {
  TextReader::TextReaderImpl::Reset(options);
  src_.Reset(std::move(src));
  this->Initialize(src_.get());
}

template <ReadNewline newline, typename Src>
void TextReader<newline, Src>::Done() {
  TextReader::TextReaderImpl::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      this->FailWithoutAnnotation(this->AnnotateOverSrc(src_->status()));
    }
  }
}

template <ReadNewline newline, typename Src>
void TextReader<newline, Src>::SetReadAllHintImpl(bool read_all_hint) {
  TextReader::TextReaderImpl::SetReadAllHintImpl(read_all_hint);
  if (src_.IsOwning()) src_->SetReadAllHint(read_all_hint);
}

template <ReadNewline newline, typename Src>
void TextReader<newline, Src>::VerifyEndImpl() {
  TextReader::TextReaderImpl::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(this->ok())) src_->VerifyEnd();
}

template <typename Src>
inline TextReader<ReadNewline::kLf, Src>::TextReader(
    Initializer<Src> src, ABSL_ATTRIBUTE_UNUSED Options options)
    : TextReader::PrefixLimitingReader(std::move(src)) {}

template <typename Src>
inline void TextReader<ReadNewline::kLf, Src>::Reset(Closed) {
  TextReader::PrefixLimitingReader::Reset(kClosed);
}

template <typename Src>
inline void TextReader<ReadNewline::kLf, Src>::Reset(
    Initializer<Src> src, ABSL_ATTRIBUTE_UNUSED Options options) {
  TextReader::PrefixLimitingReader::Reset(std::move(src));
}

template <
    typename Src,
    std::enable_if_t<IsValidDependency<Reader*, std::decay_t<Src>>::value, int>>
AnyTextReader<Src> MakeAnyTextReader(Src&& src, AnyTextReaderOptions options) {
  return MakeAnyTextReader(
      Initializer<std::decay_t<Src>>(std::forward<Src>(src)),
      std::move(options));
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src>::value, int>>
AnyTextReader<Src> MakeAnyTextReader(Initializer<Src> src,
                                     AnyTextReaderOptions options) {
  AnyTextReader<Src> result;
  switch (options.newline()) {
    case ReadNewline::kLf:
      result.template Emplace<TextReader<ReadNewline::kLf, Src>>(
          std::move(src), options.buffer_options());
      return result;
    case ReadNewline::kCrLfOrLf:
      result.template Emplace<TextReader<ReadNewline::kCrLfOrLf, Src>>(
          std::move(src), options.buffer_options());
      return result;
    case ReadNewline::kAny:
      result.template Emplace<TextReader<ReadNewline::kAny, Src>>(
          std::move(src), options.buffer_options());
      return result;
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown newline: " << static_cast<int>(options.newline());
}

}  // namespace riegeli

#endif  // RIEGELI_LINES_TEXT_READER_H_
