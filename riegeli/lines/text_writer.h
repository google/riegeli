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

#ifndef RIEGELI_LINES_TEXT_WRITER_H_
#define RIEGELI_LINES_TEXT_WRITER_H_

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/utility/utility.h"
#include "riegeli/base/any_dependency.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/prefix_limiting_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/lines/newline.h"

namespace riegeli {

// Template parameter independent part of `TextWriter<newline, Dest>` when
// `newline != WriteNewline::kLf`.
class TextWriterBase : public BufferedWriter {
 public:
  using Options = BufferOptions;

  // Returns the original `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const = 0;

 protected:
  using BufferedWriter::BufferedWriter;

  TextWriterBase(TextWriterBase&& that) = default;
  TextWriterBase& operator=(TextWriterBase&& that) = default;

  void Initialize(Writer* dest);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
};

namespace text_writer_internal {

template <WriteNewline newline>
class TextWriterImpl : public TextWriterBase {
 protected:
  using TextWriterBase::TextWriterBase;

  TextWriterImpl(TextWriterImpl&& that) = default;
  TextWriterImpl& operator=(TextWriterImpl&& that) = default;

  bool WriteInternal(absl::string_view src) override;
};

extern template class TextWriterImpl<WriteNewline::kCr>;
extern template class TextWriterImpl<WriteNewline::kCrLf>;

}  // namespace text_writer_internal

template <WriteNewline newline = WriteNewline::kNative, typename Dest = Writer*>
class TextWriter : public text_writer_internal::TextWriterImpl<newline> {
 public:
  using Options = TextWriterBase::Options;

  // Creates a closed `TextWriter`.
  explicit TextWriter(Closed) noexcept : TextWriter::TextWriterImpl(kClosed) {}

  // Will write to the original `Writer` provided by `dest`.
  explicit TextWriter(Initializer<Dest> dest, Options options = Options());

  TextWriter(TextWriter&& that) noexcept;
  TextWriter& operator=(TextWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `TextWriter`. This avoids
  // constructing a temporary `TextWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the original `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Specialization of `TextWriter<newline, Dest>` when
// `newline == WriteNewline::kLf`.
//
// In contrast to the primary class template, this specialization exposes
// optional functionality of the original `Writer` (e.g. random access) and
// avoids adding a buffering layer.
template <typename Dest>
class TextWriter<WriteNewline::kLf, Dest> : public PrefixLimitingWriter<Dest> {
 public:
  using Options = TextWriterBase::Options;

  // Creates a closed `TextWriter`.
  explicit TextWriter(Closed) noexcept
      : TextWriter::PrefixLimitingWriter(kClosed) {}

  // Will write to the original `Writer` provided by `dest`.
  //
  // `options` are ignored in this class template specialization.
  explicit TextWriter(Initializer<Dest> dest, Options options = Options());

  TextWriter(TextWriter&& that) = default;
  TextWriter& operator=(TextWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `TextWriter`. This avoids
  // constructing a temporary `TextWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());
};

// Support CTAD.
#if __cpp_deduction_guides
explicit TextWriter(Closed)
    -> TextWriter<WriteNewline::kNative, DeleteCtad<Closed>>;
template <typename Dest>
explicit TextWriter(Dest&& dest,
                    TextWriterBase::Options options = TextWriterBase::Options())
    -> TextWriter<WriteNewline::kNative, std::decay_t<Dest>>;
template <typename... DestArgs>
explicit TextWriter(std::tuple<DestArgs...> dest_args,
                    TextWriterBase::Options options = TextWriterBase::Options())
    -> TextWriter<WriteNewline::kNative, DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Wraps a `TextWriter` for a line terminator specified at runtime.
template <typename Dest = Writer*>
using AnyTextWriter =
    AnyDependency<Writer*>::Inlining<TextWriter<WriteNewline::kLf, Dest>,
                                     TextWriter<WriteNewline::kCr, Dest>,
                                     TextWriter<WriteNewline::kCrLf, Dest>>;

// Options for `MakeAnyTextWriter()`.
class AnyTextWriterOptions : public BufferOptionsBase<AnyTextWriterOptions> {
 public:
  AnyTextWriterOptions() noexcept {}

  // Line terminator representation to translate from LF.
  //
  // Default: `WriteNewline::kNative`.
  AnyTextWriterOptions& set_newline(WriteNewline newline) & {
    newline_ = newline;
    return *this;
  }
  AnyTextWriterOptions&& set_newline(WriteNewline newline) && {
    return std::move(set_newline(newline));
  }
  WriteNewline newline() const { return newline_; }

 private:
  WriteNewline newline_ = WriteNewline::kNative;
};

// Factory functions for `AnyTextWriter`.
template <typename Dest,
          std::enable_if_t<
              IsValidDependency<Writer*, std::decay_t<Dest>>::value, int> = 0>
AnyTextWriter<Dest> MakeAnyTextWriter(
    Dest&& dest, AnyTextWriterOptions options = AnyTextWriterOptions());
template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest>::value, int> = 0>
AnyTextWriter<Dest> MakeAnyTextWriter(
    Initializer<Dest> dest,
    AnyTextWriterOptions options = AnyTextWriterOptions());

// Implementation details below.

inline void TextWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of TextWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
  }
}

template <WriteNewline newline, typename Dest>
inline TextWriter<newline, Dest>::TextWriter(Initializer<Dest> dest,
                                             Options options)
    : TextWriter::TextWriterImpl(options), dest_(std::move(dest)) {
  this->Initialize(dest_.get());
}

template <WriteNewline newline, typename Dest>
inline TextWriter<newline, Dest>::TextWriter(TextWriter&& that) noexcept
    : TextWriter::TextWriterImpl(
          static_cast<typename TextWriter::TextWriterImpl&&>(that)),
      dest_(std::move(that.dest_)) {}

template <WriteNewline newline, typename Dest>
inline TextWriter<newline, Dest>& TextWriter<newline, Dest>::operator=(
    TextWriter&& that) noexcept {
  TextWriter::TextWriterImpl::operator=(
      static_cast<typename TextWriter::TextWriterImpl&&>(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <WriteNewline newline, typename Dest>
inline void TextWriter<newline, Dest>::Reset(Closed) {
  TextWriter::TextWriterImpl::Reset(kClosed);
  dest_.Reset();
}

template <WriteNewline newline, typename Dest>
inline void TextWriter<newline, Dest>::Reset(Initializer<Dest> dest,
                                             Options options) {
  TextWriter::TextWriterImpl::Reset(options);
  dest_.Reset(std::move(dest));
  this->Initialize(dest_.get());
}

template <WriteNewline newline, typename Dest>
void TextWriter<newline, Dest>::Done() {
  TextWriter::TextWriterImpl::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      this->FailWithoutAnnotation(this->AnnotateOverDest(dest_->status()));
    }
  }
}

template <WriteNewline newline, typename Dest>
bool TextWriter<newline, Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!TextWriter::TextWriterImpl::FlushImpl(flush_type))) {
    return false;
  }
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
      return this->FailWithoutAnnotation(
          this->AnnotateOverDest(dest_->status()));
    }
  }
  return true;
}

template <typename Dest>
inline TextWriter<WriteNewline::kLf, Dest>::TextWriter(
    Initializer<Dest> dest, ABSL_ATTRIBUTE_UNUSED Options options)
    : TextWriter::PrefixLimitingWriter(std::move(dest)) {}

template <typename Dest>
inline void TextWriter<WriteNewline::kLf, Dest>::Reset(Closed) {
  TextWriter::PrefixLimitingWriter::Reset(kClosed);
}

template <typename Dest>
inline void TextWriter<WriteNewline::kLf, Dest>::Reset(
    Initializer<Dest> dest, ABSL_ATTRIBUTE_UNUSED Options options) {
  TextWriter::PrefixLimitingWriter::Reset(std::move(dest));
}

template <typename Dest,
          std::enable_if_t<
              IsValidDependency<Writer*, std::decay_t<Dest>>::value, int>>
AnyTextWriter<Dest> MakeAnyTextWriter(Dest&& dest,
                                      AnyTextWriterOptions options) {
  return MakeAnyTextWriter(
      Initializer<std::decay_t<Dest>>(std::forward<Dest>(dest)),
      std::move(options));
}

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest>::value, int>>
AnyTextWriter<Dest> MakeAnyTextWriter(Initializer<Dest> dest,
                                      AnyTextWriterOptions options) {
  switch (options.newline()) {
    case WriteNewline::kLf:
      return {absl::in_place_type<TextWriter<WriteNewline::kLf, Dest>>,
              std::move(dest), options.buffer_options()};
    case WriteNewline::kCr:
      return {absl::in_place_type<TextWriter<WriteNewline::kCr, Dest>>,
              std::move(dest), options.buffer_options()};
    case WriteNewline::kCrLf:
      return {absl::in_place_type<TextWriter<WriteNewline::kCrLf, Dest>>,
              std::move(dest), options.buffer_options()};
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown newline: " << static_cast<int>(options.newline());
}

}  // namespace riegeli

#endif  // RIEGELI_LINES_TEXT_WRITER_H_
