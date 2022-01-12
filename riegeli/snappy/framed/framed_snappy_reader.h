// Copyright 2019 Google LLC
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

#ifndef RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_READER_H_
#define RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_READER_H_

#include <stddef.h>

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `FramedSnappyReader`.
class FramedSnappyReaderBase : public PullableReader {
 public:
  class Options {};

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  bool SupportsRewind() override;
  bool SupportsNewReader() override;

 protected:
  using PullableReader::PullableReader;

  FramedSnappyReaderBase(FramedSnappyReaderBase&& that) noexcept;
  FramedSnappyReaderBase& operator=(FramedSnappyReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Reader* src);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PullBehindScratch() override;
  bool SeekBehindScratch(Position new_pos) override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailInvalidStream(absl::string_view message);

  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  Position initial_compressed_pos_ = 0;
  // Buffered uncompressed data.
  Buffer uncompressed_;

  // Invariant if scratch is not used:
  //   `start() == nullptr` or `start() == uncompressed_.data()` or
  //   `limit() == src_reader()->cursor()`
};

// A `Reader` which decompresses data with framed Snappy format after getting
// it from another `Reader`:
// https://github.com/google/snappy/blob/master/framing_format.txt
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The compressed `Reader` must not be accessed until the `FramedSnappyReader`
// is closed or no longer used.
template <typename Src = Reader*>
class FramedSnappyReader : public FramedSnappyReaderBase {
 public:
  // Creates a closed `FramedSnappyReader`.
  explicit FramedSnappyReader(Closed) noexcept
      : FramedSnappyReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit FramedSnappyReader(const Src& src, Options options = Options());
  explicit FramedSnappyReader(Src&& src, Options options = Options());

  // Will read from the compressed `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit FramedSnappyReader(std::tuple<SrcArgs...> src_args,
                              Options options = Options());

  FramedSnappyReader(FramedSnappyReader&& that) noexcept;
  FramedSnappyReader& operator=(FramedSnappyReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FramedSnappyReader`. This
  // avoids constructing a temporary `FramedSnappyReader` and moving from it.
  void Reset(Closed);
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());

  // Returns the object providing and possibly owning the compressed `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

  void VerifyEnd() override;

 protected:
  void Done() override;

 private:
  void MoveSrc(FramedSnappyReader&& that);

  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit FramedSnappyReader(Closed)->FramedSnappyReader<DeleteCtad<Closed>>;
template <typename Src>
explicit FramedSnappyReader(
    const Src& src,
    FramedSnappyReaderBase::Options options = FramedSnappyReaderBase::Options())
    -> FramedSnappyReader<std::decay_t<Src>>;
template <typename Src>
explicit FramedSnappyReader(Src&& src, FramedSnappyReaderBase::Options options =
                                           FramedSnappyReaderBase::Options())
    -> FramedSnappyReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit FramedSnappyReader(
    std::tuple<SrcArgs...> src_args,
    FramedSnappyReaderBase::Options options = FramedSnappyReaderBase::Options())
    -> FramedSnappyReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline FramedSnappyReaderBase::FramedSnappyReaderBase(
    FramedSnappyReaderBase&& that) noexcept
    : PullableReader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      truncated_(that.truncated_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      uncompressed_(std::move(that.uncompressed_)) {}

inline FramedSnappyReaderBase& FramedSnappyReaderBase::operator=(
    FramedSnappyReaderBase&& that) noexcept {
  PullableReader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  truncated_ = that.truncated_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  uncompressed_ = std::move(that.uncompressed_);
  return *this;
}

inline void FramedSnappyReaderBase::Reset(Closed) {
  PullableReader::Reset(kClosed);
  truncated_ = false;
  initial_compressed_pos_ = 0;
  uncompressed_ = Buffer();
}

inline void FramedSnappyReaderBase::Reset() {
  PullableReader::Reset();
  truncated_ = false;
  initial_compressed_pos_ = 0;
}

template <typename Src>
inline FramedSnappyReader<Src>::FramedSnappyReader(const Src& src,
                                                   Options options)
    : src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline FramedSnappyReader<Src>::FramedSnappyReader(Src&& src, Options options)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline FramedSnappyReader<Src>::FramedSnappyReader(
    std::tuple<SrcArgs...> src_args, Options options)
    : src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline FramedSnappyReader<Src>::FramedSnappyReader(
    FramedSnappyReader&& that) noexcept
    : FramedSnappyReaderBase(std::move(that)) {
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveSrc(std::move(that));
}

template <typename Src>
inline FramedSnappyReader<Src>& FramedSnappyReader<Src>::operator=(
    FramedSnappyReader&& that) noexcept {
  FramedSnappyReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void FramedSnappyReader<Src>::Reset(Closed) {
  FramedSnappyReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void FramedSnappyReader<Src>::Reset(const Src& src, Options options) {
  FramedSnappyReaderBase::Reset();
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void FramedSnappyReader<Src>::Reset(Src&& src, Options options) {
  FramedSnappyReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void FramedSnappyReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                           Options options) {
  FramedSnappyReaderBase::Reset();
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
inline void FramedSnappyReader<Src>::MoveSrc(FramedSnappyReader&& that) {
  // Buffer pointers are already moved so `limit()` is taken from `*this`,
  // `src_` is not moved yet so `src_` is taken from `that`.
  if (src_.kIsStable() || ABSL_PREDICT_FALSE(!is_open()) ||
      limit() != that.src_->cursor()) {
    src_ = std::move(that.src_);
  } else {
    // Buffer pointers read uncompressed data from `that.src_`.
    const size_t available_length = available();
    that.src_->set_cursor(that.src_->cursor() - available_length);
    src_ = std::move(that.src_);
    if (ABSL_PREDICT_FALSE(!src_->Pull(available_length))) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
      return;
    }
    set_buffer(src_->cursor(), available_length);
    src_->move_cursor(available_length);
  }
}

template <typename Src>
void FramedSnappyReader<Src>::Done() {
  FramedSnappyReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void FramedSnappyReader<Src>::VerifyEnd() {
  FramedSnappyReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_READER_H_
