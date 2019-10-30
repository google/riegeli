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

#ifndef RIEGELI_BYTES_FRAMED_SNAPPY_READER_H_
#define RIEGELI_BYTES_FRAMED_SNAPPY_READER_H_

#include <stddef.h>

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter invariant part of `FramedSnappyReader`.
class FramedSnappyReaderBase : public PullableReader {
 public:
  class Options {};

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

 protected:
  explicit FramedSnappyReaderBase(InitiallyClosed) noexcept
      : PullableReader(kInitiallyClosed) {}
  explicit FramedSnappyReaderBase(InitiallyOpen) noexcept
      : PullableReader(kInitiallyOpen) {}

  FramedSnappyReaderBase(FramedSnappyReaderBase&& that) noexcept;
  FramedSnappyReaderBase& operator=(FramedSnappyReaderBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(Reader* src);

  void Done() override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;

 private:
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // Buffered uncompressed data.
  Buffer uncompressed_;

  // Invariant if scratch is not used:
  //   `start_ == nullptr` or `start_ == uncompressed_.GetData()` or
  //   `limit_ == src_reader()->cursor()`
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
// The compressed `Reader` must not be accessed until the `FramedSnappyReader`
// is closed or no longer used.
template <typename Src = Reader*>
class FramedSnappyReader : public FramedSnappyReaderBase {
 public:
  // Creates a closed `FramedSnappyReader`.
  FramedSnappyReader() noexcept : FramedSnappyReaderBase(kInitiallyClosed) {}

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
  void Reset();
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

 protected:
  void Done() override;
  void VerifyEnd() override;

 private:
  void MoveSrc(FramedSnappyReader&& that);

  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Implementation details follow.

inline FramedSnappyReaderBase::FramedSnappyReaderBase(
    FramedSnappyReaderBase&& that) noexcept
    : PullableReader(std::move(that)),
      uncompressed_(std::move(that.uncompressed_)) {}

inline FramedSnappyReaderBase& FramedSnappyReaderBase::operator=(
    FramedSnappyReaderBase&& that) noexcept {
  PullableReader::operator=(std::move(that));
  uncompressed_ = std::move(that.uncompressed_);
  return *this;
}

inline void FramedSnappyReaderBase::Reset(InitiallyClosed) {
  PullableReader::Reset(kInitiallyClosed);
}

inline void FramedSnappyReaderBase::Reset(InitiallyOpen) {
  PullableReader::Reset(kInitiallyOpen);
}

template <typename Src>
inline FramedSnappyReader<Src>::FramedSnappyReader(const Src& src,
                                                   Options options)
    : FramedSnappyReaderBase(kInitiallyOpen), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline FramedSnappyReader<Src>::FramedSnappyReader(Src&& src, Options options)
    : FramedSnappyReaderBase(kInitiallyOpen), src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline FramedSnappyReader<Src>::FramedSnappyReader(
    std::tuple<SrcArgs...> src_args, Options options)
    : FramedSnappyReaderBase(kInitiallyOpen), src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline FramedSnappyReader<Src>::FramedSnappyReader(
    FramedSnappyReader&& that) noexcept
    : FramedSnappyReaderBase(std::move(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline FramedSnappyReader<Src>& FramedSnappyReader<Src>::operator=(
    FramedSnappyReader&& that) noexcept {
  FramedSnappyReaderBase::operator=(std::move(that));
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void FramedSnappyReader<Src>::Reset() {
  FramedSnappyReaderBase::Reset(kInitiallyClosed);
  src_.Reset();
}

template <typename Src>
inline void FramedSnappyReader<Src>::Reset(const Src& src, Options options) {
  FramedSnappyReaderBase::Reset(kInitiallyOpen);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void FramedSnappyReader<Src>::Reset(Src&& src, Options options) {
  FramedSnappyReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void FramedSnappyReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                           Options options) {
  FramedSnappyReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
inline void FramedSnappyReader<Src>::MoveSrc(FramedSnappyReader&& that) {
  // Buffer pointers are already moved so `limit_` is taken from `*this`,
  // `src_` is not moved yet so `src_` is taken from `that`.
  if (src_.kIsStable() || ABSL_PREDICT_FALSE(closed()) ||
      limit_ != that.src_->cursor()) {
    src_ = std::move(that.src_);
  } else {
    // Buffer pointers read uncompressed data from `that.src_`.
    const size_t available_length = available();
    that.src_->set_cursor(that.src_->cursor() - available_length);
    src_ = std::move(that.src_);
    if (ABSL_PREDICT_FALSE(!src_->Pull(available_length))) {
      Fail(*src_);
      return;
    }
    start_ = src_->cursor();
    cursor_ = start_;
    limit_ = start_ + available_length;
    src_->set_cursor(src_->cursor() + available_length);
  }
}

template <typename Src>
void FramedSnappyReader<Src>::Done() {
  FramedSnappyReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src>
void FramedSnappyReader<Src>::VerifyEnd() {
  FramedSnappyReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) src_->VerifyEnd();
}

template <typename Src>
struct Resetter<FramedSnappyReader<Src>>
    : ResetterByReset<FramedSnappyReader<Src>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FRAMED_SNAPPY_READER_H_
