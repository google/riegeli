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
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `FramedSnappyReader`.
class FramedSnappyReaderBase : public PullableReader {
 public:
  class Options {};

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

  bool ToleratesReadingAhead() override;
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
  bool PullBehindScratch(size_t recommended_length) override;
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
  //   `limit() == SrcReader()->cursor()`
};

// A `Reader` which decompresses data with framed Snappy format after getting
// it from another `Reader`:
// https://github.com/google/snappy/blob/master/framing_format.txt
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `Any<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
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
  explicit FramedSnappyReader(Initializer<Src> src,
                              Options options = Options());

  FramedSnappyReader(FramedSnappyReader&& that) = default;
  FramedSnappyReader& operator=(FramedSnappyReader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `FramedSnappyReader`. This
  // avoids constructing a temporary `FramedSnappyReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());

  // Returns the object providing and possibly owning the compressed `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;
  void SetReadAllHintImpl(bool read_all_hint) override;
  void VerifyEndImpl() override;

 private:
  class Mover;

  // The object providing and possibly owning the compressed `Reader`.
  MovingDependency<Reader*, Src, Mover> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit FramedSnappyReader(Closed) -> FramedSnappyReader<DeleteCtad<Closed>>;
template <typename Src>
explicit FramedSnappyReader(Src&& src, FramedSnappyReaderBase::Options options =
                                           FramedSnappyReaderBase::Options())
    -> FramedSnappyReader<InitializerTargetT<Src>>;
#endif

// Returns `true` if the data look like they have been FramedSnappy-compressed.
//
// The current position of `src` is unchanged.
bool RecognizeFramedSnappy(Reader& src);

// Implementation details follow.

inline FramedSnappyReaderBase::FramedSnappyReaderBase(
    FramedSnappyReaderBase&& that) noexcept
    : PullableReader(static_cast<PullableReader&&>(that)),
      truncated_(that.truncated_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      uncompressed_(std::move(that.uncompressed_)) {}

inline FramedSnappyReaderBase& FramedSnappyReaderBase::operator=(
    FramedSnappyReaderBase&& that) noexcept {
  PullableReader::operator=(static_cast<PullableReader&&>(that));
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
class FramedSnappyReader<Src>::Mover {
 public:
  static auto member() { return &FramedSnappyReader::src_; }

  explicit Mover(FramedSnappyReader& self, FramedSnappyReader& that)
      : behind_scratch_(&self),
        // Buffer pointers are already moved so `limit()` is taken from `self`.
        // `src_` is not moved yet so `src_` is taken from `that`.
        reads_uncompressed_(ABSL_PREDICT_TRUE(self.is_open()) &&
                            self.limit() == that.src_->cursor()) {
    if (reads_uncompressed_) {
      available_ = self.available();
      that.src_->set_cursor(that.src_->cursor() - available_);
    }
  }

  void Done(FramedSnappyReader& self) {
    if (reads_uncompressed_) {
      if (ABSL_PREDICT_FALSE(!self.src_->Pull(available_))) {
        self.FailWithoutAnnotation(self.AnnotateOverSrc(self.src_->status()));
        return;
      }
      self.set_buffer(self.src_->cursor(), available_);
      self.src_->move_cursor(available_);
    }
  }

 private:
  BehindScratch behind_scratch_;
  bool reads_uncompressed_;
  size_t available_;
};

template <typename Src>
inline FramedSnappyReader<Src>::FramedSnappyReader(
    Initializer<Src> src, ABSL_ATTRIBUTE_UNUSED Options options)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline void FramedSnappyReader<Src>::Reset(Closed) {
  FramedSnappyReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void FramedSnappyReader<Src>::Reset(
    Initializer<Src> src, ABSL_ATTRIBUTE_UNUSED Options options) {
  FramedSnappyReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void FramedSnappyReader<Src>::Done() {
  FramedSnappyReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void FramedSnappyReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  if (src_.IsOwning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void FramedSnappyReader<Src>::VerifyEndImpl() {
  FramedSnappyReaderBase::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_READER_H_
