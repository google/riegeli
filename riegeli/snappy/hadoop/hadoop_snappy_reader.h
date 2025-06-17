// Copyright 2020 Google LLC
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

#ifndef RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_READER_H_
#define RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_READER_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `HadoopSnappyReader`.
class HadoopSnappyReaderBase : public PullableReader {
 public:
  class Options {};

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  bool ToleratesReadingAhead() override;
  bool SupportsRewind() override;
  bool SupportsNewReader() override;

 protected:
  using PullableReader::PullableReader;

  HadoopSnappyReaderBase(HadoopSnappyReaderBase&& that) noexcept;
  HadoopSnappyReaderBase& operator=(HadoopSnappyReaderBase&& that) noexcept;

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

  Position initial_compressed_pos_ = 0;
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // Remaining number of uncompressed bytes in the current chunk.
  uint32_t remaining_chunk_length_ = 0;
  // Buffered uncompressed data.
  Buffer uncompressed_;

  // Invariant if scratch is not used:
  //   `start() == nullptr` or `start() == uncompressed_.data()`
};

// A `Reader` which decompresses data with Hadoop Snappy format after getting
// it from another `Reader`:
// https://github.com/kubo/snzip#hadoop-snappy-format
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `Any<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
//
// The compressed `Reader` must not be accessed until the `HadoopSnappyReader`
// is closed or no longer used.
template <typename Src = Reader*>
class HadoopSnappyReader : public HadoopSnappyReaderBase {
 public:
  // Creates a closed `HadoopSnappyReader`.
  explicit HadoopSnappyReader(Closed) noexcept
      : HadoopSnappyReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit HadoopSnappyReader(Initializer<Src> src,
                              Options options = Options());

  HadoopSnappyReader(HadoopSnappyReader&& that) = default;
  HadoopSnappyReader& operator=(HadoopSnappyReader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `HadoopSnappyReader`. This
  // avoids constructing a temporary `HadoopSnappyReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src,
                                          Options options = Options());

  // Returns the object providing and possibly owning the compressed `Reader`.
  // Unchanged by `Close()`.
  Src& src() ABSL_ATTRIBUTE_LIFETIME_BOUND { return src_.manager(); }
  const Src& src() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return src_.manager();
  }
  Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return src_.get();
  }

 protected:
  void Done() override;
  void SetReadAllHintImpl(bool read_all_hint) override;
  void VerifyEndImpl() override;

 private:
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

explicit HadoopSnappyReader(Closed) -> HadoopSnappyReader<DeleteCtad<Closed>>;
template <typename Src>
explicit HadoopSnappyReader(Src&& src, HadoopSnappyReaderBase::Options options =
                                           HadoopSnappyReaderBase::Options())
    -> HadoopSnappyReader<TargetT<Src>>;

// Implementation details follow.

inline HadoopSnappyReaderBase::HadoopSnappyReaderBase(
    HadoopSnappyReaderBase&& that) noexcept
    : PullableReader(static_cast<PullableReader&&>(that)),
      initial_compressed_pos_(that.initial_compressed_pos_),
      truncated_(that.truncated_),
      remaining_chunk_length_(that.remaining_chunk_length_),
      uncompressed_(std::move(that.uncompressed_)) {}

inline HadoopSnappyReaderBase& HadoopSnappyReaderBase::operator=(
    HadoopSnappyReaderBase&& that) noexcept {
  PullableReader::operator=(static_cast<PullableReader&&>(that));
  initial_compressed_pos_ = that.initial_compressed_pos_;
  truncated_ = that.truncated_;
  remaining_chunk_length_ = that.remaining_chunk_length_;
  uncompressed_ = std::move(that.uncompressed_);
  return *this;
}

inline void HadoopSnappyReaderBase::Reset(Closed) {
  PullableReader::Reset(kClosed);
  initial_compressed_pos_ = 0;
  truncated_ = false;
  remaining_chunk_length_ = 0;
  uncompressed_ = Buffer();
}

inline void HadoopSnappyReaderBase::Reset() {
  PullableReader::Reset();
  initial_compressed_pos_ = 0;
  truncated_ = false;
  remaining_chunk_length_ = 0;
}

template <typename Src>
inline HadoopSnappyReader<Src>::HadoopSnappyReader(
    Initializer<Src> src, ABSL_ATTRIBUTE_UNUSED Options options)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline void HadoopSnappyReader<Src>::Reset(Closed) {
  HadoopSnappyReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void HadoopSnappyReader<Src>::Reset(
    Initializer<Src> src, ABSL_ATTRIBUTE_UNUSED Options options) {
  HadoopSnappyReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void HadoopSnappyReader<Src>::Done() {
  HadoopSnappyReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void HadoopSnappyReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  if (src_.IsOwning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void HadoopSnappyReader<Src>::VerifyEndImpl() {
  HadoopSnappyReaderBase::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_READER_H_
