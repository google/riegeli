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
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/dependency.h"
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
  virtual Reader* SrcReader() = 0;
  virtual const Reader* SrcReader() const = 0;

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

  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // Remaining number of uncompressed bytes in the current chunk.
  uint32_t remaining_chunk_length_ = 0;
  Position initial_compressed_pos_ = 0;
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
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
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
  explicit HadoopSnappyReader(const Src& src, Options options = Options());
  explicit HadoopSnappyReader(Src&& src, Options options = Options());

  // Will read from the compressed `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit HadoopSnappyReader(std::tuple<SrcArgs...> src_args,
                              Options options = Options());

  HadoopSnappyReader(HadoopSnappyReader&& that) noexcept;
  HadoopSnappyReader& operator=(HadoopSnappyReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `HadoopSnappyReader`. This
  // avoids constructing a temporary `HadoopSnappyReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Src& src,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src,
                                          Options options = Options());
  template <typename... SrcArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<SrcArgs...> src_args,
                                          Options options = Options());

  // Returns the object providing and possibly owning the compressed `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() override { return src_.get(); }
  const Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;
  void SetReadAllHintImpl(bool read_all_hint) override;
  void VerifyEndImpl() override;

 private:
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit HadoopSnappyReader(Closed)->HadoopSnappyReader<DeleteCtad<Closed>>;
template <typename Src>
explicit HadoopSnappyReader(
    const Src& src,
    HadoopSnappyReaderBase::Options options = HadoopSnappyReaderBase::Options())
    -> HadoopSnappyReader<std::decay_t<Src>>;
template <typename Src>
explicit HadoopSnappyReader(Src&& src, HadoopSnappyReaderBase::Options options =
                                           HadoopSnappyReaderBase::Options())
    -> HadoopSnappyReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit HadoopSnappyReader(
    std::tuple<SrcArgs...> src_args,
    HadoopSnappyReaderBase::Options options = HadoopSnappyReaderBase::Options())
    -> HadoopSnappyReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline HadoopSnappyReaderBase::HadoopSnappyReaderBase(
    HadoopSnappyReaderBase&& that) noexcept
    : PullableReader(static_cast<PullableReader&&>(that)),
      truncated_(that.truncated_),
      remaining_chunk_length_(that.remaining_chunk_length_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      uncompressed_(std::move(that.uncompressed_)) {}

inline HadoopSnappyReaderBase& HadoopSnappyReaderBase::operator=(
    HadoopSnappyReaderBase&& that) noexcept {
  PullableReader::operator=(static_cast<PullableReader&&>(that));
  truncated_ = that.truncated_;
  remaining_chunk_length_ = that.remaining_chunk_length_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  uncompressed_ = std::move(that.uncompressed_);
  return *this;
}

inline void HadoopSnappyReaderBase::Reset(Closed) {
  PullableReader::Reset(kClosed);
  truncated_ = false;
  remaining_chunk_length_ = 0;
  initial_compressed_pos_ = 0;
  uncompressed_ = Buffer();
}

inline void HadoopSnappyReaderBase::Reset() {
  PullableReader::Reset();
  truncated_ = false;
  remaining_chunk_length_ = 0;
  initial_compressed_pos_ = 0;
}

template <typename Src>
inline HadoopSnappyReader<Src>::HadoopSnappyReader(const Src& src,
                                                   Options options)
    : src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline HadoopSnappyReader<Src>::HadoopSnappyReader(Src&& src, Options options)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline HadoopSnappyReader<Src>::HadoopSnappyReader(
    std::tuple<SrcArgs...> src_args, Options options)
    : src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline HadoopSnappyReader<Src>::HadoopSnappyReader(
    HadoopSnappyReader&& that) noexcept
    : HadoopSnappyReaderBase(static_cast<HadoopSnappyReaderBase&&>(that)) {
  src_ = std::move(that.src_);
}

template <typename Src>
inline HadoopSnappyReader<Src>& HadoopSnappyReader<Src>::operator=(
    HadoopSnappyReader&& that) noexcept {
  HadoopSnappyReaderBase::operator=(
      static_cast<HadoopSnappyReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void HadoopSnappyReader<Src>::Reset(Closed) {
  HadoopSnappyReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void HadoopSnappyReader<Src>::Reset(const Src& src, Options options) {
  HadoopSnappyReaderBase::Reset();
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void HadoopSnappyReader<Src>::Reset(Src&& src, Options options) {
  HadoopSnappyReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void HadoopSnappyReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                           Options options) {
  HadoopSnappyReaderBase::Reset();
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void HadoopSnappyReader<Src>::Done() {
  HadoopSnappyReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void HadoopSnappyReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  if (src_.is_owning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void HadoopSnappyReader<Src>::VerifyEndImpl() {
  HadoopSnappyReaderBase::VerifyEndImpl();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_READER_H_
