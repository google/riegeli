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
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `HadoopSnappyReader`.
class HadoopSnappyReaderBase : public PullableReader {
 public:
  class Options {};

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // `HadoopSnappyReaderBase` overrides `Reader::Fail()` to annotate the status
  // with the current position, clarifying that this is the uncompressed
  // position. A status propagated from `*src_reader()` might carry annotation
  // with the compressed position.
  using PullableReader::Fail;
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status) override;

 protected:
  explicit HadoopSnappyReaderBase(InitiallyClosed) noexcept
      : PullableReader(kInitiallyClosed) {}
  explicit HadoopSnappyReaderBase(InitiallyOpen) noexcept
      : PullableReader(kInitiallyOpen) {}

  HadoopSnappyReaderBase(HadoopSnappyReaderBase&& that) noexcept;
  HadoopSnappyReaderBase& operator=(HadoopSnappyReaderBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(Reader* src);

  void Done() override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailInvalidStream(absl::string_view message);

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
  HadoopSnappyReader() noexcept : HadoopSnappyReaderBase(kInitiallyClosed) {}

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

  void VerifyEnd() override;

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Src>
HadoopSnappyReader(Src&& src, HadoopSnappyReaderBase::Options options =
                                  HadoopSnappyReaderBase::Options())
    -> HadoopSnappyReader<std::decay_t<Src>>;
template <typename... SrcArgs>
HadoopSnappyReader(
    std::tuple<SrcArgs...> src_args,
    HadoopSnappyReaderBase::Options options = HadoopSnappyReaderBase::Options())
    -> HadoopSnappyReader<void>;  // Delete.
#endif

// Implementation details follow.

inline HadoopSnappyReaderBase::HadoopSnappyReaderBase(
    HadoopSnappyReaderBase&& that) noexcept
    : PullableReader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      truncated_(that.truncated_),
      remaining_chunk_length_(that.remaining_chunk_length_),
      uncompressed_(std::move(that.uncompressed_)) {}

inline HadoopSnappyReaderBase& HadoopSnappyReaderBase::operator=(
    HadoopSnappyReaderBase&& that) noexcept {
  PullableReader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  truncated_ = that.truncated_;
  remaining_chunk_length_ = that.remaining_chunk_length_;
  uncompressed_ = std::move(that.uncompressed_);
  return *this;
}

inline void HadoopSnappyReaderBase::Reset(InitiallyClosed) {
  PullableReader::Reset(kInitiallyClosed);
  truncated_ = false;
  remaining_chunk_length_ = 0;
}

inline void HadoopSnappyReaderBase::Reset(InitiallyOpen) {
  PullableReader::Reset(kInitiallyOpen);
  truncated_ = false;
  remaining_chunk_length_ = 0;
}

template <typename Src>
inline HadoopSnappyReader<Src>::HadoopSnappyReader(const Src& src,
                                                   Options options)
    : HadoopSnappyReaderBase(kInitiallyOpen), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline HadoopSnappyReader<Src>::HadoopSnappyReader(Src&& src, Options options)
    : HadoopSnappyReaderBase(kInitiallyOpen), src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline HadoopSnappyReader<Src>::HadoopSnappyReader(
    std::tuple<SrcArgs...> src_args, Options options)
    : HadoopSnappyReaderBase(kInitiallyOpen), src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline HadoopSnappyReader<Src>::HadoopSnappyReader(
    HadoopSnappyReader&& that) noexcept
    : HadoopSnappyReaderBase(std::move(that)) {
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
}

template <typename Src>
inline HadoopSnappyReader<Src>& HadoopSnappyReader<Src>::operator=(
    HadoopSnappyReader&& that) noexcept {
  HadoopSnappyReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void HadoopSnappyReader<Src>::Reset() {
  HadoopSnappyReaderBase::Reset(kInitiallyClosed);
  src_.Reset();
}

template <typename Src>
inline void HadoopSnappyReader<Src>::Reset(const Src& src, Options options) {
  HadoopSnappyReaderBase::Reset(kInitiallyOpen);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void HadoopSnappyReader<Src>::Reset(Src&& src, Options options) {
  HadoopSnappyReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void HadoopSnappyReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                           Options options) {
  HadoopSnappyReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void HadoopSnappyReader<Src>::Done() {
  HadoopSnappyReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src>
void HadoopSnappyReader<Src>::VerifyEnd() {
  HadoopSnappyReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_READER_H_
