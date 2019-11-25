// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BYTES_BROTLI_READER_H_
#define RIEGELI_BYTES_BROTLI_READER_H_

#include <stddef.h>

#include <memory>
#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "brotli/decode.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `BrotliReader`.
class BrotliReaderBase : public PullableReader {
 public:
  class Options {};

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

 protected:
  explicit BrotliReaderBase(InitiallyClosed) noexcept
      : PullableReader(kInitiallyClosed) {}
  explicit BrotliReaderBase(InitiallyOpen) noexcept
      : PullableReader(kInitiallyOpen) {}

  BrotliReaderBase(BrotliReaderBase&& that) noexcept;
  BrotliReaderBase& operator=(BrotliReaderBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(Reader* src);

  void Done() override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;

 private:
  struct BrotliDecoderStateDeleter {
    void operator()(BrotliDecoderState* ptr) const {
      BrotliDecoderDestroyInstance(ptr);
    }
  };

  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // If `healthy()` but `decompressor_ == nullptr` then all data have been
  // decompressed.
  std::unique_ptr<BrotliDecoderState, BrotliDecoderStateDeleter> decompressor_;

  // Invariant if `healthy()` and scratch is not used:
  //   `start()` and `limit()` point to the buffer returned by
  //   `BrotliDecoderTakeOutput()` or are both `nullptr`
};

// A `Reader` which decompresses data with Brotli after getting it from another
// `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// The compressed `Reader` must not be accessed until the `BrotliReader` is
// closed or no longer used.
template <typename Src = Reader*>
class BrotliReader : public BrotliReaderBase {
 public:
  // Creates a closed `BrotliReader`.
  BrotliReader() noexcept : BrotliReaderBase(kInitiallyClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit BrotliReader(const Src& src, Options options = Options());
  explicit BrotliReader(Src&& src, Options options = Options());

  // Will read from the compressed `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit BrotliReader(std::tuple<SrcArgs...> src_args,
                        Options options = Options());

  BrotliReader(BrotliReader&& that) noexcept;
  BrotliReader& operator=(BrotliReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BrotliReader`. This avoids
  // constructing a temporary `BrotliReader` and moving from it.
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

// Implementation details follow.

inline BrotliReaderBase::BrotliReaderBase(BrotliReaderBase&& that) noexcept
    : PullableReader(std::move(that)),
      truncated_(that.truncated_),
      decompressor_(std::move(that.decompressor_)) {}

inline BrotliReaderBase& BrotliReaderBase::operator=(
    BrotliReaderBase&& that) noexcept {
  PullableReader::operator=(std::move(that));
  truncated_ = that.truncated_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void BrotliReaderBase::Reset(InitiallyClosed) {
  PullableReader::Reset(kInitiallyClosed);
  truncated_ = false;
  decompressor_.reset();
}

inline void BrotliReaderBase::Reset(InitiallyOpen) {
  PullableReader::Reset(kInitiallyOpen);
  truncated_ = false;
  decompressor_.reset();
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(const Src& src, Options options)
    : BrotliReaderBase(kInitiallyOpen), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(Src&& src, Options options)
    : BrotliReaderBase(kInitiallyOpen), src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline BrotliReader<Src>::BrotliReader(std::tuple<SrcArgs...> src_args,
                                       Options options)
    : BrotliReaderBase(kInitiallyOpen), src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline BrotliReader<Src>::BrotliReader(BrotliReader&& that) noexcept
    : BrotliReaderBase(std::move(that)), src_(std::move(that.src_)) {}

template <typename Src>
inline BrotliReader<Src>& BrotliReader<Src>::operator=(
    BrotliReader&& that) noexcept {
  BrotliReaderBase::operator=(std::move(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void BrotliReader<Src>::Reset() {
  BrotliReaderBase::Reset(kInitiallyClosed);
  src_.Reset();
}

template <typename Src>
inline void BrotliReader<Src>::Reset(const Src& src, Options options) {
  BrotliReaderBase::Reset(kInitiallyOpen);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void BrotliReader<Src>::Reset(Src&& src, Options options) {
  BrotliReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void BrotliReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                     Options options) {
  BrotliReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void BrotliReader<Src>::Done() {
  BrotliReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src>
void BrotliReader<Src>::VerifyEnd() {
  BrotliReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) src_->VerifyEnd();
}

template <typename Src>
struct Resetter<BrotliReader<Src>> : ResetterByReset<BrotliReader<Src>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BROTLI_READER_H_
