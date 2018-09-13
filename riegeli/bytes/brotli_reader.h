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

#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "brotli/decode.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter invariant part of BrotliReader.
class BrotliReaderBase : public Reader {
 public:
  class Options {};

  // Returns the compressed Reader. Unchanged by Close().
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

 protected:
  explicit BrotliReaderBase(State state) noexcept : Reader(state) {}

  BrotliReaderBase(BrotliReaderBase&& that) noexcept;
  BrotliReaderBase& operator=(BrotliReaderBase&& that) noexcept;

  void Initialize();
  void Done() override;
  bool PullSlow() override;

 private:
  struct BrotliDecoderStateDeleter {
    void operator()(BrotliDecoderState* ptr) const {
      BrotliDecoderDestroyInstance(ptr);
    }
  };

  // If true, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, Close() will
  // fail.
  bool truncated_ = false;
  // If healthy() but decompressor_ == nullptr then all data have been
  // decompressed.
  std::unique_ptr<BrotliDecoderState, BrotliDecoderStateDeleter> decompressor_;
};

// A Reader which decompresses data with Brotli after getting it from another
// Reader.
//
// The Src template parameter specifies the type of the object providing and
// possibly owning the compressed Reader. Src must support
// Dependency<Reader*, Src>, e.g. Reader* (not owned, default),
// unique_ptr<Reader> (owned), ChainReader<> (owned).
//
// The compressed Reader must not be accessed until the BrotliReader is closed
// or no longer used.
template <typename Src = Reader*>
class BrotliReader : public BrotliReaderBase {
 public:
  // Creates a closed BrotliReader.
  BrotliReader() noexcept : BrotliReaderBase(State::kClosed) {}

  // Will read from the compressed Reader provided by src.
  explicit BrotliReader(Src src, Options options = Options());

  BrotliReader(BrotliReader&& that) noexcept;
  BrotliReader& operator=(BrotliReader&& that) noexcept;

  // Returns the object providing and possibly owning the compressed Reader.
  // Unchanged by Close().
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.ptr(); }
  const Reader* src_reader() const override { return src_.ptr(); }

 protected:
  void Done() override;
  void VerifyEnd() override;

 private:
  // The object providing and possibly owning the compressed Reader.
  Dependency<Reader*, Src> src_;

  // Invariant:
  //   cursor_ and limit_ point inside the buffer returned by
  //   BrotliDecoderTakeOutput() or are both nullptr
};

// Implementation details follow.

inline BrotliReaderBase::BrotliReaderBase(BrotliReaderBase&& that) noexcept
    : Reader(std::move(that)),
      truncated_(riegeli::exchange(that.truncated_, false)),
      decompressor_(std::move(that.decompressor_)) {}

inline BrotliReaderBase& BrotliReaderBase::operator=(
    BrotliReaderBase&& that) noexcept {
  Reader::operator=(std::move(that));
  truncated_ = riegeli::exchange(that.truncated_, false);
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

template <typename Src>
BrotliReader<Src>::BrotliReader(Src src, Options options)
    : BrotliReaderBase(State::kOpen), src_(std::move(src)) {
  RIEGELI_ASSERT(src_.ptr() != nullptr)
      << "Failed precondition of BrotliReader<Src>::BrotliReader(Src): "
         "null Reader pointer";
  Initialize();
}

template <typename Src>
BrotliReader<Src>::BrotliReader(BrotliReader&& that) noexcept
    : BrotliReaderBase(std::move(that)), src_(std::move(that.src_)) {}

template <typename Src>
BrotliReader<Src>& BrotliReader<Src>::operator=(BrotliReader&& that) noexcept {
  BrotliReaderBase::operator=(std::move(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
void BrotliReader<Src>::Done() {
  BrotliReaderBase::Done();
  if (src_.kIsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src>
void BrotliReader<Src>::VerifyEnd() {
  BrotliReaderBase::VerifyEnd();
  if (src_.kIsOwning()) src_->VerifyEnd();
}

extern template class BrotliReader<Reader*>;
extern template class BrotliReader<std::unique_ptr<Reader>>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BROTLI_READER_H_
