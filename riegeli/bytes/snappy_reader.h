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

#ifndef RIEGELI_BYTES_SNAPPY_READER_H_
#define RIEGELI_BYTES_SNAPPY_READER_H_

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter invariant part of `SnappyReader`.
class SnappyReaderBase : public ChainReader<Chain> {
 public:
  class Options {};

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

 protected:
  SnappyReaderBase(InitiallyClosed) noexcept {}

  explicit SnappyReaderBase(InitiallyOpen);

  SnappyReaderBase(SnappyReaderBase&& that) noexcept;
  SnappyReaderBase& operator=(SnappyReaderBase&& that) noexcept;

  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);
  void Initialize(Reader* src);
};

// A `Reader` which decompresses data with Snappy after getting it from another
// `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// The compressed `Reader` must support random access.
//
// The compressed `Reader` must not be accessed until the `SnappyReader` is
// closed or no longer used.
//
// `SnappyReader` does not decompress incrementally but reads compressed data
// and decompresses them all in the constructor.
//
// `SnappyReader` does not support reading from a growing source. If source is
// truncated, decompression fails.
template <typename Src = Reader*>
class SnappyReader : public SnappyReaderBase {
 public:
  // Creates a closed `SnappyReader`.
  SnappyReader() noexcept : SnappyReaderBase(kInitiallyClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit SnappyReader(const Src& src, Options options = Options());
  explicit SnappyReader(Src&& src, Options options = Options());

  // Will read from the compressed `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit SnappyReader(std::tuple<SrcArgs...> src_args,
                        Options options = Options());

  SnappyReader(SnappyReader&& that) noexcept;
  SnappyReader& operator=(SnappyReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `SnappyReader`. This avoids
  // constructing a temporary `SnappyReader` and moving from it.
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
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Implementation details follow.

inline SnappyReaderBase::SnappyReaderBase(InitiallyOpen)
    // Empty `Chain` as the `ChainReader` source is a placeholder, it will be
    // set by `Initialize()`.
    : ChainReader(std::forward_as_tuple()) {}

inline SnappyReaderBase::SnappyReaderBase(SnappyReaderBase&& that) noexcept
    : ChainReader(std::move(that)) {}

inline SnappyReaderBase& SnappyReaderBase::operator=(
    SnappyReaderBase&& that) noexcept {
  ChainReader::operator=(std::move(that));
  return *this;
}

inline void SnappyReaderBase::Reset(InitiallyClosed) { ChainReader::Reset(); }

inline void SnappyReaderBase::Reset(InitiallyOpen) {
  // Empty `Chain` as the `ChainReader` source is a placeholder, it will be set
  // by `Initialize()`.
  ChainReader::Reset(std::forward_as_tuple());
}

template <typename Src>
inline SnappyReader<Src>::SnappyReader(const Src& src, Options options)
    : SnappyReaderBase(kInitiallyOpen), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline SnappyReader<Src>::SnappyReader(Src&& src, Options options)
    : SnappyReaderBase(kInitiallyOpen), src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline SnappyReader<Src>::SnappyReader(std::tuple<SrcArgs...> src_args,
                                       Options options)
    : SnappyReaderBase(kInitiallyOpen), src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline SnappyReader<Src>::SnappyReader(SnappyReader&& that) noexcept
    : SnappyReaderBase(std::move(that)), src_(std::move(that.src_)) {}

template <typename Src>
inline SnappyReader<Src>& SnappyReader<Src>::operator=(
    SnappyReader&& that) noexcept {
  SnappyReaderBase::operator=(std::move(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void SnappyReader<Src>::Reset() {
  SnappyReaderBase::Reset(kInitiallyClosed);
  src_.Reset();
}

template <typename Src>
inline void SnappyReader<Src>::Reset(const Src& src, Options options) {
  SnappyReaderBase::Reset(kInitiallyOpen);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void SnappyReader<Src>::Reset(Src&& src, Options options) {
  SnappyReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void SnappyReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                     Options options) {
  SnappyReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void SnappyReader<Src>::Done() {
  SnappyReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src>
void SnappyReader<Src>::VerifyEnd() {
  SnappyReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) src_->VerifyEnd();
}

template <typename Src>
struct Resetter<SnappyReader<Src>> : ResetterByReset<SnappyReader<Src>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_SNAPPY_READER_H_
