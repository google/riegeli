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
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `SnappyReader`.
class SnappyReaderBase : public ChainReader<Chain> {
 public:
  class Options {};

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // `SnappyReaderBase` overrides `Reader::Fail()` to annotate the status with
  // the current position, clarifying that this is the uncompressed position.
  // A status propagated from `*src_reader()` might carry annotation with the
  // compressed position.
  using ChainReader::Fail;
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status) override;

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
// The compressed `Reader` must support `Size()`.
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

  void VerifyEnd() override;

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// An alternative interface to Snappy which avoids buffering uncompressed data.
// Calling `SnappyDecompress()` is equivalent to copying all data from a
// `SnappyReader<Src>` to `dest`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the uncompressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// The compressed `Reader` must support `Size()`.
template <typename Src, typename Dest>
absl::Status SnappyDecompress(const Src& src, const Dest& dest);
template <typename Src, typename Dest>
absl::Status SnappyDecompress(const Src& src, Dest&& dest);
template <typename Src, typename Dest, typename... DestArgs>
absl::Status SnappyDecompress(const Src& src,
                              std::tuple<DestArgs...> dest_args);
template <typename Src, typename Dest>
absl::Status SnappyDecompress(Src&& src, const Dest& dest);
template <typename Src, typename Dest>
absl::Status SnappyDecompress(Src&& src, Dest&& dest);
template <typename Src, typename Dest, typename... DestArgs>
absl::Status SnappyDecompress(Src&& src, std::tuple<DestArgs...> dest_args);
template <typename Src, typename Dest, typename... SrcArgs>
absl::Status SnappyDecompress(std::tuple<SrcArgs...> src_args,
                              const Dest& dest);
template <typename Src, typename Dest, typename... SrcArgs>
absl::Status SnappyDecompress(std::tuple<SrcArgs...> src_args, Dest&& dest);
template <typename Src, typename Dest, typename... SrcArgs,
          typename... DestArgs>
absl::Status SnappyDecompress(std::tuple<SrcArgs...> src_args,
                              std::tuple<DestArgs...> dest_args);

// Returns the claimed decompressed size of Snappy-compressed data.
//
// Return values:
//  * `true`  - success (`*size` is set)
//  * `false` - failure
bool SnappyDecompressedSize(Reader* src, size_t* size);

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

namespace internal {

absl::Status SnappyDecompressImpl(Reader* src, Writer* dest);

template <typename Src, typename Dest>
inline absl::Status SnappyDecompressImpl(Dependency<Reader*, Src> src,
                                         Dependency<Writer*, Dest> dest) {
  absl::Status status = SnappyDecompressImpl(src.get(), dest.get());
  if (dest.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = dest->status();
    }
  }
  if (src.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = src->status();
    }
  }
  return status;
}

}  // namespace internal

template <typename Src, typename Dest>
inline absl::Status SnappyDecompress(const Src& src, const Dest& dest) {
  return internal::SnappyDecompressImpl(Dependency<Reader*, Src>(src),
                                        Dependency<Writer*, Dest>(dest));
}

template <typename Src, typename Dest>
inline absl::Status SnappyDecompress(const Src& src, Dest&& dest) {
  return internal::SnappyDecompressImpl(
      Dependency<Reader*, Src>(src),
      Dependency<Writer*, std::decay_t<Dest>>(std::forward<Dest>(dest)));
}

template <typename Src, typename Dest, typename... DestArgs>
inline absl::Status SnappyDecompress(const Src& src,
                                     std::tuple<DestArgs...> dest_args) {
  return internal::SnappyDecompressImpl(
      Dependency<Reader*, Src>(src),
      Dependency<Writer*, Dest>(std::move(dest_args)));
}

template <typename Src, typename Dest>
inline absl::Status SnappyDecompress(Src&& src, const Dest& dest) {
  return internal::SnappyDecompressImpl(
      Dependency<Reader*, std::decay_t<Src>>(std::forward<Src>(src)),
      Dependency<Writer*, Dest>(dest));
}

template <typename Src, typename Dest>
inline absl::Status SnappyDecompress(Src&& src, Dest&& dest) {
  return internal::SnappyDecompressImpl(
      Dependency<Reader*, std::decay_t<Src>>(std::forward<Src>(src)),
      Dependency<Writer*, std::decay_t<Dest>>(std::forward<Dest>(dest)));
}

template <typename Src, typename Dest, typename... DestArgs>
inline absl::Status SnappyDecompress(Src&& src,
                                     std::tuple<DestArgs...> dest_args) {
  return internal::SnappyDecompressImpl(
      Dependency<Reader*, std::decay_t<Src>>(std::forward<Src>(src)),
      Dependency<Writer*, Dest>(std::move(dest_args)));
}

template <typename Src, typename Dest, typename... SrcArgs>
inline absl::Status SnappyDecompress(std::tuple<SrcArgs...> src_args,
                                     const Dest& dest) {
  return internal::SnappyDecompressImpl(
      Dependency<Reader*, Src>(std::move(src_args)),
      Dependency<Writer*, Dest>(dest));
}

template <typename Src, typename Dest, typename... SrcArgs>
inline absl::Status SnappyDecompress(std::tuple<SrcArgs...> src_args,
                                     Dest&& dest) {
  return internal::SnappyDecompressImpl(
      Dependency<Reader*, Src>(std::move(src_args)),
      Dependency<Writer*, std::decay_t<Dest>>(std::forward<Dest>(dest)));
}

template <typename Src, typename Dest, typename... SrcArgs,
          typename... DestArgs>
inline absl::Status SnappyDecompress(std::tuple<SrcArgs...> src_args,
                                     std::tuple<DestArgs...> dest_args) {
  return internal::SnappyDecompressImpl(
      Dependency<Reader*, Src>(std::move(src_args)),
      Dependency<Writer*, Dest>(std::move(dest_args)));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_SNAPPY_READER_H_
