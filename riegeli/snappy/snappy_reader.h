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

#ifndef RIEGELI_SNAPPY_SNAPPY_READER_H_
#define RIEGELI_SNAPPY_SNAPPY_READER_H_

#include <stddef.h>

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class Writer;

// Template parameter independent part of `SnappyReader`.
class SnappyReaderBase : public ChainReader<Chain> {
 public:
  class Options {};

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

 protected:
  explicit SnappyReaderBase(Closed) noexcept : ChainReader(kClosed) {}

  SnappyReaderBase();

  SnappyReaderBase(SnappyReaderBase&& that) noexcept;
  SnappyReaderBase& operator=(SnappyReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Reader* src);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
};

// A `Reader` which decompresses data with Snappy after getting it from another
// `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The compressed `Reader` must support `Size()`. To supply or override this
// size, the `Reader` can be wrapped in a `LimitingReader` with
// `LimitingReaderBase::Options().set_exact_length(size)`.
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
  explicit SnappyReader(Closed) noexcept : SnappyReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit SnappyReader(Initializer<Src> src, Options options = Options());

  SnappyReader(SnappyReader&& that) noexcept;
  SnappyReader& operator=(SnappyReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `SnappyReader`. This avoids
  // constructing a temporary `SnappyReader` and moving from it.
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
  void VerifyEndImpl() override;

 private:
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit SnappyReader(Closed) -> SnappyReader<DeleteCtad<Closed>>;
template <typename Src>
explicit SnappyReader(
    Src&& src, SnappyReaderBase::Options options = SnappyReaderBase::Options())
    -> SnappyReader<InitializerTargetT<Src>>;
#endif

// An alternative interface to Snappy which avoids buffering uncompressed data.
// Calling `SnappyDecompress()` is equivalent to copying all data from a
// `SnappyReader<Src&&>` to `dest`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the uncompressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest&&>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyDependency<Writer*>` (maybe owned).
//
// The compressed `Reader` must support `Size()`. To supply or override this
// size, the `Reader` can be wrapped in a `LimitingReader` with
// `LimitingReaderBase::Options().set_exact_length(size)`.
template <typename Src, typename Dest,
          std::enable_if_t<
              absl::conjunction<IsValidDependency<Reader*, Src&&>,
                                IsValidDependency<Writer*, Dest&&>>::value,
              int> = 0>
absl::Status SnappyDecompress(Src&& src, Dest&& dest);

// Returns the claimed uncompressed size of Snappy-compressed data.
//
// Returns `absl::nullopt` on failure.
//
// The current position of `src` is unchanged.
absl::optional<size_t> SnappyUncompressedSize(Reader& src);

// Implementation details follow.

inline SnappyReaderBase::SnappyReaderBase()
    // Empty `Chain` as the `ChainReader` source is a placeholder, it will be
    // set by `Initialize()`.
    : ChainReader(riegeli::Maker()) {}

inline SnappyReaderBase::SnappyReaderBase(SnappyReaderBase&& that) noexcept
    : ChainReader(static_cast<ChainReader&&>(that)) {}

inline SnappyReaderBase& SnappyReaderBase::operator=(
    SnappyReaderBase&& that) noexcept {
  ChainReader::operator=(static_cast<ChainReader&&>(that));
  return *this;
}

inline void SnappyReaderBase::Reset(Closed) { ChainReader::Reset(kClosed); }

inline void SnappyReaderBase::Reset() {
  // Empty `Chain` as the `ChainReader` source is a placeholder, it will be set
  // by `Initialize()`.
  ChainReader::Reset(riegeli::Maker());
}

template <typename Src>
inline SnappyReader<Src>::SnappyReader(Initializer<Src> src,
                                       ABSL_ATTRIBUTE_UNUSED Options options)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline SnappyReader<Src>::SnappyReader(SnappyReader&& that) noexcept
    : SnappyReaderBase(static_cast<SnappyReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline SnappyReader<Src>& SnappyReader<Src>::operator=(
    SnappyReader&& that) noexcept {
  SnappyReaderBase::operator=(static_cast<SnappyReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void SnappyReader<Src>::Reset(Closed) {
  SnappyReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void SnappyReader<Src>::Reset(Initializer<Src> src,
                                     ABSL_ATTRIBUTE_UNUSED Options options) {
  SnappyReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void SnappyReader<Src>::Done() {
  SnappyReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void SnappyReader<Src>::VerifyEndImpl() {
  SnappyReaderBase::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

namespace snappy_internal {

absl::Status SnappyDecompressImpl(Reader& src, Writer& dest);

}  // namespace snappy_internal

template <typename Src, typename Dest,
          std::enable_if_t<
              absl::conjunction<IsValidDependency<Reader*, Src&&>,
                                IsValidDependency<Writer*, Dest&&>>::value,
              int>>
inline absl::Status SnappyDecompress(Src&& src, Dest&& dest) {
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  Dependency<Writer*, Dest&&> dest_dep(std::forward<Dest>(dest));
  if (src_dep.IsOwning()) src_dep->SetReadAllHint(true);
  if (dest_dep.IsOwning()) {
    dest_dep->SetWriteSizeHint(SnappyUncompressedSize(*src_dep));
  }
  absl::Status status =
      snappy_internal::SnappyDecompressImpl(*src_dep, *dest_dep);
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  if (src_dep.IsOwning()) {
    if (ABSL_PREDICT_TRUE(status.ok())) src_dep->VerifyEnd();
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) status.Update(src_dep->status());
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_SNAPPY_READER_H_
