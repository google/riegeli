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

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class Writer;

// Template parameter independent part of `SnappyReader`.
class SnappyReaderBase : public ChainReader<Chain> {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `absl::nullopt`, the compressed `Reader` must support `Size()`.
    //
    // If not `absl::nullopt`, overrides that size.
    //
    // Default: `absl::nullopt`.
    Options& set_assumed_size(absl::optional<Position> assumed_size) & {
      assumed_size_ = assumed_size;
      return *this;
    }
    Options&& set_assumed_size(absl::optional<Position> assumed_size) && {
      return std::move(set_assumed_size(assumed_size));
    }
    absl::optional<Position> assumed_size() const { return assumed_size_; }

   private:
    absl::optional<Position> assumed_size_;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

 protected:
  explicit SnappyReaderBase(Closed) noexcept : ChainReader(kClosed) {}

  SnappyReaderBase();

  SnappyReaderBase(SnappyReaderBase&& that) noexcept;
  SnappyReaderBase& operator=(SnappyReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Reader* src, absl::optional<Position> assumed_size);
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
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The compressed `Reader` must support `Size()` if
// `Options::assumed_size() == absl::nullopt`.
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
  void Reset(Closed);
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
explicit SnappyReader(Closed)->SnappyReader<DeleteCtad<Closed>>;
template <typename Src>
explicit SnappyReader(const Src& src, SnappyReaderBase::Options options =
                                          SnappyReaderBase::Options())
    -> SnappyReader<std::decay_t<Src>>;
template <typename Src>
explicit SnappyReader(
    Src&& src, SnappyReaderBase::Options options = SnappyReaderBase::Options())
    -> SnappyReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit SnappyReader(
    std::tuple<SrcArgs...> src_args,
    SnappyReaderBase::Options options = SnappyReaderBase::Options())
    -> SnappyReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Options for `SnappyDecompress()`.
class SnappyDecompressOptions {
 public:
  SnappyDecompressOptions() noexcept {}

  // If `absl::nullopt`, the compressed `Reader` must support `Size()`.
  //
  // If not `absl::nullopt`, overrides that size.
  //
  // Default: `absl::nullopt`.
  SnappyDecompressOptions& set_assumed_size(
      absl::optional<Position> assumed_size) & {
    assumed_size_ = assumed_size;
    return *this;
  }
  SnappyDecompressOptions&& set_assumed_size(
      absl::optional<Position> assumed_size) && {
    return std::move(set_assumed_size(assumed_size));
  }
  absl::optional<Position> assumed_size() const { return assumed_size_; }

 private:
  absl::optional<Position> assumed_size_;
};

// An alternative interface to Snappy which avoids buffering uncompressed data.
// Calling `SnappyDecompress()` is equivalent to copying all data from a
// `SnappyReader<Src>` to `dest`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader&` (not owned),
// `Reader*` (not owned), `std::unique_ptr<Reader>` (owned),
// `ChainReader<>` (owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the uncompressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer&` (not owned),
// `Writer*` (not owned), `std::unique_ptr<Writer>` (owned),
// `ChainWriter<>` (owned).
//
// The compressed `Reader` must support `Size()` if
// `SnappyDecompressOptions::assumed_size() == absl::nullopt`.
template <typename Src, typename Dest,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int> = 0>
absl::Status SnappyDecompress(
    Src&& src, Dest&& dest,
    SnappyDecompressOptions options = SnappyDecompressOptions());

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
    : ChainReader(std::forward_as_tuple()) {}

inline SnappyReaderBase::SnappyReaderBase(SnappyReaderBase&& that) noexcept
    : ChainReader(std::move(that)) {}

inline SnappyReaderBase& SnappyReaderBase::operator=(
    SnappyReaderBase&& that) noexcept {
  ChainReader::operator=(std::move(that));
  return *this;
}

inline void SnappyReaderBase::Reset(Closed) { ChainReader::Reset(kClosed); }

inline void SnappyReaderBase::Reset() {
  // Empty `Chain` as the `ChainReader` source is a placeholder, it will be set
  // by `Initialize()`.
  ChainReader::Reset(std::forward_as_tuple());
}

template <typename Src>
inline SnappyReader<Src>::SnappyReader(const Src& src, Options options)
    : src_(src) {
  Initialize(src_.get(), options.assumed_size());
}

template <typename Src>
inline SnappyReader<Src>::SnappyReader(Src&& src, Options options)
    : src_(std::move(src)) {
  Initialize(src_.get(), options.assumed_size());
}

template <typename Src>
template <typename... SrcArgs>
inline SnappyReader<Src>::SnappyReader(std::tuple<SrcArgs...> src_args,
                                       Options options)
    : src_(std::move(src_args)) {
  Initialize(src_.get(), options.assumed_size());
}

template <typename Src>
inline SnappyReader<Src>::SnappyReader(SnappyReader&& that) noexcept
    : SnappyReaderBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      src_(std::move(that.src_)) {}

template <typename Src>
inline SnappyReader<Src>& SnappyReader<Src>::operator=(
    SnappyReader&& that) noexcept {
  SnappyReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void SnappyReader<Src>::Reset(Closed) {
  SnappyReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void SnappyReader<Src>::Reset(const Src& src, Options options) {
  SnappyReaderBase::Reset();
  src_.Reset(src);
  Initialize(src_.get(), options.assumed_size());
}

template <typename Src>
inline void SnappyReader<Src>::Reset(Src&& src, Options options) {
  SnappyReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.assumed_size());
}

template <typename Src>
template <typename... SrcArgs>
inline void SnappyReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                     Options options) {
  SnappyReaderBase::Reset();
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), options.assumed_size());
}

template <typename Src>
void SnappyReader<Src>::Done() {
  SnappyReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void SnappyReader<Src>::VerifyEnd() {
  SnappyReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) src_->VerifyEnd();
}

namespace snappy_internal {

absl::Status SnappyDecompressImpl(Reader& src, Writer& dest,
                                  SnappyDecompressOptions options);

}  // namespace snappy_internal

template <typename Src, typename Dest,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int>>
inline absl::Status SnappyDecompress(Src&& src, Dest&& dest,
                                     SnappyDecompressOptions options) {
  Dependency<Reader*, Src&&> src_ref(std::forward<Src>(src));
  Dependency<Writer*, Dest&&> dest_ref(std::forward<Dest>(dest));
  absl::Status status = snappy_internal::SnappyDecompressImpl(
      *src_ref, *dest_ref, std::move(options));
  if (dest_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_ref->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = dest_ref->status();
    }
  }
  if (src_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_ref->Close())) {
      if (ABSL_PREDICT_TRUE(status.ok())) status = src_ref->status();
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_SNAPPY_READER_H_
