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

#ifndef RIEGELI_ZSTD_ZSTD_READER_H_
#define RIEGELI_ZSTD_ZSTD_READER_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/zstd/zstd_dictionary.h"  // IWYU pragma: export
#include "zstd.h"

namespace riegeli {

// Template parameter independent part of `ZstdReader`.
class ZstdReaderBase : public BufferedReader {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `true`, supports decompressing as much as possible from a truncated
    // source, then retrying when the source has grown. This has a small
    // performance penalty.
    //
    // Default: `false`.
    Options& set_growing_source(bool growing_source) & {
      growing_source_ = growing_source;
      return *this;
    }
    Options&& set_growing_source(bool growing_source) && {
      return std::move(set_growing_source(growing_source));
    }
    bool growing_source() const { return growing_source_; }

    // Zstd dictionary. The same dictionary must have been used for compression,
    // except that it is allowed to supply a dictionary for decompression even
    // if no dictionary was used for compression.
    //
    // Default: `ZstdDictionary()`.
    Options& set_dictionary(ZstdDictionary dictionary) & {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(ZstdDictionary dictionary) && {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    ZstdDictionary& dictionary() { return dictionary_; }
    const ZstdDictionary& dictionary() const { return dictionary_; }

    // Options for a global `RecyclingPool` of decompression contexts.
    //
    // They tune the amount of memory which is kept to speed up creation of new
    // decompression sessions, and usage of a background thread to clean it.
    //
    // Default: `RecyclingPoolOptions()`.
    Options& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) & {
      recycling_pool_options_ = recycling_pool_options;
      return *this;
    }
    Options&& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) && {
      return std::move(set_recycling_pool_options(recycling_pool_options));
    }
    const RecyclingPoolOptions& recycling_pool_options() const {
      return recycling_pool_options_;
    }

   private:
    bool growing_source_ = false;
    ZstdDictionary dictionary_;
    RecyclingPoolOptions recycling_pool_options_;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

  // Returns `true` if the source is truncated (without a clean end of the
  // compressed stream) at the current position. In such case, if the source
  // does not grow, `Close()` will fail.
  bool truncated() const { return truncated_ && available() == 0; }

  bool ToleratesReadingAhead() override;
  bool SupportsRewind() override;
  bool SupportsNewReader() override;

 protected:
  explicit ZstdReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit ZstdReaderBase(BufferOptions buffer_options, bool growing_source,
                          ZstdDictionary&& dictionary,
                          const RecyclingPoolOptions& recycling_pool_options);

  ZstdReaderBase(ZstdReaderBase&& that) noexcept;
  ZstdReaderBase& operator=(ZstdReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, bool growing_source,
             ZstdDictionary&& dictionary,
             const RecyclingPoolOptions& recycling_pool_options);
  void Initialize(Reader* src);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  void ExactSizeReached() override;
  bool SeekBehindBuffer(Position new_pos) override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  struct ZSTD_DCtxDeleter {
    void operator()(ZSTD_DCtx* ptr) const { ZSTD_freeDCtx(ptr); }
  };

  void InitializeDecompressor(Reader& src);

  // If `true`, supports decompressing as much as possible from a truncated
  // source, then retrying when the source has grown.
  bool growing_source_ = false;
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // If `true`, calling `ZSTD_DCtx_setParameter()` is valid.
  bool just_initialized_ = false;
  ZstdDictionary dictionary_;
  RecyclingPoolOptions recycling_pool_options_;
  Position initial_compressed_pos_ = 0;
  // If `ok()` but `decompressor_ == nullptr` then all data have been
  // decompressed, `exact_size() == limit_pos()`, and `ReadInternal()` must not
  // be called again.
  RecyclingPool<ZSTD_DCtx, ZSTD_DCtxDeleter>::Handle decompressor_;
};

// A `Reader` which decompresses data with Zstd after getting it from another
// `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The compressed `Reader` must not be accessed until the `ZstdReader` is closed
// or no longer used.
template <typename Src = Reader*>
class ZstdReader : public ZstdReaderBase {
 public:
  // Creates a closed `ZstdReader`.
  explicit ZstdReader(Closed) noexcept : ZstdReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit ZstdReader(Initializer<Src> src, Options options = Options());

  ZstdReader(ZstdReader&& that) noexcept;
  ZstdReader& operator=(ZstdReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ZstdReader`. This avoids
  // constructing a temporary `ZstdReader` and moving from it.
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
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit ZstdReader(Closed) -> ZstdReader<DeleteCtad<Closed>>;
template <typename Src>
explicit ZstdReader(Src&& src,
                    ZstdReaderBase::Options options = ZstdReaderBase::Options())
    -> ZstdReader<InitializerTargetT<Src>>;
#endif

// Returns `true` if the data look like they have been Zstd-compressed.
//
// The current position of `src` is unchanged.
bool RecognizeZstd(Reader& src);

// Returns the claimed uncompressed size of Zstd-compressed data.
//
// Returns `absl::nullopt` if the size was not stored or on failure. The size is
// stored if `ZstdWriterBase::Options::pledged_size() != absl::nullopt`.
//
// The current position of `src` is unchanged.
absl::optional<Position> ZstdUncompressedSize(Reader& src);

// Returns the dictionary ID needed to read `src`, or 0 if none is needed.
//
// Returns `absl::nullopt` on failure.
//
// The current position of `src` is unchanged.
absl::optional<uint32_t> ZstdDictId(Reader& src);

// Implementation details follow.

inline ZstdReaderBase::ZstdReaderBase(
    BufferOptions buffer_options, bool growing_source,
    ZstdDictionary&& dictionary,
    const RecyclingPoolOptions& recycling_pool_options)
    : BufferedReader(buffer_options),
      growing_source_(growing_source),
      dictionary_(std::move(dictionary)),
      recycling_pool_options_(recycling_pool_options) {}

inline ZstdReaderBase::ZstdReaderBase(ZstdReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      growing_source_(that.growing_source_),
      truncated_(that.truncated_),
      just_initialized_(that.just_initialized_),
      dictionary_(std::move(that.dictionary_)),
      recycling_pool_options_(that.recycling_pool_options_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline ZstdReaderBase& ZstdReaderBase::operator=(
    ZstdReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  growing_source_ = that.growing_source_;
  truncated_ = that.truncated_;
  just_initialized_ = that.just_initialized_;
  dictionary_ = std::move(that.dictionary_);
  recycling_pool_options_ = that.recycling_pool_options_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void ZstdReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  growing_source_ = false;
  truncated_ = false;
  just_initialized_ = false;
  recycling_pool_options_ = RecyclingPoolOptions();
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = ZstdDictionary();
}

inline void ZstdReaderBase::Reset(
    BufferOptions buffer_options, bool growing_source,
    ZstdDictionary&& dictionary,
    const RecyclingPoolOptions& recycling_pool_options) {
  BufferedReader::Reset(buffer_options);
  growing_source_ = growing_source;
  truncated_ = false;
  just_initialized_ = false;
  recycling_pool_options_ = recycling_pool_options;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = std::move(dictionary);
}

template <typename Src>
inline ZstdReader<Src>::ZstdReader(Initializer<Src> src, Options options)
    : ZstdReaderBase(options.buffer_options(), options.growing_source(),
                     std::move(options.dictionary()),
                     options.recycling_pool_options()),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline ZstdReader<Src>::ZstdReader(ZstdReader&& that) noexcept
    : ZstdReaderBase(static_cast<ZstdReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline ZstdReader<Src>& ZstdReader<Src>::operator=(ZstdReader&& that) noexcept {
  ZstdReaderBase::operator=(static_cast<ZstdReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void ZstdReader<Src>::Reset(Closed) {
  ZstdReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void ZstdReader<Src>::Reset(Initializer<Src> src, Options options) {
  ZstdReaderBase::Reset(options.buffer_options(), options.growing_source(),
                        std::move(options.dictionary()),
                        options.recycling_pool_options());
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void ZstdReader<Src>::Done() {
  ZstdReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void ZstdReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  ZstdReaderBase::SetReadAllHintImpl(read_all_hint);
  if (src_.IsOwning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void ZstdReader<Src>::VerifyEndImpl() {
  ZstdReaderBase::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_ZSTD_ZSTD_READER_H_
