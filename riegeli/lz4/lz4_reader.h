// Copyright 2022 Google LLC
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

#ifndef RIEGELI_LZ4_LZ4_READER_H_
#define RIEGELI_LZ4_LZ4_READER_H_

#include <stddef.h>

#include <memory>
#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "lz4.h"
#include "lz4frame.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/lz4/lz4_dictionary.h"  // IWYU pragma: export

namespace riegeli {

namespace lz4_internal {

bool GetFrameInfo(Reader& src, LZ4F_frameInfo_t& frame_info,
                  const RecyclingPoolOptions& recycling_pool_options);

}  // namespace lz4_internal

// Template parameter independent part of `Lz4Reader`.
class Lz4ReaderBase : public BufferedReader {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `true`, supports decompressing as much as possible from a truncated
    // source, then retrying when the source has grown. This has a small
    // performance penalty.
    //
    // Default: `false`.
    Options& set_growing_source(bool growing_source) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      growing_source_ = growing_source;
      return *this;
    }
    Options&& set_growing_source(bool growing_source) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_growing_source(growing_source));
    }
    bool growing_source() const { return growing_source_; }

    // Lz4 dictionary. The same dictionary must have been used for compression,
    // except that it is allowed to supply a dictionary even if no dictionary
    // was used for compression.
    //
    // Default: `Lz4Dictionary()`.
    Options& set_dictionary(Lz4Dictionary dictionary) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(Lz4Dictionary dictionary) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    Lz4Dictionary& dictionary() ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return dictionary_;
    }
    const Lz4Dictionary& dictionary() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return dictionary_;
    }

    // Options for a global `RecyclingPool` of decompression contexts.
    //
    // They tune the amount of memory which is kept to speed up creation of new
    // decompression sessions, and usage of a background thread to clean it.
    //
    // Default: `RecyclingPoolOptions()`.
    Options& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      recycling_pool_options_ = recycling_pool_options;
      return *this;
    }
    Options&& set_recycling_pool_options(
        const RecyclingPoolOptions& recycling_pool_options) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_recycling_pool_options(recycling_pool_options));
    }
    const RecyclingPoolOptions& recycling_pool_options() const
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return recycling_pool_options_;
    }

   private:
    bool growing_source_ = false;
    Lz4Dictionary dictionary_;
    RecyclingPoolOptions recycling_pool_options_
#if LZ4_VERSION_NUMBER <= 10904
        // Workaround for https://github.com/lz4/lz4/issues/1227.
        = RecyclingPoolOptions().set_max_size(0)
#endif
        ;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns `true` if the source is truncated (without a clean end of the
  // compressed stream) at the current position. In such case, if the source
  // does not grow, `Close()` will fail.
  bool truncated() const { return truncated_ && available() == 0; }

  bool ToleratesReadingAhead() override;
  bool SupportsRewind() override;
  bool SupportsNewReader() override;

 protected:
  explicit Lz4ReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit Lz4ReaderBase(BufferOptions buffer_options, bool growing_source,
                         Lz4Dictionary&& dictionary,
                         const RecyclingPoolOptions& recycling_pool_options);

  Lz4ReaderBase(Lz4ReaderBase&& that) noexcept;
  Lz4ReaderBase& operator=(Lz4ReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, bool growing_source,
             Lz4Dictionary&& dictionary,
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
  // For `LZ4F_dctxDeleter`.
  friend bool lz4_internal::GetFrameInfo(
      Reader& src, LZ4F_frameInfo_t& frame_info,
      const RecyclingPoolOptions& recycling_pool_options);

  struct LZ4F_dctxDeleter {
    void operator()(LZ4F_dctx* ptr) const {
      const LZ4F_errorCode_t result = LZ4F_freeDecompressionContext(ptr);
      RIEGELI_ASSERT(!LZ4F_isError(result))
          << "LZ4F_freeDecompressionContext() failed: "
          << LZ4F_getErrorName(result);
    }
  };

  void InitializeDecompressor(Reader& src);
  bool ReadHeader(Reader& src);

  // If `true`, supports decompressing as much as possible from a truncated
  // source, then retrying when the source has grown.
  bool growing_source_ = false;
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // If `true`, the frame header has been read.
  bool header_read_ = false;
  Lz4Dictionary dictionary_;
  RecyclingPoolOptions recycling_pool_options_;
  Position initial_compressed_pos_ = 0;
  // If `ok()` but `decompressor_ == nullptr` then all data have been
  // decompressed, `exact_size() == limit_pos()`, and `ReadInternal()` must not
  // be called again.
  //
  RecyclingPool<LZ4F_dctx, LZ4F_dctxDeleter>::Handle decompressor_;
};

// A `Reader` which decompresses data with Lz4 after getting it from another
// `Reader`.
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
// The compressed `Reader` must not be accessed until the `Lz4Reader` is closed
// or no longer used.
template <typename Src = Reader*>
class Lz4Reader : public Lz4ReaderBase {
 public:
  // Creates a closed `Lz4Reader`.
  explicit Lz4Reader(Closed) noexcept : Lz4ReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit Lz4Reader(Initializer<Src> src, Options options = Options());

  Lz4Reader(Lz4Reader&& that) = default;
  Lz4Reader& operator=(Lz4Reader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `Lz4Reader`. This avoids
  // constructing a temporary `Lz4Reader` and moving from it.
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

explicit Lz4Reader(Closed) -> Lz4Reader<DeleteCtad<Closed>>;
template <typename Src>
explicit Lz4Reader(Src&& src,
                   Lz4ReaderBase::Options options = Lz4ReaderBase::Options())
    -> Lz4Reader<TargetT<Src>>;

// Returns `true` if the data look like they have been Lz4-compressed.
//
// The current position of `src` is unchanged.
bool RecognizeLz4(Reader& src,
                  const RecyclingPoolOptions& recycling_pool_options =
                      RecyclingPoolOptions());

// Returns the claimed uncompressed size of Lz4-compressed data.
//
// Returns `std::nullopt` if the size was not stored or on failure. The size is
// stored if `Lz4WriterBase::Options::pledged_size() != std::nullopt`.
//
// The current position of `src` is unchanged.
std::optional<Position> Lz4UncompressedSize(
    Reader& src, const RecyclingPoolOptions& recycling_pool_options =
                     RecyclingPoolOptions());

// Implementation details follow.

inline Lz4ReaderBase::Lz4ReaderBase(
    BufferOptions buffer_options, bool growing_source,
    Lz4Dictionary&& dictionary,
    const RecyclingPoolOptions& recycling_pool_options)
    : BufferedReader(buffer_options),
      growing_source_(growing_source),
      dictionary_(std::move(dictionary)),
      recycling_pool_options_(recycling_pool_options) {}

inline Lz4ReaderBase::Lz4ReaderBase(Lz4ReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      growing_source_(that.growing_source_),
      truncated_(that.truncated_),
      header_read_(that.header_read_),
      dictionary_(std::move(that.dictionary_)),
      recycling_pool_options_(that.recycling_pool_options_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline Lz4ReaderBase& Lz4ReaderBase::operator=(Lz4ReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  growing_source_ = that.growing_source_;
  truncated_ = that.truncated_;
  header_read_ = that.header_read_;
  dictionary_ = std::move(that.dictionary_);
  recycling_pool_options_ = that.recycling_pool_options_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void Lz4ReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  growing_source_ = false;
  truncated_ = false;
  header_read_ = false;
  recycling_pool_options_ = RecyclingPoolOptions();
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = Lz4Dictionary();
}

inline void Lz4ReaderBase::Reset(
    BufferOptions buffer_options, bool growing_source,
    Lz4Dictionary&& dictionary,
    const RecyclingPoolOptions& recycling_pool_options) {
  BufferedReader::Reset(buffer_options);
  growing_source_ = growing_source;
  truncated_ = false;
  header_read_ = false;
  recycling_pool_options_ = recycling_pool_options;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = std::move(dictionary);
}

template <typename Src>
inline Lz4Reader<Src>::Lz4Reader(Initializer<Src> src, Options options)
    : Lz4ReaderBase(options.buffer_options(), options.growing_source(),
                    std::move(options.dictionary()),
                    options.recycling_pool_options()),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline void Lz4Reader<Src>::Reset(Closed) {
  Lz4ReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void Lz4Reader<Src>::Reset(Initializer<Src> src, Options options) {
  Lz4ReaderBase::Reset(options.buffer_options(), options.growing_source(),
                       std::move(options.dictionary()),
                       options.recycling_pool_options());
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void Lz4Reader<Src>::Done() {
  Lz4ReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void Lz4Reader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  Lz4ReaderBase::SetReadAllHintImpl(read_all_hint);
  if (src_.IsOwning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void Lz4Reader<Src>::VerifyEndImpl() {
  Lz4ReaderBase::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_LZ4_LZ4_READER_H_
