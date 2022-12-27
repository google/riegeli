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
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "lz4frame.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/lz4/lz4_dictionary.h"

namespace riegeli {

namespace lz4_internal {

bool GetFrameInfo(Reader& src, LZ4F_frameInfo_t& frame_info);

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
    Options& set_growing_source(bool growing_source) & {
      growing_source_ = growing_source;
      return *this;
    }
    Options&& set_growing_source(bool growing_source) && {
      return std::move(set_growing_source(growing_source));
    }
    bool growing_source() const { return growing_source_; }

    // Lz4 dictionary. The same dictionary must have been used for compression,
    // except that it is allowed to supply a dictionary even if no dictionary
    // was used for compression.
    //
    // Default: `Lz4Dictionary()`.
    Options& set_dictionary(const Lz4Dictionary& dictionary) & {
      dictionary_ = dictionary;
      return *this;
    }
    Options& set_dictionary(Lz4Dictionary&& dictionary) & {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(const Lz4Dictionary& dictionary) && {
      return std::move(set_dictionary(dictionary));
    }
    Options&& set_dictionary(Lz4Dictionary&& dictionary) && {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    Lz4Dictionary& dictionary() { return dictionary_; }
    const Lz4Dictionary& dictionary() const { return dictionary_; }

   private:
    bool growing_source_ = false;
    Lz4Dictionary dictionary_;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() = 0;
  virtual const Reader* SrcReader() const = 0;

  // Returns `true` if the source is truncated (without a clean end of the
  // compressed stream) at the current position. In such case, if the source
  // does not grow, `Close()` will fail.
  bool truncated() const { return truncated_ && available() == 0; }

  bool ToleratesReadingAhead() override;
  bool SupportsRewind() override;
  bool SupportsSize() override { return exact_size() != absl::nullopt; }
  bool SupportsNewReader() override;

 protected:
  explicit Lz4ReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit Lz4ReaderBase(const BufferOptions& buffer_options,
                         bool growing_source, Lz4Dictionary&& dictionary);

  Lz4ReaderBase(Lz4ReaderBase&& that) noexcept;
  Lz4ReaderBase& operator=(Lz4ReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const BufferOptions& buffer_options, bool growing_source,
             Lz4Dictionary&& dictionary);
  void Initialize(Reader* src);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekBehindBuffer(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  // For `LZ4F_dctxDeleter`.
  friend bool lz4_internal::GetFrameInfo(Reader& src,
                                         LZ4F_frameInfo_t& frame_info);

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
  Position initial_compressed_pos_ = 0;
  // If `ok()` but `decompressor_ == nullptr` then all data have been
  // decompressed. In this case `LZ4F_decompress_usingDict()` must not be called
  // again.
  RecyclingPool<LZ4F_dctx, LZ4F_dctxDeleter>::Handle decompressor_;
};

// A `Reader` which decompresses data with Lz4 after getting it from another
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
// The compressed `Reader` must not be accessed until the `Lz4Reader` is closed
// or no longer used.
template <typename Src = Reader*>
class Lz4Reader : public Lz4ReaderBase {
 public:
  // Creates a closed `Lz4Reader`.
  explicit Lz4Reader(Closed) noexcept : Lz4ReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit Lz4Reader(const Src& src, Options options = Options());
  explicit Lz4Reader(Src&& src, Options options = Options());

  // Will read from the compressed `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit Lz4Reader(std::tuple<SrcArgs...> src_args,
                     Options options = Options());

  Lz4Reader(Lz4Reader&& that) noexcept;
  Lz4Reader& operator=(Lz4Reader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `Lz4Reader`. This avoids
  // constructing a temporary `Lz4Reader` and moving from it.
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
explicit Lz4Reader(Closed) -> Lz4Reader<DeleteCtad<Closed>>;
template <typename Src>
explicit Lz4Reader(const Src& src,
                   Lz4ReaderBase::Options options = Lz4ReaderBase::Options())
    -> Lz4Reader<std::decay_t<Src>>;
template <typename Src>
explicit Lz4Reader(Src&& src,
                   Lz4ReaderBase::Options options = Lz4ReaderBase::Options())
    -> Lz4Reader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit Lz4Reader(std::tuple<SrcArgs...> src_args,
                   Lz4ReaderBase::Options options = Lz4ReaderBase::Options())
    -> Lz4Reader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Returns `true` if the data look like they have been Lz4-compressed.
//
// The current position of `src` is unchanged.
bool RecognizeLz4(Reader& src);

// Returns the claimed uncompressed size of Lz4-compressed data.
//
// Returns `absl::nullopt` if the size was not stored or on failure. The size is
// stored if `Lz4WriterBase::Options::pledged_size() != absl::nullopt`.
//
// The current position of `src` is unchanged.
absl::optional<Position> Lz4UncompressedSize(Reader& src);

// Implementation details follow.

inline Lz4ReaderBase::Lz4ReaderBase(const BufferOptions& buffer_options,
                                    bool growing_source,
                                    Lz4Dictionary&& dictionary)
    : BufferedReader(buffer_options),
      growing_source_(growing_source),
      dictionary_(std::move(dictionary)) {}

inline Lz4ReaderBase::Lz4ReaderBase(Lz4ReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      growing_source_(that.growing_source_),
      truncated_(that.truncated_),
      header_read_(that.header_read_),
      dictionary_(std::move(that.dictionary_)),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline Lz4ReaderBase& Lz4ReaderBase::operator=(Lz4ReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  growing_source_ = that.growing_source_;
  truncated_ = that.truncated_;
  header_read_ = that.header_read_;
  dictionary_ = std::move(that.dictionary_);
  initial_compressed_pos_ = that.initial_compressed_pos_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void Lz4ReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  growing_source_ = false;
  truncated_ = false;
  header_read_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = Lz4Dictionary();
}

inline void Lz4ReaderBase::Reset(const BufferOptions& buffer_options,
                                 bool growing_source,
                                 Lz4Dictionary&& dictionary) {
  BufferedReader::Reset(buffer_options);
  growing_source_ = growing_source;
  truncated_ = false;
  header_read_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = std::move(dictionary);
}

template <typename Src>
inline Lz4Reader<Src>::Lz4Reader(const Src& src, Options options)
    : Lz4ReaderBase(options.buffer_options(), options.growing_source(),
                    std::move(options.dictionary())),
      src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline Lz4Reader<Src>::Lz4Reader(Src&& src, Options options)
    : Lz4ReaderBase(options.buffer_options(), options.growing_source(),
                    std::move(options.dictionary())),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline Lz4Reader<Src>::Lz4Reader(std::tuple<SrcArgs...> src_args,
                                 Options options)
    : Lz4ReaderBase(options.buffer_options(), options.growing_source(),
                    std::move(options.dictionary())),
      src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline Lz4Reader<Src>::Lz4Reader(Lz4Reader&& that) noexcept
    : Lz4ReaderBase(static_cast<Lz4ReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline Lz4Reader<Src>& Lz4Reader<Src>::operator=(Lz4Reader&& that) noexcept {
  Lz4ReaderBase::operator=(static_cast<Lz4ReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void Lz4Reader<Src>::Reset(Closed) {
  Lz4ReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void Lz4Reader<Src>::Reset(const Src& src, Options options) {
  Lz4ReaderBase::Reset(options.buffer_options(), options.growing_source(),
                       std::move(options.dictionary()));
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void Lz4Reader<Src>::Reset(Src&& src, Options options) {
  Lz4ReaderBase::Reset(options.buffer_options(), options.growing_source(),
                       std::move(options.dictionary()));
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void Lz4Reader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                  Options options) {
  Lz4ReaderBase::Reset(options.buffer_options(), options.growing_source(),
                       std::move(options.dictionary()));
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void Lz4Reader<Src>::Done() {
  Lz4ReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void Lz4Reader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  Lz4ReaderBase::SetReadAllHintImpl(read_all_hint);
  if (src_.is_owning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void Lz4Reader<Src>::VerifyEndImpl() {
  Lz4ReaderBase::VerifyEndImpl();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_LZ4_LZ4_READER_H_
