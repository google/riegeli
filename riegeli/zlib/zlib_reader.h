// Copyright 2018 Google LLC
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

#ifndef RIEGELI_ZLIB_ZLIB_READER_H_
#define RIEGELI_ZLIB_ZLIB_READER_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/zlib/zlib_dictionary.h"  // IWYU pragma: export

struct z_stream_s;  // `zlib.h` has `typedef struct z_stream_s z_stream`.

namespace riegeli {

// Template parameter independent part of `ZlibReader`.
class ZlibReaderBase : public BufferedReader {
 public:
  // Specifies what format of header to expect.
  enum class Header {
    kZlib = 0,         // Zlib header.
    kGzip = 16,        // Gzip header.
    kZlibOrGzip = 32,  // Zlib or Gzip header.
    kRaw = -1,         // No header; compressor must write no header too.
  };

  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // What format of header to expect.
    //
    // Default: `Header::kZlibOrGzip`.
    static constexpr Header kDefaultHeader = Header::kZlibOrGzip;
    Options& set_header(Header header) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      header_ = header;
      return *this;
    }
    Options&& set_header(Header header) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_header(header));
    }
    Header header() const { return header_; }

    // Maximum acceptable logarithm of the LZ77 sliding window size.
    //
    // `window_log` must be between `kMinWindowLog` (9) and
    // `kMaxWindowLog` (15). Default: `kDefaultWindowLog` (15).
    static constexpr int kMinWindowLog = 9;
    static constexpr int kMaxWindowLog = 15;      // `MAX_WBITS`
    static constexpr int kDefaultWindowLog = 15;  // `MAX_WBITS`
    Options& set_window_log(int window_log) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      RIEGELI_ASSERT_GE(window_log, kMinWindowLog)
          << "Failed precondition of "
             "ZlibReaderBase::Options::set_window_log(): "
             "window log out of range";
      RIEGELI_ASSERT_LE(window_log, kMaxWindowLog)
          << "Failed precondition of "
             "ZlibReaderBase::Options::set_window_log(): "
             "window log out of range";
      window_log_ = window_log;
      return *this;
    }
    Options&& set_window_log(int window_log) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_window_log(window_log));
    }
    int window_log() const { return window_log_; }

    // If `true`, concatenated compressed streams are decoded to concatenation
    // of their decompressed contents. An empty compressed stream is decoded to
    // empty decompressed contents.
    //
    // If `false`, exactly one compressed stream is consumed.
    //
    // Default: `false`.
    Options& set_concatenate(bool concatenate) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      concatenate_ = concatenate;
      return *this;
    }
    Options&& set_concatenate(bool concatenate) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_concatenate(concatenate));
    }
    bool concatenate() const { return concatenate_; }

    // Zlib dictionary. The same dictionary must have been used for compression,
    // except that it is allowed to supply a dictionary for decompression even
    // if no dictionary was used for compression.
    //
    // Default: `ZlibDictionary()`.
    Options& set_dictionary(ZlibDictionary dictionary) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(ZlibDictionary dictionary) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    ZlibDictionary& dictionary() ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return dictionary_;
    }
    const ZlibDictionary& dictionary() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
    Header header_ = kDefaultHeader;
    int window_log_ = kDefaultWindowLog;
    bool concatenate_ = false;
    ZlibDictionary dictionary_;
    RecyclingPoolOptions recycling_pool_options_;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns `true` if the source is truncated (without a clean end of the
  // compressed stream) at the current position. In such case, if the source
  // does not grow, `Close()` will fail.
  //
  // Precondition: `Options::concatenate()` was `false`.
  bool truncated() const {
    RIEGELI_ASSERT(!concatenate_)
        << "Failed precondition of ZlibReaderBase::truncated(): "
           "Options::concatenate() is true";
    return truncated_ && available() == 0;
  }

  bool ToleratesReadingAhead() override;
  bool SupportsRewind() override;
  bool SupportsNewReader() override;

 protected:
  explicit ZlibReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit ZlibReaderBase(BufferOptions buffer_options, int window_bits,
                          bool concatenate, ZlibDictionary&& dictionary,
                          const RecyclingPoolOptions& recycling_pool_options);

  ZlibReaderBase(ZlibReaderBase&& that) noexcept;
  ZlibReaderBase& operator=(ZlibReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, int window_bits, bool concatenate,
             ZlibDictionary&& dictionary,
             const RecyclingPoolOptions& recycling_pool_options);
  static int GetWindowBits(const Options& options);
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
  // For `ZStreamDeleter`.
  friend bool RecognizeZlib(Reader& src, ZlibReaderBase::Header header,
                            const RecyclingPoolOptions& recycling_pool_options);

  struct ZStreamDeleter {
    void operator()(z_stream_s* ptr) const;
  };

  void InitializeDecompressor();
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation,
                                         int zlib_code);

  int window_bits_ = 0;
  bool concatenate_ = false;
  Position initial_compressed_pos_ = 0;
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // If `true`, some compressed data from the current stream were processed.
  // If `concatenate_` and `!stream_had_data_`, an end of the source is
  // legitimate, it does not imply that the source is truncated.
  bool stream_had_data_ = false;
  ZlibDictionary dictionary_;
  RecyclingPoolOptions recycling_pool_options_;
  // If `ok()` but `decompressor_ == nullptr` then all data have been
  // decompressed, `exact_size() == limit_pos()`, and `ReadInternal()` must not
  // be called again.
  RecyclingPool<z_stream_s, ZStreamDeleter>::Handle decompressor_;
};

// A `Reader` which decompresses data with Zlib after getting it from another
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
// The compressed `Reader` must not be accessed until the `ZlibReader` is closed
// or no longer used.
template <typename Src = Reader*>
class ZlibReader : public ZlibReaderBase {
 public:
  // Creates a closed `ZlibReader`.
  explicit ZlibReader(Closed) noexcept : ZlibReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit ZlibReader(Initializer<Src> src, Options options = Options());

  ZlibReader(ZlibReader&&) = default;
  ZlibReader& operator=(ZlibReader&&) = default;

  // Makes `*this` equivalent to a newly constructed `ZlibReader`. This avoids
  // constructing a temporary `ZlibReader` and moving from it.
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

explicit ZlibReader(Closed) -> ZlibReader<DeleteCtad<Closed>>;
template <typename Src>
explicit ZlibReader(Src&& src,
                    ZlibReaderBase::Options options = ZlibReaderBase::Options())
    -> ZlibReader<TargetT<Src>>;

// Returns `true` if the data look like they have been Zlib-compressed.
//
// The current position of `src` is unchanged.
//
// Precondition: `header != ZlibReaderBase::Header::kRaw`
bool RecognizeZlib(
    Reader& src,
    ZlibReaderBase::Header header = ZlibReaderBase::Header::kZlibOrGzip,
    const RecyclingPoolOptions& recycling_pool_options =
        RecyclingPoolOptions());
bool RecognizeZlib(Reader& src,
                   const RecyclingPoolOptions& recycling_pool_options);

// Returns the claimed uncompressed size of Gzip-compressed data (with
// `ZlibWriterBase::Header::kGzip`) modulo 4G. The compressed stream must not
// have anything appended.
//
// If the data consists of multiple streams, only the last stream is considered.
//
// Returns `std::nullopt` on failure. If the data are not Gzip-compressed, or
// have something appended, then this is generally not detected and the returned
// value will be meaningless. If the data were longer than 4G, then only the
// lowest 32 bits are returned.
//
// The current position of `src` is unchanged.
//
// Precondition: `src.SupportsRandomAccess()`
std::optional<uint32_t> GzipUncompressedSizeModulo4G(Reader& src);

// Implementation details follow.

inline ZlibReaderBase::ZlibReaderBase(
    BufferOptions buffer_options, int window_bits, bool concatenate,
    ZlibDictionary&& dictionary,
    const RecyclingPoolOptions& recycling_pool_options)
    : BufferedReader(buffer_options),
      window_bits_(window_bits),
      concatenate_(concatenate),
      dictionary_(std::move(dictionary)),
      recycling_pool_options_(recycling_pool_options) {}

inline ZlibReaderBase::ZlibReaderBase(ZlibReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      window_bits_(that.window_bits_),
      concatenate_(that.concatenate_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      truncated_(that.truncated_),
      stream_had_data_(that.stream_had_data_),
      dictionary_(std::move(that.dictionary_)),
      recycling_pool_options_(that.recycling_pool_options_),
      decompressor_(std::move(that.decompressor_)) {}

inline ZlibReaderBase& ZlibReaderBase::operator=(
    ZlibReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  window_bits_ = that.window_bits_;
  concatenate_ = that.concatenate_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  truncated_ = that.truncated_;
  stream_had_data_ = that.stream_had_data_;
  dictionary_ = std::move(that.dictionary_);
  recycling_pool_options_ = that.recycling_pool_options_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void ZlibReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  window_bits_ = 0;
  concatenate_ = false;
  initial_compressed_pos_ = 0;
  truncated_ = false;
  stream_had_data_ = false;
  recycling_pool_options_ = RecyclingPoolOptions();
  decompressor_.reset();
  // Must be destroyed after `decompressor_`.
  dictionary_ = ZlibDictionary();
}

inline void ZlibReaderBase::Reset(
    BufferOptions buffer_options, int window_bits, bool concatenate,
    ZlibDictionary&& dictionary,
    const RecyclingPoolOptions& recycling_pool_options) {
  BufferedReader::Reset(buffer_options);
  window_bits_ = window_bits;
  concatenate_ = concatenate;
  initial_compressed_pos_ = 0;
  truncated_ = false;
  stream_had_data_ = false;
  recycling_pool_options_ = recycling_pool_options;
  decompressor_.reset();
  // Must be destroyed after `decompressor_`.
  dictionary_ = std::move(dictionary);
}

inline int ZlibReaderBase::GetWindowBits(const Options& options) {
  return options.header() == Header::kRaw
             ? -options.window_log()
             : options.window_log() + static_cast<int>(options.header());
}

template <typename Src>
inline ZlibReader<Src>::ZlibReader(Initializer<Src> src, Options options)
    : ZlibReaderBase(options.buffer_options(), GetWindowBits(options),
                     options.concatenate(), std::move(options.dictionary()),
                     options.recycling_pool_options()),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline void ZlibReader<Src>::Reset(Closed) {
  ZlibReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void ZlibReader<Src>::Reset(Initializer<Src> src, Options options) {
  ZlibReaderBase::Reset(options.buffer_options(), GetWindowBits(options),
                        options.concatenate(), std::move(options.dictionary()),
                        options.recycling_pool_options());
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void ZlibReader<Src>::Done() {
  ZlibReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void ZlibReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  ZlibReaderBase::SetReadAllHintImpl(read_all_hint);
  if (src_.IsOwning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void ZlibReader<Src>::VerifyEndImpl() {
  ZlibReaderBase::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

inline bool RecognizeZlib(Reader& src,
                          const RecyclingPoolOptions& recycling_pool_options) {
  return RecognizeZlib(src, ZlibReaderBase::Header::kZlibOrGzip,
                       recycling_pool_options);
}

}  // namespace riegeli

#endif  // RIEGELI_ZLIB_ZLIB_READER_H_
