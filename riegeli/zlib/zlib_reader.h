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
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/zlib/zlib_dictionary.h"

struct z_stream_s;  // `zlib.h` has `typedef struct z_stream_s z_stream`.

namespace riegeli {

// Template parameter independent part of `ZlibReader`.
class ZlibReaderBase : public BufferedReader {
 public:
  // Specifies what format of header to expect.
  enum class Header {
    // Zlib header.
    kZlib = 0,
    // Gzip header.
    kGzip = 16,
    // Zlib or Gzip header.
    kZlibOrGzip = 32,
    // No header; compressor must write no header too.
    kRaw = -1,
  };

  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // Maximum acceptable logarithm of the LZ77 sliding window size.
    //
    // `window_log` must be between `kMinWindowLog` (9) and
    // `kMaxWindowLog` (15). Default: `kDefaultWindowLog` (15).
    static constexpr int kMinWindowLog = 9;
    static constexpr int kMaxWindowLog = 15;      // `MAX_WBITS`
    static constexpr int kDefaultWindowLog = 15;  // `MAX_WBITS`
    Options& set_window_log(int window_log) & {
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
    Options&& set_window_log(int window_log) && {
      return std::move(set_window_log(window_log));
    }
    int window_log() const { return window_log_; }

    // What format of header to expect.
    //
    // Default: `Header::kZlibOrGzip`.
    static constexpr Header kDefaultHeader = Header::kZlibOrGzip;
    Options& set_header(Header header) & {
      header_ = header;
      return *this;
    }
    Options&& set_header(Header header) && {
      return std::move(set_header(header));
    }
    Header header() const { return header_; }

    // Zlib dictionary. The same dictionary must have been used for compression,
    // except that it is allowed to supply a dictionary for decompression even
    // if no dictionary was used for compression.
    //
    // Default: `ZlibDictionary()`.
    Options& set_dictionary(const ZlibDictionary& dictionary) & {
      dictionary_ = dictionary;
      return *this;
    }
    Options& set_dictionary(ZlibDictionary&& dictionary) & {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(const ZlibDictionary& dictionary) && {
      return std::move(set_dictionary(dictionary));
    }
    Options&& set_dictionary(ZlibDictionary&& dictionary) && {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    ZlibDictionary& dictionary() { return dictionary_; }
    const ZlibDictionary& dictionary() const { return dictionary_; }

    // If `true`, concatenated compressed streams are decoded to concatenation
    // of their decompressed contents. An empty compressed stream is decoded to
    // empty decompressed contents.
    //
    // If `false`, exactly one compressed stream is consumed.
    //
    // Default: `false`.
    Options& set_concatenate(bool concatenate) & {
      concatenate_ = concatenate;
      return *this;
    }
    Options&& set_concatenate(bool concatenate) && {
      return std::move(set_concatenate(concatenate));
    }
    bool concatenate() const { return concatenate_; }

   private:
    int window_log_ = kDefaultWindowLog;
    Header header_ = kDefaultHeader;
    ZlibDictionary dictionary_;
    bool concatenate_ = false;
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

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

  explicit ZlibReaderBase(const BufferOptions& buffer_options, int window_bits,
                          ZlibDictionary&& dictionary, bool concatenate);

  ZlibReaderBase(ZlibReaderBase&& that) noexcept;
  ZlibReaderBase& operator=(ZlibReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const BufferOptions& buffer_options, int window_bits,
             ZlibDictionary&& dictionary, bool concatenate);
  static int GetWindowBits(const Options& options);
  void Initialize(Reader* src);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  bool ReadInternal(size_t min_length, size_t max_length, char* dest) override;
  bool SeekBehindBuffer(Position new_pos) override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  // For `ZStreamDeleter`.
  friend bool RecognizeZlib(Reader& src, ZlibReaderBase::Header header);

  struct ZStreamDeleter {
    void operator()(z_stream_s* ptr) const;
  };

  void InitializeDecompressor();
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::StatusCode code,
                                         absl::string_view operation,
                                         int zlib_code);

  int window_bits_ = 0;
  bool concatenate_ = false;
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // If `true`, some compressed data from the current stream were processed.
  // If `concatenate_` and `!stream_had_data_`, an end of the source is
  // legitimate, it does not imply that the source is truncated.
  bool stream_had_data_ = false;
  ZlibDictionary dictionary_;
  Position initial_compressed_pos_ = 0;
  RecyclingPool<z_stream_s, ZStreamDeleter>::Handle decompressor_;
};

// A `Reader` which decompresses data with Zlib after getting it from another
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
// The compressed `Reader` must not be accessed until the `ZlibReader` is closed
// or no longer used.
template <typename Src = Reader*>
class ZlibReader : public ZlibReaderBase {
 public:
  // Creates a closed `ZlibReader`.
  explicit ZlibReader(Closed) noexcept : ZlibReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit ZlibReader(const Src& src, Options options = Options());
  explicit ZlibReader(Src&& src, Options options = Options());

  // Will read from the compressed `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit ZlibReader(std::tuple<SrcArgs...> src_args,
                      Options options = Options());

  ZlibReader(ZlibReader&&) noexcept;
  ZlibReader& operator=(ZlibReader&&) noexcept;

  // Makes `*this` equivalent to a newly constructed `ZlibReader`. This avoids
  // constructing a temporary `ZlibReader` and moving from it.
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
explicit ZlibReader(Closed)->ZlibReader<DeleteCtad<Closed>>;
template <typename Src>
explicit ZlibReader(const Src& src,
                    ZlibReaderBase::Options options = ZlibReaderBase::Options())
    -> ZlibReader<std::decay_t<Src>>;
template <typename Src>
explicit ZlibReader(Src&& src,
                    ZlibReaderBase::Options options = ZlibReaderBase::Options())
    -> ZlibReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit ZlibReader(std::tuple<SrcArgs...> src_args,
                    ZlibReaderBase::Options options = ZlibReaderBase::Options())
    -> ZlibReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Returns `true` if the data look like they have been Zlib-compressed.
//
// The current position of `src` is unchanged.
//
// Precondition: `header != ZlibReaderBase::Header::kRaw`
bool RecognizeZlib(Reader& src, ZlibReaderBase::Header header =
                                    ZlibReaderBase::Header::kZlibOrGzip);

// Returns the claimed uncompressed size of Gzip-compressed data (with
// `ZlibWriterBase::Header::kGzip`) modulo 4G. The compressed stream must not
// have anything appended.
//
// Returns `absl::nullopt` on failure. If the data are not Gzip-compressed, or
// have something appended, then this is generally not detected and the returned
// value will be meaningless. If the data were longer than 4G, then only the
// lowest 32 bits are returned.
//
// The current position of `src` is unchanged.
//
// Precondition: `src.SupportsRandomAccess()`
absl::optional<uint32_t> GzipUncompressedSizeModulo4G(Reader& src);

// Implementation details follow.

inline ZlibReaderBase::ZlibReaderBase(const BufferOptions& buffer_options,
                                      int window_bits,
                                      ZlibDictionary&& dictionary,
                                      bool concatenate)
    : BufferedReader(buffer_options),
      window_bits_(window_bits),
      concatenate_(concatenate),
      dictionary_(std::move(dictionary)) {}

inline ZlibReaderBase::ZlibReaderBase(ZlibReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      window_bits_(that.window_bits_),
      concatenate_(that.concatenate_),
      truncated_(that.truncated_),
      stream_had_data_(that.stream_had_data_),
      dictionary_(std::move(that.dictionary_)),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline ZlibReaderBase& ZlibReaderBase::operator=(
    ZlibReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  window_bits_ = that.window_bits_;
  concatenate_ = that.concatenate_;
  truncated_ = that.truncated_;
  stream_had_data_ = that.stream_had_data_;
  dictionary_ = std::move(that.dictionary_);
  initial_compressed_pos_ = that.initial_compressed_pos_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void ZlibReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  window_bits_ = 0;
  concatenate_ = false;
  truncated_ = false;
  stream_had_data_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = ZlibDictionary();
}

inline void ZlibReaderBase::Reset(const BufferOptions& buffer_options,
                                  int window_bits, ZlibDictionary&& dictionary,
                                  bool concatenate) {
  BufferedReader::Reset(buffer_options);
  window_bits_ = window_bits;
  concatenate_ = concatenate;
  truncated_ = false;
  stream_had_data_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
  dictionary_ = std::move(dictionary);
}

inline int ZlibReaderBase::GetWindowBits(const Options& options) {
  return options.header() == Header::kRaw
             ? -options.window_log()
             : options.window_log() + static_cast<int>(options.header());
}

template <typename Src>
inline ZlibReader<Src>::ZlibReader(const Src& src, Options options)
    : ZlibReaderBase(options.buffer_options(), GetWindowBits(options),
                     std::move(options.dictionary()), options.concatenate()),
      src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline ZlibReader<Src>::ZlibReader(Src&& src, Options options)
    : ZlibReaderBase(options.buffer_options(), GetWindowBits(options),
                     std::move(options.dictionary()), options.concatenate()),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline ZlibReader<Src>::ZlibReader(std::tuple<SrcArgs...> src_args,
                                   Options options)
    : ZlibReaderBase(options.buffer_options(), GetWindowBits(options),
                     std::move(options.dictionary()), options.concatenate()),
      src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline ZlibReader<Src>::ZlibReader(ZlibReader&& that) noexcept
    : ZlibReaderBase(static_cast<ZlibReaderBase&&>(that)),
      src_(std::move(that.src_)) {}

template <typename Src>
inline ZlibReader<Src>& ZlibReader<Src>::operator=(ZlibReader&& that) noexcept {
  ZlibReaderBase::operator=(static_cast<ZlibReaderBase&&>(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void ZlibReader<Src>::Reset(Closed) {
  ZlibReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void ZlibReader<Src>::Reset(const Src& src, Options options) {
  ZlibReaderBase::Reset(options.buffer_options(), GetWindowBits(options),
                        std::move(options.dictionary()), options.concatenate());
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void ZlibReader<Src>::Reset(Src&& src, Options options) {
  ZlibReaderBase::Reset(options.buffer_options(), GetWindowBits(options),
                        std::move(options.dictionary()), options.concatenate());
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void ZlibReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                   Options options) {
  ZlibReaderBase::Reset(options.buffer_options(), GetWindowBits(options),
                        std::move(options.dictionary()), options.concatenate());
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void ZlibReader<Src>::Done() {
  ZlibReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void ZlibReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  ZlibReaderBase::SetReadAllHintImpl(read_all_hint);
  if (src_.is_owning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void ZlibReader<Src>::VerifyEndImpl() {
  ZlibReaderBase::VerifyEndImpl();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_ZLIB_ZLIB_READER_H_
