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

#ifndef RIEGELI_ZLIB_ZLIB_WRITER_H_
#define RIEGELI_ZLIB_ZLIB_WRITER_H_

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/zlib/zlib_dictionary.h"  // IWYU pragma: export

struct z_stream_s;  // `zlib.h` has `typedef struct z_stream_s z_stream`.

namespace riegeli {

class Reader;
template <typename Src>
class ZlibReader;

// Template parameter independent part of `ZlibWriter`.
class ZlibWriterBase : public BufferedWriter {
 public:
  // Specifies what format of header to write.
  enum class Header {
    kZlib = 0,   // Zlib header.
    kGzip = 16,  // Gzip header.
    kRaw = -1,   // No header; decompressor must expect no header too.
  };

  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // What format of header to write.
    //
    // Default: `Header::kZlib`.
    static constexpr Header kDefaultHeader = Header::kZlib;
    Options& set_header(Header header) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      header_ = header;
      return *this;
    }
    Options&& set_header(Header header) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_header(header));
    }
    Header header() const { return header_; }

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // `compression_level` must be between `kMinCompressionLevel` (0) and
    // `kMaxCompressionLevel` (9). Default: `kDefaultCompressionLevel` (6).
    static constexpr int kMinCompressionLevel = 0;  // `Z_NO_COMPRESSION`
    static constexpr int kMaxCompressionLevel = 9;  // `Z_BEST_COMPRESSION`
    static constexpr int kDefaultCompressionLevel = 6;
    Options& set_compression_level(int compression_level) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel)
          << "Failed precondition of "
             "ZlibWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel)
          << "Failed precondition of "
             "ZlibWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      compression_level_ = compression_level;
      return *this;
    }
    Options&& set_compression_level(int compression_level) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_compression_level(compression_level));
    }
    int compression_level() const { return compression_level_; }

    // Logarithm of the LZ77 sliding window size. This tunes the tradeoff
    // between compression density and memory usage (higher = better density but
    // more memory).
    //
    // `window_log` must be between `kMinWindowLog` (9) and
    // `kMaxWindowLog` (15). Default: `kDefaultWindowLog` (15).
    static constexpr int kMinWindowLog = 9;
    static constexpr int kMaxWindowLog = 15;      // `MAX_WBITS`
    static constexpr int kDefaultWindowLog = 15;  // `MAX_WBITS`
    Options& set_window_log(int window_log) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      RIEGELI_ASSERT_GE(window_log, kMinWindowLog)
          << "Failed precondition of "
             "ZlibWriterBase::Options::set_window_log(): "
             "window log out of range";
      RIEGELI_ASSERT_LE(window_log, kMaxWindowLog)
          << "Failed precondition of "
             "ZlibWriterBase::Options::set_window_log(): "
             "window log out of range";
      window_log_ = window_log;
      return *this;
    }
    Options&& set_window_log(int window_log) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_window_log(window_log));
    }
    int window_log() const { return window_log_; }

    // Zlib dictionary. The same dictionary must be used for decompression.
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

    // Options for a global `KeyedRecyclingPool` of compression contexts.
    //
    // They tune the amount of memory which is kept to speed up creation of new
    // compression sessions, and usage of a background thread to clean it.
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
    int compression_level_ = kDefaultCompressionLevel;
    int window_log_ = kDefaultWindowLog;
    ZlibDictionary dictionary_;
    RecyclingPoolOptions recycling_pool_options_;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  bool SupportsReadMode() override;

 protected:
  explicit ZlibWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit ZlibWriterBase(BufferOptions buffer_options, int window_bits,
                          ZlibDictionary&& dictionary,
                          const RecyclingPoolOptions& recycling_pool_options);

  ZlibWriterBase(ZlibWriterBase&& that) noexcept;
  ZlibWriterBase& operator=(ZlibWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, int window_bits,
             ZlibDictionary&& dictionary,
             const RecyclingPoolOptions& recycling_pool_options);
  static int GetWindowBits(const Options& options);
  void Initialize(Writer* dest, int compression_level);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  void DoneBehindBuffer(absl::string_view src) override;
  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  struct ZStreamDeleter {
    void operator()(z_stream_s* ptr) const;
  };

  struct ZStreamKey : WithEqual<ZStreamKey> {
    ZStreamKey() = default;
    explicit ZStreamKey(int compression_level, int window_bits)
        : compression_level(compression_level), window_bits(window_bits) {}

    friend bool operator==(ZStreamKey a, ZStreamKey b) {
      return a.compression_level == b.compression_level &&
             a.window_bits == b.window_bits;
    }
    template <typename HashState>
    friend HashState AbslHashValue(HashState hash_state, ZStreamKey self) {
      return HashState::combine(std::move(hash_state), self.compression_level,
                                self.window_bits);
    }

    int compression_level;
    int window_bits;
  };

  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation,
                                         int zlib_code);
  bool WriteInternal(absl::string_view src, Writer& dest, int flush);

  int window_bits_ = 0;
  ZlibDictionary dictionary_;
  RecyclingPoolOptions recycling_pool_options_;
  Position initial_compressed_pos_ = 0;
  KeyedRecyclingPool<z_stream_s, ZStreamKey, ZStreamDeleter>::Handle
      compressor_;

  AssociatedReader<ZlibReader<Reader*>> associated_reader_;
};

// A `Writer` which compresses data with Zlib before passing it to another
// `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `Any<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
//
// The compressed `Writer` must not be accessed until the `ZlibWriter` is closed
// or no longer used, except that it is allowed to read the destination of the
// compressed `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class ZlibWriter : public ZlibWriterBase {
 public:
  // Creates a closed `ZlibWriter`.
  explicit ZlibWriter(Closed) noexcept : ZlibWriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit ZlibWriter(Initializer<Dest> dest, Options options = Options());

  ZlibWriter(ZlibWriter&& that) = default;
  ZlibWriter& operator=(ZlibWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `ZlibWriter`. This avoids
  // constructing a temporary `ZlibWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the compressed `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }
  Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get();
  }

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the compressed `Writer`.
  Dependency<Writer*, Dest> dest_;
};

explicit ZlibWriter(Closed) -> ZlibWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit ZlibWriter(Dest&& dest,
                    ZlibWriterBase::Options options = ZlibWriterBase::Options())
    -> ZlibWriter<TargetT<Dest>>;

// Implementation details follow.

inline ZlibWriterBase::ZlibWriterBase(
    BufferOptions buffer_options, int window_bits, ZlibDictionary&& dictionary,
    const RecyclingPoolOptions& recycling_pool_options)
    : BufferedWriter(buffer_options),
      window_bits_(window_bits),
      dictionary_(std::move(dictionary)),
      recycling_pool_options_(recycling_pool_options) {}

inline ZlibWriterBase::ZlibWriterBase(ZlibWriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      window_bits_(that.window_bits_),
      dictionary_(std::move(that.dictionary_)),
      recycling_pool_options_(that.recycling_pool_options_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      compressor_(std::move(that.compressor_)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline ZlibWriterBase& ZlibWriterBase::operator=(
    ZlibWriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  window_bits_ = that.window_bits_;
  dictionary_ = std::move(that.dictionary_);
  recycling_pool_options_ = that.recycling_pool_options_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  compressor_ = std::move(that.compressor_);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void ZlibWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  window_bits_ = 0;
  recycling_pool_options_ = RecyclingPoolOptions();
  initial_compressed_pos_ = 0;
  compressor_.reset();
  dictionary_ = ZlibDictionary();
  associated_reader_.Reset();
}

inline void ZlibWriterBase::Reset(
    BufferOptions buffer_options, int window_bits, ZlibDictionary&& dictionary,
    const RecyclingPoolOptions& recycling_pool_options) {
  BufferedWriter::Reset(buffer_options);
  window_bits_ = window_bits;
  recycling_pool_options_ = recycling_pool_options;
  initial_compressed_pos_ = 0;
  compressor_.reset();
  dictionary_ = std::move(dictionary);
  associated_reader_.Reset();
}

inline int ZlibWriterBase::GetWindowBits(const Options& options) {
  return options.header() == Header::kRaw
             ? -options.window_log()
             : options.window_log() + static_cast<int>(options.header());
}

template <typename Dest>
inline ZlibWriter<Dest>::ZlibWriter(Initializer<Dest> dest, Options options)
    : ZlibWriterBase(options.buffer_options(), GetWindowBits(options),
                     std::move(options.dictionary()),
                     options.recycling_pool_options()),
      dest_(std::move(dest)) {
  Initialize(dest_.get(), options.compression_level());
}

template <typename Dest>
inline void ZlibWriter<Dest>::Reset(Closed) {
  ZlibWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void ZlibWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  ZlibWriterBase::Reset(options.buffer_options(), GetWindowBits(options),
                        std::move(options.dictionary()),
                        options.recycling_pool_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.compression_level());
}

template <typename Dest>
void ZlibWriter<Dest>::Done() {
  ZlibWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
bool ZlibWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ZlibWriterBase::FlushImpl(flush_type))) return false;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_ZLIB_ZLIB_WRITER_H_
