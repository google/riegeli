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

#ifndef RIEGELI_ZSTD_ZSTD_WRITER_H_
#define RIEGELI_ZSTD_ZSTD_WRITER_H_

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/zstd/zstd_dictionary.h"  // IWYU pragma: export
#include "zstd.h"

namespace riegeli {

class Reader;
template <typename Src>
class ZstdReader;

// Template parameter independent part of `ZstdWriter`.
class ZstdWriterBase : public BufferedWriter {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // `compression_level` must be between `kMinCompressionLevel` (-131072) and
    // `kMaxCompressionLevel` (22). Level 0 is currently equivalent to 3.
    // Default: `kDefaultCompressionLevel` (3).
    static constexpr int kMinCompressionLevel =
        -(1 << 17);                                  // `ZSTD_minCLevel()`
    static constexpr int kMaxCompressionLevel = 22;  // `ZSTD_maxCLevel()`
    static constexpr int kDefaultCompressionLevel = 3;
    Options& set_compression_level(int compression_level) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel)
          << "Failed precondition of "
             "ZstdWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel)
          << "Failed precondition of "
             "ZstdWriterBase::Options::set_compression_level(): "
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
    // Special value `absl::nullopt` means to derive `window_log` from
    // `compression_level` and `size_hint`.
    //
    // `window_log` must be `absl::nullopt` or between `kMinWindowLog` (10) and
    // `kMaxWindowLog` (30 in 32-bit build, 31 in 64-bit build). Default:
    // `absl::nullopt`.
    static constexpr int kMinWindowLog = 10;  // `ZSTD_WINDOWLOG_MIN`
    static constexpr int kMaxWindowLog =
        sizeof(size_t) == 4 ? 30 : 31;  // `ZSTD_WINDOWLOG_MAX`
    Options& set_window_log(absl::optional<int> window_log) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      if (window_log != absl::nullopt) {
        RIEGELI_ASSERT_GE(*window_log, kMinWindowLog)
            << "Failed precondition of "
               "ZstdWriterBase::Options::set_window_log(): "
               "window log out of range";
        RIEGELI_ASSERT_LE(*window_log, kMaxWindowLog)
            << "Failed precondition of "
               "ZstdWriterBase::Options::set_window_log(): "
               "window log out of range";
      }
      window_log_ = window_log;
      return *this;
    }
    Options&& set_window_log(absl::optional<int> window_log) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_window_log(window_log));
    }
    absl::optional<int> window_log() const { return window_log_; }

    // Zstd dictionary. The same dictionary must be used for decompression.
    //
    // Default: `ZstdDictionary()`.
    Options& set_dictionary(ZstdDictionary dictionary) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(ZstdDictionary dictionary) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    ZstdDictionary& dictionary() ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return dictionary_;
    }
    const ZstdDictionary& dictionary() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return dictionary_;
    }

    // If `true`, computes checksum of uncompressed data and stores it in the
    // compressed stream. This lets decompression verify the checksum.
    //
    // Default: `false`.
    Options& set_store_checksum(bool store_checksum) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      store_checksum_ = store_checksum;
      return *this;
    }
    Options&& set_store_checksum(bool store_checksum) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_store_checksum(store_checksum));
    }
    bool store_checksum() const { return store_checksum_; }

    // Exact uncompressed size, or `absl::nullopt` if unknown. This may improve
    // compression density and performance, and causes the size to be stored in
    // the compressed stream header.
    //
    // If the pledged size turns out to not match reality, compression fails.
    //
    // Default: `absl::nullopt`.
    Options& set_pledged_size(absl::optional<Position> pledged_size) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      pledged_size_ = pledged_size;
      return *this;
    }
    Options&& set_pledged_size(absl::optional<Position> pledged_size) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_pledged_size(pledged_size));
    }
    absl::optional<Position> pledged_size() const { return pledged_size_; }

    // If `false`, `ZstdWriter` lets the destination choose buffer sizes.
    //
    // If `true`, `ZstdWriter` tries to compress all data in one step:
    //
    //  * Flattens uncompressed data if `pledged_size()` is not `absl::nullopt`.
    //
    //  * Asks the destination for a flat buffer with the maximum possible
    //    compressed size, as long as the uncompressed size is known before
    //    compression begins, e.g. if `pledged_size()` is not `absl::nullopt`.
    //
    // This makes compression slightly faster, but increases memory usage.
    //
    // Default: `false`.
    Options& set_reserve_max_size(bool reserve_max_size) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      reserve_max_size_ = reserve_max_size;
      return *this;
    }
    Options&& set_reserve_max_size(bool reserve_max_size) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_reserve_max_size(reserve_max_size));
    }
    bool reserve_max_size() const { return reserve_max_size_; }

    // Returns effective `BufferOptions` as overridden by other options:
    // If `reserve_max_size()` is `true` and `pledged_size()` is not
    // `absl::nullopt`, then `pledged_size()` overrides `buffer_size()`.
    BufferOptions effective_buffer_options() const {
      BufferOptions options = buffer_options();
      if (reserve_max_size() && pledged_size() != absl::nullopt) {
        options.set_buffer_size(
            UnsignedMax(SaturatingIntCast<size_t>(*pledged_size()), size_t{1}));
      }
      return options;
    }

    // Options for a global `RecyclingPool` of compression contexts.
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
    int compression_level_ = kDefaultCompressionLevel;
    absl::optional<int> window_log_;
    ZstdDictionary dictionary_;
    bool store_checksum_ = false;
    absl::optional<Position> pledged_size_;
    bool reserve_max_size_ = false;
    RecyclingPoolOptions recycling_pool_options_;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  bool SupportsReadMode() override;

 protected:
  explicit ZstdWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit ZstdWriterBase(BufferOptions buffer_options,
                          ZstdDictionary&& dictionary,
                          absl::optional<Position> pledged_size,
                          bool reserve_max_size,
                          const RecyclingPoolOptions& recycling_pool_options);

  ZstdWriterBase(ZstdWriterBase&& that) noexcept;
  ZstdWriterBase& operator=(ZstdWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, ZstdDictionary&& dictionary,
             absl::optional<Position> pledged_size, bool reserve_max_size,
             const RecyclingPoolOptions& recycling_pool_options);
  void Initialize(Writer* dest, int compression_level,
                  absl::optional<int> window_log, bool store_checksum);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  void DoneBehindBuffer(absl::string_view src) override;
  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  struct ZSTD_CCtxDeleter {
    void operator()(ZSTD_CCtx* ptr) const { ZSTD_freeCCtx(ptr); }
  };

  bool WriteInternal(absl::string_view src, Writer& dest,
                     ZSTD_EndDirective end_op);

  ZstdDictionary dictionary_;
  ZstdDictionary::ZSTD_CDictHandle compression_dictionary_;
  absl::optional<Position> pledged_size_;
  bool reserve_max_size_ = false;
  RecyclingPoolOptions recycling_pool_options_;
  Position initial_compressed_pos_ = 0;
  // If `ok()` but `compressor_ == nullptr` then `*pledged_size_` has been
  // reached. In this case `ZSTD_compressStream()` must not be called again.
  RecyclingPool<ZSTD_CCtx, ZSTD_CCtxDeleter>::Handle compressor_;

  AssociatedReader<ZstdReader<Reader*>> associated_reader_;
};

// A `Writer` which compresses data with Zstd before passing it to another
// `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `Any<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument. This requires C++17.
//
// The compressed `Writer` must not be accessed until the `ZstdWriter` is closed
// or no longer used, except that it is allowed to read the destination of the
// compressed `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class ZstdWriter : public ZstdWriterBase {
 public:
  // Creates a closed `ZstdWriter`.
  explicit ZstdWriter(Closed) noexcept : ZstdWriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit ZstdWriter(Initializer<Dest> dest, Options options = Options());

  ZstdWriter(ZstdWriter&& that) = default;
  ZstdWriter& operator=(ZstdWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `ZstdWriter`. This avoids
  // constructing a temporary `ZstdWriter` and moving from it.
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

// Support CTAD.
#if __cpp_deduction_guides
explicit ZstdWriter(Closed) -> ZstdWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit ZstdWriter(Dest&& dest,
                    ZstdWriterBase::Options options = ZstdWriterBase::Options())
    -> ZstdWriter<TargetT<Dest>>;
#endif

// Implementation details follow.

inline ZstdWriterBase::ZstdWriterBase(
    BufferOptions buffer_options, ZstdDictionary&& dictionary,
    absl::optional<Position> pledged_size, bool reserve_max_size,
    const RecyclingPoolOptions& recycling_pool_options)
    : BufferedWriter(buffer_options),
      dictionary_(std::move(dictionary)),
      pledged_size_(pledged_size),
      reserve_max_size_(reserve_max_size),
      recycling_pool_options_(recycling_pool_options) {}

inline ZstdWriterBase::ZstdWriterBase(ZstdWriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      dictionary_(std::move(that.dictionary_)),
      compression_dictionary_(std::move(that.compression_dictionary_)),
      pledged_size_(that.pledged_size_),
      reserve_max_size_(that.reserve_max_size_),
      recycling_pool_options_(that.recycling_pool_options_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      compressor_(std::move(that.compressor_)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline ZstdWriterBase& ZstdWriterBase::operator=(
    ZstdWriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  dictionary_ = std::move(that.dictionary_);
  compression_dictionary_ = std::move(that.compression_dictionary_);
  pledged_size_ = that.pledged_size_;
  reserve_max_size_ = that.reserve_max_size_;
  recycling_pool_options_ = that.recycling_pool_options_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  compressor_ = std::move(that.compressor_);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void ZstdWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  pledged_size_ = absl::nullopt;
  reserve_max_size_ = false;
  recycling_pool_options_ = RecyclingPoolOptions();
  initial_compressed_pos_ = 0;
  compressor_.reset();
  dictionary_ = ZstdDictionary();
  compression_dictionary_.reset();
  associated_reader_.Reset();
}

inline void ZstdWriterBase::Reset(
    BufferOptions buffer_options, ZstdDictionary&& dictionary,
    absl::optional<Position> pledged_size, bool reserve_max_size,
    const RecyclingPoolOptions& recycling_pool_options) {
  BufferedWriter::Reset(buffer_options);
  pledged_size_ = pledged_size;
  reserve_max_size_ = reserve_max_size;
  recycling_pool_options_ = recycling_pool_options;
  initial_compressed_pos_ = 0;
  compressor_.reset();
  dictionary_ = std::move(dictionary);
  compression_dictionary_.reset();
  associated_reader_.Reset();
}

template <typename Dest>
inline ZstdWriter<Dest>::ZstdWriter(Initializer<Dest> dest, Options options)
    : ZstdWriterBase(options.effective_buffer_options(),
                     std::move(options.dictionary()), options.pledged_size(),
                     options.reserve_max_size(),
                     options.recycling_pool_options()),
      dest_(std::move(dest)) {
  Initialize(dest_.get(), options.compression_level(), options.window_log(),
             options.store_checksum());
}

template <typename Dest>
inline void ZstdWriter<Dest>::Reset(Closed) {
  ZstdWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void ZstdWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  ZstdWriterBase::Reset(options.effective_buffer_options(),
                        std::move(options.dictionary()), options.pledged_size(),
                        options.reserve_max_size(),
                        options.recycling_pool_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.compression_level(), options.window_log(),
             options.store_checksum());
}

template <typename Dest>
void ZstdWriter<Dest>::Done() {
  ZstdWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
bool ZstdWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ZstdWriterBase::FlushImpl(flush_type))) return false;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_ZSTD_ZSTD_WRITER_H_
