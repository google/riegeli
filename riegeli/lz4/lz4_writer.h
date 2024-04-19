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

#ifndef RIEGELI_LZ4_LZ4_WRITER_H_
#define RIEGELI_LZ4_LZ4_WRITER_H_

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "lz4frame.h"
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
#include "riegeli/lz4/lz4_dictionary.h"  // IWYU pragma: export

namespace riegeli {

class Reader;
template <typename Src>
class Lz4Reader;

// Template parameter independent part of `Lz4Writer`.
class Lz4WriterBase : public BufferedWriter {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {
      RIEGELI_ASSERT_EQ(LZ4F_compressionLevel_max(), kMaxCompressionLevel)
          << "Unexpected value of LZ4F_compressionLevel_max()";
    }

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // `compression_level` must be between `kMinCompressionLevel` (`-65536`)
    // and `kMaxCompressionLevel` (12). Levels [0..2] are currently equivalent.
    // Default: `kDefaultCompressionLevel` (0).
    static constexpr int kMinCompressionLevel = -(64 << 10);
    static constexpr int kMaxCompressionLevel =
        12;  // `LZ4F_compressionLevel_max()`
    static constexpr int kDefaultCompressionLevel = 0;
    Options& set_compression_level(int compression_level) & {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel)
          << "Failed precondition of "
             "Lz4WriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel)
          << "Failed precondition of "
             "Lz4WriterBase::Options::set_compression_level(): "
             "compression level out of range";
      compression_level_ = compression_level;
      return *this;
    }
    Options&& set_compression_level(int compression_level) && {
      return std::move(set_compression_level(compression_level));
    }
    int compression_level() const { return compression_level_; }

    // Logarithm of the block size. This tunes the tradeoff between compression
    // density and memory usage (higher = better density but more memory).
    //
    // Lz4 supports four effective values: 16, 18, 20, and 22. Other values are
    // rounded downwards.
    //
    // `window_log` must be between `kMinWindowLog` (0) and `kMaxWindowLog`
    // (22). Default: `kDefaultWindowLog` (16).
    static constexpr int kMinWindowLog = 16;
    static constexpr int kMaxWindowLog = 22;
    static constexpr int kDefaultWindowLog = 16;
    Options& set_window_log(int window_log) & {
      RIEGELI_ASSERT_GE(window_log, kMinWindowLog)
          << "Failed precondition of Lz4WriterBase::Options::set_window_log(): "
             "window log out of range";
      RIEGELI_ASSERT_LE(window_log, kMaxWindowLog)
          << "Failed precondition of Lz4WriterBase::Options::set_window_log(): "
             "window log out of range";
      window_log_ = window_log;
      return *this;
    }
    Options&& set_window_log(int window_log) && {
      return std::move(set_window_log(window_log));
    }
    int window_log() const { return window_log_; }

    // Lz4 dictionary. The same dictionary must be used for decompression.
    //
    // Default: `Lz4Dictionary()`.
    Options& set_dictionary(Lz4Dictionary dictionary) & {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(Lz4Dictionary dictionary) && {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    Lz4Dictionary& dictionary() { return dictionary_; }
    const Lz4Dictionary& dictionary() const { return dictionary_; }

    // If `true`, computes checksum of uncompressed data and stores it in the
    // compressed stream for each frame, i.e. at coarse granularity. This lets
    // decompression verify the checksum.
    //
    // Default: `false`.
    Options& set_store_content_checksum(bool store_content_checksum) & {
      store_content_checksum_ = store_content_checksum;
      return *this;
    }
    Options&& set_store_content_checksum(bool store_content_checksum) && {
      return std::move(set_store_content_checksum(store_content_checksum));
    }
    bool store_content_checksum() const { return store_content_checksum_; }

    // If `true`, computes checksum of uncompressed data and stores it in the
    // compressed stream for each block, i.e. at coarse granularity. This lets
    // decompression verify the checksum.
    //
    // Default: `false`.
    Options& set_store_block_checksum(bool store_block_checksum) & {
      store_block_checksum_ = store_block_checksum;
      return *this;
    }
    Options&& set_store_block_checksum(bool store_block_checksum) && {
      return std::move(set_store_block_checksum(store_block_checksum));
    }
    bool store_block_checksum() const { return store_block_checksum_; }

    // Exact uncompressed size, or `absl::nullopt` if unknown. This may improve
    // compression density and performance, and causes the size to be stored in
    // the compressed stream header.
    //
    // If the pledged size turns out to not match reality, compression fails.
    //
    // Default: `absl::nullopt`.
    Options& set_pledged_size(absl::optional<Position> pledged_size) & {
      pledged_size_ = pledged_size;
      return *this;
    }
    Options&& set_pledged_size(absl::optional<Position> pledged_size) && {
      return std::move(set_pledged_size(pledged_size));
    }
    absl::optional<Position> pledged_size() const { return pledged_size_; }

    // If `false`, `Lz4Writer` lets the destination choose buffer sizes
    // (at least the maximum possible compressed size of a block size though).
    //
    // If `true`, `Lz4Writer` tries to compress all data in one step:
    //
    //  * Flattens uncompressed data if `pledged_size()` is not `absl::nullopt`.
    //
    //  * Asks the destination for a flat buffer with the maximum possible
    //    compressed size for each flat piece of uncompressed data.
    //
    // This makes compression slightly faster, but increases memory usage.
    //
    // Default: `false`.
    Options& set_reserve_max_size(bool reserve_max_size) & {
      reserve_max_size_ = reserve_max_size;
      return *this;
    }
    Options&& set_reserve_max_size(bool reserve_max_size) && {
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
    int compression_level_ = kDefaultCompressionLevel;
    int window_log_ = kDefaultWindowLog;
    Lz4Dictionary dictionary_;
    bool store_content_checksum_ = false;
    bool store_block_checksum_ = false;
    absl::optional<Position> pledged_size_;
    bool reserve_max_size_ = false;
    RecyclingPoolOptions recycling_pool_options_;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const = 0;

  bool SupportsReadMode() override;

 protected:
  explicit Lz4WriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit Lz4WriterBase(BufferOptions buffer_options,
                         Lz4Dictionary&& dictionary,
                         absl::optional<Position> pledged_size,
                         bool reserve_max_size,
                         const RecyclingPoolOptions& recycling_pool_options);

  Lz4WriterBase(Lz4WriterBase&& that) noexcept;
  Lz4WriterBase& operator=(Lz4WriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, Lz4Dictionary&& dictionary,
             absl::optional<Position> pledged_size, bool reserve_max_size,
             const RecyclingPoolOptions& recycling_pool_options);
  void Initialize(Writer* dest, int compression_level, int window_log,
                  bool store_content_checksum, bool store_block_checksum);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushImpl(FlushType flush_type);
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  struct LZ4F_cctxDeleter {
    void operator()(LZ4F_cctx* ptr) const {
      const LZ4F_errorCode_t result = LZ4F_freeCompressionContext(ptr);
      RIEGELI_ASSERT(!LZ4F_isError(result))
          << "LZ4F_freeCompressionContext() failed: "
          << LZ4F_getErrorName(result);
    }
  };

  bool DoneCompression(Writer& dest);

  Lz4Dictionary dictionary_;
  absl::optional<Position> pledged_size_;
  bool reserve_max_size_ = false;
  RecyclingPoolOptions recycling_pool_options_;
  Position initial_compressed_pos_ = 0;
  LZ4F_preferences_t preferences_{};
  // If `ok()` but `compressor_ == nullptr` then `LZ4F_compressEnd()` was
  // already called.
  RecyclingPool<LZ4F_cctx, LZ4F_cctxDeleter>::Handle compressor_;
  // `stable_src_` becomes `true` when all data remaining to compress are known
  // to stay under their current addresses.
  bool stable_src_ = false;
  // The amount of uncompressed data buffered in `LZ4F_cctx`. This allows to
  // reduce data copying by aligning source boundaries appropriately.
  size_t buffered_length_ = 0;

  AssociatedReader<Lz4Reader<Reader*>> associated_reader_;
};

// A `Writer` which compresses data with Lz4 before passing it to another
// `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyDependency<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The compressed `Writer` must not be accessed until the `Lz4Writer` is closed
// or no longer used, except that it is allowed to read the destination of the
// compressed `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class Lz4Writer : public Lz4WriterBase {
 public:
  // Creates a closed `Lz4Writer`.
  explicit Lz4Writer(Closed) noexcept : Lz4WriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit Lz4Writer(Initializer<Dest> dest, Options options = Options());

  Lz4Writer(Lz4Writer&& that) = default;
  Lz4Writer& operator=(Lz4Writer&& that) = default;

  // Makes `*this` equivalent to a newly constructed `Lz4Writer`. This avoids
  // constructing a temporary `Lz4Writer` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the compressed `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the compressed `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit Lz4Writer(Closed) -> Lz4Writer<DeleteCtad<Closed>>;
template <typename Dest>
explicit Lz4Writer(Dest&& dest,
                   Lz4WriterBase::Options options = Lz4WriterBase::Options())
    -> Lz4Writer<InitializerTargetT<Dest>>;
#endif

// Implementation details follow.

inline Lz4WriterBase::Lz4WriterBase(
    BufferOptions buffer_options, Lz4Dictionary&& dictionary,
    absl::optional<Position> pledged_size, bool reserve_max_size,
    const RecyclingPoolOptions& recycling_pool_options)
    : BufferedWriter(buffer_options),
      dictionary_(std::move(dictionary)),
      pledged_size_(pledged_size),
      reserve_max_size_(reserve_max_size),
      recycling_pool_options_(recycling_pool_options) {}

inline Lz4WriterBase::Lz4WriterBase(Lz4WriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      dictionary_(std::move(that.dictionary_)),
      pledged_size_(that.pledged_size_),
      reserve_max_size_(that.reserve_max_size_),
      recycling_pool_options_(that.recycling_pool_options_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      preferences_(that.preferences_),
      compressor_(std::move(that.compressor_)),
      stable_src_(that.stable_src_),
      buffered_length_(that.buffered_length_),
      associated_reader_(std::move(that.associated_reader_)) {}

inline Lz4WriterBase& Lz4WriterBase::operator=(Lz4WriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  dictionary_ = std::move(that.dictionary_);
  pledged_size_ = that.pledged_size_;
  reserve_max_size_ = that.reserve_max_size_;
  recycling_pool_options_ = that.recycling_pool_options_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  preferences_ = that.preferences_;
  compressor_ = std::move(that.compressor_);
  stable_src_ = that.stable_src_;
  buffered_length_ = that.buffered_length_;
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void Lz4WriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  pledged_size_ = absl::nullopt;
  reserve_max_size_ = false;
  recycling_pool_options_ = RecyclingPoolOptions();
  initial_compressed_pos_ = 0;
  preferences_ = {};
  compressor_.reset();
  dictionary_ = Lz4Dictionary();
  stable_src_ = false;
  buffered_length_ = 0;
  associated_reader_.Reset();
}

inline void Lz4WriterBase::Reset(
    BufferOptions buffer_options, Lz4Dictionary&& dictionary,
    absl::optional<Position> pledged_size, bool reserve_max_size,
    const RecyclingPoolOptions& recycling_pool_options) {
  BufferedWriter::Reset(buffer_options);
  pledged_size_ = pledged_size;
  reserve_max_size_ = reserve_max_size;
  recycling_pool_options_ = recycling_pool_options;
  initial_compressed_pos_ = 0;
  preferences_ = {};
  compressor_.reset();
  dictionary_ = std::move(dictionary);
  stable_src_ = false;
  buffered_length_ = 0;
  associated_reader_.Reset();
}

template <typename Dest>
inline Lz4Writer<Dest>::Lz4Writer(Initializer<Dest> dest, Options options)
    : Lz4WriterBase(options.effective_buffer_options(),
                    std::move(options.dictionary()), options.pledged_size(),
                    options.reserve_max_size(),
                    options.recycling_pool_options()),
      dest_(std::move(dest)) {
  Initialize(dest_.get(), options.compression_level(), options.window_log(),
             options.store_content_checksum(), options.store_block_checksum());
}

template <typename Dest>
inline void Lz4Writer<Dest>::Reset(Closed) {
  Lz4WriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void Lz4Writer<Dest>::Reset(Initializer<Dest> dest, Options options) {
  Lz4WriterBase::Reset(options.effective_buffer_options(),
                       std::move(options.dictionary()), options.pledged_size(),
                       options.reserve_max_size(),
                       options.recycling_pool_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.compression_level(), options.window_log(),
             options.store_content_checksum(), options.store_block_checksum());
}

template <typename Dest>
void Lz4Writer<Dest>::Done() {
  Lz4WriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
bool Lz4Writer<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!Lz4WriterBase::FlushImpl(flush_type))) return false;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_LZ4_LZ4_WRITER_H_
