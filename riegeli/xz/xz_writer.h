// Copyright 2023 Google LLC
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

#ifndef RIEGELI_XZ_XZ_WRITER_H_
#define RIEGELI_XZ_XZ_WRITER_H_

#include <stdint.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "lzma.h"
#include "riegeli/base/arithmetic.h"
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

namespace riegeli {

class Reader;
template <typename Src>
class XzReader;

// Template parameter independent part of `XzWriter`.
class XzWriterBase : public BufferedWriter {
 public:
  // Specifies what container format to write.
  enum class Container {
    kXz,    // Xz container (recommended).
    kLzma,  // Lzma container (legacy file format).
  };

  // Specifies what integrity check to use.
  enum class Check {
    kNone = LZMA_CHECK_NONE,      // No check.
    kCrc32 = LZMA_CHECK_CRC32,    // CRC32 (IEEE 802.3)
    kCrc64 = LZMA_CHECK_CRC64,    // CRC64 (ECMA-182; default)
    kSha256 = LZMA_CHECK_SHA256,  // SHA-256
  };

  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // What container format to write.
    //
    // `Flush()` is effective and `ReadMode()` is supported only with
    // `Container::kXz`.
    //
    // Default: `Container::kXz`.
    static constexpr Container kDefaultContainer = Container::kXz;
    Options& set_container(Container container) & {
      container_ = container;
      return *this;
    }
    Options&& set_container(Container container) && {
      return std::move(set_container(container));
    }
    Container container() const { return container_; }

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower and requires more memory for
    // compression and decompression).
    //
    // `compression_level` must be between `kMinCompressionLevel` (0) and
    // `kMaxCompressionLevel` (9). Default: `kDefaultCompressionLevel` (6).
    static constexpr int kMinCompressionLevel = 0;
    static constexpr int kMaxCompressionLevel = 9;
    static constexpr int kDefaultCompressionLevel = 6;
    Options& set_compression_level(int compression_level) & {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel)
          << "Failed precondition of "
             "XzWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel)
          << "Failed precondition of "
             "XzWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      preset_ = (preset_ & ~LZMA_PRESET_LEVEL_MASK) |
                IntCast<uint32_t>(compression_level);
      return *this;
    }
    Options&& set_compression_level(int compression_level) && {
      return std::move(set_compression_level(compression_level));
    }
    int compression_level() const {
      return IntCast<int>(preset_ & LZMA_PRESET_LEVEL_MASK);
    }

    // Within a given compression level, further tunes the tradeoff between
    // compression density and compression speed (`true` = better density but
    // slower), without affecting memory requirements (only compression requires
    // slightly more memory with compression levels <= 3).
    Options& set_extreme(bool extreme) & {
      preset_ = (preset_ & LZMA_PRESET_LEVEL_MASK) |
                (extreme ? LZMA_PRESET_EXTREME : 0);
      return *this;
    }
    Options&& set_extreme(bool extreme) && {
      return std::move(set_extreme(extreme));
    }
    bool extreme() const { return (preset_ & LZMA_PRESET_EXTREME) != 0; }

    // Integrity check to use.
    //
    // This is effective only with `Container::kXz`.
    //
    // Default: `Check::kCrc64`.
    static constexpr Check kDefaultCheck = Check::kCrc64;
    Options& set_check(Check check) & {
      check_ = check;
      return *this;
    }
    Options&& set_check(Check check) && { return std::move(set_check(check)); }
    Check check() const { return check_; }

    // Number of background threads to use. Larger parallelism can increase
    // throughput, up to a point where it no longer matters; smaller parallelism
    // reduces memory usage. `parallelism() == 0` disables background threads.
    //
    // `parallelism() > 0` is effective only with `Container::kXz`.
    //
    // `parallelism() > 0` has a side effect of forcing `Flush()` to finish the
    // current block, which degrades compression density.
    //
    // Default: 0.
    Options& set_parallelism(int parallelism) & {
      RIEGELI_ASSERT_GE(parallelism, 0)
          << "Failed precondition of XzWriterBase::Options::set_parallelism(): "
             "negative parallelism";
      parallelism_ = parallelism;
      return *this;
    }
    Options&& set_parallelism(int parallelism) && {
      return std::move(set_parallelism(parallelism));
    }
    int parallelism() const { return parallelism_; }

    // Options for a global `KeyedRecyclingPool` of compression contexts.
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
    template <typename Dest>
    friend class XzWriter;  // For `preset_`.

    Container container_ = kDefaultContainer;
    uint32_t preset_ = kDefaultCompressionLevel;
    Check check_ = kDefaultCheck;
    int parallelism_ = 0;
    RecyclingPoolOptions recycling_pool_options_;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const = 0;

  bool SupportsReadMode() override;

 protected:
  explicit XzWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit XzWriterBase(BufferOptions buffer_options, Container container,
                        const RecyclingPoolOptions& recycling_pool_options);

  XzWriterBase(XzWriterBase&& that) noexcept;
  XzWriterBase& operator=(XzWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, Container container,
             const RecyclingPoolOptions& recycling_pool_options);
  void Initialize(Writer* dest, uint32_t preset, Check check, int parallelism);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  void DoneBehindBuffer(absl::string_view src) override;
  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;
  Reader* ReadModeBehindBuffer(Position initial_pos) override;

 private:
  struct LzmaStreamDeleter {
    void operator()(lzma_stream* ptr) const {
      lzma_end(ptr);
      delete ptr;
    }
  };

  struct LzmaStreamKey : WithEqual<LzmaStreamKey> {
    LzmaStreamKey() = default;
    explicit LzmaStreamKey(Container container, bool with_parallelism,
                           uint32_t preset)
        : container(container),
          with_parallelism(with_parallelism),
          preset(preset) {}

    friend bool operator==(LzmaStreamKey a, LzmaStreamKey b) {
      return a.container == b.container &&
             a.with_parallelism == b.with_parallelism && a.preset == b.preset;
    }
    template <typename HashState>
    friend HashState AbslHashValue(HashState hash_state, LzmaStreamKey self) {
      return HashState::combine(std::move(hash_state), self.container,
                                self.with_parallelism, self.preset);
    }

    Container container;
    bool with_parallelism;
    uint32_t preset;
  };

  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation,
                                         lzma_ret liblzma_code);
  bool WriteInternal(absl::string_view src, Writer& dest, lzma_action flush);

  Container container_ = Container::kXz;
  lzma_action flush_action_ = LZMA_SYNC_FLUSH;
  RecyclingPoolOptions recycling_pool_options_;
  Position initial_compressed_pos_ = 0;
  KeyedRecyclingPool<lzma_stream, LzmaStreamKey, LzmaStreamDeleter>::Handle
      compressor_;

  AssociatedReader<XzReader<Reader*>> associated_reader_;
};

// A `Writer` which compresses data with Xz (LZMA) before passing it to another
// `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `Any<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The compressed `Writer` must not be accessed until the `XzWriter` is closed
// or no longer used, except that it is allowed to read the destination of the
// compressed `Writer` immediately after `Flush()`. `Flush()` is effective only
// with `Container::kXz`.
template <typename Dest = Writer*>
class XzWriter : public XzWriterBase {
 public:
  // Creates a closed `XzWriter`.
  explicit XzWriter(Closed) noexcept : XzWriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit XzWriter(Initializer<Dest> dest, Options options = Options());

  XzWriter(XzWriter&& that) = default;
  XzWriter& operator=(XzWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `XzWriter`. This avoids
  // constructing a temporary `XzWriter` and moving from it.
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
explicit XzWriter(Closed) -> XzWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit XzWriter(Dest&& dest,
                  XzWriterBase::Options options = XzWriterBase::Options())
    -> XzWriter<InitializerTargetT<Dest>>;
#endif

// Implementation details follow.

inline XzWriterBase::XzWriterBase(
    BufferOptions buffer_options, Container container,
    const RecyclingPoolOptions& recycling_pool_options)
    : BufferedWriter(buffer_options),
      container_(container),
      recycling_pool_options_(recycling_pool_options) {}

inline XzWriterBase::XzWriterBase(XzWriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      container_(that.container_),
      flush_action_(that.flush_action_),
      recycling_pool_options_(that.recycling_pool_options_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      compressor_(std::move(that.compressor_)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline XzWriterBase& XzWriterBase::operator=(XzWriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  container_ = that.container_;
  flush_action_ = that.flush_action_;
  recycling_pool_options_ = that.recycling_pool_options_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  compressor_ = std::move(that.compressor_);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void XzWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  container_ = Options::kDefaultContainer;
  flush_action_ = LZMA_SYNC_FLUSH;
  recycling_pool_options_ = RecyclingPoolOptions();
  initial_compressed_pos_ = 0;
  compressor_.reset();
  associated_reader_.Reset();
}

inline void XzWriterBase::Reset(
    BufferOptions buffer_options, Container container,
    const RecyclingPoolOptions& recycling_pool_options) {
  BufferedWriter::Reset(buffer_options);
  container_ = container;
  flush_action_ = LZMA_SYNC_FLUSH;
  recycling_pool_options_ = recycling_pool_options;
  initial_compressed_pos_ = 0;
  compressor_.reset();
  associated_reader_.Reset();
}

template <typename Dest>
inline XzWriter<Dest>::XzWriter(Initializer<Dest> dest, Options options)
    : XzWriterBase(options.buffer_options(), options.container(),
                   options.recycling_pool_options()),
      dest_(std::move(dest)) {
  Initialize(dest_.get(), options.preset_, options.check(),
             options.parallelism());
}

template <typename Dest>
inline void XzWriter<Dest>::Reset(Closed) {
  XzWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void XzWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  XzWriterBase::Reset(options.buffer_options(), options.container(),
                      options.recycling_pool_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.preset_, options.check(),
             options.parallelism());
}

template <typename Dest>
void XzWriter<Dest>::Done() {
  XzWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
bool XzWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!XzWriterBase::FlushImpl(flush_type))) return false;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_XZ_XZ_WRITER_H_
