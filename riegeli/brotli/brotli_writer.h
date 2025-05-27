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

#ifndef RIEGELI_BROTLI_BROTLI_WRITER_H_
#define RIEGELI_BROTLI_BROTLI_WRITER_H_

#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "brotli/encode.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/brotli/brotli_allocator.h"   // IWYU pragma: export
#include "riegeli/brotli/brotli_dictionary.h"  // IWYU pragma: export
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class BrotliReader;
class Reader;

// Template parameter independent part of `BrotliWriter`.
class BrotliWriterBase : public BufferedWriter {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // `compression_level` must be between `kMinCompressionLevel` (0) and
    // `kMaxCompressionLevel` (11). Default: `kDefaultCompressionLevel` (6).
    static constexpr int kMinCompressionLevel = BROTLI_MIN_QUALITY;
    static constexpr int kMaxCompressionLevel = BROTLI_MAX_QUALITY;
    static constexpr int kDefaultCompressionLevel = 6;
    Options& set_compression_level(int compression_level) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel)
          << "Failed precondition of "
             "BrotliWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel)
          << "Failed precondition of "
             "BrotliWriterBase::Options::set_compression_level(): "
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
    // `window_log` must be between `kMinWindowLog` (10) and `kMaxWindowLog`
    // (30). Default: `kDefaultWindowLog` (22).
    static constexpr int kMinWindowLog = BROTLI_MIN_WINDOW_BITS;
    static constexpr int kMaxWindowLog = BROTLI_LARGE_MAX_WINDOW_BITS;
    static constexpr int kDefaultWindowLog = BROTLI_DEFAULT_WINDOW;
    Options& set_window_log(int window_log) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      RIEGELI_ASSERT_GE(window_log, kMinWindowLog)
          << "Failed precondition of "
             "BrotliWriterBase::Options::set_window_log(): "
             "window log out of range";
      RIEGELI_ASSERT_LE(window_log, kMaxWindowLog)
          << "Failed precondition of "
             "BrotliWriterBase::Options::set_window_log(): "
             "window log out of range";
      window_log_ = window_log;
      return *this;
    }
    Options&& set_window_log(int window_log) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_window_log(window_log));
    }
    int window_log() const { return window_log_; }

    // Shared Brotli dictionary. The same dictionary must have been used for
    // compression.
    //
    // Default: `BrotliDictionary()`.
    Options& set_dictionary(BrotliDictionary dictionary) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      dictionary_ = std::move(dictionary);
      return *this;
    }
    Options&& set_dictionary(BrotliDictionary dictionary) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_dictionary(std::move(dictionary)));
    }
    BrotliDictionary& dictionary() ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return dictionary_;
    }
    const BrotliDictionary& dictionary() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return dictionary_;
    }

    // Memory allocator used by the Brotli engine.
    //
    // Default: `BrotliAllocator()`.
    Options& set_allocator(BrotliAllocator allocator) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      allocator_ = std::move(allocator);
      return *this;
    }
    Options&& set_allocator(BrotliAllocator allocator) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_allocator(std::move(allocator)));
    }
    BrotliAllocator& allocator() ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return allocator_;
    }
    const BrotliAllocator& allocator() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return allocator_;
    }

   private:
    int compression_level_ = kDefaultCompressionLevel;
    int window_log_ = kDefaultWindowLog;
    BrotliDictionary dictionary_;
    BrotliAllocator allocator_;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  bool SupportsReadMode() override;

 protected:
  explicit BrotliWriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit BrotliWriterBase(BufferOptions buffer_options,
                            BrotliDictionary&& dictionary,
                            BrotliAllocator&& allocator);

  BrotliWriterBase(BrotliWriterBase&& that) noexcept;
  BrotliWriterBase& operator=(BrotliWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, BrotliDictionary&& dictionary,
             BrotliAllocator&& allocator);
  void Initialize(Writer* dest, int compression_level, int window_log);
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
  struct BrotliEncoderStateDeleter {
    void operator()(BrotliEncoderState* ptr) const {
      BrotliEncoderDestroyInstance(ptr);
    }
  };

  bool WriteInternal(absl::string_view src, Writer& dest,
                     BrotliEncoderOperation op);

  BrotliDictionary dictionary_;
  BrotliAllocator allocator_;
  Position initial_compressed_pos_ = 0;
  std::unique_ptr<BrotliEncoderState, BrotliEncoderStateDeleter> compressor_;

  AssociatedReader<BrotliReader<Reader*>> associated_reader_;
};

// A `Writer` which compresses data with Brotli before passing it to another
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
// The compressed `Writer` must not be accessed until the `BrotliWriter` is
// closed or no longer used, except that it is allowed to read the destination
// of the compressed `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class BrotliWriter : public BrotliWriterBase {
 public:
  // Creates a closed `BrotliWriter`.
  explicit BrotliWriter(Closed) noexcept : BrotliWriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit BrotliWriter(Initializer<Dest> dest, Options options = Options());

  BrotliWriter(BrotliWriter&& that) = default;
  BrotliWriter& operator=(BrotliWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `BrotliWriter`. This avoids
  // constructing a temporary `BrotliWriter` and moving from it.
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

explicit BrotliWriter(Closed) -> BrotliWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit BrotliWriter(Dest&& dest, BrotliWriterBase::Options options =
                                       BrotliWriterBase::Options())
    -> BrotliWriter<TargetT<Dest>>;

// Implementation details follow.

inline BrotliWriterBase::BrotliWriterBase(BufferOptions buffer_options,
                                          BrotliDictionary&& dictionary,
                                          BrotliAllocator&& allocator)
    : BufferedWriter(buffer_options),
      dictionary_(std::move(dictionary)),
      allocator_(std::move(allocator)) {}

inline BrotliWriterBase::BrotliWriterBase(BrotliWriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      dictionary_(std::move(that.dictionary_)),
      allocator_(std::move(that.allocator_)),
      initial_compressed_pos_(that.initial_compressed_pos_),
      compressor_(std::move(that.compressor_)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline BrotliWriterBase& BrotliWriterBase::operator=(
    BrotliWriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  dictionary_ = std::move(that.dictionary_);
  allocator_ = std::move(that.allocator_);
  initial_compressed_pos_ = that.initial_compressed_pos_;
  compressor_ = std::move(that.compressor_);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void BrotliWriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  initial_compressed_pos_ = 0;
  compressor_.reset();
  dictionary_ = BrotliDictionary();
  allocator_ = BrotliAllocator();
  associated_reader_.Reset();
}

inline void BrotliWriterBase::Reset(BufferOptions buffer_options,
                                    BrotliDictionary&& dictionary,
                                    BrotliAllocator&& allocator) {
  BufferedWriter::Reset(buffer_options);
  initial_compressed_pos_ = 0;
  compressor_.reset();
  dictionary_ = std::move(dictionary);
  allocator_ = std::move(allocator);
  associated_reader_.Reset();
}

template <typename Dest>
inline BrotliWriter<Dest>::BrotliWriter(Initializer<Dest> dest, Options options)
    : BrotliWriterBase(options.buffer_options(),
                       std::move(options.dictionary()),
                       std::move(options.allocator())),
      dest_(std::move(dest)) {
  Initialize(dest_.get(), options.compression_level(), options.window_log());
}

template <typename Dest>
inline void BrotliWriter<Dest>::Reset(Closed) {
  BrotliWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void BrotliWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  BrotliWriterBase::Reset(options.buffer_options(),
                          std::move(options.dictionary()),
                          std::move(options.allocator()));
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.compression_level(), options.window_log());
}

template <typename Dest>
void BrotliWriter<Dest>::Done() {
  BrotliWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
bool BrotliWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!BrotliWriterBase::FlushImpl(flush_type))) {
    return false;
  }
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BROTLI_BROTLI_WRITER_H_
