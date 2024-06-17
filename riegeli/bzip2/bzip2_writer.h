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

#ifndef RIEGELI_BZIP2_BZIP2_WRITER_H_
#define RIEGELI_BZIP2_BZIP2_WRITER_H_

#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "bzlib.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `Bzip2Writer`.
class Bzip2WriterBase : public BufferedWriter {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // `compression_level` must be between `kMinCompressionLevel` (1) and
    // `kMaxCompressionLevel` (9). Default: `kDefaultCompressionLevel` (9).
    static constexpr int kMinCompressionLevel = 1;
    static constexpr int kMaxCompressionLevel = 9;
    static constexpr int kDefaultCompressionLevel = 9;
    Options& set_compression_level(int compression_level) & {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel)
          << "Failed precondition of "
             "Bzip2WriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel)
          << "Failed precondition of "
             "Bzip2WriterBase::Options::set_compression_level(): "
             "compression level out of range";
      compression_level_ = compression_level;
      return *this;
    }
    Options&& set_compression_level(int compression_level) && {
      return std::move(set_compression_level(compression_level));
    }
    int compression_level() const { return compression_level_; }

   private:
    int compression_level_ = kDefaultCompressionLevel;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const = 0;

 protected:
  explicit Bzip2WriterBase(Closed) noexcept : BufferedWriter(kClosed) {}

  explicit Bzip2WriterBase(BufferOptions buffer_options);

  Bzip2WriterBase(Bzip2WriterBase&& that) noexcept;
  Bzip2WriterBase& operator=(Bzip2WriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options);
  void Initialize(Writer* dest, int compression_level);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  void DoneBehindBuffer(absl::string_view src) override;
  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool WriteInternal(absl::string_view src) override;
  bool FlushBehindBuffer(absl::string_view src, FlushType flush_type) override;

 private:
  struct BZStreamDeleter {
    void operator()(bz_stream* ptr) const {
      const int bzlib_code = BZ2_bzCompressEnd(ptr);
      RIEGELI_ASSERT(bzlib_code == BZ_OK)
          << "BZ2_bzCompressEnd() failed: " << bzlib_code;
      delete ptr;
    }
  };

  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation,
                                         int bzlib_code);
  bool WriteInternal(absl::string_view src, Writer& dest, int flush);

  Position initial_compressed_pos_ = 0;
  std::unique_ptr<bz_stream, BZStreamDeleter> compressor_;
};

// A `Writer` which compresses data with Bzip2 before passing it to another
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
// The compressed `Writer` must not be accessed until the `Bzip2Writer` is
// closed or no longer used.
//
// Because Bzip2 blocks are bit-aligned, `Flush()` is not effective (the effect
// is usually delayed by one block) and `ReadMode()` is not supported.
template <typename Dest = Writer*>
class Bzip2Writer : public Bzip2WriterBase {
 public:
  // Creates a closed `Bzip2Writer`.
  explicit Bzip2Writer(Closed) noexcept : Bzip2WriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit Bzip2Writer(Initializer<Dest> dest, Options options = Options());

  Bzip2Writer(Bzip2Writer&& that) = default;
  Bzip2Writer& operator=(Bzip2Writer&& that) = default;

  // Makes `*this` equivalent to a newly constructed `Bzip2Writer`. This avoids
  // constructing a temporary `Bzip2Writer` and moving from it.
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
explicit Bzip2Writer(Closed) -> Bzip2Writer<DeleteCtad<Closed>>;
template <typename Dest>
explicit Bzip2Writer(
    Dest&& dest, Bzip2WriterBase::Options options = Bzip2WriterBase::Options())
    -> Bzip2Writer<InitializerTargetT<Dest>>;
#endif

// Implementation details follow.

inline Bzip2WriterBase::Bzip2WriterBase(BufferOptions buffer_options)
    : BufferedWriter(buffer_options) {}

inline Bzip2WriterBase::Bzip2WriterBase(Bzip2WriterBase&& that) noexcept
    : BufferedWriter(static_cast<BufferedWriter&&>(that)),
      initial_compressed_pos_(that.initial_compressed_pos_),
      compressor_(std::move(that.compressor_)) {}

inline Bzip2WriterBase& Bzip2WriterBase::operator=(
    Bzip2WriterBase&& that) noexcept {
  BufferedWriter::operator=(static_cast<BufferedWriter&&>(that));
  initial_compressed_pos_ = that.initial_compressed_pos_;
  compressor_ = std::move(that.compressor_);
  return *this;
}

inline void Bzip2WriterBase::Reset(Closed) {
  BufferedWriter::Reset(kClosed);
  initial_compressed_pos_ = 0;
  compressor_.reset();
}

inline void Bzip2WriterBase::Reset(BufferOptions buffer_options) {
  BufferedWriter::Reset(buffer_options);
  initial_compressed_pos_ = 0;
  compressor_.reset();
}

template <typename Dest>
inline Bzip2Writer<Dest>::Bzip2Writer(Initializer<Dest> dest, Options options)
    : Bzip2WriterBase(options.buffer_options()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.compression_level());
}

template <typename Dest>
inline void Bzip2Writer<Dest>::Reset(Closed) {
  Bzip2WriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void Bzip2Writer<Dest>::Reset(Initializer<Dest> dest, Options options) {
  Bzip2WriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.compression_level());
}

template <typename Dest>
void Bzip2Writer<Dest>::Done() {
  Bzip2WriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
bool Bzip2Writer<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!Bzip2WriterBase::FlushImpl(flush_type))) return false;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BZIP2_BZIP2_WRITER_H_
