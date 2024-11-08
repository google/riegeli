// Copyright 2020 Google LLC
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

#ifndef RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_WRITER_H_
#define RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_WRITER_H_

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/writer.h"
#include "snappy.h"

namespace riegeli {

template <typename Src>
class HadoopSnappyReader;
class Reader;

// Template parameter independent part of `HadoopSnappyWriter`.
class HadoopSnappyWriterBase : public PushableWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // `compression_level` must be between `kMinCompressionLevel` (1) and
    // `kMaxCompressionLevel` (2).
    static constexpr int kMinCompressionLevel =
        snappy::CompressionOptions::MinCompressionLevel();
    static constexpr int kMaxCompressionLevel =
        snappy::CompressionOptions::MaxCompressionLevel();
    static constexpr int kDefaultCompressionLevel =
        snappy::CompressionOptions::DefaultCompressionLevel();
    Options& set_compression_level(int compression_level) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel)
          << "Failed precondition of "
             "HadoopSnappyWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel)
          << "Failed precondition of "
             "HadoopSnappyWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      compression_level_ = compression_level;
      return *this;
    }
    Options&& set_compression_level(int compression_level) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_compression_level(compression_level));
    }
    int compression_level() const { return compression_level_; }

   private:
    int compression_level_ = kDefaultCompressionLevel;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  bool SupportsReadMode() override;

 protected:
  explicit HadoopSnappyWriterBase(Closed) noexcept : PushableWriter(kClosed) {}

  explicit HadoopSnappyWriterBase() {}

  HadoopSnappyWriterBase(HadoopSnappyWriterBase&& that) noexcept;
  HadoopSnappyWriterBase& operator=(HadoopSnappyWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Writer* dest, int compression_level);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool PushBehindScratch(size_t recommended_length) override;
  bool FlushBehindScratch(FlushType flush_type);
  Reader* ReadModeBehindScratch(Position initial_pos) override;

 private:
  // Compresses buffered data, but unlike `PushSlow()`, does not ensure that a
  // buffer is allocated.
  //
  // Precondition: `ok()`
  //
  // Postcondition: `start_to_cursor() == 0`
  bool PushInternal(Writer& dest);

  int compression_level_ = Options::kDefaultCompressionLevel;
  absl::optional<Position> size_hint_;
  Position initial_compressed_pos_ = 0;
  // Buffered uncompressed data.
  Buffer uncompressed_;

  AssociatedReader<HadoopSnappyReader<Reader*>> associated_reader_;

  // Invariants if scratch is not used:
  //   `start() == nullptr` or `start() == uncompressed_.data()`
  //   `start_to_limit() <= snappy::kBlockSize`
};

// A `Writer` which compresses data with Hadoop Snappy format before passing it
// to another `Writer`:
// https://github.com/kubo/snzip#hadoop-snappy-format
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
// The compressed `Writer` must not be accessed until the `HadoopSnappyWriter`
// is closed or no longer used.
template <typename Dest = Writer*>
class HadoopSnappyWriter : public HadoopSnappyWriterBase {
 public:
  // Creates a closed `HadoopSnappyWriter`.
  explicit HadoopSnappyWriter(Closed) noexcept
      : HadoopSnappyWriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit HadoopSnappyWriter(Initializer<Dest> dest,
                              Options options = Options());

  HadoopSnappyWriter(HadoopSnappyWriter&& that) = default;
  HadoopSnappyWriter& operator=(HadoopSnappyWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `HadoopSnappyWriter`. This
  // avoids constructing a temporary `HadoopSnappyWriter` and moving from it.
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
explicit HadoopSnappyWriter(Closed) -> HadoopSnappyWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit HadoopSnappyWriter(
    Dest&& dest,
    HadoopSnappyWriterBase::Options options = HadoopSnappyWriterBase::Options())
    -> HadoopSnappyWriter<TargetT<Dest>>;
#endif

// Implementation details follow.

inline HadoopSnappyWriterBase::HadoopSnappyWriterBase(
    HadoopSnappyWriterBase&& that) noexcept
    : PushableWriter(static_cast<PushableWriter&&>(that)),
      compression_level_(that.compression_level_),
      size_hint_(that.size_hint_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      uncompressed_(std::move(that.uncompressed_)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline HadoopSnappyWriterBase& HadoopSnappyWriterBase::operator=(
    HadoopSnappyWriterBase&& that) noexcept {
  PushableWriter::operator=(static_cast<PushableWriter&&>(that));
  compression_level_ = that.compression_level_;
  size_hint_ = that.size_hint_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  uncompressed_ = std::move(that.uncompressed_);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void HadoopSnappyWriterBase::Reset(Closed) {
  PushableWriter::Reset(kClosed);
  compression_level_ = Options::kDefaultCompressionLevel;
  size_hint_ = absl::nullopt;
  initial_compressed_pos_ = 0;
  uncompressed_ = Buffer();
  associated_reader_.Reset();
}

inline void HadoopSnappyWriterBase::Reset() {
  PushableWriter::Reset();
  compression_level_ = Options::kDefaultCompressionLevel;
  size_hint_ = absl::nullopt;
  initial_compressed_pos_ = 0;
  associated_reader_.Reset();
}

template <typename Dest>
inline HadoopSnappyWriter<Dest>::HadoopSnappyWriter(Initializer<Dest> dest,
                                                    Options options)
    : dest_(std::move(dest)) {
  Initialize(dest_.get(), options.compression_level());
}

template <typename Dest>
inline void HadoopSnappyWriter<Dest>::Reset(Closed) {
  HadoopSnappyWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void HadoopSnappyWriter<Dest>::Reset(Initializer<Dest> dest,
                                            Options options) {
  HadoopSnappyWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.compression_level());
}

template <typename Dest>
void HadoopSnappyWriter<Dest>::Done() {
  HadoopSnappyWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
bool HadoopSnappyWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!HadoopSnappyWriterBase::FlushImpl(flush_type))) {
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

#endif  // RIEGELI_SNAPPY_HADOOP_HADOOP_SNAPPY_WRITER_H_
