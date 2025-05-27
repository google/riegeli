// Copyright 2019 Google LLC
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

#ifndef RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_WRITER_H_
#define RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_WRITER_H_

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
class FramedSnappyReader;
class Reader;

// Template parameter independent part of `FramedSnappyWriter`.
class FramedSnappyWriterBase : public PushableWriter {
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
             "FramedSnappyWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel)
          << "Failed precondition of "
             "FramedSnappyWriterBase::Options::set_compression_level(): "
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
  explicit FramedSnappyWriterBase(Closed) noexcept : PushableWriter(kClosed) {}

  explicit FramedSnappyWriterBase() {}

  FramedSnappyWriterBase(FramedSnappyWriterBase&& that) noexcept;
  FramedSnappyWriterBase& operator=(FramedSnappyWriterBase&& that) noexcept;

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

  AssociatedReader<FramedSnappyReader<Reader*>> associated_reader_;

  // Invariants if scratch is not used:
  //   `start() == nullptr` or `start() == uncompressed_.data()`
  //   `start_to_limit() <= snappy::kBlockSize`
};

// A `Writer` which compresses data with framed Snappy format before passing it
// to another `Writer`:
// https://github.com/google/snappy/blob/master/framing_format.txt
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
// The compressed `Writer` must not be accessed until the `FramedSnappyWriter`
// is closed or no longer used.
template <typename Dest = Writer*>
class FramedSnappyWriter : public FramedSnappyWriterBase {
 public:
  // Creates a closed `FramedSnappyWriter`.
  explicit FramedSnappyWriter(Closed) noexcept
      : FramedSnappyWriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit FramedSnappyWriter(Initializer<Dest> dest,
                              Options options = Options());

  FramedSnappyWriter(FramedSnappyWriter&& that) = default;
  FramedSnappyWriter& operator=(FramedSnappyWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `FramedSnappyWriter`. This
  // avoids constructing a temporary `FramedSnappyWriter` and moving from it.
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

explicit FramedSnappyWriter(Closed) -> FramedSnappyWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit FramedSnappyWriter(
    Dest&& dest,
    FramedSnappyWriterBase::Options options = FramedSnappyWriterBase::Options())
    -> FramedSnappyWriter<TargetT<Dest>>;

// Implementation details follow.

inline FramedSnappyWriterBase::FramedSnappyWriterBase(
    FramedSnappyWriterBase&& that) noexcept
    : PushableWriter(static_cast<PushableWriter&&>(that)),
      compression_level_(that.compression_level_),
      size_hint_(that.size_hint_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      uncompressed_(std::move(that.uncompressed_)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline FramedSnappyWriterBase& FramedSnappyWriterBase::operator=(
    FramedSnappyWriterBase&& that) noexcept {
  PushableWriter::operator=(static_cast<PushableWriter&&>(that));
  compression_level_ = that.compression_level_;
  size_hint_ = that.size_hint_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  uncompressed_ = std::move(that.uncompressed_);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void FramedSnappyWriterBase::Reset(Closed) {
  PushableWriter::Reset(kClosed);
  compression_level_ = Options::kDefaultCompressionLevel;
  size_hint_ = absl::nullopt;
  initial_compressed_pos_ = 0;
  uncompressed_ = Buffer();
  associated_reader_.Reset();
}

inline void FramedSnappyWriterBase::Reset() {
  PushableWriter::Reset();
  compression_level_ = Options::kDefaultCompressionLevel;
  size_hint_ = absl::nullopt;
  initial_compressed_pos_ = 0;
  associated_reader_.Reset();
}

template <typename Dest>
inline FramedSnappyWriter<Dest>::FramedSnappyWriter(Initializer<Dest> dest,
                                                    Options options)
    : dest_(std::move(dest)) {
  Initialize(dest_.get(), options.compression_level());
}

template <typename Dest>
inline void FramedSnappyWriter<Dest>::Reset(Closed) {
  FramedSnappyWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void FramedSnappyWriter<Dest>::Reset(Initializer<Dest> dest,
                                            Options options) {
  FramedSnappyWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.compression_level());
}

template <typename Dest>
void FramedSnappyWriter<Dest>::Done() {
  FramedSnappyWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
bool FramedSnappyWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!FramedSnappyWriterBase::FlushImpl(flush_type))) {
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

#endif  // RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_WRITER_H_
