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

#ifndef RIEGELI_BZIP2_BZIP2_READER_H_
#define RIEGELI_BZIP2_BZIP2_READER_H_

#include <stddef.h>

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
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Template parameter independent part of `Bzip2Reader`.
class Bzip2ReaderBase : public BufferedReader {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

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

   private:
    bool concatenate_ = false;
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
        << "Failed precondition of Bzip2ReaderBase::truncated(): "
           "Options::concatenate() is true";
    return truncated_ && available() == 0;
  }

  bool ToleratesReadingAhead() override;
  bool SupportsRewind() override;
  bool SupportsNewReader() override;

 protected:
  explicit Bzip2ReaderBase(Closed) noexcept : BufferedReader(kClosed) {}

  explicit Bzip2ReaderBase(BufferOptions buffer_options, bool concatenate);

  Bzip2ReaderBase(Bzip2ReaderBase&& that) noexcept;
  Bzip2ReaderBase& operator=(Bzip2ReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options, bool concatenate);
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
  struct BZStreamDeleter {
    void operator()(bz_stream* ptr) const {
      const int bzlib_code = BZ2_bzDecompressEnd(ptr);
      RIEGELI_ASSERT_EQ(bzlib_code, BZ_OK)
          << "BZ2_bzDecompressEnd() failed: " << bzlib_code;
      delete ptr;
    }
  };

  void InitializeDecompressor();
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation,
                                         int bzlib_code);

  bool concatenate_ = false;
  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // If `true`, some compressed data from the current stream were processed.
  // If `concatenate_` and `!stream_had_data_`, an end of the source is
  // legitimate, it does not imply that the source is truncated.
  bool stream_had_data_ = false;
  Position initial_compressed_pos_ = 0;
  // If `ok()` but `decompressor_ == nullptr` then all data have been
  // decompressed, `exact_size() == limit_pos()`, and `ReadInternal()` must not
  // be called again.
  std::unique_ptr<bz_stream, BZStreamDeleter> decompressor_;
};

// A `Reader` which decompresses data with Bzip2 after getting it from another
// `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `Any<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument. This requires C++17.
//
// The compressed `Reader` must not be accessed until the `Bzip2Reader` is
// closed or no longer used.
template <typename Src = Reader*>
class Bzip2Reader : public Bzip2ReaderBase {
 public:
  // Creates a closed `Bzip2Reader`.
  explicit Bzip2Reader(Closed) noexcept : Bzip2ReaderBase(kClosed) {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit Bzip2Reader(Initializer<Src> src, Options options = Options());

  Bzip2Reader(Bzip2Reader&&) = default;
  Bzip2Reader& operator=(Bzip2Reader&&) = default;

  // Makes `*this` equivalent to a newly constructed `Bzip2Reader`. This avoids
  // constructing a temporary `Bzip2Reader` and moving from it.
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

// Support CTAD.
#if __cpp_deduction_guides
explicit Bzip2Reader(Closed) -> Bzip2Reader<DeleteCtad<Closed>>;
template <typename Src>
explicit Bzip2Reader(
    Src&& src, Bzip2ReaderBase::Options options = Bzip2ReaderBase::Options())
    -> Bzip2Reader<TargetT<Src>>;
#endif

// Returns `true` if the data look like they have been Bzip2-compressed.
//
// The current position of `src` is unchanged.
bool RecognizeBzip2(Reader& src);

// Implementation details follow.

inline Bzip2ReaderBase::Bzip2ReaderBase(BufferOptions buffer_options,
                                        bool concatenate)
    : BufferedReader(buffer_options), concatenate_(concatenate) {}

inline Bzip2ReaderBase::Bzip2ReaderBase(Bzip2ReaderBase&& that) noexcept
    : BufferedReader(static_cast<BufferedReader&&>(that)),
      concatenate_(that.concatenate_),
      truncated_(that.truncated_),
      stream_had_data_(that.stream_had_data_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      decompressor_(std::move(that.decompressor_)) {}

inline Bzip2ReaderBase& Bzip2ReaderBase::operator=(
    Bzip2ReaderBase&& that) noexcept {
  BufferedReader::operator=(static_cast<BufferedReader&&>(that));
  concatenate_ = that.concatenate_;
  truncated_ = that.truncated_;
  stream_had_data_ = that.stream_had_data_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void Bzip2ReaderBase::Reset(Closed) {
  BufferedReader::Reset(kClosed);
  concatenate_ = false;
  truncated_ = false;
  stream_had_data_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
}

inline void Bzip2ReaderBase::Reset(BufferOptions buffer_options,
                                   bool concatenate) {
  BufferedReader::Reset(buffer_options);
  concatenate_ = concatenate;
  truncated_ = false;
  stream_had_data_ = false;
  initial_compressed_pos_ = 0;
  decompressor_.reset();
}

template <typename Src>
inline Bzip2Reader<Src>::Bzip2Reader(Initializer<Src> src, Options options)
    : Bzip2ReaderBase(options.buffer_options(), options.concatenate()),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline void Bzip2Reader<Src>::Reset(Closed) {
  Bzip2ReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void Bzip2Reader<Src>::Reset(Initializer<Src> src, Options options) {
  Bzip2ReaderBase::Reset(options.buffer_options(), options.concatenate());
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
void Bzip2Reader<Src>::Done() {
  Bzip2ReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void Bzip2Reader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  Bzip2ReaderBase::SetReadAllHintImpl(read_all_hint);
  if (src_.IsOwning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void Bzip2Reader<Src>::VerifyEndImpl() {
  Bzip2ReaderBase::VerifyEndImpl();
  if (src_.IsOwning() && ABSL_PREDICT_TRUE(ok())) src_->VerifyEnd();
}

}  // namespace riegeli

#endif  // RIEGELI_BZIP2_BZIP2_READER_H_
