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

#ifndef RIEGELI_BYTES_ZSTD_READER_H_
#define RIEGELI_BYTES_ZSTD_READER_H_

#include <stddef.h>

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zstd.h"

namespace riegeli {

// Template parameter independent part of `ZstdReader`.
class ZstdReaderBase : public BufferedReader {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Expected uncompressed size, or 0 if unknown. This may improve
    // performance.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    Options& set_size_hint(Position size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(Position size_hint) && {
      return std::move(set_size_hint(size_hint));
    }
    Position size_hint() const { return size_hint_; }

    // Tunes how much data is buffered after calling the decompression engine.
    //
    // Default: `ZSTD_DStreamOutSize()`
    static size_t DefaultBufferSize() { return ZSTD_DStreamOutSize(); }
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "ZstdReaderBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }
    size_t buffer_size() const { return buffer_size_; }

   private:
    Position size_hint_ = 0;
    size_t buffer_size_ = DefaultBufferSize();
  };

  // Returns the compressed `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

 protected:
  ZstdReaderBase() noexcept {}

  explicit ZstdReaderBase(size_t buffer_size, Position size_hint);

  ZstdReaderBase(ZstdReaderBase&& that) noexcept;
  ZstdReaderBase& operator=(ZstdReaderBase&& that) noexcept;

  void Reset();
  void Reset(size_t buffer_size, Position size_hint);
  void Initialize(Reader* src);

  void Done() override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  bool ReadInternal(char* dest, size_t min_length, size_t max_length) override;

 private:
  struct ZSTD_DCtxDeleter {
    void operator()(ZSTD_DCtx* ptr) const { ZSTD_freeDCtx(ptr); }
  };

  // If `true`, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, `Close()`
  // will fail.
  bool truncated_ = false;
  // If `healthy()` but `decompressor_ == nullptr` then all data have been
  // decompressed. In this case `ZSTD_decompressStream()` must not be called
  // again.
  RecyclingPool<ZSTD_DCtx, ZSTD_DCtxDeleter>::Handle decompressor_;
};

// A `Reader` which decompresses data with Zstd after getting it from another
// `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the compressed `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// The compressed `Reader` must not be accessed until the `ZstdReader` is closed
// or no longer used.
template <typename Src = Reader*>
class ZstdReader : public ZstdReaderBase {
 public:
  // Creates a closed `ZstdReader`.
  ZstdReader() noexcept {}

  // Will read from the compressed `Reader` provided by `src`.
  explicit ZstdReader(const Src& src, Options options = Options());
  explicit ZstdReader(Src&& src, Options options = Options());

  // Will read from the compressed `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit ZstdReader(std::tuple<SrcArgs...> src_args,
                      Options options = Options());

  ZstdReader(ZstdReader&& that) noexcept;
  ZstdReader& operator=(ZstdReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ZstdReader`. This avoids
  // constructing a temporary `ZstdReader` and moving from it.
  void Reset();
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

  void VerifyEnd() override;

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the compressed `Reader`.
  Dependency<Reader*, Src> src_;
};

// Implementation details follow.

inline ZstdReaderBase::ZstdReaderBase(size_t buffer_size, Position size_hint)
    : BufferedReader(buffer_size, size_hint) {}

inline ZstdReaderBase::ZstdReaderBase(ZstdReaderBase&& that) noexcept
    : BufferedReader(std::move(that)),
      truncated_(that.truncated_),
      decompressor_(std::move(that.decompressor_)) {}

inline ZstdReaderBase& ZstdReaderBase::operator=(
    ZstdReaderBase&& that) noexcept {
  BufferedReader::operator=(std::move(that));
  truncated_ = that.truncated_;
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

inline void ZstdReaderBase::Reset() {
  BufferedReader::Reset();
  truncated_ = false;
  decompressor_.reset();
}

inline void ZstdReaderBase::Reset(size_t buffer_size, Position size_hint) {
  BufferedReader::Reset(buffer_size, size_hint);
  truncated_ = false;
  decompressor_.reset();
}

template <typename Src>
inline ZstdReader<Src>::ZstdReader(const Src& src, Options options)
    : ZstdReaderBase(options.buffer_size(), options.size_hint()), src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline ZstdReader<Src>::ZstdReader(Src&& src, Options options)
    : ZstdReaderBase(options.buffer_size(), options.size_hint()),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline ZstdReader<Src>::ZstdReader(std::tuple<SrcArgs...> src_args,
                                   Options options)
    : ZstdReaderBase(options.buffer_size(), options.size_hint()),
      src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline ZstdReader<Src>::ZstdReader(ZstdReader&& that) noexcept
    : ZstdReaderBase(std::move(that)), src_(std::move(that.src_)) {}

template <typename Src>
inline ZstdReader<Src>& ZstdReader<Src>::operator=(ZstdReader&& that) noexcept {
  ZstdReaderBase::operator=(std::move(that));
  src_ = std::move(that.src_);
  return *this;
}

template <typename Src>
inline void ZstdReader<Src>::Reset() {
  ZstdReaderBase::Reset();
  src_.Reset();
}

template <typename Src>
inline void ZstdReader<Src>::Reset(const Src& src, Options options) {
  ZstdReaderBase::Reset(options.buffer_size(), options.size_hint());
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void ZstdReader<Src>::Reset(Src&& src, Options options) {
  ZstdReaderBase::Reset(options.buffer_size(), options.size_hint());
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void ZstdReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                   Options options) {
  ZstdReaderBase::Reset(options.buffer_size(), options.size_hint());
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
void ZstdReader<Src>::Done() {
  ZstdReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src>
void ZstdReader<Src>::VerifyEnd() {
  ZstdReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) src_->VerifyEnd();
}

template <typename Src>
struct Resetter<ZstdReader<Src>> : ResetterByReset<ZstdReader<Src>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ZSTD_READER_H_
