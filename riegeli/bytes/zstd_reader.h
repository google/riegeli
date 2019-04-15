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

#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zstd.h"

namespace riegeli {

// Template parameter invariant part of ZstdReader.
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

    // Tunes how much data is buffered after calling the decompression engine.
    //
    // Default: ZSTD_DStreamOutSize()
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

   private:
    template <typename Src>
    friend class ZstdReader;

    Position size_hint_ = 0;
    size_t buffer_size_ = DefaultBufferSize();
  };

  // Returns the compressed Reader. Unchanged by Close().
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

 protected:
  ZstdReaderBase() noexcept {}

  explicit ZstdReaderBase(size_t buffer_size, Position size_hint) noexcept
      : BufferedReader(buffer_size, size_hint) {}

  ZstdReaderBase(ZstdReaderBase&& that) noexcept;
  ZstdReaderBase& operator=(ZstdReaderBase&& that) noexcept;

  void Initialize(Reader* src);
  void Done() override;
  bool PullSlow() override;
  bool ReadInternal(char* dest, size_t min_length, size_t max_length) override;

 private:
  struct ZSTD_DStreamDeleter {
    void operator()(ZSTD_DStream* ptr) const { ZSTD_freeDStream(ptr); }
  };

  // If true, the source is truncated (without a clean end of the compressed
  // stream) at the current position. If the source does not grow, Close() will
  // fail.
  bool truncated_ = false;
  // If healthy() but decompressor_ == nullptr then all data have been
  // decompressed. In this case ZSTD_decompressStream() must not be called
  // again.
  RecyclingPool<ZSTD_DStream, ZSTD_DStreamDeleter>::Handle decompressor_;
};

// A Reader which decompresses data with Zstd after getting it from another
// Reader.
//
// The Src template parameter specifies the type of the object providing and
// possibly owning the compressed Reader. Src must support
// Dependency<Reader*, Src>, e.g. Reader* (not owned, default),
// unique_ptr<Reader> (owned), ChainReader<> (owned).
//
// The compressed Reader must not be accessed until the ZstdReader is closed or
// no longer used.
template <typename Src = Reader*>
class ZstdReader : public ZstdReaderBase {
 public:
  // Creates a closed ZstdReader.
  ZstdReader() noexcept {}

  // Will read from the compressed Reader provided by src.
  explicit ZstdReader(Src src, Options options = Options());

  ZstdReader(ZstdReader&& that) noexcept;
  ZstdReader& operator=(ZstdReader&& that) noexcept;

  // Returns the object providing and possibly owning the compressed Reader.
  // Unchanged by Close().
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

 protected:
  void Done() override;
  void VerifyEnd() override;

 private:
  // The object providing and possibly owning the compressed Reader.
  Dependency<Reader*, Src> src_;
};

// Implementation details follow.

inline ZstdReaderBase::ZstdReaderBase(ZstdReaderBase&& that) noexcept
    : BufferedReader(std::move(that)),
      truncated_(absl::exchange(that.truncated_, false)),
      decompressor_(std::move(that.decompressor_)) {}

inline ZstdReaderBase& ZstdReaderBase::operator=(
    ZstdReaderBase&& that) noexcept {
  BufferedReader::operator=(std::move(that));
  truncated_ = absl::exchange(that.truncated_, false);
  decompressor_ = std::move(that.decompressor_);
  return *this;
}

template <typename Src>
ZstdReader<Src>::ZstdReader(Src src, Options options)
    : ZstdReaderBase(options.buffer_size_, options.size_hint_),
      src_(std::move(src)) {
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

extern template class ZstdReader<Reader*>;
extern template class ZstdReader<std::unique_ptr<Reader>>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ZSTD_READER_H_
