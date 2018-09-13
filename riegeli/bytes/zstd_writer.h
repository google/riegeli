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

#ifndef RIEGELI_BYTES_ZSTD_WRITER_H_
#define RIEGELI_BYTES_ZSTD_WRITER_H_

#include <stddef.h>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "zstd.h"

namespace riegeli {

// Template parameter invariant part of ZstdWriter.
class ZstdWriterBase : public BufferedWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // compression_level must be between kMinCompressionLevel() (-32) and
    // kMaxCompressionLevel() (22). Level 0 is currently equivalent to 3.
    // Default: kDefaultCompressionLevel() (9).
    static int kMinCompressionLevel() { return -32; }
    static int kMaxCompressionLevel() { return ZSTD_maxCLevel(); }
    static constexpr int kDefaultCompressionLevel() { return 9; }
    Options& set_compression_level(int compression_level) & {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel())
          << "Failed precondition of "
             "ZstdWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel())
          << "Failed precondition of "
             "ZstdWriterBase::Options::set_compression_level()"
             "compression level out of range";
      compression_level_ = compression_level;
      return *this;
    }
    Options&& set_compression_level(int level) && {
      return std::move(set_compression_level(level));
    }

    // Logarithm of the LZ77 sliding window size. This tunes the tradeoff
    // between compression density and memory usage (higher = better density but
    // more memory).
    //
    // Special value kDefaultWindowLog() (-1) means to derive window_log from
    // compression_level and size_hint.
    //
    // window_log must be kDefaultWindowLog() (-1) or between kMinWindowLog()
    // (10) and kMaxWindowLog() (30 in 32-bit build, 31 in 64-bit build).
    // Default: kDefaultWindowLog() (-1).
    static int kMinWindowLog();
    static int kMaxWindowLog();
    static constexpr int kDefaultWindowLog() { return -1; }
    Options& set_window_log(int window_log) & {
      if (window_log != kDefaultWindowLog()) {
        RIEGELI_ASSERT_GE(window_log, kMinWindowLog())
            << "Failed precondition of "
               "ZstdWriterBase::Options::set_window_log(): "
               "window log out of range";
        RIEGELI_ASSERT_LE(window_log, kMaxWindowLog())
            << "Failed precondition of "
               "ZstdWriterBase::Options::set_window_log(): "
               "window log out of range";
      }
      window_log_ = window_log;
      return *this;
    }
    Options&& set_window_log(int window_log) && {
      return std::move(set_window_log(window_log));
    }

    // Announce in advance the destination size. This may improve compression
    // density, and this causes the size to be stored in the compressed stream
    // header.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    Options& set_size_hint(Position size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(Position size_hint) && {
      return std::move(set_size_hint(size_hint));
    }

    static size_t kDefaultBufferSize() { return ZSTD_CStreamInSize(); }
    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "ZstdWriterBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

   private:
    template <typename Dest>
    friend class ZstdWriter;

    int compression_level_ = kDefaultCompressionLevel();
    int window_log_ = kDefaultWindowLog();
    Position size_hint_ = 0;
    size_t buffer_size_ = kDefaultBufferSize();
  };

  // Returns the compressed Writer. Unchanged by Close().
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  bool Flush(FlushType flush_type) override;

 protected:
  ZstdWriterBase() noexcept {}

  ZstdWriterBase(int compression_level, int window_log, Position size_hint,
                 size_t buffer_size) noexcept
      : BufferedWriter(buffer_size),
        compression_level_(compression_level),
        window_log_(window_log),
        size_hint_(size_hint) {}

  ZstdWriterBase(ZstdWriterBase&& that) noexcept;
  ZstdWriterBase& operator=(ZstdWriterBase&& that) noexcept;

  void Done() override;
  bool WriteInternal(absl::string_view src) override;

 private:
  struct ZSTD_CStreamDeleter {
    void operator()(ZSTD_CStream* ptr) const { ZSTD_freeCStream(ptr); }
  };

  bool EnsureCStreamCreated();
  bool InitializeCStream();

  template <typename Function>
  bool FlushInternal(Function function, absl::string_view function_name,
                     Writer* dest);

  int compression_level_ = 0;
  int window_log_ = 0;
  Position size_hint_ = 0;
  // If healthy() but compressor_ == nullptr then compressor_ was not created
  // yet.
  std::unique_ptr<ZSTD_CStream, ZSTD_CStreamDeleter> compressor_;
};

// A Writer which compresses data with Zstd before passing it to another Writer.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the compressed Writer. Dest must support
// Dependency<Writer*, Dest>, e.g. Writer* (not owned, default),
// unique_ptr<Writer> (owned), ChainWriter<> (owned).
//
// The compressed Writer must not be accessed until the ZstdWriter is closed or
// no longer used, except that it is allowed to read the destination of the
// compressed Writer immediately after Flush().
template <typename Dest = Writer*>
class ZstdWriter : public ZstdWriterBase {
 public:
  // Creates a closed ZstdWriter.
  ZstdWriter() noexcept {}

  // Will write to the compressed Writer provided by dest.
  explicit ZstdWriter(Dest dest, Options options = Options());

  ZstdWriter(ZstdWriter&& that) noexcept;
  ZstdWriter& operator=(ZstdWriter&& that) noexcept;

  // Returns the object providing and possibly owning the compressed Writer.
  // Unchanged by Close().
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.ptr(); }
  const Writer* dest_writer() const override { return dest_.ptr(); }

  void Done() override;

 private:
  // The object providing and possibly owning the compressed Writer.
  Dependency<Writer*, Dest> dest_;
};

// Implementation details follow.

inline ZstdWriterBase::ZstdWriterBase(ZstdWriterBase&& that) noexcept
    : BufferedWriter(std::move(that)),
      compression_level_(riegeli::exchange(that.compression_level_, 0)),
      window_log_(riegeli::exchange(that.window_log_, 0)),
      size_hint_(riegeli::exchange(that.size_hint_, 0)),
      compressor_(std::move(that.compressor_)) {}

template <typename Dest>
ZstdWriter<Dest>::ZstdWriter(Dest dest, Options options)
    : ZstdWriterBase(options.compression_level_, options.window_log_,
                     options.size_hint_, options.buffer_size_),
      dest_(std::move(dest)) {
  RIEGELI_ASSERT(dest_.ptr() != nullptr)
      << "Failed precondition of ZstdWriter<Dest>::ZstdWriter(Dest): "
         "null Writer pointer";
}

template <typename Dest>
ZstdWriter<Dest>::ZstdWriter(ZstdWriter&& that) noexcept
    : ZstdWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
ZstdWriter<Dest>& ZstdWriter<Dest>::operator=(ZstdWriter&& that) noexcept {
  ZstdWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
void ZstdWriter<Dest>::Done() {
  ZstdWriterBase::Done();
  if (dest_.kIsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

extern template class ZstdWriter<Writer*>;
extern template class ZstdWriter<std::unique_ptr<Writer>>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ZSTD_WRITER_H_
