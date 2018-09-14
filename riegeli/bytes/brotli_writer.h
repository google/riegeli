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

#ifndef RIEGELI_BYTES_BROTLI_WRITER_H_
#define RIEGELI_BYTES_BROTLI_WRITER_H_

#include <stddef.h>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "brotli/encode.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter invariant part of BrotliWriter.
class BrotliWriterBase : public BufferedWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // compression_level must be between kMinCompressionLevel() (0) and
    // kMaxCompressionLevel() (11). Default: kDefaultCompressionLevel() (9).
    static constexpr int kMinCompressionLevel() { return BROTLI_MIN_QUALITY; }
    static constexpr int kMaxCompressionLevel() { return BROTLI_MAX_QUALITY; }
    static constexpr int kDefaultCompressionLevel() { return 9; }
    Options& set_compression_level(int compression_level) & {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel())
          << "Failed precondition of "
             "BrotliWriterBase::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel())
          << "Failed precondition of "
             "BrotliWriterBase::Options::set_compression_level(): "
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
    // window_log must be between kMinWindowLog() (10) and kMaxWindowLog() (30).
    // Default: kDefaultWindowLog() (22).
    static constexpr int kMinWindowLog() { return BROTLI_MIN_WINDOW_BITS; }
    static constexpr int kMaxWindowLog() {
      return BROTLI_LARGE_MAX_WINDOW_BITS;
    }
    static constexpr int kDefaultWindowLog() { return BROTLI_DEFAULT_WINDOW; }
    Options& set_window_log(int window_log) & {
      RIEGELI_ASSERT_GE(window_log, kMinWindowLog())
          << "Failed precondition of "
             "BrotliWriterBase::Options::set_window_log(): "
             "window log out of range";
      RIEGELI_ASSERT_LE(window_log, kMaxWindowLog())
          << "Failed precondition of "
             "BrotliWriterBase::Options::set_window_log(): "
             "window log out of range";
      window_log_ = window_log;
      return *this;
    }
    Options&& set_window_log(int window_log) && {
      return std::move(set_window_log(window_log));
    }

    // Announce in advance the destination size. This may improve compression
    // density.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    Options& set_size_hint(Position size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(Position size_hint) && {
      return std::move(set_size_hint(size_hint));
    }

    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of "
             "BrotliWriterBase::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

   private:
    template <typename Dest>
    friend class BrotliWriter;

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
  BrotliWriterBase() noexcept {}

  explicit BrotliWriterBase(size_t buffer_size) noexcept
      : BufferedWriter(buffer_size) {}

  BrotliWriterBase(BrotliWriterBase&& that) noexcept;
  BrotliWriterBase& operator=(BrotliWriterBase&& that) noexcept;

  void Initialize(int compression_level, int window_log, Position size_hint);
  void Done() override;
  bool WriteInternal(absl::string_view src) override;

 private:
  struct BrotliEncoderStateDeleter {
    void operator()(BrotliEncoderState* ptr) const {
      BrotliEncoderDestroyInstance(ptr);
    }
  };

  bool WriteInternal(absl::string_view src, Writer* dest,
                     BrotliEncoderOperation op);

  std::unique_ptr<BrotliEncoderState, BrotliEncoderStateDeleter> compressor_;
};

// A Writer which compresses data with Brotli before passing it to another
// Writer.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the compressed Writer. Dest must support
// Dependency<Writer*, Dest>, e.g. Writer* (not owned, default),
// unique_ptr<Writer> (owned), ChainWriter<> (owned).
//
// The compressed Writer must not be accessed until the BrotliWriter is closed
// or no longer used, except that it is allowed to read the destination of the
// compressed Writer immediately after Flush().
template <typename Dest = Writer*>
class BrotliWriter : public BrotliWriterBase {
 public:
  // Creates a closed BrotliWriter.
  BrotliWriter() noexcept {}

  // Will write to the compressed Writer provided by dest.
  explicit BrotliWriter(Dest dest, Options options = Options());

  BrotliWriter(BrotliWriter&& that) noexcept;
  BrotliWriter& operator=(BrotliWriter&& that) noexcept;

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

inline BrotliWriterBase::BrotliWriterBase(BrotliWriterBase&& that) noexcept
    : BufferedWriter(std::move(that)),
      compressor_(std::move(that.compressor_)) {}

inline BrotliWriterBase& BrotliWriterBase::operator=(
    BrotliWriterBase&& that) noexcept {
  BufferedWriter::operator=(std::move(that));
  compressor_ = std::move(that.compressor_);
  return *this;
}

template <typename Dest>
BrotliWriter<Dest>::BrotliWriter(Dest dest, Options options)
    : BrotliWriterBase(options.buffer_size_), dest_(std::move(dest)) {
  RIEGELI_ASSERT(dest_.ptr() != nullptr)
      << "Failed precondition of BrotliWriter<Dest>::BrotliWriter(Dest): "
         "null Writer pointer";
  Initialize(options.compression_level_, options.window_log_,
             options.size_hint_);
}

template <typename Dest>
inline BrotliWriter<Dest>::BrotliWriter(BrotliWriter&& that) noexcept
    : BrotliWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
inline BrotliWriter<Dest>& BrotliWriter<Dest>::operator=(
    BrotliWriter&& that) noexcept {
  BrotliWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
void BrotliWriter<Dest>::Done() {
  BrotliWriterBase::Done();
  if (dest_.kIsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

extern template class BrotliWriter<Writer*>;
extern template class BrotliWriter<std::unique_ptr<Writer>>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BROTLI_WRITER_H_
