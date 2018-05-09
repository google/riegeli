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

#include "absl/strings/string_view.h"
#include "brotli/encode.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A Writer which compresses data with Brotli before passing it to another
// Writer.
class BrotliWriter final : public BufferedWriter {
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
             "BrotliWriter::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel())
          << "Failed precondition of "
             "BrotliWriter::Options::set_compression_level(): "
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
             "BrotliWriter::Options::set_window_log(): "
             "window log out of range";
      RIEGELI_ASSERT_LE(window_log, kMaxWindowLog())
          << "Failed precondition of "
             "BrotliWriter::Options::set_window_log(): "
             "window log out of range";
      window_log_ = window_log;
      return *this;
    }
    Options&& set_window_log(int window_log) && {
      return std::move(set_window_log(window_log));
    }

    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of BrotliWriter::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
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

   private:
    friend class BrotliWriter;

    int compression_level_ = kDefaultCompressionLevel();
    int window_log_ = kDefaultWindowLog();
    size_t buffer_size_ = kDefaultBufferSize();
    Position size_hint_ = 0;
  };

  // Creates a closed BrotliWriter.
  BrotliWriter() noexcept {}

  // Will write Brotli-compressed stream to the byte Writer which is owned by
  // this BrotliWriter and will be closed and deleted when the BrotliWriter is
  // closed.
  explicit BrotliWriter(std::unique_ptr<Writer> dest,
                        Options options = Options());

  // Will write Brotli-compressed stream to the byte Writer which is not owned
  // by this BrotliWriter and must be kept alive but not accessed until closing
  // the BrotliWriter, except that it is allowed to read its destination
  // directly after Flush().
  explicit BrotliWriter(Writer* dest, Options options = Options());

  BrotliWriter(BrotliWriter&& src) noexcept;
  BrotliWriter& operator=(BrotliWriter&& src) noexcept;

  bool Flush(FlushType flush_type) override;

 protected:
  void Done() override;
  bool WriteInternal(absl::string_view src) override;

 private:
  struct BrotliEncoderStateDeleter {
    void operator()(BrotliEncoderState* ptr) const {
      BrotliEncoderDestroyInstance(ptr);
    }
  };

  bool WriteInternal(absl::string_view src, BrotliEncoderOperation op);

  std::unique_ptr<Writer> owned_dest_;
  // Invariant: if healthy() then dest_ != nullptr
  Writer* dest_ = nullptr;
  std::unique_ptr<BrotliEncoderState, BrotliEncoderStateDeleter> compressor_;
};

// Implementation details follow.

inline BrotliWriter::BrotliWriter(std::unique_ptr<Writer> dest, Options options)
    : BrotliWriter(dest.get(), options) {
  owned_dest_ = std::move(dest);
}

inline BrotliWriter::BrotliWriter(BrotliWriter&& src) noexcept
    : BufferedWriter(std::move(src)),
      owned_dest_(std::move(src.owned_dest_)),
      dest_(riegeli::exchange(src.dest_, nullptr)),
      compressor_(std::move(src.compressor_)) {}

inline BrotliWriter& BrotliWriter::operator=(BrotliWriter&& src) noexcept {
  BufferedWriter::operator=(std::move(src));
  owned_dest_ = std::move(src.owned_dest_);
  dest_ = riegeli::exchange(src.dest_, nullptr);
  compressor_ = std::move(src.compressor_);
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BROTLI_WRITER_H_
