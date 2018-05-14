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

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "zstd.h"

namespace riegeli {

// A Writer which compresses data with Zstd before passing it to another Writer.
class ZstdWriter final : public BufferedWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Tunes the tradeoff between compression density and compression speed
    // (higher = better density but slower).
    //
    // compression_level must be between kMinCompressionLevel() (1) and
    // kMaxCompressionLevel() (22). Default: kDefaultCompressionLevel() (9).
    static constexpr int kMinCompressionLevel() { return 1; }
    static int kMaxCompressionLevel() { return ZSTD_maxCLevel(); }
    static constexpr int kDefaultCompressionLevel() { return 9; }
    Options& set_compression_level(int compression_level) & {
      RIEGELI_ASSERT_GE(compression_level, kMinCompressionLevel())
          << "Failed precondition of "
             "ZstdWriter::Options::set_compression_level(): "
             "compression level out of range";
      RIEGELI_ASSERT_LE(compression_level, kMaxCompressionLevel())
          << "Failed precondition of "
             "ZstdWriter::Options::set_compression_level()"
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
            << "Failed precondition of ZstdWriter::Options::set_window_log(): "
               "window log out of range";
        RIEGELI_ASSERT_LE(window_log, kMaxWindowLog())
            << "Failed precondition of ZstdWriter::Options::set_window_log(): "
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
          << "Failed precondition of ZstdWriter::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

   private:
    friend class ZstdWriter;

    int compression_level_ = kDefaultCompressionLevel();
    int window_log_ = kDefaultWindowLog();
    Position size_hint_ = 0;
    size_t buffer_size_ = kDefaultBufferSize();
  };

  // Creates a closed ZstdWriter.
  ZstdWriter() noexcept {}

  // Will write Zstd-compressed stream to the byte Writer which is owned by this
  // ZstdWriter and will be closed and deleted when the ZstdWriter is closed.
  explicit ZstdWriter(std::unique_ptr<Writer> dest,
                      Options options = Options());

  // Will write Zstd-compressed stream to the byte Writer which is not owned by
  // this ZstdWriter and must be kept alive but not accessed until closing the
  // ZstdWriter, except that it is allowed to read its destination directly
  // after Flush().
  explicit ZstdWriter(Writer* dest, Options options = Options());

  ZstdWriter(ZstdWriter&& src) noexcept;
  ZstdWriter& operator=(ZstdWriter&& src) noexcept;

  bool Flush(FlushType flush_type) override;

 protected:
  void Done() override;
  bool WriteInternal(absl::string_view src) override;

 private:
  struct ZSTD_CStreamDeleter {
    void operator()(ZSTD_CStream* ptr) const { ZSTD_freeCStream(ptr); }
  };

  bool EnsureCStreamCreated();
  bool InitializeCStream();

  template <typename Function>
  bool FlushInternal(Function function, absl::string_view function_name);

  std::unique_ptr<Writer> owned_dest_;
  // Invariant: if healthy() then dest_ != nullptr
  Writer* dest_ = nullptr;
  int compression_level_ = 0;
  int window_log_ = 0;
  Position size_hint_ = 0;
  // If healthy() but compressor_ == nullptr then compressor_ was not created
  // yet.
  std::unique_ptr<ZSTD_CStream, ZSTD_CStreamDeleter> compressor_;
};

// Implementation details follow.

inline ZstdWriter::ZstdWriter(std::unique_ptr<Writer> dest, Options options)
    : ZstdWriter(dest.get(), options) {
  owned_dest_ = std::move(dest);
}

inline ZstdWriter::ZstdWriter(Writer* dest, Options options)
    : BufferedWriter(options.buffer_size_),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      compression_level_(options.compression_level_),
      window_log_(options.window_log_),
      size_hint_(options.size_hint_) {}

inline ZstdWriter::ZstdWriter(ZstdWriter&& src) noexcept
    : BufferedWriter(std::move(src)),
      owned_dest_(std::move(src.owned_dest_)),
      dest_(riegeli::exchange(src.dest_, nullptr)),
      compression_level_(riegeli::exchange(src.compression_level_, 0)),
      window_log_(riegeli::exchange(src.window_log_, 0)),
      size_hint_(riegeli::exchange(src.size_hint_, 0)),
      compressor_(std::move(src.compressor_)) {}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ZSTD_WRITER_H_
