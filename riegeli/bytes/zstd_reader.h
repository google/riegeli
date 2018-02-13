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

#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zstd.h"

namespace riegeli {

// A Reader which decompresses data with Zstd after getting it from another
// Reader.
class ZstdReader final : public BufferedReader {
 public:
  class Options {
   public:
    // Not defaulted because of a C++ defect:
    // https://stackoverflow.com/questions/17430377
    Options() noexcept {}

    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of ZstdReader::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

   private:
    friend class ZstdReader;

    size_t buffer_size_ = ZSTD_DStreamOutSize();
  };

  // Creates a closed ZstdReader.
  ZstdReader() noexcept {}

  // Will read Zstd-compressed stream from the byte Reader which is owned by
  // this ZstdReader and will be closed and deleted when the ZstdReader is
  // closed.
  explicit ZstdReader(std::unique_ptr<Reader> src, Options options = Options());

  // Will read Zstd-compressed stream from the byte Reader which is not owned by
  // this ZstdReader and must be kept alive but not accessed until closing the
  // ZstdReader.
  explicit ZstdReader(Reader* src, Options options = Options());

  ZstdReader(ZstdReader&& src) noexcept;
  ZstdReader& operator=(ZstdReader&& src) noexcept;

 protected:
  void Done() override;
  bool PullSlow() override;
  bool ReadInternal(char* dest, size_t min_length, size_t max_length) override;
  bool HopeForMoreSlow() const override;

 private:
  struct ZSTD_DStreamDeleter {
    void operator()(ZSTD_DStream* ptr) const { ZSTD_freeDStream(ptr); }
  };

  std::unique_ptr<Reader> owned_src_;
  // Invariant: if healthy() then src_ != nullptr
  Reader* src_ = nullptr;
  // If healthy() but decompressor_ == nullptr then all data have been
  // decompressed. In this case ZSTD_decompressStream() must not be called
  // again.
  std::unique_ptr<ZSTD_DStream, ZSTD_DStreamDeleter> decompressor_;
};

// Implementation details follow.

inline ZstdReader::ZstdReader(std::unique_ptr<Reader> src, Options options)
    : ZstdReader(src.get(), options) {
  owned_src_ = std::move(src);
}

inline ZstdReader::ZstdReader(ZstdReader&& src) noexcept
    : BufferedReader(std::move(src)),
      owned_src_(std::move(src.owned_src_)),
      src_(riegeli::exchange(src.src_, nullptr)),
      decompressor_(std::move(src.decompressor_)) {}

inline ZstdReader& ZstdReader::operator=(ZstdReader&& src) noexcept {
  BufferedReader::operator=(std::move(src));
  owned_src_ = std::move(src.owned_src_);
  src_ = riegeli::exchange(src.src_, nullptr);
  decompressor_ = std::move(src.decompressor_);
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ZSTD_READER_H_
