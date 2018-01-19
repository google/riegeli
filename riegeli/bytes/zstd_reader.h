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

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zstd.h"

namespace riegeli {

// ZstdReader::Options.
class ZstdReaderOptions {
 public:
  ZstdReaderOptions& set_buffer_size(size_t buffer_size) & {
    RIEGELI_ASSERT_GT(buffer_size, 0u);
    buffer_size_ = buffer_size;
    return *this;
  }
  ZstdReaderOptions&& set_buffer_size(size_t buffer_size) && {
    return std::move(set_buffer_size(buffer_size));
  }

 private:
  friend class ZstdReader;

  size_t buffer_size_ = kDefaultBufferSize();
};

// A Reader which decompresses data with Zstd after getting it from another
// Reader.
class ZstdReader final : public BufferedReader {
 public:
  using Options = ZstdReaderOptions;

  // Creates a closed ZstdReader.
  ZstdReader();

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

  ~ZstdReader();

 protected:
  void Done() override;
  bool PullSlow() override;
  bool ReadInternal(char* dest, size_t min_length, size_t max_length) override;
  bool HopeForMoreSlow() const override;

 private:
  struct ZSTD_DStreamDeleter {
    void operator()(ZSTD_DStream* ptr) const;
  };

  std::unique_ptr<Reader> owned_src_;
  // Invariant: if healthy() then src_ != nullptr
  Reader* src_;
  // If healthy() but decompressor_ == nullptr then all data have been
  // decompressed. In this case ZSTD_decompressStream() must not be called
  // again.
  std::unique_ptr<ZSTD_DStream, ZSTD_DStreamDeleter> decompressor_;
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ZSTD_READER_H_
