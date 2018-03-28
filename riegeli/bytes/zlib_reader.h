// Copyright 2018 Google LLC
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

#ifndef RIEGELI_BYTES_ZLIB_READER_H_
#define RIEGELI_BYTES_ZLIB_READER_H_

#include <stddef.h>
#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zlib.h"

namespace riegeli {

class ZLibReader : public BufferedReader {
 public:
  class Options {
   public:
    // Not defaulted because of a C++ defect:
    // https://stackoverflow.com/questions/17430377
    Options() noexcept {}

    // Parameter interpreted by inflateInit2() which specifies the acceptable
    // base two logarithm of the maximum window size, and which kinds of a
    // header are expected.
    //
    // A negative window_bits means no header, with the negated window_bits
    // specifying the actual number of bits in range 8..15.
    //
    // Otherwise 0 means that any number of bits is acceptable (up to 15),
    // a value in range 8..15 means that up to this number of window bits is
    // acceptable, and further window_bits can be incremented to specify which
    // kinds of a header are expected:
    //  * bits + 0  - zlib header
    //  * bits + 16 - gzip header
    //  * bits + 32 - zlib or gzip header
    Options& set_window_bits(int window_bits) & {
      window_bits_ = window_bits;
      return *this;
    }
    Options&& set_window_bits(int window_bits) && {
      return std::move(set_window_bits(window_bits));
    }

    Options& set_buffer_size(size_t buffer_size) & {
      RIEGELI_ASSERT_GT(buffer_size, 0u)
          << "Failed precondition of ZLibReader::Options::set_buffer_size(): "
             "zero buffer size";
      buffer_size_ = buffer_size;
      return *this;
    }
    Options&& set_buffer_size(size_t buffer_size) && {
      return std::move(set_buffer_size(buffer_size));
    }

   private:
    friend class ZLibReader;

    int window_bits_ = 32;
    size_t buffer_size_ = kDefaultBufferSize();
  };

  // Creates a closed ZLibReader.
  ZLibReader() noexcept {}

  // Will read zlib-compressed stream from the byte Reader which is owned by
  // this ZLibReader and will be closed and deleted when the ZLibReader is
  // closed.
  explicit ZLibReader(std::unique_ptr<Reader> src, Options options = Options());

  // Will read zlib-compressed stream from the byte Reader which is not owned by
  // this ZLibReader and must be kept alive but not accessed until closing the
  // ZLibReader.
  explicit ZLibReader(Reader* src, Options options = Options());

  ZLibReader(ZLibReader&&) noexcept;
  ZLibReader& operator=(ZLibReader&&) noexcept;

  ~ZLibReader();

 protected:
  void Done() override;
  bool PullSlow() override;
  bool ReadInternal(char* dest, size_t min_length, size_t max_length) override;
  bool HopeForMoreSlow() const override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailOperation(absl::string_view operation);

  std::unique_ptr<Reader> owned_src_;
  // Invariant: if healthy() then src_ != nullptr
  Reader* src_ = nullptr;
  bool decompressor_present_ = false;
  z_stream decompressor_;
};

// Implementation details follow.

inline ZLibReader::ZLibReader(std::unique_ptr<Reader> src, Options options)
    : ZLibReader(src.get(), options) {
  owned_src_ = std::move(src);
}

inline ZLibReader::ZLibReader(ZLibReader&& src) noexcept
    : BufferedReader(std::move(src)),
      owned_src_(std::move(src.owned_src_)),
      src_(riegeli::exchange(src.src_, nullptr)),
      decompressor_present_(
          riegeli::exchange(src.decompressor_present_, false)),
      decompressor_(src.decompressor_) {}

inline ZLibReader& ZLibReader::operator=(ZLibReader&& src) noexcept {
  // Exchange decompressor_present_ early to support self-assignment.
  const bool decompressor_present =
      riegeli::exchange(src.decompressor_present_, false);
  if (decompressor_present_) {
    const int result = inflateEnd(&decompressor_);
    RIEGELI_ASSERT_EQ(result, Z_OK) << "inflateEnd() failed";
  }
  BufferedReader::operator=(std::move(src));
  owned_src_ = std::move(src.owned_src_);
  src_ = riegeli::exchange(src.src_, nullptr);
  decompressor_present_ = decompressor_present;
  decompressor_ = src.decompressor_;
  return *this;
}

inline ZLibReader::~ZLibReader() {
  if (decompressor_present_) {
    const int result = inflateEnd(&decompressor_);
    RIEGELI_ASSERT_EQ(result, Z_OK) << "inflateEnd() failed";
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_ZLIB_READER_H_
