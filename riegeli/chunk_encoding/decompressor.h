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

#ifndef RIEGELI_CHUNK_ENCODING_DECOMPRESSOR_H_
#define RIEGELI_CHUNK_ENCODING_DECOMPRESSOR_H_

#include <stdint.h>
#include <memory>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"

namespace riegeli {
namespace internal {

class Decompressor final : public Object {
 public:
  // Sets *uncompressed_size to uncompressed size of compressed_data.
  //
  // If compression_type is kNone, uncompressed size is the same as compressed
  // size, otherwise reads uncompressed size as a varint from the beginning of
  // compressed_data.
  //
  // Return values:
  //  * true  - success
  //  * false - failure
  static bool UncompressedSize(const Chain& compressed_data,
                               CompressionType compression_type,
                               uint64_t* uncompressed_size);

  // Creates a closed Decompressor.
  Decompressor() noexcept : Object(State::kClosed) {}

  // Will read compressed stream from the byte Reader which is owned by this
  // Decompressor and will be closed and deleted when the Decompressor is
  // closed.
  //
  // If compression_type is not kNone, reads uncompressed size as a varint from
  // the beginning of compressed data.
  Decompressor(std::unique_ptr<Reader> src, CompressionType compression_type);

  // Will read compressed stream from the byte Reader which is not owned by this
  // Decompressor and must be kept alive but not accessed until closing the
  // Decompressor.
  //
  // If compression_type is not kNone, reads uncompressed size as a varint from
  // the beginning of compressed data.
  Decompressor(Reader* src, CompressionType compression_type);

  Decompressor(Decompressor&& src) noexcept;
  Decompressor& operator=(Decompressor&& src) noexcept;

  // Returns the Reader from which uncompressed data should be read.
  //
  // Precondition: healthy()
  Reader* reader() const;

  // Verifies that the source ends at the current position (i.e. has no more
  // compressed data and has no data after the compressed stream), failing the
  // Decompressor if not. Closes the Decompressor.
  //
  // Return values:
  //  * true  - success (the source ends at the former current position)
  //  * false - failure (the source does not end at the former current position
  //                     or the Decompressor was not healthy before closing)
  bool VerifyEndAndClose();

 protected:
  void Done() override;

 private:
  std::unique_ptr<Reader> owned_src_;
  std::unique_ptr<Reader> owned_reader_;
  Reader* reader_ = nullptr;
};

// Implementation details follow.

inline Decompressor::Decompressor(Decompressor&& src) noexcept
    : Object(std::move(src)),
      owned_src_(std::move(src.owned_src_)),
      owned_reader_(std::move(src.owned_reader_)),
      reader_(riegeli::exchange(src.reader_, nullptr)) {}

inline Decompressor& Decompressor::operator=(Decompressor&& src) noexcept {
  Object::operator=(std::move(src));
  owned_src_ = std::move(src.owned_src_);
  owned_reader_ = std::move(src.owned_reader_);
  reader_ = riegeli::exchange(src.reader_, nullptr);
  return *this;
}

inline Reader* Decompressor::reader() const {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of Decompressor::reader(): " << message();
  return reader_;
}

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_DECOMPRESSOR_H_
