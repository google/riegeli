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

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/types/variant.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/brotli_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/bytes/zstd_reader.h"
#include "riegeli/chunk_encoding/constants.h"

namespace riegeli {
namespace internal {

// Sets *uncompressed_size to uncompressed size of compressed_data.
//
// If compression_type is kNone, uncompressed size is the same as compressed
// size, otherwise reads uncompressed size as a varint from the beginning of
// compressed_data.
//
// Return values:
//  * true  - success
//  * false - failure
bool UncompressedSize(const Chain& compressed_data,
                      CompressionType compression_type,
                      uint64_t* uncompressed_size);

template <typename Src = Reader*>
class Decompressor : public Object {
 public:
  // Creates a closed Decompressor.
  Decompressor() noexcept : Object(State::kClosed) {}

  // Will read from the compressed stream provided by src.
  //
  // If compression_type is not kNone, reads uncompressed size as a varint from
  // the beginning of compressed data.
  explicit Decompressor(Src src, CompressionType compression_type);

  Decompressor(Decompressor&& that) noexcept;
  Decompressor& operator=(Decompressor&& that) noexcept;

  // Returns the Reader from which uncompressed data should be read.
  //
  // Precondition: healthy()
  Reader* reader();

  // Verifies that the source ends at the current position (i.e. has no more
  // compressed data and has no data after the compressed stream), failing the
  // Decompressor if not. Closes the Decompressor.
  //
  // Return values:
  //  * true  - success (the source ends at the former current position)
  //  * false - failure (the source does not end at the former current position
  //                     or the Decompressor was not healthy before closing)
  bool VerifyEndAndClose();

  // Verifies that the source ends at the current position (i.e. has no more
  // compressed data and has no data after the compressed stream), failing the
  // Decompressor if not.
  void VerifyEnd();

 protected:
  void Done() override;

 private:
  absl::variant<Dependency<Reader*, Src>, BrotliReader<Src>, ZstdReader<Src>>
      reader_;
};

// Implementation details follow.

template <typename Src>
Decompressor<Src>::Decompressor(Src src, CompressionType compression_type)
    : Object(State::kOpen) {
  Dependency<Reader*, Src> compressed_reader(std::move(src));
  if (compression_type == CompressionType::kNone) {
    reader_ = std::move(compressed_reader);
    return;
  }
  uint64_t decompressed_size;
  if (ABSL_PREDICT_FALSE(
          !ReadVarint64(compressed_reader.ptr(), &decompressed_size))) {
    Fail("Reading decompressed size failed");
    return;
  }
  switch (compression_type) {
    case CompressionType::kNone:
      RIEGELI_ASSERT_UNREACHABLE() << "kNone handled above";
    case CompressionType::kBrotli:
      reader_ = BrotliReader<Src>(std::move(compressed_reader.manager()));
      return;
    case CompressionType::kZstd:
      reader_ = ZstdReader<Src>(std::move(compressed_reader.manager()));
      return;
  }
  Fail(absl::StrCat("Unknown compression type: ",
                    static_cast<unsigned>(compression_type)));
}

template <typename Src>
inline Decompressor<Src>::Decompressor(Decompressor&& that) noexcept
    : Object(std::move(that)), reader_(std::move(that.reader_)) {}

template <typename Src>
inline Decompressor<Src>& Decompressor<Src>::operator=(
    Decompressor&& that) noexcept {
  Object::operator=(std::move(that));
  reader_ = std::move(that.reader_);
  return *this;
}

template <typename Src>
inline Reader* Decompressor<Src>::reader() {
  struct Visitor {
    Reader* operator()(Dependency<Reader*, Src>& reader) const {
      return reader.ptr();
    }
    Reader* operator()(Reader& reader) const { return &reader; }
  };
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of Decompressor::reader(): " << message();
  return absl::visit(Visitor(), reader_);
}

template <typename Src>
void Decompressor<Src>::Done() {
  struct Visitor {
    void operator()(Dependency<Reader*, Src>& reader) const {
      if (reader.is_owning()) {
        if (ABSL_PREDICT_FALSE(!reader->Close())) self->Fail(*reader);
      }
    }
    void operator()(Reader& reader) const {
      if (ABSL_PREDICT_FALSE(!reader.Close())) self->Fail(reader);
    }
    Decompressor* self;
  };
  absl::visit(Visitor{this}, reader_);
}

template <typename Src>
inline bool Decompressor<Src>::VerifyEndAndClose() {
  VerifyEnd();
  return Close();
}

template <typename Src>
inline void Decompressor<Src>::VerifyEnd() {
  struct Visitor {
    void operator()(Dependency<Reader*, Src>& reader) const {
      if (reader.is_owning()) reader->VerifyEnd();
    }
    void operator()(Reader& reader) const { reader.VerifyEnd(); }
  };
  if (ABSL_PREDICT_TRUE(healthy())) absl::visit(Visitor(), reader_);
}

extern template class Decompressor<Reader*>;
extern template class Decompressor<std::unique_ptr<Reader>>;

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_DECOMPRESSOR_H_
