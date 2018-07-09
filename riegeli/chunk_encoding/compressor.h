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

#ifndef RIEGELI_CHUNK_ENCODING_COMPRESSOR_H_
#define RIEGELI_CHUNK_ENCODING_COMPRESSOR_H_

#include <stdint.h>

#include "absl/types/variant.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/brotli_writer.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/zstd_writer.h"
#include "riegeli/chunk_encoding/compressor_options.h"

namespace riegeli {
namespace internal {

class Compressor : public Object {
 public:
  // Creates a closed Compressor.
  Compressor() noexcept : Object(State::kClosed) {}

  // Creates an empty Compressor.
  Compressor(CompressorOptions options, uint64_t size_hint = 0);

  Compressor(const Compressor&) = delete;
  Compressor& operator=(const Compressor&) = delete;

  // Resets the Compressor back to empty.
  void Reset();

  // Returns the Writer to which uncompressed data should be written.
  //
  // Precondition: healthy()
  Writer* writer();

  // Writes compressed data to *dest. Closes the Compressor.
  //
  // If options.compression_type() is not kNone, writes uncompressed size as a
  // varint before the data.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  bool EncodeAndClose(Writer* dest);

 protected:
  void Done() override;

 private:
  ChainWriter::Options GetChainWriterOptions() const;
  BrotliWriter::Options GetBrotliWriterOptions() const;
  ZstdWriter::Options GetZstdWriterOptions() const;

  CompressorOptions options_;
  uint64_t size_hint_ = 0;
  ChainWriter compressed_writer_;
  // Invariants:
  //   options_.compression_type() is consistent with the active member of the
  //       variant
  //   if options_.compression_type() == CompressionType::kNone then
  //       writer_ holds &compressed_writer_
  absl::variant<ChainWriter*, BrotliWriter, ZstdWriter> writer_;
};

// Implementation details follow.

inline Writer* Compressor::writer() {
  struct Visitor {
    Writer* operator()(ChainWriter* writer) const { return writer; }
    Writer* operator()(BrotliWriter& writer) const { return &writer; }
    Writer* operator()(ZstdWriter& writer) const { return &writer; }
  };
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of Compressor::writer(): " << message();
  return absl::visit(Visitor(), writer_);
}

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_COMPRESSOR_H_
