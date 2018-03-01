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
#include <memory>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {
namespace internal {

class Compressor final : public Object {
 public:
  // Creates a closed Compressor.
  Compressor() noexcept : Object(State::kClosed) {}

  // Creates an empty Compressor.
  Compressor(CompressionType compression_type, int compression_level,
             uint64_t size_hint = 0);

  Compressor(const Compressor&) = delete;
  Compressor& operator=(const Compressor&) = delete;

  // Resets the Compressor back to empty.
  void Reset();

  // Returns the Writer to which uncompressed data should be written.
  //
  // Precondition: healthy()
  Writer* writer() const;

  // Writes compressed data to *dest. Closes the Compressor.
  //
  // If compression_type is not kNone, writes uncompressed size as a varint
  // before the data.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  bool EncodeAndClose(Writer* dest);

 protected:
  void Done() override;

 private:
  CompressionType compression_type_ = CompressionType::kNone;
  int compression_level_ = 0;
  uint64_t size_hint_ = 0;
  Chain compressed_;
  std::unique_ptr<Writer> writer_;
};

// Implementation details follow.

inline Writer* Compressor::writer() const {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of Compressor::writer(): " << Message();
  return writer_.get();
}

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_COMPRESSOR_H_
