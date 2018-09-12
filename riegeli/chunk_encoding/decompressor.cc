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

#include "riegeli/chunk_encoding/decompressor.h"

#include <stdint.h>

#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/chunk_encoding/constants.h"

namespace riegeli {
namespace internal {

bool UncompressedSize(const Chain& compressed_data,
                      CompressionType compression_type,
                      uint64_t* uncompressed_size) {
  if (compression_type == CompressionType::kNone) {
    *uncompressed_size = compressed_data.size();
    return true;
  }
  ChainReader<> compressed_data_reader(&compressed_data);
  return ReadVarint64(&compressed_data_reader, uncompressed_size);
}

template class Decompressor<Reader*>;
template class Decompressor<std::unique_ptr<Reader>>;

}  // namespace internal
}  // namespace riegeli
