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

#include <stddef.h>
#include <stdint.h>

#include <optional>

#include "absl/base/optimization.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/varint/varint_reading.h"

namespace riegeli::chunk_encoding_internal {

std::optional<uint64_t> UncompressedSize(const Chain& compressed_data,
                                         CompressionType compression_type) {
  if (compression_type == CompressionType::kNone) return compressed_data.size();
  ChainReader<> compressed_data_reader(&compressed_data);
  uint64_t size;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(compressed_data_reader, size))) {
    return std::nullopt;
  }
  return size;
}

}  // namespace riegeli::chunk_encoding_internal
