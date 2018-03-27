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

#include "riegeli/chunk_encoding/simple_decoder.h"

#include <stddef.h>
#include <stdint.h>
#include <limits>
#include <memory>

#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/chunk_encoding/decompressor.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

void SimpleDecoder::Done() {
  if (RIEGELI_UNLIKELY(!values_decompressor_.Close())) {
    Fail(values_decompressor_);
  }
}

bool SimpleDecoder::Reset(Reader* src, uint64_t num_records,
                          uint64_t decoded_data_size,
                          std::vector<size_t>* limits) {
  MarkHealthy();
  if (RIEGELI_UNLIKELY(num_records > limits->max_size())) {
    return Fail("Too many records");
  }
  if (RIEGELI_UNLIKELY(decoded_data_size >
                       std::numeric_limits<size_t>::max())) {
    return Fail("Records too large");
  }

  uint8_t compression_type_byte;
  if (RIEGELI_UNLIKELY(!ReadByte(src, &compression_type_byte))) {
    return Fail("Reading compression type failed", *src);
  }
  const CompressionType compression_type =
      static_cast<CompressionType>(compression_type_byte);

  uint64_t sizes_size;
  if (RIEGELI_UNLIKELY(!ReadVarint64(src, &sizes_size))) {
    return Fail("Reading size of sizes failed", *src);
  }

  if (RIEGELI_UNLIKELY(sizes_size >
                       std::numeric_limits<Position>::max() - src->pos())) {
    return Fail("Size of sizes too large");
  }
  LimitingReader compressed_sizes_reader(src, src->pos() + sizes_size);
  internal::Decompressor sizes_decompressor(&compressed_sizes_reader,
                                            compression_type);
  if (RIEGELI_UNLIKELY(!sizes_decompressor.healthy())) {
    compressed_sizes_reader.Close();
    return Fail(sizes_decompressor);
  }
  limits->clear();
  size_t limit = 0;
  while (limits->size() != num_records) {
    uint64_t size;
    if (RIEGELI_UNLIKELY(!ReadVarint64(sizes_decompressor.reader(), &size))) {
      compressed_sizes_reader.Close();
      return Fail("Reading record size failed", *sizes_decompressor.reader());
    }
    if (RIEGELI_UNLIKELY(size > decoded_data_size - limit)) {
      compressed_sizes_reader.Close();
      return Fail("Decoded data size larger than expected");
    }
    limit += IntCast<size_t>(size);
    limits->push_back(limit);
  }
  if (RIEGELI_UNLIKELY(!sizes_decompressor.VerifyEndAndClose())) {
    compressed_sizes_reader.Close();
    return Fail(sizes_decompressor);
  }
  if (RIEGELI_UNLIKELY(!compressed_sizes_reader.VerifyEndAndClose())) {
    return Fail(compressed_sizes_reader);
  }
  if (RIEGELI_UNLIKELY(limit != decoded_data_size)) {
    return Fail("Decoded data size smaller than expected");
  }

  values_decompressor_ = internal::Decompressor(src, compression_type);
  if (RIEGELI_UNLIKELY(!values_decompressor_.healthy())) {
    return Fail(values_decompressor_);
  }
  return true;
}

bool SimpleDecoder::VerifyEndAndClose() {
  if (RIEGELI_UNLIKELY(!values_decompressor_.VerifyEndAndClose())) {
    return Fail(values_decompressor_);
  }
  return Close();
}

}  // namespace riegeli
