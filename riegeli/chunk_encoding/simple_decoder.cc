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
#include "riegeli/base/str_cat.h"
#include "riegeli/bytes/brotli_reader.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"
#include "riegeli/bytes/zstd_reader.h"
#include "riegeli/chunk_encoding/types.h"

namespace riegeli {

inline bool SimpleDecoder::Decompressor::Reset(
    Reader* src, CompressionType compression_type) {
  RIEGELI_ASSERT_NOTNULL(src);
  MarkHealthy();
  switch (compression_type) {
    case CompressionType::kNone:
      reader_ = src;
      return true;
    case CompressionType::kBrotli:
      owned_reader_ = riegeli::make_unique<BrotliReader>(src);
      reader_ = owned_reader_.get();
      return true;
    case CompressionType::kZstd:
      owned_reader_ = riegeli::make_unique<ZstdReader>(src);
      reader_ = owned_reader_.get();
      return true;
  }
  return Fail(StrCat("Unknown compression type: ",
                     static_cast<unsigned>(compression_type)));
}

void SimpleDecoder::Decompressor::Done() {
  owned_reader_.reset();
  reader_ = nullptr;
}

inline bool SimpleDecoder::Decompressor::VerifyEndAndClose() {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (owned_reader_ != nullptr) {
    if (RIEGELI_UNLIKELY(!owned_reader_->VerifyEndAndClose())) {
      return Fail(*owned_reader_);
    }
  }
  return Close();
}

void SimpleDecoder::Done() {
  if (RIEGELI_UNLIKELY(!values_decompressor_.Close())) {
    Fail(values_decompressor_);
  }
}

bool SimpleDecoder::Reset(Reader* src, uint64_t num_records,
                          uint64_t decoded_data_size,
                          std::vector<size_t>* boundaries) {
  MarkHealthy();

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
  Decompressor sizes_decompressor;
  if (RIEGELI_UNLIKELY(!sizes_decompressor.Reset(&compressed_sizes_reader,
                                                 compression_type))) {
    compressed_sizes_reader.Close();
    return Fail(sizes_decompressor);
  }
  boundaries->clear();
  const size_t num_boundaries = IntCast<size_t>(num_records) + 1;
  size_t boundary = 0;
  for (;;) {
    boundaries->push_back(boundary);
    if (boundaries->size() == num_boundaries) break;
    uint64_t size;
    if (RIEGELI_UNLIKELY(!ReadVarint64(sizes_decompressor.reader(), &size))) {
      compressed_sizes_reader.Close();
      return Fail("Reading record size failed", *sizes_decompressor.reader());
    }
    if (RIEGELI_UNLIKELY(size > decoded_data_size - boundary)) {
      compressed_sizes_reader.Close();
      return Fail("Decoded data size larger than expected");
    }
    boundary += size;
  }
  if (RIEGELI_UNLIKELY(!sizes_decompressor.VerifyEndAndClose())) {
    compressed_sizes_reader.Close();
    return Fail(sizes_decompressor);
  }
  if (RIEGELI_UNLIKELY(!compressed_sizes_reader.VerifyEndAndClose())) {
    return Fail(compressed_sizes_reader);
  }
  if (RIEGELI_UNLIKELY(boundaries->back() != decoded_data_size)) {
    return Fail("Decoded data size smaller than expected");
  }

  if (RIEGELI_UNLIKELY(!values_decompressor_.Reset(src, compression_type))) {
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
