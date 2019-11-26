// Copyright 2019 Google LLC
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

#include "riegeli/bytes/framed_snappy_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "crc32c/crc32c.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/endian.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"
#include "snappy.h"

namespace riegeli {

namespace {

// https://github.com/google/snappy/blob/e9e11b84e629c3e06fbaa4f0a86de02ceb9d6992/framing_format.txt#L39
inline uint32_t MaskChecksum(uint32_t x) {
  return ((x >> 15) | (x << 17)) + 0xa282ead8;
}

}  // namespace

void FramedSnappyReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of FramedSnappyReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->healthy()) && src->available() == 0) {
    Fail(*src);
    return;
  }
}

void FramedSnappyReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Fail(DataLossError("Truncated Snappy-compressed stream"));
  }
  PullableReader::Done();
}

bool FramedSnappyReaderBase::PullSlow(size_t min_length,
                                      size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Reader::PullSlow(): "
         "length too small, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!PullUsingScratch(min_length, recommended_length))) {
    return available() >= min_length;
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  truncated_ = false;
  uint32_t chunk_header;
  for (;;) {
    if (ABSL_PREDICT_FALSE(!src->Pull(sizeof(chunk_header)))) {
      set_buffer();
      if (ABSL_PREDICT_FALSE(!src->healthy())) return Fail(*src);
      if (ABSL_PREDICT_FALSE(src->available() > 0)) truncated_ = true;
      return false;
    }
    std::memcpy(&chunk_header, src->cursor(), sizeof(chunk_header));
    const uint8_t chunk_type =
        static_cast<uint8_t>(ReadLittleEndian32(chunk_header));
    const size_t chunk_length =
        IntCast<size_t>(ReadLittleEndian32(chunk_header) >> 8);
    if (ABSL_PREDICT_FALSE(!src->Pull(sizeof(chunk_header) + chunk_length))) {
      set_buffer();
      if (ABSL_PREDICT_FALSE(!src->healthy())) return Fail(*src);
      if (ABSL_PREDICT_FALSE(src->available() > 0)) truncated_ = true;
      return false;
    }
    const char* const compressed_chunk = src->cursor();
    if (ABSL_PREDICT_FALSE(src->pos() == 0 &&
                           chunk_type != 0xff /* Stream identifier */)) {
      set_buffer();
      return Fail(DataLossError(
          "Invalid Snappy-compressed stream: missing stream identifier"));
    }
    switch (chunk_type) {
      case 0x00: {  // Compressed data.
        uint32_t checksum;
        if (ABSL_PREDICT_FALSE(chunk_length < sizeof(checksum))) {
          set_buffer();
          return Fail(DataLossError(
              "Invalid Snappy-compressed stream: compressed data too short"));
        }
        const char* const compressed_data =
            compressed_chunk + sizeof(chunk_header) + sizeof(checksum);
        const size_t compressed_length = chunk_length - sizeof(checksum);
        size_t uncompressed_length;
        if (ABSL_PREDICT_FALSE(!snappy::GetUncompressedLength(
                compressed_data, compressed_length, &uncompressed_length))) {
          set_buffer();
          return Fail(DataLossError(
              "Invalid Snappy-compressed stream: invalid uncompressed length"));
        }
        if (ABSL_PREDICT_FALSE(uncompressed_length > snappy::kBlockSize)) {
          set_buffer();
          return Fail(
              DataLossError("Invalid Snappy-compressed stream: "
                            "uncompressed length too large"));
        }
        uncompressed_.Resize(uncompressed_length);
        char* const uncompressed_data = uncompressed_.GetData();
        if (ABSL_PREDICT_FALSE(!snappy::RawUncompress(
                compressed_data, compressed_length, uncompressed_data))) {
          set_buffer();
          return Fail(DataLossError(
              "Invalid Snappy-compressed stream: invalid compressed data"));
        }
        std::memcpy(&checksum, compressed_chunk + sizeof(chunk_header),
                    sizeof(checksum));
        if (ABSL_PREDICT_FALSE(MaskChecksum(crc32c::Crc32c(
                                   uncompressed_data, uncompressed_length)) !=
                               ReadLittleEndian32(checksum))) {
          set_buffer();
          return Fail(DataLossError(
              "Invalid Snappy-compressed stream: wrong checksum"));
        }
        src->move_cursor(sizeof(chunk_header) + chunk_length);
        if (ABSL_PREDICT_FALSE(uncompressed_length == 0)) continue;
        if (ABSL_PREDICT_FALSE(uncompressed_length >
                               std::numeric_limits<Position>::max() -
                                   limit_pos())) {
          set_buffer(uncompressed_data);
          return FailOverflow();
        }
        set_buffer(uncompressed_data, uncompressed_length);
        move_limit_pos(available());
        return true;
      }
      case 0x01: {  // Uncompressed data.
        uint32_t checksum;
        if (ABSL_PREDICT_FALSE(chunk_length < sizeof(checksum))) {
          set_buffer();
          return Fail(DataLossError(
              "Invalid Snappy-compressed stream: uncompressed data too short"));
        }
        const char* const uncompressed_data =
            compressed_chunk + sizeof(chunk_header) + sizeof(checksum);
        const size_t uncompressed_length = chunk_length - sizeof(checksum);
        if (ABSL_PREDICT_FALSE(uncompressed_length > snappy::kBlockSize)) {
          set_buffer();
          return Fail(
              DataLossError("Invalid Snappy-compressed stream: "
                            "uncompressed length too large"));
        }
        std::memcpy(&checksum, compressed_chunk + sizeof(chunk_header),
                    sizeof(checksum));
        if (ABSL_PREDICT_FALSE(MaskChecksum(crc32c::Crc32c(
                                   uncompressed_data, uncompressed_length)) !=
                               ReadLittleEndian32(checksum))) {
          set_buffer();
          return Fail(DataLossError(
              "Invalid Snappy-compressed stream: wrong checksum"));
        }
        src->move_cursor(sizeof(chunk_header) + chunk_length);
        if (ABSL_PREDICT_FALSE(uncompressed_length == 0)) continue;
        if (ABSL_PREDICT_FALSE(uncompressed_length >
                               std::numeric_limits<Position>::max() -
                                   limit_pos())) {
          set_buffer();
          return FailOverflow();
        }
        set_buffer(uncompressed_data, uncompressed_length);
        move_limit_pos(available());
        return true;
      }
      case 0xff:  // Stream identifier.
        if (ABSL_PREDICT_FALSE(
                absl::string_view(compressed_chunk + sizeof(chunk_header),
                                  chunk_length) !=
                absl::string_view("sNaPpY", 6))) {
          set_buffer();
          return Fail(DataLossError(
              "Invalid Snappy-compressed stream: invalid stream identifier"));
        }
        src->move_cursor(sizeof(chunk_header) + chunk_length);
        continue;
      default:
        if (ABSL_PREDICT_FALSE(chunk_type < 0x80)) {
          set_buffer();
          return Fail(DataLossError(
              "Invalid Snappy-compressed stream: reserved unskippable chunk"));
        }
        src->move_cursor(sizeof(chunk_header) + chunk_length);
        continue;
    }
  }
}

}  // namespace riegeli
