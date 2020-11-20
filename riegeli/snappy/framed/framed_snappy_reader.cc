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

#include "riegeli/snappy/framed/framed_snappy_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "crc32c/crc32c.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/endian/endian_reading.h"
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
  if (ABSL_PREDICT_FALSE(!src->healthy()) && src->available() == 0) Fail(*src);
}

void FramedSnappyReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Reader& src = *src_reader();
    Fail(Annotate(absl::DataLossError("Truncated Snappy-compressed stream"),
                  absl::StrCat("at byte ", src.pos())));
  }
  PullableReader::Done();
}

bool FramedSnappyReaderBase::FailInvalidStream(absl::string_view message) {
  Reader& src = *src_reader();
  return Fail(Annotate(absl::DataLossError(absl::StrCat(
                           "Invalid Snappy-compressed stream: ", message)),
                       absl::StrCat("at byte ", src.pos())));
}

bool FramedSnappyReaderBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at uncompressed byte ", pos())));
}

bool FramedSnappyReaderBase::PullSlow(size_t min_length,
                                      size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!PullUsingScratch(min_length, recommended_length))) {
    return available() >= min_length;
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  truncated_ = false;
  while (src.Pull(sizeof(uint32_t))) {
    const uint32_t chunk_header = ReadLittleEndian32(src.cursor());
    const uint8_t chunk_type = static_cast<uint8_t>(chunk_header);
    const size_t chunk_length = IntCast<size_t>(chunk_header >> 8);
    if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint32_t) + chunk_length))) {
      set_buffer();
      if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
      truncated_ = true;
      return false;
    }
    if (ABSL_PREDICT_FALSE(src.pos() == 0 &&
                           chunk_type != 0xff /* Stream identifier */)) {
      set_buffer();
      return FailInvalidStream("missing stream identifier");
    }
    switch (chunk_type) {
      case 0x00: {  // Compressed data.
        if (ABSL_PREDICT_FALSE(chunk_length < sizeof(uint32_t))) {
          set_buffer();
          return FailInvalidStream("compressed data too short");
        }
        const uint32_t checksum =
            ReadLittleEndian32(src.cursor() + sizeof(uint32_t));
        const char* const compressed_data = src.cursor() + 2 * sizeof(uint32_t);
        const size_t compressed_length = chunk_length - sizeof(uint32_t);
        size_t uncompressed_length;
        if (ABSL_PREDICT_FALSE(!snappy::GetUncompressedLength(
                compressed_data, compressed_length, &uncompressed_length))) {
          set_buffer();
          return FailInvalidStream("invalid uncompressed length");
        }
        if (ABSL_PREDICT_FALSE(uncompressed_length > snappy::kBlockSize)) {
          set_buffer();
          return FailInvalidStream("uncompressed length too large");
        }
        uncompressed_.Resize(uncompressed_length);
        char* const uncompressed_data = uncompressed_.GetData();
        if (ABSL_PREDICT_FALSE(!snappy::RawUncompress(
                compressed_data, compressed_length, uncompressed_data))) {
          set_buffer();
          return FailInvalidStream("invalid compressed data");
        }
        if (ABSL_PREDICT_FALSE(MaskChecksum(crc32c::Crc32c(
                                   uncompressed_data, uncompressed_length)) !=
                               checksum)) {
          set_buffer();
          return FailInvalidStream(
              "Invalid Snappy-compressed stream: wrong checksum");
        }
        src.move_cursor(sizeof(uint32_t) + chunk_length);
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
        if (ABSL_PREDICT_FALSE(chunk_length < sizeof(uint32_t))) {
          set_buffer();
          return FailInvalidStream("uncompressed data too short");
        }
        const uint32_t checksum =
            ReadLittleEndian32(src.cursor() + sizeof(uint32_t));
        const char* const uncompressed_data =
            src.cursor() + 2 * sizeof(uint32_t);
        const size_t uncompressed_length = chunk_length - sizeof(uint32_t);
        if (ABSL_PREDICT_FALSE(uncompressed_length > snappy::kBlockSize)) {
          set_buffer();
          return FailInvalidStream("uncompressed length too large");
        }
        if (ABSL_PREDICT_FALSE(MaskChecksum(crc32c::Crc32c(
                                   uncompressed_data, uncompressed_length)) !=
                               checksum)) {
          set_buffer();
          return FailInvalidStream("wrong checksum");
        }
        src.move_cursor(sizeof(uint32_t) + chunk_length);
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
                absl::string_view(src.cursor() + sizeof(uint32_t),
                                  chunk_length) !=
                absl::string_view("sNaPpY", 6))) {
          set_buffer();
          return FailInvalidStream("invalid stream identifier");
        }
        src.move_cursor(sizeof(uint32_t) + chunk_length);
        continue;
      default:
        if (ABSL_PREDICT_FALSE(chunk_type < 0x80)) {
          set_buffer();
          return FailInvalidStream("reserved unskippable chunk");
        }
        src.move_cursor(sizeof(uint32_t) + chunk_length);
        continue;
    }
  }
  set_buffer();
  if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
  if (ABSL_PREDICT_FALSE(src.available() > 0)) truncated_ = true;
  return false;
}

}  // namespace riegeli
