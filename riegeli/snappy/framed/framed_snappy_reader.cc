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

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
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
  if (ABSL_PREDICT_FALSE(!src->healthy()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  initial_compressed_pos_ = src->pos();
}

void FramedSnappyReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Fail(
        absl::InvalidArgumentError("Truncated FramedSnappy-compressed stream"));
  }
  PullableReader::Done();
  uncompressed_ = Buffer();
}

bool FramedSnappyReaderBase::FailInvalidStream(absl::string_view message) {
  return Fail(absl::InvalidArgumentError(
      absl::StrCat("Invalid FramedSnappy-compressed stream: ", message)));
}

absl::Status FramedSnappyReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Reader& src = *src_reader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*src->reader()` with the
  // compressed position. Clarify that the current position is the uncompressed
  // position instead of delegating to `PullableReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status FramedSnappyReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool FramedSnappyReaderBase::PullBehindScratch() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "some data available, use Pull() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  truncated_ = false;
  while (src.Pull(sizeof(uint32_t))) {
    const uint32_t chunk_header = ReadLittleEndian32(src.cursor());
    const uint8_t chunk_type = static_cast<uint8_t>(chunk_header);
    const size_t chunk_length = IntCast<size_t>(chunk_header >> 8);
    if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint32_t) + chunk_length))) {
      set_buffer();
      if (ABSL_PREDICT_FALSE(!src.healthy())) {
        return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
      }
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
        uncompressed_.Reset(uncompressed_length);
        if (ABSL_PREDICT_FALSE(!snappy::RawUncompress(
                compressed_data, compressed_length, uncompressed_.data()))) {
          set_buffer();
          return FailInvalidStream("invalid compressed data");
        }
        if (ABSL_PREDICT_FALSE(
                MaskChecksum(crc32c::Crc32c(
                    uncompressed_.data(), uncompressed_length)) != checksum)) {
          set_buffer();
          return FailInvalidStream(
              "Invalid FramedSnappy-compressed stream: wrong checksum");
        }
        src.move_cursor(sizeof(uint32_t) + chunk_length);
        if (ABSL_PREDICT_FALSE(uncompressed_length == 0)) continue;
        if (ABSL_PREDICT_FALSE(uncompressed_length >
                               std::numeric_limits<Position>::max() -
                                   limit_pos())) {
          set_buffer(uncompressed_.data());
          return FailOverflow();
        }
        set_buffer(uncompressed_.data(), uncompressed_length);
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
  if (ABSL_PREDICT_FALSE(!src.healthy())) {
    return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
  }
  if (ABSL_PREDICT_FALSE(src.available() > 0)) truncated_ = true;
  return false;
}

bool FramedSnappyReaderBase::SupportsRewind() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsRewind();
}

bool FramedSnappyReaderBase::SeekBehindScratch(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "scratch used";
  if (new_pos <= limit_pos()) {
    // Seeking backwards.
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
    Reader& src = *src_reader();
    truncated_ = false;
    set_buffer();
    set_limit_pos(0);
    if (ABSL_PREDICT_FALSE(!src.Seek(initial_compressed_pos_))) {
      return FailWithoutAnnotation(
          AnnotateOverSrc(src.StatusOrAnnotate(absl::DataLossError(
              "FramedSnappy-compressed stream got truncated"))));
    }
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
    if (new_pos == 0) return true;
  }
  return PullableReader::SeekBehindScratch(new_pos);
}

bool FramedSnappyReaderBase::SupportsNewReader() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> FramedSnappyReaderBase::NewReaderImpl(
    Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point
  // if `src_reader()->SupportsNewReader()`.
  Reader& src = *src_reader();
  std::unique_ptr<Reader> compressed_reader =
      src.NewReader(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverSrc(src.status()));
    return nullptr;
  }
  std::unique_ptr<Reader> reader =
      std::make_unique<FramedSnappyReader<std::unique_ptr<Reader>>>(
          std::move(compressed_reader));
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
