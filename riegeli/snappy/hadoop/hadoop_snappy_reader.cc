// Copyright 2020 Google LLC
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

#include "riegeli/snappy/hadoop/hadoop_snappy_reader.h"

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
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/endian/endian_reading.h"
#include "snappy.h"

namespace riegeli {

void HadoopSnappyReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of HadoopSnappyReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  initial_compressed_pos_ = src->pos();
}

void HadoopSnappyReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Fail(
        absl::InvalidArgumentError("Truncated HadoopSnappy-compressed stream"));
  }
  PullableReader::Done();
  uncompressed_ = Buffer();
}

bool HadoopSnappyReaderBase::FailInvalidStream(absl::string_view message) {
  return Fail(absl::InvalidArgumentError(
      absl::StrCat("Invalid HadoopSnappy-compressed stream: ", message)));
}

absl::Status HadoopSnappyReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Reader& src = *src_reader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*src->reader()` with the
  // compressed position. Clarify that the current position is the uncompressed
  // position instead of delegating to `PullableReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status HadoopSnappyReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool HadoopSnappyReaderBase::PullBehindScratch() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "some data available, use Pull() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  truncated_ = false;
  while (remaining_chunk_length_ == 0) {
    if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint32_t)))) {
      set_buffer();
      if (ABSL_PREDICT_FALSE(!src.ok())) {
        return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
      }
      if (ABSL_PREDICT_FALSE(src.available() > 0)) truncated_ = true;
      return false;
    }
    remaining_chunk_length_ = ReadBigEndian32(src.cursor());
    src.move_cursor(sizeof(uint32_t));
  }
  size_t uncompressed_length;
  char* uncompressed_data;
  do {
    if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint32_t)))) {
      set_buffer();
      if (ABSL_PREDICT_FALSE(!src.ok())) {
        return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
      }
      truncated_ = true;
      return false;
    }
    const uint32_t compressed_length = ReadBigEndian32(src.cursor());
    if (ABSL_PREDICT_FALSE(compressed_length >
                           std::numeric_limits<uint32_t>::max() -
                               sizeof(uint32_t))) {
      set_buffer();
      return FailInvalidStream("compressed length too large");
    }
    if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint32_t) + compressed_length))) {
      set_buffer();
      if (ABSL_PREDICT_FALSE(!src.ok())) {
        return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
      }
      truncated_ = true;
      return false;
    }
    const char* const compressed_data = src.cursor() + sizeof(uint32_t);
    if (ABSL_PREDICT_FALSE(!snappy::GetUncompressedLength(
            compressed_data, compressed_length, &uncompressed_length))) {
      set_buffer();
      return FailInvalidStream("invalid uncompressed length");
    }
    if (ABSL_PREDICT_FALSE(uncompressed_length > remaining_chunk_length_)) {
      set_buffer();
      return FailInvalidStream("uncompressed length too large");
    }
    uncompressed_.Reset(uncompressed_length);
    uncompressed_data = uncompressed_.data();
    if (ABSL_PREDICT_FALSE(!snappy::RawUncompress(
            compressed_data, compressed_length, uncompressed_data))) {
      set_buffer();
      return FailInvalidStream("invalid compressed data");
    }
    src.move_cursor(sizeof(uint32_t) + compressed_length);
  } while (uncompressed_length == 0);
  remaining_chunk_length_ -= uncompressed_length;
  if (ABSL_PREDICT_FALSE(uncompressed_length >
                         std::numeric_limits<Position>::max() - limit_pos())) {
    set_buffer(uncompressed_data);
    return FailOverflow();
  }
  set_buffer(uncompressed_data, uncompressed_length);
  move_limit_pos(available());
  return true;
}

bool HadoopSnappyReaderBase::SupportsRewind() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsRewind();
}

bool HadoopSnappyReaderBase::SeekBehindScratch(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "scratch used";
  if (new_pos <= limit_pos()) {
    // Seeking backwards.
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    Reader& src = *src_reader();
    truncated_ = false;
    remaining_chunk_length_ = 0;
    set_buffer();
    set_limit_pos(0);
    if (ABSL_PREDICT_FALSE(!src.Seek(initial_compressed_pos_))) {
      return FailWithoutAnnotation(
          AnnotateOverSrc(src.StatusOrAnnotate(absl::DataLossError(
              "HadoopSnappy-compressed stream got truncated"))));
    }
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    if (new_pos == 0) return true;
  }
  return PullableReader::SeekBehindScratch(new_pos);
}

bool HadoopSnappyReaderBase::SupportsNewReader() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> HadoopSnappyReaderBase::NewReaderImpl(
    Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
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
      std::make_unique<HadoopSnappyReader<std::unique_ptr<Reader>>>(
          std::move(compressed_reader));
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
