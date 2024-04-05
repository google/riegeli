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

#include "riegeli/snappy/framed/framed_snappy_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/digests/crc32c_digester.h"
#include "riegeli/digests/digesting_writer.h"
#include "riegeli/endian/endian_writing.h"
#include "riegeli/snappy/framed/framed_snappy_reader.h"
#include "snappy.h"

namespace riegeli {

void FramedSnappyWriterBase::Initialize(Writer* dest, int compression_level) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of FramedSnappyWriter: null Writer pointer";
  compression_level_ = compression_level;
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  initial_compressed_pos_ = dest->pos();
  if (dest->pos() == 0) {
    // Stream identifier.
    if (ABSL_PREDICT_FALSE(!dest->Write(absl::string_view("\xff\x06\x00\x00"
                                                          "sNaPpY",
                                                          10)))) {
      FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    }
  }
}

void FramedSnappyWriterBase::Done() {
  PushableWriter::Done();
  uncompressed_ = Buffer();
  associated_reader_.Reset();
}

absl::Status FramedSnappyWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `dest` with the compressed
  // position. Clarify that the current position is the uncompressed position
  // instead of delegating to `PushableWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status FramedSnappyWriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

void FramedSnappyWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  size_hint_ =
      write_size_hint == absl::nullopt
          ? absl::nullopt
          : absl::make_optional(SaturatingAdd(pos(), *write_size_hint));
}

bool FramedSnappyWriterBase::PushBehindScratch(size_t recommended_length) {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PushableWriter::PushBehindScratch(): "
         "some space available, use Push() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::PushBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(!PushInternal(dest))) return false;
  if (ABSL_PREDICT_FALSE(start_pos() == std::numeric_limits<Position>::max())) {
    return FailOverflow();
  }
  const size_t length = UnsignedMin(
      UnsignedMax(ApplySizeHint(snappy::kBlockSize, size_hint_, start_pos()),
                  recommended_length),
      snappy::kBlockSize, std::numeric_limits<Position>::max() - start_pos());
  uncompressed_.Reset(length);
  set_buffer(uncompressed_.data(), length);
  return true;
}

inline bool FramedSnappyWriterBase::PushInternal(Writer& dest) {
  const size_t uncompressed_length = start_to_cursor();
  RIEGELI_ASSERT_LE(uncompressed_length, snappy::kBlockSize)
      << "Failed invariant of FramedSnappyWriterBase: buffer too large";
  if (uncompressed_length == 0) return true;
  set_cursor(start());
  const char* const uncompressed_data = cursor();
  if (ABSL_PREDICT_FALSE(
          !dest.Push(2 * sizeof(uint32_t) +
                     snappy::MaxCompressedLength(uncompressed_length)))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  char* const compressed_chunk = dest.cursor();
  size_t compressed_length;
  snappy::RawCompress(uncompressed_data, uncompressed_length,
                      compressed_chunk + 2 * sizeof(uint32_t),
                      &compressed_length, {/*level=*/compression_level_});
  if (compressed_length < uncompressed_length) {
    WriteLittleEndian32(
        IntCast<uint32_t>(0x00 /* Compressed data */ |
                          ((sizeof(uint32_t) + compressed_length) << 8)),
        compressed_chunk);
  } else {
    std::memcpy(compressed_chunk + 2 * sizeof(uint32_t), uncompressed_data,
                uncompressed_length);
    compressed_length = uncompressed_length;
    WriteLittleEndian32(
        IntCast<uint32_t>(0x01 /* Uncompressed data */ |
                          ((sizeof(uint32_t) + compressed_length) << 8)),
        compressed_chunk);
  }
  WriteLittleEndian32(
      MaskCrc32c(
          DigestFrom(absl::string_view(uncompressed_data, uncompressed_length),
                     Crc32cDigester())),
      compressed_chunk + sizeof(uint32_t));
  dest.move_cursor(2 * sizeof(uint32_t) + compressed_length);
  move_start_pos(uncompressed_length);
  return true;
}

bool FramedSnappyWriterBase::FlushBehindScratch(FlushType flush_type) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::FlushBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  return PushInternal(dest);
}

bool FramedSnappyWriterBase::SupportsReadMode() {
  Writer* const dest = DestWriter();
  return dest != nullptr && dest->SupportsReadMode();
}

Reader* FramedSnappyWriterBase::ReadModeBehindScratch(Position initial_pos) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::ReadModeBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!FramedSnappyWriterBase::FlushBehindScratch(
          FlushType::kFromObject))) {
    return nullptr;
  }
  Writer& dest = *DestWriter();
  Reader* const compressed_reader = dest.ReadMode(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    return nullptr;
  }
  FramedSnappyReader<>* const reader =
      associated_reader_.ResetReader(compressed_reader);
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
