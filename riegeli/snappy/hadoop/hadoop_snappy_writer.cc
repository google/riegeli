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

#include "riegeli/snappy/hadoop/hadoop_snappy_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/endian/endian_writing.h"
#include "riegeli/snappy/hadoop/hadoop_snappy_reader.h"
#include "snappy.h"

namespace riegeli {

void HadoopSnappyWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of HadoopSnappyWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
  }
}

void HadoopSnappyWriterBase::Done() {
  PushableWriter::Done();
  uncompressed_ = Buffer();
  associated_reader_.Reset();
}

absl::Status HadoopSnappyWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *dest_writer();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*dest->writer()` with the
  // compressed position. Clarify that the current position is the uncompressed
  // position instead of delegating to `PushableWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status HadoopSnappyWriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool HadoopSnappyWriterBase::PushBehindScratch() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PushableWriter::PushBehindScratch(): "
         "some space available, use Push() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::PushBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!PushInternal(dest))) return false;
  if (ABSL_PREDICT_FALSE(start_pos() == std::numeric_limits<Position>::max())) {
    return FailOverflow();
  }
  const size_t length =
      UnsignedMin(BufferLength(1, snappy::kBlockSize, size_hint_, start_pos()),
                  std::numeric_limits<Position>::max() - start_pos());
  uncompressed_.Reset(length);
  set_buffer(uncompressed_.data(), length);
  return true;
}

inline bool HadoopSnappyWriterBase::PushInternal(Writer& dest) {
  const size_t uncompressed_length = start_to_cursor();
  RIEGELI_ASSERT_LE(uncompressed_length, snappy::kBlockSize)
      << "Failed invariant of HadoopSnappyWriterBase: buffer too large";
  if (uncompressed_length == 0) return true;
  set_cursor(start());
  const char* const uncompressed_data = cursor();
  if (ABSL_PREDICT_FALSE(
          !dest.Push(2 * sizeof(uint32_t) +
                     snappy::MaxCompressedLength(uncompressed_length)))) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  char* const compressed_chunk = dest.cursor();
  WriteBigEndian32(IntCast<uint32_t>(uncompressed_length), compressed_chunk);
  size_t compressed_length;
  snappy::RawCompress(uncompressed_data, uncompressed_length,
                      compressed_chunk + 2 * sizeof(uint32_t),
                      &compressed_length);
  WriteBigEndian32(IntCast<uint32_t>(compressed_length),
                   compressed_chunk + sizeof(uint32_t));
  dest.move_cursor(2 * sizeof(uint32_t) + compressed_length);
  move_start_pos(uncompressed_length);
  return true;
}

bool HadoopSnappyWriterBase::FlushBehindScratch(FlushType flush_type) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::FlushBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *dest_writer();
  return PushInternal(dest);
}

bool HadoopSnappyWriterBase::SupportsReadMode() {
  Writer* const dest = dest_writer();
  return dest != nullptr && dest->SupportsReadMode();
}

Reader* HadoopSnappyWriterBase::ReadModeBehindScratch(Position initial_pos) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::ReadModeBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!HadoopSnappyWriterBase::FlushBehindScratch(
          FlushType::kFromObject))) {
    return nullptr;
  }
  Writer& dest = *dest_writer();
  Reader* const compressed_reader = dest.ReadMode(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    return nullptr;
  }
  HadoopSnappyReader<>* const reader =
      associated_reader_.ResetReader(compressed_reader);
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
