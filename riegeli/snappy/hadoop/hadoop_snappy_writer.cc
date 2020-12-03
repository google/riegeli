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

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/endian/endian_writing.h"
#include "snappy.h"

namespace riegeli {

void HadoopSnappyWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of HadoopSnappyWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->healthy())) Fail(*dest);
}

void HadoopSnappyWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    if (ABSL_PREDICT_TRUE(SyncScratch())) {
      Writer& dest = *dest_writer();
      PushInternal(dest);
    }
  }
  PushableWriter::Done();
}

bool HadoopSnappyWriterBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at uncompressed byte ", pos())));
}

bool HadoopSnappyWriterBase::PushSlow(size_t min_length,
                                      size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!PushUsingScratch(min_length, recommended_length))) {
    return available() >= min_length;
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
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
  const size_t uncompressed_length = written_to_buffer();
  RIEGELI_ASSERT_LE(uncompressed_length, snappy::kBlockSize)
      << "Failed invariant of HadoopSnappyWriterBase: buffer too large";
  if (uncompressed_length == 0) return true;
  set_cursor(start());
  const char* const uncompressed_data = cursor();
  if (ABSL_PREDICT_FALSE(
          !dest.Push(2 * sizeof(uint32_t) +
                     snappy::MaxCompressedLength(uncompressed_length)))) {
    return Fail(dest);
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

bool HadoopSnappyWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!PushInternal(dest))) return false;
  if (ABSL_PREDICT_FALSE(!dest.Flush(flush_type))) return Fail(dest);
  return true;
}

}  // namespace riegeli
