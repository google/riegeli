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

#include "riegeli/bytes/brotli_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "brotli/encode.h"
#include "riegeli/base/base.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/brotli_allocator.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr int BrotliWriterBase::Options::kMinCompressionLevel;
constexpr int BrotliWriterBase::Options::kMaxCompressionLevel;
constexpr int BrotliWriterBase::Options::kDefaultCompressionLevel;
constexpr int BrotliWriterBase::Options::kMinWindowLog;
constexpr int BrotliWriterBase::Options::kMaxWindowLog;
constexpr int BrotliWriterBase::Options::kDefaultWindowLog;
#endif

void BrotliWriterBase::Initialize(Writer* dest, int compression_level,
                                  int window_log, Position size_hint) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of BrotliWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->healthy())) {
    Fail(*dest);
    return;
  }
  compressor_.reset(BrotliEncoderCreateInstance(
      allocator_.alloc_func(), allocator_.free_func(), allocator_.opaque()));
  if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
    Fail(absl::InternalError("BrotliEncoderCreateInstance() failed"));
    return;
  }
  if (ABSL_PREDICT_FALSE(
          !BrotliEncoderSetParameter(compressor_.get(), BROTLI_PARAM_QUALITY,
                                     IntCast<uint32_t>(compression_level)))) {
    Fail(absl::InternalError(
        "BrotliEncoderSetParameter(BROTLI_PARAM_QUALITY) failed"));
    return;
  }
  if (ABSL_PREDICT_FALSE(!BrotliEncoderSetParameter(
          compressor_.get(), BROTLI_PARAM_LARGE_WINDOW,
          uint32_t{window_log > BROTLI_MAX_WINDOW_BITS}))) {
    Fail(absl::InternalError(
        "BrotliEncoderSetParameter(BROTLI_PARAM_LARGE_WINDOW) failed"));
    return;
  }
  if (ABSL_PREDICT_FALSE(
          !BrotliEncoderSetParameter(compressor_.get(), BROTLI_PARAM_LGWIN,
                                     IntCast<uint32_t>(window_log)))) {
    Fail(absl::InternalError(
        "BrotliEncoderSetParameter(BROTLI_PARAM_LGWIN) failed"));
    return;
  }
  if (size_hint > 0) {
    // Ignore errors from tuning.
    BrotliEncoderSetParameter(compressor_.get(), BROTLI_PARAM_SIZE_HINT,
                              SaturatingIntCast<uint32_t>(size_hint));
  }
}

void BrotliWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer& dest = *dest_writer();
    const absl::string_view data(start(), written_to_buffer());
    set_buffer();
    WriteInternal(data, dest, BROTLI_OPERATION_FINISH);
  }
  compressor_.reset();
  BufferedWriter::Done();
}

bool BrotliWriterBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at uncompressed byte ", pos())));
}

bool BrotliWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "buffer not empty";
  Writer& dest = *dest_writer();
  return WriteInternal(src, dest, BROTLI_OPERATION_PROCESS);
}

inline bool BrotliWriterBase::WriteInternal(absl::string_view src, Writer& dest,
                                            BrotliEncoderOperation op) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BrotliWriterBase::WriteInternal(): "
      << status();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BrotliWriterBase::WriteInternal(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  size_t available_in = src.size();
  const uint8_t* next_in = reinterpret_cast<const uint8_t*>(src.data());
  size_t available_out = 0;
  for (;;) {
    if (ABSL_PREDICT_FALSE(!BrotliEncoderCompressStream(
            compressor_.get(), op, &available_in, &next_in, &available_out,
            nullptr, nullptr))) {
      return Fail(
          Annotate(absl::InternalError("BrotliEncoderCompressStream() failed"),
                   absl::StrCat("at byte ", dest.pos())));
    }
    size_t length = 0;
    const char* const data = reinterpret_cast<const char*>(
        BrotliEncoderTakeOutput(compressor_.get(), &length));
    if (length > 0) {
      if (ABSL_PREDICT_FALSE(!dest.Write(absl::string_view(data, length)))) {
        return Fail(dest);
      }
    } else if (available_in == 0) {
      move_start_pos(src.size());
      return true;
    }
  }
}

bool BrotliWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  const absl::string_view data(start(), written_to_buffer());
  set_buffer();
  if (ABSL_PREDICT_FALSE(!WriteInternal(data, dest, BROTLI_OPERATION_FLUSH))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!dest.Flush(flush_type))) return Fail(dest);
  return true;
}

}  // namespace riegeli
