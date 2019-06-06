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

// Make ZSTD_WINDOWLOG_MIN, ZSTD_WINDOWLOG_MAX, ZSTD_getParams(), and
// ZSTD_initCStream_advanced() available.
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/bytes/zstd_writer.h"

#include <stddef.h>

#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "zstd.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr int ZstdWriterBase::Options::kMinCompressionLevel;
constexpr int ZstdWriterBase::Options::kMaxCompressionLevel;
constexpr int ZstdWriterBase::Options::kDefaultCompressionLevel;
constexpr int ZstdWriterBase::Options::kMinWindowLog;
constexpr int ZstdWriterBase::Options::kMaxWindowLog;
constexpr int ZstdWriterBase::Options::kDefaultWindowLog;
#endif

namespace {

// Reduces the size hint to a class index which actually determines internal
// parameters of the zstd compressor. This avoids varying ZSTD_CStreamKey for
// inconsequential variations of the size hint.
//
// This follows tableID computation in ZSTD_getCParams().
int SizeHintClass(Position size_hint) {
  if (size_hint == 0) return 0;
  return (size_hint <= Position{256} << 10 ? 1 : 0) +
         (size_hint <= Position{128} << 10 ? 1 : 0) +
         (size_hint <= Position{16} << 10 ? 1 : 0);
}

}  // namespace

void ZstdWriterBase::Initialize(Writer* dest, int compression_level,
                                int window_log,
                                absl::optional<Position> final_size,
                                Position size_hint, bool store_checksum) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of ZstdWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->healthy())) {
    Fail(*dest);
    return;
  }
  compressor_ =
      RecyclingPool<ZSTD_CStream, ZSTD_CStreamDeleter,
                    ZSTD_CStreamKey>::global()
          .Get(ZSTD_CStreamKey{compression_level, window_log,
                               SizeHintClass(size_hint)},
               [] {
                 return std::unique_ptr<ZSTD_CStream, ZSTD_CStreamDeleter>(
                     ZSTD_createCStream());
               });
  if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
    Fail(InternalError("ZSTD_createCStream() failed"));
    return;
  }
  ZSTD_parameters params = ZSTD_getParams(
      compression_level, IntCast<unsigned long long>(size_hint), 0);
  if (window_log >= 0) {
    params.cParams.windowLog = IntCast<unsigned>(window_log);
  }
  params.fParams.checksumFlag = store_checksum ? unsigned{1} : unsigned{0};
  const size_t result = ZSTD_initCStream_advanced(
      compressor_.get(), nullptr, 0, params,
      final_size.has_value() ? IntCast<unsigned long long>(*final_size)
                             : ZSTD_CONTENTSIZE_UNKNOWN);
  if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
    Fail(InternalError(absl::StrCat("ZSTD_initCStream_advanced() failed: ",
                                    ZSTD_getErrorName(result))));
  }
}

void ZstdWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(PushInternal())) {
    Writer* const dest = dest_writer();
    RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
        << "BufferedWriter::PushInternal() did not empty the buffer";
    FlushInternal(ZSTD_endStream, "ZSTD_endStream()", dest);
  }
  compressor_.reset();
  BufferedWriter::Done();
}

bool ZstdWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "buffer not empty";
  Writer* const dest = dest_writer();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - limit_pos())) {
    return FailOverflow();
  }
  ZSTD_inBuffer input = {src.data(), src.size(), 0};
  for (;;) {
    ZSTD_outBuffer output = {dest->cursor(), dest->available(), 0};
    const size_t result =
        ZSTD_compressStream(compressor_.get(), &output, &input);
    dest->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(InternalError(absl::StrCat("ZSTD_compressStream() failed: ",
                                             ZSTD_getErrorName(result))));
    }
    if (output.pos < output.size) {
      RIEGELI_ASSERT_EQ(input.pos, input.size)
          << "ZSTD_compressStream() returned but there are still input data "
             "and output space";
      start_pos_ += input.pos;
      return true;
    }
    if (ABSL_PREDICT_FALSE(!dest->Push())) return Fail(*dest);
  }
}

bool ZstdWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  Writer* const dest = dest_writer();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "BufferedWriter::PushInternal() did not empty the buffer";
  if (ABSL_PREDICT_FALSE(
          !FlushInternal(ZSTD_flushStream, "ZSTD_flushStream()", dest))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!dest->Flush(flush_type))) return Fail(*dest);
  return true;
}

template <typename Function>
bool ZstdWriterBase::FlushInternal(Function function,
                                   absl::string_view function_name,
                                   Writer* dest) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of ZstdWriterBase::FlushInternal(): " << status();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of ZstdWriterBase::FlushInternal(): "
         "buffer not empty";
  for (;;) {
    ZSTD_outBuffer output = {dest->cursor(), dest->available(), 0};
    const size_t result = function(compressor_.get(), &output);
    dest->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (result == 0) return true;
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(InternalError(
          absl::StrCat(function_name, " failed: ", ZSTD_getErrorName(result))));
    }
    RIEGELI_ASSERT_EQ(output.pos, output.size)
        << function_name << " returned but there is still output space";
    if (ABSL_PREDICT_FALSE(!dest->Push())) return Fail(*dest);
  }
}

}  // namespace riegeli
