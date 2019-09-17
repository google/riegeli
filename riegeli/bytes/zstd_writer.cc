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

// Enables the experimental zstd API:
//  * ZSTD_CCtx_params
//  * ZSTD_freeCCtxParams
//  * ZSTD_createCCtxParams
//  * ZSTD_CCtxParams_init_advanced
//  * ZSTD_getParams
//  * ZSTD_CCtx_setParametersUsingCCtxParams
//
// Using the experimental zstd API is optional. If this gets removed, size_hint
// is ignored by zstd if final_size is not set (size_hint remains used only for
// BufferedWriter tuning).
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/bytes/zstd_writer.h"

#include <stddef.h>

#include <limits>
#include <memory>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/canonical_errors.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
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
// parameters of the zstd compressor. This avoids varying ZSTD_CCtxKey for
// inconsequential variations of the size hint.
//
// This follows tableID computation in ZSTD_getCParams().
int SizeHintClass(Position size_hint) {
  if (size_hint == 0) return 0;
  return (size_hint <= Position{256} << 10 ? 1 : 0) +
         (size_hint <= Position{128} << 10 ? 1 : 0) +
         (size_hint <= Position{16} << 10 ? 1 : 0);
}

#ifdef ZSTD_STATIC_LINKING_ONLY
struct ZSTD_CCtxParamsDeleter {
  void operator()(ZSTD_CCtx_params* ptr) const { ZSTD_freeCCtxParams(ptr); }
};
#endif

}  // namespace

void ZstdWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of ZstdWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->healthy())) {
    Fail(*dest);
    return;
  }
}

inline bool ZstdWriterBase::CreateCompressor() {
  compressor_ =
      RecyclingPool<ZSTD_CCtx, ZSTD_CCtxDeleter, ZSTD_CCtxKey>::global().Get(
          ZSTD_CCtxKey{compressor_options_.compression_level,
                       compressor_options_.window_log,
                       SizeHintClass(compressor_options_.size_hint)},
          [] {
            return std::unique_ptr<ZSTD_CCtx, ZSTD_CCtxDeleter>(
                ZSTD_createCCtx());
          },
          [](ZSTD_CCtx* compressor) {
            const size_t result =
                ZSTD_CCtx_reset(compressor, ZSTD_reset_session_and_parameters);
            RIEGELI_ASSERT(!ZSTD_isError(result))
                << "ZSTD_CCtx_reset() failed: " << ZSTD_getErrorName(result);
          });
  if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
    return Fail(InternalError("ZSTD_createCCtx() failed"));
  }
#ifdef ZSTD_STATIC_LINKING_ONLY
  // size_hint is redundant if final_size is present.
  if (compressor_options_.size_hint > 0 &&
      !compressor_options_.final_size.has_value()) {
    const std::unique_ptr<ZSTD_CCtx_params, ZSTD_CCtxParamsDeleter> params(
        ZSTD_createCCtxParams());
    if (ABSL_PREDICT_FALSE(params == nullptr)) {
      return Fail(InternalError("ZSTD_createCCtxParams() failed"));
    }
    {
      const size_t result = ZSTD_CCtxParams_init_advanced(
          params.get(),
          ZSTD_getParams(
              compressor_options_.compression_level,
              IntCast<unsigned long long>(compressor_options_.size_hint), 0));
      if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
        return Fail(InternalError(
            absl::StrCat("ZSTD_CCtxParams_init_advanced() failed: ",
                         ZSTD_getErrorName(result))));
      }
    }
    {
      const size_t result = ZSTD_CCtx_setParametersUsingCCtxParams(
          compressor_.get(), params.get());
      if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
        return Fail(InternalError(
            absl::StrCat("ZSTD_CCtx_setParametersUsingCCtxParams() failed: ",
                         ZSTD_getErrorName(result))));
      }
    }
  }
#endif
  {
    const size_t result =
        ZSTD_CCtx_setParameter(compressor_.get(), ZSTD_c_compressionLevel,
                               compressor_options_.compression_level);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(InternalError(absl::StrCat(
          "ZSTD_CCtx_setParameter(ZSTD_c_compressionLevel) failed: ",
          ZSTD_getErrorName(result))));
    }
  }
  if (compressor_options_.window_log != Options::kDefaultWindowLog) {
    const size_t result = ZSTD_CCtx_setParameter(
        compressor_.get(), ZSTD_c_windowLog, compressor_options_.window_log);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(InternalError(
          absl::StrCat("ZSTD_CCtx_setParameter(ZSTD_c_windowLog) failed: ",
                       ZSTD_getErrorName(result))));
    }
  }
  {
    const size_t result =
        ZSTD_CCtx_setParameter(compressor_.get(), ZSTD_c_checksumFlag,
                               compressor_options_.store_checksum ? 1 : 0);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(InternalError(
          absl::StrCat("ZSTD_CCtx_setParameter(ZSTD_c_checksumFlag) failed: ",
                       ZSTD_getErrorName(result))));
    }
  }
  if (compressor_options_.final_size.has_value()) {
    const size_t result = ZSTD_CCtx_setPledgedSrcSize(
        compressor_.get(),
        IntCast<unsigned long long>(*compressor_options_.final_size));
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(
          InternalError(absl::StrCat("ZSTD_CCtx_setPledgedSrcSize() failed: ",
                                     ZSTD_getErrorName(result))));
    }
  }
  return true;
}

void ZstdWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer* const dest = dest_writer();
    const size_t buffered_length = written_to_buffer();
    cursor_ = start_;
    WriteInternal(absl::string_view(start_, buffered_length), dest, ZSTD_e_end);
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
  return WriteInternal(src, dest, ZSTD_e_continue);
}

bool ZstdWriterBase::WriteInternal(absl::string_view src, Writer* dest,
                                   ZSTD_EndDirective end_op) {
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - limit_pos())) {
    return FailOverflow();
  }
  ZSTD_inBuffer input = {src.data(), src.size(), 0};
  if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
    if (end_op == ZSTD_e_end) {
      // The input is flat and its size is known. This causes zstdlib to tune
      // compression parameters for that size, which should be reflected in
      // ZSTD_CCtxKey::size_hint_class.
      compressor_options_.final_size = input.size;
      compressor_options_.size_hint = input.size;
    }
    if (ABSL_PREDICT_FALSE(!CreateCompressor())) return false;
  }
  for (;;) {
    ZSTD_outBuffer output = {dest->cursor(), dest->available(), 0};
    const size_t result =
        ZSTD_compressStream2(compressor_.get(), &output, &input, end_op);
    dest->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (result == 0) {
      RIEGELI_ASSERT_EQ(input.pos, input.size)
          << "ZSTD_compressStream2() returned 0 but there are still input data";
      start_pos_ += input.pos;
      return true;
    }
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(InternalError(absl::StrCat("ZSTD_compressStream2() failed: ",
                                             ZSTD_getErrorName(result))));
    }
    if (output.pos < output.size) {
      RIEGELI_ASSERT_EQ(input.pos, input.size)
          << "ZSTD_compressStream2() returned but there are still input data "
             "and output space";
      start_pos_ += input.pos;
      return true;
    }
    if (ABSL_PREDICT_FALSE(!dest->Push())) return Fail(*dest);
  }
}

bool ZstdWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  const size_t buffered_length = written_to_buffer();
  cursor_ = start_;
  if (ABSL_PREDICT_FALSE(!WriteInternal(
          absl::string_view(start_, buffered_length), dest, ZSTD_e_flush))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!dest->Flush(flush_type))) return Fail(*dest);
  return true;
}

}  // namespace riegeli
