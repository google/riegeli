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
//  * `ZSTD_c_srcSizeHint`
//
// Using the experimental zstd API is optional. If this gets removed,
// `size_hint` is ignored by zstd if `final_size` is not set (`size_hint`
// remains used only for `BufferedWriter` tuning).
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/bytes/zstd_writer.h"

#include <stddef.h>

#include <limits>
#include <memory>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
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

#ifdef ZSTD_STATIC_LINKING_ONLY
struct ZSTD_CCtxParamsDeleter {
  void operator()(ZSTD_CCtx_params* ptr) const { ZSTD_freeCCtxParams(ptr); }
};
#endif

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
  compressor_ = RecyclingPool<ZSTD_CCtx, ZSTD_CCtxDeleter>::global().Get(
      [] {
        return std::unique_ptr<ZSTD_CCtx, ZSTD_CCtxDeleter>(ZSTD_createCCtx());
      },
      [](ZSTD_CCtx* compressor) {
        const size_t result =
            ZSTD_CCtx_reset(compressor, ZSTD_reset_session_and_parameters);
        RIEGELI_ASSERT(!ZSTD_isError(result))
            << "ZSTD_CCtx_reset() failed: " << ZSTD_getErrorName(result);
      });
  if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
    Fail(absl::InternalError("ZSTD_createCCtx() failed"));
    return;
  }
  {
    const size_t result = ZSTD_CCtx_setParameter(
        compressor_.get(), ZSTD_c_compressionLevel, compression_level);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(absl::StrCat(
          "ZSTD_CCtx_setParameter(ZSTD_c_compressionLevel) failed: ",
          ZSTD_getErrorName(result))));
      return;
    }
  }
  if (window_log != Options::kDefaultWindowLog) {
    const size_t result =
        ZSTD_CCtx_setParameter(compressor_.get(), ZSTD_c_windowLog, window_log);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("ZSTD_CCtx_setParameter(ZSTD_c_windowLog) failed: ",
                       ZSTD_getErrorName(result))));
      return;
    }
  }
  {
    const size_t result = ZSTD_CCtx_setParameter(
        compressor_.get(), ZSTD_c_checksumFlag, store_checksum ? 1 : 0);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("ZSTD_CCtx_setParameter(ZSTD_c_checksumFlag) failed: ",
                       ZSTD_getErrorName(result))));
      return;
    }
  }
  if (final_size != absl::nullopt) {
    const size_t result = ZSTD_CCtx_setPledgedSrcSize(
        compressor_.get(), IntCast<unsigned long long>(*final_size));
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("ZSTD_CCtx_setPledgedSrcSize() failed: ",
                       ZSTD_getErrorName(result))));
      return;
    }
  }
#ifdef ZSTD_STATIC_LINKING_ONLY
  else if (size_hint > 0) {
    const size_t result =
        ZSTD_CCtx_setParameter(compressor_.get(), ZSTD_c_srcSizeHint,
                               SaturatingIntCast<int>(size_hint));
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("ZSTD_CCtx_setParameter(ZSTD_c_srcSizeHint) failed: ",
                       ZSTD_getErrorName(result))));
      return;
    }
  }
#endif
}

void ZstdWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer* const dest = dest_writer();
    const absl::string_view data(start(), written_to_buffer());
    set_buffer();
    WriteInternal(data, dest, ZSTD_e_end);
  }
  compressor_.reset();
  BufferedWriter::Done();
}

bool ZstdWriterBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at uncompressed byte ", pos())));
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
  for (;;) {
    ZSTD_outBuffer output = {dest->cursor(), dest->available(), 0};
    const size_t result =
        ZSTD_compressStream2(compressor_.get(), &output, &input, end_op);
    dest->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (result == 0) {
      RIEGELI_ASSERT_EQ(input.pos, input.size)
          << "ZSTD_compressStream2() returned 0 but there are still input data";
      move_start_pos(input.pos);
      return true;
    }
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(Annotate(
          absl::InternalError(absl::StrCat("ZSTD_compressStream2() failed: ",
                                           ZSTD_getErrorName(result))),
          absl::StrCat("at byte ", dest->pos())));
    }
    if (output.pos < output.size) {
      RIEGELI_ASSERT_EQ(input.pos, input.size)
          << "ZSTD_compressStream2() returned but there are still input data "
             "and output space";
      move_start_pos(input.pos);
      return true;
    }
    if (ABSL_PREDICT_FALSE(!dest->Push())) return Fail(*dest);
  }
}

bool ZstdWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  const absl::string_view data(start(), written_to_buffer());
  set_buffer();
  if (ABSL_PREDICT_FALSE(!WriteInternal(data, dest, ZSTD_e_flush))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!dest->Flush(flush_type))) return Fail(*dest);
  return true;
}

}  // namespace riegeli
